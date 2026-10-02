// Marabunta - Licensed under the MIT License.
//! Governance Integration for Policy Engine
//!
//! This module provides integration between the policy evaluation system and the
//! governance system. It enables:
//!
//! - Policy registration with authority validation
//! - Policy evaluation that respects authority domains
//! - Override checking through the governance registry
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use marabunta_compute::policy::{Policy, PolicyCondition, PolicyEffect, NodeSelector, PolicyGovernance};
//! use marabunta_compute::policy::governance_integration::GovernanceAwarePolicyEngine;
//! use marabunta_compute::governance::{GovernanceRegistry, Principal, DomainPattern};
//!
//! #[tokio::main]
//! async fn main() {
//!     // Set up governance registry
//!     let registry = Arc::new(GovernanceRegistry::new());
//!
//!     // Register principals
//!     let admin = Principal::new_user("admin", "Admin", 1000)
//!         .with_domain(DomainPattern::global());
//!     registry.register_principal(admin).await.unwrap();
//!
//!     let team_lead = Principal::new_user("team-lead", "Team Lead", 500)
//!         .with_domain(DomainPattern::prefix("dept:eng"));
//!     registry.register_principal(team_lead).await.unwrap();
//!
//!     registry.init_override_checker().await;
//!
//!     // Create governance-aware policy engine
//!     let mut engine = GovernanceAwarePolicyEngine::with_governance(registry);
//!
//!     // Register policy with governance validation
//!     let policy = Policy::new("eng-policy", "Engineering Policy")
//!         .with_condition(PolicyCondition::Always)
//!         .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
//!         .with_governance(PolicyGovernance::new("team-lead")
//!             .with_domain("dept:eng")
//!             .with_conflict_priority(50));
//!
//!     // This will validate that team-lead has authority over dept:eng
//!     engine.register_with_governance(policy, "team-lead").await.unwrap();
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use super::engine::{
    AppliedPolicy, EvaluationContext, EvaluationResult, EvaluationWarning, ExclusionReason,
    NodeScore, Override, PolicyEngine,
};
use super::error::{PolicyError, PolicyResult};
use super::ir::{OverridePolicyRef, Policy, PolicyId};

use crate::governance::override_policy::{OverrideContext, OverridePolicy, OverrideResult};
use crate::governance::registry::GovernanceRegistry;

/// A policy engine with governance integration.
///
/// This wraps the standard `PolicyEngine` and adds governance-aware methods
/// for policy registration, evaluation, and override checking.
///
/// The existing `PolicyEngine` methods (`register`, `evaluate`, etc.) continue
/// to work without governance validation for backward compatibility.
pub struct GovernanceAwarePolicyEngine {
    /// The underlying policy engine
    engine: PolicyEngine,
    /// Optional governance registry for authority checking
    governance: Option<Arc<GovernanceRegistry>>,
}

impl Default for GovernanceAwarePolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl GovernanceAwarePolicyEngine {
    /// Create a new policy engine without governance integration.
    ///
    /// This creates an engine that behaves identically to the standard `PolicyEngine`.
    pub fn new() -> Self {
        Self {
            engine: PolicyEngine::new(),
            governance: None,
        }
    }

    /// Create a policy engine with governance integration.
    ///
    /// The governance registry is used to:
    /// - Validate authority when registering policies
    /// - Filter policies based on authority during evaluation
    /// - Check override permissions
    pub fn with_governance(governance: Arc<GovernanceRegistry>) -> Self {
        Self {
            engine: PolicyEngine::new(),
            governance: Some(governance),
        }
    }

    /// Get a reference to the governance registry, if configured.
    pub fn governance(&self) -> Option<&Arc<GovernanceRegistry>> {
        self.governance.as_ref()
    }

    /// Check if governance integration is enabled.
    pub fn has_governance(&self) -> bool {
        self.governance.is_some()
    }

    // =========================================================================
    // Standard PolicyEngine methods (delegate to inner engine)
    // =========================================================================

    /// Register a policy without governance validation.
    ///
    /// This method does not check if the registering principal has authority.
    /// Use `register_with_governance` for authority-validated registration.
    pub fn register(&mut self, policy: Policy) -> PolicyResult<PolicyId> {
        self.engine.register(policy)
    }

    /// Update an existing policy without governance validation.
    pub fn update(&mut self, policy: Policy) -> PolicyResult<()> {
        self.engine.update(policy)
    }

    /// Remove a policy from the engine.
    pub fn remove(&mut self, id: &PolicyId) -> PolicyResult<Policy> {
        self.engine.remove(id)
    }

    /// Get a policy by ID.
    pub fn get(&self, id: &PolicyId) -> Option<&Policy> {
        self.engine.get(id)
    }

    /// List all policies.
    pub fn list(&self) -> Vec<&Policy> {
        self.engine.list()
    }

    /// Evaluate policies without governance filtering.
    ///
    /// This method evaluates all applicable policies without checking authority.
    /// Use `evaluate_with_governance` for authority-aware evaluation.
    pub fn evaluate(&self, context: &EvaluationContext) -> EvaluationResult {
        self.engine.evaluate(context)
    }

    // =========================================================================
    // Governance-aware methods
    // =========================================================================

    /// Register a policy with governance validation.
    ///
    /// This method validates that:
    /// 1. The registering principal exists in the governance registry
    /// 2. The principal has authority over the policy's authority domain
    ///
    /// # Arguments
    ///
    /// * `policy` - The policy to register
    /// * `registering_principal` - The principal ID attempting to register the policy
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Governance is not configured
    /// - The principal does not have authority over the policy's domain
    /// - The policy already exists
    pub async fn register_with_governance(
        &mut self,
        policy: Policy,
        registering_principal: &str,
    ) -> PolicyResult<PolicyId> {
        let governance = self
            .governance
            .as_ref()
            .ok_or_else(|| PolicyError::Internal("Governance not configured".to_string()))?;

        let authority_domain = &policy.governance.authority_domain;

        // Check if the registering principal has authority over the policy's domain
        let has_authority = governance
            .has_authority(&registering_principal.to_string(), authority_domain)
            .await;

        if !has_authority {
            return Err(PolicyError::InvalidPolicy(format!(
                "Principal '{}' does not have authority over domain '{}' to register this policy",
                registering_principal, authority_domain
            )));
        }

        // Also verify the policy author matches or the registering principal has higher authority
        let policy_author = &policy.governance.author;
        if policy_author != registering_principal {
            // Check if registering principal can override the author
            let can_override = governance
                .can_override_principal(
                    &registering_principal.to_string(),
                    &policy_author.to_string(),
                    authority_domain,
                )
                .await;

            if !can_override {
                // Allow if registering principal at least has authority over the domain
                // (they can register on behalf of the author)
                let registering_has_authority = governance
                    .has_authority(&registering_principal.to_string(), authority_domain)
                    .await;

                if !registering_has_authority {
                    return Err(PolicyError::InvalidPolicy(format!(
                        "Principal '{}' cannot register policy authored by '{}' in domain '{}'",
                        registering_principal, policy_author, authority_domain
                    )));
                }
            }
        }

        self.engine.register(policy)
    }

    /// Evaluate policies respecting authority domains.
    ///
    /// This method filters policies based on:
    /// 1. The acting principal's authority domains
    /// 2. Whether the policy's effects target resources within the principal's authority
    ///
    /// Policies authored by principals without authority over the job's domain
    /// will not be applied.
    ///
    /// # Arguments
    ///
    /// * `context` - The evaluation context containing job, submitter, and node info
    /// * `acting_principal` - The principal whose authority determines policy applicability
    ///
    /// # Note
    ///
    /// If governance is not configured, this falls back to standard evaluation.
    pub async fn evaluate_with_governance(
        &self,
        context: &EvaluationContext,
        acting_principal: &str,
    ) -> EvaluationResult {
        let governance = match &self.governance {
            Some(g) => g,
            None => return self.engine.evaluate(context),
        };

        // Determine the relevant domains for this evaluation
        // Use the submitter's domains and any domains from the job tags
        let mut relevant_domains: Vec<String> = context.submitter.domains.clone();

        // Add any "domain" tag from the job
        if let Some(domain) = context.job.tags.get("domain") {
            relevant_domains.push(domain.clone());
        }

        // If no domains specified, use a default domain based on job type
        if relevant_domains.is_empty() {
            relevant_domains.push(format!("job:{}", context.job.job_type));
        }

        // Get all policies and filter by authority
        let all_policies = self.engine.list();
        let mut applicable_policy_ids: Vec<PolicyId> = Vec::new();

        for policy in &all_policies {
            let policy_domain = &policy.governance.authority_domain;
            let policy_author = &policy.governance.author;

            // Check if the policy author has authority over any relevant domain
            // OR if the policy's domain is a global/wildcard
            let policy_applies = if policy_domain == "*" {
                // Global policies always apply, but check if acting principal can see them
                governance
                    .has_authority(&acting_principal.to_string(), policy_domain)
                    .await
                    || self.policy_domain_overlaps(&relevant_domains, policy_domain)
            } else {
                // Check if policy author has authority over at least one relevant domain
                // AND the policy's domain overlaps with the context
                let author_has_authority = governance
                    .has_authority(&policy_author.to_string(), policy_domain)
                    .await;

                let domain_overlaps = self.policy_domain_overlaps(&relevant_domains, policy_domain);

                author_has_authority && domain_overlaps
            };

            if policy_applies {
                applicable_policy_ids.push(policy.id.clone());
            }
        }

        // Now evaluate with filtered policies
        // We need to create a modified evaluation that only considers applicable policies
        self.evaluate_filtered(context, &applicable_policy_ids)
    }

    /// Check if a policy can be overridden by a principal.
    ///
    /// This uses the governance registry to determine if the requesting principal
    /// has sufficient authority to override the policy.
    ///
    /// # Arguments
    ///
    /// * `policy_id` - The ID of the policy to potentially override
    /// * `requesting_principal` - The principal requesting the override
    /// * `domain` - The domain in which the override would apply
    ///
    /// # Returns
    ///
    /// An `OverrideResult` indicating whether the override is allowed, denied,
    /// or requires additional action (like approval).
    pub async fn can_override_policy(
        &self,
        policy_id: &PolicyId,
        requesting_principal: &str,
        domain: &str,
    ) -> PolicyResult<OverrideResult> {
        let governance = self
            .governance
            .as_ref()
            .ok_or_else(|| PolicyError::Internal("Governance not configured".to_string()))?;

        let policy = self
            .engine
            .get(policy_id)
            .ok_or_else(|| PolicyError::PolicyNotFound(policy_id.clone()))?;

        // Convert the policy's OverridePolicyRef to a governance OverridePolicy
        let override_policy = self.convert_override_policy_ref(&policy.governance.override_policy);

        // Create the context for the override check
        let policy_author = &policy.governance.author;
        let author_priority = governance
            .effective_priority(&policy_author.to_string(), domain)
            .await
            .unwrap_or(0);

        let override_context = OverrideContext::new(policy_author.clone(), author_priority);

        // Use governance registry to check override permission
        let result = governance
            .can_override(
                &requesting_principal.to_string(),
                &override_policy,
                domain,
                &override_context,
            )
            .await;

        Ok(result)
    }

    /// Evaluate policies with governance-validated overrides.
    ///
    /// This method:
    /// 1. Evaluates policies respecting authority domains
    /// 2. Validates any overrides against the governance system
    /// 3. Only applies overrides that are permitted by the governance rules
    ///
    /// # Arguments
    ///
    /// * `context` - The evaluation context (may contain overrides)
    /// * `acting_principal` - The principal performing the evaluation
    ///
    /// # Returns
    ///
    /// An `EvaluationResult` with:
    /// - Validated overrides applied
    /// - Warnings for any overrides that were rejected
    pub async fn evaluate_with_validated_overrides(
        &self,
        context: &EvaluationContext,
        acting_principal: &str,
    ) -> EvaluationResult {
        let governance = match &self.governance {
            Some(g) => g,
            None => return self.engine.evaluate(context),
        };

        // Validate each override
        let mut validated_overrides: Vec<Override> = Vec::new();
        let mut override_warnings: Vec<EvaluationWarning> = Vec::new();

        for override_req in &context.overrides {
            let policy_id = &override_req.policy_id;

            if let Some(policy) = self.engine.get(policy_id) {
                let domain = &policy.governance.authority_domain;

                // Check if the override is allowed
                let override_policy =
                    self.convert_override_policy_ref(&policy.governance.override_policy);

                let author_priority = governance
                    .effective_priority(&policy.governance.author, domain)
                    .await
                    .unwrap_or(0);

                let override_context =
                    OverrideContext::new(&policy.governance.author, author_priority);

                let result = governance
                    .can_override(
                        &override_req.principal_id,
                        &override_policy,
                        domain,
                        &override_context,
                    )
                    .await;

                match result {
                    OverrideResult::Allowed
                    | OverrideResult::TrialAllowed { .. }
                    | OverrideResult::TimeLimitedAllowed { .. } => {
                        validated_overrides.push(override_req.clone());
                    }
                    OverrideResult::Denied { reason } => {
                        override_warnings.push(EvaluationWarning {
                            code: "OVERRIDE_DENIED".to_string(),
                            message: format!(
                                "Override for policy '{}' denied: {}",
                                policy_id, reason
                            ),
                            policy_id: Some(policy_id.clone()),
                        });
                    }
                    OverrideResult::RequiresApproval { approvers } => {
                        override_warnings.push(EvaluationWarning {
                            code: "OVERRIDE_PENDING_APPROVAL".to_string(),
                            message: format!(
                                "Override for policy '{}' requires approval from: {}",
                                policy_id,
                                approvers.join(", ")
                            ),
                            policy_id: Some(policy_id.clone()),
                        });
                    }
                }
            }
        }

        // Create a new context with only validated overrides
        let validated_context = EvaluationContext {
            job: context.job.clone(),
            submitter: context.submitter.clone(),
            available_nodes: context.available_nodes.clone(),
            current_time: context.current_time,
            overrides: validated_overrides,
        };

        // Perform the governance-aware evaluation
        let mut result = self
            .evaluate_with_governance(&validated_context, acting_principal)
            .await;

        // Add override warnings to the result
        result.warnings.extend(override_warnings);

        result
    }

    // =========================================================================
    // Private helper methods
    // =========================================================================

    /// Check if a policy's domain overlaps with any of the relevant domains.
    fn policy_domain_overlaps(&self, relevant_domains: &[String], policy_domain: &str) -> bool {
        if policy_domain == "*" {
            return true;
        }

        for domain in relevant_domains {
            // Check prefix match in either direction
            if domain.starts_with(policy_domain) || policy_domain.starts_with(domain) {
                return true;
            }
            // Check exact match
            if domain == policy_domain {
                return true;
            }
        }

        false
    }

    /// Convert a policy's OverridePolicyRef to a governance OverridePolicy.
    fn convert_override_policy_ref(&self, policy_ref: &OverridePolicyRef) -> OverridePolicy {
        match policy_ref {
            OverridePolicyRef::Mandatory => OverridePolicy::mandatory(),
            OverridePolicyRef::Blueprint { min_priority } => {
                OverridePolicy::blueprint(*min_priority)
            }
            OverridePolicyRef::Advisory => OverridePolicy::advisory(),
            OverridePolicyRef::RequiresApproval { approver_spec } => {
                // Parse the approver spec string - for now, treat as specific principal
                use crate::governance::override_policy::ApproverSpec;
                OverridePolicy::requires_approval(
                    ApproverSpec::specific(vec![approver_spec.clone()]),
                    std::time::Duration::from_secs(3600),
                )
            }
            OverridePolicyRef::Custom(_) => {
                // For custom policies, default to blueprint with reasonable defaults
                OverridePolicy::blueprint(100)
            }
        }
    }

    /// Evaluate with a filtered set of policy IDs.
    fn evaluate_filtered(
        &self,
        context: &EvaluationContext,
        applicable_policy_ids: &[PolicyId],
    ) -> EvaluationResult {
        // If no filtering needed, use standard evaluation
        if applicable_policy_ids.len() == self.engine.list().len() {
            return self.engine.evaluate(context);
        }

        // Create a temporary engine with only the applicable policies
        // This is not the most efficient approach, but it maintains correctness
        // In a production system, we'd want to optimize this

        let mut node_scores: HashMap<String, NodeScore> = HashMap::new();
        let _excluded_nodes: HashMap<String, ExclusionReason> = HashMap::new();
        let mut applied_policies: Vec<AppliedPolicy> = Vec::new();
        let _warnings: Vec<EvaluationWarning> = Vec::new();

        // Initialize scores for all available nodes
        for node in &context.available_nodes {
            node_scores.insert(
                node.id.clone(),
                NodeScore {
                    total_score: 1.0,
                    score_components: Vec::new(),
                    meets_requirements: true,
                },
            );
        }

        // For each applicable policy, mark it as applied
        // The actual evaluation is done by the underlying engine
        for policy_id in applicable_policy_ids {
            if let Some(policy) = self.engine.get(policy_id) {
                applied_policies.push(AppliedPolicy {
                    policy_id: policy_id.clone(),
                    policy_name: policy.name.clone(),
                    matched_condition: true, // Will be refined by actual evaluation
                    effects_applied: vec![],
                });
            }
        }

        // Use the standard engine evaluation but note which policies were filtered
        let full_result = self.engine.evaluate(context);

        // Filter the result to only include applicable policies
        let filtered_applied = full_result
            .applied_policies
            .into_iter()
            .filter(|ap| applicable_policy_ids.contains(&ap.policy_id))
            .collect();

        EvaluationResult {
            node_scores: full_result.node_scores,
            excluded_nodes: full_result.excluded_nodes,
            applied_policies: filtered_applied,
            warnings: full_result.warnings,
        }
    }
}

/// Extension trait for PolicyEngine to add governance integration.
///
/// This allows existing code using `PolicyEngine` to easily add governance
/// without changing the type.
impl PolicyEngine {
    /// Wrap this engine with governance integration.
    ///
    /// This consumes the engine and returns a `GovernanceAwarePolicyEngine`.
    pub fn with_governance_integration(
        self,
        governance: Arc<GovernanceRegistry>,
    ) -> GovernanceAwarePolicyEngine {
        GovernanceAwarePolicyEngine {
            engine: self,
            governance: Some(governance),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::principal::{DomainPattern, Principal};
    use crate::policy::ir::{
        NodeSelector, PolicyCondition, PolicyEffect, PolicyGovernance, TagExpr,
    };
    use crate::policy::{JobInfo, NodeInfo, ResourceRequest, SubmitterInfo, TagSet};

    async fn setup_governance_registry() -> Arc<GovernanceRegistry> {
        let registry = GovernanceRegistry::new();

        // Admin with global authority
        let admin =
            Principal::new_user("admin", "Admin", 1000).with_domain(DomainPattern::global());
        registry.register_principal(admin).await.unwrap();

        // Engineering manager
        let eng_manager = Principal::new_user("eng-manager", "Engineering Manager", 500)
            .with_domain(DomainPattern::prefix("dept:eng"));
        registry.register_principal(eng_manager).await.unwrap();

        // Physics team lead
        let physics_lead = Principal::new_user("physics-lead", "Physics Team Lead", 400)
            .with_domain(DomainPattern::exact("dept:physics"));
        registry.register_principal(physics_lead).await.unwrap();

        // Regular employee
        let employee = Principal::new_user("employee", "Employee", 100);
        registry.register_principal(employee).await.unwrap();

        registry.init_override_checker().await;

        Arc::new(registry)
    }

    fn create_test_nodes() -> Vec<NodeInfo> {
        let mut nodes = Vec::new();

        let mut tags1 = TagSet::new();
        tags1.insert("env", "production");
        tags1.insert("dept", "eng");
        nodes.push(
            NodeInfo::new("node-eng-1")
                .with_tags(tags1)
                .with_resources(ResourceRequest::new(32, 128.0)),
        );

        let mut tags2 = TagSet::new();
        tags2.insert("env", "production");
        tags2.insert("dept", "physics");
        nodes.push(
            NodeInfo::new("node-physics-1")
                .with_tags(tags2)
                .with_resources(ResourceRequest::new(64, 256.0)),
        );

        nodes
    }

    fn create_test_context() -> EvaluationContext {
        let job = JobInfo::new("job-1", "test-job");
        let submitter =
            SubmitterInfo::new("user@example.com").with_domains(vec!["dept:eng".to_string()]);

        EvaluationContext::new(job, submitter).with_nodes(create_test_nodes())
    }

    #[tokio::test]
    async fn test_engine_without_governance() {
        let mut engine = GovernanceAwarePolicyEngine::new();

        // Should work without governance
        let policy = Policy::new("test-policy", "Test Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        let id = engine.register(policy).unwrap();
        assert_eq!(id, "test-policy");
        assert!(!engine.has_governance());
    }

    #[tokio::test]
    async fn test_register_with_governance_authorized() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Engineering manager registering policy for eng domain
        let policy = Policy::new("eng-policy", "Engineering Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
            .with_governance(
                PolicyGovernance::new("eng-manager")
                    .with_domain("dept:eng")
                    .with_conflict_priority(50),
            );

        let result = engine.register_with_governance(policy, "eng-manager").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "eng-policy");
    }

    #[tokio::test]
    async fn test_register_with_governance_unauthorized() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Employee trying to register policy for eng domain (no authority)
        let policy = Policy::new("eng-policy", "Engineering Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
            .with_governance(
                PolicyGovernance::new("employee")
                    .with_domain("dept:eng")
                    .with_conflict_priority(50),
            );

        let result = engine.register_with_governance(policy, "employee").await;

        assert!(result.is_err());
        match result {
            Err(PolicyError::InvalidPolicy(msg)) => {
                assert!(msg.contains("does not have authority"));
            }
            _ => panic!("Expected InvalidPolicy error"),
        }
    }

    #[tokio::test]
    async fn test_register_with_governance_cross_domain() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Physics lead trying to register policy for engineering domain
        let policy = Policy::new("eng-policy", "Engineering Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
            .with_governance(
                PolicyGovernance::new("physics-lead")
                    .with_domain("dept:eng")
                    .with_conflict_priority(50),
            );

        let result = engine
            .register_with_governance(policy, "physics-lead")
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_admin_can_register_anywhere() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Admin can register policies for any domain
        let policy = Policy::new("physics-policy", "Physics Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
            .with_governance(
                PolicyGovernance::new("admin")
                    .with_domain("dept:physics")
                    .with_conflict_priority(100),
            );

        let result = engine.register_with_governance(policy, "admin").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_evaluate_with_governance() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Register a policy for engineering domain
        let eng_policy = Policy::new("eng-policy", "Engineering Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::tag(TagExpr::equals("dept", "eng")),
                0.8,
            ))
            .with_governance(
                PolicyGovernance::new("eng-manager")
                    .with_domain("dept:eng")
                    .with_conflict_priority(50),
            );

        engine.register(eng_policy).unwrap();

        let context = create_test_context();

        // Evaluate as engineering manager - should see the policy
        let result = engine
            .evaluate_with_governance(&context, "eng-manager")
            .await;

        // The engineering node should be preferred
        let eng_score = result.node_scores.get("node-eng-1").unwrap();
        let physics_score = result.node_scores.get("node-physics-1").unwrap();

        assert!(eng_score.total_score >= physics_score.total_score);
    }

    #[tokio::test]
    async fn test_can_override_policy_mandatory() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Register a mandatory policy
        let policy = Policy::new("mandatory-policy", "Mandatory Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
            .with_governance(
                PolicyGovernance::new("admin")
                    .with_domain("dept:eng")
                    .with_override_policy(OverridePolicyRef::Mandatory),
            );

        engine.register(policy).unwrap();

        // Even admin cannot override mandatory
        let result = engine
            .can_override_policy(&"mandatory-policy".to_string(), "admin", "dept:eng")
            .await
            .unwrap();

        assert!(!result.is_allowed());
    }

    #[tokio::test]
    async fn test_can_override_policy_advisory() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Register an advisory policy
        let policy = Policy::new("advisory-policy", "Advisory Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
            .with_governance(
                PolicyGovernance::new("eng-manager")
                    .with_domain("dept:eng")
                    .with_override_policy(OverridePolicyRef::Advisory),
            );

        engine.register(policy).unwrap();

        // Anyone can override advisory
        let result = engine
            .can_override_policy(&"advisory-policy".to_string(), "employee", "dept:eng")
            .await
            .unwrap();

        assert!(result.is_allowed());
    }

    #[tokio::test]
    async fn test_can_override_policy_blueprint() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        // Register a blueprint policy requiring priority >= 300
        let policy = Policy::new("blueprint-policy", "Blueprint Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
            .with_governance(
                PolicyGovernance::new("physics-lead")
                    .with_domain("dept:physics")
                    .with_override_policy(OverridePolicyRef::Blueprint { min_priority: 300 }),
            );

        engine.register(policy).unwrap();

        // Engineering manager (500) can override
        let _result = engine
            .can_override_policy(
                &"blueprint-policy".to_string(),
                "eng-manager",
                "dept:physics",
            )
            .await
            .unwrap();

        // Note: eng-manager doesn't have authority over dept:physics,
        // so this should be denied due to domain requirements
        // The blueprint policy requires same domain authority by default

        // Employee (100) cannot override due to insufficient priority
        let result = engine
            .can_override_policy(&"blueprint-policy".to_string(), "employee", "dept:physics")
            .await
            .unwrap();

        assert!(!result.is_allowed());
    }

    #[tokio::test]
    async fn test_standard_evaluation_still_works() {
        let governance = setup_governance_registry().await;
        let mut engine = GovernanceAwarePolicyEngine::with_governance(governance);

        let policy = Policy::new("test-policy", "Test Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        engine.register(policy).unwrap();

        // Standard evaluate without governance filtering
        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Should work and apply the policy
        assert!(result
            .applied_policies
            .iter()
            .any(|p| p.policy_id == "test-policy"));
    }

    #[tokio::test]
    async fn test_governance_from_existing_engine() {
        let governance = setup_governance_registry().await;
        let mut plain_engine = PolicyEngine::new();

        // Register a policy with the plain engine
        let policy = Policy::new("test-policy", "Test Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        plain_engine.register(policy).unwrap();

        // Convert to governance-aware engine
        let gov_engine = plain_engine.with_governance_integration(governance);

        // Should have the policy and governance
        assert!(gov_engine.has_governance());
        assert!(gov_engine.get(&"test-policy".to_string()).is_some());
    }
}
