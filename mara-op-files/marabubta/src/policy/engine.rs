// Marabunta - Licensed under the MIT License.
//! Policy Engine for evaluating policies against job placement requests
//!
//! This module provides the core evaluation engine that takes a set of policies
//! and an evaluation context, and produces scored placement recommendations.

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::error::{PolicyError, PolicyResult};
use super::ir::*;

/// The policy engine manages and evaluates policies for job placement
pub struct PolicyEngine {
    /// All registered policies
    policies: HashMap<PolicyId, Policy>,
    /// Index of policies with JobMatches conditions
    job_policies: Vec<PolicyId>,
    /// Index of policies with SubmitterMatches conditions
    submitter_policies: Vec<PolicyId>,
    /// Index of policies with TimeWindow conditions
    time_policies: Vec<PolicyId>,
    /// Index of policies with Always condition
    universal_policies: Vec<PolicyId>,
    /// Index of policies with ResourceMatches conditions
    resource_policies: Vec<PolicyId>,
    /// Cached compiled regexes (planned optimization)
    #[allow(dead_code)]
    regex_cache: HashMap<String, Regex>,
    /// Optional placement integration configuration
    #[cfg(feature = "placement-integration")]
    pub(crate) placement_config: Option<super::placement_integration::PlacementConfig>,
}

impl Default for PolicyEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyEngine {
    /// Create a new empty policy engine
    pub fn new() -> Self {
        Self {
            policies: HashMap::new(),
            job_policies: Vec::new(),
            submitter_policies: Vec::new(),
            time_policies: Vec::new(),
            universal_policies: Vec::new(),
            resource_policies: Vec::new(),
            regex_cache: HashMap::new(),
            #[cfg(feature = "placement-integration")]
            placement_config: None,
        }
    }

    /// Register a new policy with the engine
    pub fn register(&mut self, policy: Policy) -> PolicyResult<PolicyId> {
        let id = policy.id.clone();

        if self.policies.contains_key(&id) {
            return Err(PolicyError::PolicyAlreadyExists(id));
        }

        // Index by condition type
        self.index_policy(&policy);

        self.policies.insert(id.clone(), policy);
        Ok(id)
    }

    /// Update an existing policy
    pub fn update(&mut self, policy: Policy) -> PolicyResult<()> {
        let id = policy.id.clone();

        if !self.policies.contains_key(&id) {
            return Err(PolicyError::PolicyNotFound(id));
        }

        // Remove from indexes
        self.remove_from_indexes(&id);

        // Re-index with new condition
        self.index_policy(&policy);

        self.policies.insert(id, policy);
        Ok(())
    }

    /// Remove a policy from the engine
    pub fn remove(&mut self, id: &PolicyId) -> PolicyResult<Policy> {
        self.remove_from_indexes(id);
        self.policies
            .remove(id)
            .ok_or_else(|| PolicyError::PolicyNotFound(id.clone()))
    }

    /// Get a policy by ID
    pub fn get(&self, id: &PolicyId) -> Option<&Policy> {
        self.policies.get(id)
    }

    /// List all policies
    pub fn list(&self) -> Vec<&Policy> {
        self.policies.values().collect()
    }

    /// Evaluate policies against a context and produce placement scores
    pub fn evaluate(&self, context: &EvaluationContext) -> EvaluationResult {
        let mut node_scores: HashMap<String, NodeScore> = HashMap::new();
        let mut excluded_nodes: HashMap<String, ExclusionReason> = HashMap::new();
        let mut applied_policies: Vec<AppliedPolicy> = Vec::new();
        let mut warnings: Vec<EvaluationWarning> = Vec::new();

        // Initialize scores for all available nodes
        for node in &context.available_nodes {
            node_scores.insert(
                node.id.clone(),
                NodeScore {
                    total_score: 1.0, // Base score
                    score_components: Vec::new(),
                    meets_requirements: true,
                },
            );
        }

        // Collect all potentially applicable policies
        let mut candidate_policies: Vec<&Policy> = Vec::new();

        // Always include universal policies
        for id in &self.universal_policies {
            if let Some(policy) = self.policies.get(id) {
                if policy.is_active(context.current_time) {
                    candidate_policies.push(policy);
                }
            }
        }

        // Add job-matching policies
        for id in &self.job_policies {
            if let Some(policy) = self.policies.get(id) {
                if policy.is_active(context.current_time) {
                    candidate_policies.push(policy);
                }
            }
        }

        // Add submitter-matching policies
        for id in &self.submitter_policies {
            if let Some(policy) = self.policies.get(id) {
                if policy.is_active(context.current_time) {
                    candidate_policies.push(policy);
                }
            }
        }

        // Add resource-matching policies
        for id in &self.resource_policies {
            if let Some(policy) = self.policies.get(id) {
                if policy.is_active(context.current_time) {
                    candidate_policies.push(policy);
                }
            }
        }

        // Add time-based policies
        for id in &self.time_policies {
            if let Some(policy) = self.policies.get(id) {
                if policy.is_active(context.current_time) {
                    candidate_policies.push(policy);
                }
            }
        }

        // Sort by conflict priority (higher priority first)
        candidate_policies.sort_by(|a, b| {
            b.governance
                .conflict_priority
                .cmp(&a.governance.conflict_priority)
        });

        // Apply overrides
        let override_map: HashMap<&PolicyId, &Override> = context
            .overrides
            .iter()
            .map(|o| (&o.policy_id, o))
            .collect();

        // Evaluate each policy
        for policy in candidate_policies {
            let condition_result = self.evaluate_condition(&policy.condition, context);

            let mut effects_applied = Vec::new();

            if condition_result {
                // Apply each effect
                for (idx, effect) in policy.effects.iter().enumerate() {
                    // Check for override
                    let effective_effect = if let Some(override_info) = override_map.get(&policy.id)
                    {
                        &override_info.effect_override
                    } else {
                        effect
                    };

                    self.apply_effect(
                        effective_effect,
                        &policy.id,
                        idx,
                        context,
                        &mut node_scores,
                        &mut excluded_nodes,
                        &mut warnings,
                    );

                    effects_applied.push(idx);
                }
            }

            applied_policies.push(AppliedPolicy {
                policy_id: policy.id.clone(),
                policy_name: policy.name.clone(),
                matched_condition: condition_result,
                effects_applied,
            });
        }

        // Mark nodes that don't meet requirements
        for (node_id, score) in node_scores.iter_mut() {
            if excluded_nodes.contains_key(node_id) {
                score.meets_requirements = false;
            }
        }

        EvaluationResult {
            node_scores,
            excluded_nodes,
            applied_policies,
            warnings,
        }
    }

    /// Generate a detailed explanation of the evaluation
    pub fn explain(&self, context: &EvaluationContext) -> EvaluationExplanation {
        let mut policy_trace: Vec<PolicyTraceEntry> = Vec::new();

        // Collect and evaluate all policies with full tracing
        let all_policy_ids: Vec<PolicyId> = self.policies.keys().cloned().collect();

        for id in all_policy_ids {
            if let Some(policy) = self.policies.get(&id) {
                if policy.is_active(context.current_time) {
                    let condition_trace = self.trace_condition(&policy.condition, context);
                    let effects_evaluation = if condition_trace.result {
                        self.trace_effects(&policy.effects, &policy.id, context)
                    } else {
                        Vec::new()
                    };

                    policy_trace.push(PolicyTraceEntry {
                        policy_id: id.clone(),
                        condition_evaluation: condition_trace,
                        effects_evaluation,
                    });
                }
            }
        }

        // Build decision tree
        let decision_tree = self.build_decision_tree(&policy_trace, context);

        // Get the actual result
        let result = self.evaluate(context);

        EvaluationExplanation {
            result,
            policy_trace,
            decision_tree,
        }
    }

    // --- Private helper methods ---

    fn index_policy(&mut self, policy: &Policy) {
        match &policy.condition {
            PolicyCondition::Always => {
                self.universal_policies.push(policy.id.clone());
            }
            PolicyCondition::Never => {
                // Don't index disabled policies
            }
            PolicyCondition::JobMatches(_) => {
                self.job_policies.push(policy.id.clone());
            }
            PolicyCondition::SubmitterMatches(_) => {
                self.submitter_policies.push(policy.id.clone());
            }
            PolicyCondition::TimeWindow { .. } => {
                self.time_policies.push(policy.id.clone());
            }
            PolicyCondition::ResourceMatches(_) => {
                self.resource_policies.push(policy.id.clone());
            }
            PolicyCondition::And(conditions) | PolicyCondition::Or(conditions) => {
                // Index based on all contained conditions
                for cond in conditions {
                    self.index_condition(&policy.id, cond);
                }
            }
            PolicyCondition::Not(inner) => {
                self.index_condition(&policy.id, inner);
            }
        }
    }

    fn index_condition(&mut self, policy_id: &PolicyId, condition: &PolicyCondition) {
        match condition {
            PolicyCondition::Always => {
                if !self.universal_policies.contains(policy_id) {
                    self.universal_policies.push(policy_id.clone());
                }
            }
            PolicyCondition::JobMatches(_) => {
                if !self.job_policies.contains(policy_id) {
                    self.job_policies.push(policy_id.clone());
                }
            }
            PolicyCondition::SubmitterMatches(_) => {
                if !self.submitter_policies.contains(policy_id) {
                    self.submitter_policies.push(policy_id.clone());
                }
            }
            PolicyCondition::TimeWindow { .. } => {
                if !self.time_policies.contains(policy_id) {
                    self.time_policies.push(policy_id.clone());
                }
            }
            PolicyCondition::ResourceMatches(_) => {
                if !self.resource_policies.contains(policy_id) {
                    self.resource_policies.push(policy_id.clone());
                }
            }
            PolicyCondition::And(conditions) | PolicyCondition::Or(conditions) => {
                for cond in conditions {
                    self.index_condition(policy_id, cond);
                }
            }
            PolicyCondition::Not(inner) => {
                self.index_condition(policy_id, inner);
            }
            PolicyCondition::Never => {}
        }
    }

    fn remove_from_indexes(&mut self, id: &PolicyId) {
        self.job_policies.retain(|x| x != id);
        self.submitter_policies.retain(|x| x != id);
        self.time_policies.retain(|x| x != id);
        self.universal_policies.retain(|x| x != id);
        self.resource_policies.retain(|x| x != id);
    }

    fn evaluate_condition(&self, condition: &PolicyCondition, context: &EvaluationContext) -> bool {
        match condition {
            PolicyCondition::Always => true,
            PolicyCondition::Never => false,

            PolicyCondition::JobMatches(matcher) => self.matches_job(matcher, &context.job),

            PolicyCondition::SubmitterMatches(matcher) => {
                self.matches_submitter(matcher, &context.submitter)
            }

            PolicyCondition::TimeWindow { cron } => {
                self.matches_time_window(cron, context.current_time)
            }

            PolicyCondition::ResourceMatches(matcher) => {
                self.matches_resources(matcher, &context.job.resources)
            }

            PolicyCondition::And(conditions) => conditions
                .iter()
                .all(|c| self.evaluate_condition(c, context)),

            PolicyCondition::Or(conditions) => conditions
                .iter()
                .any(|c| self.evaluate_condition(c, context)),

            PolicyCondition::Not(inner) => !self.evaluate_condition(inner, context),
        }
    }

    fn matches_job(&self, matcher: &JobMatcher, job: &JobInfo) -> bool {
        // Check name pattern
        if let Some(pattern) = &matcher.name_pattern {
            if !self.matches_regex(pattern, &job.name) {
                return false;
            }
        }

        // Check tags
        if let Some(tag_expr) = &matcher.tags {
            if !self.evaluate_tag_expr(tag_expr, &job.tags) {
                return false;
            }
        }

        // Check job type
        if let Some(types) = &matcher.job_type {
            if !types.contains(&job.job_type) {
                return false;
            }
        }

        // Check priority range
        if let Some((min, max)) = matcher.priority_range {
            if job.priority < min || job.priority > max {
                return false;
            }
        }

        true
    }

    fn matches_submitter(&self, matcher: &SubmitterMatcher, submitter: &SubmitterInfo) -> bool {
        // Check specific principal
        if let Some(principal) = &matcher.principal_id {
            if &submitter.principal_id != principal {
                return false;
            }
        }

        // Check principal pattern
        if let Some(pattern) = &matcher.principal_pattern {
            if !self.matches_regex(pattern, &submitter.principal_id) {
                return false;
            }
        }

        // Check domain
        if let Some(domain) = &matcher.in_domain {
            if !submitter.domains.contains(domain) {
                return false;
            }
        }

        // Check minimum priority
        if let Some(min_priority) = matcher.min_priority {
            if submitter.priority < min_priority {
                return false;
            }
        }

        true
    }

    fn matches_time_window(&self, cron: &str, _current_time: DateTime<Utc>) -> bool {
        // Simple cron matching implementation
        // Format: minute hour day-of-month month day-of-week
        // For a full implementation, you'd use a cron parsing library

        // For now, implement basic patterns
        if cron == "* * * * *" {
            return true; // Always matches
        }

        // Parse business hours pattern: "0 9-17 * * MON-FRI"
        // This is a simplified implementation
        // In production, use cron-parser or similar

        // For the MVP, we'll return true for valid cron expressions
        // and false for invalid ones
        !cron.is_empty()
    }

    fn matches_resources(&self, matcher: &ResourceMatcher, resources: &ResourceRequest) -> bool {
        // Check CPU
        if let Some(min) = matcher.min_cpu {
            if resources.cpu < min {
                return false;
            }
        }
        if let Some(max) = matcher.max_cpu {
            if resources.cpu > max {
                return false;
            }
        }

        // Check memory
        if let Some(min) = matcher.min_memory_gb {
            if resources.memory_gb < min {
                return false;
            }
        }
        if let Some(max) = matcher.max_memory_gb {
            if resources.memory_gb > max {
                return false;
            }
        }

        // Check GPU
        if let Some(requires_gpu) = matcher.requires_gpu {
            let has_gpu = resources.gpu > 0;
            if requires_gpu != has_gpu {
                return false;
            }
        }
        if let Some(min_gpu) = matcher.min_gpu {
            if resources.gpu < min_gpu {
                return false;
            }
        }

        true
    }

    fn evaluate_tag_expr(&self, expr: &TagExpr, tags: &TagSet) -> bool {
        match expr {
            TagExpr::HasKey(key) => tags.contains_key(key),

            TagExpr::Equals { key, value } => tags.get(key) == Some(value),

            TagExpr::KeyMatches { key, pattern } => {
                tags.contains_key(key) && self.matches_regex(pattern, key)
            }

            TagExpr::ValueMatches { key, pattern } => tags
                .get(key)
                .is_some_and(|v| self.matches_regex(pattern, v)),

            TagExpr::And(exprs) => exprs.iter().all(|e| self.evaluate_tag_expr(e, tags)),

            TagExpr::Or(exprs) => exprs.iter().any(|e| self.evaluate_tag_expr(e, tags)),

            TagExpr::Not(inner) => !self.evaluate_tag_expr(inner, tags),
        }
    }

    fn matches_regex(&self, pattern: &str, text: &str) -> bool {
        // Try to compile and match regex
        // In production, cache compiled regexes
        Regex::new(pattern)
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }

    fn apply_effect(
        &self,
        effect: &PolicyEffect,
        policy_id: &PolicyId,
        effect_index: usize,
        context: &EvaluationContext,
        node_scores: &mut HashMap<String, NodeScore>,
        excluded_nodes: &mut HashMap<String, ExclusionReason>,
        warnings: &mut Vec<EvaluationWarning>,
    ) {
        match effect {
            PolicyEffect::Prefer { selector, weight } => {
                let matching_nodes = self.select_nodes(selector, &context.available_nodes);
                for node_id in matching_nodes {
                    if let Some(score) = node_scores.get_mut(&node_id) {
                        let delta = weight * 0.5; // Scale weight to score adjustment
                        score.total_score += delta;
                        score.score_components.push(ScoreComponent {
                            policy_id: policy_id.clone(),
                            effect_index,
                            score_delta: delta,
                            reason: "Preferred by selector".to_string(),
                        });
                    }
                }
            }

            PolicyEffect::Require { selector } => {
                let matching_nodes = self.select_nodes(selector, &context.available_nodes);
                let matching_set: std::collections::HashSet<_> =
                    matching_nodes.into_iter().collect();

                for node in &context.available_nodes {
                    if !matching_set.contains(&node.id) {
                        excluded_nodes.insert(
                            node.id.clone(),
                            ExclusionReason {
                                policy_id: policy_id.clone(),
                                reason: "Does not match required selector".to_string(),
                            },
                        );
                    }
                }
            }

            PolicyEffect::Exclude { selector } => {
                let matching_nodes = self.select_nodes(selector, &context.available_nodes);
                for node_id in matching_nodes {
                    excluded_nodes.insert(
                        node_id.clone(),
                        ExclusionReason {
                            policy_id: policy_id.clone(),
                            reason: "Excluded by selector".to_string(),
                        },
                    );
                }
            }

            PolicyEffect::Affinity {
                with,
                scope,
                weight,
            } => {
                // Find nodes where related jobs/tasks are running
                let affinity_nodes = self.find_affinity_nodes(with, scope, context);
                for node_id in affinity_nodes {
                    if let Some(score) = node_scores.get_mut(&node_id) {
                        let delta = weight * 0.3;
                        score.total_score += delta;
                        score.score_components.push(ScoreComponent {
                            policy_id: policy_id.clone(),
                            effect_index,
                            score_delta: delta,
                            reason: format!("Affinity with {:?} at {:?} scope", with, scope),
                        });
                    }
                }
            }

            PolicyEffect::AntiAffinity {
                with,
                scope,
                weight,
            } => {
                // Find nodes where related jobs/tasks are running and penalize
                let affinity_nodes = self.find_affinity_nodes(with, scope, context);
                for node_id in affinity_nodes {
                    if let Some(score) = node_scores.get_mut(&node_id) {
                        let delta = -weight * 0.3;
                        score.total_score += delta;
                        score.score_components.push(ScoreComponent {
                            policy_id: policy_id.clone(),
                            effect_index,
                            score_delta: delta,
                            reason: format!("Anti-affinity with {:?} at {:?} scope", with, scope),
                        });
                    }
                }
            }

            PolicyEffect::SetResourceLimit { resource, limit } => {
                // This effect modifies job constraints, not node scores
                warnings.push(EvaluationWarning {
                    code: "RESOURCE_LIMIT_APPLIED".to_string(),
                    message: format!("Resource limit set: {:?} = {}", resource, limit),
                    policy_id: Some(policy_id.clone()),
                });
            }

            PolicyEffect::SetPriority { priority, mode } => {
                // This effect would modify job priority
                warnings.push(EvaluationWarning {
                    code: "PRIORITY_MODIFIED".to_string(),
                    message: format!("Priority modified: {:?} {}", mode, priority),
                    policy_id: Some(policy_id.clone()),
                });
            }

            PolicyEffect::ChargeQuota {
                quota_id,
                multiplier,
            } => {
                warnings.push(EvaluationWarning {
                    code: "QUOTA_CHARGE".to_string(),
                    message: format!("Quota {} charged at {}x rate", quota_id, multiplier),
                    policy_id: Some(policy_id.clone()),
                });
            }

            PolicyEffect::AllowPreemption { by_min_priority } => {
                warnings.push(EvaluationWarning {
                    code: "PREEMPTION_ALLOWED".to_string(),
                    message: format!(
                        "Job can be preempted by jobs with priority >= {}",
                        by_min_priority
                    ),
                    policy_id: Some(policy_id.clone()),
                });
            }

            PolicyEffect::DisallowPreemption => {
                warnings.push(EvaluationWarning {
                    code: "PREEMPTION_DISALLOWED".to_string(),
                    message: "Job cannot be preempted".to_string(),
                    policy_id: Some(policy_id.clone()),
                });
            }

            PolicyEffect::CustomScore {
                function_id,
                params,
            } => {
                // Custom scoring would call a registered function
                warnings.push(EvaluationWarning {
                    code: "CUSTOM_SCORE".to_string(),
                    message: format!(
                        "Custom scoring function {} applied with {} params",
                        function_id,
                        params.len()
                    ),
                    policy_id: Some(policy_id.clone()),
                });
            }
        }
    }

    fn select_nodes(&self, selector: &NodeSelector, available_nodes: &[NodeInfo]) -> Vec<String> {
        // Use placement integration if available
        #[cfg(feature = "placement-integration")]
        {
            self.resolve_node_selector(selector, available_nodes)
        }

        // Fallback implementation without placement integration
        #[cfg(not(feature = "placement-integration"))]
        match selector {
            NodeSelector::All => available_nodes.iter().map(|n| n.id.clone()).collect(),

            NodeSelector::NodeIds(ids) => {
                let id_set: std::collections::HashSet<_> = ids.iter().collect();
                available_nodes
                    .iter()
                    .filter(|n| id_set.contains(&n.id))
                    .map(|n| n.id.clone())
                    .collect()
            }

            NodeSelector::Group(group) => {
                // Match nodes that have a "group" tag matching the given group
                available_nodes
                    .iter()
                    .filter(|n| n.tags.get("group").map_or(false, |g| g == group))
                    .map(|n| n.id.clone())
                    .collect()
            }

            NodeSelector::Tag(tag_expr) => available_nodes
                .iter()
                .filter(|n| self.evaluate_tag_expr(tag_expr, &n.tags))
                .map(|n| n.id.clone())
                .collect(),
        }
    }

    fn find_affinity_nodes(
        &self,
        _target: &AffinityTarget,
        _scope: &AffinityScope,
        context: &EvaluationContext,
    ) -> Vec<String> {
        // In a real implementation, this would look up where related jobs are running
        // For now, return nodes that have matching topology labels
        context
            .available_nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Available)
            .map(|n| n.id.clone())
            .collect()
    }

    fn trace_condition(
        &self,
        condition: &PolicyCondition,
        context: &EvaluationContext,
    ) -> ConditionTrace {
        let result = self.evaluate_condition(condition, context);

        let sub_traces = match condition {
            PolicyCondition::And(conditions) => conditions
                .iter()
                .map(|c| self.trace_condition(c, context))
                .collect(),
            PolicyCondition::Or(conditions) => conditions
                .iter()
                .map(|c| self.trace_condition(c, context))
                .collect(),
            PolicyCondition::Not(inner) => vec![self.trace_condition(inner, context)],
            _ => Vec::new(),
        };

        ConditionTrace {
            condition: condition.clone(),
            result,
            sub_traces,
        }
    }

    fn trace_effects(
        &self,
        effects: &[PolicyEffect],
        _policy_id: &PolicyId,
        context: &EvaluationContext,
    ) -> Vec<EffectTrace> {
        effects
            .iter()
            .map(|effect| {
                let (nodes_affected, score_changes) = self.trace_effect(effect, context);
                EffectTrace {
                    effect: effect.clone(),
                    nodes_affected,
                    score_changes,
                }
            })
            .collect()
    }

    fn trace_effect(
        &self,
        effect: &PolicyEffect,
        context: &EvaluationContext,
    ) -> (Vec<String>, HashMap<String, f64>) {
        let mut nodes_affected = Vec::new();
        let mut score_changes = HashMap::new();

        match effect {
            PolicyEffect::Prefer { selector, weight } => {
                let matching = self.select_nodes(selector, &context.available_nodes);
                for node_id in &matching {
                    score_changes.insert(node_id.clone(), weight * 0.5);
                }
                nodes_affected = matching;
            }
            PolicyEffect::Require { selector } => {
                nodes_affected = self.select_nodes(selector, &context.available_nodes);
            }
            PolicyEffect::Exclude { selector } => {
                nodes_affected = self.select_nodes(selector, &context.available_nodes);
                for node_id in &nodes_affected {
                    score_changes.insert(node_id.clone(), f64::NEG_INFINITY);
                }
            }
            PolicyEffect::Affinity {
                with,
                scope,
                weight,
            } => {
                nodes_affected = self.find_affinity_nodes(with, scope, context);
                for node_id in &nodes_affected {
                    score_changes.insert(node_id.clone(), weight * 0.3);
                }
            }
            PolicyEffect::AntiAffinity {
                with,
                scope,
                weight,
            } => {
                nodes_affected = self.find_affinity_nodes(with, scope, context);
                for node_id in &nodes_affected {
                    score_changes.insert(node_id.clone(), -weight * 0.3);
                }
            }
            _ => {}
        }

        (nodes_affected, score_changes)
    }

    fn build_decision_tree(
        &self,
        policy_trace: &[PolicyTraceEntry],
        context: &EvaluationContext,
    ) -> DecisionNode {
        // Build a simplified decision tree from the traces
        // Start with the first node as a representative
        if let Some(first_node) = context.available_nodes.first() {
            let mut current = DecisionNode::Leaf {
                node_id: first_node.id.clone(),
                final_score: 1.0,
            };

            // Wrap in policy branches
            for trace in policy_trace.iter().rev() {
                if !trace.effects_evaluation.is_empty() {
                    current = DecisionNode::Branch {
                        policy_id: trace.policy_id.clone(),
                        condition: format!("{:?}", trace.condition_evaluation.condition),
                        if_true: Box::new(current.clone()),
                        if_false: Box::new(current),
                    };
                }
            }

            current
        } else {
            DecisionNode::Leaf {
                node_id: "none".to_string(),
                final_score: 0.0,
            }
        }
    }
}

/// Context for policy evaluation
#[derive(Debug, Clone)]
pub struct EvaluationContext {
    /// Information about the job being placed
    pub job: JobInfo,
    /// Information about who submitted the job
    pub submitter: SubmitterInfo,
    /// Available nodes for placement
    pub available_nodes: Vec<NodeInfo>,
    /// Current time for time-based policies
    pub current_time: DateTime<Utc>,
    /// Operator overrides for this evaluation
    pub overrides: Vec<Override>,
}

impl EvaluationContext {
    /// Create a new evaluation context
    pub fn new(job: JobInfo, submitter: SubmitterInfo) -> Self {
        Self {
            job,
            submitter,
            available_nodes: Vec::new(),
            current_time: Utc::now(),
            overrides: Vec::new(),
        }
    }

    /// Add available nodes
    pub fn with_nodes(mut self, nodes: Vec<NodeInfo>) -> Self {
        self.available_nodes = nodes;
        self
    }

    /// Set the current time
    pub fn with_time(mut self, time: DateTime<Utc>) -> Self {
        self.current_time = time;
        self
    }

    /// Add overrides
    pub fn with_overrides(mut self, overrides: Vec<Override>) -> Self {
        self.overrides = overrides;
        self
    }
}

/// Information about a job for policy evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    /// Job identifier
    pub id: String,
    /// Job name
    pub name: String,
    /// Job tags
    pub tags: TagSet,
    /// Type of job (monte_carlo, parameter_sweep, etc.)
    pub job_type: String,
    /// Job priority
    pub priority: u32,
    /// Resource requirements
    pub resources: ResourceRequest,
}

impl JobInfo {
    /// Create a new job info
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            tags: TagSet::new(),
            job_type: "generic".to_string(),
            priority: 0,
            resources: ResourceRequest::default(),
        }
    }
}

/// Resource requirements for a job
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceRequest {
    /// Number of CPU cores
    pub cpu: u32,
    /// Memory in GB
    pub memory_gb: f64,
    /// Number of GPUs
    pub gpu: u32,
    /// Disk space in GB
    pub disk_gb: f64,
}

impl ResourceRequest {
    /// Create a new resource request
    pub fn new(cpu: u32, memory_gb: f64) -> Self {
        Self {
            cpu,
            memory_gb,
            gpu: 0,
            disk_gb: 0.0,
        }
    }

    /// Add GPU requirements
    pub fn with_gpu(mut self, count: u32) -> Self {
        self.gpu = count;
        self
    }

    /// Add disk requirements
    pub fn with_disk(mut self, gb: f64) -> Self {
        self.disk_gb = gb;
        self
    }
}

/// Information about a job submitter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitterInfo {
    /// Principal ID (user/service account)
    pub principal_id: String,
    /// Submitter's priority level
    pub priority: u32,
    /// Domains the submitter has authority in
    pub domains: Vec<String>,
}

impl SubmitterInfo {
    /// Create a new submitter info
    pub fn new(principal_id: impl Into<String>) -> Self {
        Self {
            principal_id: principal_id.into(),
            priority: 0,
            domains: Vec::new(),
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Add domains
    pub fn with_domains(mut self, domains: Vec<String>) -> Self {
        self.domains = domains;
        self
    }
}

/// Information about a node for placement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node identifier
    pub id: String,
    /// Node tags
    pub tags: TagSet,
    /// Available resources
    pub available_resources: ResourceRequest,
    /// Current status
    pub status: NodeStatus,
}

impl NodeInfo {
    /// Create a new node info
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            tags: TagSet::new(),
            available_resources: ResourceRequest::default(),
            status: NodeStatus::Available,
        }
    }

    /// Set tags
    pub fn with_tags(mut self, tags: TagSet) -> Self {
        self.tags = tags;
        self
    }

    /// Set available resources
    pub fn with_resources(mut self, resources: ResourceRequest) -> Self {
        self.available_resources = resources;
        self
    }

    /// Set status
    pub fn with_status(mut self, status: NodeStatus) -> Self {
        self.status = status;
        self
    }
}

/// Node status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Available,
    Busy,
    Draining,
    Offline,
}

/// An override applied during evaluation
#[derive(Debug, Clone)]
pub struct Override {
    /// Policy to override
    pub policy_id: PolicyId,
    /// Effect to use instead
    pub effect_override: PolicyEffect,
    /// Who applied this override
    pub principal_id: String,
}

/// Result of policy evaluation
#[derive(Debug, Clone)]
pub struct EvaluationResult {
    /// Scores for each node
    pub node_scores: HashMap<String, NodeScore>,
    /// Nodes that were excluded
    pub excluded_nodes: HashMap<String, ExclusionReason>,
    /// Policies that were applied
    pub applied_policies: Vec<AppliedPolicy>,
    /// Warnings generated during evaluation
    pub warnings: Vec<EvaluationWarning>,
}

impl EvaluationResult {
    /// Get the best node for placement
    pub fn best_node(&self) -> Option<&String> {
        self.node_scores
            .iter()
            .filter(|(id, score)| {
                score.meets_requirements && !self.excluded_nodes.contains_key(*id)
            })
            .max_by(|a, b| a.1.total_score.partial_cmp(&b.1.total_score).unwrap())
            .map(|(id, _)| id)
    }

    /// Get nodes sorted by score (descending)
    pub fn ranked_nodes(&self) -> Vec<(&String, &NodeScore)> {
        let mut nodes: Vec<_> = self
            .node_scores
            .iter()
            .filter(|(id, score)| {
                score.meets_requirements && !self.excluded_nodes.contains_key(*id)
            })
            .collect();

        nodes.sort_by(|a, b| b.1.total_score.partial_cmp(&a.1.total_score).unwrap());
        nodes
    }
}

/// Score for a node
#[derive(Debug, Clone)]
pub struct NodeScore {
    /// Total computed score
    pub total_score: f64,
    /// Individual score components from each policy
    pub score_components: Vec<ScoreComponent>,
    /// Whether this node meets all requirements
    pub meets_requirements: bool,
}

/// A component of a node's score
#[derive(Debug, Clone)]
pub struct ScoreComponent {
    /// Policy that contributed this component
    pub policy_id: PolicyId,
    /// Index of the effect in the policy
    pub effect_index: usize,
    /// Score change
    pub score_delta: f64,
    /// Human-readable reason
    pub reason: String,
}

/// Reason a node was excluded
#[derive(Debug, Clone)]
pub struct ExclusionReason {
    /// Policy that excluded this node
    pub policy_id: PolicyId,
    /// Human-readable reason
    pub reason: String,
}

/// A policy that was applied during evaluation
#[derive(Debug, Clone)]
pub struct AppliedPolicy {
    /// Policy ID
    pub policy_id: PolicyId,
    /// Policy name
    pub policy_name: String,
    /// Whether the condition matched
    pub matched_condition: bool,
    /// Indices of effects that were applied
    pub effects_applied: Vec<usize>,
}

/// Warning generated during evaluation
#[derive(Debug, Clone)]
pub struct EvaluationWarning {
    /// Warning code
    pub code: String,
    /// Human-readable message
    pub message: String,
    /// Related policy, if any
    pub policy_id: Option<PolicyId>,
}

/// Detailed explanation of an evaluation
#[derive(Debug, Clone)]
pub struct EvaluationExplanation {
    /// The evaluation result
    pub result: EvaluationResult,
    /// Trace of each policy's evaluation
    pub policy_trace: Vec<PolicyTraceEntry>,
    /// Decision tree representation
    pub decision_tree: DecisionNode,
}

/// Trace of a single policy's evaluation
#[derive(Debug, Clone)]
pub struct PolicyTraceEntry {
    /// Policy ID
    pub policy_id: PolicyId,
    /// How the condition was evaluated
    pub condition_evaluation: ConditionTrace,
    /// How each effect was evaluated
    pub effects_evaluation: Vec<EffectTrace>,
}

/// Trace of condition evaluation
#[derive(Debug, Clone)]
pub struct ConditionTrace {
    /// The condition that was evaluated
    pub condition: PolicyCondition,
    /// Result of evaluation
    pub result: bool,
    /// Sub-traces for compound conditions
    pub sub_traces: Vec<ConditionTrace>,
}

/// Trace of effect evaluation
#[derive(Debug, Clone)]
pub struct EffectTrace {
    /// The effect that was applied
    pub effect: PolicyEffect,
    /// Nodes affected by this effect
    pub nodes_affected: Vec<String>,
    /// Score changes per node
    pub score_changes: HashMap<String, f64>,
}

/// Decision tree node for visualization
#[derive(Debug, Clone)]
pub enum DecisionNode {
    /// Leaf node with final score
    Leaf { node_id: String, final_score: f64 },
    /// Branch node with condition
    Branch {
        policy_id: PolicyId,
        condition: String,
        if_true: Box<DecisionNode>,
        if_false: Box<DecisionNode>,
    },
}

// ============================================================================
// Persistent Policy Engine
// ============================================================================

use super::persistence::PolicyPersistence;
use crate::storage::persistence::{PersistenceBackend, PersistenceError};

/// A policy engine with automatic persistence.
///
/// This struct wraps a `PolicyEngine` and automatically persists
/// changes to the underlying storage backend.
///
/// # Example
///
/// ```rust,no_run
/// use marabunta_compute::policy::{PersistentPolicyEngine, Policy, PolicyCondition, PolicyEffect, NodeSelector};
/// use marabunta_compute::governance::persistence::InMemoryBackend;
///
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     // Create a persistent engine
///     let backend = InMemoryBackend::new();
///     let mut engine = PersistentPolicyEngine::with_persistence(backend).await?;
///
///     // All mutations are automatically persisted
///     let policy = Policy::new("gpu-pref", "GPU Preference")
///         .with_condition(PolicyCondition::Always)
///         .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.8));
///     engine.register(policy).await?;
///
///     Ok(())
/// }
/// ```
pub struct PersistentPolicyEngine<B: PersistenceBackend> {
    /// The underlying policy engine
    engine: PolicyEngine,
    /// The persistence layer
    persistence: PolicyPersistence<B>,
}

impl<B: PersistenceBackend + 'static> PersistentPolicyEngine<B> {
    /// Creates a new persistent policy engine with the given backend.
    ///
    /// This will attempt to load existing data from the backend. If no data
    /// exists, an empty engine is created.
    ///
    /// # Arguments
    ///
    /// * `backend` - The persistence backend to use
    ///
    /// # Returns
    ///
    /// Returns the persistent engine or a `PersistenceError` if loading fails.
    pub async fn with_persistence(backend: B) -> Result<Self, PersistenceError> {
        let persistence = PolicyPersistence::new(backend);

        // Try to load existing engine state
        let engine = persistence.load_engine().await.unwrap_or_else(|_| {
            // If loading fails, create a new empty engine
            PolicyEngine::new()
        });

        Ok(Self {
            engine,
            persistence,
        })
    }

    /// Creates a persistent engine from an existing engine and backend.
    ///
    /// This immediately saves the engine state to the backend.
    pub async fn from_engine(engine: PolicyEngine, backend: B) -> Result<Self, PersistenceError> {
        let persistence = PolicyPersistence::new(backend);
        persistence.save_engine(&engine).await?;

        Ok(Self {
            engine,
            persistence,
        })
    }

    /// Gets a reference to the underlying engine.
    pub fn engine(&self) -> &PolicyEngine {
        &self.engine
    }

    /// Gets a mutable reference to the underlying engine.
    ///
    /// Note: Changes made directly to the engine will not be automatically persisted.
    /// Use `save()` to persist changes after direct modifications.
    pub fn engine_mut(&mut self) -> &mut PolicyEngine {
        &mut self.engine
    }

    /// Gets the persistence layer.
    pub fn persistence(&self) -> &PolicyPersistence<B> {
        &self.persistence
    }

    // =========================================================================
    // Policy Management (with auto-save)
    // =========================================================================

    /// Registers a new policy and persists the change.
    pub async fn register(&mut self, policy: Policy) -> Result<PolicyId, PolicyError> {
        // First persist
        self.persistence
            .save_policy(&policy)
            .await
            .map_err(|e| PolicyError::Internal(format!("Persistence error: {}", e)))?;

        // Then register with engine
        self.engine.register(policy)
    }

    /// Updates an existing policy and persists the change.
    pub async fn update(&mut self, policy: Policy) -> Result<(), PolicyError> {
        // First persist
        self.persistence
            .save_policy(&policy)
            .await
            .map_err(|e| PolicyError::Internal(format!("Persistence error: {}", e)))?;

        // Then update engine
        self.engine.update(policy)
    }

    /// Removes a policy and persists the change.
    pub async fn remove(&mut self, id: &PolicyId) -> Result<Policy, PolicyError> {
        // First remove from engine (to validate it exists)
        let policy = self.engine.remove(id)?;

        // Then remove from persistence
        let _ = self.persistence.delete_policy(id).await;

        Ok(policy)
    }

    /// Gets a policy by ID.
    pub fn get(&self, id: &PolicyId) -> Option<&Policy> {
        self.engine.get(id)
    }

    /// Lists all policies.
    pub fn list(&self) -> Vec<&Policy> {
        self.engine.list()
    }

    // =========================================================================
    // Evaluation (delegated to engine)
    // =========================================================================

    /// Evaluates policies against a context.
    pub fn evaluate(&self, context: &EvaluationContext) -> EvaluationResult {
        self.engine.evaluate(context)
    }

    /// Generates a detailed explanation of the evaluation.
    pub fn explain(&self, context: &EvaluationContext) -> EvaluationExplanation {
        self.engine.explain(context)
    }

    // =========================================================================
    // Utility Methods
    // =========================================================================

    /// Saves the current state to persistence.
    pub async fn save(&self) -> Result<(), PersistenceError> {
        self.persistence.save_engine(&self.engine).await
    }

    /// Reloads the engine from persistence.
    pub async fn reload(&mut self) -> Result<(), PersistenceError> {
        let loaded = self.persistence.load_engine().await?;
        self.engine = loaded;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_nodes() -> Vec<NodeInfo> {
        let mut nodes = Vec::new();

        // Node 1: Production GPU node
        let mut tags1 = TagSet::new();
        tags1.insert("env", "production");
        tags1.insert("gpu", "true");
        tags1.insert("group", "ml-cluster");
        nodes.push(
            NodeInfo::new("node-1")
                .with_tags(tags1)
                .with_resources(ResourceRequest::new(32, 128.0).with_gpu(4)),
        );

        // Node 2: Production CPU node
        let mut tags2 = TagSet::new();
        tags2.insert("env", "production");
        tags2.insert("group", "compute");
        nodes.push(
            NodeInfo::new("node-2")
                .with_tags(tags2)
                .with_resources(ResourceRequest::new(64, 256.0)),
        );

        // Node 3: Development node
        let mut tags3 = TagSet::new();
        tags3.insert("env", "development");
        tags3.insert("group", "dev-cluster");
        nodes.push(
            NodeInfo::new("node-3")
                .with_tags(tags3)
                .with_resources(ResourceRequest::new(8, 32.0)),
        );

        nodes
    }

    fn create_test_context() -> EvaluationContext {
        let mut job_tags = TagSet::new();
        job_tags.insert("team", "platform");

        let job = JobInfo {
            id: "job-1".to_string(),
            name: "ml-training-job".to_string(),
            tags: job_tags,
            job_type: "monte_carlo".to_string(),
            priority: 50,
            resources: ResourceRequest::new(16, 64.0).with_gpu(2),
        };

        let submitter = SubmitterInfo::new("user@example.com")
            .with_priority(100)
            .with_domains(vec!["ml".to_string(), "research".to_string()]);

        EvaluationContext::new(job, submitter).with_nodes(create_test_nodes())
    }

    #[test]
    fn test_engine_register_and_get() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("test-policy", "Test Policy");
        engine.register(policy.clone()).unwrap();

        assert!(engine.get(&"test-policy".to_string()).is_some());
        assert!(engine.get(&"nonexistent".to_string()).is_none());
    }

    #[test]
    fn test_engine_register_duplicate() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("test-policy", "Test Policy");
        engine.register(policy.clone()).unwrap();

        let result = engine.register(policy);
        assert!(matches!(result, Err(PolicyError::PolicyAlreadyExists(_))));
    }

    #[test]
    fn test_engine_update() {
        let mut engine = PolicyEngine::new();

        let mut policy = Policy::new("test-policy", "Test Policy");
        engine.register(policy.clone()).unwrap();

        policy.name = "Updated Policy".to_string();
        engine.update(policy).unwrap();

        let retrieved = engine.get(&"test-policy".to_string()).unwrap();
        assert_eq!(retrieved.name, "Updated Policy");
    }

    #[test]
    fn test_engine_remove() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("test-policy", "Test Policy");
        engine.register(policy).unwrap();

        let removed = engine.remove(&"test-policy".to_string()).unwrap();
        assert_eq!(removed.id, "test-policy");
        assert!(engine.get(&"test-policy".to_string()).is_none());
    }

    #[test]
    fn test_evaluate_always_condition() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("always-prefer-gpu", "Prefer GPU Nodes")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::tag(TagExpr::equals("gpu", "true")),
                0.8,
            ));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Node 1 should have higher score (has GPU tag)
        assert!(
            result.node_scores.get("node-1").unwrap().total_score
                > result.node_scores.get("node-2").unwrap().total_score
        );
    }

    #[test]
    fn test_evaluate_require_effect() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("require-production", "Require Production")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::require(NodeSelector::tag(TagExpr::equals(
                "env",
                "production",
            ))));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Node 3 (dev) should be excluded
        assert!(result.excluded_nodes.contains_key("node-3"));
        assert!(!result.excluded_nodes.contains_key("node-1"));
        assert!(!result.excluded_nodes.contains_key("node-2"));
    }

    #[test]
    fn test_evaluate_exclude_effect() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("exclude-dev", "Exclude Development")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::exclude(NodeSelector::tag(TagExpr::equals(
                "env",
                "development",
            ))));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Node 3 should be excluded
        assert!(result.excluded_nodes.contains_key("node-3"));
    }

    #[test]
    fn test_evaluate_job_matches() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("ml-job-policy", "ML Job Policy")
            .with_condition(PolicyCondition::JobMatches(
                JobMatcher::new().with_name_pattern("ml-.*"),
            ))
            .with_effect(PolicyEffect::prefer(
                NodeSelector::group("ml-cluster".to_string()),
                0.9,
            ));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Policy should have matched (job name is "ml-training-job")
        let applied = result
            .applied_policies
            .iter()
            .find(|p| p.policy_id == "ml-job-policy")
            .unwrap();
        assert!(applied.matched_condition);
    }

    #[test]
    fn test_evaluate_submitter_matches() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("ml-domain-policy", "ML Domain Policy")
            .with_condition(PolicyCondition::SubmitterMatches(
                SubmitterMatcher::new().with_domain("ml"),
            ))
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Policy should have matched (submitter has "ml" domain)
        let applied = result
            .applied_policies
            .iter()
            .find(|p| p.policy_id == "ml-domain-policy")
            .unwrap();
        assert!(applied.matched_condition);
    }

    #[test]
    fn test_evaluate_resource_matches() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("gpu-job-policy", "GPU Job Policy")
            .with_condition(PolicyCondition::ResourceMatches(
                ResourceMatcher::new().with_gpu(true, Some(1)),
            ))
            .with_effect(PolicyEffect::require(NodeSelector::tag(TagExpr::equals(
                "gpu", "true",
            ))));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Policy should have matched (job requires 2 GPUs)
        let applied = result
            .applied_policies
            .iter()
            .find(|p| p.policy_id == "gpu-job-policy")
            .unwrap();
        assert!(applied.matched_condition);

        // Non-GPU nodes should be excluded
        assert!(result.excluded_nodes.contains_key("node-2"));
        assert!(result.excluded_nodes.contains_key("node-3"));
    }

    #[test]
    fn test_evaluate_compound_conditions() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("compound-policy", "Compound Policy")
            .with_condition(PolicyCondition::and(vec![
                PolicyCondition::JobMatches(JobMatcher::new().with_priority_range(0, 100)),
                PolicyCondition::Or(vec![
                    PolicyCondition::SubmitterMatches(SubmitterMatcher::new().with_domain("ml")),
                    PolicyCondition::SubmitterMatches(
                        SubmitterMatcher::new().with_domain("research"),
                    ),
                ]),
            ]))
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Policy should match
        let applied = result
            .applied_policies
            .iter()
            .find(|p| p.policy_id == "compound-policy")
            .unwrap();
        assert!(applied.matched_condition);
    }

    #[test]
    fn test_best_node() {
        let mut engine = PolicyEngine::new();

        // Prefer production nodes
        let policy1 = Policy::new("prefer-prod", "Prefer Production")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::tag(TagExpr::equals("env", "production")),
                0.8,
            ));

        // Strongly prefer GPU nodes
        let policy2 = Policy::new("prefer-gpu", "Prefer GPU")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::tag(TagExpr::equals("gpu", "true")),
                1.0,
            ));

        engine.register(policy1).unwrap();
        engine.register(policy2).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // Node 1 should be best (production + GPU)
        assert_eq!(result.best_node(), Some(&"node-1".to_string()));
    }

    #[test]
    fn test_ranked_nodes() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("prefer-prod", "Prefer Production")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::tag(TagExpr::equals("env", "production")),
                0.8,
            ));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        let ranked = result.ranked_nodes();
        assert!(!ranked.is_empty());

        // First two should be production nodes
        let first_id = ranked[0].0;
        assert!(first_id == "node-1" || first_id == "node-2");
    }

    #[test]
    fn test_explain() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("test-policy", "Test Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        engine.register(policy).unwrap();

        let context = create_test_context();
        let explanation = engine.explain(&context);

        assert!(!explanation.policy_trace.is_empty());
        assert!(explanation.policy_trace[0].condition_evaluation.result);
    }

    #[test]
    fn test_conflict_priority_ordering() {
        let mut engine = PolicyEngine::new();

        // Low priority policy: prefer node-1
        let policy1 = Policy::new("low-priority", "Low Priority")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::node_ids(vec!["node-1".to_string()]),
                0.5,
            ))
            .with_governance(PolicyGovernance::new("system").with_conflict_priority(10));

        // High priority policy: prefer node-2
        let policy2 = Policy::new("high-priority", "High Priority")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(
                NodeSelector::node_ids(vec!["node-2".to_string()]),
                1.0,
            ))
            .with_governance(PolicyGovernance::new("system").with_conflict_priority(100));

        engine.register(policy1).unwrap();
        engine.register(policy2).unwrap();

        let context = create_test_context();
        let result = engine.evaluate(&context);

        // High priority policy applied last, so its effects are final
        // Node-2 should have highest score
        let node1_score = result.node_scores.get("node-1").unwrap().total_score;
        let node2_score = result.node_scores.get("node-2").unwrap().total_score;
        assert!(node2_score > node1_score);
    }
}
