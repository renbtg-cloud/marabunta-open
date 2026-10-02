// Marabunta - Licensed under the MIT License.
//! Policy IR and Evaluation Engine for Job Placement
//!
//! This module provides a complete Policy Intermediate Representation (IR) and
//! evaluation engine for the marabunta-compute job placement system. All policy formats
//! (DSL, visual, Python, etc.) compile to this IR for uniform evaluation.
//!
//! # Architecture
//!
//! ```text
//! +------------------+     +------------------+     +------------------+
//! | Policy DSL       | --> |                  |     |                  |
//! +------------------+     |                  |     |                  |
//!                          |   Policy IR      | --> |  Policy Engine   | --> Placement
//! +------------------+     |   (ir.rs)        |     |  (engine.rs)     |     Decision
//! | Visual Editor    | --> |                  |     |                  |
//! +------------------+     |                  |     |                  |
//!                          +------------------+     +------------------+
//! +------------------+            ^                        ^
//! | Python API       | -----------+                        |
//! +------------------+                                     |
//!                                                          |
//! +------------------+     +------------------+            |
//! | Validator        | --> | Conflict         | -----------+
//! | (validation.rs)  |     | Detector         |
//! +------------------+     | (conflict.rs)    |
//!                          +------------------+
//! ```
//!
//! # Key Components
//!
//! - **Policy IR** (`ir.rs`): The core representation that all policy formats compile to.
//!   Includes conditions (JobMatches, SubmitterMatches, etc.) and effects (Prefer, Require,
//!   Exclude, Affinity, etc.).
//!
//! - **Policy Engine** (`engine.rs`): Evaluates policies against job placement requests.
//!   Produces scored node recommendations and explanations.
//!
//! - **Policy Validation** (`validation.rs`): Validates policies for correctness before
//!   registration. Checks for invalid regex, conflicting effects, and semantic issues.
//!
//! - **Conflict Detection** (`conflict.rs`): Detects potential conflicts between policies,
//!   such as contradictory effects or priority ambiguity.
//!
//! # Example Usage
//!
//! ```rust
//! use marabunta_compute::policy::*;
//!
//! // Create a policy engine
//! let mut engine = PolicyEngine::new();
//!
//! // Define a policy that prefers GPU nodes for ML jobs
//! let ml_policy = Policy::new("ml-gpu-preference", "ML GPU Preference")
//!     .with_condition(PolicyCondition::JobMatches(
//!         JobMatcher::new()
//!             .with_name_pattern("ml-.*")
//!             .with_job_types(vec!["monte_carlo".to_string()])
//!     ))
//!     .with_effect(PolicyEffect::prefer(
//!         NodeSelector::tag(TagExpr::equals("gpu", "true")),
//!         0.9,
//!     ))
//!     .with_governance(
//!         PolicyGovernance::new("ml-team")
//!             .with_domain("ml")
//!             .with_conflict_priority(50)
//!     );
//!
//! // Validate before registering
//! let validator = PolicyValidator::new();
//! let validation = validator.validate(&ml_policy);
//! if validation.valid {
//!     engine.register(ml_policy).unwrap();
//! }
//!
//! // Create evaluation context
//! let job = JobInfo::new("job-1", "ml-training-run")
//!     .with_resources(ResourceRequest::new(16, 64.0).with_gpu(2));
//! let submitter = SubmitterInfo::new("alice@ml-team.org")
//!     .with_domains(vec!["ml".to_string()]);
//!
//! let context = EvaluationContext::new(job, submitter)
//!     .with_nodes(vec![
//!         NodeInfo::new("gpu-node-1").with_tags(/* ... */),
//!         NodeInfo::new("cpu-node-1").with_tags(/* ... */),
//!     ]);
//!
//! // Evaluate policies
//! let result = engine.evaluate(&context);
//!
//! // Get the best node
//! if let Some(best) = result.best_node() {
//!     println!("Place job on: {}", best);
//! }
//! ```
//!
//! # Policy Conditions
//!
//! Policies can have various conditions that determine when they apply:
//!
//! - `Always` - Policy always applies
//! - `Never` - Policy never applies (effectively disabled)
//! - `JobMatches` - Job attributes match criteria (name pattern, tags, type, priority)
//! - `SubmitterMatches` - Submitter attributes match criteria (principal, domain, priority)
//! - `TimeWindow` - Current time matches cron expression
//! - `ResourceMatches` - Resource request matches criteria (CPU, memory, GPU)
//! - `And`, `Or`, `Not` - Logical combinations
//!
//! # Policy Effects
//!
//! Effects determine what happens when a policy applies:
//!
//! - `Prefer` - Prefer nodes matching selector (soft constraint with weight)
//! - `Require` - Require nodes matching selector (hard constraint)
//! - `Exclude` - Exclude nodes matching selector (hard constraint)
//! - `Affinity` - Co-locate with other jobs/tasks
//! - `AntiAffinity` - Spread away from other jobs/tasks
//! - `SetResourceLimit` - Set a resource limit
//! - `SetPriority` - Adjust job priority
//! - `ChargeQuota` - Charge against a quota
//! - `AllowPreemption` / `DisallowPreemption` - Control preemption
//! - `CustomScore` - Custom scoring function
//!
//! # Governance
//!
//! Each policy has governance metadata:
//!
//! - `author` - Who created/owns the policy
//! - `authority_domain` - Domain this policy has authority over
//! - `override_policy` - How this policy can be overridden (Mandatory, Blueprint, Advisory)
//! - `conflict_priority` - Priority for conflict resolution (higher wins)

pub mod conflict;
pub mod dsl;
pub mod engine;
pub mod error;
pub mod governance_integration;
pub mod ir;
pub mod persistence;
#[cfg(feature = "placement-integration")]
pub mod placement_integration;
pub mod templates;
pub mod validation;

// Re-export commonly used types
pub use conflict::{ConflictDetector, ConflictSeverity, ConflictType, PolicyConflict};
pub use engine::{
    AppliedPolicy, ConditionTrace, DecisionNode, EffectTrace, EvaluationContext,
    EvaluationExplanation, EvaluationResult, EvaluationWarning, ExclusionReason, JobInfo, NodeInfo,
    NodeScore, NodeStatus, Override, PersistentPolicyEngine, PolicyEngine, PolicyTraceEntry,
    ResourceRequest, ScoreComponent, SubmitterInfo,
};
pub use error::{PolicyError, PolicyResult};
pub use governance_integration::GovernanceAwarePolicyEngine;
pub use ir::{
    AffinityScope, AffinityTarget, JobMatcher, NodeSelector, OverridePolicyRef, Policy,
    PolicyCondition, PolicyEffect, PolicyGovernance, PolicyId, PriorityMode, ResourceMatcher,
    ResourceType, SubmitterMatcher, TagExpr, TagSet,
};
pub use persistence::{EngineExport, PolicyPersistence, PolicyStorageStats};
pub use templates::{
    ParameterDef, ParameterType, PolicyTemplate, TemplateCategory, TemplateError, TemplateParams,
    TemplateRegistry,
};
pub use validation::{PolicyValidator, ValidationError, ValidationResult, ValidationWarning};

#[cfg(feature = "placement-integration")]
pub use placement_integration::{
    get_available_policy_nodes, placement_node_to_policy_node, PlacementConfig,
    PolicyTagExprAdapter,
};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// Integration test showing the full workflow
    #[test]
    fn test_full_workflow() {
        // 1. Create policies
        let gpu_policy = Policy::new("gpu-preference", "GPU Preference")
            .with_condition(PolicyCondition::ResourceMatches(
                ResourceMatcher::new().with_gpu(true, Some(1)),
            ))
            .with_effect(PolicyEffect::require(NodeSelector::tag(TagExpr::equals(
                "gpu", "true",
            ))))
            .with_governance(
                PolicyGovernance::new("platform-team")
                    .with_domain("compute")
                    .with_conflict_priority(100),
            );

        let prod_policy = Policy::new("production-only", "Production Only")
            .with_condition(PolicyCondition::SubmitterMatches(
                SubmitterMatcher::new().with_domain("production"),
            ))
            .with_effect(PolicyEffect::require(NodeSelector::tag(TagExpr::equals(
                "env",
                "production",
            ))))
            .with_governance(
                PolicyGovernance::new("ops-team")
                    .with_domain("operations")
                    .with_conflict_priority(200)
                    .with_override_policy(OverridePolicyRef::Mandatory),
            );

        // 2. Validate policies
        let validator = PolicyValidator::new();

        let gpu_validation = validator.validate(&gpu_policy);
        assert!(gpu_validation.valid, "GPU policy should be valid");

        let prod_validation = validator.validate(&prod_policy);
        assert!(prod_validation.valid, "Production policy should be valid");

        // 3. Check for conflicts
        let detector = ConflictDetector::new(std::collections::HashMap::new());
        let _conflicts = detector.detect_conflicts(&[&gpu_policy, &prod_policy]);
        // These policies shouldn't conflict (different conditions)

        // 4. Register with engine
        let mut engine = PolicyEngine::new();
        engine.register(gpu_policy).unwrap();
        engine.register(prod_policy).unwrap();

        // 5. Create nodes
        let mut gpu_prod_tags = TagSet::new();
        gpu_prod_tags.insert("gpu", "true");
        gpu_prod_tags.insert("env", "production");

        let mut cpu_prod_tags = TagSet::new();
        cpu_prod_tags.insert("env", "production");

        let mut gpu_dev_tags = TagSet::new();
        gpu_dev_tags.insert("gpu", "true");
        gpu_dev_tags.insert("env", "development");

        let nodes = vec![
            NodeInfo::new("gpu-prod-1")
                .with_tags(gpu_prod_tags)
                .with_resources(ResourceRequest::new(32, 128.0).with_gpu(4)),
            NodeInfo::new("cpu-prod-1")
                .with_tags(cpu_prod_tags)
                .with_resources(ResourceRequest::new(64, 256.0)),
            NodeInfo::new("gpu-dev-1")
                .with_tags(gpu_dev_tags)
                .with_resources(ResourceRequest::new(16, 64.0).with_gpu(2)),
        ];

        // 6. Create job context
        let mut job = JobInfo::new("job-1", "gpu-training");
        job.resources = ResourceRequest::new(16, 64.0).with_gpu(2);

        let submitter = SubmitterInfo::new("alice@company.com")
            .with_priority(100)
            .with_domains(vec!["production".to_string()]);

        let context = EvaluationContext::new(job, submitter).with_nodes(nodes);

        // 7. Evaluate
        let result = engine.evaluate(&context);

        // 8. Check results
        // GPU job from production submitter should only be placed on gpu-prod-1
        assert!(
            result.excluded_nodes.contains_key("cpu-prod-1"),
            "CPU node should be excluded (no GPU)"
        );
        assert!(
            result.excluded_nodes.contains_key("gpu-dev-1"),
            "Dev GPU node should be excluded (not production)"
        );
        assert!(
            !result.excluded_nodes.contains_key("gpu-prod-1"),
            "Production GPU node should not be excluded"
        );

        // Best node should be gpu-prod-1
        assert_eq!(result.best_node(), Some(&"gpu-prod-1".to_string()));
    }

    /// Test the explanation feature
    #[test]
    fn test_explanation() {
        let mut engine = PolicyEngine::new();

        let policy = Policy::new("test-policy", "Test Policy")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5));

        engine.register(policy).unwrap();

        let job = JobInfo::new("job-1", "test-job");
        let submitter = SubmitterInfo::new("user@test.com");
        let mut tags = TagSet::new();
        tags.insert("env", "test");
        let nodes = vec![NodeInfo::new("node-1").with_tags(tags)];

        let context = EvaluationContext::new(job, submitter).with_nodes(nodes);

        let explanation = engine.explain(&context);

        // Should have a trace for our policy
        assert!(explanation
            .policy_trace
            .iter()
            .any(|t| t.policy_id == "test-policy"));

        // Condition should have evaluated to true
        let trace = explanation
            .policy_trace
            .iter()
            .find(|t| t.policy_id == "test-policy")
            .unwrap();
        assert!(trace.condition_evaluation.result);
    }

    /// Test conflict detection integration
    #[test]
    fn test_conflict_detection_integration() {
        let policy_a = Policy::new("a", "Policy A")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::require(NodeSelector::group(
                "group-x".to_string(),
            )));

        let policy_b = Policy::new("b", "Policy B")
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::exclude(NodeSelector::group(
                "group-x".to_string(),
            )));

        let mut engine = PolicyEngine::new();
        engine.register(policy_a.clone()).unwrap();

        // Check if new policy conflicts with existing
        let detector = ConflictDetector::from_engine(&engine);
        let conflicts = detector.check_new_policy(&policy_b);

        assert!(!conflicts.is_empty());
        assert!(conflicts
            .iter()
            .any(|c| c.conflict_type == ConflictType::ContradictoryEffects));
    }
}
