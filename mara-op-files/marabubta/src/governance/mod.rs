// Marabunta - Licensed under the MIT License.
//! Governance System for Marabunta Compute Job Placement
//!
//! This module provides a comprehensive governance system for managing principals,
//! authority delegation, and override policies in the distributed computing framework.
//!
//! # Overview
//!
//! The governance system is built around several key concepts:
//!
//! - **Principals**: Entities (users, groups, services, system) that can have authority
//! - **Domains**: Namespaced resources that authority applies to (e.g., "dept:physics")
//! - **Delegations**: Transfer of authority from one principal to another
//! - **Override Policies**: Rules governing when and how actions can be overridden
//!
//! # Principal Hierarchy
//!
//! Each principal has a priority level that determines their authority:
//!
//! - System: `u32::MAX` - highest authority
//! - CEO/Admin: 1000 - executive authority
//! - VP/Director: 800 - departmental authority
//! - Manager: 500 - team authority
//! - Employee: 100 - individual authority
//! - Postdoc/Intern: 50 - limited authority
//!
//! # Domain Patterns
//!
//! Domains use a hierarchical naming convention:
//!
//! - `*` - Global (matches everything)
//! - `dept:physics` - Exact match
//! - `project:collider-*` - Prefix match
//! - `/^project:.*-test$/` - Regex pattern
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use marabunta_compute::governance::{
//!     GovernanceRegistry, Principal, DomainPattern, Delegation,
//!     OverridePolicy, OverrideContext,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let registry = GovernanceRegistry::new();
//!
//!     // Register principals
//!     let ceo = Principal::new_user("ceo", "Chief Executive", 1000)
//!         .with_domain(DomainPattern::global());
//!     registry.register_principal(ceo).await.unwrap();
//!
//!     let manager = Principal::new_user("manager", "Engineering Manager", 500)
//!         .with_domain(DomainPattern::prefix("dept:eng"));
//!     registry.register_principal(manager).await.unwrap();
//!
//!     let employee = Principal::new_user("employee", "Software Engineer", 100);
//!     registry.register_principal(employee).await.unwrap();
//!
//!     // Delegate authority
//!     let delegation = Delegation::new(
//!         "del-1",
//!         "manager",
//!         "employee",
//!         vec![DomainPattern::exact("dept:eng:project-alpha")],
//!     );
//!     registry.create_delegation(delegation).await.unwrap();
//!
//!     // Check authority
//!     assert!(registry.has_authority(&"employee".to_string(), "dept:eng:project-alpha").await);
//!
//!     // Initialize override checker
//!     registry.init_override_checker().await;
//!
//!     // Check if override is allowed
//!     let policy = OverridePolicy::blueprint(200);
//!     let context = OverrideContext::new("employee", 100);
//!     let result = registry.can_override(
//!         &"manager".to_string(),
//!         &policy,
//!         "dept:eng",
//!         &context,
//!     ).await;
//!
//!     assert!(result.is_allowed());
//! }
//! ```
//!
//! # Thread Safety
//!
//! All components in this module are thread-safe and use `Arc<RwLock<>>` internally
//! for safe concurrent access. The registry can be safely shared across tasks and threads.
//!
//! # Modules
//!
//! - [`principal`]: Principal types and domain patterns
//! - [`authority`]: Authority checking and delegation management
//! - [`override_policy`]: Override policies and checking
//! - [`registry`]: Unified governance registry
//! - [`errors`]: Error types

pub mod authority;
pub mod errors;
pub mod override_policy;
pub mod persistence;
pub mod principal;
pub mod registry;

// Re-export commonly used types at module level
pub use authority::{
    AuthorityChain, AuthorityChecker, AuthorityStep, Delegation, DelegationConstraints,
};
pub use errors::{AuthorityError, GovernanceError, GovernanceResult};
pub use override_policy::{
    ApprovalDefault, ApproverSpec, OverrideChecker, OverrideContext, OverridePolicy, OverrideResult,
};
pub use persistence::{GovernancePersistence, GovernanceStorageStats, InMemoryBackend};
pub use principal::{DomainPattern, Principal, PrincipalId, PrincipalMetadata, PrincipalType};
pub use registry::{
    GovernanceRegistry, PersistentGovernanceRegistry, RegistryExport, ValidationIssue,
};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::time::Duration;

    /// Full integration test of the governance system
    #[tokio::test]
    async fn test_full_governance_workflow() {
        // Create registry
        let registry = GovernanceRegistry::new();

        // Set up organizational hierarchy
        let system = Principal::system();
        registry.register_principal(system).await.unwrap();

        let ceo = Principal::new_user("ceo", "Chief Executive Officer", 1000)
            .with_domain(DomainPattern::global())
            .with_email("ceo@example.com");
        registry.register_principal(ceo).await.unwrap();

        let vp_eng = Principal::new_user("vp-eng", "VP of Engineering", 800)
            .with_domain(DomainPattern::prefix("dept:eng"));
        registry.register_principal(vp_eng).await.unwrap();

        let vp_research = Principal::new_user("vp-research", "VP of Research", 800)
            .with_domain(DomainPattern::prefix("dept:research"));
        registry.register_principal(vp_research).await.unwrap();

        let physics_lead = Principal::new_user("physics-lead", "Physics Team Lead", 500)
            .with_domain(DomainPattern::prefix("dept:research:physics"));
        registry.register_principal(physics_lead).await.unwrap();

        let researcher = Principal::new_user("researcher", "Physics Researcher", 100);
        registry.register_principal(researcher).await.unwrap();

        let postdoc = Principal::new_user("postdoc", "Postdoctoral Researcher", 50);
        registry.register_principal(postdoc).await.unwrap();

        // Create delegations
        // Physics lead delegates lab access to researcher
        let lab_delegation = Delegation::new(
            "del-lab",
            "physics-lead",
            "researcher",
            vec![DomainPattern::exact("dept:research:physics:lab-1")],
        )
        .with_constraints(DelegationConstraints::default().with_max_priority(80));
        registry.create_delegation(lab_delegation).await.unwrap();

        // Researcher delegates to postdoc with no further redelegation
        let postdoc_delegation = Delegation::new(
            "del-postdoc",
            "researcher",
            "postdoc",
            vec![DomainPattern::exact("dept:research:physics:lab-1")],
        )
        .with_constraints(DelegationConstraints::no_redelegate());
        registry
            .create_delegation(postdoc_delegation)
            .await
            .unwrap();

        // Initialize override checker
        registry.init_override_checker().await;

        // Test authority checks
        // CEO has global authority
        assert!(registry.has_authority(&"ceo".to_string(), "anything").await);

        // VP Research has authority over research departments
        assert!(
            registry
                .has_authority(&"vp-research".to_string(), "dept:research:physics")
                .await
        );
        assert!(
            !registry
                .has_authority(&"vp-research".to_string(), "dept:eng:team-a")
                .await
        );

        // VP Engineering has authority over engineering
        assert!(
            registry
                .has_authority(&"vp-eng".to_string(), "dept:eng:team-a")
                .await
        );
        assert!(
            !registry
                .has_authority(&"vp-eng".to_string(), "dept:research:physics")
                .await
        );

        // Researcher has delegated authority over lab-1
        assert!(
            registry
                .has_authority(&"researcher".to_string(), "dept:research:physics:lab-1")
                .await
        );
        assert!(
            !registry
                .has_authority(&"researcher".to_string(), "dept:research:physics:lab-2")
                .await
        );

        // Postdoc has delegated authority (limited by max_priority)
        assert!(
            registry
                .has_authority(&"postdoc".to_string(), "dept:research:physics:lab-1")
                .await
        );

        // Test effective priorities
        assert_eq!(
            registry
                .effective_priority(&"ceo".to_string(), "anywhere")
                .await,
            Some(1000)
        );
        assert_eq!(
            registry
                .effective_priority(&"researcher".to_string(), "dept:research:physics:lab-1")
                .await,
            Some(80) // Limited by delegation constraint
        );
        assert_eq!(
            registry
                .effective_priority(&"postdoc".to_string(), "dept:research:physics:lab-1")
                .await,
            Some(80) // Limited by upstream constraint
        );

        // Test override policies
        let mandatory_policy = OverridePolicy::mandatory();
        let blueprint_policy = OverridePolicy::blueprint(200);
        let advisory_policy = OverridePolicy::advisory();
        let trial_policy = OverridePolicy::trial(3, OverridePolicy::mandatory());

        let context = OverrideContext::new("researcher", 100);

        // Nobody can override mandatory
        let result = registry
            .can_override(
                &"ceo".to_string(),
                &mandatory_policy,
                "dept:research:physics:lab-1",
                &context,
            )
            .await;
        assert!(!result.is_allowed());

        // VP can override blueprint (priority >= 200)
        let result = registry
            .can_override(
                &"vp-research".to_string(),
                &blueprint_policy,
                "dept:research:physics:lab-1",
                &context,
            )
            .await;
        assert!(result.is_allowed());

        // Anyone can override advisory
        let result = registry
            .can_override(
                &"postdoc".to_string(),
                &advisory_policy,
                "dept:research:physics:lab-1",
                &context,
            )
            .await;
        assert!(result.is_allowed());

        // Trial policy allows limited overrides
        let result = registry
            .can_override(
                &"physics-lead".to_string(),
                &trial_policy,
                "dept:research:physics:lab-1",
                &context,
            )
            .await;
        assert!(matches!(
            result,
            OverrideResult::TrialAllowed { remaining_uses: 2 }
        ));

        // Test authority chain tracing
        let chain = registry
            .authority_chain(&"postdoc".to_string(), "dept:research:physics:lab-1")
            .await;
        assert!(chain.has_authority);
        assert!(chain.granted_via.len() >= 2); // Multiple delegation steps

        // Test can override principal
        assert!(
            registry
                .can_override_principal(
                    &"vp-research".to_string(),
                    &"physics-lead".to_string(),
                    "dept:research:physics"
                )
                .await
        );
        assert!(
            !registry
                .can_override_principal(
                    &"postdoc".to_string(),
                    &"researcher".to_string(),
                    "dept:research:physics:lab-1"
                )
                .await
        );

        // Test authorities for domain
        let authorities = registry
            .authorities_for_domain("dept:research:physics:lab-1")
            .await;
        let authority_ids: Vec<_> = authorities.iter().map(|p| p.id.clone()).collect();
        assert!(authority_ids.contains(&"ceo".to_string()));
        assert!(authority_ids.contains(&"vp-research".to_string()));
        assert!(authority_ids.contains(&"physics-lead".to_string()));
        assert!(authority_ids.contains(&"researcher".to_string()));
        assert!(authority_ids.contains(&"postdoc".to_string()));

        // Test delegation revocation
        registry.revoke_delegation("del-postdoc").await.unwrap();
        assert!(
            !registry
                .has_authority(&"postdoc".to_string(), "dept:research:physics:lab-1")
                .await
        );

        // Test validation and cleanup
        let issues = registry.validate().await;
        // Should have no issues in a well-formed registry
        for issue in &issues {
            println!("Validation issue: {}", issue);
        }

        // Test export/import
        let export = registry.export().await;
        assert_eq!(export.principals.len(), 7); // Including system
        assert_eq!(export.delegations.len(), 2);

        let new_registry = GovernanceRegistry::new();
        new_registry.import(export).await.unwrap();
        assert_eq!(new_registry.principal_count().await, 7);
    }

    /// Test complex delegation scenarios
    #[tokio::test]
    async fn test_complex_delegation_chains() {
        let registry = GovernanceRegistry::new();

        // Create hierarchy
        let admin =
            Principal::new_user("admin", "Admin", 1000).with_domain(DomainPattern::global());
        registry.register_principal(admin).await.unwrap();

        let manager_a = Principal::new_user("manager-a", "Manager A", 500)
            .with_domain(DomainPattern::prefix("project:a"));
        registry.register_principal(manager_a).await.unwrap();

        let manager_b = Principal::new_user("manager-b", "Manager B", 500)
            .with_domain(DomainPattern::prefix("project:b"));
        registry.register_principal(manager_b).await.unwrap();

        let dev1 = Principal::new_user("dev1", "Developer 1", 100);
        registry.register_principal(dev1).await.unwrap();

        let dev2 = Principal::new_user("dev2", "Developer 2", 100);
        registry.register_principal(dev2).await.unwrap();

        // Manager A delegates project:a to dev1
        registry
            .create_delegation(Delegation::new(
                "del-a1",
                "manager-a",
                "dev1",
                vec![DomainPattern::prefix("project:a")],
            ))
            .await
            .unwrap();

        // Manager B delegates project:b to dev1 as well
        registry
            .create_delegation(Delegation::new(
                "del-b1",
                "manager-b",
                "dev1",
                vec![DomainPattern::prefix("project:b")],
            ))
            .await
            .unwrap();

        // Dev1 now has authority over both projects
        assert!(
            registry
                .has_authority(&"dev1".to_string(), "project:a:feature")
                .await
        );
        assert!(
            registry
                .has_authority(&"dev1".to_string(), "project:b:feature")
                .await
        );

        // Dev1 delegates project:a to dev2
        registry
            .create_delegation(Delegation::new(
                "del-a2",
                "dev1",
                "dev2",
                vec![DomainPattern::prefix("project:a")],
            ))
            .await
            .unwrap();

        // Dev2 has authority over project:a but not project:b
        assert!(
            registry
                .has_authority(&"dev2".to_string(), "project:a:feature")
                .await
        );
        assert!(
            !registry
                .has_authority(&"dev2".to_string(), "project:b:feature")
                .await
        );

        // Test union domain patterns
        let multi_domain = Principal::new_user("multi", "Multi-domain", 400).with_domain(
            DomainPattern::union(vec![
                DomainPattern::prefix("dept:eng"),
                DomainPattern::prefix("dept:sales"),
            ]),
        );
        registry.register_principal(multi_domain).await.unwrap();

        assert!(
            registry
                .has_authority(&"multi".to_string(), "dept:eng:team-1")
                .await
        );
        assert!(
            registry
                .has_authority(&"multi".to_string(), "dept:sales:region-1")
                .await
        );
        assert!(
            !registry
                .has_authority(&"multi".to_string(), "dept:hr")
                .await
        );
    }

    /// Test time-limited and trial policies
    #[tokio::test]
    async fn test_temporal_policies() {
        let registry = GovernanceRegistry::new();

        let owner = Principal::new_user("owner", "Resource Owner", 500)
            .with_domain(DomainPattern::exact("resource:critical"));
        registry.register_principal(owner).await.unwrap();

        let user = Principal::new_user("user", "Regular User", 200)
            .with_domain(DomainPattern::exact("resource:critical"));
        registry.register_principal(user).await.unwrap();

        registry.init_override_checker().await;

        // Time-limited policy
        let time_policy =
            OverridePolicy::time_limited(Duration::from_secs(3600), OverridePolicy::mandatory());

        // User requests a 30-minute override
        let context = OverrideContext::new("owner", 500).with_duration(Duration::from_secs(1800));

        let result = registry
            .can_override(
                &"user".to_string(),
                &time_policy,
                "resource:critical",
                &context,
            )
            .await;

        assert!(matches!(
            result,
            OverrideResult::TimeLimitedAllowed { max_duration }
            if max_duration == Duration::from_secs(3600)
        ));

        // User requests a 2-hour override (exceeds limit)
        let context = OverrideContext::new("owner", 500).with_duration(Duration::from_secs(7200));

        let result = registry
            .can_override(
                &"user".to_string(),
                &time_policy,
                "resource:critical",
                &context,
            )
            .await;

        assert!(matches!(result, OverrideResult::Denied { .. }));
    }

    /// Test approver specifications
    #[tokio::test]
    async fn test_approver_specs() {
        let registry = GovernanceRegistry::new();

        let admin1 =
            Principal::new_user("admin1", "Admin 1", 1000).with_domain(DomainPattern::global());
        let admin2 =
            Principal::new_user("admin2", "Admin 2", 1000).with_domain(DomainPattern::global());
        let manager = Principal::new_user("manager", "Manager", 500)
            .with_domain(DomainPattern::prefix("dept:"));
        let user = Principal::new_user("user", "User", 100);

        registry.register_principal(admin1).await.unwrap();
        registry.register_principal(admin2).await.unwrap();
        registry.register_principal(manager).await.unwrap();
        registry.register_principal(user).await.unwrap();

        registry.init_override_checker().await;

        // Create policy requiring approval from specific admins
        let policy = OverridePolicy::RequiresApproval {
            approvers: ApproverSpec::any_of(vec![
                ApproverSpec::specific(vec!["admin1".to_string()]),
                ApproverSpec::specific(vec!["admin2".to_string()]),
            ]),
            timeout: Duration::from_secs(3600),
            default_on_timeout: ApprovalDefault::Reject,
        };

        let context = OverrideContext::new("manager", 500);

        let result = registry
            .can_override(&"user".to_string(), &policy, "dept:eng", &context)
            .await;

        match result {
            OverrideResult::RequiresApproval { approvers } => {
                assert!(!approvers.is_empty());
                // Should include admin1 or admin2
                assert!(
                    approvers.contains(&"admin1".to_string())
                        || approvers.contains(&"admin2".to_string())
                );
            }
            _ => panic!("Expected RequiresApproval result"),
        }
    }
}
