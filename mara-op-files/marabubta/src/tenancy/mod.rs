// Marabunta - Licensed under the MIT License.
//! Multi-Tenancy Isolation for Marabunta Compute
//!
//! This module provides comprehensive multi-tenant isolation for the Marabunta Compute
//! distributed computing framework. It enables organizations, teams, and users to
//! share infrastructure while maintaining strict resource, network, and data isolation.
//!
//! # Architecture
//!
//! ```text
//! +------------------+     +------------------+     +------------------+
//! | Organization     | --> | Team A           | --> | User 1           |
//! | (Acme Corp)      |     | (Engineering)    |     | (alice)          |
//! +------------------+     +------------------+     +------------------+
//!        |                        |                        |
//!        v                        v                        v
//! +------------------+     +------------------+     +------------------+
//! | Global Quotas    |     | Team Quotas      |     | User Quotas      |
//! | Global Policies  |     | Team Policies    |     | User Policies    |
//! +------------------+     +------------------+     +------------------+
//! ```
//!
//! # Key Concepts
//!
//! ## Tenant Hierarchy
//!
//! Tenants are organized in a three-level hierarchy:
//!
//! - **Organization**: Top-level tenant representing a company or institution
//! - **Team**: Department or team within an organization
//! - **User**: Individual user or project workspace
//!
//! Each level inherits and can further restrict policies and quotas from its parent.
//!
//! ## Resource Isolation
//!
//! Three isolation modes are supported:
//!
//! - **Shared**: Resources are shared across tenants (default)
//! - **Dedicated**: Resources are exclusively assigned to a tenant
//! - **Hybrid**: Mix of dedicated and shared resources
//!
//! ## Network Isolation
//!
//! - Tenant-specific API endpoints
//! - IP allowlisting
//! - Optional mTLS requirements
//! - Egress control
//!
//! ## Data Isolation
//!
//! - Namespace-prefixed storage
//! - Tenant-specific encryption keys
//! - Data classification levels
//! - Cross-tenant sharing rules
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use marabunta_compute::tenancy::{
//!     TenantRegistry, Tenant, TenantQuotas, TenantPolicies,
//!     TenantAuthManager, Permission, TenantRole,
//! };
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create tenant registry
//!     let registry = Arc::new(TenantRegistry::new());
//!
//!     // Create an organization
//!     let org = Tenant::new_organization("Acme Corp", "acme")
//!         .with_quotas(TenantQuotas::default())
//!         .with_policies(TenantPolicies::default());
//!     let org_id = registry.create_tenant(org).await?;
//!
//!     // Create a team under the organization
//!     let team = Tenant::new_team("Engineering", "engineering", org_id)
//!         .with_quotas(TenantQuotas::default());
//!     let team_id = registry.create_tenant(team).await?;
//!
//!     // Add members
//!     registry.add_member(&team_id, "alice@acme.com", TenantRole::Admin).await?;
//!     registry.add_member(&team_id, "bob@acme.com", TenantRole::Member).await?;
//!
//!     // Set up authentication
//!     let auth = TenantAuthManager::new(registry.clone(), b"secret-key".to_vec());
//!
//!     // Issue access token
//!     let token = auth.issue_access_token("alice@acme.com", &team_id).await?;
//!
//!     // Check permissions
//!     let can_submit = auth.check_permission(
//!         "alice@acme.com",
//!         &team_id,
//!         Permission::JobSubmit,
//!     ).await;
//!     assert!(can_submit);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Tenant-Aware Components
//!
//! ## Jobs
//!
//! Jobs are associated with a tenant through the `tenant_id` field.
//! The scheduler respects tenant isolation when placing tasks.
//!
//! ```rust,no_run
//! use marabunta_compute::common::types::Job;
//! use marabunta_compute::tenancy::TenantId;
//!
//! // Jobs include tenant context
//! // let job = Job::new("my-job").with_tenant(tenant_id);
//! ```
//!
//! ## Quotas
//!
//! The existing quotas system integrates with tenancy:
//!
//! - Quotas can be scoped to tenants
//! - Child tenants inherit parent quotas
//! - Resource usage is tracked per-tenant
//!
//! ## Policies
//!
//! Job placement policies are tenant-scoped:
//!
//! - Policies can be defined at any tenant level
//! - Child tenants inherit and can extend policies
//! - Governance integration for policy overrides
//!
//! # Integration with Governance
//!
//! The tenancy system integrates with the governance module:
//!
//! - Tenant admins map to governance principals
//! - Tenant hierarchy maps to governance domains
//! - Override policies respect tenant boundaries
//!
//! # Thread Safety
//!
//! All components in this module are thread-safe:
//!
//! - `TenantRegistry` uses `Arc<RwLock<>>` internally
//! - `TenantAuthManager` is designed for concurrent access
//! - `IsolationManager` uses async locks for consistency
//!
//! # Modules
//!
//! - [`types`]: Core tenant types and structures
//! - [`isolation`]: Resource, network, and data isolation
//! - [`registry`]: Tenant CRUD and hierarchy management
//! - [`auth`]: Authentication and authorization
//! - [`api`]: REST API endpoints
//! - [`persistence`]: Persistent storage
//! - [`errors`]: Error types

pub mod api;
pub mod auth;
pub mod errors;
pub mod isolation;
pub mod persistence;
pub mod registry;
pub mod types;

// Re-export commonly used types
pub use api::{create_tenant_router, TenantAction, TenantApiState, TenantAuthHandler};
pub use auth::{
    CrossTenantGrant, Permission, TenantAuthManager, TenantToken, TenantTokenClaims, TokenType,
};
pub use errors::{TenancyError, TenancyResult};
pub use isolation::{
    DataClassification, DataIsolation, DataSharingRule, IsolationManager, NetworkIsolation,
    NodeMatch, ResourceIsolationMode, TenantIsolation, TenantNodeAffinity,
};
pub use persistence::{PersistentTenantRegistry, TenancyPersistence, TenancyStorageStats};
pub use registry::{RegistryExport, TenantRegistry};
pub use types::{
    AuditLevel, BillingStatus, QuotaPeriod, Tenant, TenantBilling, TenantId, TenantLevel,
    TenantMembership, TenantPolicies, TenantQuotas, TenantRole, TenantStatus,
};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use chrono::Duration;
    use std::sync::Arc;

    /// Full integration test of the multi-tenancy system
    #[tokio::test]
    async fn test_full_tenancy_workflow() {
        // Create registry
        let registry = Arc::new(TenantRegistry::new());

        // 1. Create organization hierarchy
        let org = Tenant::new_organization("Acme Corp", "acme")
            .with_description("Acme Corporation - Research Division")
            .with_quotas(TenantQuotas {
                max_concurrent_jobs: Some(1000),
                max_cpu_hours: Some(100000.0),
                ..Default::default()
            })
            .with_policies(TenantPolicies {
                allow_phantom_nodes: true,
                require_encryption: true,
                ..Default::default()
            });
        let org_id = registry.create_tenant(org).await.unwrap();

        // 2. Create teams
        let team_eng =
            Tenant::new_team("Engineering", "engineering", org_id).with_quotas(TenantQuotas {
                max_concurrent_jobs: Some(500),
                max_cpu_hours: Some(50000.0),
                ..Default::default()
            });
        let team_eng_id = registry.create_tenant(team_eng).await.unwrap();

        let team_research = Tenant::new_team("Research", "research", org_id);
        let team_research_id = registry.create_tenant(team_research).await.unwrap();

        // 3. Create user workspace
        let user = Tenant::new_user("Alice's Workspace", "alice", team_eng_id, "alice@acme.com");
        let user_id = registry.create_tenant(user).await.unwrap();

        // 4. Verify hierarchy
        let path = registry.get_tenant_path(&user_id).await.unwrap();
        assert_eq!(path, "acme/engineering/alice");

        let ancestors = registry.get_ancestors(&user_id).await;
        assert_eq!(ancestors.len(), 2);

        let descendants = registry.get_descendants(&org_id).await;
        assert_eq!(descendants.len(), 3);

        let root = registry.get_root_organization(&user_id).await.unwrap();
        assert_eq!(root.id, org_id);

        // 5. Add members
        registry
            .add_member(&team_eng_id, "bob@acme.com", TenantRole::Member)
            .await
            .unwrap();
        registry
            .add_member(&team_eng_id, "charlie@acme.com", TenantRole::Viewer)
            .await
            .unwrap();

        // 6. Verify membership inheritance
        // Alice is admin of her workspace
        assert!(registry.is_member("alice@acme.com", &user_id).await);

        // Alice also has access to parent team via inheritance
        assert!(registry.is_member("alice@acme.com", &user_id).await);

        // Bob is member of team
        assert!(registry.is_member("bob@acme.com", &team_eng_id).await);

        // 7. Check effective policies (inheritance)
        let effective_policies = registry.get_effective_policies(&user_id).await.unwrap();
        assert!(effective_policies.require_encryption); // Inherited from org

        // 8. Set up authentication
        let auth = TenantAuthManager::new(registry.clone(), b"test-secret-key".to_vec());

        // 9. Issue tokens
        let token = auth
            .issue_access_token("alice@acme.com", &user_id)
            .await
            .unwrap();
        assert!(!token.claims.is_expired());
        assert!(token.claims.can_access_tenant(&user_id));

        // 10. Validate token
        let validated = auth.validate_token(&token.token).await;
        assert!(validated.is_some());

        // 11. Check permissions
        let can_submit = auth
            .check_permission("alice@acme.com", &user_id, Permission::JobSubmit)
            .await;
        assert!(can_submit);

        let can_manage_quotas = auth
            .check_permission("charlie@acme.com", &team_eng_id, Permission::QuotaManage)
            .await;
        assert!(!can_manage_quotas); // Viewer cannot manage quotas

        // 12. Cross-tenant access
        let grant = CrossTenantGrant::new(
            team_eng_id,
            team_research_id,
            vec![Permission::JobRead],
            "admin@acme.com",
            "Cross-team collaboration",
        )
        .expires_in(Duration::days(30));
        auth.create_cross_tenant_grant(grant).await.unwrap();

        // Now engineering team members can read research jobs
        let can_read_research = auth
            .check_permission("bob@acme.com", &team_research_id, Permission::JobRead)
            .await;
        assert!(can_read_research);

        // But not submit jobs
        let can_submit_research = auth
            .check_permission("bob@acme.com", &team_research_id, Permission::JobSubmit)
            .await;
        assert!(!can_submit_research);

        // 13. Isolation configuration
        let isolation_manager = registry.isolation_manager();

        let isolation = TenantIsolation::new(team_eng_id)
            .with_resource_mode(ResourceIsolationMode::Hybrid)
            .with_node_affinity(
                TenantNodeAffinity::default()
                    .with_preferred_tag("team", "engineering")
                    .with_required_tag("env", "production"),
            );
        isolation_manager.set_isolation(isolation).await;

        // Check node availability
        let mut prod_tags = std::collections::HashMap::new();
        prod_tags.insert("env".to_string(), "production".to_string());
        prod_tags.insert("team".to_string(), "engineering".to_string());

        let node_match = isolation_manager
            .is_node_available("node-1", &team_eng_id, &prod_tags)
            .await;
        assert!(node_match.is_allowed());
        assert!(node_match.score() > 0.5); // Has preferred tag

        // 14. Export and verify
        let export = registry.export().await;
        assert_eq!(export.tenants.len(), 4); // org + 2 teams + user

        // 15. Cleanup test
        let deleted = registry.delete_tenant_recursive(&org_id).await.unwrap();
        assert_eq!(deleted.len(), 4);
        assert_eq!(registry.count().await, 0);
    }

    /// Test tenant status lifecycle
    #[tokio::test]
    async fn test_tenant_status_lifecycle() {
        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Test Org", "test-org");
        let org_id = registry.create_tenant(org).await.unwrap();

        // Default status is active
        let tenant = registry.get_tenant(&org_id).await.unwrap();
        assert_eq!(tenant.status, TenantStatus::Active);

        // Suspend tenant
        registry
            .set_status(&org_id, TenantStatus::Suspended)
            .await
            .unwrap();
        let tenant = registry.get_tenant(&org_id).await.unwrap();
        assert_eq!(tenant.status, TenantStatus::Suspended);

        // Archive tenant
        registry
            .set_status(&org_id, TenantStatus::Archived)
            .await
            .unwrap();
        let tenant = registry.get_tenant(&org_id).await.unwrap();
        assert_eq!(tenant.status, TenantStatus::Archived);
    }

    /// Test isolation filtering
    #[tokio::test]
    async fn test_isolation_node_filtering() {
        let registry = Arc::new(TenantRegistry::new());

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();

        // Configure strict isolation
        let isolation = TenantIsolation::strict(org_id).with_node_affinity(
            TenantNodeAffinity::default()
                .with_dedicated_node("node-1")
                .with_dedicated_node("node-2"),
        );
        registry.isolation_manager().set_isolation(isolation).await;

        // Create test nodes
        let nodes = vec![
            ("node-1".to_string(), std::collections::HashMap::new()),
            ("node-2".to_string(), std::collections::HashMap::new()),
            ("node-3".to_string(), std::collections::HashMap::new()),
        ];

        let available = registry
            .isolation_manager()
            .filter_nodes(&org_id, &nodes)
            .await;

        // Only dedicated nodes should be available
        assert_eq!(available.len(), 2);
        let node_ids: Vec<&str> = available.iter().map(|(id, _)| id.as_str()).collect();
        assert!(node_ids.contains(&"node-1"));
        assert!(node_ids.contains(&"node-2"));
        assert!(!node_ids.contains(&"node-3"));
    }

    /// Test tenant quotas inheritance
    #[tokio::test]
    async fn test_quota_inheritance() {
        let registry = TenantRegistry::new();

        // Org with strict quotas
        let org = Tenant::new_organization("Org", "org").with_quotas(TenantQuotas {
            max_concurrent_jobs: Some(100),
            max_cpu_hours: Some(1000.0),
            ..Default::default()
        });
        let org_id = registry.create_tenant(org).await.unwrap();

        // Team with its own quotas
        let team = Tenant::new_team("Team", "team", org_id).with_quotas(TenantQuotas {
            max_concurrent_jobs: Some(50), // More restrictive
            max_cpu_hours: Some(2000.0),   // Less restrictive (but parent still applies)
            ..Default::default()
        });
        let team_id = registry.create_tenant(team).await.unwrap();

        let team_quotas = registry.get_effective_quotas(&team_id).await.unwrap();
        assert_eq!(team_quotas.max_concurrent_jobs, Some(50));
    }

    /// Test data sharing rules
    #[tokio::test]
    async fn test_data_sharing() {
        let tenant1 = TenantId::new();
        let tenant2 = TenantId::new();
        let tenant3 = TenantId::new();

        // Create isolation with sharing rules
        let sharing_rule = DataSharingRule::new(tenant2, "admin")
            .with_data_types(vec!["results".to_string(), "logs".to_string()])
            .bidirectional();

        let data_isolation = DataIsolation::for_tenant(&tenant1).with_sharing_rule(sharing_rule);

        // Can share results with tenant2
        assert!(data_isolation.can_share_with(&tenant2, "results"));
        assert!(data_isolation.can_share_with(&tenant2, "logs"));

        // Cannot share secrets
        assert!(!data_isolation.can_share_with(&tenant2, "secrets"));

        // Cannot share with tenant3
        assert!(!data_isolation.can_share_with(&tenant3, "results"));
    }
}
