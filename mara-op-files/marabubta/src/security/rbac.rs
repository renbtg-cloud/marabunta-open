// Marabunta - Licensed under the MIT License.
//! SecurityDomain RBAC roles composable with the existing [`Permission`] hierarchy.
//!
//! EU highestsec compliance requires fine-grained role separation beyond the
//! standard Admin/Operator/User/ReadOnly permission levels. This module adds
//! a [`SecurityDomainRole`] enum and a [`RoleSet`] that composes base permissions
//! with security_domain-specific roles for access control decisions.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::security::auth::Permission;

// ============================================================================
// SecurityDomainRole
// ============================================================================

/// SecurityDomain-domain roles that layer on top of the base [`Permission`] hierarchy.
///
/// Each role grants a narrow capability within the highestsec compliance
/// framework. A single identity may hold multiple security_domain roles.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum SecurityDomainRole {
    /// May submit blind jobs and cleartext jobs to the swarm.
    Submitter,
    /// May execute job chunks inside the sandbox.
    Executor,
    /// May aggregate partial results into final outputs.
    Consolidator,
    /// May access audit trails, chain verification, and GDPR reports.
    Auditor,
}

impl std::fmt::Display for SecurityDomainRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityDomainRole::Submitter => write!(f, "submitter"),
            SecurityDomainRole::Executor => write!(f, "executor"),
            SecurityDomainRole::Consolidator => write!(f, "consolidator"),
            SecurityDomainRole::Auditor => write!(f, "auditor"),
        }
    }
}

// ============================================================================
// RoleSet
// ============================================================================

/// A combination of a base [`Permission`] level and zero or more
/// [`SecurityDomainRole`]s. All access-control checks in the highestsec compliance
/// layer go through this struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleSet {
    /// The underlying platform permission level.
    pub base_permission: Permission,
    /// Additional security_domain-domain roles.
    pub security_domain_roles: HashSet<SecurityDomainRole>,
}

impl RoleSet {
    /// Create a new role set with the given base permission and no security_domain roles.
    pub fn new(base_permission: Permission) -> Self {
        Self {
            base_permission,
            security_domain_roles: HashSet::new(),
        }
    }

    /// Builder-style method to add a security_domain role.
    pub fn with_role(mut self, role: SecurityDomainRole) -> Self {
        self.security_domain_roles.insert(role);
        self
    }

    /// Check whether the role set contains a specific security_domain role.
    pub fn has_security_domain_role(&self, role: SecurityDomainRole) -> bool {
        self.security_domain_roles.contains(&role)
    }

    /// Whether this role set can access audit trails.
    ///
    /// Requires the [`SecurityDomainRole::Auditor`] role **or** [`Permission::Admin`].
    pub fn can_access_audit(&self) -> bool {
        self.security_domain_roles.contains(&SecurityDomainRole::Auditor)
            || self.base_permission == Permission::Admin
    }

    /// Whether this role set can submit jobs (cleartext).
    ///
    /// Requires the [`SecurityDomainRole::Submitter`] role **or** base permission
    /// >= [`Permission::User`].
    pub fn can_submit_jobs(&self) -> bool {
        self.security_domain_roles.contains(&SecurityDomainRole::Submitter)
            || self.base_permission.allows(Permission::User)
    }

    /// Whether this role set can execute job chunks.
    ///
    /// Requires the [`SecurityDomainRole::Executor`] role **or** base permission
    /// >= [`Permission::User`].
    pub fn can_execute(&self) -> bool {
        self.security_domain_roles.contains(&SecurityDomainRole::Executor)
            || self.base_permission.allows(Permission::User)
    }

    /// Whether this role set can consolidate (aggregate) partial results.
    ///
    /// Requires the [`SecurityDomainRole::Consolidator`] role **or** base permission
    /// >= [`Permission::Operator`].
    pub fn can_consolidate(&self) -> bool {
        self.security_domain_roles.contains(&SecurityDomainRole::Consolidator)
            || self.base_permission.allows(Permission::Operator)
    }

    /// Whether this role set can submit blind (encrypted) jobs.
    ///
    /// Requires **both** the [`SecurityDomainRole::Submitter`] role **and** base
    /// permission >= [`Permission::User`]. This is stricter than
    /// `can_submit_jobs` because blind pipelines carry classified data.
    pub fn can_submit_blind(&self) -> bool {
        self.security_domain_roles.contains(&SecurityDomainRole::Submitter)
            && self.base_permission.allows(Permission::User)
    }

    /// Whether this role set can manage sovereignty zones.
    ///
    /// Requires [`Permission::Admin`] only -- no security_domain role shortcut.
    pub fn can_manage_zones(&self) -> bool {
        self.base_permission == Permission::Admin
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_has_no_security_domain_roles() {
        let rs = RoleSet::new(Permission::User);
        assert!(rs.security_domain_roles.is_empty());
        assert_eq!(rs.base_permission, Permission::User);
    }

    #[test]
    fn test_with_role_adds_role() {
        let rs = RoleSet::new(Permission::ReadOnly)
            .with_role(SecurityDomainRole::Auditor)
            .with_role(SecurityDomainRole::Submitter);
        assert!(rs.has_security_domain_role(SecurityDomainRole::Auditor));
        assert!(rs.has_security_domain_role(SecurityDomainRole::Submitter));
        assert!(!rs.has_security_domain_role(SecurityDomainRole::Executor));
    }

    #[test]
    fn test_can_access_audit_with_auditor_role() {
        let rs = RoleSet::new(Permission::ReadOnly).with_role(SecurityDomainRole::Auditor);
        assert!(rs.can_access_audit());
    }

    #[test]
    fn test_can_access_audit_with_admin_permission() {
        let rs = RoleSet::new(Permission::Admin);
        assert!(rs.can_access_audit());
    }

    #[test]
    fn test_cannot_access_audit_without_auditor_or_admin() {
        let rs = RoleSet::new(Permission::Operator);
        assert!(!rs.can_access_audit());
    }

    #[test]
    fn test_can_submit_jobs_with_submitter_role() {
        let rs = RoleSet::new(Permission::ReadOnly).with_role(SecurityDomainRole::Submitter);
        assert!(rs.can_submit_jobs());
    }

    #[test]
    fn test_can_submit_jobs_with_user_permission() {
        let rs = RoleSet::new(Permission::User);
        assert!(rs.can_submit_jobs());
    }

    #[test]
    fn test_cannot_submit_jobs_readonly_no_role() {
        let rs = RoleSet::new(Permission::ReadOnly);
        assert!(!rs.can_submit_jobs());
    }

    #[test]
    fn test_can_execute_with_executor_role() {
        let rs = RoleSet::new(Permission::ReadOnly).with_role(SecurityDomainRole::Executor);
        assert!(rs.can_execute());
    }

    #[test]
    fn test_can_execute_with_user_permission() {
        let rs = RoleSet::new(Permission::User);
        assert!(rs.can_execute());
    }

    #[test]
    fn test_can_consolidate_with_consolidator_role() {
        let rs = RoleSet::new(Permission::ReadOnly).with_role(SecurityDomainRole::Consolidator);
        assert!(rs.can_consolidate());
    }

    #[test]
    fn test_can_consolidate_with_operator_permission() {
        let rs = RoleSet::new(Permission::Operator);
        assert!(rs.can_consolidate());
    }

    #[test]
    fn test_cannot_consolidate_user_no_role() {
        let rs = RoleSet::new(Permission::User);
        assert!(!rs.can_consolidate());
    }

    #[test]
    fn test_can_submit_blind_requires_both() {
        // Submitter role alone with ReadOnly -- cannot
        let rs1 = RoleSet::new(Permission::ReadOnly).with_role(SecurityDomainRole::Submitter);
        assert!(!rs1.can_submit_blind());

        // User permission alone without Submitter role -- cannot
        let rs2 = RoleSet::new(Permission::User);
        assert!(!rs2.can_submit_blind());

        // Both Submitter role AND User permission -- can
        let rs3 = RoleSet::new(Permission::User).with_role(SecurityDomainRole::Submitter);
        assert!(rs3.can_submit_blind());
    }

    #[test]
    fn test_can_manage_zones_admin_only() {
        assert!(RoleSet::new(Permission::Admin).can_manage_zones());
        assert!(!RoleSet::new(Permission::Operator).can_manage_zones());
        assert!(
            !RoleSet::new(Permission::Operator)
                .with_role(SecurityDomainRole::Auditor)
                .can_manage_zones()
        );
    }

    #[test]
    fn test_security_domain_role_display() {
        assert_eq!(SecurityDomainRole::Submitter.to_string(), "submitter");
        assert_eq!(SecurityDomainRole::Executor.to_string(), "executor");
        assert_eq!(SecurityDomainRole::Consolidator.to_string(), "consolidator");
        assert_eq!(SecurityDomainRole::Auditor.to_string(), "auditor");
    }

    #[test]
    fn test_role_set_serialization_roundtrip() {
        let rs = RoleSet::new(Permission::Operator)
            .with_role(SecurityDomainRole::Auditor)
            .with_role(SecurityDomainRole::Executor);
        let json = serde_json::to_string(&rs).expect("serialize");
        let deserialized: RoleSet = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.base_permission, Permission::Operator);
        assert!(deserialized.has_security_domain_role(SecurityDomainRole::Auditor));
        assert!(deserialized.has_security_domain_role(SecurityDomainRole::Executor));
        assert!(!deserialized.has_security_domain_role(SecurityDomainRole::Submitter));
    }
}
