// Marabunta - Licensed under the MIT License.
//! Governance Registry
//!
//! This module provides a unified registry for managing principals, delegations,
//! and authority/override checking in the governance system.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::authority::{AuthorityChain, AuthorityChecker, Delegation};
use super::errors::{GovernanceError, GovernanceResult};
use super::override_policy::{OverrideChecker, OverrideContext, OverridePolicy, OverrideResult};
use super::principal::{Principal, PrincipalId};

/// Unified governance registry for managing all governance-related data.
///
/// This registry provides a thread-safe interface for:
/// - Principal registration and management
/// - Delegation creation and revocation
/// - Authority checking
/// - Override policy enforcement
pub struct GovernanceRegistry {
    /// Registered principals indexed by ID.
    principals: Arc<RwLock<HashMap<PrincipalId, Principal>>>,
    /// Active delegations indexed by ID.
    delegations: Arc<RwLock<HashMap<String, Delegation>>>,
    /// The authority checker for validating authority.
    authority_checker: Arc<AuthorityChecker>,
    /// The override checker for validating overrides.
    override_checker: Arc<RwLock<Option<OverrideChecker>>>,
}

impl GovernanceRegistry {
    /// Creates a new empty governance registry.
    pub fn new() -> Self {
        let authority_checker = Arc::new(AuthorityChecker::new());

        Self {
            principals: Arc::new(RwLock::new(HashMap::new())),
            delegations: Arc::new(RwLock::new(HashMap::new())),
            authority_checker,
            override_checker: Arc::new(RwLock::new(None)),
        }
    }

    /// Creates a registry with a pre-configured authority checker.
    pub fn with_authority_checker(authority_checker: AuthorityChecker) -> Self {
        let authority_checker = Arc::new(authority_checker);
        Self {
            principals: Arc::new(RwLock::new(HashMap::new())),
            delegations: Arc::new(RwLock::new(HashMap::new())),
            authority_checker,
            override_checker: Arc::new(RwLock::new(None)),
        }
    }

    /// Initializes the override checker (call after registering principals).
    pub async fn init_override_checker(&self) {
        // Create a new authority checker that shares state
        let authority = AuthorityChecker::new();

        // Copy principals to the new checker
        let principals = self.principals.read().await;
        for principal in principals.values() {
            authority.register_principal(principal.clone()).await;
        }
        drop(principals);

        // Copy delegations
        let delegations = self.delegations.read().await;
        for delegation in delegations.values() {
            let _ = authority.add_delegation(delegation.clone()).await;
        }
        drop(delegations);

        let override_checker = OverrideChecker::new(authority);
        let mut checker = self.override_checker.write().await;
        *checker = Some(override_checker);
    }

    // =========================================================================
    // Principal Management
    // =========================================================================

    /// Registers a new principal.
    ///
    /// Returns an error if a principal with the same ID already exists.
    pub async fn register_principal(&self, principal: Principal) -> GovernanceResult<()> {
        let principal_id = principal.id.clone();

        // Check for duplicates
        {
            let principals = self.principals.read().await;
            if principals.contains_key(&principal_id) {
                return Err(GovernanceError::PrincipalAlreadyExists(principal_id));
            }
        }

        // Add to local registry
        {
            let mut principals = self.principals.write().await;
            principals.insert(principal_id.clone(), principal.clone());
        }

        // Register with authority checker
        self.authority_checker
            .register_principal(principal.clone())
            .await;

        // Update override checker if initialized
        if let Some(ref checker) = *self.override_checker.read().await {
            checker.authority().register_principal(principal).await;
        }

        Ok(())
    }

    /// Updates an existing principal.
    ///
    /// Returns an error if the principal doesn't exist.
    pub async fn update_principal(&self, principal: Principal) -> GovernanceResult<()> {
        let principal_id = principal.id.clone();

        // Check that principal exists
        {
            let principals = self.principals.read().await;
            if !principals.contains_key(&principal_id) {
                return Err(GovernanceError::PrincipalNotFound(principal_id));
            }
        }

        // Update in local registry
        {
            let mut principals = self.principals.write().await;
            principals.insert(principal_id.clone(), principal.clone());
        }

        // Update in authority checker (re-register)
        self.authority_checker
            .register_principal(principal.clone())
            .await;

        // Update in override checker if initialized
        if let Some(ref checker) = *self.override_checker.read().await {
            checker.authority().register_principal(principal).await;
        }

        Ok(())
    }

    /// Removes a principal.
    ///
    /// Returns the removed principal or an error if it doesn't exist.
    /// Note: This does not automatically revoke delegations involving this principal.
    pub async fn remove_principal(&self, id: &PrincipalId) -> GovernanceResult<Principal> {
        let mut principals = self.principals.write().await;
        principals
            .remove(id)
            .ok_or_else(|| GovernanceError::PrincipalNotFound(id.clone()))
    }

    /// Gets a principal by ID.
    pub async fn get_principal(&self, id: &PrincipalId) -> Option<Principal> {
        let principals = self.principals.read().await;
        principals.get(id).cloned()
    }

    /// Lists all registered principals.
    pub async fn list_principals(&self) -> Vec<Principal> {
        let principals = self.principals.read().await;
        principals.values().cloned().collect()
    }

    /// Gets the count of registered principals.
    pub async fn principal_count(&self) -> usize {
        let principals = self.principals.read().await;
        principals.len()
    }

    // =========================================================================
    // Delegation Management
    // =========================================================================

    /// Creates a new delegation.
    ///
    /// This validates:
    /// - The delegator exists and has authority over the domains
    /// - The delegatee exists
    /// - The delegation doesn't create a cycle
    /// - Redelegation constraints are respected
    pub async fn create_delegation(&self, delegation: Delegation) -> GovernanceResult<()> {
        let delegation_id = delegation.id.clone();

        // Check for duplicate
        {
            let delegations = self.delegations.read().await;
            if delegations.contains_key(&delegation_id) {
                return Err(GovernanceError::DelegationAlreadyExists(delegation_id));
            }
        }

        // Validate and add via authority checker
        self.authority_checker
            .add_delegation(delegation.clone())
            .await
            .map_err(|e| GovernanceError::CannotDelegate(e.to_string()))?;

        // Store locally
        {
            let mut delegations = self.delegations.write().await;
            delegations.insert(delegation_id, delegation);
        }

        Ok(())
    }

    /// Revokes a delegation by ID.
    pub async fn revoke_delegation(&self, id: &str) -> GovernanceResult<()> {
        // Check delegation exists
        {
            let delegations = self.delegations.read().await;
            if !delegations.contains_key(id) {
                return Err(GovernanceError::DelegationNotFound(id.to_string()));
            }
        }

        // Revoke in authority checker
        self.authority_checker
            .revoke_delegation(id)
            .await
            .map_err(|e| GovernanceError::CannotDelegate(e.to_string()))?;

        // Update local record
        {
            let mut delegations = self.delegations.write().await;
            if let Some(d) = delegations.get_mut(id) {
                d.revoked = true;
            }
        }

        Ok(())
    }

    /// Gets a delegation by ID.
    pub async fn get_delegation(&self, id: &str) -> Option<Delegation> {
        let delegations = self.delegations.read().await;
        delegations.get(id).cloned()
    }

    /// Lists all delegations.
    pub async fn list_delegations(&self) -> Vec<Delegation> {
        let delegations = self.delegations.read().await;
        delegations.values().cloned().collect()
    }

    /// Lists only valid (non-expired, non-revoked) delegations.
    pub async fn list_valid_delegations(&self) -> Vec<Delegation> {
        let delegations = self.delegations.read().await;
        delegations
            .values()
            .filter(|d| d.is_valid())
            .cloned()
            .collect()
    }

    /// Gets delegations granted by a principal.
    pub async fn delegations_from(&self, principal_id: &PrincipalId) -> Vec<Delegation> {
        let delegations = self.delegations.read().await;
        delegations
            .values()
            .filter(|d| &d.from == principal_id)
            .cloned()
            .collect()
    }

    /// Gets delegations received by a principal.
    pub async fn delegations_to(&self, principal_id: &PrincipalId) -> Vec<Delegation> {
        let delegations = self.delegations.read().await;
        delegations
            .values()
            .filter(|d| &d.to == principal_id)
            .cloned()
            .collect()
    }

    // =========================================================================
    // Authority Queries
    // =========================================================================

    /// Checks if a principal has authority over a domain.
    pub async fn has_authority(&self, principal: &PrincipalId, domain: &str) -> bool {
        self.authority_checker
            .has_authority(principal, domain)
            .await
    }

    /// Gets the effective priority for a principal in a domain.
    pub async fn effective_priority(&self, principal: &PrincipalId, domain: &str) -> Option<u32> {
        self.authority_checker
            .effective_priority(principal, domain)
            .await
    }

    /// Checks if one principal can override another in a domain.
    pub async fn can_override_principal(
        &self,
        actor: &PrincipalId,
        target: &PrincipalId,
        domain: &str,
    ) -> bool {
        self.authority_checker
            .can_override(actor, target, domain)
            .await
    }

    /// Gets all principals with authority over a domain.
    pub async fn authorities_for_domain(&self, domain: &str) -> Vec<Principal> {
        self.authority_checker.authorities_for_domain(domain).await
    }

    /// Gets the authority chain for a principal over a domain.
    pub async fn authority_chain(&self, principal: &PrincipalId, domain: &str) -> AuthorityChain {
        self.authority_checker
            .authority_chain(principal, domain)
            .await
    }

    // =========================================================================
    // Override Policy Checking
    // =========================================================================

    /// Checks if a principal can override according to a policy.
    ///
    /// Note: You must call `init_override_checker()` before using this method.
    pub async fn can_override(
        &self,
        actor: &PrincipalId,
        policy: &OverridePolicy,
        domain: &str,
        ctx: &OverrideContext,
    ) -> OverrideResult {
        let checker = self.override_checker.read().await;
        match &*checker {
            Some(c) => c.can_override(actor, policy, domain, ctx).await,
            None => OverrideResult::Denied {
                reason: "Override checker not initialized".to_string(),
            },
        }
    }

    // =========================================================================
    // Utility Methods
    // =========================================================================

    /// Gets a reference to the authority checker.
    pub fn authority_checker(&self) -> &AuthorityChecker {
        &self.authority_checker
    }

    /// Validates the entire registry state.
    ///
    /// Checks for:
    /// - Orphaned delegations (referencing non-existent principals)
    /// - Expired delegations
    /// - Invalid authority chains
    pub async fn validate(&self) -> Vec<ValidationIssue> {
        let mut issues = Vec::new();

        let principals = self.principals.read().await;
        let delegations = self.delegations.read().await;

        // Check for orphaned delegations
        for (id, delegation) in delegations.iter() {
            if !principals.contains_key(&delegation.from) {
                issues.push(ValidationIssue::OrphanedDelegation {
                    delegation_id: id.clone(),
                    missing_principal: delegation.from.clone(),
                    role: "delegator".to_string(),
                });
            }
            if !principals.contains_key(&delegation.to) {
                issues.push(ValidationIssue::OrphanedDelegation {
                    delegation_id: id.clone(),
                    missing_principal: delegation.to.clone(),
                    role: "delegatee".to_string(),
                });
            }
        }

        // Check for expired delegations
        for (id, delegation) in delegations.iter() {
            if delegation.is_expired() && !delegation.revoked {
                issues.push(ValidationIssue::ExpiredDelegation {
                    delegation_id: id.clone(),
                    expired_at: delegation.expires_at.unwrap(),
                });
            }
        }

        issues
    }

    /// Cleans up expired and revoked delegations.
    pub async fn cleanup_delegations(&self) -> usize {
        let mut delegations = self.delegations.write().await;
        let before = delegations.len();

        delegations.retain(|_, d| d.is_valid());

        before - delegations.len()
    }

    /// Exports the registry state for persistence.
    pub async fn export(&self) -> RegistryExport {
        let principals = self.principals.read().await;
        let delegations = self.delegations.read().await;

        RegistryExport {
            principals: principals.values().cloned().collect(),
            delegations: delegations.values().cloned().collect(),
        }
    }

    /// Imports registry state (clears existing state first).
    /// Note: Delegations are imported without re-validation since they were
    /// previously validated when created.
    pub async fn import(&self, export: RegistryExport) -> GovernanceResult<()> {
        // Clear existing state
        {
            let mut principals = self.principals.write().await;
            principals.clear();
        }
        {
            let mut delegations = self.delegations.write().await;
            delegations.clear();
        }

        // Import principals
        for principal in export.principals {
            self.register_principal(principal).await?;
        }

        // Import delegations directly without re-validation
        // (they were validated when originally created)
        {
            let mut delegations = self.delegations.write().await;
            for delegation in export.delegations {
                // Also add to authority checker (unchecked)
                self.authority_checker
                    .add_delegation_unchecked(delegation.clone())
                    .await;
                delegations.insert(delegation.id.clone(), delegation);
            }
        }

        // Re-initialize override checker
        self.init_override_checker().await;

        Ok(())
    }
}

impl Default for GovernanceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Persistent Governance Registry
// ============================================================================

use super::persistence::GovernancePersistence;
use crate::storage::persistence::{PersistenceBackend, PersistenceError};

/// A governance registry with automatic persistence.
///
/// This struct wraps a `GovernanceRegistry` and automatically persists
/// changes to the underlying storage backend.
///
/// # Example
///
/// ```rust,no_run
/// use marabunta_compute::governance::{PersistentGovernanceRegistry, Principal, DomainPattern};
/// use marabunta_compute::governance::persistence::InMemoryBackend;
///
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     // Create a persistent registry
///     let backend = InMemoryBackend::new();
///     let registry = PersistentGovernanceRegistry::with_persistence(backend).await?;
///
///     // All mutations are automatically persisted
///     let principal = Principal::new_user("alice", "Alice", 100)
///         .with_domain(DomainPattern::exact("project:alpha"));
///     registry.register_principal(principal).await?;
///
///     Ok(())
/// }
/// ```
pub struct PersistentGovernanceRegistry<B: PersistenceBackend> {
    /// The underlying governance registry
    registry: GovernanceRegistry,
    /// The persistence layer
    persistence: GovernancePersistence<B>,
}

impl<B: PersistenceBackend + 'static> PersistentGovernanceRegistry<B> {
    /// Creates a new persistent governance registry with the given backend.
    ///
    /// This will attempt to load existing data from the backend. If no data
    /// exists, an empty registry is created.
    ///
    /// # Arguments
    ///
    /// * `backend` - The persistence backend to use
    ///
    /// # Returns
    ///
    /// Returns the persistent registry or a `PersistenceError` if loading fails.
    pub async fn with_persistence(backend: B) -> Result<Self, PersistenceError> {
        let persistence = GovernancePersistence::new(backend);

        // Try to load existing registry state
        let registry = persistence.load_registry().await.unwrap_or_else(|_| {
            // If loading fails, create a new empty registry
            GovernanceRegistry::new()
        });

        Ok(Self {
            registry,
            persistence,
        })
    }

    /// Creates a persistent registry from an existing registry and backend.
    ///
    /// This immediately saves the registry state to the backend.
    pub async fn from_registry(
        registry: GovernanceRegistry,
        backend: B,
    ) -> Result<Self, PersistenceError> {
        let persistence = GovernancePersistence::new(backend);
        persistence.save_registry(&registry).await?;

        Ok(Self {
            registry,
            persistence,
        })
    }

    /// Gets a reference to the underlying registry.
    pub fn registry(&self) -> &GovernanceRegistry {
        &self.registry
    }

    /// Gets the persistence layer.
    pub fn persistence(&self) -> &GovernancePersistence<B> {
        &self.persistence
    }

    // =========================================================================
    // Principal Management (with auto-save)
    // =========================================================================

    /// Registers a new principal and persists the change.
    pub async fn register_principal(&self, principal: Principal) -> GovernanceResult<()> {
        // First persist
        self.persistence
            .save_principal(&principal)
            .await
            .map_err(|e| GovernanceError::Internal(format!("Persistence error: {}", e)))?;

        // Then update registry
        self.registry.register_principal(principal).await
    }

    /// Updates an existing principal and persists the change.
    pub async fn update_principal(&self, principal: Principal) -> GovernanceResult<()> {
        // First persist
        self.persistence
            .save_principal(&principal)
            .await
            .map_err(|e| GovernanceError::Internal(format!("Persistence error: {}", e)))?;

        // Then update registry
        self.registry.update_principal(principal).await
    }

    /// Removes a principal and persists the change.
    pub async fn remove_principal(&self, id: &PrincipalId) -> GovernanceResult<Principal> {
        // First remove from registry (to validate it exists)
        let principal = self.registry.remove_principal(id).await?;

        // Then remove from persistence
        let _ = self.persistence.delete_principal(id).await;

        Ok(principal)
    }

    /// Gets a principal by ID (no persistence needed for reads).
    pub async fn get_principal(&self, id: &PrincipalId) -> Option<Principal> {
        self.registry.get_principal(id).await
    }

    /// Lists all principals.
    pub async fn list_principals(&self) -> Vec<Principal> {
        self.registry.list_principals().await
    }

    /// Gets the count of registered principals.
    pub async fn principal_count(&self) -> usize {
        self.registry.principal_count().await
    }

    // =========================================================================
    // Delegation Management (with auto-save)
    // =========================================================================

    /// Creates a new delegation and persists the change.
    pub async fn create_delegation(&self, delegation: Delegation) -> GovernanceResult<()> {
        // First validate and create in registry
        self.registry.create_delegation(delegation.clone()).await?;

        // Then persist
        self.persistence
            .save_delegation(&delegation)
            .await
            .map_err(|e| GovernanceError::Internal(format!("Persistence error: {}", e)))?;

        Ok(())
    }

    /// Revokes a delegation and persists the change.
    pub async fn revoke_delegation(&self, id: &str) -> GovernanceResult<()> {
        // First revoke in registry
        self.registry.revoke_delegation(id).await?;

        // Then update persistence (load, modify, save)
        if let Ok(Some(mut delegation)) = self.persistence.load_delegation(id).await {
            delegation.revoked = true;
            let _ = self.persistence.save_delegation(&delegation).await;
        }

        Ok(())
    }

    /// Gets a delegation by ID.
    pub async fn get_delegation(&self, id: &str) -> Option<Delegation> {
        self.registry.get_delegation(id).await
    }

    /// Lists all delegations.
    pub async fn list_delegations(&self) -> Vec<Delegation> {
        self.registry.list_delegations().await
    }

    /// Lists valid delegations.
    pub async fn list_valid_delegations(&self) -> Vec<Delegation> {
        self.registry.list_valid_delegations().await
    }

    // =========================================================================
    // Authority Queries (delegated to registry)
    // =========================================================================

    /// Checks if a principal has authority over a domain.
    pub async fn has_authority(&self, principal: &PrincipalId, domain: &str) -> bool {
        self.registry.has_authority(principal, domain).await
    }

    /// Gets the effective priority for a principal in a domain.
    pub async fn effective_priority(&self, principal: &PrincipalId, domain: &str) -> Option<u32> {
        self.registry.effective_priority(principal, domain).await
    }

    /// Checks if one principal can override another.
    pub async fn can_override_principal(
        &self,
        actor: &PrincipalId,
        target: &PrincipalId,
        domain: &str,
    ) -> bool {
        self.registry
            .can_override_principal(actor, target, domain)
            .await
    }

    /// Gets the authority chain for a principal over a domain.
    pub async fn authority_chain(&self, principal: &PrincipalId, domain: &str) -> AuthorityChain {
        self.registry.authority_chain(principal, domain).await
    }

    // =========================================================================
    // Utility Methods
    // =========================================================================

    /// Initializes the override checker.
    pub async fn init_override_checker(&self) {
        self.registry.init_override_checker().await
    }

    /// Validates the registry.
    pub async fn validate(&self) -> Vec<ValidationIssue> {
        self.registry.validate().await
    }

    /// Saves the current state to persistence.
    pub async fn save(&self) -> Result<(), PersistenceError> {
        self.persistence.save_registry(&self.registry).await
    }

    /// Reloads the registry from persistence.
    pub async fn reload(&mut self) -> Result<(), PersistenceError> {
        let loaded = self.persistence.load_registry().await?;
        self.registry = loaded;
        Ok(())
    }

    /// Exports the registry state.
    pub async fn export(&self) -> RegistryExport {
        self.registry.export().await
    }
}

/// Issues found during registry validation.
#[derive(Debug, Clone)]
pub enum ValidationIssue {
    /// A delegation references a non-existent principal.
    OrphanedDelegation {
        delegation_id: String,
        missing_principal: PrincipalId,
        role: String,
    },
    /// A delegation has expired but wasn't revoked.
    ExpiredDelegation {
        delegation_id: String,
        expired_at: chrono::DateTime<chrono::Utc>,
    },
    /// A principal has invalid authority configuration.
    InvalidAuthority {
        principal_id: PrincipalId,
        reason: String,
    },
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationIssue::OrphanedDelegation {
                delegation_id,
                missing_principal,
                role,
            } => write!(
                f,
                "Delegation {} references missing {} '{}'",
                delegation_id, role, missing_principal
            ),
            ValidationIssue::ExpiredDelegation {
                delegation_id,
                expired_at,
            } => write!(f, "Delegation {} expired at {}", delegation_id, expired_at),
            ValidationIssue::InvalidAuthority {
                principal_id,
                reason,
            } => write!(
                f,
                "Principal {} has invalid authority: {}",
                principal_id, reason
            ),
        }
    }
}

/// Exported registry state for persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RegistryExport {
    /// All principals.
    pub principals: Vec<Principal>,
    /// All delegations.
    pub delegations: Vec<Delegation>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::principal::DomainPattern;
    use std::time::Duration;

    async fn setup_test_registry() -> GovernanceRegistry {
        let registry = GovernanceRegistry::new();

        // Register principals
        let ceo = Principal::new_user("ceo", "CEO", 1000).with_domain(DomainPattern::global());
        let vp = Principal::new_user("vp", "VP", 800).with_domain(DomainPattern::prefix("dept:"));
        let manager = Principal::new_user("manager", "Manager", 500)
            .with_domain(DomainPattern::exact("dept:eng"));
        let employee = Principal::new_user("employee", "Employee", 100);

        registry.register_principal(ceo).await.unwrap();
        registry.register_principal(vp).await.unwrap();
        registry.register_principal(manager).await.unwrap();
        registry.register_principal(employee).await.unwrap();

        registry.init_override_checker().await;

        registry
    }

    #[tokio::test]
    async fn test_principal_registration() {
        let registry = GovernanceRegistry::new();

        let principal = Principal::new_user("alice", "Alice", 100);
        registry.register_principal(principal).await.unwrap();

        let retrieved = registry.get_principal(&"alice".to_string()).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "Alice");
    }

    #[tokio::test]
    async fn test_duplicate_principal() {
        let registry = GovernanceRegistry::new();

        let principal1 = Principal::new_user("alice", "Alice", 100);
        let principal2 = Principal::new_user("alice", "Alice 2", 200);

        registry.register_principal(principal1).await.unwrap();
        let result = registry.register_principal(principal2).await;

        assert!(matches!(
            result,
            Err(GovernanceError::PrincipalAlreadyExists(_))
        ));
    }

    #[tokio::test]
    async fn test_principal_update() {
        let registry = GovernanceRegistry::new();

        let principal = Principal::new_user("alice", "Alice", 100);
        registry.register_principal(principal).await.unwrap();

        let updated = Principal::new_user("alice", "Alice Smith", 150);
        registry.update_principal(updated).await.unwrap();

        let retrieved = registry.get_principal(&"alice".to_string()).await.unwrap();
        assert_eq!(retrieved.name, "Alice Smith");
        assert_eq!(retrieved.priority, 150);
    }

    #[tokio::test]
    async fn test_principal_removal() {
        let registry = GovernanceRegistry::new();

        let principal = Principal::new_user("alice", "Alice", 100);
        registry.register_principal(principal).await.unwrap();

        let removed = registry
            .remove_principal(&"alice".to_string())
            .await
            .unwrap();
        assert_eq!(removed.name, "Alice");

        assert!(registry.get_principal(&"alice".to_string()).await.is_none());
    }

    #[tokio::test]
    async fn test_delegation_creation() {
        let registry = setup_test_registry().await;

        let delegation = Delegation::new(
            "del-1",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        );
        registry.create_delegation(delegation).await.unwrap();

        // Employee should now have authority
        assert!(
            registry
                .has_authority(&"employee".to_string(), "dept:eng")
                .await
        );
    }

    #[tokio::test]
    async fn test_delegation_revocation() {
        let registry = setup_test_registry().await;

        let delegation = Delegation::new(
            "del-1",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        );
        registry.create_delegation(delegation).await.unwrap();

        // Revoke
        registry.revoke_delegation("del-1").await.unwrap();

        // Employee should no longer have authority
        assert!(
            !registry
                .has_authority(&"employee".to_string(), "dept:eng")
                .await
        );
    }

    #[tokio::test]
    async fn test_authority_queries() {
        let registry = setup_test_registry().await;

        // CEO has global authority
        assert!(registry.has_authority(&"ceo".to_string(), "anything").await);
        assert!(registry.has_authority(&"ceo".to_string(), "dept:eng").await);

        // VP has authority over dept:*
        assert!(registry.has_authority(&"vp".to_string(), "dept:eng").await);
        assert!(
            registry
                .has_authority(&"vp".to_string(), "dept:sales")
                .await
        );

        // Manager has authority over dept:eng only
        assert!(
            registry
                .has_authority(&"manager".to_string(), "dept:eng")
                .await
        );
        assert!(
            !registry
                .has_authority(&"manager".to_string(), "dept:sales")
                .await
        );

        // Employee has no authority
        assert!(
            !registry
                .has_authority(&"employee".to_string(), "anything")
                .await
        );
    }

    #[tokio::test]
    async fn test_effective_priority() {
        let registry = setup_test_registry().await;

        assert_eq!(
            registry
                .effective_priority(&"ceo".to_string(), "anything")
                .await,
            Some(1000)
        );
        assert_eq!(
            registry
                .effective_priority(&"vp".to_string(), "dept:eng")
                .await,
            Some(800)
        );
        assert_eq!(
            registry
                .effective_priority(&"employee".to_string(), "anything")
                .await,
            None
        );
    }

    #[tokio::test]
    async fn test_can_override_principal() {
        let registry = setup_test_registry().await;

        // CEO can override VP
        assert!(
            registry
                .can_override_principal(&"ceo".to_string(), &"vp".to_string(), "dept:eng")
                .await
        );

        // VP cannot override CEO
        assert!(
            !registry
                .can_override_principal(&"vp".to_string(), &"ceo".to_string(), "dept:eng")
                .await
        );
    }

    #[tokio::test]
    async fn test_override_policy_check() {
        let registry = setup_test_registry().await;

        let policy = OverridePolicy::blueprint(200);
        let context = OverrideContext::new("employee", 100);

        // VP can override
        let result = registry
            .can_override(&"vp".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(result.is_allowed());

        // Employee cannot (insufficient priority)
        let result = registry
            .can_override(&"employee".to_string(), &policy, "dept:eng", &context)
            .await;
        assert!(!result.is_allowed());
    }

    #[tokio::test]
    async fn test_authority_chain() {
        let registry = setup_test_registry().await;

        // Create delegation chain
        let del1 = Delegation::new(
            "del-1",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        );
        registry.create_delegation(del1).await.unwrap();

        let chain = registry
            .authority_chain(&"employee".to_string(), "dept:eng")
            .await;

        assert!(chain.has_authority);
        assert!(!chain.granted_via.is_empty());
    }

    #[tokio::test]
    async fn test_validation() {
        let registry = GovernanceRegistry::new();

        let principal =
            Principal::new_user("alice", "Alice", 100).with_domain(DomainPattern::global());
        registry.register_principal(principal).await.unwrap();

        // Create a delegation with missing delegatee
        let delegation = Delegation::new(
            "del-1",
            "alice",
            "bob", // doesn't exist
            vec![DomainPattern::global()],
        );

        // Add directly to bypass validation
        {
            let mut delegations = registry.delegations.write().await;
            delegations.insert("del-1".to_string(), delegation);
        }

        let issues = registry.validate().await;
        assert!(!issues.is_empty());
        assert!(issues.iter().any(|i| matches!(
            i,
            ValidationIssue::OrphanedDelegation { missing_principal, .. }
            if missing_principal == "bob"
        )));
    }

    #[tokio::test]
    async fn test_cleanup_expired_delegations() {
        let registry = setup_test_registry().await;

        // Create an expired delegation
        let delegation = Delegation::new(
            "del-expired",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        )
        .expires_in(chrono::Duration::seconds(-3600)); // expired 1 hour ago

        // Add directly to bypass validation
        {
            let mut delegations = registry.delegations.write().await;
            delegations.insert("del-expired".to_string(), delegation);
        }

        // Create a valid delegation
        let valid_delegation = Delegation::new(
            "del-valid",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        )
        .expires_in(chrono::Duration::hours(24));

        {
            let mut delegations = registry.delegations.write().await;
            delegations.insert("del-valid".to_string(), valid_delegation);
        }

        // Cleanup
        let removed = registry.cleanup_delegations().await;
        assert_eq!(removed, 1);

        // Check state
        let delegations = registry.list_delegations().await;
        assert_eq!(delegations.len(), 1);
        assert_eq!(delegations[0].id, "del-valid");
    }

    #[tokio::test]
    async fn test_export_import() {
        let registry1 = setup_test_registry().await;

        // Add a delegation
        let delegation = Delegation::new(
            "del-1",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        );
        registry1.create_delegation(delegation).await.unwrap();

        // Export
        let export = registry1.export().await;

        // Import to new registry
        let registry2 = GovernanceRegistry::new();
        registry2.import(export).await.unwrap();

        // Verify state
        assert_eq!(registry2.principal_count().await, 4);
        assert_eq!(registry2.list_delegations().await.len(), 1);
        assert!(
            registry2
                .has_authority(&"employee".to_string(), "dept:eng")
                .await
        );
    }

    #[tokio::test]
    async fn test_authorities_for_domain() {
        let registry = setup_test_registry().await;

        let authorities = registry.authorities_for_domain("dept:eng").await;

        // CEO, VP, and Manager all have authority over dept:eng
        let ids: Vec<_> = authorities.iter().map(|p| p.id.clone()).collect();
        assert!(ids.contains(&"ceo".to_string()));
        assert!(ids.contains(&"vp".to_string()));
        assert!(ids.contains(&"manager".to_string()));
        assert!(!ids.contains(&"employee".to_string()));
    }
}
