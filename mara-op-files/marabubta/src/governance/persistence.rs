// Marabunta - Licensed under the MIT License.
//! Governance Persistence Layer
//!
//! This module provides persistence for the governance system, including
//! principals, delegations, and override policy configurations.
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::governance::persistence::GovernancePersistence;
//! use marabunta_compute::governance::{GovernanceRegistry, Principal, DomainPattern};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create persistence with an in-memory backend
//!     let backend = InMemoryBackend::new();
//!     let persistence = GovernancePersistence::new(backend);
//!
//!     // Create and save a principal
//!     let principal = Principal::new_user("alice", "Alice", 100)
//!         .with_domain(DomainPattern::exact("project:alpha"));
//!     persistence.save_principal(&principal).await?;
//!
//!     // Load all principals
//!     let principals = persistence.load_all_principals().await?;
//!
//!     // Or save/load entire registry
//!     let registry = GovernanceRegistry::new();
//!     registry.register_principal(principal).await?;
//!     persistence.save_registry(&registry).await?;
//!
//!     let restored = persistence.load_registry().await?;
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use crate::storage::persistence::{PersistenceBackend, PersistenceError, TypedStore};

use super::authority::Delegation;
use super::principal::Principal;
use super::registry::{GovernanceRegistry, RegistryExport};

/// Namespace constants for storage
const PRINCIPALS_NAMESPACE: &str = "governance:principals";
const DELEGATIONS_NAMESPACE: &str = "governance:delegations";
const REGISTRY_NAMESPACE: &str = "governance:registry";
const REGISTRY_SNAPSHOT_KEY: &str = "snapshot";

/// Persistence layer for the governance system.
///
/// This struct provides methods to save and load governance data including
/// principals, delegations, and full registry snapshots.
pub struct GovernancePersistence<B: PersistenceBackend> {
    /// Store for principal data
    principals_store: TypedStore<B>,
    /// Store for delegation data
    delegations_store: TypedStore<B>,
    /// Store for registry snapshots
    registry_store: TypedStore<B>,
}

impl<B: PersistenceBackend> GovernancePersistence<B> {
    /// Creates a new governance persistence layer with the given backend.
    ///
    /// # Arguments
    ///
    /// * `backend` - The persistence backend to use for storage
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// let backend = InMemoryBackend::new();
    /// let persistence = GovernancePersistence::new(backend);
    /// ```
    pub fn new(backend: B) -> Self {
        let backend = Arc::new(backend);
        Self {
            principals_store: TypedStore::from_arc(Arc::clone(&backend), PRINCIPALS_NAMESPACE),
            delegations_store: TypedStore::from_arc(Arc::clone(&backend), DELEGATIONS_NAMESPACE),
            registry_store: TypedStore::from_arc(backend, REGISTRY_NAMESPACE),
        }
    }

    /// Creates a new governance persistence layer from an Arc'd backend.
    ///
    /// This is useful when sharing a backend between multiple persistence layers.
    pub fn from_arc(backend: Arc<B>) -> Self {
        Self {
            principals_store: TypedStore::from_arc(Arc::clone(&backend), PRINCIPALS_NAMESPACE),
            delegations_store: TypedStore::from_arc(Arc::clone(&backend), DELEGATIONS_NAMESPACE),
            registry_store: TypedStore::from_arc(backend, REGISTRY_NAMESPACE),
        }
    }

    // =========================================================================
    // Principal Operations
    // =========================================================================

    /// Saves a principal to persistent storage.
    ///
    /// If a principal with the same ID already exists, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `principal` - The principal to save
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success or a `PersistenceError` on failure.
    pub async fn save_principal(&self, principal: &Principal) -> Result<(), PersistenceError> {
        self.principals_store.put(&principal.id, principal).await
    }

    /// Loads all principals from persistent storage.
    ///
    /// # Returns
    ///
    /// Returns a vector of all stored principals, or an empty vector if none exist.
    pub async fn load_all_principals(&self) -> Result<Vec<Principal>, PersistenceError> {
        let items = self.principals_store.get_all::<Principal>().await?;
        Ok(items.into_iter().map(|(_, p)| p).collect())
    }

    /// Loads a specific principal by ID.
    ///
    /// # Arguments
    ///
    /// * `id` - The principal ID to load
    ///
    /// # Returns
    ///
    /// Returns `Some(Principal)` if found, `None` if not found.
    pub async fn load_principal(&self, id: &str) -> Result<Option<Principal>, PersistenceError> {
        self.principals_store.get(id).await
    }

    /// Deletes a principal from persistent storage.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the principal to delete
    ///
    /// # Returns
    ///
    /// Returns `true` if the principal was deleted, `false` if it didn't exist.
    pub async fn delete_principal(&self, id: &str) -> Result<bool, PersistenceError> {
        self.principals_store.delete(id).await
    }

    /// Checks if a principal exists in storage.
    ///
    /// # Arguments
    ///
    /// * `id` - The principal ID to check
    pub async fn principal_exists(&self, id: &str) -> Result<bool, PersistenceError> {
        self.principals_store.exists(id).await
    }

    /// Lists all principal IDs in storage.
    pub async fn list_principal_ids(&self) -> Result<Vec<String>, PersistenceError> {
        self.principals_store.list_keys(None).await
    }

    // =========================================================================
    // Delegation Operations
    // =========================================================================

    /// Saves a delegation to persistent storage.
    ///
    /// If a delegation with the same ID already exists, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `delegation` - The delegation to save
    pub async fn save_delegation(&self, delegation: &Delegation) -> Result<(), PersistenceError> {
        self.delegations_store.put(&delegation.id, delegation).await
    }

    /// Loads all delegations from persistent storage.
    ///
    /// # Returns
    ///
    /// Returns a vector of all stored delegations, or an empty vector if none exist.
    pub async fn load_all_delegations(&self) -> Result<Vec<Delegation>, PersistenceError> {
        let items = self.delegations_store.get_all::<Delegation>().await?;
        Ok(items.into_iter().map(|(_, d)| d).collect())
    }

    /// Loads a specific delegation by ID.
    ///
    /// # Arguments
    ///
    /// * `id` - The delegation ID to load
    ///
    /// # Returns
    ///
    /// Returns `Some(Delegation)` if found, `None` if not found.
    pub async fn load_delegation(&self, id: &str) -> Result<Option<Delegation>, PersistenceError> {
        self.delegations_store.get(id).await
    }

    /// Deletes a delegation from persistent storage.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the delegation to delete
    ///
    /// # Returns
    ///
    /// Returns `true` if the delegation was deleted, `false` if it didn't exist.
    pub async fn delete_delegation(&self, id: &str) -> Result<bool, PersistenceError> {
        self.delegations_store.delete(id).await
    }

    /// Checks if a delegation exists in storage.
    ///
    /// # Arguments
    ///
    /// * `id` - The delegation ID to check
    pub async fn delegation_exists(&self, id: &str) -> Result<bool, PersistenceError> {
        self.delegations_store.exists(id).await
    }

    /// Lists all delegation IDs in storage.
    pub async fn list_delegation_ids(&self) -> Result<Vec<String>, PersistenceError> {
        self.delegations_store.list_keys(None).await
    }

    /// Loads delegations granted by a specific principal.
    ///
    /// # Arguments
    ///
    /// * `principal_id` - The ID of the granting principal
    pub async fn load_delegations_from(
        &self,
        principal_id: &str,
    ) -> Result<Vec<Delegation>, PersistenceError> {
        let all = self.load_all_delegations().await?;
        Ok(all.into_iter().filter(|d| d.from == principal_id).collect())
    }

    /// Loads delegations received by a specific principal.
    ///
    /// # Arguments
    ///
    /// * `principal_id` - The ID of the receiving principal
    pub async fn load_delegations_to(
        &self,
        principal_id: &str,
    ) -> Result<Vec<Delegation>, PersistenceError> {
        let all = self.load_all_delegations().await?;
        Ok(all.into_iter().filter(|d| d.to == principal_id).collect())
    }

    // =========================================================================
    // Registry Operations
    // =========================================================================

    /// Saves the entire registry state as a snapshot.
    ///
    /// This exports all principals and delegations from the registry and stores
    /// them as a single snapshot. This is useful for backup/restore scenarios.
    ///
    /// # Arguments
    ///
    /// * `registry` - The governance registry to save
    pub async fn save_registry(
        &self,
        registry: &GovernanceRegistry,
    ) -> Result<(), PersistenceError> {
        let export = registry.export().await;

        // Save as a single snapshot
        self.registry_store
            .put(REGISTRY_SNAPSHOT_KEY, &export)
            .await?;

        // Also save individual items for incremental access
        for principal in &export.principals {
            self.save_principal(principal).await?;
        }
        for delegation in &export.delegations {
            self.save_delegation(delegation).await?;
        }

        Ok(())
    }

    /// Loads and reconstructs the registry from persistent storage.
    ///
    /// This creates a new `GovernanceRegistry` and populates it with all
    /// stored principals and delegations.
    ///
    /// # Returns
    ///
    /// Returns a fully reconstructed `GovernanceRegistry`.
    pub async fn load_registry(&self) -> Result<GovernanceRegistry, PersistenceError> {
        // First try to load from snapshot
        if let Some(export) = self
            .registry_store
            .get::<RegistryExport>(REGISTRY_SNAPSHOT_KEY)
            .await?
        {
            let registry = GovernanceRegistry::new();
            registry.import(export).await.map_err(|e| {
                PersistenceError::Database(format!("Failed to import registry: {}", e))
            })?;
            return Ok(registry);
        }

        // Fall back to loading individual items
        let principals = self.load_all_principals().await?;
        let delegations = self.load_all_delegations().await?;

        let export = RegistryExport {
            principals,
            delegations,
        };

        let registry = GovernanceRegistry::new();
        registry
            .import(export)
            .await
            .map_err(|e| PersistenceError::Database(format!("Failed to import registry: {}", e)))?;

        Ok(registry)
    }

    /// Clears all governance data from storage.
    ///
    /// This removes all principals, delegations, and registry snapshots.
    ///
    /// # Returns
    ///
    /// Returns the total number of items deleted.
    pub async fn clear_all(&self) -> Result<u64, PersistenceError> {
        let mut total = 0u64;
        total += self.principals_store.clear().await?;
        total += self.delegations_store.clear().await?;
        total += self.registry_store.clear().await?;
        Ok(total)
    }

    /// Gets statistics about stored governance data.
    pub async fn stats(&self) -> Result<GovernanceStorageStats, PersistenceError> {
        let principal_ids = self.principals_store.list_keys(None).await?;
        let delegation_ids = self.delegations_store.list_keys(None).await?;
        let has_snapshot = self.registry_store.exists(REGISTRY_SNAPSHOT_KEY).await?;

        Ok(GovernanceStorageStats {
            principal_count: principal_ids.len(),
            delegation_count: delegation_ids.len(),
            has_registry_snapshot: has_snapshot,
        })
    }
}

impl<B: PersistenceBackend> Clone for GovernancePersistence<B> {
    fn clone(&self) -> Self {
        Self {
            principals_store: self.principals_store.clone(),
            delegations_store: self.delegations_store.clone(),
            registry_store: self.registry_store.clone(),
        }
    }
}

/// Statistics about governance storage.
#[derive(Debug, Clone)]
pub struct GovernanceStorageStats {
    /// Number of stored principals
    pub principal_count: usize,
    /// Number of stored delegations
    pub delegation_count: usize,
    /// Whether a registry snapshot exists
    pub has_registry_snapshot: bool,
}

/// In-memory persistence backend for testing.
///
/// This backend stores data in memory and is useful for testing without
/// requiring external storage systems.
pub struct InMemoryBackend {
    data: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>>,
}

impl InMemoryBackend {
    /// Creates a new in-memory backend.
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    fn make_key(namespace: &str, key: &str) -> String {
        format!("{}:{}", namespace, key)
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl PersistenceBackend for InMemoryBackend {
    async fn put(&self, namespace: &str, key: &str, value: &[u8]) -> Result<(), PersistenceError> {
        let full_key = Self::make_key(namespace, key);
        self.data.write().await.insert(full_key, value.to_vec());
        Ok(())
    }

    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, PersistenceError> {
        let full_key = Self::make_key(namespace, key);
        Ok(self.data.read().await.get(&full_key).cloned())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
        let full_key = Self::make_key(namespace, key);
        Ok(self.data.write().await.remove(&full_key).is_some())
    }

    async fn list_keys(
        &self,
        namespace: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, PersistenceError> {
        let namespace_prefix = format!("{}:", namespace);
        let data = self.data.read().await;

        let keys: Vec<String> = data
            .keys()
            .filter_map(|k| {
                if k.starts_with(&namespace_prefix) {
                    let key = k.strip_prefix(&namespace_prefix).unwrap().to_string();
                    if let Some(p) = prefix {
                        if key.starts_with(p) {
                            Some(key)
                        } else {
                            None
                        }
                    } else {
                        Some(key)
                    }
                } else {
                    None
                }
            })
            .collect();

        Ok(keys)
    }

    async fn exists(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
        let full_key = Self::make_key(namespace, key);
        Ok(self.data.read().await.contains_key(&full_key))
    }

    async fn batch_put(
        &self,
        namespace: &str,
        items: &[(&str, &[u8])],
    ) -> Result<(), PersistenceError> {
        let mut data = self.data.write().await;
        for (key, value) in items {
            let full_key = Self::make_key(namespace, key);
            data.insert(full_key, value.to_vec());
        }
        Ok(())
    }

    async fn clear_namespace(&self, namespace: &str) -> Result<u64, PersistenceError> {
        let namespace_prefix = format!("{}:", namespace);
        let mut data = self.data.write().await;

        let keys_to_remove: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(&namespace_prefix))
            .cloned()
            .collect();

        let count = keys_to_remove.len() as u64;
        for key in keys_to_remove {
            data.remove(&key);
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::authority::DelegationConstraints;
    use crate::governance::principal::DomainPattern;

    #[tokio::test]
    async fn test_save_load_principal() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        let principal = Principal::new_user("alice", "Alice Smith", 100)
            .with_domain(DomainPattern::exact("project:alpha"))
            .with_email("alice@example.com");

        // Save
        persistence.save_principal(&principal).await.unwrap();

        // Load
        let loaded = persistence.load_principal("alice").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, "alice");
        assert_eq!(loaded.name, "Alice Smith");
        assert_eq!(loaded.priority, 100);
    }

    #[tokio::test]
    async fn test_save_load_all_principals() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        let p1 = Principal::new_user("alice", "Alice", 100);
        let p2 = Principal::new_user("bob", "Bob", 200);
        let p3 = Principal::new_user("charlie", "Charlie", 300);

        persistence.save_principal(&p1).await.unwrap();
        persistence.save_principal(&p2).await.unwrap();
        persistence.save_principal(&p3).await.unwrap();

        let all = persistence.load_all_principals().await.unwrap();
        assert_eq!(all.len(), 3);

        let ids: Vec<_> = all.iter().map(|p| p.id.clone()).collect();
        assert!(ids.contains(&"alice".to_string()));
        assert!(ids.contains(&"bob".to_string()));
        assert!(ids.contains(&"charlie".to_string()));
    }

    #[tokio::test]
    async fn test_delete_principal() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        let principal = Principal::new_user("alice", "Alice", 100);
        persistence.save_principal(&principal).await.unwrap();

        assert!(persistence.principal_exists("alice").await.unwrap());

        let deleted = persistence.delete_principal("alice").await.unwrap();
        assert!(deleted);

        assert!(!persistence.principal_exists("alice").await.unwrap());
    }

    #[tokio::test]
    async fn test_save_load_delegation() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        let delegation = Delegation::new(
            "del-1",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        )
        .with_constraints(DelegationConstraints::default().with_max_priority(80));

        // Save
        persistence.save_delegation(&delegation).await.unwrap();

        // Load
        let loaded = persistence.load_delegation("del-1").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, "del-1");
        assert_eq!(loaded.from, "manager");
        assert_eq!(loaded.to, "employee");
        assert_eq!(loaded.constraints.max_priority, Some(80));
    }

    #[tokio::test]
    async fn test_save_load_all_delegations() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        let d1 = Delegation::new("del-1", "ceo", "vp", vec![DomainPattern::global()]);
        let d2 = Delegation::new(
            "del-2",
            "vp",
            "manager",
            vec![DomainPattern::prefix("dept:")],
        );

        persistence.save_delegation(&d1).await.unwrap();
        persistence.save_delegation(&d2).await.unwrap();

        let all = persistence.load_all_delegations().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn test_load_delegations_from() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        let d1 = Delegation::new("del-1", "manager", "emp1", vec![DomainPattern::global()]);
        let d2 = Delegation::new("del-2", "manager", "emp2", vec![DomainPattern::global()]);
        let d3 = Delegation::new(
            "del-3",
            "director",
            "manager",
            vec![DomainPattern::global()],
        );

        persistence.save_delegation(&d1).await.unwrap();
        persistence.save_delegation(&d2).await.unwrap();
        persistence.save_delegation(&d3).await.unwrap();

        let from_manager = persistence.load_delegations_from("manager").await.unwrap();
        assert_eq!(from_manager.len(), 2);

        let ids: Vec<_> = from_manager.iter().map(|d| d.id.clone()).collect();
        assert!(ids.contains(&"del-1".to_string()));
        assert!(ids.contains(&"del-2".to_string()));
    }

    #[tokio::test]
    async fn test_load_delegations_to() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        let d1 = Delegation::new("del-1", "ceo", "manager", vec![DomainPattern::global()]);
        let d2 = Delegation::new("del-2", "vp", "manager", vec![DomainPattern::global()]);
        let d3 = Delegation::new(
            "del-3",
            "manager",
            "employee",
            vec![DomainPattern::global()],
        );

        persistence.save_delegation(&d1).await.unwrap();
        persistence.save_delegation(&d2).await.unwrap();
        persistence.save_delegation(&d3).await.unwrap();

        let to_manager = persistence.load_delegations_to("manager").await.unwrap();
        assert_eq!(to_manager.len(), 2);
    }

    #[tokio::test]
    async fn test_save_load_registry() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        // Create registry with data
        let registry = GovernanceRegistry::new();

        let ceo = Principal::new_user("ceo", "CEO", 1000).with_domain(DomainPattern::global());
        let manager = Principal::new_user("manager", "Manager", 500)
            .with_domain(DomainPattern::prefix("dept:"));
        let employee = Principal::new_user("employee", "Employee", 100);

        registry.register_principal(ceo).await.unwrap();
        registry.register_principal(manager).await.unwrap();
        registry.register_principal(employee).await.unwrap();

        let delegation = Delegation::new(
            "del-1",
            "manager",
            "employee",
            vec![DomainPattern::exact("dept:eng")],
        );
        registry.create_delegation(delegation).await.unwrap();

        // Save registry
        persistence.save_registry(&registry).await.unwrap();

        // Load into new registry
        let restored = persistence.load_registry().await.unwrap();

        // Verify data
        assert_eq!(restored.principal_count().await, 3);
        assert_eq!(restored.list_delegations().await.len(), 1);

        // Verify authority still works
        assert!(restored.has_authority(&"ceo".to_string(), "anything").await);
        assert!(
            restored
                .has_authority(&"employee".to_string(), "dept:eng")
                .await
        );
    }

    #[tokio::test]
    async fn test_registry_reconstruction_from_individual_items() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        // Save individual items without snapshot
        let ceo = Principal::new_user("ceo", "CEO", 1000).with_domain(DomainPattern::global());
        let manager = Principal::new_user("manager", "Manager", 500)
            .with_domain(DomainPattern::prefix("dept:"));

        persistence.save_principal(&ceo).await.unwrap();
        persistence.save_principal(&manager).await.unwrap();

        let delegation = Delegation::new("del-1", "ceo", "manager", vec![DomainPattern::global()]);
        persistence.save_delegation(&delegation).await.unwrap();

        // Load registry (should reconstruct from individual items)
        let restored = persistence.load_registry().await.unwrap();

        assert_eq!(restored.principal_count().await, 2);
        assert_eq!(restored.list_delegations().await.len(), 1);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        // Add some data
        let p1 = Principal::new_user("alice", "Alice", 100);
        let d1 = Delegation::new("del-1", "alice", "bob", vec![DomainPattern::global()]);

        persistence.save_principal(&p1).await.unwrap();
        persistence.save_delegation(&d1).await.unwrap();

        // Clear
        let deleted = persistence.clear_all().await.unwrap();
        assert!(deleted >= 2);

        // Verify empty
        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.principal_count, 0);
        assert_eq!(stats.delegation_count, 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        // Initial stats
        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.principal_count, 0);
        assert_eq!(stats.delegation_count, 0);
        assert!(!stats.has_registry_snapshot);

        // Add data
        let p1 = Principal::new_user("alice", "Alice", 100);
        let p2 = Principal::new_user("bob", "Bob", 200);
        let d1 = Delegation::new("del-1", "alice", "bob", vec![DomainPattern::global()]);

        persistence.save_principal(&p1).await.unwrap();
        persistence.save_principal(&p2).await.unwrap();
        persistence.save_delegation(&d1).await.unwrap();

        // Check stats
        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.principal_count, 2);
        assert_eq!(stats.delegation_count, 1);
        assert!(!stats.has_registry_snapshot);

        // Save registry to create snapshot
        let registry = GovernanceRegistry::new();
        registry.register_principal(p1).await.unwrap();
        registry.register_principal(p2).await.unwrap();
        persistence.save_registry(&registry).await.unwrap();

        let stats = persistence.stats().await.unwrap();
        assert!(stats.has_registry_snapshot);
    }

    #[tokio::test]
    async fn test_persistence_clone() {
        let backend = InMemoryBackend::new();
        let persistence = GovernancePersistence::new(backend);

        // Save via original
        let principal = Principal::new_user("alice", "Alice", 100);
        persistence.save_principal(&principal).await.unwrap();

        // Clone and load via clone
        let cloned = persistence.clone();
        let loaded = cloned.load_principal("alice").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "Alice");
    }
}
