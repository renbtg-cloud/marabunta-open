// Marabunta - Licensed under the MIT License.
//! Tenant Persistence
//!
//! This module provides persistent storage for tenant data including
//! tenants, memberships, and isolation configurations.

use std::collections::HashMap;
use std::sync::Arc;

use crate::storage::persistence::{PersistenceBackend, PersistenceError, TypedStore};

use super::isolation::TenantIsolation;
use super::registry::{RegistryExport, TenantRegistry};
use super::types::{Tenant, TenantId, TenantMembership};

/// Namespace constants for storage organization.
const NAMESPACE_TENANTS: &str = "tenancy:tenants";
const NAMESPACE_MEMBERSHIPS: &str = "tenancy:memberships";
const NAMESPACE_ISOLATION: &str = "tenancy:isolation";
const NAMESPACE_REGISTRY: &str = "tenancy:registry";
const REGISTRY_SNAPSHOT_KEY: &str = "snapshot";

/// Persistence layer for the tenancy system.
pub struct TenancyPersistence<B: PersistenceBackend> {
    /// Store for tenant data
    tenants_store: TypedStore<B>,
    /// Store for membership data
    memberships_store: TypedStore<B>,
    /// Store for isolation configurations
    isolation_store: TypedStore<B>,
    /// Store for registry snapshots
    registry_store: TypedStore<B>,
}

impl<B: PersistenceBackend> TenancyPersistence<B> {
    /// Creates a new persistence layer with the given backend.
    pub fn new(backend: B) -> Self {
        let backend = Arc::new(backend);
        Self {
            tenants_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_TENANTS),
            memberships_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_MEMBERSHIPS),
            isolation_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_ISOLATION),
            registry_store: TypedStore::from_arc(backend, NAMESPACE_REGISTRY),
        }
    }

    /// Creates a persistence layer from an Arc'd backend.
    pub fn from_arc(backend: Arc<B>) -> Self {
        Self {
            tenants_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_TENANTS),
            memberships_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_MEMBERSHIPS),
            isolation_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_ISOLATION),
            registry_store: TypedStore::from_arc(backend, NAMESPACE_REGISTRY),
        }
    }

    // =========================================================================
    // Tenant Operations
    // =========================================================================

    /// Saves a tenant.
    pub async fn save_tenant(&self, tenant: &Tenant) -> Result<(), PersistenceError> {
        self.tenants_store
            .put(&tenant.id.0.to_string(), tenant)
            .await
    }

    /// Loads a tenant by ID.
    pub async fn load_tenant(&self, id: &TenantId) -> Result<Option<Tenant>, PersistenceError> {
        self.tenants_store.get(&id.0.to_string()).await
    }

    /// Loads all tenants.
    pub async fn load_all_tenants(&self) -> Result<Vec<Tenant>, PersistenceError> {
        let items = self.tenants_store.get_all::<Tenant>().await?;
        Ok(items.into_iter().map(|(_, t)| t).collect())
    }

    /// Deletes a tenant.
    pub async fn delete_tenant(&self, id: &TenantId) -> Result<bool, PersistenceError> {
        self.tenants_store.delete(&id.0.to_string()).await
    }

    /// Saves multiple tenants.
    pub async fn save_tenants_batch(&self, tenants: &[&Tenant]) -> Result<(), PersistenceError> {
        let items: Vec<(&str, &Tenant)> = tenants
            .iter()
            .map(|t| {
                // Create a temporary string that will live long enough
                let id_str = t.id.0.to_string();
                // This is a workaround - in production, use owned types
                (id_str.leak() as &str, *t)
            })
            .collect();
        self.tenants_store.batch_put(&items).await
    }

    // =========================================================================
    // Membership Operations
    // =========================================================================

    /// Saves user memberships.
    pub async fn save_user_memberships(
        &self,
        user_id: &str,
        memberships: &[TenantMembership],
    ) -> Result<(), PersistenceError> {
        self.memberships_store.put(user_id, &memberships).await
    }

    /// Loads user memberships.
    pub async fn load_user_memberships(
        &self,
        user_id: &str,
    ) -> Result<Option<Vec<TenantMembership>>, PersistenceError> {
        self.memberships_store.get(user_id).await
    }

    /// Loads all memberships.
    pub async fn load_all_memberships(
        &self,
    ) -> Result<HashMap<String, Vec<TenantMembership>>, PersistenceError> {
        let items = self
            .memberships_store
            .get_all::<Vec<TenantMembership>>()
            .await?;
        Ok(items.into_iter().collect())
    }

    /// Deletes user memberships.
    pub async fn delete_user_memberships(&self, user_id: &str) -> Result<bool, PersistenceError> {
        self.memberships_store.delete(user_id).await
    }

    // =========================================================================
    // Isolation Operations
    // =========================================================================

    /// Saves tenant isolation configuration.
    pub async fn save_isolation(
        &self,
        isolation: &TenantIsolation,
    ) -> Result<(), PersistenceError> {
        self.isolation_store
            .put(&isolation.tenant_id.0.to_string(), isolation)
            .await
    }

    /// Loads tenant isolation configuration.
    pub async fn load_isolation(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantIsolation>, PersistenceError> {
        self.isolation_store.get(&tenant_id.0.to_string()).await
    }

    /// Loads all isolation configurations.
    pub async fn load_all_isolations(&self) -> Result<Vec<TenantIsolation>, PersistenceError> {
        let items = self.isolation_store.get_all::<TenantIsolation>().await?;
        Ok(items.into_iter().map(|(_, i)| i).collect())
    }

    /// Deletes tenant isolation configuration.
    pub async fn delete_isolation(&self, tenant_id: &TenantId) -> Result<bool, PersistenceError> {
        self.isolation_store.delete(&tenant_id.0.to_string()).await
    }

    // =========================================================================
    // Registry Operations
    // =========================================================================

    /// Saves the entire registry state.
    pub async fn save_registry(&self, registry: &TenantRegistry) -> Result<(), PersistenceError> {
        let export = registry.export().await;

        // Save as snapshot
        self.registry_store
            .put(REGISTRY_SNAPSHOT_KEY, &export)
            .await?;

        // Also save individual items
        for tenant in &export.tenants {
            self.save_tenant(tenant).await?;
        }
        for (user_id, memberships) in &export.memberships {
            self.save_user_memberships(user_id, memberships).await?;
        }
        for isolation in &export.isolations {
            self.save_isolation(isolation).await?;
        }

        Ok(())
    }

    /// Loads and reconstructs the registry from persistence.
    pub async fn load_registry(&self) -> Result<TenantRegistry, PersistenceError> {
        // Try snapshot first
        if let Some(export) = self
            .registry_store
            .get::<RegistryExport>(REGISTRY_SNAPSHOT_KEY)
            .await?
        {
            let registry = TenantRegistry::new();
            registry.import(export).await.map_err(|e| {
                PersistenceError::Database(format!("Failed to import registry: {}", e))
            })?;
            return Ok(registry);
        }

        // Fall back to loading individual items
        let tenants = self.load_all_tenants().await?;
        let memberships = self.load_all_memberships().await?;
        let isolations = self.load_all_isolations().await?;

        let export = RegistryExport {
            tenants,
            memberships,
            isolations,
        };

        let registry = TenantRegistry::new();
        registry
            .import(export)
            .await
            .map_err(|e| PersistenceError::Database(format!("Failed to import registry: {}", e)))?;

        Ok(registry)
    }

    /// Clears all tenancy data.
    pub async fn clear_all(&self) -> Result<u64, PersistenceError> {
        let mut total = 0u64;
        total += self.tenants_store.clear().await?;
        total += self.memberships_store.clear().await?;
        total += self.isolation_store.clear().await?;
        total += self.registry_store.clear().await?;
        Ok(total)
    }

    /// Gets storage statistics.
    pub async fn stats(&self) -> Result<TenancyStorageStats, PersistenceError> {
        let tenant_count = self.tenants_store.list_keys(None).await?.len();
        let membership_count = self.memberships_store.list_keys(None).await?.len();
        let isolation_count = self.isolation_store.list_keys(None).await?.len();
        let has_snapshot = self.registry_store.exists(REGISTRY_SNAPSHOT_KEY).await?;

        Ok(TenancyStorageStats {
            tenant_count,
            membership_count,
            isolation_count,
            has_registry_snapshot: has_snapshot,
        })
    }
}

impl<B: PersistenceBackend> Clone for TenancyPersistence<B> {
    fn clone(&self) -> Self {
        Self {
            tenants_store: self.tenants_store.clone(),
            memberships_store: self.memberships_store.clone(),
            isolation_store: self.isolation_store.clone(),
            registry_store: self.registry_store.clone(),
        }
    }
}

/// Statistics about tenancy storage.
#[derive(Debug, Clone)]
pub struct TenancyStorageStats {
    /// Number of stored tenants
    pub tenant_count: usize,
    /// Number of users with memberships
    pub membership_count: usize,
    /// Number of isolation configurations
    pub isolation_count: usize,
    /// Whether a registry snapshot exists
    pub has_registry_snapshot: bool,
}

/// Persistent tenant registry wrapper.
///
/// This wraps a `TenantRegistry` and automatically persists changes.
pub struct PersistentTenantRegistry<B: PersistenceBackend + 'static> {
    /// The underlying registry
    registry: TenantRegistry,
    /// The persistence layer
    persistence: TenancyPersistence<B>,
}

impl<B: PersistenceBackend + 'static> PersistentTenantRegistry<B> {
    /// Creates a new persistent registry.
    pub async fn new(backend: B) -> Result<Self, PersistenceError> {
        let persistence = TenancyPersistence::new(backend);

        // Try to load existing data
        let registry = persistence
            .load_registry()
            .await
            .unwrap_or_else(|_| TenantRegistry::new());

        Ok(Self {
            registry,
            persistence,
        })
    }

    /// Gets a reference to the underlying registry.
    pub fn registry(&self) -> &TenantRegistry {
        &self.registry
    }

    /// Saves the current state.
    pub async fn save(&self) -> Result<(), PersistenceError> {
        self.persistence.save_registry(&self.registry).await
    }

    /// Reloads from persistence.
    pub async fn reload(&mut self) -> Result<(), PersistenceError> {
        self.registry = self.persistence.load_registry().await?;
        Ok(())
    }

    /// Gets storage statistics.
    pub async fn stats(&self) -> Result<TenancyStorageStats, PersistenceError> {
        self.persistence.stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::persistence::InMemoryBackend;
    use crate::tenancy::types::{TenantLevel, TenantRole};

    #[tokio::test]
    async fn test_save_load_tenant() {
        let backend = InMemoryBackend::new();
        let persistence = TenancyPersistence::new(backend);

        let tenant = Tenant::new_organization("Test Org", "test-org");
        persistence.save_tenant(&tenant).await.unwrap();

        let loaded = persistence.load_tenant(&tenant.id).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "Test Org");
    }

    #[tokio::test]
    async fn test_save_load_memberships() {
        let backend = InMemoryBackend::new();
        let persistence = TenancyPersistence::new(backend);

        let tenant_id = TenantId::new();
        let memberships = vec![TenantMembership {
            tenant_id,
            user_id: "user@example.com".to_string(),
            role: TenantRole::Member,
            joined_at: chrono::Utc::now(),
            expires_at: None,
        }];

        persistence
            .save_user_memberships("user@example.com", &memberships)
            .await
            .unwrap();

        let loaded = persistence
            .load_user_memberships("user@example.com")
            .await
            .unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_save_load_isolation() {
        let backend = InMemoryBackend::new();
        let persistence = TenancyPersistence::new(backend);

        let tenant_id = TenantId::new();
        let isolation = TenantIsolation::new(tenant_id);

        persistence.save_isolation(&isolation).await.unwrap();

        let loaded = persistence.load_isolation(&tenant_id).await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().tenant_id, tenant_id);
    }

    #[tokio::test]
    async fn test_save_load_registry() {
        let backend = InMemoryBackend::new();
        let persistence = TenancyPersistence::new(backend);

        let registry = TenantRegistry::new();

        let org = Tenant::new_organization("Org", "org");
        let org_id = registry.create_tenant(org).await.unwrap();
        registry
            .add_member(&org_id, "user@example.com", TenantRole::Member)
            .await
            .unwrap();

        persistence.save_registry(&registry).await.unwrap();

        let loaded = persistence.load_registry().await.unwrap();
        assert_eq!(loaded.count().await, 1);
    }

    #[tokio::test]
    async fn test_stats() {
        let backend = InMemoryBackend::new();
        let persistence = TenancyPersistence::new(backend);

        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.tenant_count, 0);

        let tenant = Tenant::new_organization("Test", "test");
        persistence.save_tenant(&tenant).await.unwrap();

        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.tenant_count, 1);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let backend = InMemoryBackend::new();
        let persistence = TenancyPersistence::new(backend);

        let tenant = Tenant::new_organization("Test", "test");
        persistence.save_tenant(&tenant).await.unwrap();

        let cleared = persistence.clear_all().await.unwrap();
        assert!(cleared >= 1);

        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.tenant_count, 0);
    }
}
