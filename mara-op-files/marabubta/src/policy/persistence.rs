// Marabunta - Licensed under the MIT License.
//! Policy Persistence Layer
//!
//! This module provides persistence for the policy system, including
//! saving and loading policies and complete engine state.
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::policy::persistence::PolicyPersistence;
//! use marabunta_compute::policy::{Policy, PolicyEngine, PolicyCondition, PolicyEffect, NodeSelector};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create persistence with an in-memory backend
//!     let backend = InMemoryBackend::new();
//!     let persistence = PolicyPersistence::new(backend);
//!
//!     // Create and save a policy
//!     let policy = Policy::new("gpu-preference", "GPU Preference")
//!         .with_condition(PolicyCondition::Always)
//!         .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.8));
//!     persistence.save_policy(&policy).await?;
//!
//!     // Load all policies
//!     let policies = persistence.load_all_policies().await?;
//!
//!     // Or save/load entire engine
//!     let mut engine = PolicyEngine::new();
//!     engine.register(policy)?;
//!     persistence.save_engine(&engine).await?;
//!
//!     let restored = persistence.load_engine().await?;
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::storage::persistence::{PersistenceBackend, PersistenceError, TypedStore};

use super::engine::PolicyEngine;
use super::ir::Policy;

/// Namespace constants for storage
const POLICIES_NAMESPACE: &str = "policy:policies";
const ENGINE_NAMESPACE: &str = "policy:engine";
const ENGINE_SNAPSHOT_KEY: &str = "snapshot";

/// Persistence layer for the policy system.
///
/// This struct provides methods to save and load policy data including
/// individual policies and full engine snapshots.
pub struct PolicyPersistence<B: PersistenceBackend> {
    /// Store for policy data
    policies_store: TypedStore<B>,
    /// Store for engine snapshots
    engine_store: TypedStore<B>,
}

impl<B: PersistenceBackend> PolicyPersistence<B> {
    /// Creates a new policy persistence layer with the given backend.
    ///
    /// # Arguments
    ///
    /// * `backend` - The persistence backend to use for storage
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// let backend = InMemoryBackend::new();
    /// let persistence = PolicyPersistence::new(backend);
    /// ```
    pub fn new(backend: B) -> Self {
        let backend = Arc::new(backend);
        Self {
            policies_store: TypedStore::from_arc(Arc::clone(&backend), POLICIES_NAMESPACE),
            engine_store: TypedStore::from_arc(backend, ENGINE_NAMESPACE),
        }
    }

    /// Creates a new policy persistence layer from an Arc'd backend.
    ///
    /// This is useful when sharing a backend between multiple persistence layers.
    pub fn from_arc(backend: Arc<B>) -> Self {
        Self {
            policies_store: TypedStore::from_arc(Arc::clone(&backend), POLICIES_NAMESPACE),
            engine_store: TypedStore::from_arc(backend, ENGINE_NAMESPACE),
        }
    }

    // =========================================================================
    // Policy Operations
    // =========================================================================

    /// Saves a policy to persistent storage.
    ///
    /// If a policy with the same ID already exists, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `policy` - The policy to save
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success or a `PersistenceError` on failure.
    pub async fn save_policy(&self, policy: &Policy) -> Result<(), PersistenceError> {
        self.policies_store.put(&policy.id, policy).await
    }

    /// Loads all policies from persistent storage.
    ///
    /// # Returns
    ///
    /// Returns a vector of all stored policies, or an empty vector if none exist.
    pub async fn load_all_policies(&self) -> Result<Vec<Policy>, PersistenceError> {
        let items = self.policies_store.get_all::<Policy>().await?;
        Ok(items.into_iter().map(|(_, p)| p).collect())
    }

    /// Loads a specific policy by ID.
    ///
    /// # Arguments
    ///
    /// * `id` - The policy ID to load
    ///
    /// # Returns
    ///
    /// Returns `Some(Policy)` if found, `None` if not found.
    pub async fn load_policy(&self, id: &str) -> Result<Option<Policy>, PersistenceError> {
        self.policies_store.get(id).await
    }

    /// Deletes a policy from persistent storage.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the policy to delete
    ///
    /// # Returns
    ///
    /// Returns `true` if the policy was deleted, `false` if it didn't exist.
    pub async fn delete_policy(&self, id: &str) -> Result<bool, PersistenceError> {
        self.policies_store.delete(id).await
    }

    /// Checks if a policy exists in storage.
    ///
    /// # Arguments
    ///
    /// * `id` - The policy ID to check
    pub async fn policy_exists(&self, id: &str) -> Result<bool, PersistenceError> {
        self.policies_store.exists(id).await
    }

    /// Lists all policy IDs in storage.
    pub async fn list_policy_ids(&self) -> Result<Vec<String>, PersistenceError> {
        self.policies_store.list_keys(None).await
    }

    /// Loads policies matching a governance author.
    ///
    /// # Arguments
    ///
    /// * `author` - The author to filter by
    pub async fn load_policies_by_author(
        &self,
        author: &str,
    ) -> Result<Vec<Policy>, PersistenceError> {
        let all = self.load_all_policies().await?;
        Ok(all
            .into_iter()
            .filter(|p| p.governance.author == author)
            .collect())
    }

    /// Loads policies for a specific domain.
    ///
    /// # Arguments
    ///
    /// * `domain` - The authority domain to filter by
    pub async fn load_policies_for_domain(
        &self,
        domain: &str,
    ) -> Result<Vec<Policy>, PersistenceError> {
        let all = self.load_all_policies().await?;
        Ok(all
            .into_iter()
            .filter(|p| {
                p.governance.authority_domain == "*"
                    || p.governance.authority_domain == domain
                    || domain.starts_with(&p.governance.authority_domain)
            })
            .collect())
    }

    /// Loads only enabled policies.
    pub async fn load_enabled_policies(&self) -> Result<Vec<Policy>, PersistenceError> {
        let all = self.load_all_policies().await?;
        Ok(all.into_iter().filter(|p| p.enabled).collect())
    }

    // =========================================================================
    // Engine Operations
    // =========================================================================

    /// Saves the entire engine state as a snapshot.
    ///
    /// This exports all policies from the engine and stores them as a
    /// single snapshot. This is useful for backup/restore scenarios.
    ///
    /// # Arguments
    ///
    /// * `engine` - The policy engine to save
    pub async fn save_engine(&self, engine: &PolicyEngine) -> Result<(), PersistenceError> {
        let policies: Vec<Policy> = engine.list().iter().map(|p| (*p).clone()).collect();
        let export = EngineExport { policies };

        // Save as a single snapshot
        self.engine_store.put(ENGINE_SNAPSHOT_KEY, &export).await?;

        // Also save individual items for incremental access
        for policy in &export.policies {
            self.save_policy(policy).await?;
        }

        Ok(())
    }

    /// Loads and reconstructs the engine from persistent storage.
    ///
    /// This creates a new `PolicyEngine` and registers all stored policies.
    ///
    /// # Returns
    ///
    /// Returns a fully reconstructed `PolicyEngine`.
    pub async fn load_engine(&self) -> Result<PolicyEngine, PersistenceError> {
        // First try to load from snapshot
        if let Some(export) = self
            .engine_store
            .get::<EngineExport>(ENGINE_SNAPSHOT_KEY)
            .await?
        {
            let mut engine = PolicyEngine::new();
            for policy in export.policies {
                engine.register(policy).map_err(|e| {
                    PersistenceError::Database(format!("Failed to register policy: {}", e))
                })?;
            }
            return Ok(engine);
        }

        // Fall back to loading individual items
        let policies = self.load_all_policies().await?;
        let mut engine = PolicyEngine::new();

        for policy in policies {
            engine.register(policy).map_err(|e| {
                PersistenceError::Database(format!("Failed to register policy: {}", e))
            })?;
        }

        Ok(engine)
    }

    /// Clears all policy data from storage.
    ///
    /// This removes all policies and engine snapshots.
    ///
    /// # Returns
    ///
    /// Returns the total number of items deleted.
    pub async fn clear_all(&self) -> Result<u64, PersistenceError> {
        let mut total = 0u64;
        total += self.policies_store.clear().await?;
        total += self.engine_store.clear().await?;
        Ok(total)
    }

    /// Gets statistics about stored policy data.
    pub async fn stats(&self) -> Result<PolicyStorageStats, PersistenceError> {
        let policy_ids = self.policies_store.list_keys(None).await?;
        let has_snapshot = self.engine_store.exists(ENGINE_SNAPSHOT_KEY).await?;

        // Count enabled policies
        let policies = self.load_all_policies().await?;
        let enabled_count = policies.iter().filter(|p| p.enabled).count();

        Ok(PolicyStorageStats {
            policy_count: policy_ids.len(),
            enabled_count,
            has_engine_snapshot: has_snapshot,
        })
    }
}

impl<B: PersistenceBackend> Clone for PolicyPersistence<B> {
    fn clone(&self) -> Self {
        Self {
            policies_store: self.policies_store.clone(),
            engine_store: self.engine_store.clone(),
        }
    }
}

/// Exported engine state for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineExport {
    /// All policies in the engine
    pub policies: Vec<Policy>,
}

/// Statistics about policy storage.
#[derive(Debug, Clone)]
pub struct PolicyStorageStats {
    /// Number of stored policies
    pub policy_count: usize,
    /// Number of enabled policies
    pub enabled_count: usize,
    /// Whether an engine snapshot exists
    pub has_engine_snapshot: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::persistence::InMemoryBackend;
    use crate::policy::{
        JobMatcher, NodeSelector, OverridePolicyRef, PolicyCondition, PolicyEffect,
        PolicyGovernance, ResourceMatcher, SubmitterMatcher, TagExpr,
    };

    fn create_test_policy(id: &str, name: &str) -> Policy {
        Policy::new(id, name)
            .with_condition(PolicyCondition::Always)
            .with_effect(PolicyEffect::prefer(NodeSelector::all(), 0.5))
    }

    #[tokio::test]
    async fn test_save_load_policy() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        let policy = Policy::new("gpu-pref", "GPU Preference")
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

        // Save
        persistence.save_policy(&policy).await.unwrap();

        // Load
        let loaded = persistence.load_policy("gpu-pref").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, "gpu-pref");
        assert_eq!(loaded.name, "GPU Preference");
        assert_eq!(loaded.governance.author, "platform-team");
        assert_eq!(loaded.governance.conflict_priority, 100);
    }

    #[tokio::test]
    async fn test_save_load_all_policies() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        let p1 = create_test_policy("policy-1", "Policy 1");
        let p2 = create_test_policy("policy-2", "Policy 2");
        let p3 = create_test_policy("policy-3", "Policy 3");

        persistence.save_policy(&p1).await.unwrap();
        persistence.save_policy(&p2).await.unwrap();
        persistence.save_policy(&p3).await.unwrap();

        let all = persistence.load_all_policies().await.unwrap();
        assert_eq!(all.len(), 3);

        let ids: Vec<_> = all.iter().map(|p| p.id.clone()).collect();
        assert!(ids.contains(&"policy-1".to_string()));
        assert!(ids.contains(&"policy-2".to_string()));
        assert!(ids.contains(&"policy-3".to_string()));
    }

    #[tokio::test]
    async fn test_delete_policy() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        let policy = create_test_policy("test-policy", "Test Policy");
        persistence.save_policy(&policy).await.unwrap();

        assert!(persistence.policy_exists("test-policy").await.unwrap());

        let deleted = persistence.delete_policy("test-policy").await.unwrap();
        assert!(deleted);

        assert!(!persistence.policy_exists("test-policy").await.unwrap());
    }

    #[tokio::test]
    async fn test_load_policies_by_author() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        let p1 = Policy::new("p1", "Policy 1")
            .with_condition(PolicyCondition::Always)
            .with_governance(PolicyGovernance::new("team-a"));
        let p2 = Policy::new("p2", "Policy 2")
            .with_condition(PolicyCondition::Always)
            .with_governance(PolicyGovernance::new("team-a"));
        let p3 = Policy::new("p3", "Policy 3")
            .with_condition(PolicyCondition::Always)
            .with_governance(PolicyGovernance::new("team-b"));

        persistence.save_policy(&p1).await.unwrap();
        persistence.save_policy(&p2).await.unwrap();
        persistence.save_policy(&p3).await.unwrap();

        let team_a = persistence.load_policies_by_author("team-a").await.unwrap();
        assert_eq!(team_a.len(), 2);

        let team_b = persistence.load_policies_by_author("team-b").await.unwrap();
        assert_eq!(team_b.len(), 1);
    }

    #[tokio::test]
    async fn test_load_policies_for_domain() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        let p1 = Policy::new("p1", "Global Policy")
            .with_condition(PolicyCondition::Always)
            .with_governance(PolicyGovernance::new("admin").with_domain("*"));
        let p2 = Policy::new("p2", "Compute Policy")
            .with_condition(PolicyCondition::Always)
            .with_governance(PolicyGovernance::new("platform").with_domain("compute"));
        let p3 = Policy::new("p3", "Storage Policy")
            .with_condition(PolicyCondition::Always)
            .with_governance(PolicyGovernance::new("platform").with_domain("storage"));

        persistence.save_policy(&p1).await.unwrap();
        persistence.save_policy(&p2).await.unwrap();
        persistence.save_policy(&p3).await.unwrap();

        // Global domain should match all
        let compute = persistence
            .load_policies_for_domain("compute")
            .await
            .unwrap();
        assert_eq!(compute.len(), 2); // p1 and p2

        let storage = persistence
            .load_policies_for_domain("storage")
            .await
            .unwrap();
        assert_eq!(storage.len(), 2); // p1 and p3

        let other = persistence.load_policies_for_domain("other").await.unwrap();
        assert_eq!(other.len(), 1); // only p1 (global)
    }

    #[tokio::test]
    async fn test_load_enabled_policies() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        let mut p1 = create_test_policy("p1", "Enabled Policy");
        p1.enabled = true;

        let mut p2 = create_test_policy("p2", "Disabled Policy");
        p2.enabled = false;

        let mut p3 = create_test_policy("p3", "Another Enabled");
        p3.enabled = true;

        persistence.save_policy(&p1).await.unwrap();
        persistence.save_policy(&p2).await.unwrap();
        persistence.save_policy(&p3).await.unwrap();

        let enabled = persistence.load_enabled_policies().await.unwrap();
        assert_eq!(enabled.len(), 2);

        let ids: Vec<_> = enabled.iter().map(|p| p.id.clone()).collect();
        assert!(ids.contains(&"p1".to_string()));
        assert!(ids.contains(&"p3".to_string()));
        assert!(!ids.contains(&"p2".to_string()));
    }

    #[tokio::test]
    async fn test_save_load_engine() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        // Create engine with policies
        let mut engine = PolicyEngine::new();

        let p1 = Policy::new("gpu-policy", "GPU Policy")
            .with_condition(PolicyCondition::ResourceMatches(
                ResourceMatcher::new().with_gpu(true, Some(1)),
            ))
            .with_effect(PolicyEffect::require(NodeSelector::tag(TagExpr::equals(
                "gpu", "true",
            ))));

        let p2 = Policy::new("prod-policy", "Production Policy")
            .with_condition(PolicyCondition::SubmitterMatches(
                SubmitterMatcher::new().with_domain("production"),
            ))
            .with_effect(PolicyEffect::require(NodeSelector::tag(TagExpr::equals(
                "env",
                "production",
            ))));

        engine.register(p1).unwrap();
        engine.register(p2).unwrap();

        // Save engine
        persistence.save_engine(&engine).await.unwrap();

        // Load into new engine
        let restored = persistence.load_engine().await.unwrap();

        // Verify policies
        let policies = restored.list();
        assert_eq!(policies.len(), 2);

        assert!(restored.get(&"gpu-policy".to_string()).is_some());
        assert!(restored.get(&"prod-policy".to_string()).is_some());
    }

    #[tokio::test]
    async fn test_engine_reconstruction_from_individual_items() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        // Save individual policies without snapshot
        let p1 = create_test_policy("p1", "Policy 1");
        let p2 = create_test_policy("p2", "Policy 2");

        persistence.save_policy(&p1).await.unwrap();
        persistence.save_policy(&p2).await.unwrap();

        // Load engine (should reconstruct from individual items)
        let restored = persistence.load_engine().await.unwrap();

        let policies = restored.list();
        assert_eq!(policies.len(), 2);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        // Add some data
        let p1 = create_test_policy("p1", "Policy 1");
        persistence.save_policy(&p1).await.unwrap();

        // Clear
        let deleted = persistence.clear_all().await.unwrap();
        assert!(deleted >= 1);

        // Verify empty
        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.policy_count, 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        // Initial stats
        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.policy_count, 0);
        assert_eq!(stats.enabled_count, 0);
        assert!(!stats.has_engine_snapshot);

        // Add policies
        let mut p1 = create_test_policy("p1", "Policy 1");
        p1.enabled = true;

        let mut p2 = create_test_policy("p2", "Policy 2");
        p2.enabled = false;

        persistence.save_policy(&p1).await.unwrap();
        persistence.save_policy(&p2).await.unwrap();

        // Check stats
        let stats = persistence.stats().await.unwrap();
        assert_eq!(stats.policy_count, 2);
        assert_eq!(stats.enabled_count, 1);
        assert!(!stats.has_engine_snapshot);

        // Save engine to create snapshot
        let mut engine = PolicyEngine::new();
        engine.register(p1).unwrap();
        persistence.save_engine(&engine).await.unwrap();

        let stats = persistence.stats().await.unwrap();
        assert!(stats.has_engine_snapshot);
    }

    #[tokio::test]
    async fn test_complex_policy_serialization() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        // Create a complex policy with various conditions and effects
        let policy = Policy::new("complex-policy", "Complex Policy")
            .with_condition(PolicyCondition::and(vec![
                PolicyCondition::JobMatches(
                    JobMatcher::new()
                        .with_name_pattern("ml-.*")
                        .with_priority_range(0, 100),
                ),
                PolicyCondition::or(vec![
                    PolicyCondition::SubmitterMatches(
                        SubmitterMatcher::new().with_domain("research"),
                    ),
                    PolicyCondition::SubmitterMatches(
                        SubmitterMatcher::new().with_min_priority(500),
                    ),
                ]),
            ]))
            .with_effect(PolicyEffect::prefer(
                NodeSelector::tag(TagExpr::and(vec![
                    TagExpr::equals("gpu", "true"),
                    TagExpr::has_key("high-memory"),
                ])),
                0.9,
            ))
            .with_effect(PolicyEffect::exclude(NodeSelector::tag(TagExpr::equals(
                "env",
                "development",
            ))))
            .with_governance(
                PolicyGovernance::new("ml-team")
                    .with_domain("ml")
                    .with_override_policy(OverridePolicyRef::Blueprint { min_priority: 500 })
                    .with_conflict_priority(75),
            );

        // Save and reload
        persistence.save_policy(&policy).await.unwrap();
        let loaded = persistence
            .load_policy("complex-policy")
            .await
            .unwrap()
            .unwrap();

        // Verify structure is preserved
        assert_eq!(loaded.id, "complex-policy");
        assert_eq!(loaded.effects.len(), 2);
        assert_eq!(loaded.governance.conflict_priority, 75);

        // Verify condition structure
        match &loaded.condition {
            PolicyCondition::And(conditions) => {
                assert_eq!(conditions.len(), 2);
            }
            _ => panic!("Expected And condition"),
        }
    }

    #[tokio::test]
    async fn test_persistence_clone() {
        let backend = InMemoryBackend::new();
        let persistence = PolicyPersistence::new(backend);

        // Save via original
        let policy = create_test_policy("test", "Test Policy");
        persistence.save_policy(&policy).await.unwrap();

        // Clone and load via clone
        let cloned = persistence.clone();
        let loaded = cloned.load_policy("test").await.unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "Test Policy");
    }
}
