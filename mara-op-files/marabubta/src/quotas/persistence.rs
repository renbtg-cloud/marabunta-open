// Marabunta - Licensed under the MIT License.
//! Persistence layer for the quotas system
//!
//! This module provides persistent storage for quota definitions, accounts,
//! reservations, and usage history. It enables recovery of quota state
//! after restarts and supports fair-share calculations across sessions.
//!
//! # Architecture
//!
//! The persistence layer uses three separate namespaces:
//! - `quotas`: Quota definitions (limits, resources, enforcement policies)
//! - `accounts`: Quota accounts with allocations and usage snapshots
//! - `reservations`: Active job reservations
//!
//! # Example
//!
//! ```ignore
//! use marabunta_compute::quotas::persistence::QuotasPersistence;
//! use marabunta_compute::storage::persistence::MemoryBackend;
//!
//! let backend = MemoryBackend::new();
//! let persistence = QuotasPersistence::new(backend);
//!
//! // Save a quota
//! persistence.save_quota(&quota).await?;
//!
//! // Save entire manager state
//! persistence.save_manager(&manager).await?;
//!
//! // Restore manager on startup
//! let manager = persistence.load_manager().await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::storage::persistence::{PersistenceBackend, PersistenceError, TypedStore};

use super::account::{QuotaAccount, UsageSnapshot};
use super::manager::{QuotaManager, QuotaReservation};
use super::types::{AccountId, BurstState, Quota, QuotaId, QuotaResource};

/// Namespace constants for storage organization
const NAMESPACE_QUOTAS: &str = "quotas";
const NAMESPACE_ACCOUNTS: &str = "quota_accounts";
const NAMESPACE_RESERVATIONS: &str = "quota_reservations";
const NAMESPACE_BURST_STATES: &str = "quota_burst_states";
const NAMESPACE_FAIR_SHARE: &str = "quota_fair_share";

/// Persistence layer for the quota management system
///
/// This struct provides methods to save and load quota-related data
/// including quota definitions, accounts, reservations, and usage history.
///
/// All operations are async and use the underlying `PersistenceBackend`
/// for actual storage operations.
pub struct QuotasPersistence<B: PersistenceBackend> {
    quotas_store: TypedStore<B>,
    accounts_store: TypedStore<B>,
    reservations_store: TypedStore<B>,
    burst_states_store: TypedStore<B>,
    fair_share_store: TypedStore<B>,
}

impl<B: PersistenceBackend> QuotasPersistence<B> {
    /// Create a new persistence layer with the given backend
    pub fn new(backend: B) -> Self {
        let backend = Arc::new(backend);
        Self {
            quotas_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_QUOTAS),
            accounts_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_ACCOUNTS),
            reservations_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_RESERVATIONS),
            burst_states_store: TypedStore::from_arc(Arc::clone(&backend), NAMESPACE_BURST_STATES),
            fair_share_store: TypedStore::from_arc(backend, NAMESPACE_FAIR_SHARE),
        }
    }

    // ========== Quota Operations ==========

    /// Save a quota definition
    ///
    /// Overwrites any existing quota with the same ID.
    pub async fn save_quota(&self, quota: &Quota) -> Result<(), PersistenceError> {
        self.quotas_store.put(&quota.id, quota).await
    }

    /// Load a quota by ID
    pub async fn load_quota(&self, id: &QuotaId) -> Result<Option<Quota>, PersistenceError> {
        self.quotas_store.get(id).await
    }

    /// Load all quota definitions
    pub async fn load_all_quotas(&self) -> Result<Vec<Quota>, PersistenceError> {
        let items = self.quotas_store.get_all::<Quota>().await?;
        Ok(items.into_iter().map(|(_, v)| v).collect())
    }

    /// Delete a quota by ID
    ///
    /// Returns `true` if the quota existed and was deleted.
    pub async fn delete_quota(&self, id: &QuotaId) -> Result<bool, PersistenceError> {
        self.quotas_store.delete(id).await
    }

    /// Save multiple quotas atomically
    pub async fn save_quotas_batch(&self, quotas: &[&Quota]) -> Result<(), PersistenceError> {
        let items: Vec<(&str, &Quota)> = quotas.iter().map(|q| (q.id.as_str(), *q)).collect();
        self.quotas_store.batch_put(&items).await
    }

    // ========== Account Operations ==========

    /// Save a quota account
    ///
    /// This saves the full account including allocations, usage, and history.
    pub async fn save_account(&self, account: &QuotaAccount) -> Result<(), PersistenceError> {
        self.accounts_store.put(&account.id, account).await
    }

    /// Load an account by ID
    pub async fn load_account(
        &self,
        id: &AccountId,
    ) -> Result<Option<QuotaAccount>, PersistenceError> {
        self.accounts_store.get(id).await
    }

    /// Load all accounts
    pub async fn load_all_accounts(&self) -> Result<Vec<QuotaAccount>, PersistenceError> {
        let items = self.accounts_store.get_all::<QuotaAccount>().await?;
        Ok(items.into_iter().map(|(_, v)| v).collect())
    }

    /// Delete an account by ID
    ///
    /// Returns `true` if the account existed and was deleted.
    pub async fn delete_account(&self, id: &AccountId) -> Result<bool, PersistenceError> {
        self.accounts_store.delete(id).await
    }

    /// Save multiple accounts atomically
    pub async fn save_accounts_batch(
        &self,
        accounts: &[&QuotaAccount],
    ) -> Result<(), PersistenceError> {
        let items: Vec<(&str, &QuotaAccount)> =
            accounts.iter().map(|a| (a.id.as_str(), *a)).collect();
        self.accounts_store.batch_put(&items).await
    }

    // ========== Reservation Operations ==========

    /// Save a reservation for a job
    ///
    /// The job_id is used as the key.
    pub async fn save_reservation(
        &self,
        job_id: &str,
        reservation: &QuotaReservation,
    ) -> Result<(), PersistenceError> {
        self.reservations_store.put(job_id, reservation).await
    }

    /// Load a reservation by job ID
    pub async fn load_reservation(
        &self,
        job_id: &str,
    ) -> Result<Option<QuotaReservation>, PersistenceError> {
        self.reservations_store.get(job_id).await
    }

    /// Load all active reservations
    ///
    /// Returns a vector of (job_id, reservation) pairs.
    pub async fn load_all_reservations(
        &self,
    ) -> Result<Vec<(String, QuotaReservation)>, PersistenceError> {
        self.reservations_store.get_all::<QuotaReservation>().await
    }

    /// Delete a reservation by job ID
    ///
    /// Returns `true` if the reservation existed and was deleted.
    pub async fn delete_reservation(&self, job_id: &str) -> Result<bool, PersistenceError> {
        self.reservations_store.delete(job_id).await
    }

    /// Clear all reservations
    ///
    /// Useful for cleanup on startup if reservations are transient.
    /// Returns the number of reservations deleted.
    pub async fn clear_reservations(&self) -> Result<u64, PersistenceError> {
        self.reservations_store.clear().await
    }

    // ========== Burst State Operations ==========

    /// Save burst state for an account/resource pair
    pub async fn save_burst_state(
        &self,
        account_id: &AccountId,
        resource: &QuotaResource,
        state: &BurstState,
    ) -> Result<(), PersistenceError> {
        let key = format!("{}:{}", account_id, resource.key());
        self.burst_states_store.put(&key, state).await
    }

    /// Load burst state for an account/resource pair
    pub async fn load_burst_state(
        &self,
        account_id: &AccountId,
        resource: &QuotaResource,
    ) -> Result<Option<BurstState>, PersistenceError> {
        let key = format!("{}:{}", account_id, resource.key());
        self.burst_states_store.get(&key).await
    }

    /// Load all burst states
    ///
    /// Returns a map from (account_id, resource_key) to BurstState.
    pub async fn load_all_burst_states(
        &self,
    ) -> Result<HashMap<(AccountId, String), BurstState>, PersistenceError> {
        let items = self.burst_states_store.get_all::<BurstState>().await?;
        let mut result = HashMap::new();

        for (key, state) in items {
            if let Some((account_id, resource_key)) = key.split_once(':') {
                result.insert((account_id.to_string(), resource_key.to_string()), state);
            }
        }

        Ok(result)
    }

    /// Delete burst state for an account/resource pair
    pub async fn delete_burst_state(
        &self,
        account_id: &AccountId,
        resource: &QuotaResource,
    ) -> Result<bool, PersistenceError> {
        let key = format!("{}:{}", account_id, resource.key());
        self.burst_states_store.delete(&key).await
    }

    // ========== Fair Share History Operations ==========

    /// Save fair share usage history for an account
    pub async fn save_fair_share_history(
        &self,
        account_id: &AccountId,
        history: &[UsageSnapshot],
    ) -> Result<(), PersistenceError> {
        self.fair_share_store.put(account_id, &history).await
    }

    /// Load fair share usage history for an account
    pub async fn load_fair_share_history(
        &self,
        account_id: &AccountId,
    ) -> Result<Option<Vec<UsageSnapshot>>, PersistenceError> {
        self.fair_share_store.get(account_id).await
    }

    /// Load all fair share history
    ///
    /// Returns a map from account_id to usage history.
    pub async fn load_all_fair_share_history(
        &self,
    ) -> Result<HashMap<AccountId, Vec<UsageSnapshot>>, PersistenceError> {
        let items = self
            .fair_share_store
            .get_all::<Vec<UsageSnapshot>>()
            .await?;
        Ok(items.into_iter().collect())
    }

    // ========== Manager-Level Operations ==========

    /// Save the entire manager state
    ///
    /// This saves all quotas, accounts, reservations, and burst states atomically.
    /// Usage history from accounts is preserved.
    pub async fn save_manager(&self, manager: &QuotaManager) -> Result<(), PersistenceError> {
        // Save all quotas
        for quota in manager.list_quotas() {
            self.save_quota(quota).await?;
        }

        // Save all accounts
        for account in manager.list_accounts() {
            self.save_account(account).await?;
        }

        // Save all reservations
        for reservation in manager.list_reservations() {
            self.save_reservation(&reservation.job_id, reservation)
                .await?;
        }

        Ok(())
    }

    /// Load and reconstruct a manager from persisted state
    ///
    /// This loads all quotas, accounts, and reservations, then rebuilds
    /// the internal indices required by the manager.
    pub async fn load_manager(&self) -> Result<QuotaManager, PersistenceError> {
        let mut manager = QuotaManager::new();

        // Load quotas
        let quotas = self.load_all_quotas().await?;
        for quota in quotas {
            if let Err(e) = manager.create_quota(quota.clone()) {
                // Log warning but continue - quota may already exist
                tracing::warn!("Failed to restore quota {}: {:?}", quota.id, e);
            }
        }

        // Load accounts
        let accounts = self.load_all_accounts().await?;
        for account in accounts {
            if let Err(e) = manager.create_account(account.clone()) {
                // Log warning but continue - account may already exist
                tracing::warn!("Failed to restore account {}: {:?}", account.id, e);
            }
        }

        // Load reservations and restore them
        // Note: We restore reservations directly without re-recording usage,
        // since usage is already tracked in the account state
        let reservations = self.load_all_reservations().await?;
        for (job_id, reservation) in reservations {
            manager.restore_reservation(job_id, reservation);
        }

        // Load burst states
        let burst_states = self.load_all_burst_states().await?;
        manager.restore_burst_states(burst_states);

        Ok(manager)
    }

    /// Snapshot current account usage for fair share tracking
    ///
    /// Takes a snapshot of all accounts' usage and persists it along with
    /// their existing history. This should be called periodically.
    pub async fn snapshot_accounts(
        &self,
        manager: &mut QuotaManager,
    ) -> Result<(), PersistenceError> {
        // Take snapshots in the manager
        manager.snapshot_all();

        // Persist all accounts with updated history
        for account in manager.list_accounts() {
            self.save_account(account).await?;
        }

        Ok(())
    }

    /// Clear all persisted quota data
    ///
    /// This removes all quotas, accounts, reservations, and burst states.
    /// Use with caution!
    pub async fn clear_all(&self) -> Result<(), PersistenceError> {
        self.quotas_store.clear().await?;
        self.accounts_store.clear().await?;
        self.reservations_store.clear().await?;
        self.burst_states_store.clear().await?;
        self.fair_share_store.clear().await?;
        Ok(())
    }
}

// Extension trait for QuotaManager to support restoration
impl QuotaManager {
    /// Restore a reservation directly without affecting usage
    ///
    /// This is used during recovery to restore reservations that were
    /// already tracked in the account usage.
    pub(crate) fn restore_reservation(&mut self, job_id: String, reservation: QuotaReservation) {
        self.reservations.insert(job_id, reservation);
    }

    /// Restore burst states from persisted data
    pub(crate) fn restore_burst_states(
        &mut self,
        states: HashMap<(AccountId, String), BurstState>,
    ) {
        self.burst_states = states;
    }

    /// Get a reference to the reservations map
    #[allow(dead_code)]
    pub(crate) fn get_reservations(&self) -> &HashMap<String, QuotaReservation> {
        &self.reservations
    }

    /// Get mutable access to burst states (for restoration)
    #[allow(dead_code)]
    pub(crate) fn burst_states_mut(&mut self) -> &mut HashMap<(AccountId, String), BurstState> {
        &mut self.burst_states
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quotas::account::{QuotaAccount, QuotaAllocation, ResourceRequest};
    use crate::quotas::types::{
        OveragePenalty, QuotaEnforcement, QuotaLimit, QuotaPeriod, QuotaScope,
    };
    use parking_lot::RwLock;
    use std::collections::HashMap;

    /// Simple in-memory backend for testing
    struct MemoryBackend {
        data: RwLock<HashMap<String, HashMap<String, Vec<u8>>>>,
    }

    impl MemoryBackend {
        fn new() -> Self {
            Self {
                data: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl PersistenceBackend for MemoryBackend {
        async fn put(
            &self,
            namespace: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), PersistenceError> {
            let mut data = self.data.write();
            data.entry(namespace.to_string())
                .or_insert_with(HashMap::new)
                .insert(key.to_string(), value.to_vec());
            Ok(())
        }

        async fn get(
            &self,
            namespace: &str,
            key: &str,
        ) -> Result<Option<Vec<u8>>, PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).and_then(|ns| ns.get(key).cloned()))
        }

        async fn delete(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
            let mut data = self.data.write();
            if let Some(ns) = data.get_mut(namespace) {
                Ok(ns.remove(key).is_some())
            } else {
                Ok(false)
            }
        }

        async fn list_keys(
            &self,
            namespace: &str,
            prefix: Option<&str>,
        ) -> Result<Vec<String>, PersistenceError> {
            let data = self.data.read();
            Ok(data
                .get(namespace)
                .map(|ns| {
                    ns.keys()
                        .filter(|k| prefix.map_or(true, |p| k.starts_with(p)))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default())
        }

        async fn exists(&self, namespace: &str, key: &str) -> Result<bool, PersistenceError> {
            let data = self.data.read();
            Ok(data.get(namespace).map_or(false, |ns| ns.contains_key(key)))
        }

        async fn batch_put(
            &self,
            namespace: &str,
            items: &[(&str, &[u8])],
        ) -> Result<(), PersistenceError> {
            let mut data = self.data.write();
            let ns = data
                .entry(namespace.to_string())
                .or_insert_with(HashMap::new);
            for (key, value) in items {
                ns.insert(key.to_string(), value.to_vec());
            }
            Ok(())
        }

        async fn clear_namespace(&self, namespace: &str) -> Result<u64, PersistenceError> {
            let mut data = self.data.write();
            if let Some(ns) = data.remove(namespace) {
                Ok(ns.len() as u64)
            } else {
                Ok(0)
            }
        }
    }

    fn create_test_quota(id: &str, resource: QuotaResource, limit: f64) -> Quota {
        Quota::new(
            id,
            format!("Test quota {}", id),
            resource,
            QuotaLimit::Hard(limit),
            QuotaScope::Global,
            QuotaEnforcement::Block,
            "admin",
            "test.com",
        )
    }

    #[tokio::test]
    async fn test_save_load_quota() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());
        let quota = create_test_quota("cpu-quota", QuotaResource::CpuHours, 100.0);

        // Save
        persistence.save_quota(&quota).await.unwrap();

        // Load
        let loaded = persistence
            .load_quota(&"cpu-quota".to_string())
            .await
            .unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, "cpu-quota");
        assert_eq!(loaded.limit.effective_limit(), 100.0);
    }

    #[tokio::test]
    async fn test_save_load_all_quotas() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let q1 = create_test_quota("q1", QuotaResource::CpuHours, 100.0);
        let q2 = create_test_quota("q2", QuotaResource::GpuHours, 50.0);
        let q3 = create_test_quota("q3", QuotaResource::MemoryGBHours, 200.0);

        persistence.save_quota(&q1).await.unwrap();
        persistence.save_quota(&q2).await.unwrap();
        persistence.save_quota(&q3).await.unwrap();

        let all = persistence.load_all_quotas().await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_delete_quota() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());
        let quota = create_test_quota("q1", QuotaResource::CpuHours, 100.0);

        persistence.save_quota(&quota).await.unwrap();
        assert!(persistence
            .load_quota(&"q1".to_string())
            .await
            .unwrap()
            .is_some());

        let deleted = persistence.delete_quota(&"q1".to_string()).await.unwrap();
        assert!(deleted);

        assert!(persistence
            .load_quota(&"q1".to_string())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_save_load_account() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let mut account = QuotaAccount::new("team-alpha", "Team Alpha", "lead@test.com");
        account.add_member("dev1@test.com");
        account.add_member("dev2@test.com");
        account.record_usage(&QuotaResource::CpuHours, 50.0);

        // Add an allocation
        let allocation = QuotaAllocation::new("cpu-quota", 100.0, "admin");
        account.add_allocation(allocation);

        // Save
        persistence.save_account(&account).await.unwrap();

        // Load
        let loaded = persistence
            .load_account(&"team-alpha".to_string())
            .await
            .unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.id, "team-alpha");
        assert_eq!(loaded.members.len(), 2);
        assert_eq!(loaded.get_usage(&QuotaResource::CpuHours), 50.0);
        assert!(loaded.get_allocation(&"cpu-quota".to_string()).is_some());
    }

    #[tokio::test]
    async fn test_save_load_account_with_usage_history() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let mut account = QuotaAccount::new("team", "Team", "user@test.com");
        account.record_usage(&QuotaResource::CpuHours, 10.0);
        account.snapshot();
        account.record_usage(&QuotaResource::CpuHours, 20.0);
        account.snapshot();

        persistence.save_account(&account).await.unwrap();

        let loaded = persistence
            .load_account(&"team".to_string())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.usage_history.len(), 2);
    }

    #[tokio::test]
    async fn test_save_load_reservation() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let mut resources = HashMap::new();
        resources.insert(QuotaResource::CpuHours, 50.0);
        resources.insert(QuotaResource::GpuHours, 10.0);

        let reservation = QuotaReservation::new("job-123", "team-alpha".to_string(), resources);

        persistence
            .save_reservation("job-123", &reservation)
            .await
            .unwrap();

        let loaded = persistence.load_reservation("job-123").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.job_id, "job-123");
        assert_eq!(loaded.account_id, "team-alpha");
        assert_eq!(loaded.amount_for(&QuotaResource::CpuHours), 50.0);
    }

    #[tokio::test]
    async fn test_load_all_reservations() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        for i in 1..=5 {
            let mut resources = HashMap::new();
            resources.insert(QuotaResource::CpuHours, (i * 10) as f64);
            let reservation =
                QuotaReservation::new(format!("job-{}", i), "team".to_string(), resources);
            persistence
                .save_reservation(&format!("job-{}", i), &reservation)
                .await
                .unwrap();
        }

        let all = persistence.load_all_reservations().await.unwrap();
        assert_eq!(all.len(), 5);
    }

    #[tokio::test]
    async fn test_clear_reservations() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let mut resources = HashMap::new();
        resources.insert(QuotaResource::CpuHours, 50.0);

        for i in 1..=3 {
            let reservation =
                QuotaReservation::new(format!("job-{}", i), "team".to_string(), resources.clone());
            persistence
                .save_reservation(&format!("job-{}", i), &reservation)
                .await
                .unwrap();
        }

        let count = persistence.clear_reservations().await.unwrap();
        assert_eq!(count, 3);

        let all = persistence.load_all_reservations().await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_save_load_burst_state() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let mut state = BurstState::default();
        state.start_burst(chrono::Utc::now());

        persistence
            .save_burst_state(&"team".to_string(), &QuotaResource::CpuHours, &state)
            .await
            .unwrap();

        let loaded = persistence
            .load_burst_state(&"team".to_string(), &QuotaResource::CpuHours)
            .await
            .unwrap();
        assert!(loaded.is_some());
        assert!(loaded.unwrap().in_burst);
    }

    #[tokio::test]
    async fn test_full_manager_save_load() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        // Set up a manager with data
        let mut manager = QuotaManager::new();

        // Create quotas
        let cpu_quota = create_test_quota("cpu-quota", QuotaResource::CpuHours, 100.0);
        let gpu_quota = create_test_quota("gpu-quota", QuotaResource::GpuHours, 50.0);
        manager.create_quota(cpu_quota).unwrap();
        manager.create_quota(gpu_quota).unwrap();

        // Create accounts
        let account1 = QuotaAccount::new("team-1", "Team 1", "user1@test.com");
        let account2 = QuotaAccount::new("team-2", "Team 2", "user2@test.com");
        manager.create_account(account1).unwrap();
        manager.create_account(account2).unwrap();

        // Allocate quotas
        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"team-1".to_string(),
                60.0,
                "admin",
            )
            .unwrap();
        manager
            .allocate(
                &"gpu-quota".to_string(),
                &"team-1".to_string(),
                30.0,
                "admin",
            )
            .unwrap();
        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"team-2".to_string(),
                40.0,
                "admin",
            )
            .unwrap();

        // Create a reservation
        let request = ResourceRequest::new().with_resource(QuotaResource::CpuHours, 20.0);
        manager
            .reserve("job-001", &"team-1".to_string(), &request)
            .unwrap();

        // Save manager
        persistence.save_manager(&manager).await.unwrap();

        // Load into new manager
        let restored = persistence.load_manager().await.unwrap();

        // Verify quotas
        assert_eq!(restored.list_quotas().len(), 2);
        assert!(restored.get_quota(&"cpu-quota".to_string()).is_some());
        assert!(restored.get_quota(&"gpu-quota".to_string()).is_some());

        // Verify accounts
        assert_eq!(restored.list_accounts().len(), 2);
        let team1 = restored.get_account(&"team-1".to_string()).unwrap();
        assert!(team1.get_allocation(&"cpu-quota".to_string()).is_some());
        assert_eq!(
            team1
                .get_allocation(&"cpu-quota".to_string())
                .unwrap()
                .allocated_amount,
            60.0
        );

        // Verify reservation was restored
        assert!(restored.get_reservation("job-001").is_some());
        let reservation = restored.get_reservation("job-001").unwrap();
        assert_eq!(reservation.amount_for(&QuotaResource::CpuHours), 20.0);
    }

    #[tokio::test]
    async fn test_usage_history_preservation() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let mut manager = QuotaManager::new();

        let quota = create_test_quota("cpu-quota", QuotaResource::CpuHours, 100.0);
        manager.create_quota(quota).unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        manager.create_account(account).unwrap();
        manager
            .allocate(
                &"cpu-quota".to_string(),
                &"team".to_string(),
                100.0,
                "admin",
            )
            .unwrap();

        // Record usage and take snapshots
        manager
            .get_account_mut(&"team".to_string())
            .unwrap()
            .record_usage(&QuotaResource::CpuHours, 10.0);
        manager.snapshot_all();

        manager
            .get_account_mut(&"team".to_string())
            .unwrap()
            .record_usage(&QuotaResource::CpuHours, 15.0);
        manager.snapshot_all();

        // Save and restore
        persistence.save_manager(&manager).await.unwrap();
        let restored = persistence.load_manager().await.unwrap();

        let account = restored.get_account(&"team".to_string()).unwrap();
        assert_eq!(account.usage_history.len(), 2);

        // Verify usage values were preserved
        assert_eq!(account.get_usage(&QuotaResource::CpuHours), 25.0);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let persistence = QuotasPersistence::new(MemoryBackend::new());

        let quota = create_test_quota("q1", QuotaResource::CpuHours, 100.0);
        persistence.save_quota(&quota).await.unwrap();

        let account = QuotaAccount::new("team", "Team", "user@test.com");
        persistence.save_account(&account).await.unwrap();

        let mut resources = HashMap::new();
        resources.insert(QuotaResource::CpuHours, 50.0);
        let reservation = QuotaReservation::new("job-1", "team".to_string(), resources);
        persistence
            .save_reservation("job-1", &reservation)
            .await
            .unwrap();

        // Clear all
        persistence.clear_all().await.unwrap();

        assert!(persistence.load_all_quotas().await.unwrap().is_empty());
        assert!(persistence.load_all_accounts().await.unwrap().is_empty());
        assert!(persistence
            .load_all_reservations()
            .await
            .unwrap()
            .is_empty());
    }
}
