// Marabunta - Licensed under the MIT License.
//! Data migration tracking for the plugin system.
//!
//! Provides a [`MigrationTracker`] that manages the lifecycle of data
//! migrations initiated by plugins. Each migration goes through the states:
//! Pending -> InProgress -> Completed | Failed | Cancelled.
//!
//! Concurrency-safe via [`DashMap`] for lock-free reads and fine-grained
//! write locking. The tracker enforces a configurable maximum number of
//! concurrent active migrations to prevent overload.

use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::plugin::types::{MigrationRequest, MigrationResponse, PluginError, PluginResult, Priority};

// ============================================================================
// MigrationState
// ============================================================================

/// Lifecycle state of a data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    /// Migration has been accepted but not yet started.
    Pending,
    /// Migration is actively transferring data.
    InProgress,
    /// Migration completed successfully.
    Completed,
    /// Migration failed with an error.
    Failed,
    /// Migration was cancelled before completion.
    Cancelled,
}

// ============================================================================
// MigrationRecord
// ============================================================================

/// Full record of a data migration, including its current state and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    /// Unique migration identifier.
    pub id: String,
    /// Key of the data being migrated.
    pub data_key: Vec<u8>,
    /// Node the data is being migrated from.
    pub from_node: String,
    /// Target node the data is being migrated to (set after target selection).
    pub to_node: Option<String>,
    /// Preferred traits for the target node.
    pub preferred_traits: Vec<String>,
    /// Priority level of this migration.
    pub priority: Priority,
    /// Human-readable reason for the migration.
    pub reason: String,
    /// Current lifecycle state.
    pub state: MigrationState,
    /// When this migration was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When this migration was completed (or failed/cancelled).
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Error message if the migration failed.
    pub error: Option<String>,
}

// ============================================================================
// MigrationTracker
// ============================================================================

/// Tracks active and completed data migrations.
///
/// The tracker enforces a maximum number of concurrent migrations via
/// `max_concurrent`. Once the limit is reached, new migration requests
/// are rejected with a [`PluginError::CapacityExceeded`] error.
pub struct MigrationTracker {
    /// Currently active migrations (Pending or InProgress).
    active: DashMap<String, MigrationRecord>,
    /// Completed, failed, or cancelled migrations (kept for audit/query).
    completed: DashMap<String, MigrationRecord>,
    /// Maximum number of simultaneous active migrations.
    max_concurrent: usize,
}

impl MigrationTracker {
    /// Create a new migration tracker with the given concurrency limit.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            active: DashMap::new(),
            completed: DashMap::new(),
            max_concurrent,
        }
    }

    /// Request a new data migration.
    ///
    /// If the number of active migrations is at or above `max_concurrent`,
    /// the request is rejected. Otherwise a new [`MigrationRecord`] is
    /// created in the `Pending` state and inserted into the active map.
    pub fn request_migration(
        &self,
        request: MigrationRequest,
    ) -> PluginResult<MigrationResponse> {
        if self.active.len() >= self.max_concurrent {
            return Err(PluginError::CapacityExceeded(format!(
                "maximum concurrent migrations reached ({})",
                self.max_concurrent
            )));
        }

        let migration_id = Uuid::new_v4().to_string();
        let record = MigrationRecord {
            id: migration_id.clone(),
            data_key: request.data_key,
            from_node: request.from_node,
            to_node: None,
            preferred_traits: request.preferred_traits,
            priority: request.priority,
            reason: request.reason,
            state: MigrationState::Pending,
            created_at: Utc::now(),
            completed_at: None,
            error: None,
        };

        self.active.insert(migration_id.clone(), record);

        Ok(MigrationResponse {
            accepted: true,
            migration_id,
            error: String::new(),
        })
    }

    /// Look up a migration by ID across both active and completed maps.
    pub fn get_migration(&self, id: &str) -> Option<MigrationRecord> {
        self.active
            .get(id)
            .map(|r| r.value().clone())
            .or_else(|| self.completed.get(id).map(|r| r.value().clone()))
    }

    /// Update the state of an active migration.
    ///
    /// Returns `true` if the migration was found and updated, `false` otherwise.
    pub fn update_state(&self, id: &str, state: MigrationState) -> bool {
        if let Some(mut entry) = self.active.get_mut(id) {
            entry.state = state;
            true
        } else {
            false
        }
    }

    /// Set the target node for an active migration.
    ///
    /// Returns `true` if the migration was found and updated, `false` otherwise.
    pub fn set_target(&self, id: &str, to_node: String) -> bool {
        if let Some(mut entry) = self.active.get_mut(id) {
            entry.to_node = Some(to_node);
            true
        } else {
            false
        }
    }

    /// Mark a migration as completed and move it from active to completed.
    ///
    /// Returns `true` if the migration was found and moved, `false` otherwise.
    pub fn complete_migration(&self, id: &str) -> bool {
        if let Some((_, mut record)) = self.active.remove(id) {
            record.state = MigrationState::Completed;
            record.completed_at = Some(Utc::now());
            self.completed.insert(record.id.clone(), record);
            true
        } else {
            false
        }
    }

    /// Mark a migration as failed with an error message and move it to completed.
    ///
    /// Returns `true` if the migration was found and moved, `false` otherwise.
    pub fn fail_migration(&self, id: &str, error: String) -> bool {
        if let Some((_, mut record)) = self.active.remove(id) {
            record.state = MigrationState::Failed;
            record.completed_at = Some(Utc::now());
            record.error = Some(error);
            self.completed.insert(record.id.clone(), record);
            true
        } else {
            false
        }
    }

    /// Cancel an active migration and move it to completed.
    ///
    /// Returns `true` if the migration was found and cancelled, `false` otherwise.
    pub fn cancel_migration(&self, id: &str) -> bool {
        if let Some((_, mut record)) = self.active.remove(id) {
            record.state = MigrationState::Cancelled;
            record.completed_at = Some(Utc::now());
            self.completed.insert(record.id.clone(), record);
            true
        } else {
            false
        }
    }

    /// Return the number of currently active migrations.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// Return a snapshot of all currently active migrations.
    pub fn list_active(&self) -> Vec<MigrationRecord> {
        self.active.iter().map(|r| r.value().clone()).collect()
    }

    /// Remove completed/failed/cancelled migrations older than `max_age`.
    ///
    /// Returns the number of records pruned.
    pub fn prune_completed(&self, max_age: Duration) -> usize {
        let now = Utc::now();
        let cutoff = now
            - chrono::Duration::from_std(max_age)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));

        let to_remove: Vec<String> = self
            .completed
            .iter()
            .filter(|r| {
                r.value()
                    .completed_at
                    .map(|t| t < cutoff)
                    .unwrap_or(false)
            })
            .map(|r| r.key().clone())
            .collect();

        let count = to_remove.len();
        for id in to_remove {
            self.completed.remove(&id);
        }
        count
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::types::Priority;

    /// Helper: create a standard migration request.
    fn make_request(reason: &str) -> MigrationRequest {
        MigrationRequest {
            data_key: b"test-data-key".to_vec(),
            from_node: "node-abc".into(),
            preferred_traits: vec!["CanStoreState".into()],
            priority: Priority::Normal,
            reason: reason.into(),
        }
    }

    // ---- request_migration ----

    #[test]
    fn request_migration_succeeds() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("rebalance")).unwrap();
        assert!(response.accepted);
        assert!(!response.migration_id.is_empty());
        assert!(response.error.is_empty());
        assert_eq!(tracker.active_count(), 1);
    }

    #[test]
    fn request_migration_generates_unique_ids() {
        let tracker = MigrationTracker::new(16);
        let r1 = tracker.request_migration(make_request("rebalance")).unwrap();
        let r2 = tracker.request_migration(make_request("failure")).unwrap();
        assert_ne!(r1.migration_id, r2.migration_id);
    }

    #[test]
    fn request_migration_capacity_exceeded() {
        let tracker = MigrationTracker::new(2);
        tracker.request_migration(make_request("a")).unwrap();
        tracker.request_migration(make_request("b")).unwrap();

        let result = tracker.request_migration(make_request("c"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::CapacityExceeded(msg) => {
                assert!(msg.contains("maximum concurrent migrations"));
            }
            other => panic!("expected CapacityExceeded, got {:?}", other),
        }
    }

    #[test]
    fn request_migration_stores_correct_fields() {
        let tracker = MigrationTracker::new(16);
        let request = MigrationRequest {
            data_key: b"my-key".to_vec(),
            from_node: "node-xyz".into(),
            preferred_traits: vec!["CanExecute".into(), "CanStoreState".into()],
            priority: Priority::High,
            reason: "failure recovery".into(),
        };
        let response = tracker.request_migration(request).unwrap();
        let record = tracker.get_migration(&response.migration_id).unwrap();

        assert_eq!(record.data_key, b"my-key");
        assert_eq!(record.from_node, "node-xyz");
        assert_eq!(record.preferred_traits.len(), 2);
        assert_eq!(record.priority, Priority::High);
        assert_eq!(record.reason, "failure recovery");
        assert_eq!(record.state, MigrationState::Pending);
        assert!(record.to_node.is_none());
        assert!(record.completed_at.is_none());
        assert!(record.error.is_none());
    }

    // ---- get_migration ----

    #[test]
    fn get_migration_active() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();
        let record = tracker.get_migration(&response.migration_id);
        assert!(record.is_some());
        assert_eq!(record.unwrap().state, MigrationState::Pending);
    }

    #[test]
    fn get_migration_completed() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();
        tracker.complete_migration(&response.migration_id);

        let record = tracker.get_migration(&response.migration_id);
        assert!(record.is_some());
        assert_eq!(record.unwrap().state, MigrationState::Completed);
    }

    #[test]
    fn get_migration_not_found() {
        let tracker = MigrationTracker::new(16);
        assert!(tracker.get_migration("nonexistent-id").is_none());
    }

    // ---- update_state ----

    #[test]
    fn update_state_success() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();

        assert!(tracker.update_state(&response.migration_id, MigrationState::InProgress));
        let record = tracker.get_migration(&response.migration_id).unwrap();
        assert_eq!(record.state, MigrationState::InProgress);
    }

    #[test]
    fn update_state_nonexistent() {
        let tracker = MigrationTracker::new(16);
        assert!(!tracker.update_state("nonexistent", MigrationState::InProgress));
    }

    // ---- set_target ----

    #[test]
    fn set_target_success() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();

        assert!(tracker.set_target(&response.migration_id, "node-target".into()));
        let record = tracker.get_migration(&response.migration_id).unwrap();
        assert_eq!(record.to_node, Some("node-target".into()));
    }

    #[test]
    fn set_target_nonexistent() {
        let tracker = MigrationTracker::new(16);
        assert!(!tracker.set_target("nonexistent", "node-target".into()));
    }

    // ---- complete_migration ----

    #[test]
    fn complete_migration_moves_to_completed() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();

        assert!(tracker.complete_migration(&response.migration_id));
        assert_eq!(tracker.active_count(), 0);

        let record = tracker.get_migration(&response.migration_id).unwrap();
        assert_eq!(record.state, MigrationState::Completed);
        assert!(record.completed_at.is_some());
        assert!(record.error.is_none());
    }

    #[test]
    fn complete_migration_nonexistent() {
        let tracker = MigrationTracker::new(16);
        assert!(!tracker.complete_migration("nonexistent"));
    }

    #[test]
    fn complete_migration_frees_capacity() {
        let tracker = MigrationTracker::new(1);
        let r1 = tracker.request_migration(make_request("first")).unwrap();
        // At capacity.
        assert!(tracker.request_migration(make_request("second")).is_err());

        // Complete first, freeing a slot.
        tracker.complete_migration(&r1.migration_id);
        assert!(tracker.request_migration(make_request("second")).is_ok());
    }

    // ---- fail_migration ----

    #[test]
    fn fail_migration_moves_to_completed_with_error() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();

        assert!(tracker.fail_migration(&response.migration_id, "disk full".into()));
        assert_eq!(tracker.active_count(), 0);

        let record = tracker.get_migration(&response.migration_id).unwrap();
        assert_eq!(record.state, MigrationState::Failed);
        assert!(record.completed_at.is_some());
        assert_eq!(record.error, Some("disk full".into()));
    }

    #[test]
    fn fail_migration_nonexistent() {
        let tracker = MigrationTracker::new(16);
        assert!(!tracker.fail_migration("nonexistent", "error".into()));
    }

    // ---- cancel_migration ----

    #[test]
    fn cancel_migration_moves_to_completed() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();

        assert!(tracker.cancel_migration(&response.migration_id));
        assert_eq!(tracker.active_count(), 0);

        let record = tracker.get_migration(&response.migration_id).unwrap();
        assert_eq!(record.state, MigrationState::Cancelled);
        assert!(record.completed_at.is_some());
    }

    #[test]
    fn cancel_migration_nonexistent() {
        let tracker = MigrationTracker::new(16);
        assert!(!tracker.cancel_migration("nonexistent"));
    }

    // ---- active_count / list_active ----

    #[test]
    fn active_count_tracks_correctly() {
        let tracker = MigrationTracker::new(16);
        assert_eq!(tracker.active_count(), 0);

        let r1 = tracker.request_migration(make_request("a")).unwrap();
        assert_eq!(tracker.active_count(), 1);

        tracker.request_migration(make_request("b")).unwrap();
        assert_eq!(tracker.active_count(), 2);

        tracker.complete_migration(&r1.migration_id);
        assert_eq!(tracker.active_count(), 1);
    }

    #[test]
    fn list_active_returns_only_active() {
        let tracker = MigrationTracker::new(16);
        let r1 = tracker.request_migration(make_request("a")).unwrap();
        let _r2 = tracker.request_migration(make_request("b")).unwrap();

        tracker.complete_migration(&r1.migration_id);

        let active = tracker.list_active();
        assert_eq!(active.len(), 1);
        assert_ne!(active[0].id, r1.migration_id);
    }

    // ---- prune_completed ----

    #[test]
    fn prune_completed_removes_old_records() {
        let tracker = MigrationTracker::new(16);

        // Create and complete a migration, then backdate its completed_at.
        let r1 = tracker.request_migration(make_request("old")).unwrap();
        tracker.complete_migration(&r1.migration_id);

        // Manually backdate: remove and reinsert with old timestamp.
        if let Some((_, mut record)) = tracker.completed.remove(&r1.migration_id) {
            record.completed_at = Some(Utc::now() - chrono::Duration::hours(2));
            tracker.completed.insert(record.id.clone(), record);
        }

        // Create a fresh completed migration.
        let r2 = tracker.request_migration(make_request("fresh")).unwrap();
        tracker.complete_migration(&r2.migration_id);

        // Prune anything older than 1 hour.
        let pruned = tracker.prune_completed(Duration::from_secs(3600));
        assert_eq!(pruned, 1);

        // The old one should be gone, the fresh one should remain.
        assert!(tracker.get_migration(&r1.migration_id).is_none());
        assert!(tracker.get_migration(&r2.migration_id).is_some());
    }

    #[test]
    fn prune_completed_nothing_to_prune() {
        let tracker = MigrationTracker::new(16);
        let pruned = tracker.prune_completed(Duration::from_secs(3600));
        assert_eq!(pruned, 0);
    }

    #[test]
    fn prune_completed_does_not_touch_active() {
        let tracker = MigrationTracker::new(16);
        tracker.request_migration(make_request("active")).unwrap();

        let pruned = tracker.prune_completed(Duration::from_secs(0));
        assert_eq!(pruned, 0);
        assert_eq!(tracker.active_count(), 1);
    }

    // ---- lifecycle flow ----

    #[test]
    fn full_lifecycle_pending_to_completed() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("lifecycle")).unwrap();
        let id = &response.migration_id;

        // Pending.
        assert_eq!(tracker.get_migration(id).unwrap().state, MigrationState::Pending);

        // -> InProgress.
        tracker.update_state(id, MigrationState::InProgress);
        assert_eq!(tracker.get_migration(id).unwrap().state, MigrationState::InProgress);

        // Set target.
        tracker.set_target(id, "node-dest".into());
        assert_eq!(
            tracker.get_migration(id).unwrap().to_node,
            Some("node-dest".into())
        );

        // -> Completed.
        tracker.complete_migration(id);
        let final_record = tracker.get_migration(id).unwrap();
        assert_eq!(final_record.state, MigrationState::Completed);
        assert!(final_record.completed_at.is_some());
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn full_lifecycle_pending_to_failed() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("lifecycle")).unwrap();
        let id = &response.migration_id;

        tracker.update_state(id, MigrationState::InProgress);
        tracker.fail_migration(id, "network timeout".into());

        let final_record = tracker.get_migration(id).unwrap();
        assert_eq!(final_record.state, MigrationState::Failed);
        assert_eq!(final_record.error, Some("network timeout".into()));
        assert_eq!(tracker.active_count(), 0);
    }

    #[test]
    fn full_lifecycle_pending_to_cancelled() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("lifecycle")).unwrap();
        let id = &response.migration_id;

        tracker.cancel_migration(id);

        let final_record = tracker.get_migration(id).unwrap();
        assert_eq!(final_record.state, MigrationState::Cancelled);
        assert_eq!(tracker.active_count(), 0);
    }

    // ---- edge cases ----

    #[test]
    fn max_concurrent_zero_rejects_all() {
        let tracker = MigrationTracker::new(0);
        let result = tracker.request_migration(make_request("test"));
        assert!(result.is_err());
    }

    #[test]
    fn migration_id_is_valid_uuid() {
        let tracker = MigrationTracker::new(16);
        let response = tracker.request_migration(make_request("test")).unwrap();
        assert!(Uuid::parse_str(&response.migration_id).is_ok());
    }

    #[test]
    fn concurrent_operations_do_not_panic() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(MigrationTracker::new(100));
        let mut handles = vec![];

        for i in 0..20 {
            let t = Arc::clone(&tracker);
            handles.push(thread::spawn(move || {
                let resp = t
                    .request_migration(make_request(&format!("thread-{}", i)))
                    .unwrap();
                t.update_state(&resp.migration_id, MigrationState::InProgress);
                t.set_target(&resp.migration_id, format!("node-{}", i));
                if i % 3 == 0 {
                    t.complete_migration(&resp.migration_id);
                } else if i % 3 == 1 {
                    t.fail_migration(&resp.migration_id, "error".into());
                } else {
                    t.cancel_migration(&resp.migration_id);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All migrations should have moved to completed.
        assert_eq!(tracker.active_count(), 0);
    }
}
