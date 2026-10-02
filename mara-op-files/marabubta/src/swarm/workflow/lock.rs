// Marabunta - Licensed under the MIT License.
//! Distributed lock mechanism for workflow instance transitions (W3A).
//!
//! Provides a DashMap-based in-memory distributed lock that prevents
//! concurrent transitions on the same workflow instance. Each lock entry
//! tracks the holder node, acquisition time, and TTL. Locks that exceed
//! their TTL are considered expired and can be stolen by other nodes.
//!
//! In a production deployment this would be backed by a consensus protocol
//! (Raft, Paxos) or an external coordinator. The DashMap implementation
//! provides correct single-process semantics and serves as the integration
//! point for a future distributed backend.

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::warn;

use crate::swarm::config::{WF_LOCK_MAX_RETRIES, WF_LOCK_RETRY_MS, WF_LOCK_TTL_SECS};

// ============================================================================
// Types
// ============================================================================

/// A single lock entry in the distributed lock store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockEntry {
    /// Node ID that currently holds the lock.
    pub holder_node: String,
    /// Timestamp (epoch millis) when the lock was acquired or last refreshed.
    pub acquired_at_ms: u64,
    /// Time-to-live in seconds. After `acquired_at_ms + ttl_secs * 1000` the
    /// lock is considered expired and may be stolen.
    pub ttl_secs: u32,
}

/// Read-only snapshot of a lock's current state.
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Node ID that currently holds the lock.
    pub holder_node: String,
    /// Remaining TTL in milliseconds (0 if expired).
    pub remaining_ttl_ms: u64,
}

/// Errors that can occur during lock operations.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("lock acquisition timed out after {0} retries")]
    Timeout(u32),

    #[error("not the lock holder (expected {expected}, actual {actual})")]
    NotHolder { expected: String, actual: String },

    #[error("lock not found or already expired")]
    Expired,

    #[error("store error: {0}")]
    StoreError(String),
}

// ============================================================================
// DistributedLock
// ============================================================================

/// In-memory distributed lock backed by a concurrent DashMap.
///
/// Each key is a resource identifier (typically a workflow instance ID).
/// The value is a [`LockEntry`] recording who holds the lock and when
/// it expires.
#[derive(Debug, Clone)]
pub struct DistributedLock {
    pub(crate) store: Arc<DashMap<String, LockEntry>>,
    pub(crate) node_id: String,
}

impl DistributedLock {
    /// Create a new distributed lock scoped to the given node.
    pub fn new(node_id: String) -> Self {
        Self {
            store: Arc::new(DashMap::new()),
            node_id,
        }
    }

    /// Current epoch millis from chrono.
    fn now_ms() -> u64 {
        Utc::now().timestamp_millis() as u64
    }

    /// Check whether a lock entry has expired relative to `now_ms`.
    fn is_expired(entry: &LockEntry, now_ms: u64) -> bool {
        let expiry_ms = entry.acquired_at_ms + (entry.ttl_secs as u64) * 1000;
        now_ms >= expiry_ms
    }

    /// Try to acquire a lock on `resource` with the configured TTL.
    ///
    /// Retries up to `WF_LOCK_MAX_RETRIES` times with `WF_LOCK_RETRY_MS`
    /// delay between attempts. If the lock is held by another node and has
    /// not expired, the caller waits and retries.
    pub async fn acquire(&self, resource: &str) -> Result<DistributedLockGuard, LockError> {
        self.acquire_with_ttl(resource, WF_LOCK_TTL_SECS).await
    }

    /// Try to acquire a lock with a custom TTL (seconds).
    pub async fn acquire_with_ttl(
        &self,
        resource: &str,
        ttl_secs: u32,
    ) -> Result<DistributedLockGuard, LockError> {
        for attempt in 0..WF_LOCK_MAX_RETRIES {
            let now = Self::now_ms();

            // Try to insert or replace an expired entry.
            let acquired = {
                let entry_ref = self.store.get(resource);
                match entry_ref {
                    None => true,
                    Some(ref existing) => Self::is_expired(existing.value(), now),
                }
            };

            if acquired {
                let new_entry = LockEntry {
                    holder_node: self.node_id.clone(),
                    acquired_at_ms: now,
                    ttl_secs,
                };
                self.store.insert(resource.to_string(), new_entry);
                return Ok(DistributedLockGuard {
                    lock: self.clone(),
                    resource: resource.to_string(),
                });
            }

            // Lock is held by someone else and not expired -- wait and retry.
            if attempt + 1 < WF_LOCK_MAX_RETRIES {
                sleep(std::time::Duration::from_millis(WF_LOCK_RETRY_MS)).await;
            }
        }

        warn!(
            resource = resource,
            node = %self.node_id,
            "lock acquisition timed out"
        );
        Err(LockError::Timeout(WF_LOCK_MAX_RETRIES))
    }

    /// Release a previously acquired lock.
    ///
    /// Returns `Err(NotHolder)` if the caller is not the current holder.
    /// Returns `Err(Expired)` if the lock has already expired or does not exist.
    pub fn release(&self, resource: &str) -> Result<(), LockError> {
        let removed = self.store.remove_if(resource, |_k, entry| {
            entry.holder_node == self.node_id
        });

        match removed {
            Some(_) => Ok(()),
            None => {
                // Check why removal failed.
                match self.store.get(resource) {
                    Some(ref entry) => Err(LockError::NotHolder {
                        expected: self.node_id.clone(),
                        actual: entry.holder_node.clone(),
                    }),
                    None => Err(LockError::Expired),
                }
            }
        }
    }

    /// Refresh (extend) the TTL of a held lock.
    ///
    /// Updates `acquired_at_ms` to the current time, effectively resetting
    /// the TTL countdown. Only the current holder may refresh.
    pub fn refresh(&self, resource: &str) -> Result<(), LockError> {
        let now = Self::now_ms();

        match self.store.get_mut(resource) {
            Some(mut entry) => {
                if entry.holder_node != self.node_id {
                    return Err(LockError::NotHolder {
                        expected: self.node_id.clone(),
                        actual: entry.holder_node.clone(),
                    });
                }
                if Self::is_expired(&entry, now) {
                    return Err(LockError::Expired);
                }
                entry.acquired_at_ms = now;
                Ok(())
            }
            None => Err(LockError::Expired),
        }
    }

    /// Check whether a resource is currently locked (and not expired).
    pub fn is_locked(&self, resource: &str) -> bool {
        match self.store.get(resource) {
            Some(ref entry) => !Self::is_expired(entry.value(), Self::now_ms()),
            None => false,
        }
    }

    /// Return information about the current lock holder, if any.
    pub fn info(&self, resource: &str) -> Option<LockInfo> {
        self.store.get(resource).and_then(|entry| {
            let now = Self::now_ms();
            let expiry_ms = entry.acquired_at_ms + (entry.ttl_secs as u64) * 1000;
            if now >= expiry_ms {
                None
            } else {
                Some(LockInfo {
                    holder_node: entry.holder_node.clone(),
                    remaining_ttl_ms: expiry_ms - now,
                })
            }
        })
    }
}

// ============================================================================
// DistributedLockGuard (RAII)
// ============================================================================

/// RAII guard that releases the lock on drop.
///
/// When dropped, the guard spawns a background tokio task to release the
/// lock so that `Drop` does not block.
pub struct DistributedLockGuard {
    lock: DistributedLock,
    resource: String,
}

impl DistributedLockGuard {
    /// Explicitly release the lock (preferred over relying on Drop).
    pub fn release(self) -> Result<(), LockError> {
        // Prevent Drop from running a second release.
        let resource = self.resource.clone();
        let lock = self.lock.clone();
        std::mem::forget(self);
        lock.release(&resource)
    }

    /// The resource key this guard protects.
    pub fn resource(&self) -> &str {
        &self.resource
    }
}

impl Drop for DistributedLockGuard {
    fn drop(&mut self) {
        let lock = self.lock.clone();
        let resource = self.resource.clone();
        tokio::spawn(async move {
            if let Err(e) = lock.release(&resource) {
                warn!(resource = %resource, error = %e, "failed to release lock on drop");
            }
        });
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_and_release() {
        let lock = DistributedLock::new("node-1".to_string());
        let guard = lock.acquire("wf-instance-1").await.unwrap();
        assert!(lock.is_locked("wf-instance-1"));
        guard.release().unwrap();
        // After explicit release, the entry is removed.
        assert!(!lock.is_locked("wf-instance-1"));
    }

    #[tokio::test]
    async fn test_acquire_contention() {
        let lock_a = DistributedLock::new("node-a".to_string());
        let lock_b = DistributedLock {
            store: lock_a.store.clone(),
            node_id: "node-b".to_string(),
        };

        // Node A acquires.
        let _guard_a = lock_a.acquire_with_ttl("res-1", 60).await.unwrap();

        // Node B should timeout (use tiny retry count via direct loop).
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            lock_b.acquire_with_ttl("res-1", 60),
        )
        .await;

        // Either Timeout error from lock or from tokio -- both acceptable.
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_ttl_expiry() {
        let lock = DistributedLock::new("node-1".to_string());
        // Acquire with 1 second TTL.
        let _guard = lock.acquire_with_ttl("res-ttl", 1).await.unwrap();
        assert!(lock.is_locked("res-ttl"));

        // Wait for TTL to expire.
        sleep(std::time::Duration::from_millis(1100)).await;
        assert!(!lock.is_locked("res-ttl"));

        // Another node should be able to acquire after expiry.
        let lock2 = DistributedLock {
            store: lock.store.clone(),
            node_id: "node-2".to_string(),
        };
        let guard2 = lock2.acquire_with_ttl("res-ttl", 30).await.unwrap();
        assert!(lock2.is_locked("res-ttl"));
        guard2.release().unwrap();
    }

    #[tokio::test]
    async fn test_refresh_extends_ttl() {
        let lock = DistributedLock::new("node-1".to_string());
        let _guard = lock.acquire_with_ttl("res-refresh", 2).await.unwrap();

        // Wait 1 second, then refresh.
        sleep(std::time::Duration::from_millis(1000)).await;
        lock.refresh("res-refresh").unwrap();

        // Wait another 1.5 seconds -- original TTL (2s) would have expired
        // but refresh reset it.
        sleep(std::time::Duration::from_millis(1500)).await;
        assert!(lock.is_locked("res-refresh"));
    }

    #[tokio::test]
    async fn test_release_wrong_holder() {
        let lock_a = DistributedLock::new("node-a".to_string());
        let lock_b = DistributedLock {
            store: lock_a.store.clone(),
            node_id: "node-b".to_string(),
        };

        let _guard = lock_a.acquire("res-wrong").await.unwrap();

        // Node B tries to release node A's lock.
        let result = lock_b.release("res-wrong");
        assert!(result.is_err());
        match result.unwrap_err() {
            LockError::NotHolder { expected, actual } => {
                assert_eq!(expected, "node-b");
                assert_eq!(actual, "node-a");
            }
            other => panic!("expected NotHolder, got {:?}", other),
        }
    }
}
