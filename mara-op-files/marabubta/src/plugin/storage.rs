// Marabunta - Licensed under the MIT License.
//! Key-value storage engine for the plugin system.
//!
//! Provides [`PluginStorage`], a concurrent in-memory key-value store that
//! plugins use to persist data in the swarm via Store/Fetch/Delete operations.
//!
//! The store is backed by [`DashMap`] for lock-free concurrent reads and
//! fine-grained write locking. Each entry carries a monotonic version counter,
//! optional TTL, consistency level, and replica count. Expired entries are
//! lazily pruned on fetch and eagerly pruned via [`PluginStorage::prune_expired`].

use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::{debug, trace, warn};

use crate::plugin::types::{
    Consistency, DeleteRequest, DeleteResponse, FetchRequest, FetchResponse, PluginError,
    PluginResult, StoreRequest, StoreResponse,
};

// ============================================================================
// Constants
// ============================================================================

/// Default maximum number of entries the storage can hold.
const DEFAULT_MAX_ENTRIES: usize = 1_000_000;

/// Default maximum value size in bytes (64 MB).
const DEFAULT_MAX_VALUE_SIZE: usize = 64 * 1024 * 1024;

/// Default maximum key size in bytes.
const DEFAULT_MAX_KEY_SIZE: usize = 4096;

// ============================================================================
// StoredEntry
// ============================================================================

/// An entry stored in the plugin key-value store.
struct StoredEntry {
    /// The raw value bytes.
    value: Vec<u8>,
    /// Monotonically increasing version counter for this key.
    version: u64,
    /// Consistency level requested at write time.
    consistency: Consistency,
    /// Number of replicas requested at write time.
    replicas: u32,
    /// When this entry was created or last updated.
    created_at: Instant,
    /// Optional time-to-live. `None` means the entry never expires.
    ttl: Option<Duration>,
}

impl StoredEntry {
    /// Returns `true` if this entry has a TTL and that TTL has elapsed.
    fn is_expired(&self) -> bool {
        match self.ttl {
            Some(ttl) => self.created_at.elapsed() >= ttl,
            None => false,
        }
    }
}

// ============================================================================
// PluginStorage
// ============================================================================

/// Concurrent key-value storage engine for plugins.
///
/// All operations are safe to call from multiple async tasks concurrently.
/// Version counters are maintained per-key and increment on every successful
/// store operation, enabling optimistic concurrency control.
pub struct PluginStorage {
    /// Primary key-value store.
    entries: DashMap<Vec<u8>, StoredEntry>,
    /// Per-key version counters (kept in sync with `entries`).
    versions: DashMap<Vec<u8>, u64>,
    /// Keys that have a TTL, mapped to their deadline instant for efficient scanning.
    ttl_entries: DashMap<Vec<u8>, Instant>,
    /// Maximum number of entries allowed in the store.
    max_entries: usize,
    /// Maximum allowed size of a single value in bytes.
    max_value_size: usize,
}

impl PluginStorage {
    /// Create a new `PluginStorage` with the given capacity limits.
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of key-value pairs. Use `0` for the default
    ///   ([`DEFAULT_MAX_ENTRIES`]).
    /// * `max_value_size` - Maximum size of a single value in bytes. Use `0` for the
    ///   default ([`DEFAULT_MAX_VALUE_SIZE`]).
    pub fn new(max_entries: usize, max_value_size: usize) -> Self {
        let max_entries = if max_entries == 0 {
            DEFAULT_MAX_ENTRIES
        } else {
            max_entries
        };
        let max_value_size = if max_value_size == 0 {
            DEFAULT_MAX_VALUE_SIZE
        } else {
            max_value_size
        };

        Self {
            entries: DashMap::new(),
            versions: DashMap::new(),
            ttl_entries: DashMap::new(),
            max_entries,
            max_value_size,
        }
    }

    /// Store a key-value pair.
    ///
    /// Validates key and value sizes, atomically inserts or updates the entry,
    /// increments the version counter, and records TTL metadata when applicable.
    /// Returns the new version number on success.
    pub async fn store(&self, request: StoreRequest) -> PluginResult<StoreResponse> {
        // Validate key size.
        if request.key.is_empty() {
            return Err(PluginError::Storage("key must not be empty".into()));
        }
        if request.key.len() > DEFAULT_MAX_KEY_SIZE {
            return Err(PluginError::Storage(format!(
                "key size {} exceeds maximum {}",
                request.key.len(),
                DEFAULT_MAX_KEY_SIZE
            )));
        }

        // Validate value size.
        if request.value.len() > self.max_value_size {
            return Err(PluginError::Storage(format!(
                "value size {} exceeds maximum {}",
                request.value.len(),
                self.max_value_size
            )));
        }

        // Enforce capacity limits before inserting a new key.
        if !self.entries.contains_key(&request.key) && self.entries.len() >= self.max_entries {
            return Err(PluginError::CapacityExceeded(format!(
                "storage full: {} entries (max {})",
                self.entries.len(),
                self.max_entries
            )));
        }

        // Compute the next version for this key.
        let new_version = {
            let mut ver = self.versions.entry(request.key.clone()).or_insert(0);
            *ver += 1;
            *ver
        };

        // Build TTL.
        let ttl = if request.options.ttl_seconds > 0 {
            Some(Duration::from_secs(request.options.ttl_seconds as u64))
        } else {
            None
        };

        let now = Instant::now();

        // Track TTL deadline for efficient pruning.
        if let Some(ttl_dur) = ttl {
            self.ttl_entries
                .insert(request.key.clone(), now + ttl_dur);
        } else {
            // If updating an entry that previously had a TTL, remove the TTL tracking.
            self.ttl_entries.remove(&request.key);
        }

        // Insert or replace the entry.
        let entry = StoredEntry {
            value: request.value,
            version: new_version,
            consistency: request.options.consistency,
            replicas: request.options.replicas,
            created_at: now,
            ttl,
        };
        self.entries.insert(request.key.clone(), entry);

        debug!(
            key_len = request.key.len(),
            version = new_version,
            ttl_seconds = request.options.ttl_seconds,
            "plugin storage: stored entry"
        );

        Ok(StoreResponse {
            success: true,
            error: String::new(),
            version: new_version,
        })
    }

    /// Fetch a value by key.
    ///
    /// Checks for TTL expiry (removes and returns not-found if expired),
    /// enforces `min_version` from fetch options, and returns the value
    /// with its current version.
    pub async fn fetch(&self, request: FetchRequest) -> PluginResult<FetchResponse> {
        if request.key.is_empty() {
            return Err(PluginError::Storage("key must not be empty".into()));
        }

        // Look up the entry.
        let entry_ref = match self.entries.get(&request.key) {
            Some(entry) => entry,
            None => {
                trace!(key_len = request.key.len(), "plugin storage: key not found");
                return Ok(FetchResponse {
                    found: false,
                    value: Vec::new(),
                    version: 0,
                    error: String::new(),
                });
            }
        };

        // Check TTL expiry.
        if entry_ref.is_expired() {
            // Drop the read guard before mutating.
            let version = entry_ref.version;
            drop(entry_ref);

            self.entries.remove(&request.key);
            self.versions.remove(&request.key);
            self.ttl_entries.remove(&request.key);

            debug!(
                key_len = request.key.len(),
                version,
                "plugin storage: entry expired on fetch"
            );

            return Ok(FetchResponse {
                found: false,
                value: Vec::new(),
                version: 0,
                error: String::new(),
            });
        }

        // Check minimum version requirement.
        if request.options.min_version > 0 && entry_ref.version < request.options.min_version {
            let current_version = entry_ref.version;
            drop(entry_ref);

            return Ok(FetchResponse {
                found: false,
                value: Vec::new(),
                version: current_version,
                error: format!(
                    "version {} is below requested minimum {}",
                    current_version, request.options.min_version
                ),
            });
        }

        let value = entry_ref.value.clone();
        let version = entry_ref.version;
        drop(entry_ref);

        trace!(
            key_len = request.key.len(),
            version,
            value_len = value.len(),
            "plugin storage: fetched entry"
        );

        Ok(FetchResponse {
            found: true,
            value,
            version,
            error: String::new(),
        })
    }

    /// Delete a key from the store.
    ///
    /// Removes the entry, its version counter, and any TTL tracking.
    /// Returns success regardless of whether the key existed.
    pub async fn delete(&self, request: DeleteRequest) -> PluginResult<DeleteResponse> {
        if request.key.is_empty() {
            return Err(PluginError::Storage("key must not be empty".into()));
        }

        let existed = self.entries.remove(&request.key).is_some();
        self.versions.remove(&request.key);
        self.ttl_entries.remove(&request.key);

        debug!(
            key_len = request.key.len(),
            existed, "plugin storage: deleted entry"
        );

        Ok(DeleteResponse {
            success: true,
            error: String::new(),
        })
    }

    /// Returns the number of entries currently in the store.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Scan all TTL-tracked entries and remove those that have expired.
    ///
    /// Returns the number of entries pruned.
    pub fn prune_expired(&self) -> usize {
        let now = Instant::now();
        let mut pruned = 0usize;

        // Collect expired keys first to avoid holding DashMap guards during removal.
        let expired_keys: Vec<Vec<u8>> = self
            .ttl_entries
            .iter()
            .filter_map(|entry| {
                if *entry.value() <= now {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        for key in expired_keys {
            // Double-check the entry is still expired (could have been updated).
            let should_remove = self
                .entries
                .get(&key)
                .map(|e| e.is_expired())
                .unwrap_or(false);

            if should_remove {
                self.entries.remove(&key);
                self.versions.remove(&key);
                self.ttl_entries.remove(&key);
                pruned += 1;
            }
        }

        if pruned > 0 {
            debug!(pruned, "plugin storage: pruned expired entries");
        }

        pruned
    }

    /// If the store exceeds `max_entries`, evict the oldest entries until
    /// the count is at or below the limit.
    ///
    /// Returns the number of entries evicted.
    pub fn enforce_limits(&self) -> usize {
        let current = self.entries.len();
        if current <= self.max_entries {
            return 0;
        }

        let to_evict = current - self.max_entries;

        // Collect all keys with their creation timestamps so we can sort by age.
        let mut keyed_times: Vec<(Vec<u8>, Instant)> = self
            .entries
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().created_at))
            .collect();

        // Sort oldest first (smallest Instant = oldest).
        keyed_times.sort_by_key(|(_k, t)| *t);

        let mut evicted = 0usize;
        for (key, _) in keyed_times.into_iter().take(to_evict) {
            self.entries.remove(&key);
            self.versions.remove(&key);
            self.ttl_entries.remove(&key);
            evicted += 1;
        }

        if evicted > 0 {
            warn!(
                evicted,
                max_entries = self.max_entries,
                "plugin storage: evicted entries to enforce capacity limit"
            );
        }

        evicted
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::types::{FetchOptions, StoreOptions};

    /// Helper to build a StoreRequest with defaults.
    fn store_req(key: &[u8], value: &[u8]) -> StoreRequest {
        StoreRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            options: StoreOptions::default(),
        }
    }

    /// Helper to build a StoreRequest with a TTL.
    fn store_req_with_ttl(key: &[u8], value: &[u8], ttl_seconds: u32) -> StoreRequest {
        StoreRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            options: StoreOptions {
                ttl_seconds,
                ..StoreOptions::default()
            },
        }
    }

    /// Helper to build a FetchRequest with defaults.
    fn fetch_req(key: &[u8]) -> FetchRequest {
        FetchRequest {
            key: key.to_vec(),
            options: FetchOptions::default(),
        }
    }

    /// Helper to build a FetchRequest with min_version.
    fn fetch_req_with_min_version(key: &[u8], min_version: u64) -> FetchRequest {
        FetchRequest {
            key: key.to_vec(),
            options: FetchOptions {
                min_version,
                ..FetchOptions::default()
            },
        }
    }

    /// Helper to build a DeleteRequest.
    fn delete_req(key: &[u8]) -> DeleteRequest {
        DeleteRequest {
            key: key.to_vec(),
        }
    }

    #[tokio::test]
    async fn new_storage_is_empty() {
        let storage = PluginStorage::new(100, 1024);
        assert_eq!(storage.entry_count(), 0);
    }

    #[tokio::test]
    async fn new_storage_with_zero_uses_defaults() {
        let storage = PluginStorage::new(0, 0);
        assert_eq!(storage.max_entries, DEFAULT_MAX_ENTRIES);
        assert_eq!(storage.max_value_size, DEFAULT_MAX_VALUE_SIZE);
    }

    #[tokio::test]
    async fn store_and_fetch_basic() {
        let storage = PluginStorage::new(100, 1024);

        let resp = storage.store(store_req(b"hello", b"world")).await.unwrap();
        assert!(resp.success);
        assert_eq!(resp.version, 1);
        assert!(resp.error.is_empty());

        let fetch = storage.fetch(fetch_req(b"hello")).await.unwrap();
        assert!(fetch.found);
        assert_eq!(fetch.value, b"world");
        assert_eq!(fetch.version, 1);
        assert!(fetch.error.is_empty());
    }

    #[tokio::test]
    async fn store_increments_version() {
        let storage = PluginStorage::new(100, 1024);

        let r1 = storage.store(store_req(b"key", b"v1")).await.unwrap();
        assert_eq!(r1.version, 1);

        let r2 = storage.store(store_req(b"key", b"v2")).await.unwrap();
        assert_eq!(r2.version, 2);

        let r3 = storage.store(store_req(b"key", b"v3")).await.unwrap();
        assert_eq!(r3.version, 3);

        let fetch = storage.fetch(fetch_req(b"key")).await.unwrap();
        assert!(fetch.found);
        assert_eq!(fetch.value, b"v3");
        assert_eq!(fetch.version, 3);
    }

    #[tokio::test]
    async fn store_overwrites_value() {
        let storage = PluginStorage::new(100, 1024);

        storage.store(store_req(b"key", b"old")).await.unwrap();
        storage.store(store_req(b"key", b"new")).await.unwrap();

        let fetch = storage.fetch(fetch_req(b"key")).await.unwrap();
        assert_eq!(fetch.value, b"new");
    }

    #[tokio::test]
    async fn fetch_nonexistent_key() {
        let storage = PluginStorage::new(100, 1024);

        let fetch = storage.fetch(fetch_req(b"missing")).await.unwrap();
        assert!(!fetch.found);
        assert!(fetch.value.is_empty());
        assert_eq!(fetch.version, 0);
    }

    #[tokio::test]
    async fn delete_existing_key() {
        let storage = PluginStorage::new(100, 1024);

        storage.store(store_req(b"key", b"value")).await.unwrap();
        assert_eq!(storage.entry_count(), 1);

        let resp = storage.delete(delete_req(b"key")).await.unwrap();
        assert!(resp.success);
        assert!(resp.error.is_empty());
        assert_eq!(storage.entry_count(), 0);

        let fetch = storage.fetch(fetch_req(b"key")).await.unwrap();
        assert!(!fetch.found);
    }

    #[tokio::test]
    async fn delete_nonexistent_key_succeeds() {
        let storage = PluginStorage::new(100, 1024);

        let resp = storage.delete(delete_req(b"ghost")).await.unwrap();
        assert!(resp.success);
    }

    #[tokio::test]
    async fn store_empty_key_rejected() {
        let storage = PluginStorage::new(100, 1024);

        let result = storage.store(store_req(b"", b"value")).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Storage(msg) => assert!(msg.contains("empty")),
            other => panic!("expected Storage error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn fetch_empty_key_rejected() {
        let storage = PluginStorage::new(100, 1024);

        let result = storage.fetch(fetch_req(b"")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn delete_empty_key_rejected() {
        let storage = PluginStorage::new(100, 1024);

        let result = storage.delete(delete_req(b"")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn store_key_too_large_rejected() {
        let storage = PluginStorage::new(100, 1024);
        let big_key = vec![0xAA; DEFAULT_MAX_KEY_SIZE + 1];

        let result = storage.store(store_req(&big_key, b"val")).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Storage(msg) => assert!(msg.contains("key size")),
            other => panic!("expected Storage error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn store_value_too_large_rejected() {
        let storage = PluginStorage::new(100, 128);
        let big_value = vec![0xBB; 129];

        let result = storage.store(store_req(b"key", &big_value)).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Storage(msg) => assert!(msg.contains("value size")),
            other => panic!("expected Storage error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn store_capacity_exceeded() {
        let storage = PluginStorage::new(2, 1024);

        storage.store(store_req(b"k1", b"v1")).await.unwrap();
        storage.store(store_req(b"k2", b"v2")).await.unwrap();

        let result = storage.store(store_req(b"k3", b"v3")).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::CapacityExceeded(msg) => assert!(msg.contains("storage full")),
            other => panic!("expected CapacityExceeded error, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn store_existing_key_does_not_count_as_new_entry() {
        let storage = PluginStorage::new(2, 1024);

        storage.store(store_req(b"k1", b"v1")).await.unwrap();
        storage.store(store_req(b"k2", b"v2")).await.unwrap();

        // Updating an existing key should succeed even at capacity.
        let resp = storage.store(store_req(b"k1", b"updated")).await.unwrap();
        assert!(resp.success);
    }

    #[tokio::test]
    async fn fetch_with_min_version_satisfied() {
        let storage = PluginStorage::new(100, 1024);

        storage.store(store_req(b"key", b"v1")).await.unwrap();
        storage.store(store_req(b"key", b"v2")).await.unwrap();
        storage.store(store_req(b"key", b"v3")).await.unwrap();

        let fetch = storage
            .fetch(fetch_req_with_min_version(b"key", 2))
            .await
            .unwrap();
        assert!(fetch.found);
        assert_eq!(fetch.version, 3);
        assert_eq!(fetch.value, b"v3");
    }

    #[tokio::test]
    async fn fetch_with_min_version_not_met() {
        let storage = PluginStorage::new(100, 1024);

        storage.store(store_req(b"key", b"v1")).await.unwrap();

        let fetch = storage
            .fetch(fetch_req_with_min_version(b"key", 5))
            .await
            .unwrap();
        assert!(!fetch.found);
        assert_eq!(fetch.version, 1);
        assert!(fetch.error.contains("below requested minimum"));
    }

    #[tokio::test]
    async fn fetch_with_min_version_zero_always_matches() {
        let storage = PluginStorage::new(100, 1024);

        storage.store(store_req(b"key", b"val")).await.unwrap();

        let fetch = storage
            .fetch(fetch_req_with_min_version(b"key", 0))
            .await
            .unwrap();
        assert!(fetch.found);
    }

    #[tokio::test]
    async fn ttl_entry_expires_on_fetch() {
        let storage = PluginStorage::new(100, 1024);

        // Store with 1-second TTL.
        storage
            .store(store_req_with_ttl(b"ephemeral", b"data", 1))
            .await
            .unwrap();

        // Should be found immediately.
        let fetch = storage.fetch(fetch_req(b"ephemeral")).await.unwrap();
        assert!(fetch.found);

        // Wait for expiry.
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Should be gone now.
        let fetch = storage.fetch(fetch_req(b"ephemeral")).await.unwrap();
        assert!(!fetch.found);
        assert_eq!(storage.entry_count(), 0);
    }

    #[tokio::test]
    async fn ttl_zero_means_no_expiry() {
        let storage = PluginStorage::new(100, 1024);

        storage
            .store(store_req_with_ttl(b"forever", b"data", 0))
            .await
            .unwrap();

        // Verify no TTL tracking.
        assert!(!storage.ttl_entries.contains_key(&b"forever".to_vec()));

        let fetch = storage.fetch(fetch_req(b"forever")).await.unwrap();
        assert!(fetch.found);
    }

    #[tokio::test]
    async fn update_removes_ttl_tracking_when_set_to_zero() {
        let storage = PluginStorage::new(100, 1024);

        // Store with TTL.
        storage
            .store(store_req_with_ttl(b"key", b"v1", 60))
            .await
            .unwrap();
        assert!(storage.ttl_entries.contains_key(&b"key".to_vec()));

        // Update without TTL.
        storage
            .store(store_req_with_ttl(b"key", b"v2", 0))
            .await
            .unwrap();
        assert!(!storage.ttl_entries.contains_key(&b"key".to_vec()));
    }

    #[tokio::test]
    async fn prune_expired_removes_stale_entries() {
        let storage = PluginStorage::new(100, 1024);

        // Store entries with 1-second TTL.
        storage
            .store(store_req_with_ttl(b"exp1", b"data1", 1))
            .await
            .unwrap();
        storage
            .store(store_req_with_ttl(b"exp2", b"data2", 1))
            .await
            .unwrap();
        // Store entry without TTL.
        storage.store(store_req(b"perm", b"data3")).await.unwrap();

        assert_eq!(storage.entry_count(), 3);

        // Wait for TTL to expire.
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let pruned = storage.prune_expired();
        assert_eq!(pruned, 2);
        assert_eq!(storage.entry_count(), 1);

        // Permanent entry is still there.
        let fetch = storage.fetch(fetch_req(b"perm")).await.unwrap();
        assert!(fetch.found);
    }

    #[tokio::test]
    async fn prune_expired_returns_zero_when_nothing_expired() {
        let storage = PluginStorage::new(100, 1024);

        storage.store(store_req(b"key", b"val")).await.unwrap();

        let pruned = storage.prune_expired();
        assert_eq!(pruned, 0);
    }

    #[tokio::test]
    async fn enforce_limits_evicts_oldest() {
        let storage = PluginStorage::new(3, 1024);

        // Insert in order with slight delays so creation times differ.
        storage.store(store_req(b"oldest", b"v1")).await.unwrap();
        // Use a small sleep to ensure Instant ordering.
        tokio::time::sleep(Duration::from_millis(5)).await;
        storage.store(store_req(b"middle", b"v2")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        storage.store(store_req(b"newest", b"v3")).await.unwrap();

        assert_eq!(storage.entry_count(), 3);

        // Temporarily lower the limit by inserting beyond capacity via direct map access.
        // Instead, we create a storage with limit 2 and move entries.
        let small_storage = PluginStorage::new(2, 1024);
        // Re-insert all 3 entries.
        small_storage
            .store(store_req(b"oldest", b"v1"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        small_storage
            .store(store_req(b"middle", b"v2"))
            .await
            .unwrap();

        // Directly insert a third entry into the DashMap to exceed the limit.
        small_storage.entries.insert(
            b"newest".to_vec(),
            StoredEntry {
                value: b"v3".to_vec(),
                version: 1,
                consistency: Consistency::Eventual,
                replicas: 3,
                created_at: Instant::now(),
                ttl: None,
            },
        );
        small_storage.versions.insert(b"newest".to_vec(), 1);

        assert_eq!(small_storage.entry_count(), 3);

        let evicted = small_storage.enforce_limits();
        assert_eq!(evicted, 1);
        assert_eq!(small_storage.entry_count(), 2);

        // The oldest should have been evicted.
        let fetch = small_storage.fetch(fetch_req(b"oldest")).await.unwrap();
        assert!(!fetch.found);

        // The newer entries should still be present.
        let fetch = small_storage.fetch(fetch_req(b"middle")).await.unwrap();
        assert!(fetch.found);
        let fetch = small_storage.fetch(fetch_req(b"newest")).await.unwrap();
        assert!(fetch.found);
    }

    #[tokio::test]
    async fn enforce_limits_does_nothing_within_capacity() {
        let storage = PluginStorage::new(10, 1024);

        storage.store(store_req(b"k1", b"v1")).await.unwrap();
        storage.store(store_req(b"k2", b"v2")).await.unwrap();

        let evicted = storage.enforce_limits();
        assert_eq!(evicted, 0);
    }

    #[tokio::test]
    async fn entry_count_tracks_inserts_and_deletes() {
        let storage = PluginStorage::new(100, 1024);

        assert_eq!(storage.entry_count(), 0);

        storage.store(store_req(b"a", b"1")).await.unwrap();
        assert_eq!(storage.entry_count(), 1);

        storage.store(store_req(b"b", b"2")).await.unwrap();
        assert_eq!(storage.entry_count(), 2);

        // Update existing key should not change count.
        storage.store(store_req(b"a", b"updated")).await.unwrap();
        assert_eq!(storage.entry_count(), 2);

        storage.delete(delete_req(b"a")).await.unwrap();
        assert_eq!(storage.entry_count(), 1);

        storage.delete(delete_req(b"b")).await.unwrap();
        assert_eq!(storage.entry_count(), 0);
    }

    #[tokio::test]
    async fn store_with_strong_consistency() {
        let storage = PluginStorage::new(100, 1024);

        let request = StoreRequest {
            key: b"strong-key".to_vec(),
            value: b"strong-value".to_vec(),
            options: StoreOptions {
                consistency: Consistency::Strong,
                replicas: 5,
                ttl_seconds: 0,
            },
        };

        let resp = storage.store(request).await.unwrap();
        assert!(resp.success);
        assert_eq!(resp.version, 1);

        // Verify the entry stores the consistency and replica metadata.
        let entry = storage.entries.get(&b"strong-key".to_vec()).unwrap();
        assert_eq!(entry.consistency, Consistency::Strong);
        assert_eq!(entry.replicas, 5);
    }

    #[tokio::test]
    async fn store_empty_value_allowed() {
        let storage = PluginStorage::new(100, 1024);

        let resp = storage.store(store_req(b"key", b"")).await.unwrap();
        assert!(resp.success);

        let fetch = storage.fetch(fetch_req(b"key")).await.unwrap();
        assert!(fetch.found);
        assert!(fetch.value.is_empty());
    }

    #[tokio::test]
    async fn store_max_key_size_accepted() {
        let storage = PluginStorage::new(100, 1024);
        let key = vec![0xCC; DEFAULT_MAX_KEY_SIZE];

        let resp = storage.store(store_req(&key, b"val")).await.unwrap();
        assert!(resp.success);

        let fetch = storage.fetch(fetch_req(&key)).await.unwrap();
        assert!(fetch.found);
        assert_eq!(fetch.value, b"val");
    }

    #[tokio::test]
    async fn store_max_value_size_accepted() {
        let max_val = 256;
        let storage = PluginStorage::new(100, max_val);
        let value = vec![0xDD; max_val];

        let resp = storage.store(store_req(b"key", &value)).await.unwrap();
        assert!(resp.success);

        let fetch = storage.fetch(fetch_req(b"key")).await.unwrap();
        assert!(fetch.found);
        assert_eq!(fetch.value.len(), max_val);
    }

    #[tokio::test]
    async fn delete_cleans_up_version_and_ttl_tracking() {
        let storage = PluginStorage::new(100, 1024);

        storage
            .store(store_req_with_ttl(b"tracked", b"data", 60))
            .await
            .unwrap();

        assert!(storage.versions.contains_key(&b"tracked".to_vec()));
        assert!(storage.ttl_entries.contains_key(&b"tracked".to_vec()));

        storage.delete(delete_req(b"tracked")).await.unwrap();

        assert!(!storage.versions.contains_key(&b"tracked".to_vec()));
        assert!(!storage.ttl_entries.contains_key(&b"tracked".to_vec()));
    }

    #[tokio::test]
    async fn version_resets_after_delete_and_reinsert() {
        let storage = PluginStorage::new(100, 1024);

        let r1 = storage.store(store_req(b"key", b"v1")).await.unwrap();
        assert_eq!(r1.version, 1);

        storage.store(store_req(b"key", b"v2")).await.unwrap();
        storage.delete(delete_req(b"key")).await.unwrap();

        // Version counter was cleaned up, so re-insert starts from 1 again.
        let r3 = storage.store(store_req(b"key", b"v3")).await.unwrap();
        assert_eq!(r3.version, 1);
    }

    #[tokio::test]
    async fn concurrent_stores_all_succeed() {
        use std::sync::Arc;

        let storage = Arc::new(PluginStorage::new(10_000, 1024));
        let mut handles = Vec::new();

        for i in 0..100u32 {
            let s = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                let key = format!("key-{}", i).into_bytes();
                let value = format!("value-{}", i).into_bytes();
                s.store(store_req(&key, &value)).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(storage.entry_count(), 100);
    }

    #[tokio::test]
    async fn concurrent_store_and_fetch() {
        use std::sync::Arc;

        let storage = Arc::new(PluginStorage::new(10_000, 1024));

        // Pre-populate.
        for i in 0..50u32 {
            let key = format!("key-{}", i).into_bytes();
            let value = format!("value-{}", i).into_bytes();
            storage.store(store_req(&key, &value)).await.unwrap();
        }

        let mut handles = Vec::new();

        // Concurrent reads.
        for i in 0..50u32 {
            let s = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                let key = format!("key-{}", i).into_bytes();
                let fetch = s.fetch(fetch_req(&key)).await.unwrap();
                assert!(fetch.found);
            }));
        }

        // Concurrent writes.
        for i in 50..100u32 {
            let s = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                let key = format!("key-{}", i).into_bytes();
                let value = format!("value-{}", i).into_bytes();
                s.store(store_req(&key, &value)).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(storage.entry_count(), 100);
    }

    #[tokio::test]
    async fn multiple_keys_independent_versions() {
        let storage = PluginStorage::new(100, 1024);

        let r1 = storage.store(store_req(b"alpha", b"a1")).await.unwrap();
        let r2 = storage.store(store_req(b"beta", b"b1")).await.unwrap();
        let r3 = storage.store(store_req(b"alpha", b"a2")).await.unwrap();

        assert_eq!(r1.version, 1);
        assert_eq!(r2.version, 1);
        assert_eq!(r3.version, 2);

        let fa = storage.fetch(fetch_req(b"alpha")).await.unwrap();
        assert_eq!(fa.version, 2);
        let fb = storage.fetch(fetch_req(b"beta")).await.unwrap();
        assert_eq!(fb.version, 1);
    }

    #[tokio::test]
    async fn binary_keys_and_values() {
        let storage = PluginStorage::new(100, 1024);

        let key = vec![0x00, 0xFF, 0x80, 0x01];
        let value = vec![0xDE, 0xAD, 0xBE, 0xEF];

        storage.store(store_req(&key, &value)).await.unwrap();

        let fetch = storage.fetch(fetch_req(&key)).await.unwrap();
        assert!(fetch.found);
        assert_eq!(fetch.value, value);
    }

    #[tokio::test]
    async fn prune_does_not_remove_unexpired_ttl_entries() {
        let storage = PluginStorage::new(100, 1024);

        // Store with a long TTL that will not expire during the test.
        storage
            .store(store_req_with_ttl(b"long-lived", b"data", 3600))
            .await
            .unwrap();

        let pruned = storage.prune_expired();
        assert_eq!(pruned, 0);
        assert_eq!(storage.entry_count(), 1);
    }

    #[tokio::test]
    async fn enforce_limits_with_ttl_entries() {
        let small = PluginStorage::new(2, 1024);

        small
            .store(store_req_with_ttl(b"t1", b"v1", 60))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        small
            .store(store_req_with_ttl(b"t2", b"v2", 60))
            .await
            .unwrap();

        // Force a third entry into the map.
        small.entries.insert(
            b"t3".to_vec(),
            StoredEntry {
                value: b"v3".to_vec(),
                version: 1,
                consistency: Consistency::Eventual,
                replicas: 3,
                created_at: Instant::now(),
                ttl: Some(Duration::from_secs(60)),
            },
        );
        small.versions.insert(b"t3".to_vec(), 1);
        small
            .ttl_entries
            .insert(b"t3".to_vec(), Instant::now() + Duration::from_secs(60));

        let evicted = small.enforce_limits();
        assert_eq!(evicted, 1);
        assert_eq!(small.entry_count(), 2);

        // Verify TTL tracking was cleaned up for the evicted key.
        assert!(!small.ttl_entries.contains_key(&b"t1".to_vec()));
    }

    #[test]
    fn stored_entry_is_expired_with_no_ttl() {
        let entry = StoredEntry {
            value: vec![],
            version: 1,
            consistency: Consistency::Eventual,
            replicas: 3,
            created_at: Instant::now(),
            ttl: None,
        };
        assert!(!entry.is_expired());
    }

    #[test]
    fn stored_entry_is_expired_with_future_ttl() {
        let entry = StoredEntry {
            value: vec![],
            version: 1,
            consistency: Consistency::Eventual,
            replicas: 3,
            created_at: Instant::now(),
            ttl: Some(Duration::from_secs(3600)),
        };
        assert!(!entry.is_expired());
    }

    #[test]
    fn stored_entry_is_expired_with_past_ttl() {
        let entry = StoredEntry {
            value: vec![],
            version: 1,
            consistency: Consistency::Eventual,
            replicas: 3,
            // Created 10 seconds ago with 1-second TTL.
            created_at: Instant::now() - Duration::from_secs(10),
            ttl: Some(Duration::from_secs(1)),
        };
        assert!(entry.is_expired());
    }

    #[test]
    fn constants_have_expected_values() {
        assert_eq!(DEFAULT_MAX_ENTRIES, 1_000_000);
        assert_eq!(DEFAULT_MAX_VALUE_SIZE, 64 * 1024 * 1024);
        assert_eq!(DEFAULT_MAX_KEY_SIZE, 4096);
    }
}
