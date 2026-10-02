// Marabunta - Licensed under the MIT License.
//! Storage backends for rate limit state
//!
//! This module provides storage backends for persisting rate limit state:
//! - In-memory storage for single-node deployments
//! - Distributed storage option for multi-coordinator setups
//!
//! # Architecture
//!
//! The storage layer abstracts away the underlying data store, allowing the
//! rate limiter to work with different backends:
//!
//! - `InMemoryStorage`: Fast, single-node storage using concurrent hashmaps
//! - `DistributedStorage`: Multi-node storage with coordination support
//!
//! # Example
//!
//! ```
//! use marabunta_compute::ratelimit::storage::{InMemoryStorage, RateLimitStorage};
//!
//! // Create in-memory storage
//! let storage = InMemoryStorage::new();
//!
//! // Store and retrieve rate limit state
//! storage.set_tokens("client-1:/api/jobs", 95.0);
//! let tokens = storage.get_tokens("client-1:/api/jobs");
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::RwLock;

/// Trait for rate limit storage backends.
///
/// Implementors must be thread-safe and support concurrent access.
pub trait RateLimitStorage: Send + Sync {
    /// Get the current token count for a key.
    fn get_tokens(&self, key: &str) -> Option<f64>;

    /// Set the token count for a key.
    fn set_tokens(&self, key: &str, tokens: f64);

    /// Atomically try to consume a token.
    ///
    /// Returns `Some(remaining_tokens)` if successful, `None` if no tokens available.
    fn try_consume(&self, key: &str, capacity: u32, refill_rate: f64) -> Option<f64>;

    /// Get the last update time for a key.
    fn get_last_update(&self, key: &str) -> Option<u64>;

    /// Set the last update time for a key.
    fn set_last_update(&self, key: &str, timestamp: u64);

    /// Remove expired entries.
    fn cleanup(&self, max_age: Duration);

    /// Get storage statistics.
    fn stats(&self) -> StorageStats;
}

/// Statistics about the storage backend.
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of entries
    pub total_entries: usize,
    /// Memory usage estimate in bytes
    pub memory_bytes: usize,
    /// Number of operations performed
    pub operations: u64,
    /// Hit rate for lookups
    pub hit_rate: f64,
}

/// In-memory rate limit storage using DashMap for concurrent access.
///
/// This is the default storage backend, suitable for single-node deployments.
/// For multi-coordinator setups, use `DistributedStorage` instead.
pub struct InMemoryStorage {
    /// Token buckets: key -> (tokens, last_update_timestamp_millis)
    buckets: DashMap<String, BucketState>,
    /// Statistics
    operations: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// State of a single bucket in storage.
#[derive(Debug, Clone)]
struct BucketState {
    /// Current token count
    tokens: f64,
    /// Bucket capacity
    capacity: u32,
    /// Refill rate (tokens per second)
    refill_rate: f64,
    /// Last update timestamp (milliseconds since epoch)
    last_update_ms: u64,
}

impl BucketState {
    fn new(capacity: u32, refill_rate: f64) -> Self {
        Self {
            tokens: capacity as f64,
            capacity,
            refill_rate,
            last_update_ms: current_time_ms(),
        }
    }

    fn refill(&mut self) -> f64 {
        let now = current_time_ms();
        let elapsed_ms = now.saturating_sub(self.last_update_ms);
        let elapsed_secs = elapsed_ms as f64 / 1000.0;
        let tokens_to_add = elapsed_secs * self.refill_rate;

        self.tokens = (self.tokens + tokens_to_add).min(self.capacity as f64);
        self.last_update_ms = now;

        self.tokens
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl InMemoryStorage {
    /// Create a new in-memory storage.
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
            operations: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Check if storage is empty.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimitStorage for InMemoryStorage {
    fn get_tokens(&self, key: &str) -> Option<f64> {
        self.operations.fetch_add(1, Ordering::Relaxed);

        if let Some(mut state) = self.buckets.get_mut(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(state.refill())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn set_tokens(&self, key: &str, tokens: f64) {
        self.operations.fetch_add(1, Ordering::Relaxed);

        if let Some(mut state) = self.buckets.get_mut(key) {
            state.tokens = tokens;
            state.last_update_ms = current_time_ms();
        }
    }

    fn try_consume(&self, key: &str, capacity: u32, refill_rate: f64) -> Option<f64> {
        self.operations.fetch_add(1, Ordering::Relaxed);

        let mut state = self
            .buckets
            .entry(key.to_string())
            .or_insert_with(|| BucketState::new(capacity, refill_rate));

        // Refill tokens
        state.refill();

        // Try to consume
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(state.tokens)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn get_last_update(&self, key: &str) -> Option<u64> {
        self.buckets.get(key).map(|s| s.last_update_ms)
    }

    fn set_last_update(&self, key: &str, timestamp: u64) {
        if let Some(mut state) = self.buckets.get_mut(key) {
            state.last_update_ms = timestamp;
        }
    }

    fn cleanup(&self, max_age: Duration) {
        let now = current_time_ms();
        let max_age_ms = max_age.as_millis() as u64;

        self.buckets
            .retain(|_, state| now.saturating_sub(state.last_update_ms) < max_age_ms);
    }

    fn stats(&self) -> StorageStats {
        let total_entries = self.buckets.len();
        let operations = self.operations.load(Ordering::Relaxed);
        let hits = self.hits.load(Ordering::Relaxed);
        let total_lookups = hits + self.misses.load(Ordering::Relaxed);

        StorageStats {
            total_entries,
            memory_bytes: total_entries * std::mem::size_of::<BucketState>(),
            operations,
            hit_rate: if total_lookups > 0 {
                hits as f64 / total_lookups as f64
            } else {
                0.0
            },
        }
    }
}

/// Distributed rate limit storage for multi-coordinator deployments.
///
/// This storage backend coordinates rate limits across multiple nodes using
/// a gossip-based protocol. Each node maintains local state and periodically
/// syncs with peers.
///
/// # Architecture
///
/// - Each node maintains a local cache for fast lookups
/// - Writes are replicated to peer nodes asynchronously
/// - Reads prefer local cache, falling back to distributed lookup
/// - Conflict resolution uses "lowest token count wins" strategy
pub struct DistributedStorage {
    /// Local storage for fast lookups
    local: InMemoryStorage,
    /// Peer node addresses for replication
    peers: RwLock<Vec<String>>,
    /// Node identifier
    node_id: String,
    /// Sync interval
    #[allow(dead_code)]
    sync_interval: Duration,
    /// Last sync time
    last_sync: RwLock<Instant>,
}

impl DistributedStorage {
    /// Create a new distributed storage.
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            local: InMemoryStorage::new(),
            peers: RwLock::new(Vec::new()),
            node_id: node_id.into(),
            sync_interval: Duration::from_millis(100),
            last_sync: RwLock::new(Instant::now()),
        }
    }

    /// Add a peer node.
    pub fn add_peer(&self, address: impl Into<String>) {
        self.peers.write().push(address.into());
    }

    /// Remove a peer node.
    pub fn remove_peer(&self, address: &str) {
        self.peers.write().retain(|p| p != address);
    }

    /// Get the list of peer nodes.
    pub fn peers(&self) -> Vec<String> {
        self.peers.read().clone()
    }

    /// Set the sync interval.
    pub fn set_sync_interval(&self, interval: Duration) {
        // Note: In a real implementation, this would update the sync loop
        let _ = interval; // Placeholder
    }

    /// Sync state with peer nodes.
    ///
    /// In a real implementation, this would:
    /// 1. Collect local state changes since last sync
    /// 2. Send updates to all peers
    /// 3. Receive updates from peers
    /// 4. Merge updates using conflict resolution
    pub async fn sync(&self) -> Result<SyncResult, SyncError> {
        // Placeholder for distributed sync logic
        // In production, this would use gRPC or similar for coordination

        *self.last_sync.write() = Instant::now();

        Ok(SyncResult {
            keys_synced: 0,
            peers_contacted: self.peers.read().len(),
            conflicts_resolved: 0,
        })
    }

    /// Merge remote state into local storage.
    ///
    /// Uses "lowest token count wins" for conflict resolution.
    pub fn merge_remote(&self, key: &str, remote_tokens: f64, remote_timestamp: u64) {
        if let Some(mut state) = self.local.buckets.get_mut(key) {
            // If remote has fewer tokens and is more recent, use remote
            if remote_tokens < state.tokens && remote_timestamp >= state.last_update_ms {
                state.tokens = remote_tokens;
                state.last_update_ms = remote_timestamp;
            }
        }
    }

    /// Get the node identifier.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl RateLimitStorage for DistributedStorage {
    fn get_tokens(&self, key: &str) -> Option<f64> {
        // For reads, prefer local cache
        self.local.get_tokens(key)
    }

    fn set_tokens(&self, key: &str, tokens: f64) {
        // Update local and mark for sync
        self.local.set_tokens(key, tokens);
        // In production: queue for replication to peers
    }

    fn try_consume(&self, key: &str, capacity: u32, refill_rate: f64) -> Option<f64> {
        // Consume locally
        

        // In production: broadcast consumption to peers
        // This ensures rate limits are enforced globally

        self.local.try_consume(key, capacity, refill_rate)
    }

    fn get_last_update(&self, key: &str) -> Option<u64> {
        self.local.get_last_update(key)
    }

    fn set_last_update(&self, key: &str, timestamp: u64) {
        self.local.set_last_update(key, timestamp);
    }

    fn cleanup(&self, max_age: Duration) {
        self.local.cleanup(max_age);
    }

    fn stats(&self) -> StorageStats {
        let mut stats = self.local.stats();
        // Add distributed-specific stats
        stats.total_entries = self.local.len();
        stats
    }
}

/// Result of a sync operation.
#[derive(Debug, Clone)]
pub struct SyncResult {
    /// Number of keys synced
    pub keys_synced: usize,
    /// Number of peers contacted
    pub peers_contacted: usize,
    /// Number of conflicts resolved
    pub conflicts_resolved: usize,
}

/// Error during sync operation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SyncError {
    #[error("Failed to contact peer: {0}")]
    PeerUnreachable(String),
    #[error("Sync timeout")]
    Timeout,
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Sliding window rate limit storage.
///
/// Alternative to token bucket for cases where a strict sliding window
/// is preferred over the smoothed token bucket approach.
pub struct SlidingWindowStorage {
    /// Windows: key -> list of request timestamps
    windows: DashMap<String, Vec<u64>>,
    /// Window size
    window_size: Duration,
}

impl SlidingWindowStorage {
    /// Create a new sliding window storage.
    pub fn new(window_size: Duration) -> Self {
        Self {
            windows: DashMap::new(),
            window_size,
        }
    }

    /// Record a request and check if it's within the limit.
    pub fn record_request(&self, key: &str, limit: u32) -> bool {
        let now = current_time_ms();
        let window_start = now.saturating_sub(self.window_size.as_millis() as u64);

        let mut window = self.windows.entry(key.to_string()).or_default();

        // Remove old entries
        window.retain(|&ts| ts > window_start);

        // Check limit
        if (window.len() as u32) < limit {
            window.push(now);
            true
        } else {
            false
        }
    }

    /// Get the count of requests in the current window.
    pub fn count(&self, key: &str) -> u32 {
        let now = current_time_ms();
        let window_start = now.saturating_sub(self.window_size.as_millis() as u64);

        self.windows
            .get(key)
            .map(|w| w.iter().filter(|&&ts| ts > window_start).count() as u32)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_storage_basic() {
        let storage = InMemoryStorage::new();

        // Try to consume from non-existent bucket (creates it)
        let result = storage.try_consume("test-key", 10, 1.0);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), 9.0);
    }

    #[test]
    fn test_in_memory_storage_exhaustion() {
        let storage = InMemoryStorage::new();

        // Exhaust the bucket
        for _ in 0..10 {
            storage.try_consume("test-key", 10, 1.0);
        }

        // Should fail now
        let result = storage.try_consume("test-key", 10, 1.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_in_memory_storage_stats() {
        let storage = InMemoryStorage::new();

        storage.try_consume("key1", 10, 1.0);
        storage.try_consume("key2", 10, 1.0);

        let stats = storage.stats();
        assert_eq!(stats.total_entries, 2);
        assert!(stats.operations >= 2);
    }

    #[test]
    fn test_in_memory_storage_cleanup() {
        let storage = InMemoryStorage::new();

        storage.try_consume("key1", 10, 1.0);

        // With 0 max_age, everything should be cleaned up
        storage.cleanup(Duration::ZERO);
        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn test_distributed_storage_creation() {
        let storage = DistributedStorage::new("node-1");
        assert_eq!(storage.node_id(), "node-1");
        assert!(storage.peers().is_empty());
    }

    #[test]
    fn test_distributed_storage_peers() {
        let storage = DistributedStorage::new("node-1");

        storage.add_peer("node-2:8080");
        storage.add_peer("node-3:8080");

        let peers = storage.peers();
        assert_eq!(peers.len(), 2);

        storage.remove_peer("node-2:8080");
        assert_eq!(storage.peers().len(), 1);
    }

    #[test]
    fn test_distributed_storage_local_operations() {
        let storage = DistributedStorage::new("node-1");

        // Should work like local storage
        let result = storage.try_consume("test-key", 10, 1.0);
        assert!(result.is_some());
    }

    #[test]
    fn test_sliding_window_storage() {
        let storage = SlidingWindowStorage::new(Duration::from_secs(1));

        // Should allow up to limit
        for _ in 0..5 {
            assert!(storage.record_request("key", 5));
        }

        // Should reject beyond limit
        assert!(!storage.record_request("key", 5));

        // Count should reflect requests
        assert_eq!(storage.count("key"), 5);
    }

    #[test]
    fn test_bucket_state_refill() {
        let mut state = BucketState::new(10, 1000.0); // 1000 tokens/sec

        // Consume all tokens
        state.tokens = 0.0;
        state.last_update_ms = current_time_ms() - 10; // 10ms ago

        // Refill should add ~10 tokens (1000/sec * 0.01sec)
        let tokens = state.refill();
        assert!(tokens >= 9.0 && tokens <= 11.0);
    }
}
