// Marabunta - Licensed under the MIT License.
//! TTL (Time-To-Live) cache implementation with automatic expiration

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tokio::sync::Notify;
use std::sync::Arc;

use super::stats::CacheStats;
use super::traits::{Cache, CacheEntry};

/// Entry with expiration tracking
struct TtlEntry<V> {
    entry: CacheEntry<V>,
    expires_at: Instant,
}

impl<V: Clone> TtlEntry<V> {
    fn new(value: V, ttl: Duration) -> Self {
        let now = Instant::now();
        Self {
            entry: CacheEntry::with_ttl(value, ttl),
            expires_at: now + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    fn time_until_expiry(&self) -> Option<Duration> {
        let now = Instant::now();
        if now >= self.expires_at {
            None
        } else {
            Some(self.expires_at - now)
        }
    }
}

/// Internal state of the TTL cache
struct TtlState<K, V> {
    /// The storage map
    map: HashMap<K, TtlEntry<V>>,
    /// Default TTL for new entries
    default_ttl: Duration,
    /// Maximum number of entries (0 = unlimited)
    max_size: usize,
}

impl<K: Clone + Eq + Hash, V: Clone> TtlState<K, V> {
    fn new(default_ttl: Duration, max_size: usize) -> Self {
        Self {
            map: if max_size > 0 {
                HashMap::with_capacity(max_size)
            } else {
                HashMap::new()
            },
            default_ttl,
            max_size,
        }
    }

    fn evict_expired(&mut self) -> usize {
        let before = self.map.len();
        self.map.retain(|_, entry| !entry.is_expired());
        before - self.map.len()
    }

    fn evict_oldest(&mut self) -> Option<(K, CacheEntry<V>)> {
        // Find the entry with the earliest expiration
        let oldest_key = self
            .map
            .iter()
            .min_by_key(|(_, entry)| entry.expires_at)
            .map(|(k, _)| k.clone());

        if let Some(key) = oldest_key {
            self.map.remove(&key).map(|entry| (key, entry.entry))
        } else {
            None
        }
    }
}

/// TTL cache with automatic expiration
pub struct TtlCache<K, V> {
    state: RwLock<TtlState<K, V>>,
    stats: CacheStats,
    /// Notify when cleanup should run
    cleanup_notify: Arc<Notify>,
}

impl<K, V> TtlCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new TTL cache with the specified default TTL
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            state: RwLock::new(TtlState::new(default_ttl, 0)),
            stats: CacheStats::new(),
            cleanup_notify: Arc::new(Notify::new()),
        }
    }

    /// Create a new TTL cache with a maximum size
    pub fn with_max_size(default_ttl: Duration, max_size: usize) -> Self {
        Self {
            state: RwLock::new(TtlState::new(default_ttl, max_size)),
            stats: CacheStats::new(),
            cleanup_notify: Arc::new(Notify::new()),
        }
    }

    /// Get the default TTL
    pub fn default_ttl(&self) -> Duration {
        self.state.read().default_ttl
    }

    /// Set the default TTL for new entries
    pub fn set_default_ttl(&self, ttl: Duration) {
        self.state.write().default_ttl = ttl;
    }

    /// Get the current statistics
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get time until a specific entry expires
    pub fn time_until_expiry(&self, key: &K) -> Option<Duration> {
        self.state
            .read()
            .map
            .get(key)
            .and_then(|entry| entry.time_until_expiry())
    }

    /// Refresh the TTL of an entry
    pub fn refresh(&self, key: &K) -> bool {
        let mut state = self.state.write();
        let ttl = state.default_ttl;
        if let Some(entry) = state.map.get_mut(key) {
            entry.expires_at = Instant::now() + ttl;
            entry.entry.record_access();
            true
        } else {
            false
        }
    }

    /// Refresh the TTL of an entry with a specific duration
    pub fn refresh_with_ttl(&self, key: &K, ttl: Duration) -> bool {
        let mut state = self.state.write();
        if let Some(entry) = state.map.get_mut(key) {
            entry.expires_at = Instant::now() + ttl;
            entry.entry.ttl = Some(ttl);
            entry.entry.record_access();
            true
        } else {
            false
        }
    }

    /// Start a background cleanup task
    pub fn start_cleanup_task(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        let evicted = cache.evict_expired();
                        if evicted > 0 {
                            tracing::debug!("TTL cache cleanup: evicted {} expired entries", evicted);
                        }
                    }
                    _ = cache.cleanup_notify.notified() => {
                        // Manual trigger
                        let evicted = cache.evict_expired();
                        if evicted > 0 {
                            tracing::debug!("TTL cache manual cleanup: evicted {} expired entries", evicted);
                        }
                    }
                }
            }
        })
    }

    /// Trigger a manual cleanup
    pub fn trigger_cleanup(&self) {
        self.cleanup_notify.notify_one();
    }

    /// Get multiple values at once
    pub fn get_many(&self, keys: &[K]) -> Vec<Option<V>> {
        let start = Instant::now();
        let state = self.state.read();
        let results: Vec<Option<V>> = keys
            .iter()
            .map(|key| {
                if let Some(entry) = state.map.get(key) {
                    if entry.is_expired() {
                        self.stats.record_miss();
                        self.stats.record_expiration();
                        None
                    } else {
                        self.stats.record_hit();
                        Some(entry.entry.value.clone())
                    }
                } else {
                    self.stats.record_miss();
                    None
                }
            })
            .collect();

        self.stats.record_operation_time(start.elapsed());
        results
    }

    /// Set multiple values at once with the default TTL
    pub fn set_many(&self, entries: Vec<(K, V)>) {
        let start = Instant::now();
        let mut state = self.state.write();
        let ttl = state.default_ttl;

        for (key, value) in entries {
            // Evict if at capacity
            if state.max_size > 0 && state.map.len() >= state.max_size {
                // First try to evict expired
                let expired = state.evict_expired();
                for _ in 0..expired {
                    self.stats.record_expiration();
                }

                // If still at capacity, evict oldest
                if state.map.len() >= state.max_size
                    && state.evict_oldest().is_some() {
                        self.stats.record_eviction();
                    }
            }

            state.map.insert(key, TtlEntry::new(value, ttl));
            self.stats.record_insert();
        }

        self.stats.record_operation_time(start.elapsed());
    }

    /// Get all non-expired entries
    pub fn entries(&self) -> Vec<(K, V)> {
        self.state
            .read()
            .map
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .map(|(k, entry)| (k.clone(), entry.entry.value.clone()))
            .collect()
    }

    /// Get all entries with their remaining TTL
    pub fn entries_with_ttl(&self) -> Vec<(K, V, Option<Duration>)> {
        self.state
            .read()
            .map
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .map(|(k, entry)| {
                (
                    k.clone(),
                    entry.entry.value.clone(),
                    entry.time_until_expiry(),
                )
            })
            .collect()
    }
}

impl<K, V> Cache<K, V> for TtlCache<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    fn get(&self, key: &K) -> Option<V> {
        let start = Instant::now();
        let state = self.state.read();

        if let Some(entry) = state.map.get(key) {
            if entry.is_expired() {
                self.stats.record_miss();
                self.stats.record_expiration();
                self.stats.record_operation_time(start.elapsed());
                return None;
            }

            self.stats.record_hit();
            self.stats.record_operation_time(start.elapsed());
            return Some(entry.entry.value.clone());
        }

        self.stats.record_miss();
        self.stats.record_operation_time(start.elapsed());
        None
    }

    fn get_entry(&self, key: &K) -> Option<CacheEntry<V>> {
        let start = Instant::now();
        let state = self.state.read();

        if let Some(entry) = state.map.get(key) {
            if entry.is_expired() {
                self.stats.record_miss();
                self.stats.record_expiration();
                self.stats.record_operation_time(start.elapsed());
                return None;
            }

            self.stats.record_hit();
            self.stats.record_operation_time(start.elapsed());
            return Some(entry.entry.clone());
        }

        self.stats.record_miss();
        self.stats.record_operation_time(start.elapsed());
        None
    }

    fn set(&self, key: K, value: V) {
        let start = Instant::now();
        let mut state = self.state.write();
        let ttl = state.default_ttl;

        // Evict if at capacity
        if state.max_size > 0 && state.map.len() >= state.max_size && !state.map.contains_key(&key)
        {
            // First try to evict expired
            let expired = state.evict_expired();
            for _ in 0..expired {
                self.stats.record_expiration();
            }

            // If still at capacity, evict oldest
            if state.map.len() >= state.max_size
                && state.evict_oldest().is_some() {
                    self.stats.record_eviction();
                }
        }

        state.map.insert(key, TtlEntry::new(value, ttl));
        self.stats.record_insert();
        self.stats.record_operation_time(start.elapsed());
    }

    fn set_with_ttl(&self, key: K, value: V, ttl: Duration) {
        let start = Instant::now();
        let mut state = self.state.write();

        // Evict if at capacity
        if state.max_size > 0 && state.map.len() >= state.max_size && !state.map.contains_key(&key)
        {
            // First try to evict expired
            let expired = state.evict_expired();
            for _ in 0..expired {
                self.stats.record_expiration();
            }

            // If still at capacity, evict oldest
            if state.map.len() >= state.max_size
                && state.evict_oldest().is_some() {
                    self.stats.record_eviction();
                }
        }

        state.map.insert(key, TtlEntry::new(value, ttl));
        self.stats.record_insert();
        self.stats.record_operation_time(start.elapsed());
    }

    fn delete(&self, key: &K) -> Option<V> {
        let start = Instant::now();
        let mut state = self.state.write();

        let result = state.map.remove(key).map(|entry| {
            self.stats.record_delete();
            entry.entry.value
        });

        self.stats.record_operation_time(start.elapsed());
        result
    }

    fn clear(&self) {
        let start = Instant::now();
        self.state.write().map.clear();
        self.stats.record_operation_time(start.elapsed());
    }

    fn len(&self) -> usize {
        // Count only non-expired entries
        self.state
            .read()
            .map
            .values()
            .filter(|e| !e.is_expired())
            .count()
    }

    fn keys(&self) -> Vec<K> {
        self.state
            .read()
            .map
            .iter()
            .filter(|(_, e)| !e.is_expired())
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn evict_expired(&self) -> usize {
        let start = Instant::now();
        let mut state = self.state.write();
        let evicted = state.evict_expired();

        for _ in 0..evicted {
            self.stats.record_expiration();
        }

        self.stats.record_operation_time(start.elapsed());
        evicted
    }
}
