// Marabunta - Licensed under the MIT License.
//! Cache statistics tracking

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;

/// Cache statistics tracker
#[derive(Debug)]
pub struct CacheStats {
    /// Number of cache hits
    hits: AtomicU64,
    /// Number of cache misses
    misses: AtomicU64,
    /// Number of entries evicted
    evictions: AtomicU64,
    /// Number of entries expired
    expirations: AtomicU64,
    /// Number of entries inserted
    inserts: AtomicU64,
    /// Number of entries deleted
    deletes: AtomicU64,
    /// Total bytes stored (approximate)
    bytes_stored: AtomicU64,
    /// Total bytes evicted (approximate)
    bytes_evicted: AtomicU64,
    /// When statistics tracking started
    started_at: Instant,
    /// Time spent on cache operations (in nanoseconds)
    operation_time_ns: AtomicU64,
    /// Recent hit rates for sliding window calculation
    recent_hits: RwLock<SlidingWindow>,
    /// Recent miss rates for sliding window calculation
    recent_misses: RwLock<SlidingWindow>,
}

/// A sliding window for calculating recent rates
#[derive(Debug)]
struct SlidingWindow {
    /// Window size
    window_size: Duration,
    /// Buckets of counts (one per second)
    buckets: Vec<(Instant, u64)>,
    /// Maximum number of buckets to keep
    max_buckets: usize,
}

impl SlidingWindow {
    fn new(window_size: Duration) -> Self {
        let max_buckets = window_size.as_secs() as usize + 1;
        Self {
            window_size,
            buckets: Vec::with_capacity(max_buckets),
            max_buckets,
        }
    }

    fn record(&mut self) {
        let now = Instant::now();

        // Clean up old buckets
        self.buckets.retain(|(time, _)| now.duration_since(*time) < self.window_size);

        // Find or create bucket for current second
        if let Some((_, count)) = self.buckets.last_mut().filter(|(time, _)| {
            now.duration_since(*time) < Duration::from_secs(1)
        }) {
            *count += 1;
        } else {
            if self.buckets.len() >= self.max_buckets {
                self.buckets.remove(0);
            }
            self.buckets.push((now, 1));
        }
    }

    fn total(&self) -> u64 {
        let now = Instant::now();
        self.buckets
            .iter()
            .filter(|(time, _)| now.duration_since(*time) < self.window_size)
            .map(|(_, count)| count)
            .sum()
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheStats {
    /// Create a new statistics tracker
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            expirations: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            bytes_stored: AtomicU64::new(0),
            bytes_evicted: AtomicU64::new(0),
            started_at: Instant::now(),
            operation_time_ns: AtomicU64::new(0),
            recent_hits: RwLock::new(SlidingWindow::new(Duration::from_secs(60))),
            recent_misses: RwLock::new(SlidingWindow::new(Duration::from_secs(60))),
        }
    }

    /// Record a cache hit
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.recent_hits.write().record();
    }

    /// Record a cache miss
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.recent_misses.write().record();
    }

    /// Record an eviction
    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an eviction with byte count
    pub fn record_eviction_bytes(&self, bytes: u64) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
        self.bytes_evicted.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an expiration
    pub fn record_expiration(&self) {
        self.expirations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an insert
    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an insert with byte count
    pub fn record_insert_bytes(&self, bytes: u64) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
        self.bytes_stored.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a delete
    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delete with byte count
    pub fn record_delete_bytes(&self, bytes: u64) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        if bytes <= self.bytes_stored.load(Ordering::Relaxed) {
            self.bytes_stored.fetch_sub(bytes, Ordering::Relaxed);
        }
    }

    /// Record operation time
    pub fn record_operation_time(&self, duration: Duration) {
        self.operation_time_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Get the total number of hits
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get the total number of misses
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Get the total number of evictions
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Get the total number of expirations
    pub fn expirations(&self) -> u64 {
        self.expirations.load(Ordering::Relaxed)
    }

    /// Get the total number of inserts
    pub fn inserts(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
    }

    /// Get the total number of deletes
    pub fn deletes(&self) -> u64 {
        self.deletes.load(Ordering::Relaxed)
    }

    /// Get the total bytes stored
    pub fn bytes_stored(&self) -> u64 {
        self.bytes_stored.load(Ordering::Relaxed)
    }

    /// Get the total bytes evicted
    pub fn bytes_evicted(&self) -> u64 {
        self.bytes_evicted.load(Ordering::Relaxed)
    }

    /// Calculate the overall hit rate (0.0 - 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Calculate the recent hit rate (last 60 seconds)
    pub fn recent_hit_rate(&self) -> f64 {
        let hits = self.recent_hits.read().total();
        let misses = self.recent_misses.read().total();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get the uptime of the cache
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Get total operation time
    pub fn total_operation_time(&self) -> Duration {
        Duration::from_nanos(self.operation_time_ns.load(Ordering::Relaxed))
    }

    /// Get average operation time
    pub fn avg_operation_time(&self) -> Duration {
        let total_ops = self.hits.load(Ordering::Relaxed)
            + self.misses.load(Ordering::Relaxed)
            + self.inserts.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed);
        if total_ops == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(
                self.operation_time_ns.load(Ordering::Relaxed) / total_ops,
            )
        }
    }

    /// Calculate operations per second
    pub fn ops_per_second(&self) -> f64 {
        let total_ops = self.hits.load(Ordering::Relaxed)
            + self.misses.load(Ordering::Relaxed)
            + self.inserts.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed);
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs < 0.001 {
            0.0
        } else {
            total_ops as f64 / uptime_secs
        }
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.expirations.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.bytes_stored.store(0, Ordering::Relaxed);
        self.bytes_evicted.store(0, Ordering::Relaxed);
        self.operation_time_ns.store(0, Ordering::Relaxed);
        self.recent_hits.write().buckets.clear();
        self.recent_misses.write().buckets.clear();
    }

    /// Take a snapshot of current statistics
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            expirations: self.expirations.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            bytes_stored: self.bytes_stored.load(Ordering::Relaxed),
            bytes_evicted: self.bytes_evicted.load(Ordering::Relaxed),
            hit_rate: self.hit_rate(),
            recent_hit_rate: self.recent_hit_rate(),
            uptime: self.uptime(),
            ops_per_second: self.ops_per_second(),
            avg_operation_time: self.avg_operation_time(),
        }
    }
}

/// A point-in-time snapshot of cache statistics
#[derive(Debug, Clone)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub expirations: u64,
    pub inserts: u64,
    pub deletes: u64,
    pub bytes_stored: u64,
    pub bytes_evicted: u64,
    pub hit_rate: f64,
    pub recent_hit_rate: f64,
    pub uptime: Duration,
    pub ops_per_second: f64,
    pub avg_operation_time: Duration,
}

impl std::fmt::Display for CacheStatsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Cache Statistics:")?;
        writeln!(f, "  Hits:        {}", self.hits)?;
        writeln!(f, "  Misses:      {}", self.misses)?;
        writeln!(f, "  Hit Rate:    {:.2}%", self.hit_rate * 100.0)?;
        writeln!(f, "  Recent Rate: {:.2}%", self.recent_hit_rate * 100.0)?;
        writeln!(f, "  Evictions:   {}", self.evictions)?;
        writeln!(f, "  Expirations: {}", self.expirations)?;
        writeln!(f, "  Inserts:     {}", self.inserts)?;
        writeln!(f, "  Deletes:     {}", self.deletes)?;
        writeln!(f, "  Bytes:       {} stored, {} evicted", self.bytes_stored, self.bytes_evicted)?;
        writeln!(f, "  Uptime:      {:?}", self.uptime)?;
        writeln!(f, "  Ops/sec:     {:.2}", self.ops_per_second)?;
        writeln!(f, "  Avg Op Time: {:?}", self.avg_operation_time)?;
        Ok(())
    }
}
