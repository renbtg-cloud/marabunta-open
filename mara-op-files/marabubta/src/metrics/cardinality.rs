// Marabunta - Licensed under the MIT License.
//! Metric cardinality limits and management
//!
//! Prevents metric cardinality explosion by enforcing limits on unique
//! label combinations and providing automatic eviction strategies.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::labels::MetricLabels;

/// Configuration for cardinality limits
#[derive(Debug, Clone)]
pub struct CardinalityConfig {
    /// Maximum number of unique label combinations per metric
    pub max_cardinality_per_metric: usize,
    /// Global maximum across all metrics
    pub max_global_cardinality: usize,
    /// Warning threshold (percentage of max)
    pub warning_threshold_percent: f64,
    /// Eviction strategy when limit is reached
    pub eviction_strategy: EvictionStrategy,
    /// TTL for unused label combinations
    pub unused_ttl: Duration,
    /// How often to run cleanup
    pub cleanup_interval: Duration,
}

impl Default for CardinalityConfig {
    fn default() -> Self {
        Self {
            max_cardinality_per_metric: 10_000,
            max_global_cardinality: 100_000,
            warning_threshold_percent: 80.0,
            eviction_strategy: EvictionStrategy::LeastRecentlyUsed,
            unused_ttl: Duration::from_secs(3600), // 1 hour
            cleanup_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}

impl CardinalityConfig {
    /// Create with custom per-metric limit
    pub fn with_limit(max_per_metric: usize) -> Self {
        Self {
            max_cardinality_per_metric: max_per_metric,
            ..Default::default()
        }
    }

    /// Set global limit
    pub fn global_limit(mut self, limit: usize) -> Self {
        self.max_global_cardinality = limit;
        self
    }

    /// Set eviction strategy
    pub fn eviction_strategy(mut self, strategy: EvictionStrategy) -> Self {
        self.eviction_strategy = strategy;
        self
    }

    /// Set unused TTL
    pub fn unused_ttl(mut self, ttl: Duration) -> Self {
        self.unused_ttl = ttl;
        self
    }
}

/// Strategy for evicting label combinations when limit is reached
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionStrategy {
    /// Evict least recently used label combinations
    LeastRecentlyUsed,
    /// Evict least frequently used label combinations
    LeastFrequentlyUsed,
    /// Evict oldest label combinations
    Oldest,
    /// Reject new label combinations (no eviction)
    RejectNew,
}

/// Tracked label combination with usage statistics
#[derive(Debug)]
struct TrackedLabels {
    /// The label combination
    labels: MetricLabels,
    /// Last access time
    last_access: Instant,
    /// Creation time
    created_at: Instant,
    /// Access count
    access_count: AtomicU64,
}

impl TrackedLabels {
    fn new(labels: MetricLabels) -> Self {
        let now = Instant::now();
        Self {
            labels,
            last_access: now,
            created_at: now,
            access_count: AtomicU64::new(1),
        }
    }

    fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    fn access_count(&self) -> u64 {
        self.access_count.load(Ordering::Relaxed)
    }
}

/// Per-metric cardinality tracker
#[derive(Debug)]
struct MetricTracker {
    /// Name of the metric
    name: String,
    /// Tracked label combinations
    labels: HashMap<u64, TrackedLabels>, // hash -> tracked
    /// Maximum cardinality for this metric
    max_cardinality: usize,
    /// Total observations count
    observations: AtomicU64,
    /// Rejected count due to cardinality
    rejected: AtomicU64,
    /// Evicted count
    evicted: AtomicU64,
}

impl MetricTracker {
    fn new(name: &str, max_cardinality: usize) -> Self {
        Self {
            name: name.to_string(),
            labels: HashMap::new(),
            max_cardinality,
            observations: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
        }
    }

    fn cardinality(&self) -> usize {
        self.labels.len()
    }

    fn is_at_limit(&self) -> bool {
        self.labels.len() >= self.max_cardinality
    }

    fn is_above_warning(&self, threshold_percent: f64) -> bool {
        let threshold = (self.max_cardinality as f64 * threshold_percent / 100.0) as usize;
        self.labels.len() >= threshold
    }
}

/// Error returned when cardinality limit is exceeded
#[derive(Debug, Clone)]
pub struct CardinalityLimitError {
    /// Name of the metric
    pub metric_name: String,
    /// Current cardinality
    pub current_cardinality: usize,
    /// Maximum allowed
    pub max_cardinality: usize,
    /// Whether this is a global or per-metric limit
    pub is_global: bool,
}

impl std::fmt::Display for CardinalityLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let limit_type = if self.is_global { "global" } else { "per-metric" };
        write!(
            f,
            "Cardinality limit exceeded for metric '{}': {} >= {} ({})",
            self.metric_name, self.current_cardinality, self.max_cardinality, limit_type
        )
    }
}

impl std::error::Error for CardinalityLimitError {}

/// Warning about approaching cardinality limits
#[derive(Debug, Clone)]
pub struct CardinalityWarning {
    /// Name of the metric
    pub metric_name: String,
    /// Current cardinality
    pub current_cardinality: usize,
    /// Maximum allowed
    pub max_cardinality: usize,
    /// Percentage of limit used
    pub usage_percent: f64,
}

impl std::fmt::Display for CardinalityWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cardinality warning for metric '{}': {}/{} ({:.1}%)",
            self.metric_name, self.current_cardinality, self.max_cardinality, self.usage_percent
        )
    }
}

/// Central cardinality limiter for all metrics
pub struct CardinalityLimiter {
    /// Configuration
    config: CardinalityConfig,
    /// Per-metric trackers
    metrics: RwLock<HashMap<String, MetricTracker>>,
    /// Global cardinality counter
    global_cardinality: AtomicUsize,
    /// Last cleanup time
    last_cleanup: RwLock<Instant>,
    /// Warning callback
    warning_callback: RwLock<Option<Box<dyn Fn(CardinalityWarning) + Send + Sync>>>,
}

impl std::fmt::Debug for CardinalityLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CardinalityLimiter")
            .field("config", &self.config)
            .field("metrics", &self.metrics)
            .field("global_cardinality", &self.global_cardinality)
            .field("last_cleanup", &self.last_cleanup)
            .field("warning_callback", &"<callback>")
            .finish()
    }
}

impl CardinalityLimiter {
    /// Create a new cardinality limiter with default configuration
    pub fn new() -> Self {
        Self::with_config(CardinalityConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: CardinalityConfig) -> Self {
        Self {
            config,
            metrics: RwLock::new(HashMap::new()),
            global_cardinality: AtomicUsize::new(0),
            last_cleanup: RwLock::new(Instant::now()),
            warning_callback: RwLock::new(None),
        }
    }

    /// Set a callback for cardinality warnings
    pub fn on_warning<F>(&self, callback: F)
    where
        F: Fn(CardinalityWarning) + Send + Sync + 'static,
    {
        *self.warning_callback.write() = Some(Box::new(callback));
    }

    /// Track a label combination for a metric
    ///
    /// Returns Ok(true) if this is a new label combination
    /// Returns Ok(false) if this label combination already exists
    /// Returns Err if cardinality limit would be exceeded
    pub fn track(
        &self,
        metric_name: &str,
        labels: &MetricLabels,
    ) -> Result<bool, CardinalityLimitError> {
        self.maybe_cleanup();

        let label_hash = Self::hash_labels(labels);
        let mut metrics = self.metrics.write();

        // Get or create tracker
        let tracker = metrics
            .entry(metric_name.to_string())
            .or_insert_with(|| MetricTracker::new(metric_name, self.config.max_cardinality_per_metric));

        // Check if labels already exist
        if let Some(tracked) = tracker.labels.get_mut(&label_hash) {
            tracked.touch();
            tracker.observations.fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }

        // Check per-metric limit
        if tracker.is_at_limit() {
            match self.config.eviction_strategy {
                EvictionStrategy::RejectNew => {
                    tracker.rejected.fetch_add(1, Ordering::Relaxed);
                    return Err(CardinalityLimitError {
                        metric_name: metric_name.to_string(),
                        current_cardinality: tracker.cardinality(),
                        max_cardinality: tracker.max_cardinality,
                        is_global: false,
                    });
                }
                _ => {
                    self.evict_one(tracker);
                }
            }
        }

        // Check global limit
        let global = self.global_cardinality.load(Ordering::Acquire);
        if global >= self.config.max_global_cardinality {
            match self.config.eviction_strategy {
                EvictionStrategy::RejectNew => {
                    tracker.rejected.fetch_add(1, Ordering::Relaxed);
                    return Err(CardinalityLimitError {
                        metric_name: metric_name.to_string(),
                        current_cardinality: global,
                        max_cardinality: self.config.max_global_cardinality,
                        is_global: true,
                    });
                }
                _ => {
                    // Global eviction would need different logic
                    // For now, just allow if per-metric is OK
                }
            }
        }

        // Add new labels
        tracker.labels.insert(label_hash, TrackedLabels::new(labels.clone()));
        tracker.observations.fetch_add(1, Ordering::Relaxed);
        self.global_cardinality.fetch_add(1, Ordering::Release);

        // Check warning threshold
        if tracker.is_above_warning(self.config.warning_threshold_percent) {
            self.emit_warning(CardinalityWarning {
                metric_name: metric_name.to_string(),
                current_cardinality: tracker.cardinality(),
                max_cardinality: tracker.max_cardinality,
                usage_percent: tracker.cardinality() as f64 / tracker.max_cardinality as f64 * 100.0,
            });
        }

        Ok(true)
    }

    /// Remove tracking for a label combination
    pub fn untrack(&self, metric_name: &str, labels: &MetricLabels) -> bool {
        let label_hash = Self::hash_labels(labels);
        let mut metrics = self.metrics.write();

        if let Some(tracker) = metrics.get_mut(metric_name) {
            if tracker.labels.remove(&label_hash).is_some() {
                self.global_cardinality.fetch_sub(1, Ordering::Release);
                return true;
            }
        }
        false
    }

    /// Get current cardinality for a metric
    pub fn get_cardinality(&self, metric_name: &str) -> usize {
        self.metrics
            .read()
            .get(metric_name)
            .map(|t| t.cardinality())
            .unwrap_or(0)
    }

    /// Get global cardinality
    pub fn global_cardinality(&self) -> usize {
        self.global_cardinality.load(Ordering::Acquire)
    }

    /// Get statistics for all metrics
    pub fn stats(&self) -> CardinalityStats {
        let metrics = self.metrics.read();
        let mut metric_stats = Vec::new();

        for (name, tracker) in metrics.iter() {
            metric_stats.push(MetricCardinalityStats {
                name: name.clone(),
                cardinality: tracker.cardinality(),
                max_cardinality: tracker.max_cardinality,
                observations: tracker.observations.load(Ordering::Relaxed),
                rejected: tracker.rejected.load(Ordering::Relaxed),
                evicted: tracker.evicted.load(Ordering::Relaxed),
            });
        }

        metric_stats.sort_by(|a, b| b.cardinality.cmp(&a.cardinality));

        CardinalityStats {
            global_cardinality: self.global_cardinality.load(Ordering::Acquire),
            max_global_cardinality: self.config.max_global_cardinality,
            metric_count: metrics.len(),
            metrics: metric_stats,
        }
    }

    /// Force cleanup of expired labels
    pub fn cleanup(&self) {
        let now = Instant::now();
        let cutoff = now - self.config.unused_ttl;
        let mut metrics = self.metrics.write();
        let mut removed = 0usize;

        for tracker in metrics.values_mut() {
            let to_remove: Vec<u64> = tracker
                .labels
                .iter()
                .filter(|(_, tracked)| tracked.last_access < cutoff)
                .map(|(hash, _)| *hash)
                .collect();

            for hash in to_remove {
                tracker.labels.remove(&hash);
                removed += 1;
            }
        }

        if removed > 0 {
            self.global_cardinality.fetch_sub(removed, Ordering::Release);
        }

        *self.last_cleanup.write() = now;
    }

    fn maybe_cleanup(&self) {
        let last = *self.last_cleanup.read();
        if last.elapsed() >= self.config.cleanup_interval {
            // Drop read lock and try cleanup
            // Note: This is racy but cleanup is idempotent
            self.cleanup();
        }
    }

    fn evict_one(&self, tracker: &mut MetricTracker) {
        let victim_hash = match self.config.eviction_strategy {
            EvictionStrategy::LeastRecentlyUsed => {
                tracker
                    .labels
                    .iter()
                    .min_by_key(|(_, t)| t.last_access)
                    .map(|(h, _)| *h)
            }
            EvictionStrategy::LeastFrequentlyUsed => {
                tracker
                    .labels
                    .iter()
                    .min_by_key(|(_, t)| t.access_count())
                    .map(|(h, _)| *h)
            }
            EvictionStrategy::Oldest => {
                tracker
                    .labels
                    .iter()
                    .min_by_key(|(_, t)| t.created_at)
                    .map(|(h, _)| *h)
            }
            EvictionStrategy::RejectNew => None,
        };

        if let Some(hash) = victim_hash {
            tracker.labels.remove(&hash);
            tracker.evicted.fetch_add(1, Ordering::Relaxed);
            self.global_cardinality.fetch_sub(1, Ordering::Release);
        }
    }

    fn emit_warning(&self, warning: CardinalityWarning) {
        if let Some(ref callback) = *self.warning_callback.read() {
            callback(warning);
        }
    }

    fn hash_labels(labels: &MetricLabels) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        labels.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for CardinalityLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about cardinality across all metrics
#[derive(Debug, Clone)]
pub struct CardinalityStats {
    /// Total global cardinality
    pub global_cardinality: usize,
    /// Maximum global cardinality
    pub max_global_cardinality: usize,
    /// Number of tracked metrics
    pub metric_count: usize,
    /// Per-metric statistics
    pub metrics: Vec<MetricCardinalityStats>,
}

impl CardinalityStats {
    /// Get top N metrics by cardinality
    pub fn top(&self, n: usize) -> Vec<&MetricCardinalityStats> {
        self.metrics.iter().take(n).collect()
    }

    /// Get global usage percentage
    pub fn global_usage_percent(&self) -> f64 {
        self.global_cardinality as f64 / self.max_global_cardinality as f64 * 100.0
    }
}

/// Statistics for a single metric
#[derive(Debug, Clone)]
pub struct MetricCardinalityStats {
    /// Metric name
    pub name: String,
    /// Current cardinality
    pub cardinality: usize,
    /// Maximum cardinality
    pub max_cardinality: usize,
    /// Total observations
    pub observations: u64,
    /// Rejected due to cardinality
    pub rejected: u64,
    /// Evicted label combinations
    pub evicted: u64,
}

impl MetricCardinalityStats {
    /// Get usage percentage
    pub fn usage_percent(&self) -> f64 {
        self.cardinality as f64 / self.max_cardinality as f64 * 100.0
    }

    /// Check if at warning level (>80%)
    pub fn is_warning(&self) -> bool {
        self.usage_percent() >= 80.0
    }

    /// Check if at critical level (>95%)
    pub fn is_critical(&self) -> bool {
        self.usage_percent() >= 95.0
    }
}

/// Guard that automatically untracks labels when dropped
#[derive(Debug)]
pub struct CardinalityGuard<'a> {
    limiter: &'a CardinalityLimiter,
    metric_name: String,
    labels: MetricLabels,
}

impl<'a> CardinalityGuard<'a> {
    /// Create a new guard
    pub fn new(
        limiter: &'a CardinalityLimiter,
        metric_name: &str,
        labels: MetricLabels,
    ) -> Result<Self, CardinalityLimitError> {
        limiter.track(metric_name, &labels)?;
        Ok(Self {
            limiter,
            metric_name: metric_name.to_string(),
            labels,
        })
    }
}

impl Drop for CardinalityGuard<'_> {
    fn drop(&mut self) {
        self.limiter.untrack(&self.metric_name, &self.labels);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tracking() {
        let limiter = CardinalityLimiter::new();

        let labels1 = MetricLabels::new(&[("a", "1")]);
        let labels2 = MetricLabels::new(&[("a", "2")]);

        assert!(limiter.track("test_metric", &labels1).unwrap());
        assert!(limiter.track("test_metric", &labels2).unwrap());

        // Same labels should return false (not new)
        assert!(!limiter.track("test_metric", &labels1).unwrap());

        assert_eq!(limiter.get_cardinality("test_metric"), 2);
        assert_eq!(limiter.global_cardinality(), 2);
    }

    #[test]
    fn test_per_metric_limit() {
        let config = CardinalityConfig::with_limit(3)
            .eviction_strategy(EvictionStrategy::RejectNew);
        let limiter = CardinalityLimiter::with_config(config);

        // Fill up to limit
        for i in 0..3 {
            let labels = MetricLabels::new(&[("id", &i.to_string())]);
            assert!(limiter.track("test_metric", &labels).is_ok());
        }

        // This should be rejected
        let labels = MetricLabels::new(&[("id", "3")]);
        let result = limiter.track("test_metric", &labels);
        assert!(result.is_err());
    }

    #[test]
    fn test_lru_eviction() {
        let config = CardinalityConfig::with_limit(3)
            .eviction_strategy(EvictionStrategy::LeastRecentlyUsed);
        let limiter = CardinalityLimiter::with_config(config);

        // Add 3 labels
        let labels0 = MetricLabels::new(&[("id", "0")]);
        let labels1 = MetricLabels::new(&[("id", "1")]);
        let labels2 = MetricLabels::new(&[("id", "2")]);

        limiter.track("test_metric", &labels0).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        limiter.track("test_metric", &labels1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        limiter.track("test_metric", &labels2).unwrap();

        // Touch labels0 to make it recently used
        limiter.track("test_metric", &labels0).unwrap();

        // Add new labels - should evict labels1 (least recently used)
        let labels3 = MetricLabels::new(&[("id", "3")]);
        limiter.track("test_metric", &labels3).unwrap();

        assert_eq!(limiter.get_cardinality("test_metric"), 3);

        let stats = limiter.stats();
        let metric_stats = &stats.metrics[0];
        assert_eq!(metric_stats.evicted, 1);
    }

    #[test]
    fn test_untrack() {
        let limiter = CardinalityLimiter::new();

        let labels = MetricLabels::new(&[("a", "1")]);
        limiter.track("test_metric", &labels).unwrap();
        assert_eq!(limiter.get_cardinality("test_metric"), 1);

        assert!(limiter.untrack("test_metric", &labels));
        assert_eq!(limiter.get_cardinality("test_metric"), 0);

        // Untracking non-existent should return false
        assert!(!limiter.untrack("test_metric", &labels));
    }

    #[test]
    fn test_cardinality_guard() {
        let limiter = CardinalityLimiter::new();

        {
            let labels = MetricLabels::new(&[("a", "1")]);
            let _guard = CardinalityGuard::new(&limiter, "test_metric", labels).unwrap();
            assert_eq!(limiter.get_cardinality("test_metric"), 1);
        }

        // Guard dropped, labels should be untracked
        assert_eq!(limiter.get_cardinality("test_metric"), 0);
    }

    #[test]
    fn test_warning_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let warning_fired = Arc::new(AtomicBool::new(false));
        let warning_fired_clone = warning_fired.clone();

        let config = CardinalityConfig::with_limit(10);
        let limiter = CardinalityLimiter::with_config(config);

        limiter.on_warning(move |_warning| {
            warning_fired_clone.store(true, Ordering::SeqCst);
        });

        // Add labels until warning threshold (80%)
        for i in 0..9 {
            let labels = MetricLabels::new(&[("id", &i.to_string())]);
            limiter.track("test_metric", &labels).unwrap();
        }

        assert!(warning_fired.load(Ordering::SeqCst));
    }

    #[test]
    fn test_stats() {
        let limiter = CardinalityLimiter::new();

        for i in 0..5 {
            let labels = MetricLabels::new(&[("id", &i.to_string())]);
            limiter.track("metric_a", &labels).unwrap();
        }

        for i in 0..3 {
            let labels = MetricLabels::new(&[("id", &i.to_string())]);
            limiter.track("metric_b", &labels).unwrap();
        }

        let stats = limiter.stats();
        assert_eq!(stats.global_cardinality, 8);
        assert_eq!(stats.metric_count, 2);

        // Should be sorted by cardinality (descending)
        assert_eq!(stats.metrics[0].name, "metric_a");
        assert_eq!(stats.metrics[0].cardinality, 5);
        assert_eq!(stats.metrics[1].name, "metric_b");
        assert_eq!(stats.metrics[1].cardinality, 3);
    }

    #[test]
    fn test_cleanup() {
        let config = CardinalityConfig::default()
            .unused_ttl(Duration::from_millis(50));
        let limiter = CardinalityLimiter::with_config(config);

        let labels = MetricLabels::new(&[("a", "1")]);
        limiter.track("test_metric", &labels).unwrap();
        assert_eq!(limiter.get_cardinality("test_metric"), 1);

        // Wait for TTL
        std::thread::sleep(Duration::from_millis(100));

        limiter.cleanup();
        assert_eq!(limiter.get_cardinality("test_metric"), 0);
    }

    #[test]
    fn test_metric_stats_helpers() {
        let stats = MetricCardinalityStats {
            name: "test".to_string(),
            cardinality: 850,
            max_cardinality: 1000,
            observations: 10000,
            rejected: 5,
            evicted: 100,
        };

        assert!((stats.usage_percent() - 85.0).abs() < 0.1);
        assert!(stats.is_warning());
        assert!(!stats.is_critical());

        let critical_stats = MetricCardinalityStats {
            cardinality: 960,
            ..stats
        };
        assert!(critical_stats.is_critical());
    }
}
