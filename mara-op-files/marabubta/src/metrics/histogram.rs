// Marabunta - Licensed under the MIT License.
//! Histogram metrics for latency distributions
//!
//! Provides labeled histogram metrics with configurable bucket boundaries
//! for measuring latency distributions across different operations.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Pre-defined bucket boundaries for different use cases
pub mod buckets {
    /// Buckets for API latency (milliseconds converted to seconds)
    /// 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s
    pub const API_LATENCY: &[f64] = &[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    /// Buckets for task execution duration (seconds)
    /// 1s, 5s, 10s, 30s, 60s, 300s (5m), 600s (10m), 1800s (30m), 3600s (1h)
    pub const TASK_DURATION: &[f64] = &[1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0];

    /// Buckets for scheduling latency (seconds)
    /// 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, 10s
    pub const SCHEDULING_LATENCY: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0];

    /// Buckets for network latency (seconds)
    /// 0.5ms, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
    pub const NETWORK_LATENCY: &[f64] = &[
        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
    ];

    /// Buckets for queue wait time (seconds)
    /// 100ms, 500ms, 1s, 5s, 10s, 30s, 60s, 300s, 600s, 1800s
    pub const QUEUE_WAIT: &[f64] = &[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0];

    /// Linear buckets generator
    pub fn linear(start: f64, width: f64, count: usize) -> Vec<f64> {
        (0..count).map(|i| start + width * i as f64).collect()
    }

    /// Exponential buckets generator
    pub fn exponential(start: f64, factor: f64, count: usize) -> Vec<f64> {
        (0..count).map(|i| start * factor.powi(i as i32)).collect()
    }
}

/// A single histogram bucket
#[derive(Debug)]
struct HistogramBucket {
    /// Upper bound of this bucket (exclusive)
    upper_bound: f64,
    /// Count of observations <= upper_bound
    count: AtomicU64,
}

impl HistogramBucket {
    fn new(upper_bound: f64) -> Self {
        Self {
            upper_bound,
            count: AtomicU64::new(0),
        }
    }
}

/// Core histogram data structure
#[derive(Debug)]
pub struct HistogramCore {
    /// Sorted bucket boundaries
    buckets: Vec<HistogramBucket>,
    /// Sum of all observed values
    sum: AtomicU64, // Stored as bits of f64
    /// Total count of observations
    count: AtomicU64,
}

impl HistogramCore {
    /// Create a new histogram with the given bucket boundaries
    pub fn new(bucket_bounds: &[f64]) -> Self {
        let mut bounds: Vec<f64> = bucket_bounds.to_vec();
        bounds.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Add +Inf bucket
        if bounds.last().map(|b| *b != f64::INFINITY).unwrap_or(true) {
            bounds.push(f64::INFINITY);
        }

        let buckets = bounds.iter().map(|&b| HistogramBucket::new(b)).collect();

        Self {
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Observe a value
    pub fn observe(&self, value: f64) {
        // Update sum using CAS loop for thread safety
        loop {
            let current_bits = self.sum.load(Ordering::Relaxed);
            let current = f64::from_bits(current_bits);
            let new_sum = current + value;
            let new_bits = new_sum.to_bits();

            if self
                .sum
                .compare_exchange_weak(current_bits, new_bits, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // Increment count
        self.count.fetch_add(1, Ordering::Release);

        // Increment appropriate bucket(s)
        for bucket in &self.buckets {
            if value <= bucket.upper_bound {
                bucket.count.fetch_add(1, Ordering::Release);
                break;
            }
        }
    }

    /// Get the sum of all observations
    pub fn sum(&self) -> f64 {
        f64::from_bits(self.sum.load(Ordering::Acquire))
    }

    /// Get the count of observations
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    /// Get bucket counts as (upper_bound, cumulative_count) pairs
    pub fn bucket_counts(&self) -> Vec<(f64, u64)> {
        let mut cumulative = 0u64;
        self.buckets
            .iter()
            .map(|b| {
                cumulative += b.count.load(Ordering::Acquire);
                (b.upper_bound, cumulative)
            })
            .collect()
    }

    /// Calculate approximate quantile (p50, p90, etc.)
    pub fn quantile(&self, q: f64) -> f64 {
        let total = self.count();
        if total == 0 {
            return 0.0;
        }

        let target = (total as f64 * q).ceil() as u64;
        let mut cumulative = 0u64;
        let mut prev_bound = 0.0;

        for bucket in &self.buckets {
            let bucket_count = bucket.count.load(Ordering::Acquire);
            cumulative += bucket_count;

            if cumulative >= target {
                // Linear interpolation within the bucket
                let bucket_fraction =
                    (target as f64 - (cumulative - bucket_count) as f64) / bucket_count as f64;
                return prev_bound + bucket_fraction * (bucket.upper_bound - prev_bound);
            }

            prev_bound = bucket.upper_bound;
        }

        self.buckets
            .last()
            .map(|b| b.upper_bound)
            .unwrap_or(f64::INFINITY)
    }

    /// Get mean of observations
    pub fn mean(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            return 0.0;
        }
        self.sum() / count as f64
    }

    /// Reset the histogram
    pub fn reset(&self) {
        self.sum.store(0, Ordering::Release);
        self.count.store(0, Ordering::Release);
        for bucket in &self.buckets {
            bucket.count.store(0, Ordering::Release);
        }
    }
}

/// Labels for a metric (sorted for consistent hashing)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Labels(Vec<(String, String)>);

impl Labels {
    /// Create new labels from key-value pairs
    pub fn new(pairs: &[(&str, &str)]) -> Self {
        let mut labels: Vec<_> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        Self(labels)
    }

    /// Create empty labels
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Get the labels as a slice
    pub fn as_slice(&self) -> &[(String, String)] {
        &self.0
    }

    /// Format labels for Prometheus output
    pub fn prometheus_format(&self) -> String {
        if self.0.is_empty() {
            return String::new();
        }

        let pairs: Vec<String> = self
            .0
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
            .collect();

        format!("{{{}}}", pairs.join(","))
    }
}

/// Escape special characters in label values
fn escape_label_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// A labeled histogram metric
#[derive(Debug)]
pub struct LabeledHistogram {
    /// Name of the metric
    name: String,
    /// Help text
    help: String,
    /// Bucket boundaries
    bucket_bounds: Vec<f64>,
    /// Per-label histograms
    histograms: RwLock<HashMap<Labels, Arc<HistogramCore>>>,
    /// Maximum cardinality (number of unique label combinations)
    max_cardinality: usize,
}

impl LabeledHistogram {
    /// Create a new labeled histogram
    pub fn new(name: &str, help: &str, bucket_bounds: &[f64]) -> Self {
        Self::with_cardinality_limit(name, help, bucket_bounds, 10000)
    }

    /// Create with a custom cardinality limit
    pub fn with_cardinality_limit(
        name: &str,
        help: &str,
        bucket_bounds: &[f64],
        max_cardinality: usize,
    ) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            bucket_bounds: bucket_bounds.to_vec(),
            histograms: RwLock::new(HashMap::new()),
            max_cardinality,
        }
    }

    /// Observe a value with labels
    pub fn observe(&self, labels: Labels, value: f64) -> Result<(), CardinalityError> {
        // Try to get existing histogram
        {
            let histograms = self.histograms.read();
            if let Some(histogram) = histograms.get(&labels) {
                histogram.observe(value);
                return Ok(());
            }
        }

        // Need to create new histogram
        let mut histograms = self.histograms.write();

        // Double-check after acquiring write lock
        if let Some(histogram) = histograms.get(&labels) {
            histogram.observe(value);
            return Ok(());
        }

        // Check cardinality limit
        if histograms.len() >= self.max_cardinality {
            return Err(CardinalityError {
                metric_name: self.name.clone(),
                current: histograms.len(),
                limit: self.max_cardinality,
            });
        }

        // Create and insert new histogram
        let histogram = Arc::new(HistogramCore::new(&self.bucket_bounds));
        histogram.observe(value);
        histograms.insert(labels, histogram);

        Ok(())
    }

    /// Get current cardinality
    pub fn cardinality(&self) -> usize {
        self.histograms.read().len()
    }

    /// Remove a label combination
    pub fn remove(&self, labels: &Labels) -> bool {
        self.histograms.write().remove(labels).is_some()
    }

    /// Reset all histograms
    pub fn reset(&self) {
        let histograms = self.histograms.read();
        for histogram in histograms.values() {
            histogram.reset();
        }
    }

    /// Clear all label combinations
    pub fn clear(&self) {
        self.histograms.write().clear();
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // HELP line
        output.push_str(&format!("# HELP {} {}\n", self.name, self.help));
        // TYPE line
        output.push_str(&format!("# TYPE {} histogram\n", self.name));

        let histograms = self.histograms.read();
        for (labels, histogram) in histograms.iter() {
            let label_str = labels.prometheus_format();

            // Output bucket counts
            for (bound, count) in histogram.bucket_counts() {
                let bucket_label = if label_str.is_empty() {
                    format!("{{le=\"{}\"}}", format_bound(bound))
                } else {
                    let inner = &label_str[1..label_str.len() - 1]; // Remove { and }
                    format!("{{le=\"{}\",{}}}", format_bound(bound), inner)
                };
                output.push_str(&format!("{}_bucket{} {}\n", self.name, bucket_label, count));
            }

            // Output sum
            output.push_str(&format!(
                "{}_sum{} {}\n",
                self.name,
                label_str,
                histogram.sum()
            ));

            // Output count
            output.push_str(&format!(
                "{}_count{} {}\n",
                self.name,
                label_str,
                histogram.count()
            ));
        }

        output
    }

    /// Get statistics for a specific label combination
    pub fn stats(&self, labels: &Labels) -> Option<HistogramStats> {
        let histograms = self.histograms.read();
        histograms.get(labels).map(|h| HistogramStats {
            count: h.count(),
            sum: h.sum(),
            mean: h.mean(),
            p50: h.quantile(0.5),
            p90: h.quantile(0.9),
            p99: h.quantile(0.99),
        })
    }
}

/// Format bucket boundary for Prometheus output
fn format_bound(bound: f64) -> String {
    if bound == f64::INFINITY {
        "+Inf".to_string()
    } else {
        format!("{}", bound)
    }
}

/// Statistics computed from a histogram
#[derive(Debug, Clone)]
pub struct HistogramStats {
    /// Total count of observations
    pub count: u64,
    /// Sum of all observations
    pub sum: f64,
    /// Mean of observations
    pub mean: f64,
    /// 50th percentile (median)
    pub p50: f64,
    /// 90th percentile
    pub p90: f64,
    /// 99th percentile
    pub p99: f64,
}

/// Error when cardinality limit is exceeded
#[derive(Debug, Clone)]
pub struct CardinalityError {
    /// Name of the metric that exceeded the limit
    pub metric_name: String,
    /// Current cardinality
    pub current: usize,
    /// Configured limit
    pub limit: usize,
}

impl std::fmt::Display for CardinalityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Metric '{}' exceeded cardinality limit: {} >= {}",
            self.metric_name, self.current, self.limit
        )
    }
}

impl std::error::Error for CardinalityError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_core_basic() {
        let hist = HistogramCore::new(&[1.0, 5.0, 10.0]);

        hist.observe(0.5);
        hist.observe(2.0);
        hist.observe(7.0);
        hist.observe(15.0);

        assert_eq!(hist.count(), 4);
        assert!((hist.sum() - 24.5).abs() < 0.001);
    }

    #[test]
    fn test_histogram_core_buckets() {
        let hist = HistogramCore::new(&[1.0, 5.0, 10.0]);

        hist.observe(0.5); // <= 1.0
        hist.observe(0.8); // <= 1.0
        hist.observe(2.0); // <= 5.0
        hist.observe(3.0); // <= 5.0
        hist.observe(4.0); // <= 5.0
        hist.observe(7.0); // <= 10.0
        hist.observe(15.0); // <= +Inf

        let buckets = hist.bucket_counts();
        assert_eq!(buckets.len(), 4); // 1.0, 5.0, 10.0, +Inf

        // Cumulative counts
        assert_eq!(buckets[0], (1.0, 2)); // 2 observations <= 1.0
        assert_eq!(buckets[1], (5.0, 5)); // 5 observations <= 5.0
        assert_eq!(buckets[2], (10.0, 6)); // 6 observations <= 10.0
        assert_eq!(buckets[3].1, 7); // 7 total observations
    }

    #[test]
    fn test_histogram_quantiles() {
        let hist = HistogramCore::new(&[1.0, 2.0, 3.0, 4.0, 5.0]);

        for _ in 0..100 {
            hist.observe(1.5);
        }
        for _ in 0..100 {
            hist.observe(2.5);
        }
        for _ in 0..100 {
            hist.observe(3.5);
        }

        // p50 should be around 2.5
        let p50 = hist.quantile(0.5);
        assert!(p50 >= 2.0 && p50 <= 3.0, "p50 = {}", p50);

        // p99 should be around 3.5
        let p99 = hist.quantile(0.99);
        assert!(p99 >= 3.0 && p99 <= 4.0, "p99 = {}", p99);
    }

    #[test]
    fn test_labeled_histogram() {
        let hist = LabeledHistogram::new(
            "http_request_duration_seconds",
            "Request duration in seconds",
            buckets::API_LATENCY,
        );

        let labels1 = Labels::new(&[("method", "GET"), ("path", "/api/v1/jobs")]);
        let labels2 = Labels::new(&[("method", "POST"), ("path", "/api/v1/jobs")]);

        hist.observe(labels1.clone(), 0.05).unwrap();
        hist.observe(labels1.clone(), 0.1).unwrap();
        hist.observe(labels2.clone(), 0.2).unwrap();

        assert_eq!(hist.cardinality(), 2);

        let stats1 = hist.stats(&labels1).unwrap();
        assert_eq!(stats1.count, 2);
        assert!((stats1.sum - 0.15).abs() < 0.001);
    }

    #[test]
    fn test_cardinality_limit() {
        let hist = LabeledHistogram::with_cardinality_limit(
            "test_metric",
            "Test metric",
            &[1.0, 5.0, 10.0],
            3,
        );

        // First 3 should succeed
        assert!(hist.observe(Labels::new(&[("id", "1")]), 1.0).is_ok());
        assert!(hist.observe(Labels::new(&[("id", "2")]), 2.0).is_ok());
        assert!(hist.observe(Labels::new(&[("id", "3")]), 3.0).is_ok());

        // 4th should fail
        let result = hist.observe(Labels::new(&[("id", "4")]), 4.0);
        assert!(result.is_err());

        // Existing labels should still work
        assert!(hist.observe(Labels::new(&[("id", "1")]), 5.0).is_ok());
    }

    #[test]
    fn test_prometheus_encoding() {
        let hist = LabeledHistogram::new("test_histogram", "A test histogram", &[1.0, 5.0, 10.0]);

        hist.observe(Labels::new(&[("method", "GET")]), 2.5)
            .unwrap();

        let output = hist.prometheus_encode();
        assert!(output.contains("# HELP test_histogram A test histogram"));
        assert!(output.contains("# TYPE test_histogram histogram"));
        assert!(output.contains("test_histogram_bucket"));
        assert!(output.contains("test_histogram_sum"));
        assert!(output.contains("test_histogram_count"));
        assert!(output.contains("method=\"GET\""));
    }

    #[test]
    fn test_bucket_generators() {
        let linear = buckets::linear(0.0, 0.5, 5);
        assert_eq!(linear, vec![0.0, 0.5, 1.0, 1.5, 2.0]);

        let exp = buckets::exponential(1.0, 2.0, 5);
        assert_eq!(exp, vec![1.0, 2.0, 4.0, 8.0, 16.0]);
    }

    #[test]
    fn test_labels() {
        let labels = Labels::new(&[("b", "2"), ("a", "1")]);
        // Should be sorted by key
        assert_eq!(labels.as_slice()[0], ("a".to_string(), "1".to_string()));
        assert_eq!(labels.as_slice()[1], ("b".to_string(), "2".to_string()));

        let prom = labels.prometheus_format();
        assert_eq!(prom, "{a=\"1\",b=\"2\"}");
    }
}
