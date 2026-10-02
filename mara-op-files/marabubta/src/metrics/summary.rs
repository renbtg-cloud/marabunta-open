// Marabunta - Licensed under the MIT License.
//! Summary metrics with quantile calculation
//!
//! Provides summary metrics that calculate quantiles (p50, p90, p99) using
//! a sliding time window approach with configurable max age and buffer size.

use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::histogram::{CardinalityError, Labels};

/// Configuration for summary quantile calculation
#[derive(Debug, Clone)]
pub struct SummaryConfig {
    /// Maximum age of observations to consider
    pub max_age: Duration,
    /// Maximum number of observations to keep in memory
    pub max_observations: usize,
    /// Quantiles to calculate (e.g., 0.5, 0.9, 0.99)
    pub quantiles: Vec<f64>,
    /// Error tolerance for quantile calculation (epsilon)
    pub epsilon: f64,
}

impl Default for SummaryConfig {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(600), // 10 minutes
            max_observations: 10000,
            quantiles: vec![0.5, 0.9, 0.95, 0.99],
            epsilon: 0.001,
        }
    }
}

impl SummaryConfig {
    /// Create a configuration with custom quantiles
    pub fn with_quantiles(quantiles: &[f64]) -> Self {
        Self {
            quantiles: quantiles.to_vec(),
            ..Default::default()
        }
    }

    /// Set max age for observations
    pub fn max_age(mut self, age: Duration) -> Self {
        self.max_age = age;
        self
    }

    /// Set max observations to keep
    pub fn max_observations(mut self, count: usize) -> Self {
        self.max_observations = count;
        self
    }
}

/// A timestamped observation
#[derive(Debug, Clone, Copy)]
struct Observation {
    value: f64,
    timestamp: Instant,
}

/// Core summary data structure with sliding window
#[derive(Debug)]
pub struct SummaryCore {
    /// Configuration
    config: SummaryConfig,
    /// Observations in order of insertion
    observations: RwLock<VecDeque<Observation>>,
    /// Total count of all observations (including expired)
    total_count: AtomicU64,
    /// Sum of all observations (including expired)
    total_sum: AtomicU64, // Stored as bits of f64
}

impl SummaryCore {
    /// Create a new summary with default configuration
    pub fn new() -> Self {
        Self::with_config(SummaryConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: SummaryConfig) -> Self {
        Self {
            config,
            observations: RwLock::new(VecDeque::new()),
            total_count: AtomicU64::new(0),
            total_sum: AtomicU64::new(0),
        }
    }

    /// Observe a value
    pub fn observe(&self, value: f64) {
        // Update total sum atomically
        loop {
            let current_bits = self.total_sum.load(Ordering::Relaxed);
            let current = f64::from_bits(current_bits);
            let new_sum = current + value;
            let new_bits = new_sum.to_bits();

            if self
                .total_sum
                .compare_exchange_weak(current_bits, new_bits, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // Increment total count
        self.total_count.fetch_add(1, Ordering::Release);

        // Add observation to sliding window
        let mut observations = self.observations.write();
        let now = Instant::now();

        observations.push_back(Observation {
            value,
            timestamp: now,
        });

        // Trim expired observations
        let cutoff = now - self.config.max_age;
        while observations
            .front()
            .map(|o| o.timestamp < cutoff)
            .unwrap_or(false)
        {
            observations.pop_front();
        }

        // Trim to max size
        while observations.len() > self.config.max_observations {
            observations.pop_front();
        }
    }

    /// Get total count of all observations
    pub fn count(&self) -> u64 {
        self.total_count.load(Ordering::Acquire)
    }

    /// Get total sum of all observations
    pub fn sum(&self) -> f64 {
        f64::from_bits(self.total_sum.load(Ordering::Acquire))
    }

    /// Get count of observations in current window
    pub fn window_count(&self) -> usize {
        self.observations.read().len()
    }

    /// Calculate a specific quantile from current window
    pub fn quantile(&self, q: f64) -> f64 {
        let observations = self.observations.read();
        if observations.is_empty() {
            return 0.0;
        }

        // Collect and sort values
        let mut values: Vec<f64> = observations.iter().map(|o| o.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let index = (values.len() as f64 * q).ceil() as usize;
        let index = index.saturating_sub(1).min(values.len() - 1);
        values[index]
    }

    /// Calculate all configured quantiles
    pub fn quantiles(&self) -> Vec<(f64, f64)> {
        let observations = self.observations.read();
        if observations.is_empty() {
            return self.config.quantiles.iter().map(|&q| (q, 0.0)).collect();
        }

        // Collect and sort values
        let mut values: Vec<f64> = observations.iter().map(|o| o.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());

        self.config
            .quantiles
            .iter()
            .map(|&q| {
                let index = (values.len() as f64 * q).ceil() as usize;
                let index = index.saturating_sub(1).min(values.len() - 1);
                (q, values[index])
            })
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> SummaryStats {
        let observations = self.observations.read();
        let quantiles = if observations.is_empty() {
            self.config.quantiles.iter().map(|&q| (q, 0.0)).collect()
        } else {
            let mut values: Vec<f64> = observations.iter().map(|o| o.value).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());

            self.config
                .quantiles
                .iter()
                .map(|&q| {
                    let index = (values.len() as f64 * q).ceil() as usize;
                    let index = index.saturating_sub(1).min(values.len() - 1);
                    (q, values[index])
                })
                .collect()
        };

        SummaryStats {
            total_count: self.total_count.load(Ordering::Acquire),
            total_sum: f64::from_bits(self.total_sum.load(Ordering::Acquire)),
            window_count: observations.len(),
            quantiles,
        }
    }

    /// Reset the summary
    pub fn reset(&self) {
        self.observations.write().clear();
        self.total_count.store(0, Ordering::Release);
        self.total_sum.store(0, Ordering::Release);
    }
}

impl Default for SummaryCore {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics from a summary
#[derive(Debug, Clone)]
pub struct SummaryStats {
    /// Total count of all observations
    pub total_count: u64,
    /// Total sum of all observations
    pub total_sum: f64,
    /// Count of observations in current window
    pub window_count: usize,
    /// Calculated quantiles as (quantile, value) pairs
    pub quantiles: Vec<(f64, f64)>,
}

impl SummaryStats {
    /// Get value for a specific quantile
    pub fn get_quantile(&self, q: f64) -> Option<f64> {
        self.quantiles
            .iter()
            .find(|(quantile, _)| (*quantile - q).abs() < 0.0001)
            .map(|(_, value)| *value)
    }

    /// Get p50 (median)
    pub fn p50(&self) -> Option<f64> {
        self.get_quantile(0.5)
    }

    /// Get p90
    pub fn p90(&self) -> Option<f64> {
        self.get_quantile(0.9)
    }

    /// Get p95
    pub fn p95(&self) -> Option<f64> {
        self.get_quantile(0.95)
    }

    /// Get p99
    pub fn p99(&self) -> Option<f64> {
        self.get_quantile(0.99)
    }
}

/// A labeled summary metric
#[derive(Debug)]
pub struct LabeledSummary {
    /// Name of the metric
    name: String,
    /// Help text
    help: String,
    /// Configuration
    config: SummaryConfig,
    /// Per-label summaries
    summaries: RwLock<HashMap<Labels, SummaryCore>>,
    /// Maximum cardinality
    max_cardinality: usize,
}

impl LabeledSummary {
    /// Create a new labeled summary
    pub fn new(name: &str, help: &str) -> Self {
        Self::with_config(name, help, SummaryConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(name: &str, help: &str, config: SummaryConfig) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            config,
            summaries: RwLock::new(HashMap::new()),
            max_cardinality: 10000,
        }
    }

    /// Create with a cardinality limit
    pub fn with_cardinality_limit(
        name: &str,
        help: &str,
        config: SummaryConfig,
        max_cardinality: usize,
    ) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            config,
            summaries: RwLock::new(HashMap::new()),
            max_cardinality,
        }
    }

    /// Observe a value with labels
    pub fn observe(&self, labels: Labels, value: f64) -> Result<(), CardinalityError> {
        // Try to get existing summary
        {
            let summaries = self.summaries.read();
            if let Some(summary) = summaries.get(&labels) {
                summary.observe(value);
                return Ok(());
            }
        }

        // Need to create new summary
        let mut summaries = self.summaries.write();

        // Double-check after acquiring write lock
        if let Some(summary) = summaries.get(&labels) {
            summary.observe(value);
            return Ok(());
        }

        // Check cardinality limit
        if summaries.len() >= self.max_cardinality {
            return Err(CardinalityError {
                metric_name: self.name.clone(),
                current: summaries.len(),
                limit: self.max_cardinality,
            });
        }

        // Create and insert new summary
        let summary = SummaryCore::with_config(self.config.clone());
        summary.observe(value);
        summaries.insert(labels, summary);

        Ok(())
    }

    /// Get current cardinality
    pub fn cardinality(&self) -> usize {
        self.summaries.read().len()
    }

    /// Remove a label combination
    pub fn remove(&self, labels: &Labels) -> bool {
        self.summaries.write().remove(labels).is_some()
    }

    /// Reset all summaries
    pub fn reset(&self) {
        let summaries = self.summaries.read();
        for summary in summaries.values() {
            summary.reset();
        }
    }

    /// Clear all label combinations
    pub fn clear(&self) {
        self.summaries.write().clear();
    }

    /// Get statistics for a specific label combination
    pub fn stats(&self, labels: &Labels) -> Option<SummaryStats> {
        let summaries = self.summaries.read();
        summaries.get(labels).map(|s| s.stats())
    }

    /// Encode in Prometheus format
    pub fn prometheus_encode(&self) -> String {
        let mut output = String::new();

        // HELP line
        output.push_str(&format!("# HELP {} {}\n", self.name, self.help));
        // TYPE line
        output.push_str(&format!("# TYPE {} summary\n", self.name));

        let summaries = self.summaries.read();
        for (labels, summary) in summaries.iter() {
            let label_str = labels.prometheus_format();
            let stats = summary.stats();

            // Output quantiles
            for (quantile, value) in &stats.quantiles {
                let quantile_label = if label_str.is_empty() {
                    format!("{{quantile=\"{}\"}}", quantile)
                } else {
                    let inner = &label_str[1..label_str.len() - 1]; // Remove { and }
                    format!("{{quantile=\"{}\",{}}}", quantile, inner)
                };
                output.push_str(&format!("{}{} {}\n", self.name, quantile_label, value));
            }

            // Output sum
            output.push_str(&format!(
                "{}_sum{} {}\n",
                self.name, label_str, stats.total_sum
            ));

            // Output count
            output.push_str(&format!(
                "{}_count{} {}\n",
                self.name, label_str, stats.total_count
            ));
        }

        output
    }
}

/// T-Digest for streaming quantile estimation with high accuracy
/// Provides better accuracy than simple sorted arrays for large streams
#[derive(Debug)]
pub struct TDigest {
    /// Centroids: (mean, weight) pairs
    centroids: Vec<(f64, f64)>,
    /// Compression factor (higher = more accuracy but more memory)
    compression: f64,
    /// Total weight
    total_weight: f64,
    /// Buffer for incoming values
    buffer: Vec<f64>,
    /// Max buffer size before merge
    max_buffer_size: usize,
}

impl TDigest {
    /// Create a new T-Digest with default compression
    pub fn new() -> Self {
        Self::with_compression(100.0)
    }

    /// Create with custom compression factor
    pub fn with_compression(compression: f64) -> Self {
        Self {
            centroids: Vec::new(),
            compression,
            total_weight: 0.0,
            buffer: Vec::new(),
            max_buffer_size: 1000,
        }
    }

    /// Add a value to the digest
    pub fn add(&mut self, value: f64) {
        self.buffer.push(value);

        if self.buffer.len() >= self.max_buffer_size {
            self.compress();
        }
    }

    /// Compress the buffer into centroids
    fn compress(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        // Sort buffer
        self.buffer.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Drain buffer values to avoid borrow conflict with add_to_centroids
        let buffered: Vec<f64> = self.buffer.drain(..).collect();

        // Add buffer values to centroids
        for value in buffered {
            self.add_to_centroids(value, 1.0);
        }

        // Merge centroids if too many
        self.merge_centroids();
    }

    fn add_to_centroids(&mut self, value: f64, weight: f64) {
        self.total_weight += weight;

        if self.centroids.is_empty() {
            self.centroids.push((value, weight));
            return;
        }

        // Find insertion point
        let pos = self
            .centroids
            .binary_search_by(|(m, _)| m.partial_cmp(&value).unwrap())
            .unwrap_or_else(|p| p);

        if pos < self.centroids.len() {
            // Merge with existing centroid
            let (mean, w) = self.centroids[pos];
            let new_weight = w + weight;
            let new_mean = (mean * w + value * weight) / new_weight;
            self.centroids[pos] = (new_mean, new_weight);
        } else {
            self.centroids.push((value, weight));
        }
    }

    fn merge_centroids(&mut self) {
        let max_centroids = (self.compression * 2.0) as usize;
        if self.centroids.len() <= max_centroids {
            return;
        }

        // Sort by mean
        self.centroids.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Merge adjacent centroids
        let mut merged = Vec::with_capacity(max_centroids);
        let mut current = self.centroids[0];

        for &(mean, weight) in &self.centroids[1..] {
            let new_weight = current.1 + weight;
            if new_weight <= self.total_weight / max_centroids as f64 * 2.0 {
                // Merge
                current.0 = (current.0 * current.1 + mean * weight) / new_weight;
                current.1 = new_weight;
            } else {
                merged.push(current);
                current = (mean, weight);
            }
        }
        merged.push(current);

        self.centroids = merged;
    }

    /// Calculate a quantile
    pub fn quantile(&mut self, q: f64) -> f64 {
        self.compress();

        if self.centroids.is_empty() {
            return 0.0;
        }

        let target_weight = q * self.total_weight;
        let mut cumulative_weight = 0.0;

        for i in 0..self.centroids.len() {
            let (mean, weight) = self.centroids[i];
            cumulative_weight += weight;

            if cumulative_weight >= target_weight {
                if i == 0 {
                    return mean;
                }

                // Interpolate
                let (prev_mean, _prev_weight) = self.centroids[i - 1];
                let prev_cumulative = cumulative_weight - weight;
                let fraction = (target_weight - prev_cumulative) / weight;

                return prev_mean + fraction * (mean - prev_mean);
            }
        }

        self.centroids.last().map(|(m, _)| *m).unwrap_or(0.0)
    }

    /// Get total count
    pub fn count(&self) -> f64 {
        self.total_weight + self.buffer.len() as f64
    }

    /// Reset the digest
    pub fn reset(&mut self) {
        self.centroids.clear();
        self.buffer.clear();
        self.total_weight = 0.0;
    }
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_summary_core_basic() {
        let summary = SummaryCore::new();

        for i in 1..=100 {
            summary.observe(i as f64);
        }

        assert_eq!(summary.count(), 100);
        assert!((summary.sum() - 5050.0).abs() < 0.001);
    }

    #[test]
    fn test_summary_quantiles() {
        let summary = SummaryCore::with_config(SummaryConfig::with_quantiles(&[0.5, 0.9, 0.99]));

        // Add values 1-100
        for i in 1..=100 {
            summary.observe(i as f64);
        }

        let stats = summary.stats();

        // p50 should be around 50
        let p50 = stats.p50().unwrap();
        assert!((p50 - 50.0).abs() < 2.0, "p50 = {}", p50);

        // p90 should be around 90
        let p90 = stats.p90().unwrap();
        assert!((p90 - 90.0).abs() < 2.0, "p90 = {}", p90);

        // p99 should be around 99
        let p99 = stats.p99().unwrap();
        assert!((p99 - 99.0).abs() < 2.0, "p99 = {}", p99);
    }

    #[test]
    fn test_summary_window_expiry() {
        let config = SummaryConfig::default()
            .max_age(Duration::from_millis(100))
            .max_observations(1000);

        let summary = SummaryCore::with_config(config);

        summary.observe(1.0);
        summary.observe(2.0);
        assert_eq!(summary.window_count(), 2);

        // Wait for observations to expire
        thread::sleep(Duration::from_millis(150));

        // Add new observation to trigger cleanup
        summary.observe(3.0);
        assert_eq!(summary.window_count(), 1);
    }

    #[test]
    fn test_labeled_summary() {
        let summary = LabeledSummary::new(
            "request_duration_seconds",
            "Request duration in seconds",
        );

        let labels1 = Labels::new(&[("method", "GET")]);
        let labels2 = Labels::new(&[("method", "POST")]);

        for i in 1..=10 {
            summary.observe(labels1.clone(), i as f64 * 0.1).unwrap();
        }
        summary.observe(labels2.clone(), 0.5).unwrap();

        assert_eq!(summary.cardinality(), 2);

        let stats1 = summary.stats(&labels1).unwrap();
        assert_eq!(stats1.total_count, 10);
    }

    #[test]
    fn test_prometheus_encoding() {
        let summary = LabeledSummary::new(
            "test_summary",
            "A test summary",
        );

        for i in 1..=100 {
            summary
                .observe(Labels::new(&[("handler", "main")]), i as f64)
                .unwrap();
        }

        let output = summary.prometheus_encode();
        assert!(output.contains("# HELP test_summary A test summary"));
        assert!(output.contains("# TYPE test_summary summary"));
        assert!(output.contains("quantile="));
        assert!(output.contains("test_summary_sum"));
        assert!(output.contains("test_summary_count"));
    }

    #[test]
    fn test_t_digest_basic() {
        let mut digest = TDigest::new();

        for i in 1..=1000 {
            digest.add(i as f64);
        }

        let p50 = digest.quantile(0.5);
        assert!((p50 - 500.0).abs() < 20.0, "p50 = {}", p50);

        let p90 = digest.quantile(0.9);
        assert!((p90 - 900.0).abs() < 20.0, "p90 = {}", p90);

        let p99 = digest.quantile(0.99);
        assert!((p99 - 990.0).abs() < 20.0, "p99 = {}", p99);
    }

    #[test]
    fn test_t_digest_uniform_distribution() {
        let mut digest = TDigest::with_compression(200.0);

        // Add uniformly distributed values
        for i in 0..10000 {
            digest.add(i as f64 / 10000.0);
        }

        // Quantiles should be approximately linear for uniform distribution
        for &q in &[0.25, 0.5, 0.75, 0.9, 0.95, 0.99] {
            let value = digest.quantile(q);
            assert!(
                (value - q).abs() < 0.05,
                "quantile {} = {}, expected ~{}",
                q,
                value,
                q
            );
        }
    }

    #[test]
    fn test_summary_stats_accessors() {
        let stats = SummaryStats {
            total_count: 100,
            total_sum: 5050.0,
            window_count: 100,
            quantiles: vec![(0.5, 50.0), (0.9, 90.0), (0.95, 95.0), (0.99, 99.0)],
        };

        assert_eq!(stats.p50(), Some(50.0));
        assert_eq!(stats.p90(), Some(90.0));
        assert_eq!(stats.p95(), Some(95.0));
        assert_eq!(stats.p99(), Some(99.0));
        assert_eq!(stats.get_quantile(0.75), None);
    }
}
