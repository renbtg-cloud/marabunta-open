// Marabunta - Licensed under the MIT License.
//! Comprehensive Metrics Collection
//!
//! This module provides real-time metrics collection, aggregation over time windows,
//! and anomaly detection for infrastructure nodes.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Metrics for a single GPU.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GpuMetrics {
    /// GPU index (0-based).
    pub index: u32,
    /// GPU utilization percentage (0.0 - 100.0).
    pub usage_percent: f32,
    /// GPU memory used in bytes.
    pub memory_used_bytes: u64,
    /// GPU temperature in Celsius.
    pub temperature_celsius: f32,
    /// GPU power consumption in watts.
    pub power_watts: f32,
}

impl GpuMetrics {
    /// Creates new GPU metrics.
    pub fn new(index: u32) -> Self {
        Self {
            index,
            usage_percent: 0.0,
            memory_used_bytes: 0,
            temperature_celsius: 0.0,
            power_watts: 0.0,
        }
    }

    /// Validates that metrics are within reasonable bounds.
    pub fn is_valid(&self) -> bool {
        (0.0..=100.0).contains(&self.usage_percent)
            && (0.0..=150.0).contains(&self.temperature_celsius)
            && self.power_watts >= 0.0
    }
}

/// Comprehensive metrics for an infrastructure node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// CPU utilization percentage (0.0 - 100.0).
    pub cpu_usage_percent: f32,
    /// Memory currently in use in bytes.
    pub memory_used_bytes: u64,
    /// Memory available (not just free, includes cache/buffer) in bytes.
    pub memory_available_bytes: u64,
    /// Per-GPU metrics.
    pub gpu_usage: Vec<GpuMetrics>,
    /// Network bytes received since boot.
    pub network_rx_bytes: u64,
    /// Network bytes transmitted since boot.
    pub network_tx_bytes: u64,
    /// Disk bytes read since boot.
    pub disk_read_bytes: u64,
    /// Disk bytes written since boot.
    pub disk_write_bytes: u64,
    /// Number of tasks completed.
    pub tasks_completed: u64,
    /// Number of tasks failed.
    pub tasks_failed: u64,
    /// Average task duration in milliseconds.
    pub avg_task_duration_ms: u64,
    /// System uptime in seconds.
    pub uptime_seconds: u64,
    /// CPU temperature in Celsius (optional, not all systems report this).
    pub temperature_celsius: Option<f32>,
    /// System power consumption in watts (optional).
    pub power_watts: Option<f32>,
    /// When these metrics were collected.
    pub collected_at: DateTime<Utc>,
}

impl NodeMetrics {
    /// Creates new metrics with the current timestamp.
    pub fn new() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_used_bytes: 0,
            memory_available_bytes: 0,
            gpu_usage: Vec::new(),
            network_rx_bytes: 0,
            network_tx_bytes: 0,
            disk_read_bytes: 0,
            disk_write_bytes: 0,
            tasks_completed: 0,
            tasks_failed: 0,
            avg_task_duration_ms: 0,
            uptime_seconds: 0,
            temperature_celsius: None,
            power_watts: None,
            collected_at: Utc::now(),
        }
    }

    /// Returns total memory in bytes.
    pub fn total_memory_bytes(&self) -> u64 {
        self.memory_used_bytes + self.memory_available_bytes
    }

    /// Returns memory usage percentage.
    pub fn memory_usage_percent(&self) -> f32 {
        let total = self.total_memory_bytes();
        if total == 0 {
            0.0
        } else {
            (self.memory_used_bytes as f64 / total as f64 * 100.0) as f32
        }
    }

    /// Returns task failure rate.
    pub fn task_failure_rate(&self) -> f32 {
        let total = self.tasks_completed + self.tasks_failed;
        if total == 0 {
            0.0
        } else {
            (self.tasks_failed as f64 / total as f64) as f32
        }
    }

    /// Returns average GPU usage across all GPUs.
    pub fn avg_gpu_usage_percent(&self) -> f32 {
        if self.gpu_usage.is_empty() {
            0.0
        } else {
            let sum: f32 = self.gpu_usage.iter().map(|g| g.usage_percent).sum();
            sum / self.gpu_usage.len() as f32
        }
    }

    /// Returns total GPU power consumption.
    pub fn total_gpu_power_watts(&self) -> f32 {
        self.gpu_usage.iter().map(|g| g.power_watts).sum()
    }

    /// Validates that all metrics are within reasonable bounds.
    pub fn is_valid(&self) -> bool {
        (0.0..=100.0).contains(&self.cpu_usage_percent)
            && self.gpu_usage.iter().all(|g| g.is_valid())
            && self
                .temperature_celsius
                .map_or(true, |t| (0.0..=150.0).contains(&t))
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Time windows for metrics aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeWindow {
    /// 1 minute window.
    OneMinute,
    /// 5 minute window.
    FiveMinutes,
    /// 1 hour window.
    OneHour,
    /// 24 hour window.
    TwentyFourHours,
}

impl TimeWindow {
    /// Returns the duration for this window.
    pub fn duration(&self) -> Duration {
        match self {
            TimeWindow::OneMinute => Duration::minutes(1),
            TimeWindow::FiveMinutes => Duration::minutes(5),
            TimeWindow::OneHour => Duration::hours(1),
            TimeWindow::TwentyFourHours => Duration::hours(24),
        }
    }

    /// Returns all time windows.
    pub fn all() -> &'static [TimeWindow] {
        &[
            TimeWindow::OneMinute,
            TimeWindow::FiveMinutes,
            TimeWindow::OneHour,
            TimeWindow::TwentyFourHours,
        ]
    }
}

/// Aggregated metrics over a time window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregatedMetrics {
    /// Time window for aggregation.
    pub window: TimeWindow,
    /// Number of samples in this aggregation.
    pub sample_count: usize,
    /// Start of the aggregation period.
    pub start_time: DateTime<Utc>,
    /// End of the aggregation period.
    pub end_time: DateTime<Utc>,
    /// CPU usage statistics.
    pub cpu_usage: MetricStats,
    /// Memory usage statistics (percentage).
    pub memory_usage: MetricStats,
    /// GPU usage statistics (average across all GPUs).
    pub gpu_usage: MetricStats,
    /// Network receive rate in bytes/second.
    pub network_rx_rate: MetricStats,
    /// Network transmit rate in bytes/second.
    pub network_tx_rate: MetricStats,
    /// Disk read rate in bytes/second.
    pub disk_read_rate: MetricStats,
    /// Disk write rate in bytes/second.
    pub disk_write_rate: MetricStats,
    /// Task completion rate per second.
    pub task_completion_rate: MetricStats,
    /// Task failure rate.
    pub task_failure_rate: MetricStats,
    /// Temperature statistics (if available).
    pub temperature: Option<MetricStats>,
}

/// Statistical summary of a metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricStats {
    /// Minimum value.
    pub min: f64,
    /// Maximum value.
    pub max: f64,
    /// Mean value.
    pub mean: f64,
    /// Standard deviation.
    pub std_dev: f64,
    /// Latest value.
    pub latest: f64,
}

impl MetricStats {
    /// Creates stats from a collection of values.
    pub fn from_values(values: &[f64]) -> Option<Self> {
        if values.is_empty() {
            return None;
        }

        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = values.iter().sum();
        let mean = sum / values.len() as f64;

        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();

        let latest = *values.last().unwrap();

        Some(Self {
            min,
            max,
            mean,
            std_dev,
            latest,
        })
    }

    /// Returns true if the latest value is unusually high.
    pub fn is_high_outlier(&self, threshold_std_devs: f64) -> bool {
        if self.std_dev < 0.001 {
            return false; // Not enough variance to detect outliers
        }
        (self.latest - self.mean) / self.std_dev > threshold_std_devs
    }

    /// Returns true if the latest value is unusually low.
    pub fn is_low_outlier(&self, threshold_std_devs: f64) -> bool {
        if self.std_dev < 0.001 {
            return false;
        }
        (self.mean - self.latest) / self.std_dev > threshold_std_devs
    }
}

impl Default for MetricStats {
    fn default() -> Self {
        Self {
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            std_dev: 0.0,
            latest: 0.0,
        }
    }
}

/// Timestamped metrics sample.
#[derive(Debug, Clone)]
struct MetricsSample {
    metrics: NodeMetrics,
    timestamp: DateTime<Utc>,
}

/// Metrics aggregator that maintains rolling windows.
///
/// Stores raw metrics and computes aggregations on demand.
#[derive(Debug)]
pub struct MetricsAggregator {
    /// Raw metrics samples.
    samples: VecDeque<MetricsSample>,
    /// Maximum samples to keep (covers 24h window at 1 sample/10s).
    max_samples: usize,
    /// Previous network/disk counters for rate calculation.
    prev_network_rx: Option<u64>,
    prev_network_tx: Option<u64>,
    prev_disk_read: Option<u64>,
    prev_disk_write: Option<u64>,
    prev_tasks_completed: Option<u64>,
    prev_sample_time: Option<DateTime<Utc>>,
}

impl MetricsAggregator {
    /// Creates a new aggregator.
    pub fn new() -> Self {
        // 24h at 10s intervals = 8640 samples
        Self::with_capacity(8640)
    }

    /// Creates an aggregator with specific capacity.
    pub fn with_capacity(max_samples: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(max_samples),
            max_samples,
            prev_network_rx: None,
            prev_network_tx: None,
            prev_disk_read: None,
            prev_disk_write: None,
            prev_tasks_completed: None,
            prev_sample_time: None,
        }
    }

    /// Adds a new metrics sample.
    pub fn add_sample(&mut self, metrics: NodeMetrics) {
        let timestamp = metrics.collected_at;

        // Store counters for rate calculation
        self.prev_network_rx = Some(metrics.network_rx_bytes);
        self.prev_network_tx = Some(metrics.network_tx_bytes);
        self.prev_disk_read = Some(metrics.disk_read_bytes);
        self.prev_disk_write = Some(metrics.disk_write_bytes);
        self.prev_tasks_completed = Some(metrics.tasks_completed);
        self.prev_sample_time = Some(timestamp);

        self.samples.push_back(MetricsSample { metrics, timestamp });

        // Trim old samples
        while self.samples.len() > self.max_samples {
            self.samples.pop_front();
        }
    }

    /// Returns the number of samples stored.
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Returns the latest metrics.
    pub fn latest(&self) -> Option<&NodeMetrics> {
        self.samples.back().map(|s| &s.metrics)
    }

    /// Computes aggregated metrics for a time window.
    pub fn aggregate(&self, window: TimeWindow) -> Option<AggregatedMetrics> {
        let cutoff = Utc::now() - window.duration();
        let samples: Vec<_> = self
            .samples
            .iter()
            .filter(|s| s.timestamp >= cutoff)
            .collect();

        if samples.is_empty() {
            return None;
        }

        let start_time = samples.first().unwrap().timestamp;
        let end_time = samples.last().unwrap().timestamp;

        // Collect values for each metric
        let cpu_values: Vec<f64> = samples
            .iter()
            .map(|s| s.metrics.cpu_usage_percent as f64)
            .collect();
        let memory_values: Vec<f64> = samples
            .iter()
            .map(|s| s.metrics.memory_usage_percent() as f64)
            .collect();
        let gpu_values: Vec<f64> = samples
            .iter()
            .map(|s| s.metrics.avg_gpu_usage_percent() as f64)
            .collect();
        let task_failure_values: Vec<f64> = samples
            .iter()
            .map(|s| s.metrics.task_failure_rate() as f64)
            .collect();
        let temp_values: Vec<f64> = samples
            .iter()
            .filter_map(|s| s.metrics.temperature_celsius)
            .map(|t| t as f64)
            .collect();

        // Calculate rates between samples
        let mut network_rx_rates = Vec::new();
        let mut network_tx_rates = Vec::new();
        let mut disk_read_rates = Vec::new();
        let mut disk_write_rates = Vec::new();
        let mut task_completion_rates = Vec::new();

        for pair in samples.windows(2) {
            let prev = &pair[0];
            let curr = &pair[1];
            let dt = (curr.timestamp - prev.timestamp).num_seconds() as f64;
            if dt > 0.0 {
                network_rx_rates.push(
                    (curr
                        .metrics
                        .network_rx_bytes
                        .saturating_sub(prev.metrics.network_rx_bytes)) as f64
                        / dt,
                );
                network_tx_rates.push(
                    (curr
                        .metrics
                        .network_tx_bytes
                        .saturating_sub(prev.metrics.network_tx_bytes)) as f64
                        / dt,
                );
                disk_read_rates.push(
                    (curr
                        .metrics
                        .disk_read_bytes
                        .saturating_sub(prev.metrics.disk_read_bytes)) as f64
                        / dt,
                );
                disk_write_rates.push(
                    (curr
                        .metrics
                        .disk_write_bytes
                        .saturating_sub(prev.metrics.disk_write_bytes)) as f64
                        / dt,
                );
                task_completion_rates.push(
                    (curr
                        .metrics
                        .tasks_completed
                        .saturating_sub(prev.metrics.tasks_completed)) as f64
                        / dt,
                );
            }
        }

        Some(AggregatedMetrics {
            window,
            sample_count: samples.len(),
            start_time,
            end_time,
            cpu_usage: MetricStats::from_values(&cpu_values).unwrap_or_default(),
            memory_usage: MetricStats::from_values(&memory_values).unwrap_or_default(),
            gpu_usage: MetricStats::from_values(&gpu_values).unwrap_or_default(),
            network_rx_rate: MetricStats::from_values(&network_rx_rates).unwrap_or_default(),
            network_tx_rate: MetricStats::from_values(&network_tx_rates).unwrap_or_default(),
            disk_read_rate: MetricStats::from_values(&disk_read_rates).unwrap_or_default(),
            disk_write_rate: MetricStats::from_values(&disk_write_rates).unwrap_or_default(),
            task_completion_rate: MetricStats::from_values(&task_completion_rates)
                .unwrap_or_default(),
            task_failure_rate: MetricStats::from_values(&task_failure_values).unwrap_or_default(),
            temperature: if temp_values.is_empty() {
                None
            } else {
                MetricStats::from_values(&temp_values)
            },
        })
    }

    /// Detects anomalies in the latest metrics.
    pub fn detect_anomalies(&self, window: TimeWindow) -> Vec<MetricsAnomaly> {
        let mut anomalies = Vec::new();

        let Some(aggregated) = self.aggregate(window) else {
            return anomalies;
        };

        // CPU spike detection
        if aggregated.cpu_usage.is_high_outlier(3.0) && aggregated.cpu_usage.latest > 90.0 {
            anomalies.push(MetricsAnomaly::CpuSpike {
                current: aggregated.cpu_usage.latest as f32,
                baseline: aggregated.cpu_usage.mean as f32,
            });
        }

        // Memory spike detection
        if aggregated.memory_usage.is_high_outlier(3.0) && aggregated.memory_usage.latest > 90.0 {
            anomalies.push(MetricsAnomaly::MemorySpike {
                current: aggregated.memory_usage.latest as f32,
                baseline: aggregated.memory_usage.mean as f32,
            });
        }

        // GPU spike detection
        if aggregated.gpu_usage.is_high_outlier(3.0) && aggregated.gpu_usage.latest > 90.0 {
            anomalies.push(MetricsAnomaly::GpuSpike {
                current: aggregated.gpu_usage.latest as f32,
                baseline: aggregated.gpu_usage.mean as f32,
            });
        }

        // Sudden drop in task completion rate
        if aggregated.task_completion_rate.is_low_outlier(2.0)
            && aggregated.task_completion_rate.mean > 0.1
        {
            anomalies.push(MetricsAnomaly::TaskCompletionDrop {
                current: aggregated.task_completion_rate.latest as f32,
                baseline: aggregated.task_completion_rate.mean as f32,
            });
        }

        // High task failure rate
        if aggregated.task_failure_rate.latest > 0.1 {
            anomalies.push(MetricsAnomaly::HighTaskFailureRate {
                rate: aggregated.task_failure_rate.latest as f32,
            });
        }

        // Temperature anomaly
        if let Some(ref temp) = aggregated.temperature {
            if temp.latest > 85.0 {
                anomalies.push(MetricsAnomaly::HighTemperature {
                    current: temp.latest as f32,
                    threshold: 85.0,
                });
            }
        }

        // Network saturation
        if aggregated.network_rx_rate.is_high_outlier(3.0)
            || aggregated.network_tx_rate.is_high_outlier(3.0)
        {
            anomalies.push(MetricsAnomaly::NetworkSaturation {
                rx_rate: aggregated.network_rx_rate.latest,
                tx_rate: aggregated.network_tx_rate.latest,
            });
        }

        anomalies
    }
}

impl Default for MetricsAggregator {
    fn default() -> Self {
        Self::new()
    }
}

/// Types of metrics anomalies that can be detected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricsAnomaly {
    /// CPU usage spike above baseline.
    CpuSpike { current: f32, baseline: f32 },
    /// Memory usage spike above baseline.
    MemorySpike { current: f32, baseline: f32 },
    /// GPU usage spike above baseline.
    GpuSpike { current: f32, baseline: f32 },
    /// Sudden drop in task completion rate.
    TaskCompletionDrop { current: f32, baseline: f32 },
    /// High task failure rate.
    HighTaskFailureRate { rate: f32 },
    /// Temperature above safe threshold.
    HighTemperature { current: f32, threshold: f32 },
    /// Network bandwidth saturation.
    NetworkSaturation { rx_rate: f64, tx_rate: f64 },
    /// Disk I/O saturation.
    DiskSaturation { read_rate: f64, write_rate: f64 },
}

impl std::fmt::Display for MetricsAnomaly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricsAnomaly::CpuSpike { current, baseline } => {
                write!(f, "CPU spike: {:.1}% (baseline: {:.1}%)", current, baseline)
            }
            MetricsAnomaly::MemorySpike { current, baseline } => {
                write!(
                    f,
                    "Memory spike: {:.1}% (baseline: {:.1}%)",
                    current, baseline
                )
            }
            MetricsAnomaly::GpuSpike { current, baseline } => {
                write!(f, "GPU spike: {:.1}% (baseline: {:.1}%)", current, baseline)
            }
            MetricsAnomaly::TaskCompletionDrop { current, baseline } => {
                write!(
                    f,
                    "Task completion drop: {:.2}/s (baseline: {:.2}/s)",
                    current, baseline
                )
            }
            MetricsAnomaly::HighTaskFailureRate { rate } => {
                write!(f, "High task failure rate: {:.1}%", rate * 100.0)
            }
            MetricsAnomaly::HighTemperature { current, threshold } => {
                write!(
                    f,
                    "High temperature: {:.1}C (threshold: {:.1}C)",
                    current, threshold
                )
            }
            MetricsAnomaly::NetworkSaturation { rx_rate, tx_rate } => {
                write!(
                    f,
                    "Network saturation: RX {:.2} MB/s, TX {:.2} MB/s",
                    rx_rate / 1_000_000.0,
                    tx_rate / 1_000_000.0
                )
            }
            MetricsAnomaly::DiskSaturation {
                read_rate,
                write_rate,
            } => {
                write!(
                    f,
                    "Disk saturation: Read {:.2} MB/s, Write {:.2} MB/s",
                    read_rate / 1_000_000.0,
                    write_rate / 1_000_000.0
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_metrics_creation() {
        let metrics = NodeMetrics::new();
        assert!(metrics.is_valid());
        assert_eq!(metrics.cpu_usage_percent, 0.0);
    }

    #[test]
    fn test_node_metrics_memory_percentage() {
        let mut metrics = NodeMetrics::new();
        metrics.memory_used_bytes = 8 * 1024 * 1024 * 1024; // 8 GB
        metrics.memory_available_bytes = 24 * 1024 * 1024 * 1024; // 24 GB
        assert!((metrics.memory_usage_percent() - 25.0).abs() < 0.1);
    }

    #[test]
    fn test_gpu_metrics_validation() {
        let mut gpu = GpuMetrics::new(0);
        gpu.usage_percent = 50.0;
        gpu.temperature_celsius = 70.0;
        gpu.power_watts = 250.0;
        assert!(gpu.is_valid());

        gpu.usage_percent = 150.0; // Invalid
        assert!(!gpu.is_valid());
    }

    #[test]
    fn test_metric_stats_calculation() {
        let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let stats = MetricStats::from_values(&values).unwrap();

        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 50.0);
        assert_eq!(stats.mean, 30.0);
        assert_eq!(stats.latest, 50.0);
        // std_dev for [10,20,30,40,50] should be ~14.14
        assert!((stats.std_dev - 14.14).abs() < 0.1);
    }

    #[test]
    fn test_metric_stats_outlier_detection() {
        let stats = MetricStats {
            min: 0.0,
            max: 100.0,
            mean: 50.0,
            std_dev: 10.0,
            latest: 95.0,
        };

        // 95 is 4.5 std devs above mean
        assert!(stats.is_high_outlier(3.0));
        assert!(!stats.is_low_outlier(3.0));

        let low_stats = MetricStats {
            min: 0.0,
            max: 100.0,
            mean: 50.0,
            std_dev: 10.0,
            latest: 5.0,
        };

        // 5 is 4.5 std devs below mean
        assert!(!low_stats.is_high_outlier(3.0));
        assert!(low_stats.is_low_outlier(3.0));
    }

    #[test]
    fn test_time_window_durations() {
        assert_eq!(TimeWindow::OneMinute.duration(), Duration::minutes(1));
        assert_eq!(TimeWindow::FiveMinutes.duration(), Duration::minutes(5));
        assert_eq!(TimeWindow::OneHour.duration(), Duration::hours(1));
        assert_eq!(TimeWindow::TwentyFourHours.duration(), Duration::hours(24));
    }

    #[test]
    fn test_metrics_aggregator_basic() {
        let mut aggregator = MetricsAggregator::new();

        // Add some samples
        for i in 0..10 {
            let mut metrics = NodeMetrics::new();
            metrics.cpu_usage_percent = 20.0 + i as f32 * 5.0;
            metrics.memory_used_bytes = 4 * 1024 * 1024 * 1024;
            metrics.memory_available_bytes = 12 * 1024 * 1024 * 1024;
            aggregator.add_sample(metrics);
        }

        assert_eq!(aggregator.sample_count(), 10);

        let latest = aggregator.latest().unwrap();
        assert!((latest.cpu_usage_percent - 65.0).abs() < 0.1);
    }

    #[test]
    fn test_metrics_aggregator_aggregation() {
        let mut aggregator = MetricsAggregator::new();

        // Add samples with known CPU values
        for i in 0..5 {
            let mut metrics = NodeMetrics::new();
            metrics.cpu_usage_percent = (i * 20) as f32; // 0, 20, 40, 60, 80
            metrics.memory_used_bytes = 4 * 1024 * 1024 * 1024;
            metrics.memory_available_bytes = 12 * 1024 * 1024 * 1024;
            metrics.collected_at = Utc::now();
            aggregator.add_sample(metrics);
        }

        let agg = aggregator.aggregate(TimeWindow::OneMinute).unwrap();
        assert_eq!(agg.sample_count, 5);
        assert_eq!(agg.cpu_usage.min, 0.0);
        assert_eq!(agg.cpu_usage.max, 80.0);
        assert!((agg.cpu_usage.mean - 40.0).abs() < 0.1);
    }

    #[test]
    fn test_anomaly_detection() {
        let mut aggregator = MetricsAggregator::with_capacity(100);

        // Add baseline samples with low CPU
        for _ in 0..20 {
            let mut metrics = NodeMetrics::new();
            metrics.cpu_usage_percent = 30.0;
            metrics.memory_used_bytes = 4 * 1024 * 1024 * 1024;
            metrics.memory_available_bytes = 12 * 1024 * 1024 * 1024;
            aggregator.add_sample(metrics);
        }

        // Add a spike
        let mut spike_metrics = NodeMetrics::new();
        spike_metrics.cpu_usage_percent = 95.0;
        spike_metrics.memory_used_bytes = 4 * 1024 * 1024 * 1024;
        spike_metrics.memory_available_bytes = 12 * 1024 * 1024 * 1024;
        aggregator.add_sample(spike_metrics);

        let anomalies = aggregator.detect_anomalies(TimeWindow::OneMinute);

        // Should detect the CPU spike
        let has_cpu_spike = anomalies
            .iter()
            .any(|a| matches!(a, MetricsAnomaly::CpuSpike { .. }));
        assert!(has_cpu_spike);
    }

    #[test]
    fn test_high_task_failure_rate_detection() {
        let mut aggregator = MetricsAggregator::with_capacity(100);

        // Add sample with high failure rate
        let mut metrics = NodeMetrics::new();
        metrics.tasks_completed = 80;
        metrics.tasks_failed = 20; // 20% failure rate
        aggregator.add_sample(metrics);

        let anomalies = aggregator.detect_anomalies(TimeWindow::OneMinute);
        let has_failure_rate_anomaly = anomalies
            .iter()
            .any(|a| matches!(a, MetricsAnomaly::HighTaskFailureRate { .. }));
        assert!(has_failure_rate_anomaly);
    }
}
