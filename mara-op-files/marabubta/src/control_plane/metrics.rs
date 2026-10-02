// Marabunta - Licensed under the MIT License.
//! Metrics Collection System for Marabunta Compute
//!
//! This module provides comprehensive metrics collection, aggregation, and analysis
//! for the distributed computing infrastructure. It handles:
//!
//! - Real-time metrics reporting from compute nodes
//! - Time-series storage with automatic downsampling
//! - Per-node, per-region, and per-job aggregation
//! - Health scoring and anomaly detection
//! - FLOPS estimation and capacity planning
//! - WebSocket event emission for real-time monitoring
//!
//! # Architecture
//!
//! ```text
//! Nodes -> MetricsCollector -> MetricsAggregator -> TimeSeriesStore
//!                                      |
//!                                      v
//!                            WebSocket Events (thresholds)
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::control_plane::metrics::{MetricsCollector, MetricsConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = MetricsConfig::default();
//!     let collector = MetricsCollector::new(config);
//!
//!     // Start background aggregation
//!     collector.start_aggregation().await;
//! }
//! ```

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

use crate::common::{JobId, RegionId};
use crate::infrastructure::NodeId;

// ================================
// Configuration
// ================================

/// Configuration for the metrics collection system
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Maximum metrics reports per node per minute
    pub rate_limit_per_node: usize,
    /// Retention for detailed metrics (1 hour)
    pub detailed_retention: ChronoDuration,
    /// Retention for hourly aggregates (24 hours)
    pub hourly_retention: ChronoDuration,
    /// Retention for daily aggregates (7 days)
    pub daily_retention: ChronoDuration,
    /// Aggregation interval
    pub aggregation_interval: Duration,
    /// Maximum ring buffer size per metric per node
    pub max_ring_buffer_size: usize,
    /// WebSocket event channel capacity
    pub event_channel_capacity: usize,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_node: 60,
            detailed_retention: ChronoDuration::hours(1),
            hourly_retention: ChronoDuration::hours(24),
            daily_retention: ChronoDuration::days(7),
            aggregation_interval: Duration::from_secs(10),
            max_ring_buffer_size: 360, // 1 hour at 10s intervals
            event_channel_capacity: 1000,
        }
    }
}

// ================================
// Node Report Types
// ================================

/// Metrics report from a compute node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReport {
    /// Node identifier
    pub node_id: NodeId,
    /// Report timestamp (Unix milliseconds)
    pub timestamp: u64,
    /// CPU usage percentage (0-100 per core, can exceed 100)
    pub cpu_usage_percent: f32,
    /// Memory used in MB
    pub memory_used_mb: u32,
    /// Estimated FLOPS capacity
    pub cpu_flops_available: f64,
    /// Network latency to coordinator in milliseconds
    pub latency_to_coordinator_ms: u32,
    /// Work unit statistics since last report
    pub work_units_since_last: WorkUnitStats,
    /// Battery level for mobile devices (0-100)
    pub battery_level: Option<u8>,
    /// CPU throttling information
    pub throttling: Option<ThrottlingInfo>,
    /// Temperature in Celsius
    pub temperature_celsius: Option<f32>,
}

/// Statistics about work units completed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkUnitStats {
    /// Number of work units completed
    pub completed: u32,
    /// Number of work units failed
    pub failed: u32,
    /// Total compute time in milliseconds
    pub total_compute_ms: u64,
    /// Average work unit duration in milliseconds
    pub avg_duration_ms: u32,
}

impl Default for WorkUnitStats {
    fn default() -> Self {
        Self {
            completed: 0,
            failed: 0,
            total_compute_ms: 0,
            avg_duration_ms: 0,
        }
    }
}

/// CPU throttling information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThrottlingInfo {
    /// Is CPU currently throttled
    pub is_throttled: bool,
    /// Throttling reason
    pub reason: ThrottlingReason,
    /// Current frequency in MHz
    pub current_freq_mhz: u32,
    /// Base frequency in MHz
    pub base_freq_mhz: u32,
}

/// Reason for CPU throttling
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThrottlingReason {
    /// Thermal throttling
    Thermal,
    /// Power limit
    PowerLimit,
    /// Battery saver mode
    BatterySaver,
    /// System policy
    SystemPolicy,
    /// No throttling
    None,
}

/// Batch of node reports for efficient transmission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReportBatch {
    /// Reports in this batch
    pub reports: Vec<NodeReport>,
}

// ================================
// Metrics Collector
// ================================

/// Metrics collector receives and validates reports from nodes
pub struct MetricsCollector {
    /// Configuration
    config: MetricsConfig,
    /// Rate limiting: node_id -> (timestamp, count)
    rate_limiter: Arc<DashMap<NodeId, (DateTime<Utc>, usize)>>,
    /// Raw metrics storage
    raw_metrics: Arc<DashMap<NodeId, VecDeque<NodeReport>>>,
    /// Metrics aggregator
    aggregator: Arc<MetricsAggregator>,
    /// Event broadcaster for WebSocket
    event_tx: broadcast::Sender<MetricsEvent>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new(config: MetricsConfig) -> Self {
        let (event_tx, _) = broadcast::channel(config.event_channel_capacity);

        Self {
            aggregator: Arc::new(MetricsAggregator::new(config.clone())),
            config,
            rate_limiter: Arc::new(DashMap::new()),
            raw_metrics: Arc::new(DashMap::new()),
            event_tx,
        }
    }

    /// Subscribe to metrics events for WebSocket streaming
    pub fn subscribe_events(&self) -> broadcast::Receiver<MetricsEvent> {
        self.event_tx.subscribe()
    }

    /// Handle a single node report (HTTP POST endpoint handler)
    pub async fn report_metrics(&self, report: NodeReport) -> Result<(), MetricsError> {
        // Validate report
        self.validate_report(&report)?;

        // Check rate limit
        if !self.check_rate_limit(&report.node_id) {
            warn!("Rate limit exceeded for node {}", report.node_id);
            return Err(MetricsError::RateLimitExceeded);
        }

        // Store raw metrics
        self.store_raw_metrics(report.clone());

        // Send to aggregator
        self.aggregator.add_report(report).await;

        Ok(())
    }

    /// Handle a batch of node reports (HTTP POST endpoint handler)
    pub async fn report_metrics_batch(&self, batch: NodeReportBatch) -> Result<(), MetricsError> {
        for report in batch.reports {
            self.report_metrics(report).await?;
        }
        Ok(())
    }

    /// Validate a metrics report
    fn validate_report(&self, report: &NodeReport) -> Result<(), MetricsError> {
        // Check timestamp is reasonable (not too old, not in future)
        let now = Utc::now().timestamp_millis() as u64;
        let report_age = now.saturating_sub(report.timestamp);

        if report_age > 300_000 {
            // More than 5 minutes old
            return Err(MetricsError::ReportTooOld);
        }

        if report.timestamp > now + 60_000 {
            // More than 1 minute in future
            return Err(MetricsError::ReportInFuture);
        }

        // Sanitize values
        if report.cpu_usage_percent < 0.0 || report.cpu_usage_percent > 10000.0 {
            return Err(MetricsError::InvalidCpuUsage);
        }

        if let Some(battery) = report.battery_level {
            if battery > 100 {
                return Err(MetricsError::InvalidBatteryLevel);
            }
        }

        if let Some(temp) = report.temperature_celsius {
            if temp < -50.0 || temp > 150.0 {
                return Err(MetricsError::InvalidTemperature);
            }
        }

        Ok(())
    }

    /// Check and update rate limit for a node
    fn check_rate_limit(&self, node_id: &NodeId) -> bool {
        let now = Utc::now();
        let mut entry = self.rate_limiter.entry(*node_id).or_insert((now, 0));

        let (last_reset, count) = *entry;
        let elapsed = now.signed_duration_since(last_reset);

        if elapsed > ChronoDuration::minutes(1) {
            // Reset counter
            *entry = (now, 1);
            true
        } else if count < self.config.rate_limit_per_node {
            // Increment counter
            *entry = (last_reset, count + 1);
            true
        } else {
            // Rate limit exceeded
            false
        }
    }

    /// Store raw metrics in ring buffer
    fn store_raw_metrics(&self, report: NodeReport) {
        let mut entry = self
            .raw_metrics
            .entry(report.node_id)
            .or_insert_with(VecDeque::new);

        entry.push_back(report);

        // Maintain max size
        while entry.len() > self.config.max_ring_buffer_size {
            entry.pop_front();
        }
    }

    /// Start background aggregation task
    pub async fn start_aggregation(&self) {
        let aggregator = self.aggregator.clone();
        let event_tx = self.event_tx.clone();
        let interval_duration = self.config.aggregation_interval;

        tokio::spawn(async move {
            let mut ticker = interval(interval_duration);

            loop {
                ticker.tick().await;

                // Run aggregation
                aggregator.aggregate().await;

                // Check thresholds and emit events
                if let Some(events) = aggregator.check_thresholds().await {
                    for event in events {
                        let _ = event_tx.send(event);
                    }
                }
            }
        });

        info!(
            "Metrics aggregation started with interval {:?}",
            interval_duration
        );
    }

    /// Get aggregator for queries
    pub fn aggregator(&self) -> Arc<MetricsAggregator> {
        self.aggregator.clone()
    }
}

// ================================
// Metrics Aggregator
// ================================

/// Aggregates raw metrics into rolling averages and statistics
pub struct MetricsAggregator {
    /// Configuration
    #[allow(dead_code)]
    config: MetricsConfig,
    /// Pending reports to aggregate
    pending: Arc<RwLock<Vec<NodeReport>>>,
    /// Per-node rolling averages
    node_averages: Arc<DashMap<NodeId, NodeAverages>>,
    /// Per-region aggregates
    #[allow(dead_code)]
    region_aggregates: Arc<DashMap<RegionId, RegionAggregate>>,
    /// Per-job aggregates
    #[allow(dead_code)]
    job_aggregates: Arc<DashMap<JobId, JobAggregate>>,
    /// Time series store
    time_series: Arc<TimeSeriesStore>,
    /// FLOPS estimator
    flops_estimator: Arc<FlopsEstimator>,
    /// Health scorer
    health_scorer: Arc<HealthScorer>,
}

impl MetricsAggregator {
    /// Create a new metrics aggregator
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            time_series: Arc::new(TimeSeriesStore::new(config.clone())),
            config,
            pending: Arc::new(RwLock::new(Vec::new())),
            node_averages: Arc::new(DashMap::new()),
            region_aggregates: Arc::new(DashMap::new()),
            job_aggregates: Arc::new(DashMap::new()),
            flops_estimator: Arc::new(FlopsEstimator::new()),
            health_scorer: Arc::new(HealthScorer::new()),
        }
    }

    /// Add a report to be aggregated
    pub async fn add_report(&self, report: NodeReport) {
        self.pending.write().push(report);
    }

    /// Run aggregation on pending reports
    pub async fn aggregate(&self) {
        let reports: Vec<NodeReport> = {
            let mut pending = self.pending.write();
            std::mem::take(&mut *pending)
        };

        if reports.is_empty() {
            return;
        }

        debug!("Aggregating {} reports", reports.len());

        for report in reports {
            // Update node averages
            self.update_node_averages(&report);

            // Store in time series
            self.time_series.store_report(&report);

            // Update health score
            self.health_scorer.update(&report);
        }
    }

    /// Update rolling averages for a node
    fn update_node_averages(&self, report: &NodeReport) {
        let mut entry = self
            .node_averages
            .entry(report.node_id)
            .or_insert_with(NodeAverages::new);

        entry.update(report);
    }

    /// Check thresholds and generate events
    pub async fn check_thresholds(&self) -> Option<Vec<MetricsEvent>> {
        let mut events = Vec::new();

        for entry in self.node_averages.iter() {
            let node_id = *entry.key();
            let averages = entry.value();

            // High CPU usage
            if averages.cpu_1min > 90.0 {
                events.push(MetricsEvent::HighCpuUsage {
                    node_id,
                    usage_percent: averages.cpu_1min,
                });
            }

            // High memory usage
            if averages.memory_1min_mb > 0 {
                let usage_percent = (averages.memory_1min_mb as f32
                    / (averages.memory_1min_mb as f32 * 1.2))
                    * 100.0;
                if usage_percent > 85.0 {
                    events.push(MetricsEvent::HighMemoryUsage {
                        node_id,
                        usage_mb: averages.memory_1min_mb as u32,
                    });
                }
            }

            // High latency
            if averages.latency_1min_ms > 500 {
                events.push(MetricsEvent::HighLatency {
                    node_id,
                    latency_ms: averages.latency_1min_ms,
                });
            }

            // Low battery (for mobile devices)
            if let Some(battery) = averages.battery_level {
                if battery < 20 {
                    events.push(MetricsEvent::LowBattery {
                        node_id,
                        battery_percent: battery,
                    });
                }
            }
        }

        if events.is_empty() {
            None
        } else {
            Some(events)
        }
    }

    /// Get node averages
    pub fn get_node_averages(&self, node_id: &NodeId) -> Option<NodeAverages> {
        self.node_averages.get(node_id).map(|r| r.value().clone())
    }

    /// Get health score for a node
    pub fn get_health_score(&self, node_id: &NodeId) -> Option<u8> {
        self.health_scorer.get_score(node_id)
    }

    /// Get time series store
    pub fn time_series(&self) -> Arc<TimeSeriesStore> {
        self.time_series.clone()
    }

    /// Get FLOPS estimator
    pub fn flops_estimator(&self) -> Arc<FlopsEstimator> {
        self.flops_estimator.clone()
    }
}

/// Rolling averages for a node
#[derive(Debug, Clone)]
pub struct NodeAverages {
    /// 1-minute CPU average
    pub cpu_1min: f32,
    /// 5-minute CPU average
    pub cpu_5min: f32,
    /// 15-minute CPU average
    pub cpu_15min: f32,
    /// 1-minute memory average (MB)
    pub memory_1min_mb: u64,
    /// 5-minute memory average (MB)
    pub memory_5min_mb: u64,
    /// 15-minute memory average (MB)
    pub memory_15min_mb: u64,
    /// 1-minute latency average (ms)
    pub latency_1min_ms: u32,
    /// 5-minute latency average (ms)
    pub latency_5min_ms: u32,
    /// 15-minute latency average (ms)
    pub latency_15min_ms: u32,
    /// Latest battery level
    pub battery_level: Option<u8>,
    /// Recent samples for 1min window
    samples_1min: VecDeque<MetricSample>,
    /// Recent samples for 5min window
    samples_5min: VecDeque<MetricSample>,
    /// Recent samples for 15min window
    samples_15min: VecDeque<MetricSample>,
}

#[derive(Debug, Clone)]
struct MetricSample {
    timestamp: DateTime<Utc>,
    cpu: f32,
    memory_mb: u32,
    latency_ms: u32,
}

impl NodeAverages {
    fn new() -> Self {
        Self {
            cpu_1min: 0.0,
            cpu_5min: 0.0,
            cpu_15min: 0.0,
            memory_1min_mb: 0,
            memory_5min_mb: 0,
            memory_15min_mb: 0,
            latency_1min_ms: 0,
            latency_5min_ms: 0,
            latency_15min_ms: 0,
            battery_level: None,
            samples_1min: VecDeque::new(),
            samples_5min: VecDeque::new(),
            samples_15min: VecDeque::new(),
        }
    }

    fn update(&mut self, report: &NodeReport) {
        let sample = MetricSample {
            timestamp: Utc::now(),
            cpu: report.cpu_usage_percent,
            memory_mb: report.memory_used_mb,
            latency_ms: report.latency_to_coordinator_ms,
        };

        // Add to all windows
        self.samples_1min.push_back(sample.clone());
        self.samples_5min.push_back(sample.clone());
        self.samples_15min.push_back(sample);

        // Prune old samples
        let now = Utc::now();
        Self::prune_samples(&mut self.samples_1min, now, 60);
        Self::prune_samples(&mut self.samples_5min, now, 300);
        Self::prune_samples(&mut self.samples_15min, now, 900);

        // Calculate averages
        self.cpu_1min = self.calculate_avg_cpu(&self.samples_1min);
        self.cpu_5min = self.calculate_avg_cpu(&self.samples_5min);
        self.cpu_15min = self.calculate_avg_cpu(&self.samples_15min);

        self.memory_1min_mb = self.calculate_avg_memory(&self.samples_1min);
        self.memory_5min_mb = self.calculate_avg_memory(&self.samples_5min);
        self.memory_15min_mb = self.calculate_avg_memory(&self.samples_15min);

        self.latency_1min_ms = self.calculate_avg_latency(&self.samples_1min);
        self.latency_5min_ms = self.calculate_avg_latency(&self.samples_5min);
        self.latency_15min_ms = self.calculate_avg_latency(&self.samples_15min);

        // Update battery level
        self.battery_level = report.battery_level;
    }

    fn prune_samples(samples: &mut VecDeque<MetricSample>, now: DateTime<Utc>, max_age_secs: i64) {
        while let Some(sample) = samples.front() {
            if now.signed_duration_since(sample.timestamp).num_seconds() > max_age_secs {
                samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn calculate_avg_cpu(&self, samples: &VecDeque<MetricSample>) -> f32 {
        if samples.is_empty() {
            return 0.0;
        }
        samples.iter().map(|s| s.cpu).sum::<f32>() / samples.len() as f32
    }

    fn calculate_avg_memory(&self, samples: &VecDeque<MetricSample>) -> u64 {
        if samples.is_empty() {
            return 0;
        }
        samples.iter().map(|s| s.memory_mb as u64).sum::<u64>() / samples.len() as u64
    }

    fn calculate_avg_latency(&self, samples: &VecDeque<MetricSample>) -> u32 {
        if samples.is_empty() {
            return 0;
        }
        (samples.iter().map(|s| s.latency_ms as u64).sum::<u64>() / samples.len() as u64) as u32
    }
}

/// Per-region aggregate statistics
#[derive(Debug, Clone, Default)]
pub struct RegionAggregate {
    /// Total nodes in region
    pub node_count: u32,
    /// Total CPU cores
    pub total_cpu_cores: u32,
    /// Total memory MB
    pub total_memory_mb: u64,
    /// Average CPU usage
    pub avg_cpu_usage: f32,
    /// Average memory usage
    pub avg_memory_usage: f32,
    /// Total FLOPS available
    pub total_flops: f64,
}

/// Per-job aggregate statistics
#[derive(Debug, Clone, Default)]
pub struct JobAggregate {
    /// Total tasks
    pub task_count: u32,
    /// Completed tasks
    pub completed_tasks: u32,
    /// Failed tasks
    pub failed_tasks: u32,
    /// Total compute time milliseconds
    pub total_compute_ms: u64,
    /// Average task duration
    pub avg_task_duration_ms: u32,
}

// ================================
// Time Series Store
// ================================

/// Time-series storage with automatic downsampling
pub struct TimeSeriesStore {
    /// Configuration
    config: MetricsConfig,
    /// Detailed metrics (per node, per metric)
    detailed: Arc<DashMap<NodeId, NodeTimeSeries>>,
    /// Hourly aggregates
    #[allow(dead_code)]
    hourly: Arc<DashMap<NodeId, VecDeque<HourlyAggregate>>>,
    /// Daily aggregates
    #[allow(dead_code)]
    daily: Arc<DashMap<NodeId, VecDeque<DailyAggregate>>>,
}

impl TimeSeriesStore {
    fn new(config: MetricsConfig) -> Self {
        Self {
            config,
            detailed: Arc::new(DashMap::new()),
            hourly: Arc::new(DashMap::new()),
            daily: Arc::new(DashMap::new()),
        }
    }

    /// Store a report in the time series
    fn store_report(&self, report: &NodeReport) {
        let mut entry = self
            .detailed
            .entry(report.node_id)
            .or_insert_with(|| NodeTimeSeries::new(self.config.max_ring_buffer_size));

        entry.add_report(report);
    }

    /// Query metrics by time range
    pub fn query_range(
        &self,
        node_id: &NodeId,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<NodeReport> {
        if let Some(entry) = self.detailed.get(node_id) {
            entry.query_range(start, end)
        } else {
            Vec::new()
        }
    }

    /// Get latest metrics for a node
    pub fn get_latest(&self, node_id: &NodeId) -> Option<NodeReport> {
        self.detailed.get(node_id)?.get_latest()
    }

    /// Calculate latency percentiles for a node
    pub fn latency_percentiles(&self, node_id: &NodeId) -> Option<LatencyPercentiles> {
        self.detailed.get(node_id)?.calculate_latency_percentiles()
    }
}

/// Time series data for a single node
#[derive(Debug)]
struct NodeTimeSeries {
    /// Ring buffer of reports
    reports: VecDeque<NodeReport>,
    /// Maximum size
    max_size: usize,
}

impl NodeTimeSeries {
    fn new(max_size: usize) -> Self {
        Self {
            reports: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    fn add_report(&mut self, report: &NodeReport) {
        self.reports.push_back(report.clone());

        while self.reports.len() > self.max_size {
            self.reports.pop_front();
        }
    }

    fn query_range(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<NodeReport> {
        let start_ms = start.timestamp_millis() as u64;
        let end_ms = end.timestamp_millis() as u64;

        self.reports
            .iter()
            .filter(|r| r.timestamp >= start_ms && r.timestamp <= end_ms)
            .cloned()
            .collect()
    }

    fn get_latest(&self) -> Option<NodeReport> {
        self.reports.back().cloned()
    }

    fn calculate_latency_percentiles(&self) -> Option<LatencyPercentiles> {
        if self.reports.is_empty() {
            return None;
        }

        let mut latencies: Vec<u32> = self
            .reports
            .iter()
            .map(|r| r.latency_to_coordinator_ms)
            .collect();

        latencies.sort_unstable();

        let p50_idx = latencies.len() / 2;
        let p95_idx = (latencies.len() * 95) / 100;
        let p99_idx = (latencies.len() * 99) / 100;

        Some(LatencyPercentiles {
            p50: latencies[p50_idx],
            p95: latencies[p95_idx.min(latencies.len() - 1)],
            p99: latencies[p99_idx.min(latencies.len() - 1)],
        })
    }
}

/// Latency percentile statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    /// 50th percentile (median)
    pub p50: u32,
    /// 95th percentile
    pub p95: u32,
    /// 99th percentile
    pub p99: u32,
}

/// Hourly aggregate
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct HourlyAggregate {
    hour: DateTime<Utc>,
    avg_cpu: f32,
    avg_memory_mb: u64,
    avg_latency_ms: u32,
}

/// Daily aggregate
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DailyAggregate {
    day: DateTime<Utc>,
    avg_cpu: f32,
    avg_memory_mb: u64,
    avg_latency_ms: u32,
}

// ================================
// FLOPS Estimator
// ================================

/// Estimates FLOPS capacity based on CPU model and benchmarks
pub struct FlopsEstimator {
    /// Lookup table for known CPU models
    cpu_flops_table: Arc<DashMap<String, f64>>,
    /// Benchmark results for unknown CPUs
    benchmark_results: Arc<DashMap<NodeId, f64>>,
}

impl FlopsEstimator {
    fn new() -> Self {
        let estimator = Self {
            cpu_flops_table: Arc::new(DashMap::new()),
            benchmark_results: Arc::new(DashMap::new()),
        };

        // Populate known CPU FLOPS
        estimator.populate_cpu_table();

        estimator
    }

    fn populate_cpu_table(&self) {
        // Intel CPUs (GFLOPS per core)
        self.cpu_flops_table
            .insert("Intel Xeon E5-2690 v4".to_string(), 38.4);
        self.cpu_flops_table
            .insert("Intel Xeon Gold 6148".to_string(), 51.2);
        self.cpu_flops_table
            .insert("Intel Xeon Platinum 8280".to_string(), 76.8);
        self.cpu_flops_table
            .insert("Intel Core i7-9700K".to_string(), 44.8);
        self.cpu_flops_table
            .insert("Intel Core i9-9900K".to_string(), 57.6);

        // AMD CPUs (GFLOPS per core)
        self.cpu_flops_table
            .insert("AMD EPYC 7742".to_string(), 64.0);
        self.cpu_flops_table
            .insert("AMD EPYC 7551".to_string(), 38.4);
        self.cpu_flops_table
            .insert("AMD Ryzen 9 5950X".to_string(), 89.6);
        self.cpu_flops_table
            .insert("AMD Ryzen 7 5800X".to_string(), 76.8);

        // ARM CPUs (GFLOPS per core)
        self.cpu_flops_table
            .insert("ARM Cortex-A76".to_string(), 12.8);
        self.cpu_flops_table
            .insert("ARM Cortex-A78".to_string(), 16.0);
        self.cpu_flops_table.insert("Apple M1".to_string(), 140.0); // Total, not per core
    }

    /// Estimate FLOPS for a CPU model
    pub fn estimate_flops(&self, cpu_model: &str, cores: u32) -> f64 {
        // Try exact match first
        if let Some(flops_per_core) = self.cpu_flops_table.get(cpu_model) {
            return *flops_per_core * cores as f64;
        }

        // Try partial match
        for entry in self.cpu_flops_table.iter() {
            if cpu_model.contains(entry.key()) || entry.key().contains(cpu_model) {
                return *entry.value() * cores as f64;
            }
        }

        // Fallback: rough estimate based on generation
        self.estimate_generic(cpu_model, cores)
    }

    fn estimate_generic(&self, cpu_model: &str, cores: u32) -> f64 {
        // Very rough estimates
        let base_flops = if cpu_model.contains("Xeon") || cpu_model.contains("EPYC") {
            40.0 // Server-grade
        } else if cpu_model.contains("Ryzen")
            || cpu_model.contains("Core i7")
            || cpu_model.contains("Core i9")
        {
            50.0 // High-end desktop
        } else if cpu_model.contains("Core i5") {
            35.0 // Mid-range desktop
        } else if cpu_model.contains("Core i3") || cpu_model.contains("Ryzen 3") {
            25.0 // Budget desktop
        } else if cpu_model.contains("Celeron") || cpu_model.contains("Pentium") {
            15.0 // Low-end
        } else if cpu_model.contains("ARM") || cpu_model.contains("Cortex") {
            12.0 // ARM
        } else {
            20.0 // Unknown
        };

        base_flops * cores as f64
    }

    /// Record benchmark result for a node
    pub fn record_benchmark(&self, node_id: NodeId, flops: f64) {
        self.benchmark_results.insert(node_id, flops);
    }

    /// Get FLOPS for a node (from benchmark if available)
    pub fn get_node_flops(&self, node_id: &NodeId, cpu_model: &str, cores: u32) -> f64 {
        // Check benchmark first
        if let Some(flops) = self.benchmark_results.get(node_id) {
            return *flops;
        }

        // Fall back to estimation
        self.estimate_flops(cpu_model, cores)
    }

    /// Calculate aggregate FLOPS by region
    pub fn aggregate_by_region(
        &self,
        nodes: &[(NodeId, String, u32, RegionId)],
    ) -> HashMap<RegionId, f64> {
        let mut region_flops: HashMap<RegionId, f64> = HashMap::new();

        for (node_id, cpu_model, cores, region_id) in nodes {
            let flops = self.get_node_flops(node_id, cpu_model, *cores);
            *region_flops.entry(*region_id).or_insert(0.0) += flops;
        }

        region_flops
    }
}

// ================================
// Health Scorer
// ================================

/// Calculates health scores for nodes based on multiple factors
pub struct HealthScorer {
    /// Health scores per node
    scores: Arc<DashMap<NodeId, NodeHealth>>,
}

impl HealthScorer {
    fn new() -> Self {
        Self {
            scores: Arc::new(DashMap::new()),
        }
    }

    /// Update health metrics for a node
    fn update(&self, report: &NodeReport) {
        let mut entry = self
            .scores
            .entry(report.node_id)
            .or_insert_with(NodeHealth::new);

        entry.update(report);
    }

    /// Get health score for a node (0-100)
    pub fn get_score(&self, node_id: &NodeId) -> Option<u8> {
        self.scores.get(node_id).map(|h| h.calculate_score())
    }

    /// Get detailed health info
    pub fn get_health(&self, node_id: &NodeId) -> Option<NodeHealth> {
        self.scores.get(node_id).map(|h| h.clone())
    }
}

/// Health information for a node
#[derive(Debug, Clone)]
pub struct NodeHealth {
    /// Uptime stability (recent disconnects)
    pub uptime_score: f32,
    /// Work unit success rate
    pub success_rate: f32,
    /// Latency consistency (low variance is better)
    pub latency_consistency: f32,
    /// Battery level (for mobile devices)
    pub battery_score: f32,
    /// Recent heartbeats
    heartbeats: VecDeque<DateTime<Utc>>,
    /// Recent work unit stats
    recent_work_units: VecDeque<WorkUnitStats>,
    /// Recent latencies
    recent_latencies: VecDeque<u32>,
}

impl NodeHealth {
    fn new() -> Self {
        Self {
            uptime_score: 100.0,
            success_rate: 100.0,
            latency_consistency: 100.0,
            battery_score: 100.0,
            heartbeats: VecDeque::new(),
            recent_work_units: VecDeque::new(),
            recent_latencies: VecDeque::new(),
        }
    }

    fn update(&mut self, report: &NodeReport) {
        // Update heartbeats
        self.heartbeats.push_back(Utc::now());
        while self.heartbeats.len() > 100 {
            self.heartbeats.pop_front();
        }

        // Update work units
        self.recent_work_units
            .push_back(report.work_units_since_last.clone());
        while self.recent_work_units.len() > 50 {
            self.recent_work_units.pop_front();
        }

        // Update latencies
        self.recent_latencies
            .push_back(report.latency_to_coordinator_ms);
        while self.recent_latencies.len() > 100 {
            self.recent_latencies.pop_front();
        }

        // Calculate scores
        self.uptime_score = self.calculate_uptime_score();
        self.success_rate = self.calculate_success_rate();
        self.latency_consistency = self.calculate_latency_consistency();
        self.battery_score = self.calculate_battery_score(report.battery_level);
    }

    fn calculate_uptime_score(&self) -> f32 {
        if self.heartbeats.len() < 2 {
            return 100.0;
        }

        // Check for gaps in heartbeats (should be every 10s)
        let mut gap_count = 0;
        let mut total_intervals = 0;

        for window in self.heartbeats.iter().collect::<Vec<_>>().windows(2) {
            let gap = window[1].signed_duration_since(*window[0]);
            total_intervals += 1;

            if gap.num_seconds() > 30 {
                // More than 30s gap indicates disconnect
                gap_count += 1;
            }
        }

        if total_intervals == 0 {
            return 100.0;
        }

        let disconnect_rate = gap_count as f32 / total_intervals as f32;
        ((1.0 - disconnect_rate) * 100.0).max(0.0)
    }

    fn calculate_success_rate(&self) -> f32 {
        if self.recent_work_units.is_empty() {
            return 100.0;
        }

        let total_completed: u32 = self.recent_work_units.iter().map(|w| w.completed).sum();
        let total_failed: u32 = self.recent_work_units.iter().map(|w| w.failed).sum();

        let total = total_completed + total_failed;
        if total == 0 {
            return 100.0;
        }

        (total_completed as f32 / total as f32) * 100.0
    }

    fn calculate_latency_consistency(&self) -> f32 {
        if self.recent_latencies.len() < 2 {
            return 100.0;
        }

        let mean: f32 = self.recent_latencies.iter().map(|&l| l as f32).sum::<f32>()
            / self.recent_latencies.len() as f32;

        let variance: f32 = self
            .recent_latencies
            .iter()
            .map(|&l| {
                let diff = l as f32 - mean;
                diff * diff
            })
            .sum::<f32>()
            / self.recent_latencies.len() as f32;

        let std_dev = variance.sqrt();
        let coefficient_of_variation = if mean > 0.0 { std_dev / mean } else { 0.0 };

        // Lower CV is better; map 0-1 to 100-0
        ((1.0 - coefficient_of_variation.min(1.0)) * 100.0).max(0.0)
    }

    fn calculate_battery_score(&self, battery_level: Option<u8>) -> f32 {
        match battery_level {
            Some(level) => level as f32,
            None => 100.0, // Not a battery device, perfect score
        }
    }

    /// Calculate overall health score (0-100)
    pub fn calculate_score(&self) -> u8 {
        let weighted_score = self.uptime_score * 0.3
            + self.success_rate * 0.4
            + self.latency_consistency * 0.2
            + self.battery_score * 0.1;

        weighted_score.round() as u8
    }
}

// ================================
// Events
// ================================

/// Events emitted when thresholds are crossed
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetricsEvent {
    /// High CPU usage detected
    HighCpuUsage { node_id: NodeId, usage_percent: f32 },
    /// High memory usage detected
    HighMemoryUsage { node_id: NodeId, usage_mb: u32 },
    /// High latency detected
    HighLatency { node_id: NodeId, latency_ms: u32 },
    /// Low battery detected
    LowBattery {
        node_id: NodeId,
        battery_percent: u8,
    },
    /// Node health degraded
    HealthDegraded { node_id: NodeId, health_score: u8 },
    /// Node throttling detected
    NodeThrottled {
        node_id: NodeId,
        reason: ThrottlingReason,
    },
}

// ================================
// Errors
// ================================

/// Errors that can occur in the metrics system
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Report timestamp too old")]
    ReportTooOld,

    #[error("Report timestamp in future")]
    ReportInFuture,

    #[error("Invalid CPU usage value")]
    InvalidCpuUsage,

    #[error("Invalid battery level")]
    InvalidBatteryLevel,

    #[error("Invalid temperature")]
    InvalidTemperature,
}

// ================================
// Tests
// ================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_validation() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);

        let valid_report = NodeReport {
            node_id: NodeId::new(),
            timestamp: Utc::now().timestamp_millis() as u64,
            cpu_usage_percent: 50.0,
            memory_used_mb: 1024,
            cpu_flops_available: 100.0,
            latency_to_coordinator_ms: 50,
            work_units_since_last: WorkUnitStats::default(),
            battery_level: Some(80),
            throttling: None,
            temperature_celsius: Some(45.0),
        };

        assert!(collector.validate_report(&valid_report).is_ok());
    }

    #[test]
    fn test_flops_estimation() {
        let estimator = FlopsEstimator::new();

        let xeon_flops = estimator.estimate_flops("Intel Xeon E5-2690 v4", 8);
        assert!(xeon_flops > 0.0);

        let unknown_flops = estimator.estimate_flops("Unknown CPU", 4);
        assert!(unknown_flops > 0.0);
    }

    #[test]
    fn test_health_scoring() {
        let scorer = HealthScorer::new();
        let node_id = NodeId::new();

        let report = NodeReport {
            node_id,
            timestamp: Utc::now().timestamp_millis() as u64,
            cpu_usage_percent: 30.0,
            memory_used_mb: 512,
            cpu_flops_available: 50.0,
            latency_to_coordinator_ms: 25,
            work_units_since_last: WorkUnitStats {
                completed: 10,
                failed: 0,
                total_compute_ms: 1000,
                avg_duration_ms: 100,
            },
            battery_level: Some(90),
            throttling: None,
            temperature_celsius: Some(40.0),
        };

        scorer.update(&report);
        let score = scorer.get_score(&node_id);
        assert!(score.is_some());
        assert!(score.unwrap() > 0);
    }

    #[test]
    fn test_node_averages() {
        let mut averages = NodeAverages::new();

        let report = NodeReport {
            node_id: NodeId::new(),
            timestamp: Utc::now().timestamp_millis() as u64,
            cpu_usage_percent: 60.0,
            memory_used_mb: 2048,
            cpu_flops_available: 100.0,
            latency_to_coordinator_ms: 30,
            work_units_since_last: WorkUnitStats::default(),
            battery_level: None,
            throttling: None,
            temperature_celsius: None,
        };

        averages.update(&report);

        assert!(averages.cpu_1min > 0.0);
        assert!(averages.memory_1min_mb > 0);
        assert!(averages.latency_1min_ms > 0);
    }

    #[test]
    fn test_time_series_storage() {
        let mut ts = NodeTimeSeries::new(100);
        let node_id = NodeId::new();

        for i in 0u32..10 {
            let report = NodeReport {
                node_id,
                timestamp: (Utc::now().timestamp_millis() as u64) + (i as u64 * 1000),
                cpu_usage_percent: 50.0 + i as f32,
                memory_used_mb: 1000 + i * 100,
                cpu_flops_available: 100.0,
                latency_to_coordinator_ms: 20 + i,
                work_units_since_last: WorkUnitStats::default(),
                battery_level: None,
                throttling: None,
                temperature_celsius: None,
            };
            ts.add_report(&report);
        }

        assert_eq!(ts.reports.len(), 10);
        assert!(ts.get_latest().is_some());
    }
}
