// Marabunta - Licensed under the MIT License.
//! Prometheus metrics export for Marabunta Compute
//!
//! This module provides a global metrics registry and standard metric types
//! for exporting cluster, node, and resource metrics in Prometheus text format.

use lazy_static::lazy_static;
use prometheus::{
    self, Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, Opts, Registry,
    TextEncoder,
};
use std::sync::RwLock;

/// Default metric prefix for all Marabunta metrics
pub const DEFAULT_METRIC_PREFIX: &str = "marabunta";

/// Configuration for the metrics registry
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Prefix for all metric names (default: "marabunta")
    pub prefix: String,
    /// Whether to include default process metrics
    pub include_process_metrics: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prefix: DEFAULT_METRIC_PREFIX.to_string(),
            include_process_metrics: true,
        }
    }
}

impl MetricsConfig {
    /// Create a new configuration with a custom prefix
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            ..Default::default()
        }
    }

    /// Format a metric name with the configured prefix
    pub fn metric_name(&self, name: &str) -> String {
        format!("{}_{}", self.prefix, name)
    }
}

lazy_static! {
    /// Global metrics registry
    static ref REGISTRY: Registry = Registry::new();

    /// Global metrics configuration (can be updated before metrics are created)
    static ref METRICS_CONFIG: RwLock<MetricsConfig> = RwLock::new(MetricsConfig::default());

    // ============== Cluster Metrics ==============

    /// Total number of nodes in the cluster, labeled by status
    /// Labels: status (ready, busy, draining, offline, starting)
    static ref NODES_TOTAL: GaugeVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("nodes_total"),
            "Total number of nodes in the cluster"
        ).namespace(config.prefix.clone());
        let gauge = GaugeVec::new(opts, &["status"]).unwrap();
        REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Total number of jobs, labeled by status
    /// Labels: status (pending, scheduled, running, completed, failed, cancelled)
    static ref JOBS_TOTAL: CounterVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("jobs_total"),
            "Total number of jobs processed"
        ).namespace(config.prefix.clone());
        let counter = CounterVec::new(opts, &["status"]).unwrap();
        REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Total number of tasks, labeled by status
    /// Labels: status (pending, assigned, running, checkpointing, completed, failed, cancelled)
    static ref TASKS_TOTAL: CounterVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("tasks_total"),
            "Total number of tasks processed"
        ).namespace(config.prefix.clone());
        let counter = CounterVec::new(opts, &["status"]).unwrap();
        REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Task duration histogram in seconds
    /// Buckets: 1s, 5s, 10s, 30s, 60s, 300s, 600s, 1800s, 3600s
    static ref TASK_DURATION_SECONDS: Histogram = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = HistogramOpts::new(
            config.metric_name("task_duration_seconds"),
            "Task execution duration in seconds"
        )
        .namespace(config.prefix.clone())
        .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]);
        let histogram = Histogram::with_opts(opts).unwrap();
        REGISTRY.register(Box::new(histogram.clone())).unwrap();
        histogram
    };

    /// Current depth of the task queue
    static ref QUEUE_DEPTH: Gauge = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("queue_depth"),
            "Current number of tasks in the queue"
        ).namespace(config.prefix.clone());
        let gauge = Gauge::with_opts(opts).unwrap();
        REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    // ============== Node Metrics ==============

    /// CPU usage per node (0.0 - 100.0)
    /// Labels: node_id
    static ref NODE_CPU_USAGE: GaugeVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("node_cpu_usage"),
            "CPU usage percentage per node (0-100)"
        ).namespace(config.prefix.clone());
        let gauge = GaugeVec::new(opts, &["node_id"]).unwrap();
        REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Memory usage in bytes per node
    /// Labels: node_id
    static ref NODE_MEMORY_BYTES: GaugeVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("node_memory_bytes"),
            "Memory used in bytes per node"
        ).namespace(config.prefix.clone());
        let gauge = GaugeVec::new(opts, &["node_id"]).unwrap();
        REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Number of tasks currently running per node
    /// Labels: node_id
    static ref NODE_TASKS_RUNNING: GaugeVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("node_tasks_running"),
            "Number of tasks currently running per node"
        ).namespace(config.prefix.clone());
        let gauge = GaugeVec::new(opts, &["node_id"]).unwrap();
        REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Seconds since last heartbeat per node
    /// Labels: node_id
    static ref NODE_LAST_HEARTBEAT_SECONDS: GaugeVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("node_last_heartbeat_seconds"),
            "Seconds since the last heartbeat from this node"
        ).namespace(config.prefix.clone());
        let gauge = GaugeVec::new(opts, &["node_id"]).unwrap();
        REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    // ============== Resource Metrics ==============

    /// Quota usage ratio per account (0.0 - 1.0+)
    /// Labels: account_id, resource_type
    static ref QUOTA_USAGE_RATIO: GaugeVec = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("quota_usage_ratio"),
            "Quota usage ratio (used/allocated) per account and resource type"
        ).namespace(config.prefix.clone());
        let gauge = GaugeVec::new(opts, &["account_id", "resource_type"]).unwrap();
        REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Total number of preemptions
    static ref PREEMPTIONS_TOTAL: Counter = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = Opts::new(
            config.metric_name("preemptions_total"),
            "Total number of task preemptions"
        ).namespace(config.prefix.clone());
        let counter = Counter::with_opts(opts).unwrap();
        REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Scheduling latency histogram in seconds
    /// Buckets: 0.001s, 0.005s, 0.01s, 0.05s, 0.1s, 0.5s, 1.0s, 5.0s, 10.0s
    static ref SCHEDULING_LATENCY_SECONDS: Histogram = {
        let config = METRICS_CONFIG.read().unwrap();
        let opts = HistogramOpts::new(
            config.metric_name("scheduling_latency_seconds"),
            "Time taken to schedule a task in seconds"
        )
        .namespace(config.prefix.clone())
        .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]);
        let histogram = Histogram::with_opts(opts).unwrap();
        REGISTRY.register(Box::new(histogram.clone())).unwrap();
        histogram
    };
}

/// Initialize metrics with custom configuration
/// Must be called before any metrics are accessed
pub fn init_metrics(config: MetricsConfig) {
    let mut cfg = METRICS_CONFIG.write().unwrap();
    *cfg = config;
}

/// Get the current metrics configuration
pub fn get_config() -> MetricsConfig {
    METRICS_CONFIG.read().unwrap().clone()
}

/// Get the global metrics registry
pub fn registry() -> &'static Registry {
    &REGISTRY
}

// ============== Cluster Metrics API ==============

/// Set the number of nodes with a specific status
pub fn set_nodes_total(status: &str, count: f64) {
    NODES_TOTAL.with_label_values(&[status]).set(count);
}

/// Record a job with a specific status
pub fn inc_jobs_total(status: &str) {
    JOBS_TOTAL.with_label_values(&[status]).inc();
}

/// Record multiple jobs with a specific status
pub fn inc_jobs_total_by(status: &str, count: f64) {
    JOBS_TOTAL.with_label_values(&[status]).inc_by(count);
}

/// Record a task with a specific status
pub fn inc_tasks_total(status: &str) {
    TASKS_TOTAL.with_label_values(&[status]).inc();
}

/// Record multiple tasks with a specific status
pub fn inc_tasks_total_by(status: &str, count: f64) {
    TASKS_TOTAL.with_label_values(&[status]).inc_by(count);
}

/// Record task duration in seconds
pub fn observe_task_duration(duration_seconds: f64) {
    TASK_DURATION_SECONDS.observe(duration_seconds);
}

/// Set the current queue depth
pub fn set_queue_depth(depth: f64) {
    QUEUE_DEPTH.set(depth);
}

/// Get the current queue depth
pub fn get_queue_depth() -> f64 {
    QUEUE_DEPTH.get()
}

// ============== Node Metrics API ==============

/// Set CPU usage for a node
pub fn set_node_cpu_usage(node_id: &str, usage_percent: f64) {
    NODE_CPU_USAGE
        .with_label_values(&[node_id])
        .set(usage_percent);
}

/// Set memory usage in bytes for a node
pub fn set_node_memory_bytes(node_id: &str, bytes: f64) {
    NODE_MEMORY_BYTES.with_label_values(&[node_id]).set(bytes);
}

/// Set number of running tasks for a node
pub fn set_node_tasks_running(node_id: &str, count: f64) {
    NODE_TASKS_RUNNING.with_label_values(&[node_id]).set(count);
}

/// Set seconds since last heartbeat for a node
pub fn set_node_last_heartbeat(node_id: &str, seconds: f64) {
    NODE_LAST_HEARTBEAT_SECONDS
        .with_label_values(&[node_id])
        .set(seconds);
}

/// Remove all metrics for a node (when node is deregistered)
pub fn remove_node_metrics(node_id: &str) {
    let _ = NODE_CPU_USAGE.remove_label_values(&[node_id]);
    let _ = NODE_MEMORY_BYTES.remove_label_values(&[node_id]);
    let _ = NODE_TASKS_RUNNING.remove_label_values(&[node_id]);
    let _ = NODE_LAST_HEARTBEAT_SECONDS.remove_label_values(&[node_id]);
}

// ============== Resource Metrics API ==============

/// Set quota usage ratio for an account and resource type
pub fn set_quota_usage_ratio(account_id: &str, resource_type: &str, ratio: f64) {
    QUOTA_USAGE_RATIO
        .with_label_values(&[account_id, resource_type])
        .set(ratio);
}

/// Remove quota metrics for an account
pub fn remove_account_metrics(account_id: &str) {
    // Remove for common resource types
    let resource_types = [
        "cpu_hours",
        "gpu_hours",
        "memory_gb_hours",
        "concurrent_jobs",
        "concurrent_tasks",
        "storage_gb",
        "network_egress_gb",
    ];
    for rt in &resource_types {
        let _ = QUOTA_USAGE_RATIO.remove_label_values(&[account_id, rt]);
    }
}

/// Increment preemptions counter
pub fn inc_preemptions() {
    PREEMPTIONS_TOTAL.inc();
}

/// Increment preemptions counter by a specific amount
pub fn inc_preemptions_by(count: f64) {
    PREEMPTIONS_TOTAL.inc_by(count);
}

/// Get total preemptions count
pub fn get_preemptions_total() -> f64 {
    PREEMPTIONS_TOTAL.get()
}

/// Record scheduling latency in seconds
pub fn observe_scheduling_latency(duration_seconds: f64) {
    SCHEDULING_LATENCY_SECONDS.observe(duration_seconds);
}

// ============== Export API ==============

/// Encode all metrics in Prometheus text format
pub fn encode_metrics() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer).unwrap_or_default())
}

/// Encode metrics with custom prefix (creates a new registry)
pub fn encode_metrics_with_prefix(prefix: &str) -> Result<String, prometheus::Error> {
    // For custom prefix, we'd need to re-register metrics
    // For now, just use the global registry
    let _ = prefix;
    encode_metrics()
}

/// Metrics collector that aggregates data from various sources
pub struct MetricsCollector {
    /// Last collection timestamp
    last_collection: std::time::Instant,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            last_collection: std::time::Instant::now(),
        }
    }

    /// Update cluster metrics from cluster state
    pub fn update_cluster_metrics(&mut self, stats: &ClusterMetricsSnapshot) {
        // Update node counts by status
        set_nodes_total("ready", stats.nodes_ready as f64);
        set_nodes_total("busy", stats.nodes_busy as f64);
        set_nodes_total("draining", stats.nodes_draining as f64);
        set_nodes_total("offline", stats.nodes_offline as f64);
        set_nodes_total("starting", stats.nodes_starting as f64);

        // Update queue depth
        set_queue_depth(stats.queue_depth as f64);

        self.last_collection = std::time::Instant::now();
    }

    /// Update node metrics
    pub fn update_node_metrics(&mut self, node_id: &str, metrics: &NodeMetricsSnapshot) {
        set_node_cpu_usage(node_id, metrics.cpu_usage_percent);
        set_node_memory_bytes(node_id, metrics.memory_bytes as f64);
        set_node_tasks_running(node_id, metrics.tasks_running as f64);
        set_node_last_heartbeat(node_id, metrics.last_heartbeat_seconds);
    }

    /// Update quota metrics for an account
    pub fn update_quota_metrics(&mut self, account_id: &str, quotas: &[QuotaMetricsSnapshot]) {
        for quota in quotas {
            set_quota_usage_ratio(account_id, &quota.resource_type, quota.usage_ratio);
        }
    }

    /// Get time since last collection
    pub fn time_since_collection(&self) -> std::time::Duration {
        self.last_collection.elapsed()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of cluster-wide metrics
#[derive(Debug, Clone, Default)]
pub struct ClusterMetricsSnapshot {
    /// Number of nodes in ready state
    pub nodes_ready: usize,
    /// Number of nodes in busy state
    pub nodes_busy: usize,
    /// Number of nodes in draining state
    pub nodes_draining: usize,
    /// Number of nodes in offline state
    pub nodes_offline: usize,
    /// Number of nodes in starting state
    pub nodes_starting: usize,
    /// Current queue depth
    pub queue_depth: usize,
}

/// Snapshot of node-level metrics
#[derive(Debug, Clone, Default)]
pub struct NodeMetricsSnapshot {
    /// CPU usage percentage (0-100)
    pub cpu_usage_percent: f64,
    /// Memory usage in bytes
    pub memory_bytes: u64,
    /// Number of tasks running
    pub tasks_running: usize,
    /// Seconds since last heartbeat
    pub last_heartbeat_seconds: f64,
}

/// Snapshot of quota metrics for a resource
#[derive(Debug, Clone)]
pub struct QuotaMetricsSnapshot {
    /// Resource type (e.g., "cpu_hours", "gpu_hours")
    pub resource_type: String,
    /// Usage ratio (used / allocated)
    pub usage_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert_eq!(config.prefix, "marabunta");
        assert!(config.include_process_metrics);
    }

    #[test]
    fn test_metrics_config_with_prefix() {
        let config = MetricsConfig::with_prefix("custom");
        assert_eq!(config.prefix, "custom");
    }

    #[test]
    fn test_metric_name_formatting() {
        let config = MetricsConfig::with_prefix("myapp");
        assert_eq!(config.metric_name("nodes_total"), "myapp_nodes_total");
    }

    #[test]
    fn test_cluster_metrics() {
        // Test setting node counts
        set_nodes_total("ready", 10.0);
        set_nodes_total("busy", 5.0);
        set_nodes_total("offline", 2.0);

        // Test jobs counter
        inc_jobs_total("completed");
        inc_jobs_total("completed");
        inc_jobs_total("failed");

        // Test tasks counter
        inc_tasks_total("running");
        inc_tasks_total_by("completed", 5.0);

        // Test task duration
        observe_task_duration(1.5);
        observe_task_duration(2.0);

        // Test queue depth
        set_queue_depth(100.0);
        assert!((get_queue_depth() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_node_metrics() {
        let node_id = "node-test-1";

        set_node_cpu_usage(node_id, 75.5);
        set_node_memory_bytes(node_id, 8_000_000_000.0);
        set_node_tasks_running(node_id, 4.0);
        set_node_last_heartbeat(node_id, 5.0);

        // Remove metrics for cleanup
        remove_node_metrics(node_id);
    }

    #[test]
    fn test_resource_metrics() {
        let account_id = "account-test-1";

        set_quota_usage_ratio(account_id, "cpu_hours", 0.75);
        set_quota_usage_ratio(account_id, "gpu_hours", 0.50);

        inc_preemptions();
        inc_preemptions_by(3.0);
        assert!((get_preemptions_total() - 4.0).abs() < f64::EPSILON);

        observe_scheduling_latency(0.05);
        observe_scheduling_latency(0.10);

        // Cleanup
        remove_account_metrics(account_id);
    }

    #[test]
    fn test_metrics_encoding() {
        // Set some metrics
        set_nodes_total("ready", 5.0);
        set_queue_depth(50.0);

        // Encode and verify output format
        let encoded = encode_metrics().unwrap();
        assert!(!encoded.is_empty());
        // Should contain Prometheus format
        assert!(
            encoded.contains("# HELP") || encoded.contains("# TYPE") || encoded.contains("marabunta")
        );
    }

    #[test]
    fn test_metrics_collector() {
        let mut collector = MetricsCollector::new();

        // Update cluster metrics
        let cluster_snapshot = ClusterMetricsSnapshot {
            nodes_ready: 10,
            nodes_busy: 5,
            nodes_draining: 1,
            nodes_offline: 2,
            nodes_starting: 3,
            queue_depth: 100,
        };
        collector.update_cluster_metrics(&cluster_snapshot);

        // Update node metrics
        let node_snapshot = NodeMetricsSnapshot {
            cpu_usage_percent: 65.5,
            memory_bytes: 4_000_000_000,
            tasks_running: 3,
            last_heartbeat_seconds: 2.5,
        };
        collector.update_node_metrics("node-1", &node_snapshot);

        // Update quota metrics
        let quota_snapshots = vec![
            QuotaMetricsSnapshot {
                resource_type: "cpu_hours".to_string(),
                usage_ratio: 0.8,
            },
            QuotaMetricsSnapshot {
                resource_type: "gpu_hours".to_string(),
                usage_ratio: 0.5,
            },
        ];
        collector.update_quota_metrics("account-1", &quota_snapshots);

        // Verify time tracking
        assert!(collector.time_since_collection().as_millis() < 1000);
    }

    #[test]
    fn test_snapshot_types() {
        let cluster = ClusterMetricsSnapshot::default();
        assert_eq!(cluster.nodes_ready, 0);

        let node = NodeMetricsSnapshot::default();
        assert!((node.cpu_usage_percent - 0.0).abs() < f64::EPSILON);

        let quota = QuotaMetricsSnapshot {
            resource_type: "cpu_hours".to_string(),
            usage_ratio: 0.5,
        };
        assert_eq!(quota.resource_type, "cpu_hours");
    }
}
