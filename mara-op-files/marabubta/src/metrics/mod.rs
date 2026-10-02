// Marabunta - Licensed under the MIT License.
//! Metrics collection and export for Marabunta Compute
//!
//! This module provides comprehensive metrics support including:
//!
//! - **Prometheus export** - Standard Prometheus text format metrics
//! - **Histogram metrics** - Latency distributions with configurable buckets
//! - **Summary metrics** - Quantile calculations (p50, p90, p99) with sliding windows
//! - **Dimensional metrics** - Labels/tags for multi-dimensional data
//! - **Cardinality limits** - Protection against metric explosion
//! - **Pushgateway support** - Push metrics to Prometheus pushgateway
//! - **Business metrics** - Jobs/sec, task throughput, queue depth, SLA tracking
//! - **Pre-built dashboards** - Grafana JSON dashboards for common monitoring needs
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::metrics::{prometheus, BusinessMetrics, histogram::Labels};
//!
//! // Initialize business metrics
//! let metrics = BusinessMetrics::new();
//!
//! // Record job submission
//! metrics.jobs.record_submission();
//!
//! // Record task completion with duration
//! metrics.tasks.record_completion(5.5, "compute");
//!
//! // Use labeled histograms for latency tracking
//! use marabunta_compute::metrics::histogram::{LabeledHistogram, buckets};
//! let http_latency = LabeledHistogram::new(
//!     "http_request_duration_seconds",
//!     "HTTP request latency",
//!     buckets::API_LATENCY,
//! );
//! http_latency.observe(Labels::new(&[("method", "GET"), ("path", "/api/v1")]), 0.05).unwrap();
//!
//! // Export in Prometheus format
//! let metrics_text = prometheus::encode_metrics().unwrap();
//! ```
//!
//! # Metrics Reference
//!
//! ## Cluster Metrics
//!
//! | Metric | Type | Labels | Description |
//! |--------|------|--------|-------------|
//! | `marabunta_nodes_total` | Gauge | status | Total nodes by status |
//! | `marabunta_jobs_total` | Counter | status | Total jobs by status |
//! | `marabunta_tasks_total` | Counter | status | Total tasks by status |
//! | `marabunta_task_duration_seconds` | Histogram | - | Task execution duration |
//! | `marabunta_queue_depth` | Gauge | - | Current queue depth |
//!
//! ## Node Metrics
//!
//! | Metric | Type | Labels | Description |
//! |--------|------|--------|-------------|
//! | `marabunta_node_cpu_usage` | Gauge | node_id | CPU usage (0-100) |
//! | `marabunta_node_memory_bytes` | Gauge | node_id | Memory used in bytes |
//! | `marabunta_node_tasks_running` | Gauge | node_id | Running tasks per node |
//! | `marabunta_node_last_heartbeat_seconds` | Gauge | node_id | Seconds since heartbeat |
//!
//! ## Business Metrics
//!
//! | Metric | Type | Labels | Description |
//! |--------|------|--------|-------------|
//! | `marabunta_jobs_per_second` | Gauge | - | Current job submission rate |
//! | `marabunta_tasks_per_second` | Gauge | - | Current task completion rate |
//! | `marabunta_sla_compliance_percent` | Gauge | - | SLA compliance percentage |
//! | `marabunta_queue_depth_total` | Gauge | - | Total queue depth |
//!
//! ## Cardinality Management
//!
//! The cardinality module provides protection against metric explosion:
//!
//! ```rust,ignore
//! use marabunta_compute::metrics::cardinality::{CardinalityLimiter, CardinalityConfig, EvictionStrategy};
//!
//! let config = CardinalityConfig::with_limit(10_000)
//!     .eviction_strategy(EvictionStrategy::LeastRecentlyUsed);
//! let limiter = CardinalityLimiter::with_config(config);
//!
//! // Track label combinations with automatic eviction
//! limiter.track("my_metric", &labels)?;
//! ```

pub mod business;
pub mod cardinality;
pub mod dashboards;
pub mod histogram;
pub mod labels;
pub mod prometheus;
pub mod pushgateway;
pub mod summary;

// Re-export commonly used items from prometheus module
pub use prometheus::{
    encode_metrics, get_config, get_preemptions_total, get_queue_depth, inc_jobs_total,
    inc_jobs_total_by, inc_preemptions, inc_preemptions_by, inc_tasks_total, inc_tasks_total_by,
    init_metrics, observe_scheduling_latency, observe_task_duration, registry,
    remove_account_metrics, remove_node_metrics, set_node_cpu_usage, set_node_last_heartbeat,
    set_node_memory_bytes, set_node_tasks_running, set_nodes_total, set_queue_depth,
    set_quota_usage_ratio, ClusterMetricsSnapshot, MetricsCollector, MetricsConfig,
    NodeMetricsSnapshot, QuotaMetricsSnapshot, DEFAULT_METRIC_PREFIX,
};

// Re-export business metrics
pub use business::BusinessMetrics;

// Re-export histogram types
pub use histogram::{
    buckets, CardinalityError, HistogramCore, HistogramStats, LabeledHistogram, Labels,
};

// Re-export summary types
pub use summary::{LabeledSummary, SummaryConfig, SummaryCore, SummaryStats, TDigest};

// Re-export cardinality types
pub use cardinality::{
    CardinalityConfig, CardinalityGuard, CardinalityLimitError, CardinalityLimiter,
    CardinalityStats, CardinalityWarning, EvictionStrategy, MetricCardinalityStats,
};

// Re-export labels types
pub use labels::{
    common_labels, LabelBuilder, LabelPool, LabelPoolStats, LabelSchema, LabelSpec,
    LabelValidationError, MetricLabels,
};

// Re-export pushgateway types
pub use pushgateway::{
    ClosureMetricSource, MetricSource, PushResult, PushService, PushStats, PushgatewayClient,
    PushgatewayConfig, PushgatewayError, RetryConfig,
};

/// Integration helpers for connecting metrics to cluster state
pub mod integration {
    use super::*;
    use chrono::{DateTime, Utc};

    /// Update cluster metrics from cluster state statistics
    ///
    /// This function provides a convenient way to update all cluster-level
    /// metrics from a ClusterStats struct (from src/coordinator/state.rs).
    pub fn update_from_cluster_stats(
        total_masters: usize,
        healthy_masters: usize,
        pending_jobs: usize,
        running_jobs: usize,
        completed_jobs: usize,
        failed_jobs: usize,
    ) {
        // Masters are counted as "ready" nodes at coordinator level
        set_nodes_total("ready", healthy_masters as f64);
        set_nodes_total("offline", (total_masters - healthy_masters) as f64);

        // Update job counters (note: these are counters, so we'd need to track deltas)
        // For gauges showing current state, we'd need different metrics
        // This is more for showing the pattern of integration
        let _ = (pending_jobs, running_jobs, completed_jobs, failed_jobs);
    }

    /// Update node metrics from worker info
    ///
    /// Updates all node-level metrics for a specific worker/node.
    pub fn update_node_from_worker_info(
        node_id: &str,
        cpu_usage: f64,
        memory_bytes: u64,
        running_tasks: usize,
        last_heartbeat: DateTime<Utc>,
    ) {
        set_node_cpu_usage(node_id, cpu_usage);
        set_node_memory_bytes(node_id, memory_bytes as f64);
        set_node_tasks_running(node_id, running_tasks as f64);

        // Calculate seconds since heartbeat
        let now = Utc::now();
        let seconds_since = (now - last_heartbeat).num_seconds().max(0) as f64;
        set_node_last_heartbeat(node_id, seconds_since);
    }

    /// Update quota metrics from quota manager state
    ///
    /// Updates quota usage ratios for an account across all resource types.
    pub fn update_quota_from_account(
        account_id: &str,
        usage_by_resource: &[(String, f64, f64)], // (resource_type, used, allocated)
    ) {
        for (resource_type, used, allocated) in usage_by_resource {
            let ratio = if *allocated > 0.0 {
                used / allocated
            } else {
                0.0
            };
            set_quota_usage_ratio(account_id, resource_type, ratio);
        }
    }

    /// Record a completed task with its duration
    ///
    /// Convenience function that increments the completed task counter
    /// and records the task duration.
    pub fn record_task_completion(duration_seconds: f64) {
        inc_tasks_total("completed");
        observe_task_duration(duration_seconds);
    }

    /// Record a failed task
    pub fn record_task_failure() {
        inc_tasks_total("failed");
    }

    /// Record a preemption event
    pub fn record_preemption() {
        inc_preemptions();
    }

    /// Record scheduling of a task with its latency
    pub fn record_task_scheduled(latency_seconds: f64) {
        inc_tasks_total("assigned");
        observe_scheduling_latency(latency_seconds);
    }

    /// Record a job state transition
    pub fn record_job_transition(new_status: &str) {
        inc_jobs_total(new_status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify all re-exports are accessible
        let _ = DEFAULT_METRIC_PREFIX;
        let _ = MetricsConfig::default();
        let _ = MetricsCollector::new();
        let _ = ClusterMetricsSnapshot::default();
        let _ = NodeMetricsSnapshot::default();
    }

    #[test]
    fn test_integration_helpers() {
        use integration::*;

        // Test cluster stats update
        update_from_cluster_stats(10, 8, 5, 3, 100, 2);

        // Test node update
        let now = chrono::Utc::now();
        update_node_from_worker_info("test-node", 50.0, 4_000_000_000, 3, now);

        // Test quota update
        let usage = vec![
            ("cpu_hours".to_string(), 50.0, 100.0),
            ("gpu_hours".to_string(), 25.0, 100.0),
        ];
        update_quota_from_account("test-account", &usage);

        // Test convenience functions
        record_task_completion(10.5);
        record_task_failure();
        record_preemption();
        record_task_scheduled(0.05);
        record_job_transition("completed");

        // Cleanup
        remove_node_metrics("test-node");
        remove_account_metrics("test-account");
    }

    #[test]
    fn test_metrics_encoding_integration() {
        // Set some metrics
        set_nodes_total("ready", 5.0);
        set_queue_depth(25.0);

        // Encode
        let result = encode_metrics();
        assert!(result.is_ok());

        let metrics_text = result.unwrap();
        assert!(!metrics_text.is_empty());
    }

    #[test]
    fn test_histogram_module() {
        let histogram = LabeledHistogram::new(
            "test_latency",
            "Test latency metric",
            buckets::API_LATENCY,
        );

        let labels = Labels::new(&[("method", "GET")]);
        assert!(histogram.observe(labels.clone(), 0.05).is_ok());
        assert!(histogram.observe(labels.clone(), 0.1).is_ok());

        let stats = histogram.stats(&labels).unwrap();
        assert_eq!(stats.count, 2);
    }

    #[test]
    fn test_summary_module() {
        let summary = LabeledSummary::new("test_summary", "Test summary metric");

        let labels = Labels::new(&[("endpoint", "/api")]);
        for i in 1..=100 {
            summary.observe(labels.clone(), i as f64).unwrap();
        }

        let stats = summary.stats(&labels).unwrap();
        assert_eq!(stats.total_count, 100);
    }

    #[test]
    fn test_cardinality_limiter() {
        let limiter = CardinalityLimiter::new();

        let labels = MetricLabels::new(&[("id", "test")]);
        assert!(limiter.track("test_metric", &labels).is_ok());
        assert_eq!(limiter.get_cardinality("test_metric"), 1);

        assert!(limiter.untrack("test_metric", &labels));
        assert_eq!(limiter.get_cardinality("test_metric"), 0);
    }

    #[test]
    fn test_business_metrics() {
        let metrics = BusinessMetrics::new();

        metrics.jobs.record_submission();
        metrics.tasks.record_start();
        metrics.queues.set_depth("default", 10);
        metrics.workers.set_total(5);

        assert_eq!(metrics.jobs.total_submitted(), 1);
        assert_eq!(metrics.tasks.active(), 1);
        assert_eq!(metrics.queues.depth("default"), 10);
        assert_eq!(metrics.workers.total(), 5);
    }

    #[test]
    fn test_label_builder() {
        let labels = LabelBuilder::new()
            .label("method", "POST")
            .label("status", "200")
            .build();

        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("method"), Some("POST"));
    }

    #[test]
    fn test_pushgateway_config() {
        let config = PushgatewayConfig::new("http://localhost:9091", "marabunta_compute")
            .instance("server-1")
            .grouping("env", "production");

        let url = config.push_url();
        assert!(url.contains("marabunta_compute"));
        assert!(url.contains("server-1"));
        assert!(url.contains("production"));
    }

    #[test]
    fn test_dashboards_available() {
        let dashboards = dashboards::all_dashboards();
        assert_eq!(dashboards.len(), 3);

        // Verify dashboard JSON is valid
        for (name, json) in dashboards {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(json);
            assert!(parsed.is_ok(), "Dashboard {} is invalid JSON", name);
        }
    }
}
