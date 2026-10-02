// Marabunta - Licensed under the MIT License.
//! Metrics collection for connection pools
//!
//! This module provides Prometheus-compatible metrics for monitoring pool
//! health and performance.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lazy_static::lazy_static;
use prometheus::{
    self, CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec,
    Opts, Registry,
};

lazy_static! {
    /// Global pool metrics registry
    static ref POOL_REGISTRY: Registry = Registry::new();

    /// Total number of connections in the pool
    static ref POOL_SIZE: GaugeVec = {
        let opts = Opts::new("marabunta_pool_size", "Total number of connections in the pool")
            .namespace("marabunta");
        let gauge = GaugeVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Number of idle connections
    static ref POOL_IDLE: GaugeVec = {
        let opts = Opts::new("marabunta_pool_idle", "Number of idle connections in the pool")
            .namespace("marabunta");
        let gauge = GaugeVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Number of connections in use
    static ref POOL_IN_USE: GaugeVec = {
        let opts = Opts::new("marabunta_pool_in_use", "Number of connections currently in use")
            .namespace("marabunta");
        let gauge = GaugeVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(gauge.clone())).unwrap();
        gauge
    };

    /// Wait time to acquire a connection
    static ref POOL_WAIT_TIME: HistogramVec = {
        let opts = HistogramOpts::new(
            "marabunta_pool_wait_time_seconds",
            "Time spent waiting to acquire a connection"
        )
        .namespace("marabunta")
        .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]);
        let histogram = HistogramVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(histogram.clone())).unwrap();
        histogram
    };

    /// Total connection creation errors
    static ref POOL_CONNECTION_ERRORS: CounterVec = {
        let opts = Opts::new(
            "marabunta_pool_connection_errors_total",
            "Total number of connection creation errors"
        )
        .namespace("marabunta");
        let counter = CounterVec::new(opts, &["pool_name", "error_type"]).unwrap();
        POOL_REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Total health check failures
    static ref POOL_HEALTH_CHECK_FAILURES: CounterVec = {
        let opts = Opts::new(
            "marabunta_pool_health_check_failures_total",
            "Total number of health check failures"
        )
        .namespace("marabunta");
        let counter = CounterVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Connections acquired total
    static ref POOL_CONNECTIONS_ACQUIRED: CounterVec = {
        let opts = Opts::new(
            "marabunta_pool_connections_acquired_total",
            "Total number of connections acquired from the pool"
        )
        .namespace("marabunta");
        let counter = CounterVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Connections released total
    static ref POOL_CONNECTIONS_RELEASED: CounterVec = {
        let opts = Opts::new(
            "marabunta_pool_connections_released_total",
            "Total number of connections released back to the pool"
        )
        .namespace("marabunta");
        let counter = CounterVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Connections created total
    static ref POOL_CONNECTIONS_CREATED: CounterVec = {
        let opts = Opts::new(
            "marabunta_pool_connections_created_total",
            "Total number of connections created"
        )
        .namespace("marabunta");
        let counter = CounterVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Connections closed total
    static ref POOL_CONNECTIONS_CLOSED: CounterVec = {
        let opts = Opts::new(
            "marabunta_pool_connections_closed_total",
            "Total number of connections closed"
        )
        .namespace("marabunta");
        let counter = CounterVec::new(opts, &["pool_name", "reason"]).unwrap();
        POOL_REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };

    /// Timeout errors
    static ref POOL_TIMEOUTS: CounterVec = {
        let opts = Opts::new(
            "marabunta_pool_timeouts_total",
            "Total number of timeout errors when acquiring connections"
        )
        .namespace("marabunta");
        let counter = CounterVec::new(opts, &["pool_name"]).unwrap();
        POOL_REGISTRY.register(Box::new(counter.clone())).unwrap();
        counter
    };
}

/// Metrics collector for a specific pool
#[derive(Debug)]
pub struct PoolMetrics {
    /// Name of the pool
    pool_name: String,
    /// Whether metrics collection is enabled
    enabled: bool,
    /// Internal counters for snapshot generation
    total_wait_time_ns: AtomicU64,
    wait_count: AtomicU64,
}

impl PoolMetrics {
    /// Create a new metrics collector for a pool
    pub fn new(pool_name: impl Into<String>, enabled: bool) -> Self {
        Self {
            pool_name: pool_name.into(),
            enabled,
            total_wait_time_ns: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
        }
    }

    /// Get the pool name
    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    /// Check if metrics collection is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Record the current pool size
    pub fn set_pool_size(&self, total: usize, idle: usize, in_use: usize) {
        if !self.enabled {
            return;
        }
        POOL_SIZE
            .with_label_values(&[&self.pool_name])
            .set(total as f64);
        POOL_IDLE
            .with_label_values(&[&self.pool_name])
            .set(idle as f64);
        POOL_IN_USE
            .with_label_values(&[&self.pool_name])
            .set(in_use as f64);
    }

    /// Record wait time for acquiring a connection
    pub fn record_wait_time(&self, duration: Duration) {
        if !self.enabled {
            return;
        }
        let seconds = duration.as_secs_f64();
        POOL_WAIT_TIME
            .with_label_values(&[&self.pool_name])
            .observe(seconds);

        // Also track for snapshot
        self.total_wait_time_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection error
    pub fn record_connection_error(&self, error_type: &str) {
        if !self.enabled {
            return;
        }
        POOL_CONNECTION_ERRORS
            .with_label_values(&[&self.pool_name, error_type])
            .inc();
    }

    /// Record a health check failure
    pub fn record_health_check_failure(&self) {
        if !self.enabled {
            return;
        }
        POOL_HEALTH_CHECK_FAILURES
            .with_label_values(&[&self.pool_name])
            .inc();
    }

    /// Record a connection acquisition
    pub fn record_connection_acquired(&self) {
        if !self.enabled {
            return;
        }
        POOL_CONNECTIONS_ACQUIRED
            .with_label_values(&[&self.pool_name])
            .inc();
    }

    /// Record a connection release
    pub fn record_connection_released(&self) {
        if !self.enabled {
            return;
        }
        POOL_CONNECTIONS_RELEASED
            .with_label_values(&[&self.pool_name])
            .inc();
    }

    /// Record a connection creation
    pub fn record_connection_created(&self) {
        if !self.enabled {
            return;
        }
        POOL_CONNECTIONS_CREATED
            .with_label_values(&[&self.pool_name])
            .inc();
    }

    /// Record a connection closure
    pub fn record_connection_closed(&self, reason: &str) {
        if !self.enabled {
            return;
        }
        POOL_CONNECTIONS_CLOSED
            .with_label_values(&[&self.pool_name, reason])
            .inc();
    }

    /// Record a timeout error
    pub fn record_timeout(&self) {
        if !self.enabled {
            return;
        }
        POOL_TIMEOUTS.with_label_values(&[&self.pool_name]).inc();
    }

    /// Get the average wait time
    pub fn average_wait_time(&self) -> Duration {
        let count = self.wait_count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        let total_ns = self.total_wait_time_ns.load(Ordering::Relaxed);
        Duration::from_nanos(total_ns / count)
    }

    /// Remove all metrics for this pool
    pub fn remove(&self) {
        if !self.enabled {
            return;
        }
        let _ = POOL_SIZE.remove_label_values(&[&self.pool_name]);
        let _ = POOL_IDLE.remove_label_values(&[&self.pool_name]);
        let _ = POOL_IN_USE.remove_label_values(&[&self.pool_name]);
        let _ = POOL_WAIT_TIME.remove_label_values(&[&self.pool_name]);
        let _ = POOL_HEALTH_CHECK_FAILURES.remove_label_values(&[&self.pool_name]);
        let _ = POOL_CONNECTIONS_ACQUIRED.remove_label_values(&[&self.pool_name]);
        let _ = POOL_CONNECTIONS_RELEASED.remove_label_values(&[&self.pool_name]);
        let _ = POOL_CONNECTIONS_CREATED.remove_label_values(&[&self.pool_name]);
        let _ = POOL_TIMEOUTS.remove_label_values(&[&self.pool_name]);
        // Note: We don't remove error and closed metrics as they have multiple label values
    }
}

impl Drop for PoolMetrics {
    fn drop(&mut self) {
        self.remove();
    }
}

/// Snapshot of pool metrics at a point in time
#[derive(Debug, Clone, Default)]
pub struct PoolMetricsSnapshot {
    /// Total number of connections (idle + in_use)
    pub total_connections: usize,
    /// Number of idle connections
    pub idle_connections: usize,
    /// Number of connections currently in use
    pub connections_in_use: usize,
    /// Total connections acquired
    pub total_acquired: u64,
    /// Total connections released
    pub total_released: u64,
    /// Total connections created
    pub total_created: u64,
    /// Total connection errors
    pub total_errors: u64,
    /// Total health check failures
    pub total_health_failures: u64,
    /// Total timeouts
    pub total_timeouts: u64,
    /// Average wait time in milliseconds
    pub avg_wait_time_ms: f64,
}

impl PoolMetricsSnapshot {
    /// Create a new metrics snapshot
    pub fn new(
        total: usize,
        idle: usize,
        in_use: usize,
        acquired: u64,
        released: u64,
        created: u64,
        errors: u64,
        health_failures: u64,
        timeouts: u64,
        avg_wait: Duration,
    ) -> Self {
        Self {
            total_connections: total,
            idle_connections: idle,
            connections_in_use: in_use,
            total_acquired: acquired,
            total_released: released,
            total_created: created,
            total_errors: errors,
            total_health_failures: health_failures,
            total_timeouts: timeouts,
            avg_wait_time_ms: avg_wait.as_secs_f64() * 1000.0,
        }
    }

    /// Get the utilization ratio (in_use / total)
    pub fn utilization(&self) -> f64 {
        if self.total_connections == 0 {
            0.0
        } else {
            self.connections_in_use as f64 / self.total_connections as f64
        }
    }

    /// Check if the pool is under pressure (high utilization)
    pub fn is_under_pressure(&self) -> bool {
        self.utilization() > 0.8
    }

    /// Check if the pool has capacity available
    pub fn has_capacity(&self) -> bool {
        self.idle_connections > 0
    }
}

/// Get the pool metrics registry
pub fn registry() -> &'static Registry {
    &POOL_REGISTRY
}

/// Encode all pool metrics in Prometheus text format
pub fn encode_pool_metrics() -> Result<String, prometheus::Error> {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = POOL_REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_metrics_creation() {
        let metrics = PoolMetrics::new("test_pool", true);
        assert_eq!(metrics.pool_name(), "test_pool");
        assert!(metrics.is_enabled());
    }

    #[test]
    fn test_pool_metrics_disabled() {
        let metrics = PoolMetrics::new("disabled_pool", false);
        assert!(!metrics.is_enabled());

        // These should be no-ops
        metrics.set_pool_size(10, 5, 5);
        metrics.record_wait_time(Duration::from_secs(1));
        metrics.record_connection_error("test");
    }

    #[test]
    fn test_pool_metrics_recording() {
        let metrics = PoolMetrics::new("recording_pool", true);

        metrics.set_pool_size(10, 5, 5);
        metrics.record_connection_acquired();
        metrics.record_connection_released();
        metrics.record_connection_created();
        metrics.record_connection_closed("idle_timeout");
        metrics.record_connection_error("connection_refused");
        metrics.record_health_check_failure();
        metrics.record_timeout();
        metrics.record_wait_time(Duration::from_millis(100));
    }

    #[test]
    fn test_pool_metrics_average_wait_time() {
        let metrics = PoolMetrics::new("wait_time_pool", true);

        // No waits yet
        assert_eq!(metrics.average_wait_time(), Duration::ZERO);

        // Record some wait times
        metrics.record_wait_time(Duration::from_millis(100));
        metrics.record_wait_time(Duration::from_millis(200));
        metrics.record_wait_time(Duration::from_millis(300));

        let avg = metrics.average_wait_time();
        // Should be approximately 200ms
        assert!(avg > Duration::from_millis(150));
        assert!(avg < Duration::from_millis(250));
    }

    #[test]
    fn test_pool_metrics_snapshot() {
        let snapshot = PoolMetricsSnapshot::new(
            10,
            5,
            5,
            100,
            95,
            50,
            3,
            2,
            1,
            Duration::from_millis(50),
        );

        assert_eq!(snapshot.total_connections, 10);
        assert_eq!(snapshot.idle_connections, 5);
        assert_eq!(snapshot.connections_in_use, 5);
        assert_eq!(snapshot.utilization(), 0.5);
        assert!(!snapshot.is_under_pressure());
        assert!(snapshot.has_capacity());
    }

    #[test]
    fn test_pool_metrics_snapshot_high_utilization() {
        let snapshot = PoolMetricsSnapshot::new(
            10,
            1,
            9,
            100,
            95,
            50,
            3,
            2,
            1,
            Duration::from_millis(50),
        );

        assert_eq!(snapshot.utilization(), 0.9);
        assert!(snapshot.is_under_pressure());
        assert!(snapshot.has_capacity());
    }

    #[test]
    fn test_pool_metrics_snapshot_no_capacity() {
        let snapshot = PoolMetricsSnapshot::new(
            10,
            0,
            10,
            100,
            95,
            50,
            3,
            2,
            1,
            Duration::from_millis(50),
        );

        assert!(!snapshot.has_capacity());
        assert!(snapshot.is_under_pressure());
    }

    #[test]
    fn test_pool_metrics_snapshot_empty() {
        let snapshot = PoolMetricsSnapshot::default();
        assert_eq!(snapshot.utilization(), 0.0);
        assert!(!snapshot.is_under_pressure());
        assert!(!snapshot.has_capacity());
    }

    #[test]
    fn test_encode_pool_metrics() {
        let result = encode_pool_metrics();
        assert!(result.is_ok());
    }
}
