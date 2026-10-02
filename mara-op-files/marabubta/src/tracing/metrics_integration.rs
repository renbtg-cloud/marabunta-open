// Marabunta - Licensed under the MIT License.
//! Metrics integration for distributed tracing
//!
//! This module integrates the tracing system with the existing Prometheus metrics,
//! providing span duration histograms, active span counters, and error tracking.

use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec_with_registry, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, CounterVec, GaugeVec, HistogramVec, Registry,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use super::span::{Span, SpanKind, SpanStatus};

/// Default metric prefix for tracing metrics
pub const TRACING_METRIC_PREFIX: &str = "marabunta_trace";

/// Configuration for tracing metrics
#[derive(Debug, Clone)]
pub struct TracingMetricsConfig {
    /// Prefix for all metric names
    pub prefix: String,
    /// Buckets for span duration histogram (in seconds)
    pub duration_buckets: Vec<f64>,
}

impl Default for TracingMetricsConfig {
    fn default() -> Self {
        Self {
            prefix: TRACING_METRIC_PREFIX.to_string(),
            // Default buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s
            duration_buckets: vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        }
    }
}

lazy_static! {
    /// Global tracing metrics registry
    static ref TRACING_REGISTRY: Registry = Registry::new();

    /// Global tracing metrics configuration
    static ref TRACING_CONFIG: RwLock<TracingMetricsConfig> = RwLock::new(TracingMetricsConfig::default());

    /// Span duration histogram
    /// Labels: operation (span name), kind (server, client, internal, etc.), status (ok, error, unset)
    static ref SPAN_DURATION_SECONDS: HistogramVec = {
        let config = TRACING_CONFIG.read().unwrap();
        let histogram = register_histogram_vec_with_registry!(
            format!("{}_span_duration_seconds", config.prefix),
            "Span duration in seconds",
            &["operation", "kind", "status"],
            config.duration_buckets.clone(),
            TRACING_REGISTRY.clone()
        ).unwrap();
        histogram
    };

    /// Active spans gauge
    /// Labels: operation, kind
    static ref ACTIVE_SPANS: GaugeVec = {
        let config = TRACING_CONFIG.read().unwrap();
        register_gauge_vec_with_registry!(
            format!("{}_active_spans", config.prefix),
            "Number of currently active spans",
            &["operation", "kind"],
            TRACING_REGISTRY.clone()
        ).unwrap()
    };

    /// Total spans started counter
    /// Labels: operation, kind
    static ref SPANS_STARTED_TOTAL: CounterVec = {
        let config = TRACING_CONFIG.read().unwrap();
        register_counter_vec_with_registry!(
            format!("{}_spans_started_total", config.prefix),
            "Total number of spans started",
            &["operation", "kind"],
            TRACING_REGISTRY.clone()
        ).unwrap()
    };

    /// Total spans completed counter
    /// Labels: operation, kind, status
    static ref SPANS_COMPLETED_TOTAL: CounterVec = {
        let config = TRACING_CONFIG.read().unwrap();
        register_counter_vec_with_registry!(
            format!("{}_spans_completed_total", config.prefix),
            "Total number of spans completed",
            &["operation", "kind", "status"],
            TRACING_REGISTRY.clone()
        ).unwrap()
    };

    /// Span errors counter
    /// Labels: operation, kind, error_type
    static ref SPAN_ERRORS_TOTAL: CounterVec = {
        let config = TRACING_CONFIG.read().unwrap();
        register_counter_vec_with_registry!(
            format!("{}_span_errors_total", config.prefix),
            "Total number of span errors",
            &["operation", "kind", "error_type"],
            TRACING_REGISTRY.clone()
        ).unwrap()
    };
}

/// Global counters for quick stats
static TOTAL_TRACES: AtomicU64 = AtomicU64::new(0);
static TOTAL_SPANS: AtomicU64 = AtomicU64::new(0);

/// Initialize tracing metrics with custom configuration
pub fn init_tracing_metrics(config: TracingMetricsConfig) {
    let mut cfg = TRACING_CONFIG.write().unwrap();
    *cfg = config;
}

/// Get the tracing metrics registry
pub fn tracing_registry() -> &'static Registry {
    &TRACING_REGISTRY
}

/// Record that a span has started
pub fn record_span_started(operation: &str, kind: SpanKind) {
    let kind_str = kind_to_string(kind);

    SPANS_STARTED_TOTAL
        .with_label_values(&[operation, kind_str])
        .inc();

    ACTIVE_SPANS.with_label_values(&[operation, kind_str]).inc();

    TOTAL_SPANS.fetch_add(1, Ordering::Relaxed);
}

/// Record that a span has completed
pub fn record_span_completed(span: &Span) {
    let operation = span.name();
    let kind_str = kind_to_string(span.kind());
    let status_str = status_to_string(&span.status());

    // Decrement active spans
    ACTIVE_SPANS.with_label_values(&[operation, kind_str]).dec();

    // Record completion
    SPANS_COMPLETED_TOTAL
        .with_label_values(&[operation, kind_str, status_str])
        .inc();

    // Record duration
    if let Some(duration) = span.duration() {
        SPAN_DURATION_SECONDS
            .with_label_values(&[operation, kind_str, status_str])
            .observe(duration.as_secs_f64());
    }

    // Record error if applicable
    if let SpanStatus::Error(ref msg) = span.status() {
        let error_type = extract_error_type(msg);
        SPAN_ERRORS_TOTAL
            .with_label_values(&[operation, kind_str, error_type])
            .inc();
    }
}

/// Record a span error
pub fn record_span_error(operation: &str, kind: SpanKind, error_type: &str) {
    let kind_str = kind_to_string(kind);
    SPAN_ERRORS_TOTAL
        .with_label_values(&[operation, kind_str, error_type])
        .inc();
}

/// Convert SpanKind to string label
fn kind_to_string(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "internal",
        SpanKind::Server => "server",
        SpanKind::Client => "client",
        SpanKind::Producer => "producer",
        SpanKind::Consumer => "consumer",
    }
}

/// Convert SpanStatus to string label
fn status_to_string(status: &SpanStatus) -> &'static str {
    match status {
        SpanStatus::Unset => "unset",
        SpanStatus::Ok => "ok",
        SpanStatus::Error(_) => "error",
    }
}

/// Extract error type from error message (first word or "unknown")
fn extract_error_type(message: &str) -> &str {
    message
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .trim_end_matches(':')
}

/// Get total number of traces created
pub fn get_total_traces() -> u64 {
    TOTAL_TRACES.load(Ordering::Relaxed)
}

/// Get total number of spans created
pub fn get_total_spans() -> u64 {
    TOTAL_SPANS.load(Ordering::Relaxed)
}

/// Increment total traces counter (call when creating a new root context)
pub fn increment_traces() {
    TOTAL_TRACES.fetch_add(1, Ordering::Relaxed);
}

/// Tracing metrics helper struct for convenient access
#[derive(Debug, Clone)]
pub struct TracingMetrics {
    service_name: String,
}

impl TracingMetrics {
    /// Create a new tracing metrics helper
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }

    /// Record span start
    pub fn on_span_start(&self, span: &Span) {
        record_span_started(span.name(), span.kind());
    }

    /// Record span end
    pub fn on_span_end(&self, span: &Span) {
        record_span_completed(span);
    }

    /// Get current stats
    pub fn stats(&self) -> TracingStats {
        TracingStats {
            service_name: self.service_name.clone(),
            total_traces: get_total_traces(),
            total_spans: get_total_spans(),
        }
    }
}

impl Default for TracingMetrics {
    fn default() -> Self {
        Self::new("marabunta-compute")
    }
}

/// Statistics for tracing
#[derive(Debug, Clone)]
pub struct TracingStats {
    /// Service name
    pub service_name: String,
    /// Total traces created
    pub total_traces: u64,
    /// Total spans created
    pub total_spans: u64,
}

/// Instrumented span that automatically records metrics
pub struct InstrumentedSpan {
    span: Span,
    metrics: TracingMetrics,
}

impl InstrumentedSpan {
    /// Create a new instrumented span
    pub fn new(span: Span, metrics: TracingMetrics) -> Self {
        metrics.on_span_start(&span);
        Self { span, metrics }
    }

    /// Get access to the underlying span
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Get mutable access to the underlying span
    pub fn span_mut(&mut self) -> &mut Span {
        &mut self.span
    }

    /// End the span and record metrics
    pub fn end(mut self) {
        self.span.end();
        self.metrics.on_span_end(&self.span);
    }
}

impl Drop for InstrumentedSpan {
    fn drop(&mut self) {
        if !self.span.is_ended() {
            self.span.end();
            self.metrics.on_span_end(&self.span);
        }
    }
}

/// Macro for creating an instrumented span
///
/// # Example
///
/// ```rust,ignore
/// use marabunta_compute::tracing::{instrumented_span, SpanKind, TracingMetrics};
///
/// let metrics = TracingMetrics::new("my-service");
///
/// let span = instrumented_span!("my_operation", SpanKind::Internal, &metrics);
/// // do work...
/// span.end();
/// ```
#[macro_export]
macro_rules! instrumented_span {
    ($name:expr, $kind:expr, $metrics:expr) => {{
        let span = $crate::tracing::Span::new($name, $kind);
        $crate::tracing::metrics_integration::InstrumentedSpan::new(span, $metrics.clone())
    }};
    ($name:expr, $kind:expr, $ctx:expr, $metrics:expr) => {{
        let span = $crate::tracing::Span::new_with_context($name, $kind, $ctx);
        $crate::tracing::metrics_integration::InstrumentedSpan::new(span, $metrics.clone())
    }};
}

/// Macro for instrumenting an async block with tracing and metrics
///
/// # Example
///
/// ```rust,ignore
/// use marabunta_compute::tracing::{instrument_async, SpanKind, TracingMetrics};
///
/// let metrics = TracingMetrics::new("my-service");
///
/// let result = instrument_async!("my_operation", SpanKind::Internal, &metrics, async {
///     // do async work...
///     42
/// }).await;
/// ```
#[macro_export]
macro_rules! instrument_async {
    ($name:expr, $kind:expr, $metrics:expr, $fut:expr) => {{
        let _span = $crate::instrumented_span!($name, $kind, $metrics);
        $fut
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracing::{Span, SpanKind, TraceContext};

    #[test]
    fn test_tracing_metrics_config() {
        let config = TracingMetricsConfig::default();
        assert_eq!(config.prefix, TRACING_METRIC_PREFIX);
        assert!(!config.duration_buckets.is_empty());
    }

    #[test]
    fn test_record_span_started() {
        record_span_started("test_operation", SpanKind::Internal);
        // Should not panic
    }

    #[test]
    fn test_record_span_completed() {
        let mut span = Span::new("test_op", SpanKind::Server);
        span.end();
        record_span_completed(&span);
        // Should not panic
    }

    #[test]
    fn test_record_span_error() {
        record_span_error("test_op", SpanKind::Client, "timeout");
        // Should not panic
    }

    #[test]
    fn test_kind_to_string() {
        assert_eq!(kind_to_string(SpanKind::Internal), "internal");
        assert_eq!(kind_to_string(SpanKind::Server), "server");
        assert_eq!(kind_to_string(SpanKind::Client), "client");
        assert_eq!(kind_to_string(SpanKind::Producer), "producer");
        assert_eq!(kind_to_string(SpanKind::Consumer), "consumer");
    }

    #[test]
    fn test_status_to_string() {
        assert_eq!(status_to_string(&SpanStatus::Unset), "unset");
        assert_eq!(status_to_string(&SpanStatus::Ok), "ok");
        assert_eq!(status_to_string(&SpanStatus::Error("test".into())), "error");
    }

    #[test]
    fn test_extract_error_type() {
        assert_eq!(extract_error_type("Timeout: connection failed"), "Timeout");
        assert_eq!(extract_error_type("NetworkError"), "NetworkError");
        assert_eq!(extract_error_type(""), "unknown");
    }

    #[test]
    fn test_tracing_metrics_helper() {
        let metrics = TracingMetrics::new("test-service");

        let span = Span::new("test_op", SpanKind::Internal);
        metrics.on_span_start(&span);

        let stats = metrics.stats();
        assert_eq!(stats.service_name, "test-service");
    }

    #[test]
    fn test_instrumented_span() {
        let metrics = TracingMetrics::new("test-service");
        let span = Span::new("instrumented_op", SpanKind::Server);

        let instrumented = InstrumentedSpan::new(span, metrics);
        assert!(!instrumented.span().is_ended());

        instrumented.end();
        // Metrics should be recorded
    }

    #[test]
    fn test_instrumented_span_drop() {
        let metrics = TracingMetrics::new("test-service");
        let span = Span::new("drop_test", SpanKind::Internal);

        {
            let _instrumented = InstrumentedSpan::new(span, metrics);
            // Don't call end(), let it drop
        }
        // Should record metrics on drop
    }

    #[test]
    fn test_global_counters() {
        let initial_spans = get_total_spans();
        record_span_started("counter_test", SpanKind::Internal);
        assert!(get_total_spans() > initial_spans);
    }
}
