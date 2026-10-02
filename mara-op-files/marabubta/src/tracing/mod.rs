// Marabunta - Licensed under the MIT License.
//! Distributed Tracing for Marabunta Compute
//!
//! This module provides comprehensive distributed tracing capabilities for observability
//! across the Marabunta Compute cluster. It follows the W3C Trace Context specification and
//! provides integration with the existing metrics module.
//!
//! # Architecture
//!
//! The tracing system consists of several components:
//!
//! - **TraceContext**: Carries trace and span identifiers across service boundaries
//! - **Span**: Represents a unit of work with timing, attributes, and events
//! - **TraceExporter**: Trait for exporting traces to various backends (Jaeger, stdout)
//! - **Middleware**: Axum middleware for automatic trace propagation via HTTP headers
//! - **Instrumentation**: Macros and helpers for instrumenting async functions
//!
//! # W3C Trace Context
//!
//! This implementation follows the W3C Trace Context specification:
//! - `traceparent`: Carries the trace-id, parent-id (span-id), and trace-flags
//! - `tracestate`: Carries vendor-specific trace information
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::tracing::{TraceContext, Span, SpanKind, TracingLayer};
//! use std::sync::Arc;
//!
//! // Create a new trace context
//! let ctx = TraceContext::new();
//!
//! // Create a span for an operation
//! let mut span = Span::new("process_task", SpanKind::Internal);
//! span.set_attribute("task.id", "task-123");
//!
//! // Do work...
//! span.add_event("checkpoint_saved");
//!
//! // Complete the span
//! span.end();
//!
//! // Use middleware in Axum
//! let app = Router::new()
//!     .route("/api/jobs", get(handler))
//!     .layer(TracingLayer::new(exporter));
//! ```
//!
//! # Metrics Integration
//!
//! The tracing module integrates with the metrics module to provide:
//! - Span duration histograms
//! - Active span counters
//! - Error rate tracking per operation
//!
//! # Jaeger Export
//!
//! Traces can be exported in Jaeger-compatible format for visualization:
//!
//! ```rust,ignore
//! use marabunta_compute::tracing::{JaegerExporter, TraceExporter};
//!
//! let exporter = JaegerExporter::new("http://jaeger:14268/api/traces");
//! exporter.export(&completed_spans).await?;
//! ```

mod context;
mod exporter;
mod metrics_integration;
mod middleware;
mod span;

pub use context::{TraceContext, TraceFlags, TraceState, TraceStateEntry};
pub use exporter::{
    JaegerBatch, JaegerExporter, JaegerProcess, JaegerSpan, JaegerTag, StdoutExporter,
    TraceExporter, TraceExporterError,
};
pub use middleware::{
    TracingLayer, TracingMiddleware, TracingMiddlewareConfig, W3C_TRACEPARENT, W3C_TRACESTATE,
};
pub use span::{Span, SpanContext, SpanEvent, SpanKind, SpanLink, SpanStatus};

// Metrics integration
pub use metrics_integration::{
    record_span_completed, record_span_error, record_span_started, InstrumentedSpan,
    TracingMetrics, TracingMetricsConfig, TracingStats,
};

// Re-export instrumentation helpers
pub use context::{current_trace_context, with_trace_context};

/// Generate a new random trace ID (128-bit)
pub fn generate_trace_id() -> [u8; 16] {
    let mut id = [0u8; 16];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut id);
    id
}

/// Generate a new random span ID (64-bit)
pub fn generate_span_id() -> [u8; 8] {
    let mut id = [0u8; 8];
    use rand::RngCore;
    rand::thread_rng().fill_bytes(&mut id);
    id
}

/// Convert bytes to hex string
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Convert hex string to bytes
pub fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[test]
    fn test_generate_trace_id() {
        let id1 = generate_trace_id();
        let id2 = generate_trace_id();

        // IDs should be different
        assert_ne!(id1, id2);

        // IDs should be 16 bytes
        assert_eq!(id1.len(), 16);
        assert_eq!(id2.len(), 16);
    }

    #[test]
    fn test_generate_span_id() {
        let id1 = generate_span_id();
        let id2 = generate_span_id();

        // IDs should be different
        assert_ne!(id1, id2);

        // IDs should be 8 bytes
        assert_eq!(id1.len(), 8);
        assert_eq!(id2.len(), 8);
    }

    #[test]
    fn test_bytes_to_hex() {
        let bytes = [0xde, 0xad, 0xbe, 0xef];
        assert_eq!(bytes_to_hex(&bytes), "deadbeef");

        let trace_id = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        assert_eq!(bytes_to_hex(&trace_id), "000102030405060708090a0b0c0d0e0f");
    }

    #[test]
    fn test_hex_to_bytes() {
        let hex = "deadbeef";
        let bytes = hex_to_bytes(hex).unwrap();
        assert_eq!(bytes, vec![0xde, 0xad, 0xbe, 0xef]);

        // Invalid hex
        assert!(hex_to_bytes("deadbee").is_none()); // Odd length
        assert!(hex_to_bytes("xyz").is_none()); // Invalid chars
    }

    #[test]
    fn test_trace_context_creation() {
        let ctx = TraceContext::new();

        // Should have valid trace_id and span_id
        assert_eq!(ctx.trace_id().len(), 16);
        assert_eq!(ctx.span_id().len(), 8);

        // No parent span for root context
        assert!(ctx.parent_span_id().is_none());
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new();
        let child = parent.new_child();

        // Child should have same trace_id
        assert_eq!(parent.trace_id(), child.trace_id());

        // Child should have different span_id
        assert_ne!(parent.span_id(), child.span_id());

        // Child's parent_span_id should be parent's span_id
        assert_eq!(child.parent_span_id(), Some(parent.span_id()));
    }

    #[test]
    fn test_traceparent_header() {
        let ctx = TraceContext::new();
        let header = ctx.to_traceparent();

        // Header should be in format: version-trace_id-span_id-flags
        let parts: Vec<&str> = header.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00"); // Version
        assert_eq!(parts[1].len(), 32); // trace_id (32 hex chars)
        assert_eq!(parts[2].len(), 16); // span_id (16 hex chars)
        assert_eq!(parts[3].len(), 2); // flags (2 hex chars)
    }

    #[test]
    fn test_traceparent_parsing() {
        let original = TraceContext::new();
        let header = original.to_traceparent();

        let parsed = TraceContext::from_traceparent(&header).unwrap();

        assert_eq!(original.trace_id(), parsed.trace_id());
        assert_eq!(original.span_id(), parsed.span_id());
        assert_eq!(original.flags(), parsed.flags());
    }

    #[test]
    fn test_invalid_traceparent() {
        // Invalid version
        assert!(TraceContext::from_traceparent("ff-0-0-00").is_none());

        // Invalid format
        assert!(TraceContext::from_traceparent("not-a-valid-header").is_none());

        // Invalid trace_id (all zeros)
        assert!(TraceContext::from_traceparent(
            "00-00000000000000000000000000000000-0000000000000001-00"
        )
        .is_none());
    }

    #[test]
    fn test_span_creation() {
        let mut span = Span::new("test_operation", SpanKind::Internal);

        assert_eq!(span.name(), "test_operation");
        assert_eq!(span.kind(), SpanKind::Internal);
        assert!(!span.is_ended());

        span.end();
        assert!(span.is_ended());
    }

    #[test]
    fn test_span_attributes() {
        let mut span = Span::new("test", SpanKind::Server);

        span.set_attribute("string", "value");
        span.set_attribute("number", 42i64);
        span.set_attribute("float", 3.14f64);
        span.set_attribute("bool", true);

        let attrs = span.attributes();
        assert_eq!(attrs.len(), 4);
    }

    #[test]
    fn test_span_events() {
        let mut span = Span::new("test", SpanKind::Internal);

        span.add_event("event1");
        span.add_event_with_attrs("event2", vec![("key", "value".into())]);

        assert_eq!(span.events().len(), 2);
    }

    #[test]
    fn test_span_status() {
        let mut span = Span::new("test", SpanKind::Internal);

        assert_eq!(span.status(), SpanStatus::Unset);

        span.set_status(SpanStatus::Ok);
        assert_eq!(span.status(), SpanStatus::Ok);

        span.set_error("Something went wrong");
        assert!(matches!(span.status(), SpanStatus::Error(_)));
    }

    #[test]
    fn test_span_timing() {
        let mut span = Span::new("test", SpanKind::Internal);
        let start = span.start_time();

        std::thread::sleep(std::time::Duration::from_millis(10));

        span.end();
        let end = span.end_time().unwrap();

        assert!(end > start);
        assert!(span.duration().unwrap().as_millis() >= 10);
    }

    #[test]
    fn test_trace_state() {
        let mut state = TraceState::new();

        state.set("vendor1", "value1");
        state.set("vendor2", "value2");

        assert_eq!(state.get("vendor1"), Some("value1"));
        assert_eq!(state.get("vendor2"), Some("value2"));
        assert_eq!(state.get("vendor3"), None);

        let header = state.to_header();
        assert!(header.contains("vendor1=value1"));
        assert!(header.contains("vendor2=value2"));

        let parsed = TraceState::from_header(&header).unwrap();
        assert_eq!(parsed.get("vendor1"), Some("value1"));
    }

    #[tokio::test]
    async fn test_stdout_exporter() {
        let exporter = StdoutExporter::new();

        let ctx = TraceContext::new();
        let mut span = Span::new_with_context("test_span", SpanKind::Internal, ctx);
        span.set_attribute("test", "value");
        span.end();

        // Should not fail
        let result = exporter.export(&[span]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_jaeger_span_conversion() {
        let ctx = TraceContext::new();
        let mut span = Span::new_with_context("test_operation", SpanKind::Server, ctx.clone());
        span.set_attribute("http.method", "GET");
        span.set_attribute("http.status_code", 200i64);
        span.add_event("request_started");
        span.end();

        let jaeger_span = JaegerSpan::from_span(&span);

        assert_eq!(jaeger_span.operation_name, "test_operation");
        assert_eq!(
            jaeger_span.trace_id_low,
            u64::from_be_bytes(ctx.trace_id()[8..16].try_into().unwrap())
        );
        assert!(!jaeger_span.tags.is_empty());
        assert!(!jaeger_span.logs.is_empty());
    }

    #[test]
    fn test_span_links() {
        let mut span = Span::new("test", SpanKind::Internal);

        let linked_ctx = TraceContext::new();
        span.add_link(SpanLink::new(linked_ctx.clone()));

        let links = span.links();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].trace_context().trace_id(), linked_ctx.trace_id());
    }

    #[test]
    fn test_trace_flags() {
        let flags = TraceFlags::new(0x01);
        assert!(flags.is_sampled());

        let flags = TraceFlags::new(0x00);
        assert!(!flags.is_sampled());

        let flags = TraceFlags::sampled();
        assert!(flags.is_sampled());
        assert_eq!(flags.as_byte(), 0x01);
    }

    #[test]
    fn test_middleware_config() {
        let config = TracingMiddlewareConfig::default();

        assert!(config.propagate_context);
        assert!(config.create_server_spans);
        assert!(!config.exempt_paths.is_empty());
    }

    #[tokio::test]
    async fn test_context_propagation() {
        // Test task-local context propagation
        let ctx = TraceContext::new();
        let ctx_clone = ctx.clone();

        with_trace_context(ctx, async {
            let current = current_trace_context();
            assert!(current.is_some());
            assert_eq!(current.unwrap().trace_id(), ctx_clone.trace_id());
        })
        .await;

        // Outside the context, should be None
        assert!(current_trace_context().is_none());
    }

    #[test]
    fn test_span_context() {
        let trace_ctx = TraceContext::new();
        let span_ctx = SpanContext::from_trace_context(&trace_ctx);

        assert_eq!(span_ctx.trace_id(), trace_ctx.trace_id());
        assert_eq!(span_ctx.span_id(), trace_ctx.span_id());
    }
}
