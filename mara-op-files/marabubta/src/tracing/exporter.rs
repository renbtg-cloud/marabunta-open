// Marabunta - Licensed under the MIT License.
//! Trace exporters for sending spans to various backends
//!
//! This module provides the `TraceExporter` trait and implementations
//! for Jaeger-compatible format and stdout debugging.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

use super::span::{AttributeValue, Span, SpanStatus};

/// Errors that can occur during trace export
#[derive(Error, Debug)]
pub enum TraceExporterError {
    /// Failed to serialize spans
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Failed to send spans to backend
    #[error("Export failed: {0}")]
    ExportFailed(String),

    /// Network error during export
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Backend returned an error
    #[error("Backend error: {status_code} - {message}")]
    BackendError { status_code: u16, message: String },

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for trace exporter operations
pub type ExporterResult<T> = Result<T, TraceExporterError>;

/// Trait for exporting completed spans
#[async_trait]
pub trait TraceExporter: Send + Sync {
    /// Export a batch of completed spans
    async fn export(&self, spans: &[Span]) -> ExporterResult<()>;

    /// Shutdown the exporter (flush any pending data)
    async fn shutdown(&self) -> ExporterResult<()> {
        Ok(())
    }

    /// Force flush any buffered spans
    async fn force_flush(&self) -> ExporterResult<()> {
        Ok(())
    }
}

// ============================================================================
// Stdout Exporter
// ============================================================================

/// Exporter that writes spans to stdout (for debugging)
#[derive(Clone)]
pub struct StdoutExporter {
    /// Whether to pretty-print JSON
    pretty: bool,
}

impl StdoutExporter {
    /// Create a new stdout exporter
    pub fn new() -> Self {
        Self { pretty: true }
    }

    /// Create a compact stdout exporter (no pretty printing)
    pub fn compact() -> Self {
        Self { pretty: false }
    }
}

impl Default for StdoutExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TraceExporter for StdoutExporter {
    async fn export(&self, spans: &[Span]) -> ExporterResult<()> {
        for span in spans {
            let output = StdoutSpan::from_span(span);
            let json = if self.pretty {
                serde_json::to_string_pretty(&output)
            } else {
                serde_json::to_string(&output)
            }
            .map_err(|e| TraceExporterError::Serialization(e.to_string()))?;

            println!("{}", json);
        }
        Ok(())
    }
}

/// Simplified span representation for stdout export
#[derive(Debug, Serialize, Deserialize)]
struct StdoutSpan {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    operation_name: String,
    kind: String,
    start_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    duration_ms: Option<i64>,
    status: String,
    attributes: HashMap<String, serde_json::Value>,
    events: Vec<StdoutEvent>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StdoutEvent {
    name: String,
    timestamp: DateTime<Utc>,
    attributes: HashMap<String, serde_json::Value>,
}

impl StdoutSpan {
    fn from_span(span: &Span) -> Self {
        Self {
            trace_id: span.trace_id_hex(),
            span_id: span.span_id_hex(),
            parent_span_id: span.parent_span_id_hex(),
            operation_name: span.name().to_string(),
            kind: span.kind().to_string(),
            start_time: span.start_time(),
            end_time: span.end_time(),
            duration_ms: span.duration().map(|d| d.as_millis() as i64),
            status: match span.status() {
                SpanStatus::Unset => "UNSET".to_string(),
                SpanStatus::Ok => "OK".to_string(),
                SpanStatus::Error(msg) => format!("ERROR: {}", msg),
            },
            attributes: span
                .attributes()
                .iter()
                .map(|(k, v)| (k.clone(), attribute_to_json(v)))
                .collect(),
            events: span
                .events()
                .iter()
                .map(|e| StdoutEvent {
                    name: e.name().to_string(),
                    timestamp: e.timestamp(),
                    attributes: e
                        .attributes()
                        .iter()
                        .map(|(k, v)| (k.clone(), attribute_to_json(v)))
                        .collect(),
                })
                .collect(),
        }
    }
}

fn attribute_to_json(value: &AttributeValue) -> serde_json::Value {
    match value {
        AttributeValue::String(s) => serde_json::Value::String(s.clone()),
        AttributeValue::Bool(b) => serde_json::Value::Bool(*b),
        AttributeValue::Int(i) => serde_json::Value::Number((*i).into()),
        AttributeValue::Float(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        AttributeValue::StringArray(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        ),
        AttributeValue::IntArray(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|i| serde_json::Value::Number((*i).into()))
                .collect(),
        ),
        AttributeValue::FloatArray(arr) => serde_json::Value::Array(
            arr.iter()
                .filter_map(|f| serde_json::Number::from_f64(*f))
                .map(serde_json::Value::Number)
                .collect(),
        ),
        AttributeValue::BoolArray(arr) => {
            serde_json::Value::Array(arr.iter().map(|b| serde_json::Value::Bool(*b)).collect())
        }
    }
}

// ============================================================================
// Jaeger Exporter
// ============================================================================

/// Tag for Jaeger spans
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum JaegerTagValue {
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "bool")]
    Bool(bool),
    #[serde(rename = "int64")]
    Int64(i64),
    #[serde(rename = "float64")]
    Float64(f64),
    #[serde(rename = "binary")]
    Binary(Vec<u8>),
}

/// Jaeger span tag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerTag {
    pub key: String,
    #[serde(flatten)]
    pub value: JaegerTagValue,
}

impl JaegerTag {
    /// Create a string tag
    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: JaegerTagValue::String(value.into()),
        }
    }

    /// Create a bool tag
    pub fn bool(key: impl Into<String>, value: bool) -> Self {
        Self {
            key: key.into(),
            value: JaegerTagValue::Bool(value),
        }
    }

    /// Create an int64 tag
    pub fn int64(key: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            value: JaegerTagValue::Int64(value),
        }
    }

    /// Create a float64 tag
    pub fn float64(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value: JaegerTagValue::Float64(value),
        }
    }
}

/// Jaeger span log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerLog {
    /// Timestamp in microseconds
    pub timestamp: i64,
    /// Log fields
    pub fields: Vec<JaegerTag>,
}

/// Jaeger span reference
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JaegerRefType {
    ChildOf,
    FollowsFrom,
}

/// Jaeger span reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerSpanRef {
    #[serde(rename = "refType")]
    pub ref_type: JaegerRefType,
    #[serde(rename = "traceIdHigh")]
    pub trace_id_high: u64,
    #[serde(rename = "traceIdLow")]
    pub trace_id_low: u64,
    #[serde(rename = "spanId")]
    pub span_id: u64,
}

/// Jaeger span in Thrift-compatible JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerSpan {
    /// Trace ID high 64 bits
    #[serde(rename = "traceIdHigh")]
    pub trace_id_high: u64,
    /// Trace ID low 64 bits
    #[serde(rename = "traceIdLow")]
    pub trace_id_low: u64,
    /// Span ID
    #[serde(rename = "spanId")]
    pub span_id: u64,
    /// Parent span ID (0 if root)
    #[serde(rename = "parentSpanId")]
    pub parent_span_id: u64,
    /// Operation name
    #[serde(rename = "operationName")]
    pub operation_name: String,
    /// Span references
    pub references: Vec<JaegerSpanRef>,
    /// Span flags
    pub flags: u32,
    /// Start time in microseconds since epoch
    #[serde(rename = "startTime")]
    pub start_time: i64,
    /// Duration in microseconds
    pub duration: i64,
    /// Tags
    pub tags: Vec<JaegerTag>,
    /// Logs
    pub logs: Vec<JaegerLog>,
}

impl JaegerSpan {
    /// Convert from our Span type to Jaeger format
    pub fn from_span(span: &Span) -> Self {
        let trace_id = span.trace_context().trace_id();
        let trace_id_high = u64::from_be_bytes(trace_id[0..8].try_into().unwrap());
        let trace_id_low = u64::from_be_bytes(trace_id[8..16].try_into().unwrap());

        let span_id = u64::from_be_bytes(span.trace_context().span_id()[..].try_into().unwrap());

        let parent_span_id = span
            .trace_context()
            .parent_span_id()
            .map(|id| u64::from_be_bytes(id[..].try_into().unwrap()))
            .unwrap_or(0);

        let mut tags = Vec::new();

        // Add span kind tag
        tags.push(JaegerTag::string("span.kind", span.kind().to_string()));

        // Add status tags
        match span.status() {
            SpanStatus::Ok => {
                tags.push(JaegerTag::string("otel.status_code", "OK"));
            }
            SpanStatus::Error(msg) => {
                tags.push(JaegerTag::bool("error", true));
                tags.push(JaegerTag::string("otel.status_code", "ERROR"));
                tags.push(JaegerTag::string("otel.status_description", msg));
            }
            SpanStatus::Unset => {}
        }

        // Convert attributes to tags
        for (key, value) in span.attributes() {
            let tag = match value {
                AttributeValue::String(s) => JaegerTag::string(key, s),
                AttributeValue::Bool(b) => JaegerTag::bool(key, *b),
                AttributeValue::Int(i) => JaegerTag::int64(key, *i),
                AttributeValue::Float(f) => JaegerTag::float64(key, *f),
                AttributeValue::StringArray(arr) => JaegerTag::string(key, arr.join(",")),
                AttributeValue::IntArray(arr) => JaegerTag::string(
                    key,
                    arr.iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ),
                AttributeValue::FloatArray(arr) => JaegerTag::string(
                    key,
                    arr.iter()
                        .map(|f| f.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ),
                AttributeValue::BoolArray(arr) => JaegerTag::string(
                    key,
                    arr.iter()
                        .map(|b| b.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ),
            };
            tags.push(tag);
        }

        // Convert events to logs
        let logs: Vec<JaegerLog> = span
            .events()
            .iter()
            .map(|event| {
                let mut fields = vec![JaegerTag::string("event", event.name())];
                for (key, value) in event.attributes() {
                    let tag = match value {
                        AttributeValue::String(s) => JaegerTag::string(key, s),
                        AttributeValue::Bool(b) => JaegerTag::bool(key, *b),
                        AttributeValue::Int(i) => JaegerTag::int64(key, *i),
                        AttributeValue::Float(f) => JaegerTag::float64(key, *f),
                        _ => JaegerTag::string(key, format!("{:?}", value)),
                    };
                    fields.push(tag);
                }
                JaegerLog {
                    timestamp: event.timestamp().timestamp_micros(),
                    fields,
                }
            })
            .collect();

        // Build references
        let mut references = Vec::new();
        if parent_span_id != 0 {
            references.push(JaegerSpanRef {
                ref_type: JaegerRefType::ChildOf,
                trace_id_high,
                trace_id_low,
                span_id: parent_span_id,
            });
        }

        // Add links as FollowsFrom references
        for link in span.links() {
            let link_trace_id = link.trace_context().trace_id();
            let link_trace_id_high = u64::from_be_bytes(link_trace_id[0..8].try_into().unwrap());
            let link_trace_id_low = u64::from_be_bytes(link_trace_id[8..16].try_into().unwrap());
            let link_span_id =
                u64::from_be_bytes(link.trace_context().span_id()[..].try_into().unwrap());

            references.push(JaegerSpanRef {
                ref_type: JaegerRefType::FollowsFrom,
                trace_id_high: link_trace_id_high,
                trace_id_low: link_trace_id_low,
                span_id: link_span_id,
            });
        }

        let start_time = span.start_time().timestamp_micros();
        let duration = span.duration_micros().unwrap_or(0);

        Self {
            trace_id_high,
            trace_id_low,
            span_id,
            parent_span_id,
            operation_name: span.name().to_string(),
            references,
            flags: if span.is_sampled() { 1 } else { 0 },
            start_time,
            duration,
            tags,
            logs,
        }
    }
}

/// Jaeger process information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerProcess {
    /// Service name
    #[serde(rename = "serviceName")]
    pub service_name: String,
    /// Process tags
    pub tags: Vec<JaegerTag>,
}

impl JaegerProcess {
    /// Create a new process
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            tags: Vec::new(),
        }
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: JaegerTag) -> Self {
        self.tags.push(tag);
        self
    }

    /// Add hostname tag
    pub fn with_hostname(self) -> Self {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        self.with_tag(JaegerTag::string("hostname", hostname))
    }

    /// Add version tag
    pub fn with_version(self, version: impl Into<String>) -> Self {
        self.with_tag(JaegerTag::string("version", version))
    }
}

/// Jaeger batch for export
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerBatch {
    /// Process information
    pub process: JaegerProcess,
    /// Spans in this batch
    pub spans: Vec<JaegerSpan>,
}

impl JaegerBatch {
    /// Create a new batch
    pub fn new(process: JaegerProcess, spans: Vec<JaegerSpan>) -> Self {
        Self { process, spans }
    }

    /// Create from our spans
    pub fn from_spans(process: JaegerProcess, spans: &[Span]) -> Self {
        Self {
            process,
            spans: spans.iter().map(JaegerSpan::from_span).collect(),
        }
    }
}

/// Jaeger exporter configuration
#[derive(Debug, Clone)]
pub struct JaegerExporterConfig {
    /// Endpoint URL for the Jaeger collector
    pub endpoint: String,
    /// Service name
    pub service_name: String,
    /// Additional process tags
    pub tags: Vec<JaegerTag>,
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Maximum batch size
    pub max_batch_size: usize,
}

impl Default for JaegerExporterConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:14268/api/traces".to_string(),
            service_name: "marabunta-compute".to_string(),
            tags: Vec::new(),
            timeout_secs: 30,
            max_batch_size: 100,
        }
    }
}

impl JaegerExporterConfig {
    /// Create a new config with endpoint
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Default::default()
        }
    }

    /// Set the service name
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: JaegerTag) -> Self {
        self.tags.push(tag);
        self
    }
}

/// Jaeger-compatible trace exporter
///
/// Exports spans to a Jaeger collector using the Thrift-over-HTTP protocol.
pub struct JaegerExporter {
    config: JaegerExporterConfig,
    client: reqwest::Client,
    buffer: Arc<Mutex<Vec<Span>>>,
}

impl JaegerExporter {
    /// Create a new Jaeger exporter with default configuration
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_config(JaegerExporterConfig::new(endpoint))
    }

    /// Create a new Jaeger exporter with configuration
    pub fn with_config(config: JaegerExporterConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            config,
            client,
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get the process information for this exporter
    fn process(&self) -> JaegerProcess {
        let mut process = JaegerProcess::new(&self.config.service_name);
        for tag in &self.config.tags {
            process.tags.push(tag.clone());
        }
        process
    }

    /// Convert spans to Jaeger batch
    fn to_batch(&self, spans: &[Span]) -> JaegerBatch {
        JaegerBatch::from_spans(self.process(), spans)
    }
}

#[async_trait]
impl TraceExporter for JaegerExporter {
    async fn export(&self, spans: &[Span]) -> ExporterResult<()> {
        if spans.is_empty() {
            return Ok(());
        }

        let batch = self.to_batch(spans);

        let response = self
            .client
            .post(&self.config.endpoint)
            .header("Content-Type", "application/json")
            .json(&batch)
            .send()
            .await
            .map_err(|e| TraceExporterError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status().as_u16();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(TraceExporterError::BackendError {
                status_code: status,
                message,
            })
        }
    }

    async fn shutdown(&self) -> ExporterResult<()> {
        self.force_flush().await
    }

    async fn force_flush(&self) -> ExporterResult<()> {
        let spans = {
            let mut buffer = self.buffer.lock().await;
            std::mem::take(&mut *buffer)
        };

        if !spans.is_empty() {
            self.export(&spans).await?;
        }

        Ok(())
    }
}

// ============================================================================
// Multi-Exporter
// ============================================================================

/// Exporter that sends spans to multiple backends
pub struct MultiExporter {
    exporters: Vec<Box<dyn TraceExporter>>,
}

impl MultiExporter {
    /// Create a new multi-exporter
    pub fn new() -> Self {
        Self {
            exporters: Vec::new(),
        }
    }

    /// Add an exporter
    pub fn add_exporter(mut self, exporter: Box<dyn TraceExporter>) -> Self {
        self.exporters.push(exporter);
        self
    }
}

impl Default for MultiExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TraceExporter for MultiExporter {
    async fn export(&self, spans: &[Span]) -> ExporterResult<()> {
        let mut last_error = None;

        for exporter in &self.exporters {
            if let Err(e) = exporter.export(spans).await {
                last_error = Some(e);
            }
        }

        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn shutdown(&self) -> ExporterResult<()> {
        for exporter in &self.exporters {
            exporter.shutdown().await?;
        }
        Ok(())
    }
}

// ============================================================================
// Buffered Exporter
// ============================================================================

/// Exporter that buffers spans before sending
pub struct BufferedExporter<E: TraceExporter> {
    inner: E,
    buffer: Arc<Mutex<Vec<Span>>>,
    max_buffer_size: usize,
}

impl<E: TraceExporter> BufferedExporter<E> {
    /// Create a new buffered exporter
    pub fn new(inner: E, max_buffer_size: usize) -> Self {
        Self {
            inner,
            buffer: Arc::new(Mutex::new(Vec::new())),
            max_buffer_size,
        }
    }

    /// Get the current buffer size
    pub async fn buffer_size(&self) -> usize {
        self.buffer.lock().await.len()
    }
}

#[async_trait]
impl<E: TraceExporter + 'static> TraceExporter for BufferedExporter<E> {
    async fn export(&self, spans: &[Span]) -> ExporterResult<()> {
        let mut buffer = self.buffer.lock().await;
        buffer.extend(spans.iter().cloned());

        if buffer.len() >= self.max_buffer_size {
            let to_export = std::mem::take(&mut *buffer);
            drop(buffer);
            self.inner.export(&to_export).await?;
        }

        Ok(())
    }

    async fn shutdown(&self) -> ExporterResult<()> {
        self.force_flush().await?;
        self.inner.shutdown().await
    }

    async fn force_flush(&self) -> ExporterResult<()> {
        let spans = {
            let mut buffer = self.buffer.lock().await;
            std::mem::take(&mut *buffer)
        };

        if !spans.is_empty() {
            self.inner.export(&spans).await?;
        }

        self.inner.force_flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracing::{SpanKind, TraceContext};

    #[test]
    fn test_jaeger_tag_creation() {
        let string_tag = JaegerTag::string("key", "value");
        assert_eq!(string_tag.key, "key");
        assert!(matches!(string_tag.value, JaegerTagValue::String(_)));

        let bool_tag = JaegerTag::bool("enabled", true);
        assert!(matches!(bool_tag.value, JaegerTagValue::Bool(true)));

        let int_tag = JaegerTag::int64("count", 42);
        assert!(matches!(int_tag.value, JaegerTagValue::Int64(42)));

        let float_tag = JaegerTag::float64("rate", 3.14);
        assert!(matches!(float_tag.value, JaegerTagValue::Float64(_)));
    }

    #[test]
    fn test_jaeger_process() {
        let process = JaegerProcess::new("test-service")
            .with_tag(JaegerTag::string("env", "test"))
            .with_version("1.0.0");

        assert_eq!(process.service_name, "test-service");
        assert_eq!(process.tags.len(), 2);
    }

    #[test]
    fn test_jaeger_span_conversion() {
        let ctx = TraceContext::new();
        let mut span = Span::new_with_context("test_operation", SpanKind::Server, ctx);
        span.set_attribute("http.method", "GET");
        span.set_attribute("http.status_code", 200i64);
        span.add_event("request_started");
        span.set_ok();
        span.end();

        let jaeger_span = JaegerSpan::from_span(&span);

        assert_eq!(jaeger_span.operation_name, "test_operation");
        assert!(jaeger_span.trace_id_low != 0 || jaeger_span.trace_id_high != 0);
        assert!(jaeger_span.span_id != 0);
        assert!(jaeger_span.duration >= 0);
        assert!(!jaeger_span.tags.is_empty());
        assert!(!jaeger_span.logs.is_empty());
    }

    #[test]
    fn test_jaeger_span_with_parent() {
        let parent = TraceContext::new();
        let child = Span::child("child_op", SpanKind::Internal, &parent);

        let jaeger_span = JaegerSpan::from_span(&child);

        assert_eq!(jaeger_span.references.len(), 1);
        assert!(matches!(
            jaeger_span.references[0].ref_type,
            JaegerRefType::ChildOf
        ));
    }

    #[test]
    fn test_jaeger_batch() {
        let process = JaegerProcess::new("test-service");
        let spans = vec![
            Span::new("op1", SpanKind::Server),
            Span::new("op2", SpanKind::Client),
        ];

        let batch = JaegerBatch::from_spans(process.clone(), &spans);

        assert_eq!(batch.process.service_name, "test-service");
        assert_eq!(batch.spans.len(), 2);
    }

    #[test]
    fn test_jaeger_exporter_config() {
        let config = JaegerExporterConfig::new("http://jaeger:14268/api/traces")
            .with_service_name("my-service")
            .with_tag(JaegerTag::string("env", "production"));

        assert_eq!(config.endpoint, "http://jaeger:14268/api/traces");
        assert_eq!(config.service_name, "my-service");
        assert_eq!(config.tags.len(), 1);
    }

    #[tokio::test]
    async fn test_stdout_exporter() {
        let exporter = StdoutExporter::new();
        let spans = vec![Span::new("test", SpanKind::Internal)];

        // Should not fail
        let result = exporter.export(&spans).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_stdout_exporter_compact() {
        let exporter = StdoutExporter::compact();
        let mut span = Span::new("test", SpanKind::Internal);
        span.set_attribute("key", "value");
        span.end();

        let result = exporter.export(&[span]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_multi_exporter() {
        let stdout1 = StdoutExporter::compact();
        let stdout2 = StdoutExporter::compact();

        let multi = MultiExporter::new()
            .add_exporter(Box::new(stdout1))
            .add_exporter(Box::new(stdout2));

        let spans = vec![Span::new("test", SpanKind::Internal)];
        let result = multi.export(&spans).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_buffered_exporter() {
        let inner = StdoutExporter::compact();
        let buffered = BufferedExporter::new(inner, 5);

        // Add 3 spans - should not flush
        let spans = vec![
            Span::new("op1", SpanKind::Internal),
            Span::new("op2", SpanKind::Internal),
            Span::new("op3", SpanKind::Internal),
        ];
        buffered.export(&spans).await.unwrap();
        assert_eq!(buffered.buffer_size().await, 3);

        // Add 3 more - should flush (>= 5)
        buffered.export(&spans).await.unwrap();
        assert_eq!(buffered.buffer_size().await, 0);
    }

    #[test]
    fn test_attribute_to_json() {
        let string_val = AttributeValue::String("test".into());
        assert!(attribute_to_json(&string_val).is_string());

        let bool_val = AttributeValue::Bool(true);
        assert!(attribute_to_json(&bool_val).is_boolean());

        let int_val = AttributeValue::Int(42);
        assert!(attribute_to_json(&int_val).is_number());

        let array_val = AttributeValue::StringArray(vec!["a".into(), "b".into()]);
        assert!(attribute_to_json(&array_val).is_array());
    }

    #[test]
    fn test_error_status_in_jaeger() {
        let mut span = Span::new("failing_op", SpanKind::Internal);
        span.set_error("Something went wrong");
        span.end();

        let jaeger_span = JaegerSpan::from_span(&span);

        // Should have error=true tag
        let has_error_tag = jaeger_span
            .tags
            .iter()
            .any(|t| t.key == "error" && matches!(t.value, JaegerTagValue::Bool(true)));
        assert!(has_error_tag);

        // Should have status description
        let has_status_desc = jaeger_span
            .tags
            .iter()
            .any(|t| t.key == "otel.status_description");
        assert!(has_status_desc);
    }
}
