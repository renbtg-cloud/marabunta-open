// Marabunta - Licensed under the MIT License.
//! Structured Logging Module for Marabunta Compute
//!
//! This module provides a comprehensive logging infrastructure with:
//! - Structured JSON log format with consistent fields
//! - Log correlation with trace context (trace_id, span_id)
//! - Log sampling for high-volume events
//! - Log aggregation helpers for collecting logs from multiple sources
//! - Dynamic log level adjustment per component via LogFilter
//! - Integration with the tracing crate ecosystem
//!
//! # Example Usage
//!
//! ```rust,ignore
//! use marabunta_compute::logging::{init_logging, LogConfig, LogLevel, LogContext};
//!
//! // Initialize logging with default config
//! let config = LogConfig::default();
//! init_logging(&config)?;
//!
//! // Use with trace context
//! let ctx = LogContext::new("worker", "node-123");
//! tracing::info!(
//!     trace_id = %ctx.trace_id,
//!     span_id = %ctx.span_id,
//!     component = %ctx.component,
//!     node_id = %ctx.node_id,
//!     "Worker started"
//! );
//! ```

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{Level, Subscriber};
use tracing_subscriber::{
    fmt::{self, format::FmtSpan, MakeWriter},
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};
use uuid::Uuid;

/// Log levels matching tracing's Level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

impl From<Level> for LogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::TRACE => LogLevel::Trace,
            Level::DEBUG => LogLevel::Debug,
            Level::INFO => LogLevel::Info,
            Level::WARN => LogLevel::Warn,
            Level::ERROR => LogLevel::Error,
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "trace"),
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warn => write!(f, "warn"),
            LogLevel::Error => write!(f, "error"),
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "trace" => Ok(LogLevel::Trace),
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "warn" | "warning" => Ok(LogLevel::Warn),
            "error" => Ok(LogLevel::Error),
            _ => Err(format!("Invalid log level: {}", s)),
        }
    }
}

/// Configuration for the logging system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Default log level for all components
    pub default_level: LogLevel,
    /// JSON output format (vs human-readable)
    pub json_format: bool,
    /// Include source file and line number
    pub include_source_location: bool,
    /// Include thread IDs in log output
    pub include_thread_id: bool,
    /// Include span events (enter/exit)
    pub include_span_events: bool,
    /// Per-component log level overrides
    pub component_levels: HashMap<String, LogLevel>,
    /// Sampling configuration for high-volume logs
    pub sampling: SamplingConfig,
    /// Output target configuration
    pub output: LogOutputConfig,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            default_level: LogLevel::Info,
            json_format: true,
            include_source_location: true,
            include_thread_id: false,
            include_span_events: false,
            component_levels: HashMap::new(),
            sampling: SamplingConfig::default(),
            output: LogOutputConfig::default(),
        }
    }
}

/// Sampling configuration for log throttling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// Enable log sampling
    pub enabled: bool,
    /// Default sampling rate (0.0 - 1.0, where 1.0 = log everything)
    pub default_rate: f64,
    /// Per-event sampling rates (event name -> rate)
    pub event_rates: HashMap<String, f64>,
    /// Sample 1 in N for high-volume events at debug level
    pub debug_sample_rate: usize,
    /// Sample 1 in N for high-volume events at trace level
    pub trace_sample_rate: usize,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_rate: 1.0,
            event_rates: HashMap::new(),
            debug_sample_rate: 10,
            trace_sample_rate: 100,
        }
    }
}

/// Log output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogOutputConfig {
    /// Write to stdout
    pub stdout: bool,
    /// Write to stderr (for errors only)
    pub stderr_for_errors: bool,
    /// Write to file
    pub file_path: Option<String>,
    /// Maximum log file size in bytes before rotation
    pub max_file_size: u64,
    /// Number of rotated files to keep
    pub max_files: usize,
}

impl Default for LogOutputConfig {
    fn default() -> Self {
        Self {
            stdout: true,
            stderr_for_errors: false,
            file_path: None,
            max_file_size: 100 * 1024 * 1024, // 100 MB
            max_files: 5,
        }
    }
}

/// Trace context for log correlation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    /// Unique trace ID for request correlation
    pub trace_id: TraceId,
    /// Span ID within the trace
    pub span_id: SpanId,
    /// Optional parent span ID
    pub parent_span_id: Option<SpanId>,
}

impl TraceContext {
    /// Creates a new root trace context
    pub fn new() -> Self {
        Self {
            trace_id: TraceId::new(),
            span_id: SpanId::new(),
            parent_span_id: None,
        }
    }

    /// Creates a child span within the same trace
    pub fn child_span(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: SpanId::new(),
            parent_span_id: Some(self.span_id),
        }
    }

    /// Creates a trace context from existing IDs
    pub fn from_ids(trace_id: TraceId, span_id: SpanId) -> Self {
        Self {
            trace_id,
            span_id,
            parent_span_id: None,
        }
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Unique trace identifier for request correlation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId(pub Uuid);

impl TraceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parse from a string representation
    pub fn from_str(s: &str) -> Option<Self> {
        Uuid::parse_str(s).ok().map(TraceId)
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Span identifier within a trace
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanId(pub u64);

impl SpanId {
    pub fn new() -> Self {
        Self(rand::thread_rng().gen())
    }

    /// Parse from a string representation
    pub fn from_str(s: &str) -> Option<Self> {
        s.parse().ok().map(SpanId)
    }
}

impl Default for SpanId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Log context containing common fields for structured logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogContext {
    /// Trace context for correlation
    pub trace: TraceContext,
    /// Component name (e.g., "worker", "master", "coordinator")
    pub component: String,
    /// Node ID (worker ID, master ID, etc.)
    pub node_id: String,
    /// Additional metadata fields
    pub metadata: HashMap<String, serde_json::Value>,
}

impl LogContext {
    /// Creates a new log context
    pub fn new(component: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            trace: TraceContext::new(),
            component: component.into(),
            node_id: node_id.into(),
            metadata: HashMap::new(),
        }
    }

    /// Creates a context with an existing trace
    pub fn with_trace(
        trace: TraceContext,
        component: impl Into<String>,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            trace,
            component: component.into(),
            node_id: node_id.into(),
            metadata: HashMap::new(),
        }
    }

    /// Adds metadata to the context
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Returns the trace ID as a string
    pub fn trace_id(&self) -> String {
        self.trace.trace_id.to_string()
    }

    /// Returns the span ID as a string
    pub fn span_id(&self) -> String {
        self.trace.span_id.to_string()
    }

    /// Creates a child context for a sub-operation
    pub fn child(&self) -> Self {
        Self {
            trace: self.trace.child_span(),
            component: self.component.clone(),
            node_id: self.node_id.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

/// Structured log entry in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredLogEntry {
    /// ISO 8601 timestamp
    pub timestamp: DateTime<Utc>,
    /// Log level
    pub level: LogLevel,
    /// Log message
    pub message: String,
    /// Trace ID for correlation
    pub trace_id: Option<String>,
    /// Span ID within the trace
    pub span_id: Option<String>,
    /// Component name
    pub component: Option<String>,
    /// Node ID
    pub node_id: Option<String>,
    /// Target (module path)
    pub target: Option<String>,
    /// Source file
    pub file: Option<String>,
    /// Source line number
    pub line: Option<u32>,
    /// Thread ID
    pub thread_id: Option<String>,
    /// Thread name
    pub thread_name: Option<String>,
    /// Additional fields
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}

impl StructuredLogEntry {
    /// Creates a new log entry
    pub fn new(level: LogLevel, message: impl Into<String>) -> Self {
        Self {
            timestamp: Utc::now(),
            level,
            message: message.into(),
            trace_id: None,
            span_id: None,
            component: None,
            node_id: None,
            target: None,
            file: None,
            line: None,
            thread_id: None,
            thread_name: None,
            fields: HashMap::new(),
        }
    }

    /// Sets trace context
    pub fn with_trace(mut self, ctx: &TraceContext) -> Self {
        self.trace_id = Some(ctx.trace_id.to_string());
        self.span_id = Some(ctx.span_id.to_string());
        self
    }

    /// Sets log context
    pub fn with_context(mut self, ctx: &LogContext) -> Self {
        self.trace_id = Some(ctx.trace.trace_id.to_string());
        self.span_id = Some(ctx.trace.span_id.to_string());
        self.component = Some(ctx.component.clone());
        self.node_id = Some(ctx.node_id.clone());
        for (key, value) in &ctx.metadata {
            self.fields.insert(key.clone(), value.clone());
        }
        self
    }

    /// Adds a field to the log entry
    pub fn with_field(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// Serializes to JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Serializes to pretty JSON string
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// Log sampler for high-volume event throttling
#[derive(Debug)]
pub struct LogSampler {
    config: SamplingConfig,
    counters: RwLock<HashMap<String, AtomicU64>>,
}

impl LogSampler {
    /// Creates a new sampler with the given configuration
    pub fn new(config: SamplingConfig) -> Self {
        Self {
            config,
            counters: RwLock::new(HashMap::new()),
        }
    }

    /// Checks if an event should be logged based on sampling configuration
    pub fn should_log(&self, event_name: &str, level: LogLevel) -> bool {
        if !self.config.enabled {
            return true;
        }

        // Get sampling rate for this event
        let rate = self
            .config
            .event_rates
            .get(event_name)
            .copied()
            .unwrap_or_else(|| match level {
                LogLevel::Trace => 1.0 / self.config.trace_sample_rate as f64,
                LogLevel::Debug => 1.0 / self.config.debug_sample_rate as f64,
                _ => self.config.default_rate,
            });

        // Always increment the counter for tracking purposes
        let counter = {
            let counters = self.counters.read();
            if let Some(counter) = counters.get(event_name) {
                counter.fetch_add(1, Ordering::Relaxed)
            } else {
                drop(counters);
                let mut counters = self.counters.write();
                counters
                    .entry(event_name.to_string())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed)
            }
        };

        if rate >= 1.0 {
            return true;
        }
        if rate <= 0.0 {
            return false;
        }

        // Sample based on counter and rate
        let sample_every = (1.0 / rate) as u64;
        counter % sample_every == 0
    }

    /// Resets all sampling counters
    pub fn reset_counters(&self) {
        let counters = self.counters.write();
        for counter in counters.values() {
            counter.store(0, Ordering::Relaxed);
        }
    }

    /// Gets the current count for an event
    pub fn get_count(&self, event_name: &str) -> u64 {
        let counters = self.counters.read();
        counters
            .get(event_name)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

impl Clone for LogSampler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            counters: RwLock::new(HashMap::new()),
        }
    }
}

/// Dynamic log filter for runtime level adjustment per component
#[derive(Debug)]
pub struct LogFilter {
    default_level: RwLock<LogLevel>,
    component_levels: RwLock<HashMap<String, LogLevel>>,
    sampler: LogSampler,
}

impl LogFilter {
    /// Creates a new log filter with default level
    pub fn new(default_level: LogLevel) -> Self {
        Self {
            default_level: RwLock::new(default_level),
            component_levels: RwLock::new(HashMap::new()),
            sampler: LogSampler::new(SamplingConfig::default()),
        }
    }

    /// Creates a log filter with custom configuration
    pub fn with_config(config: &LogConfig) -> Self {
        Self {
            default_level: RwLock::new(config.default_level),
            component_levels: RwLock::new(config.component_levels.clone()),
            sampler: LogSampler::new(config.sampling.clone()),
        }
    }

    /// Gets the current log level for a component
    pub fn get_level(&self, component: &str) -> LogLevel {
        let component_levels = self.component_levels.read();
        component_levels
            .get(component)
            .copied()
            .unwrap_or_else(|| *self.default_level.read())
    }

    /// Sets the log level for a specific component
    pub fn set_component_level(&self, component: impl Into<String>, level: LogLevel) {
        let mut component_levels = self.component_levels.write();
        component_levels.insert(component.into(), level);
    }

    /// Removes the component-specific level (falls back to default)
    pub fn clear_component_level(&self, component: &str) {
        let mut component_levels = self.component_levels.write();
        component_levels.remove(component);
    }

    /// Sets the default log level
    pub fn set_default_level(&self, level: LogLevel) {
        *self.default_level.write() = level;
    }

    /// Gets all component-specific levels
    pub fn get_component_levels(&self) -> HashMap<String, LogLevel> {
        self.component_levels.read().clone()
    }

    /// Checks if a log event should be recorded
    pub fn should_log(&self, component: &str, level: LogLevel, event_name: Option<&str>) -> bool {
        let component_level = self.get_level(component);
        if level < component_level {
            return false;
        }

        // Check sampling
        if let Some(name) = event_name {
            self.sampler.should_log(name, level)
        } else {
            true
        }
    }

    /// Returns the sampler for direct access
    pub fn sampler(&self) -> &LogSampler {
        &self.sampler
    }
}

impl Clone for LogFilter {
    fn clone(&self) -> Self {
        Self {
            default_level: RwLock::new(*self.default_level.read()),
            component_levels: RwLock::new(self.component_levels.read().clone()),
            sampler: self.sampler.clone(),
        }
    }
}

/// Log aggregator for collecting logs from multiple sources
#[derive(Debug)]
pub struct LogAggregator {
    /// Buffer for collected log entries
    entries: RwLock<Vec<StructuredLogEntry>>,
    /// Maximum entries to buffer
    max_entries: usize,
    /// Metrics about aggregation
    metrics: LogAggregatorMetrics,
}

#[derive(Debug, Default)]
struct LogAggregatorMetrics {
    total_received: AtomicU64,
    total_dropped: AtomicU64,
    total_flushed: AtomicU64,
}

impl LogAggregator {
    /// Creates a new log aggregator with the specified buffer size
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::with_capacity(max_entries)),
            max_entries,
            metrics: LogAggregatorMetrics::default(),
        }
    }

    /// Adds a log entry to the aggregator
    pub fn add(&self, entry: StructuredLogEntry) -> bool {
        self.metrics.total_received.fetch_add(1, Ordering::Relaxed);

        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            self.metrics.total_dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        entries.push(entry);
        true
    }

    /// Flushes and returns all buffered entries
    pub fn flush(&self) -> Vec<StructuredLogEntry> {
        let mut entries = self.entries.write();
        let flushed = std::mem::take(&mut *entries);
        self.metrics
            .total_flushed
            .fetch_add(flushed.len() as u64, Ordering::Relaxed);
        flushed
    }

    /// Returns the number of buffered entries
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns true if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Returns aggregation metrics
    pub fn metrics(&self) -> AggregatorMetrics {
        AggregatorMetrics {
            total_received: self.metrics.total_received.load(Ordering::Relaxed),
            total_dropped: self.metrics.total_dropped.load(Ordering::Relaxed),
            total_flushed: self.metrics.total_flushed.load(Ordering::Relaxed),
            current_buffer_size: self.len(),
            max_buffer_size: self.max_entries,
        }
    }

    /// Filters entries by level
    pub fn filter_by_level(&self, min_level: LogLevel) -> Vec<StructuredLogEntry> {
        self.entries
            .read()
            .iter()
            .filter(|e| e.level >= min_level)
            .cloned()
            .collect()
    }

    /// Filters entries by component
    pub fn filter_by_component(&self, component: &str) -> Vec<StructuredLogEntry> {
        self.entries
            .read()
            .iter()
            .filter(|e| e.component.as_deref() == Some(component))
            .cloned()
            .collect()
    }

    /// Filters entries by trace ID
    pub fn filter_by_trace(&self, trace_id: &str) -> Vec<StructuredLogEntry> {
        self.entries
            .read()
            .iter()
            .filter(|e| e.trace_id.as_deref() == Some(trace_id))
            .cloned()
            .collect()
    }

    /// Filters entries by time range
    pub fn filter_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<StructuredLogEntry> {
        self.entries
            .read()
            .iter()
            .filter(|e| e.timestamp >= start && e.timestamp <= end)
            .cloned()
            .collect()
    }

    /// Groups entries by component
    pub fn group_by_component(&self) -> HashMap<String, Vec<StructuredLogEntry>> {
        let mut groups: HashMap<String, Vec<StructuredLogEntry>> = HashMap::new();
        for entry in self.entries.read().iter() {
            let key = entry
                .component
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            groups.entry(key).or_default().push(entry.clone());
        }
        groups
    }

    /// Gets log level distribution
    pub fn level_distribution(&self) -> HashMap<LogLevel, usize> {
        let mut dist = HashMap::new();
        for entry in self.entries.read().iter() {
            *dist.entry(entry.level).or_insert(0) += 1;
        }
        dist
    }
}

/// Metrics from the log aggregator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatorMetrics {
    /// Total entries received
    pub total_received: u64,
    /// Total entries dropped due to buffer overflow
    pub total_dropped: u64,
    /// Total entries flushed
    pub total_flushed: u64,
    /// Current buffer size
    pub current_buffer_size: usize,
    /// Maximum buffer size
    pub max_buffer_size: usize,
}

/// Global log filter instance
static LOG_FILTER: once_cell::sync::OnceCell<Arc<LogFilter>> = once_cell::sync::OnceCell::new();

/// Gets the global log filter
pub fn get_log_filter() -> Option<Arc<LogFilter>> {
    LOG_FILTER.get().cloned()
}

/// Sets the global log filter
pub fn set_log_filter(filter: Arc<LogFilter>) -> Result<(), Arc<LogFilter>> {
    LOG_FILTER.set(filter)
}

/// Custom JSON formatter layer for tracing
pub struct JsonLayer<W>
where
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    writer: W,
    include_source_location: bool,
    include_thread_id: bool,
    default_component: Option<String>,
    default_node_id: Option<String>,
}

impl<W> JsonLayer<W>
where
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    /// Creates a new JSON layer
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            include_source_location: true,
            include_thread_id: false,
            default_component: None,
            default_node_id: None,
        }
    }

    /// Sets whether to include source location
    pub fn with_source_location(mut self, include: bool) -> Self {
        self.include_source_location = include;
        self
    }

    /// Sets whether to include thread ID
    pub fn with_thread_id(mut self, include: bool) -> Self {
        self.include_thread_id = include;
        self
    }

    /// Sets default component name
    pub fn with_component(mut self, component: impl Into<String>) -> Self {
        self.default_component = Some(component.into());
        self
    }

    /// Sets default node ID
    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.default_node_id = Some(node_id.into());
        self
    }
}

impl<S, W> Layer<S> for JsonLayer<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        use std::io::Write;

        let mut entry =
            StructuredLogEntry::new((*event.metadata().level()).into(), String::new());

        entry.target = Some(event.metadata().target().to_string());

        if self.include_source_location {
            entry.file = event.metadata().file().map(|s| s.to_string());
            entry.line = event.metadata().line();
        }

        if self.include_thread_id {
            entry.thread_id = Some(format!("{:?}", std::thread::current().id()));
            entry.thread_name = std::thread::current().name().map(|s| s.to_string());
        }

        entry.component = self.default_component.clone();
        entry.node_id = self.default_node_id.clone();

        // Extract fields from the event
        struct FieldVisitor<'a> {
            entry: &'a mut StructuredLogEntry,
        }

        impl<'a> tracing::field::Visit for FieldVisitor<'a> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let value_str = format!("{:?}", value);
                match field.name() {
                    "message" => self.entry.message = value_str,
                    "trace_id" => self.entry.trace_id = Some(value_str),
                    "span_id" => self.entry.span_id = Some(value_str),
                    "component" => self.entry.component = Some(value_str),
                    "node_id" => self.entry.node_id = Some(value_str),
                    name => {
                        self.entry
                            .fields
                            .insert(name.to_string(), serde_json::Value::String(value_str));
                    }
                }
            }

            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                match field.name() {
                    "message" => self.entry.message = value.to_string(),
                    "trace_id" => self.entry.trace_id = Some(value.to_string()),
                    "span_id" => self.entry.span_id = Some(value.to_string()),
                    "component" => self.entry.component = Some(value.to_string()),
                    "node_id" => self.entry.node_id = Some(value.to_string()),
                    name => {
                        self.entry.fields.insert(
                            name.to_string(),
                            serde_json::Value::String(value.to_string()),
                        );
                    }
                }
            }

            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                self.entry.fields.insert(
                    field.name().to_string(),
                    serde_json::Value::Number(value.into()),
                );
            }

            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                self.entry.fields.insert(
                    field.name().to_string(),
                    serde_json::Value::Number(value.into()),
                );
            }

            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                self.entry
                    .fields
                    .insert(field.name().to_string(), serde_json::Value::Bool(value));
            }

            fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
                if let Some(n) = serde_json::Number::from_f64(value) {
                    self.entry
                        .fields
                        .insert(field.name().to_string(), serde_json::Value::Number(n));
                }
            }
        }

        let mut visitor = FieldVisitor { entry: &mut entry };
        event.record(&mut visitor);

        // Write the JSON entry
        if let Ok(json) = serde_json::to_string(&entry) {
            let mut writer = self.writer.make_writer();
            let _ = writeln!(writer, "{}", json);
        }
    }
}

/// Initializes the logging system with the given configuration
pub fn init_logging(config: &LogConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = Arc::new(LogFilter::with_config(config));
    let _ = set_log_filter(filter);

    // Build the env filter
    let mut filter_str = config.default_level.to_string();
    for (component, level) in &config.component_levels {
        filter_str.push_str(&format!(",{}={}", component, level));
    }

    let env_filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new(&filter_str))?;

    if config.json_format {
        // Use custom JSON layer
        let json_layer = JsonLayer::new(std::io::stdout)
            .with_source_location(config.include_source_location)
            .with_thread_id(config.include_thread_id);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(json_layer)
            .try_init()?;
    } else {
        // Use standard fmt layer
        let fmt_layer = fmt::layer()
            .with_target(true)
            .with_thread_ids(config.include_thread_id)
            .with_file(config.include_source_location)
            .with_line_number(config.include_source_location)
            .with_span_events(if config.include_span_events {
                FmtSpan::FULL
            } else {
                FmtSpan::NONE
            });

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()?;
    }

    Ok(())
}

/// Initializes logging with sensible defaults for development
pub fn init_dev_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut config = LogConfig::default();
    config.json_format = false;
    config.default_level = LogLevel::Debug;
    init_logging(&config)
}

/// Initializes logging with sensible defaults for production
pub fn init_prod_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = LogConfig::default();
    init_logging(&config)
}

/// Helper macro for structured logging with context
#[macro_export]
macro_rules! log_with_context {
    ($ctx:expr, $level:ident, $($arg:tt)*) => {
        tracing::$level!(
            trace_id = %$ctx.trace_id(),
            span_id = %$ctx.span_id(),
            component = %$ctx.component,
            node_id = %$ctx.node_id,
            $($arg)*
        )
    };
}

/// Helper macro for info level logging with context
#[macro_export]
macro_rules! info_ctx {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::log_with_context!($ctx, info, $($arg)*)
    };
}

/// Helper macro for debug level logging with context
#[macro_export]
macro_rules! debug_ctx {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::log_with_context!($ctx, debug, $($arg)*)
    };
}

/// Helper macro for warn level logging with context
#[macro_export]
macro_rules! warn_ctx {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::log_with_context!($ctx, warn, $($arg)*)
    };
}

/// Helper macro for error level logging with context
#[macro_export]
macro_rules! error_ctx {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::log_with_context!($ctx, error, $($arg)*)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!("trace".parse::<LogLevel>().unwrap(), LogLevel::Trace);
        assert_eq!("debug".parse::<LogLevel>().unwrap(), LogLevel::Debug);
        assert_eq!("info".parse::<LogLevel>().unwrap(), LogLevel::Info);
        assert_eq!("warn".parse::<LogLevel>().unwrap(), LogLevel::Warn);
        assert_eq!("warning".parse::<LogLevel>().unwrap(), LogLevel::Warn);
        assert_eq!("error".parse::<LogLevel>().unwrap(), LogLevel::Error);
        assert!("invalid".parse::<LogLevel>().is_err());
    }

    #[test]
    fn test_trace_context_creation() {
        let ctx = TraceContext::new();
        assert!(ctx.parent_span_id.is_none());

        let child = ctx.child_span();
        assert_eq!(child.trace_id, ctx.trace_id);
        assert_ne!(child.span_id, ctx.span_id);
        assert_eq!(child.parent_span_id, Some(ctx.span_id));
    }

    #[test]
    fn test_log_context_creation() {
        let ctx = LogContext::new("worker", "node-123");
        assert_eq!(ctx.component, "worker");
        assert_eq!(ctx.node_id, "node-123");
        assert!(ctx.metadata.is_empty());
    }

    #[test]
    fn test_log_context_with_metadata() {
        let ctx = LogContext::new("master", "node-456")
            .with_metadata("job_id", serde_json::json!("job-789"))
            .with_metadata("priority", serde_json::json!(5));

        assert_eq!(ctx.metadata.len(), 2);
        assert_eq!(
            ctx.metadata.get("job_id"),
            Some(&serde_json::json!("job-789"))
        );
        assert_eq!(ctx.metadata.get("priority"), Some(&serde_json::json!(5)));
    }

    #[test]
    fn test_log_context_child() {
        let parent =
            LogContext::new("worker", "node-123").with_metadata("key", serde_json::json!("value"));

        let child = parent.child();

        assert_eq!(child.component, parent.component);
        assert_eq!(child.node_id, parent.node_id);
        assert_eq!(child.trace.trace_id, parent.trace.trace_id);
        assert_ne!(child.trace.span_id, parent.trace.span_id);
        assert_eq!(child.trace.parent_span_id, Some(parent.trace.span_id));
        assert_eq!(child.metadata, parent.metadata);
    }

    #[test]
    fn test_structured_log_entry() {
        let entry = StructuredLogEntry::new(LogLevel::Info, "Test message")
            .with_field("key", serde_json::json!("value"));

        assert_eq!(entry.level, LogLevel::Info);
        assert_eq!(entry.message, "Test message");
        assert_eq!(entry.fields.get("key"), Some(&serde_json::json!("value")));
    }

    #[test]
    fn test_structured_log_entry_with_context() {
        let ctx = LogContext::new("worker", "node-123");
        let entry = StructuredLogEntry::new(LogLevel::Info, "Test message").with_context(&ctx);

        assert!(entry.trace_id.is_some());
        assert!(entry.span_id.is_some());
        assert_eq!(entry.component, Some("worker".to_string()));
        assert_eq!(entry.node_id, Some("node-123".to_string()));
    }

    #[test]
    fn test_structured_log_entry_json() {
        let entry = StructuredLogEntry::new(LogLevel::Info, "Test message");
        let json = entry.to_json().unwrap();

        assert!(json.contains("\"level\":\"info\""));
        assert!(json.contains("\"message\":\"Test message\""));
        assert!(json.contains("\"timestamp\":"));
    }

    #[test]
    fn test_log_sampler_always_log() {
        let config = SamplingConfig {
            enabled: false,
            ..Default::default()
        };
        let sampler = LogSampler::new(config);

        // Should always log when sampling is disabled
        for _ in 0..100 {
            assert!(sampler.should_log("test_event", LogLevel::Debug));
        }
    }

    #[test]
    fn test_log_sampler_rate_sampling() {
        let mut config = SamplingConfig::default();
        config.enabled = true;
        config.event_rates.insert("rare_event".to_string(), 0.1); // 10% sampling

        let sampler = LogSampler::new(config);

        let mut logged = 0;
        for _ in 0..100 {
            if sampler.should_log("rare_event", LogLevel::Info) {
                logged += 1;
            }
        }

        // Should log approximately 10 times (with some variance)
        assert!(logged > 0 && logged <= 20);
    }

    #[test]
    fn test_log_sampler_zero_rate() {
        let mut config = SamplingConfig::default();
        config.enabled = true;
        config.event_rates.insert("never_log".to_string(), 0.0);

        let sampler = LogSampler::new(config);

        for _ in 0..100 {
            assert!(!sampler.should_log("never_log", LogLevel::Info));
        }
    }

    #[test]
    fn test_log_sampler_counter_tracking() {
        let mut config = SamplingConfig::default();
        config.enabled = true;
        config.default_rate = 1.0;

        let sampler = LogSampler::new(config);

        for _ in 0..10 {
            sampler.should_log("tracked_event", LogLevel::Info);
        }

        assert_eq!(sampler.get_count("tracked_event"), 10);
    }

    #[test]
    fn test_log_filter_default_level() {
        let filter = LogFilter::new(LogLevel::Info);
        assert_eq!(filter.get_level("any_component"), LogLevel::Info);
    }

    #[test]
    fn test_log_filter_component_level() {
        let filter = LogFilter::new(LogLevel::Info);
        filter.set_component_level("worker", LogLevel::Debug);

        assert_eq!(filter.get_level("worker"), LogLevel::Debug);
        assert_eq!(filter.get_level("master"), LogLevel::Info);
    }

    #[test]
    fn test_log_filter_clear_component_level() {
        let filter = LogFilter::new(LogLevel::Info);
        filter.set_component_level("worker", LogLevel::Debug);
        filter.clear_component_level("worker");

        assert_eq!(filter.get_level("worker"), LogLevel::Info);
    }

    #[test]
    fn test_log_filter_set_default_level() {
        let filter = LogFilter::new(LogLevel::Info);
        filter.set_default_level(LogLevel::Warn);

        assert_eq!(filter.get_level("any"), LogLevel::Warn);
    }

    #[test]
    fn test_log_filter_should_log() {
        let filter = LogFilter::new(LogLevel::Info);

        assert!(filter.should_log("component", LogLevel::Info, None));
        assert!(filter.should_log("component", LogLevel::Warn, None));
        assert!(filter.should_log("component", LogLevel::Error, None));
        assert!(!filter.should_log("component", LogLevel::Debug, None));
        assert!(!filter.should_log("component", LogLevel::Trace, None));
    }

    #[test]
    fn test_log_filter_with_config() {
        let mut config = LogConfig::default();
        config.default_level = LogLevel::Warn;
        config
            .component_levels
            .insert("debug_component".to_string(), LogLevel::Debug);

        let filter = LogFilter::with_config(&config);

        assert_eq!(filter.get_level("normal"), LogLevel::Warn);
        assert_eq!(filter.get_level("debug_component"), LogLevel::Debug);
    }

    #[test]
    fn test_log_aggregator_add_and_flush() {
        let aggregator = LogAggregator::new(100);

        let entry1 = StructuredLogEntry::new(LogLevel::Info, "Message 1");
        let entry2 = StructuredLogEntry::new(LogLevel::Warn, "Message 2");

        assert!(aggregator.add(entry1));
        assert!(aggregator.add(entry2));
        assert_eq!(aggregator.len(), 2);

        let flushed = aggregator.flush();
        assert_eq!(flushed.len(), 2);
        assert!(aggregator.is_empty());
    }

    #[test]
    fn test_log_aggregator_buffer_overflow() {
        let aggregator = LogAggregator::new(2);

        let entry1 = StructuredLogEntry::new(LogLevel::Info, "Message 1");
        let entry2 = StructuredLogEntry::new(LogLevel::Info, "Message 2");
        let entry3 = StructuredLogEntry::new(LogLevel::Info, "Message 3");

        assert!(aggregator.add(entry1));
        assert!(aggregator.add(entry2));
        assert!(!aggregator.add(entry3)); // Should fail, buffer full

        let metrics = aggregator.metrics();
        assert_eq!(metrics.total_received, 3);
        assert_eq!(metrics.total_dropped, 1);
    }

    #[test]
    fn test_log_aggregator_filter_by_level() {
        let aggregator = LogAggregator::new(100);

        aggregator.add(StructuredLogEntry::new(LogLevel::Debug, "Debug"));
        aggregator.add(StructuredLogEntry::new(LogLevel::Info, "Info"));
        aggregator.add(StructuredLogEntry::new(LogLevel::Warn, "Warn"));
        aggregator.add(StructuredLogEntry::new(LogLevel::Error, "Error"));

        let warnings_and_above = aggregator.filter_by_level(LogLevel::Warn);
        assert_eq!(warnings_and_above.len(), 2);
    }

    #[test]
    fn test_log_aggregator_filter_by_component() {
        let aggregator = LogAggregator::new(100);

        let mut entry1 = StructuredLogEntry::new(LogLevel::Info, "Worker message");
        entry1.component = Some("worker".to_string());

        let mut entry2 = StructuredLogEntry::new(LogLevel::Info, "Master message");
        entry2.component = Some("master".to_string());

        aggregator.add(entry1);
        aggregator.add(entry2);

        let worker_logs = aggregator.filter_by_component("worker");
        assert_eq!(worker_logs.len(), 1);
        assert_eq!(worker_logs[0].message, "Worker message");
    }

    #[test]
    fn test_log_aggregator_filter_by_trace() {
        let aggregator = LogAggregator::new(100);
        let trace_id = "trace-123";

        let mut entry1 = StructuredLogEntry::new(LogLevel::Info, "Traced");
        entry1.trace_id = Some(trace_id.to_string());

        let entry2 = StructuredLogEntry::new(LogLevel::Info, "Not traced");

        aggregator.add(entry1);
        aggregator.add(entry2);

        let traced_logs = aggregator.filter_by_trace(trace_id);
        assert_eq!(traced_logs.len(), 1);
    }

    #[test]
    fn test_log_aggregator_group_by_component() {
        let aggregator = LogAggregator::new(100);

        let mut entry1 = StructuredLogEntry::new(LogLevel::Info, "Worker 1");
        entry1.component = Some("worker".to_string());

        let mut entry2 = StructuredLogEntry::new(LogLevel::Info, "Worker 2");
        entry2.component = Some("worker".to_string());

        let mut entry3 = StructuredLogEntry::new(LogLevel::Info, "Master");
        entry3.component = Some("master".to_string());

        aggregator.add(entry1);
        aggregator.add(entry2);
        aggregator.add(entry3);

        let groups = aggregator.group_by_component();
        assert_eq!(groups.get("worker").map(|v| v.len()), Some(2));
        assert_eq!(groups.get("master").map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_log_aggregator_level_distribution() {
        let aggregator = LogAggregator::new(100);

        aggregator.add(StructuredLogEntry::new(LogLevel::Info, "Info 1"));
        aggregator.add(StructuredLogEntry::new(LogLevel::Info, "Info 2"));
        aggregator.add(StructuredLogEntry::new(LogLevel::Warn, "Warn"));
        aggregator.add(StructuredLogEntry::new(LogLevel::Error, "Error"));

        let dist = aggregator.level_distribution();
        assert_eq!(dist.get(&LogLevel::Info), Some(&2));
        assert_eq!(dist.get(&LogLevel::Warn), Some(&1));
        assert_eq!(dist.get(&LogLevel::Error), Some(&1));
    }

    #[test]
    fn test_trace_id_display() {
        let trace_id = TraceId::new();
        let display = format!("{}", trace_id);
        assert!(!display.is_empty());
        assert!(display.contains('-')); // UUID format
    }

    #[test]
    fn test_span_id_display() {
        let span_id = SpanId::new();
        let display = format!("{}", span_id);
        assert_eq!(display.len(), 16); // 16 hex chars
    }

    #[test]
    fn test_log_config_default() {
        let config = LogConfig::default();
        assert_eq!(config.default_level, LogLevel::Info);
        assert!(config.json_format);
        assert!(config.include_source_location);
        assert!(!config.include_thread_id);
        assert!(!config.include_span_events);
    }

    #[test]
    fn test_sampling_config_default() {
        let config = SamplingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.default_rate, 1.0);
        assert_eq!(config.debug_sample_rate, 10);
        assert_eq!(config.trace_sample_rate, 100);
    }

    #[test]
    fn test_log_output_config_default() {
        let config = LogOutputConfig::default();
        assert!(config.stdout);
        assert!(!config.stderr_for_errors);
        assert!(config.file_path.is_none());
        assert_eq!(config.max_file_size, 100 * 1024 * 1024);
        assert_eq!(config.max_files, 5);
    }

    #[test]
    fn test_aggregator_metrics() {
        let aggregator = LogAggregator::new(10);

        for i in 0..5 {
            aggregator.add(StructuredLogEntry::new(
                LogLevel::Info,
                format!("Message {}", i),
            ));
        }

        aggregator.flush();

        let metrics = aggregator.metrics();
        assert_eq!(metrics.total_received, 5);
        assert_eq!(metrics.total_dropped, 0);
        assert_eq!(metrics.total_flushed, 5);
        assert_eq!(metrics.current_buffer_size, 0);
        assert_eq!(metrics.max_buffer_size, 10);
    }

    #[test]
    fn test_log_level_conversion_to_tracing() {
        assert_eq!(Level::from(LogLevel::Trace), Level::TRACE);
        assert_eq!(Level::from(LogLevel::Debug), Level::DEBUG);
        assert_eq!(Level::from(LogLevel::Info), Level::INFO);
        assert_eq!(Level::from(LogLevel::Warn), Level::WARN);
        assert_eq!(Level::from(LogLevel::Error), Level::ERROR);
    }

    #[test]
    fn test_log_level_conversion_from_tracing() {
        assert_eq!(LogLevel::from(Level::TRACE), LogLevel::Trace);
        assert_eq!(LogLevel::from(Level::DEBUG), LogLevel::Debug);
        assert_eq!(LogLevel::from(Level::INFO), LogLevel::Info);
        assert_eq!(LogLevel::from(Level::WARN), LogLevel::Warn);
        assert_eq!(LogLevel::from(Level::ERROR), LogLevel::Error);
    }

    #[test]
    fn test_trace_context_from_ids() {
        let trace_id = TraceId::new();
        let span_id = SpanId::new();

        let ctx = TraceContext::from_ids(trace_id, span_id);
        assert_eq!(ctx.trace_id, trace_id);
        assert_eq!(ctx.span_id, span_id);
        assert!(ctx.parent_span_id.is_none());
    }

    #[test]
    fn test_log_sampler_debug_level_sampling() {
        let mut config = SamplingConfig::default();
        config.enabled = true;
        config.debug_sample_rate = 10; // Sample 1 in 10

        let sampler = LogSampler::new(config);

        let mut logged = 0;
        for _ in 0..100 {
            if sampler.should_log("debug_event", LogLevel::Debug) {
                logged += 1;
            }
        }

        // Should log approximately 10 times
        assert!(logged >= 5 && logged <= 15);
    }

    #[test]
    fn test_log_sampler_trace_level_sampling() {
        let mut config = SamplingConfig::default();
        config.enabled = true;
        config.trace_sample_rate = 100; // Sample 1 in 100

        let sampler = LogSampler::new(config);

        let mut logged = 0;
        for _ in 0..1000 {
            if sampler.should_log("trace_event", LogLevel::Trace) {
                logged += 1;
            }
        }

        // Should log approximately 10 times
        assert!(logged >= 5 && logged <= 20);
    }

    #[test]
    fn test_log_sampler_reset_counters() {
        let mut config = SamplingConfig::default();
        config.enabled = true;
        let sampler = LogSampler::new(config);

        for _ in 0..10 {
            sampler.should_log("event", LogLevel::Info);
        }
        assert_eq!(sampler.get_count("event"), 10);

        sampler.reset_counters();
        assert_eq!(sampler.get_count("event"), 0);
    }

    #[test]
    fn test_log_filter_get_component_levels() {
        let filter = LogFilter::new(LogLevel::Info);
        filter.set_component_level("worker", LogLevel::Debug);
        filter.set_component_level("master", LogLevel::Warn);

        let levels = filter.get_component_levels();
        assert_eq!(levels.get("worker"), Some(&LogLevel::Debug));
        assert_eq!(levels.get("master"), Some(&LogLevel::Warn));
    }

    #[test]
    fn test_log_aggregator_filter_by_time_range() {
        let aggregator = LogAggregator::new(100);

        let now = Utc::now();
        let past = now - chrono::Duration::hours(2);
        let future = now + chrono::Duration::hours(2);

        let mut old_entry = StructuredLogEntry::new(LogLevel::Info, "Old");
        old_entry.timestamp = past;

        let current_entry = StructuredLogEntry::new(LogLevel::Info, "Current");

        aggregator.add(old_entry);
        aggregator.add(current_entry);

        let in_range = aggregator.filter_by_time_range(now - chrono::Duration::minutes(5), future);
        assert_eq!(in_range.len(), 1);
        assert_eq!(in_range[0].message, "Current");
    }
}
