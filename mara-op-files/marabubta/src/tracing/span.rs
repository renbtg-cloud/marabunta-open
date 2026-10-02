// Marabunta - Licensed under the MIT License.
//! Span types for distributed tracing
//!
//! A Span represents a unit of work or operation within a trace.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::context::TraceContext;
use super::bytes_to_hex;

/// The kind of span (determines how the span relates to its parent)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[derive(Default)]
pub enum SpanKind {
    /// Default. Indicates that the span represents an internal operation.
    #[default]
    Internal,
    /// Indicates that the span covers server-side handling of a request.
    Server,
    /// Indicates that the span covers client-side sending of a request.
    Client,
    /// Indicates that the span describes a producer sending a message.
    Producer,
    /// Indicates that the span describes a consumer receiving a message.
    Consumer,
}


impl std::fmt::Display for SpanKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpanKind::Internal => write!(f, "internal"),
            SpanKind::Server => write!(f, "server"),
            SpanKind::Client => write!(f, "client"),
            SpanKind::Producer => write!(f, "producer"),
            SpanKind::Consumer => write!(f, "consumer"),
        }
    }
}

/// The status of a span
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", content = "message")]
#[derive(Default)]
pub enum SpanStatus {
    /// The default status, span has not been explicitly set.
    #[default]
    Unset,
    /// The operation completed successfully.
    Ok,
    /// The operation contained an error.
    Error(String),
}


impl SpanStatus {
    /// Check if the status represents an error
    pub fn is_error(&self) -> bool {
        matches!(self, SpanStatus::Error(_))
    }

    /// Check if the status is OK
    pub fn is_ok(&self) -> bool {
        matches!(self, SpanStatus::Ok)
    }

    /// Get the error message if this is an error status
    pub fn error_message(&self) -> Option<&str> {
        match self {
            SpanStatus::Error(msg) => Some(msg),
            _ => None,
        }
    }
}

/// Attribute value types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AttributeValue {
    /// String value
    String(String),
    /// Boolean value
    Bool(bool),
    /// Integer value
    Int(i64),
    /// Float value
    Float(f64),
    /// String array
    StringArray(Vec<String>),
    /// Integer array
    IntArray(Vec<i64>),
    /// Float array
    FloatArray(Vec<f64>),
    /// Boolean array
    BoolArray(Vec<bool>),
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        AttributeValue::String(s.to_string())
    }
}

impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(s)
    }
}

impl From<bool> for AttributeValue {
    fn from(b: bool) -> Self {
        AttributeValue::Bool(b)
    }
}

impl From<i64> for AttributeValue {
    fn from(i: i64) -> Self {
        AttributeValue::Int(i)
    }
}

impl From<i32> for AttributeValue {
    fn from(i: i32) -> Self {
        AttributeValue::Int(i as i64)
    }
}

impl From<f64> for AttributeValue {
    fn from(f: f64) -> Self {
        AttributeValue::Float(f)
    }
}

impl From<Vec<String>> for AttributeValue {
    fn from(v: Vec<String>) -> Self {
        AttributeValue::StringArray(v)
    }
}

/// A timed event within a span
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Name of the event
    name: String,
    /// Timestamp of the event
    timestamp: DateTime<Utc>,
    /// Event attributes
    attributes: HashMap<String, AttributeValue>,
}

impl SpanEvent {
    /// Create a new event
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            timestamp: Utc::now(),
            attributes: HashMap::new(),
        }
    }

    /// Create a new event with attributes
    pub fn with_attributes(
        name: impl Into<String>,
        attributes: Vec<(&str, AttributeValue)>,
    ) -> Self {
        let mut event = Self::new(name);
        for (key, value) in attributes {
            event.attributes.insert(key.to_string(), value);
        }
        event
    }

    /// Get the event name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the event timestamp
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Get the event attributes
    pub fn attributes(&self) -> &HashMap<String, AttributeValue> {
        &self.attributes
    }

    /// Add an attribute to the event
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<AttributeValue>) {
        self.attributes.insert(key.into(), value.into());
    }
}

/// A link to another span (for batch operations, fan-out, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLink {
    /// Trace context of the linked span
    trace_context: TraceContext,
    /// Link attributes
    attributes: HashMap<String, AttributeValue>,
}

impl SpanLink {
    /// Create a new link to another span
    pub fn new(trace_context: TraceContext) -> Self {
        Self {
            trace_context,
            attributes: HashMap::new(),
        }
    }

    /// Create a new link with attributes
    pub fn with_attributes(
        trace_context: TraceContext,
        attributes: Vec<(&str, AttributeValue)>,
    ) -> Self {
        let mut link = Self::new(trace_context);
        for (key, value) in attributes {
            link.attributes.insert(key.to_string(), value);
        }
        link
    }

    /// Get the linked trace context
    pub fn trace_context(&self) -> &TraceContext {
        &self.trace_context
    }

    /// Get the link attributes
    pub fn attributes(&self) -> &HashMap<String, AttributeValue> {
        &self.attributes
    }
}

/// Immutable span context for cross-cutting concerns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanContext {
    /// Trace ID
    trace_id: [u8; 16],
    /// Span ID
    span_id: [u8; 8],
    /// Whether the span is sampled
    is_sampled: bool,
    /// Whether this is a remote span
    is_remote: bool,
}

impl SpanContext {
    /// Create a span context from a trace context
    pub fn from_trace_context(ctx: &TraceContext) -> Self {
        Self {
            trace_id: *ctx.trace_id(),
            span_id: *ctx.span_id(),
            is_sampled: ctx.is_sampled(),
            is_remote: false,
        }
    }

    /// Create a remote span context
    pub fn remote(trace_id: [u8; 16], span_id: [u8; 8], is_sampled: bool) -> Self {
        Self {
            trace_id,
            span_id,
            is_sampled,
            is_remote: true,
        }
    }

    /// Get the trace ID
    pub fn trace_id(&self) -> &[u8; 16] {
        &self.trace_id
    }

    /// Get the span ID
    pub fn span_id(&self) -> &[u8; 8] {
        &self.span_id
    }

    /// Get the trace ID as hex
    pub fn trace_id_hex(&self) -> String {
        bytes_to_hex(&self.trace_id)
    }

    /// Get the span ID as hex
    pub fn span_id_hex(&self) -> String {
        bytes_to_hex(&self.span_id)
    }

    /// Check if the span is sampled
    pub fn is_sampled(&self) -> bool {
        self.is_sampled
    }

    /// Check if this is a remote span
    pub fn is_remote(&self) -> bool {
        self.is_remote
    }
}

/// A span representing a unit of work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Span {
    /// Span name/operation name
    name: String,
    /// Trace context
    trace_context: TraceContext,
    /// Span kind
    kind: SpanKind,
    /// Span status
    status: SpanStatus,
    /// Start time (wall clock)
    start_time: DateTime<Utc>,
    /// End time (wall clock)
    end_time: Option<DateTime<Utc>>,
    /// Start instant for duration calculation
    #[serde(skip)]
    start_instant: Option<Instant>,
    /// Span attributes
    attributes: HashMap<String, AttributeValue>,
    /// Span events
    events: Vec<SpanEvent>,
    /// Links to other spans
    links: Vec<SpanLink>,
}

impl Span {
    /// Create a new span with a new trace context
    pub fn new(name: impl Into<String>, kind: SpanKind) -> Self {
        Self::new_with_context(name, kind, TraceContext::new())
    }

    /// Create a new span with a specific trace context
    pub fn new_with_context(
        name: impl Into<String>,
        kind: SpanKind,
        trace_context: TraceContext,
    ) -> Self {
        Self {
            name: name.into(),
            trace_context,
            kind,
            status: SpanStatus::Unset,
            start_time: Utc::now(),
            end_time: None,
            start_instant: Some(Instant::now()),
            attributes: HashMap::new(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    /// Create a child span from a parent context
    pub fn child(name: impl Into<String>, kind: SpanKind, parent: &TraceContext) -> Self {
        Self::new_with_context(name, kind, parent.new_child())
    }

    /// Get the span name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set the span name
    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }

    /// Get the trace context
    pub fn trace_context(&self) -> &TraceContext {
        &self.trace_context
    }

    /// Get the span kind
    pub fn kind(&self) -> SpanKind {
        self.kind
    }

    /// Get the span status
    pub fn status(&self) -> SpanStatus {
        self.status.clone()
    }

    /// Set the span status
    pub fn set_status(&mut self, status: SpanStatus) {
        self.status = status;
    }

    /// Set the span to OK status
    pub fn set_ok(&mut self) {
        self.status = SpanStatus::Ok;
    }

    /// Set the span to error status
    pub fn set_error(&mut self, message: impl Into<String>) {
        self.status = SpanStatus::Error(message.into());
    }

    /// Get the start time
    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// Get the end time
    pub fn end_time(&self) -> Option<DateTime<Utc>> {
        self.end_time
    }

    /// Check if the span has ended
    pub fn is_ended(&self) -> bool {
        self.end_time.is_some()
    }

    /// Get the span duration (if ended)
    pub fn duration(&self) -> Option<Duration> {
        self.end_time.map(|end| {
            let start_millis = self.start_time.timestamp_millis();
            let end_millis = end.timestamp_millis();
            Duration::from_millis((end_millis - start_millis).max(0) as u64)
        })
    }

    /// Get the span duration in microseconds (if ended)
    pub fn duration_micros(&self) -> Option<i64> {
        self.end_time.map(|end| {
            let start_micros = self.start_time.timestamp_micros();
            let end_micros = end.timestamp_micros();
            (end_micros - start_micros).max(0)
        })
    }

    /// End the span
    pub fn end(&mut self) {
        if self.end_time.is_none() {
            self.end_time = Some(Utc::now());
        }
    }

    /// End the span with a specific time
    pub fn end_at(&mut self, time: DateTime<Utc>) {
        if self.end_time.is_none() {
            self.end_time = Some(time);
        }
    }

    /// Get span attributes
    pub fn attributes(&self) -> &HashMap<String, AttributeValue> {
        &self.attributes
    }

    /// Set a span attribute
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<AttributeValue>) {
        self.attributes.insert(key.into(), value.into());
    }

    /// Set multiple attributes
    pub fn set_attributes(&mut self, attrs: Vec<(&str, AttributeValue)>) {
        for (key, value) in attrs {
            self.attributes.insert(key.to_string(), value);
        }
    }

    /// Get an attribute value
    pub fn get_attribute(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }

    /// Get span events
    pub fn events(&self) -> &[SpanEvent] {
        &self.events
    }

    /// Add an event to the span
    pub fn add_event(&mut self, name: impl Into<String>) {
        self.events.push(SpanEvent::new(name));
    }

    /// Add an event with attributes
    pub fn add_event_with_attrs(
        &mut self,
        name: impl Into<String>,
        attributes: Vec<(&str, AttributeValue)>,
    ) {
        self.events
            .push(SpanEvent::with_attributes(name, attributes));
    }

    /// Get span links
    pub fn links(&self) -> &[SpanLink] {
        &self.links
    }

    /// Add a link to another span
    pub fn add_link(&mut self, link: SpanLink) {
        self.links.push(link);
    }

    /// Get the trace ID as hex
    pub fn trace_id_hex(&self) -> String {
        self.trace_context.trace_id_hex()
    }

    /// Get the span ID as hex
    pub fn span_id_hex(&self) -> String {
        self.trace_context.span_id_hex()
    }

    /// Get the parent span ID as hex
    pub fn parent_span_id_hex(&self) -> Option<String> {
        self.trace_context.parent_span_id_hex()
    }

    /// Check if this span is sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_context.is_sampled()
    }

    /// Create a span context from this span
    pub fn span_context(&self) -> SpanContext {
        SpanContext::from_trace_context(&self.trace_context)
    }

    /// Record an exception on the span
    pub fn record_exception(&mut self, error: &dyn std::error::Error) {
        self.set_error(error.to_string());
        self.add_event_with_attrs(
            "exception",
            vec![
                ("exception.type", error.to_string().into()),
                ("exception.message", error.to_string().into()),
            ],
        );
    }
}

/// Guard that ends a span when dropped
pub struct SpanGuard<'a> {
    span: &'a mut Span,
}

impl<'a> SpanGuard<'a> {
    /// Create a new span guard
    pub fn new(span: &'a mut Span) -> Self {
        Self { span }
    }

    /// Access the span
    pub fn span(&self) -> &Span {
        self.span
    }

    /// Access the span mutably
    pub fn span_mut(&mut self) -> &mut Span {
        self.span
    }
}

impl<'a> Drop for SpanGuard<'a> {
    fn drop(&mut self) {
        self.span.end();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_kind_default() {
        let kind: SpanKind = Default::default();
        assert_eq!(kind, SpanKind::Internal);
    }

    #[test]
    fn test_span_status() {
        let status = SpanStatus::Unset;
        assert!(!status.is_error());
        assert!(!status.is_ok());

        let status = SpanStatus::Ok;
        assert!(status.is_ok());
        assert!(!status.is_error());

        let status = SpanStatus::Error("test error".into());
        assert!(status.is_error());
        assert!(!status.is_ok());
        assert_eq!(status.error_message(), Some("test error"));
    }

    #[test]
    fn test_attribute_value_conversions() {
        let str_val: AttributeValue = "test".into();
        assert!(matches!(str_val, AttributeValue::String(_)));

        let bool_val: AttributeValue = true.into();
        assert!(matches!(bool_val, AttributeValue::Bool(true)));

        let int_val: AttributeValue = 42i64.into();
        assert!(matches!(int_val, AttributeValue::Int(42)));

        let float_val: AttributeValue = 3.14f64.into();
        assert!(matches!(float_val, AttributeValue::Float(_)));
    }

    #[test]
    fn test_span_event() {
        let mut event = SpanEvent::new("test_event");
        assert_eq!(event.name(), "test_event");
        assert!(event.attributes().is_empty());

        event.set_attribute("key", "value");
        assert_eq!(event.attributes().len(), 1);
    }

    #[test]
    fn test_span_event_with_attributes() {
        let event = SpanEvent::with_attributes(
            "test",
            vec![("key1", "value1".into()), ("key2", 42i64.into())],
        );

        assert_eq!(event.attributes().len(), 2);
    }

    #[test]
    fn test_span_link() {
        let ctx = TraceContext::new();
        let link = SpanLink::new(ctx.clone());

        assert_eq!(link.trace_context().trace_id(), ctx.trace_id());
        assert!(link.attributes().is_empty());
    }

    #[test]
    fn test_span_creation() {
        let span = Span::new("test_span", SpanKind::Server);

        assert_eq!(span.name(), "test_span");
        assert_eq!(span.kind(), SpanKind::Server);
        assert!(!span.is_ended());
        assert!(matches!(span.status(), SpanStatus::Unset));
    }

    #[test]
    fn test_span_with_context() {
        let ctx = TraceContext::new();
        let span = Span::new_with_context("test", SpanKind::Internal, ctx.clone());

        assert_eq!(span.trace_context().trace_id(), ctx.trace_id());
    }

    #[test]
    fn test_span_child() {
        let parent_ctx = TraceContext::new();
        let child = Span::child("child_span", SpanKind::Internal, &parent_ctx);

        assert_eq!(child.trace_context().trace_id(), parent_ctx.trace_id());
        assert_eq!(
            child.trace_context().parent_span_id(),
            Some(parent_ctx.span_id())
        );
    }

    #[test]
    fn test_span_attributes() {
        let mut span = Span::new("test", SpanKind::Internal);

        span.set_attribute("string", "value");
        span.set_attribute("number", 42i64);
        span.set_attribute("bool", true);

        assert_eq!(span.attributes().len(), 3);
        assert!(matches!(
            span.get_attribute("string"),
            Some(AttributeValue::String(_))
        ));
    }

    #[test]
    fn test_span_events() {
        let mut span = Span::new("test", SpanKind::Internal);

        span.add_event("event1");
        span.add_event_with_attrs("event2", vec![("key", "value".into())]);

        assert_eq!(span.events().len(), 2);
        assert_eq!(span.events()[0].name(), "event1");
        assert_eq!(span.events()[1].name(), "event2");
    }

    #[test]
    fn test_span_links() {
        let mut span = Span::new("test", SpanKind::Internal);
        let linked_ctx = TraceContext::new();

        span.add_link(SpanLink::new(linked_ctx.clone()));

        assert_eq!(span.links().len(), 1);
        assert_eq!(
            span.links()[0].trace_context().trace_id(),
            linked_ctx.trace_id()
        );
    }

    #[test]
    fn test_span_end() {
        let mut span = Span::new("test", SpanKind::Internal);
        assert!(!span.is_ended());

        span.end();
        assert!(span.is_ended());
        assert!(span.end_time().is_some());
        assert!(span.duration().is_some());

        // Ending again should not change the end time
        let end_time = span.end_time();
        span.end();
        assert_eq!(span.end_time(), end_time);
    }

    #[test]
    fn test_span_status_methods() {
        let mut span = Span::new("test", SpanKind::Internal);

        span.set_ok();
        assert!(matches!(span.status(), SpanStatus::Ok));

        span.set_error("something failed");
        assert!(matches!(span.status(), SpanStatus::Error(_)));
    }

    #[test]
    fn test_span_record_exception() {
        let mut span = Span::new("test", SpanKind::Internal);
        let error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");

        span.record_exception(&error);

        assert!(span.status().is_error());
        assert_eq!(span.events().len(), 1);
        assert_eq!(span.events()[0].name(), "exception");
    }

    #[test]
    fn test_span_guard() {
        let mut span = Span::new("test", SpanKind::Internal);
        assert!(!span.is_ended());

        {
            let _guard = SpanGuard::new(&mut span);
            // Span is still running
        }

        // Span should be ended after guard is dropped
        assert!(span.is_ended());
    }

    #[test]
    fn test_span_context() {
        let ctx = TraceContext::new();
        let span_ctx = SpanContext::from_trace_context(&ctx);

        assert_eq!(span_ctx.trace_id(), ctx.trace_id());
        assert_eq!(span_ctx.span_id(), ctx.span_id());
        assert_eq!(span_ctx.is_sampled(), ctx.is_sampled());
        assert!(!span_ctx.is_remote());
    }

    #[test]
    fn test_remote_span_context() {
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];
        let span_ctx = SpanContext::remote(trace_id, span_id, true);

        assert_eq!(span_ctx.trace_id(), &trace_id);
        assert_eq!(span_ctx.span_id(), &span_id);
        assert!(span_ctx.is_sampled());
        assert!(span_ctx.is_remote());
    }
}
