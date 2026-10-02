// Marabunta - Licensed under the MIT License.
//! Trace Context for distributed tracing
//!
//! Implements W3C Trace Context specification for trace propagation.

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fmt;
use std::future::Future;

use super::{bytes_to_hex, generate_span_id, generate_trace_id, hex_to_bytes};

/// W3C Trace Context version
const TRACE_CONTEXT_VERSION: u8 = 0x00;

/// Task-local storage for trace context propagation
tokio::task_local! {
    static CURRENT_TRACE_CONTEXT: RefCell<Option<TraceContext>>;
}

/// Get the current trace context from task-local storage
pub fn current_trace_context() -> Option<TraceContext> {
    CURRENT_TRACE_CONTEXT
        .try_with(|ctx| ctx.borrow().clone())
        .ok()
        .flatten()
}

/// Execute a future with a trace context in task-local storage
pub async fn with_trace_context<F, T>(ctx: TraceContext, fut: F) -> T
where
    F: Future<Output = T>,
{
    CURRENT_TRACE_CONTEXT
        .scope(RefCell::new(Some(ctx)), fut)
        .await
}

/// Trace flags as defined by W3C Trace Context
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceFlags(u8);

impl TraceFlags {
    /// Create new trace flags from a byte
    pub fn new(flags: u8) -> Self {
        Self(flags)
    }

    /// Create sampled trace flags
    pub fn sampled() -> Self {
        Self(0x01)
    }

    /// Create unsampled trace flags
    pub fn not_sampled() -> Self {
        Self(0x00)
    }

    /// Check if the trace is sampled
    pub fn is_sampled(&self) -> bool {
        self.0 & 0x01 != 0
    }

    /// Get the raw byte value
    pub fn as_byte(&self) -> u8 {
        self.0
    }

    /// Set the sampled flag
    pub fn set_sampled(&mut self, sampled: bool) {
        if sampled {
            self.0 |= 0x01;
        } else {
            self.0 &= !0x01;
        }
    }
}

impl Default for TraceFlags {
    fn default() -> Self {
        Self::sampled()
    }
}

impl fmt::Display for TraceFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02x}", self.0)
    }
}

/// A single entry in the tracestate header
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceStateEntry {
    /// Vendor key
    pub key: String,
    /// Value
    pub value: String,
}

impl TraceStateEntry {
    /// Create a new trace state entry
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// W3C Tracestate header value
///
/// Contains vendor-specific trace information as key-value pairs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceState {
    entries: Vec<TraceStateEntry>,
}

impl TraceState {
    /// Maximum number of entries in tracestate
    const MAX_ENTRIES: usize = 32;

    /// Create a new empty trace state
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.key == key)
            .map(|e| e.value.as_str())
    }

    /// Set a value (adds or updates)
    ///
    /// New entries are added to the front (most recent first).
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();

        // Remove existing entry if present
        self.entries.retain(|e| e.key != key);

        // Add to front
        self.entries.insert(0, TraceStateEntry::new(key, value));

        // Trim to max entries
        self.entries.truncate(Self::MAX_ENTRIES);
    }

    /// Remove an entry by key
    pub fn remove(&mut self, key: &str) {
        self.entries.retain(|e| e.key != key);
    }

    /// Check if the trace state is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate over entries
    pub fn iter(&self) -> impl Iterator<Item = &TraceStateEntry> {
        self.entries.iter()
    }

    /// Convert to tracestate header value
    pub fn to_header(&self) -> String {
        self.entries
            .iter()
            .map(|e| format!("{}={}", e.key, e.value))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Parse from tracestate header value
    pub fn from_header(header: &str) -> Option<Self> {
        let mut entries = Vec::new();

        for part in header.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let mut split = part.splitn(2, '=');
            let key = split.next()?.trim().to_string();
            let value = split.next()?.trim().to_string();

            // Validate key format (simple validation)
            if key.is_empty() || value.is_empty() {
                continue;
            }

            entries.push(TraceStateEntry { key, value });

            if entries.len() >= Self::MAX_ENTRIES {
                break;
            }
        }

        Some(Self { entries })
    }
}

/// Distributed trace context
///
/// Contains trace identification and propagation information following
/// the W3C Trace Context specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContext {
    /// 128-bit trace ID
    trace_id: [u8; 16],
    /// 64-bit span ID
    span_id: [u8; 8],
    /// Optional 64-bit parent span ID
    parent_span_id: Option<[u8; 8]>,
    /// Trace flags
    flags: TraceFlags,
    /// Trace state (vendor-specific data)
    trace_state: TraceState,
}

impl TraceContext {
    /// Create a new root trace context
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            span_id: generate_span_id(),
            parent_span_id: None,
            flags: TraceFlags::sampled(),
            trace_state: TraceState::new(),
        }
    }

    /// Create a new trace context with specific IDs
    pub fn with_ids(trace_id: [u8; 16], span_id: [u8; 8]) -> Self {
        Self {
            trace_id,
            span_id,
            parent_span_id: None,
            flags: TraceFlags::sampled(),
            trace_state: TraceState::new(),
        }
    }

    /// Create a child context (new span in the same trace)
    pub fn new_child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: generate_span_id(),
            parent_span_id: Some(self.span_id),
            flags: self.flags,
            trace_state: self.trace_state.clone(),
        }
    }

    /// Get the trace ID
    pub fn trace_id(&self) -> &[u8; 16] {
        &self.trace_id
    }

    /// Get the trace ID as a hex string
    pub fn trace_id_hex(&self) -> String {
        bytes_to_hex(&self.trace_id)
    }

    /// Get the span ID
    pub fn span_id(&self) -> &[u8; 8] {
        &self.span_id
    }

    /// Get the span ID as a hex string
    pub fn span_id_hex(&self) -> String {
        bytes_to_hex(&self.span_id)
    }

    /// Get the parent span ID
    pub fn parent_span_id(&self) -> Option<&[u8; 8]> {
        self.parent_span_id.as_ref()
    }

    /// Get the parent span ID as a hex string
    pub fn parent_span_id_hex(&self) -> Option<String> {
        self.parent_span_id.map(|id| bytes_to_hex(&id))
    }

    /// Get the trace flags
    pub fn flags(&self) -> TraceFlags {
        self.flags
    }

    /// Set the trace flags
    pub fn set_flags(&mut self, flags: TraceFlags) {
        self.flags = flags;
    }

    /// Check if sampling is enabled
    pub fn is_sampled(&self) -> bool {
        self.flags.is_sampled()
    }

    /// Get the trace state
    pub fn trace_state(&self) -> &TraceState {
        &self.trace_state
    }

    /// Get mutable trace state
    pub fn trace_state_mut(&mut self) -> &mut TraceState {
        &mut self.trace_state
    }

    /// Convert to W3C traceparent header value
    ///
    /// Format: version-trace_id-parent_id-flags
    /// Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
    pub fn to_traceparent(&self) -> String {
        format!(
            "{:02x}-{}-{}-{:02x}",
            TRACE_CONTEXT_VERSION,
            bytes_to_hex(&self.trace_id),
            bytes_to_hex(&self.span_id),
            self.flags.as_byte()
        )
    }

    /// Parse from W3C traceparent header value
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let parts: Vec<&str> = header.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        // Parse version
        let version = u8::from_str_radix(parts[0], 16).ok()?;
        if version > TRACE_CONTEXT_VERSION {
            // Unknown version, but we might still be able to parse
            // For version 0, we require exact format
            if version == 0 && parts[1].len() != 32 {
                return None;
            }
        }

        // Parse trace_id (32 hex chars = 16 bytes)
        if parts[1].len() != 32 {
            return None;
        }
        let trace_id_bytes = hex_to_bytes(parts[1])?;
        if trace_id_bytes.len() != 16 {
            return None;
        }
        let trace_id: [u8; 16] = trace_id_bytes.try_into().ok()?;

        // Validate trace_id is not all zeros
        if trace_id == [0u8; 16] {
            return None;
        }

        // Parse span_id (16 hex chars = 8 bytes)
        if parts[2].len() != 16 {
            return None;
        }
        let span_id_bytes = hex_to_bytes(parts[2])?;
        if span_id_bytes.len() != 8 {
            return None;
        }
        let span_id: [u8; 8] = span_id_bytes.try_into().ok()?;

        // Validate span_id is not all zeros
        if span_id == [0u8; 8] {
            return None;
        }

        // Parse flags
        if parts[3].len() != 2 {
            return None;
        }
        let flags = u8::from_str_radix(parts[3], 16).ok()?;

        Some(Self {
            trace_id,
            span_id,
            parent_span_id: None,
            flags: TraceFlags::new(flags),
            trace_state: TraceState::new(),
        })
    }

    /// Convert to tracestate header value
    pub fn to_tracestate(&self) -> String {
        self.trace_state.to_header()
    }

    /// Parse tracestate header and add to this context
    pub fn with_tracestate(mut self, header: &str) -> Self {
        if let Some(state) = TraceState::from_header(header) {
            self.trace_state = state;
        }
        self
    }

    /// Create a context from traceparent with optional tracestate
    pub fn from_headers(traceparent: &str, tracestate: Option<&str>) -> Option<Self> {
        let mut ctx = Self::from_traceparent(traceparent)?;
        if let Some(state_header) = tracestate {
            ctx = ctx.with_tracestate(state_header);
        }
        Some(ctx)
    }

    /// Check if this context is valid (non-zero IDs)
    pub fn is_valid(&self) -> bool {
        self.trace_id != [0u8; 16] && self.span_id != [0u8; 8]
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TraceContext(trace_id={}, span_id={}, parent={})",
            self.trace_id_hex(),
            self.span_id_hex(),
            self.parent_span_id_hex().unwrap_or_else(|| "none".into())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_flags() {
        let mut flags = TraceFlags::new(0x00);
        assert!(!flags.is_sampled());

        flags.set_sampled(true);
        assert!(flags.is_sampled());
        assert_eq!(flags.as_byte(), 0x01);

        flags.set_sampled(false);
        assert!(!flags.is_sampled());
        assert_eq!(flags.as_byte(), 0x00);
    }

    #[test]
    fn test_trace_state_operations() {
        let mut state = TraceState::new();
        assert!(state.is_empty());

        state.set("key1", "value1");
        assert_eq!(state.len(), 1);
        assert_eq!(state.get("key1"), Some("value1"));

        state.set("key2", "value2");
        assert_eq!(state.len(), 2);

        // Update existing key
        state.set("key1", "updated");
        assert_eq!(state.len(), 2);
        assert_eq!(state.get("key1"), Some("updated"));

        // New key should be at front
        assert_eq!(state.entries[0].key, "key1");

        state.remove("key1");
        assert_eq!(state.len(), 1);
        assert_eq!(state.get("key1"), None);
    }

    #[test]
    fn test_trace_state_header_roundtrip() {
        let mut state = TraceState::new();
        state.set("vendor1", "value1");
        state.set("vendor2", "value2");

        let header = state.to_header();
        let parsed = TraceState::from_header(&header).unwrap();

        assert_eq!(parsed.get("vendor1"), Some("value1"));
        assert_eq!(parsed.get("vendor2"), Some("value2"));
    }

    #[test]
    fn test_trace_context_creation() {
        let ctx = TraceContext::new();

        assert!(ctx.is_valid());
        assert!(ctx.is_sampled());
        assert!(ctx.parent_span_id().is_none());
    }

    #[test]
    fn test_trace_context_child() {
        let parent = TraceContext::new();
        let child = parent.new_child();

        // Same trace
        assert_eq!(parent.trace_id(), child.trace_id());

        // Different span
        assert_ne!(parent.span_id(), child.span_id());

        // Child has parent
        assert_eq!(child.parent_span_id(), Some(parent.span_id()));
    }

    #[test]
    fn test_traceparent_format() {
        let ctx = TraceContext::new();
        let header = ctx.to_traceparent();

        // Format: 00-{32 hex}-{16 hex}-{2 hex}
        assert_eq!(header.len(), 55);
        assert!(header.starts_with("00-"));

        let parts: Vec<&str> = header.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], "00");
        assert_eq!(parts[1].len(), 32);
        assert_eq!(parts[2].len(), 16);
        assert_eq!(parts[3].len(), 2);
    }

    #[test]
    fn test_traceparent_roundtrip() {
        let original = TraceContext::new();
        let header = original.to_traceparent();
        let parsed = TraceContext::from_traceparent(&header).unwrap();

        assert_eq!(original.trace_id(), parsed.trace_id());
        assert_eq!(original.span_id(), parsed.span_id());
        assert_eq!(original.flags(), parsed.flags());
    }

    #[test]
    fn test_traceparent_parsing_invalid() {
        // Wrong number of parts
        assert!(TraceContext::from_traceparent("00-abc").is_none());

        // Invalid version format
        assert!(TraceContext::from_traceparent("xx-0-0-00").is_none());

        // Wrong trace_id length
        assert!(TraceContext::from_traceparent("00-abc-0000000000000000-00").is_none());

        // All-zero trace_id
        assert!(TraceContext::from_traceparent(
            "00-00000000000000000000000000000000-0000000000000001-00"
        )
        .is_none());

        // All-zero span_id
        assert!(TraceContext::from_traceparent(
            "00-00000000000000000000000000000001-0000000000000000-00"
        )
        .is_none());
    }

    #[test]
    fn test_context_with_tracestate() {
        let ctx = TraceContext::new().with_tracestate("vendor1=value1,vendor2=value2");

        assert_eq!(ctx.trace_state().get("vendor1"), Some("value1"));
        assert_eq!(ctx.trace_state().get("vendor2"), Some("value2"));
    }

    #[test]
    fn test_from_headers() {
        let original = TraceContext::new();
        let traceparent = original.to_traceparent();
        let tracestate = "marabunta=test123";

        let parsed = TraceContext::from_headers(&traceparent, Some(tracestate)).unwrap();

        assert_eq!(original.trace_id(), parsed.trace_id());
        assert_eq!(parsed.trace_state().get("marabunta"), Some("test123"));
    }

    #[test]
    fn test_display() {
        let ctx = TraceContext::new();
        let display = format!("{}", ctx);

        assert!(display.contains("TraceContext"));
        assert!(display.contains("trace_id="));
        assert!(display.contains("span_id="));
    }
}
