// Marabunta - Licensed under the MIT License.
//! Event-driven workflow trigger matching engine (W3C).
//!
//! Provides a `TriggerEngine` that registers `EventPattern` definitions and
//! efficiently matches incoming events against them. Supports exact event
//! type matching, wildcard patterns (e.g., `"node.*"`), field-level
//! predicates with AND logic, source node filtering, and correlation key
//! extraction.
//!
//! Patterns are prefix-indexed by their first event type segment so that
//! matching runs in O(k) time where k is the number of patterns sharing
//! the same prefix, rather than O(n) over all registered patterns.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// ============================================================================
// Types
// ============================================================================

/// An event pattern that defines when a workflow transition should be triggered.
///
/// Patterns are registered with the `TriggerEngine` and evaluated against
/// incoming events. All field predicates use AND logic: every predicate must
/// match for the pattern to fire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPattern {
    /// Event type to match. Supports exact match (e.g., `"document.updated"`)
    /// and wildcard suffix (e.g., `"node.*"` matches any event starting with `"node."`).
    pub event_type: String,
    /// Field-level predicates. Each key is a dot-notation path into the event
    /// payload, and the value is the expected JSON value at that path.
    /// All predicates must match (AND logic).
    #[serde(default)]
    pub field_predicates: HashMap<String, Value>,
    /// If set, the event's source node must match this value.
    #[serde(default)]
    pub source_node: Option<String>,
    /// ID of the workflow definition this pattern belongs to.
    pub workflow_id: String,
    /// Name of the transition to fire when the event matches.
    pub transition: String,
    /// If true, matching this pattern creates a new workflow instance
    /// rather than advancing an existing one.
    #[serde(default)]
    pub creates_instance: bool,
    /// If set, the named field is extracted from the event payload as
    /// a correlation key used to locate the correct workflow instance.
    #[serde(default)]
    pub correlation_key: Option<String>,
}

/// Result of matching an event against registered patterns.
#[derive(Debug, Clone)]
pub struct TriggerMatch {
    /// Workflow definition ID from the matched pattern.
    pub workflow_id: String,
    /// Transition name to fire.
    pub transition: String,
    /// Whether this match should create a new instance.
    pub creates_instance: bool,
    /// Correlation value extracted from the event payload, if a
    /// `correlation_key` was specified in the pattern.
    pub correlation_value: Option<String>,
    /// All top-level fields from the event payload, flattened into
    /// a string-keyed map for use as transition bindings.
    pub bindings: HashMap<String, Value>,
}

// ============================================================================
// TriggerEngine
// ============================================================================

/// Event-driven trigger matching engine.
///
/// Maintains a list of registered `EventPattern`s and a prefix index
/// for efficient lookup. When an event arrives, only patterns whose
/// prefix matches the event's first type segment are evaluated.
pub struct TriggerEngine {
    /// All registered patterns.
    patterns: Vec<EventPattern>,
    /// Maps the first segment of the event type to indices into `patterns`.
    prefix_index: HashMap<String, Vec<usize>>,
}

impl TriggerEngine {
    /// Create an empty trigger engine.
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            prefix_index: HashMap::new(),
        }
    }

    /// Register a new event pattern.
    ///
    /// The pattern is indexed by the first segment of its `event_type`
    /// (everything before the first `'.'`). Wildcard patterns like `"node.*"`
    /// are indexed under `"node"`.
    pub fn register_pattern(&mut self, pattern: EventPattern) {
        let idx = self.patterns.len();
        let prefix = first_segment(&pattern.event_type);
        self.prefix_index
            .entry(prefix)
            .or_default()
            .push(idx);
        self.patterns.push(pattern);
    }

    /// Match an incoming event against all registered patterns.
    ///
    /// Returns all `TriggerMatch` results where the pattern's type,
    /// source node filter, and field predicates all pass.
    pub fn match_event(
        &self,
        event_type: &str,
        payload: &Value,
        source_node: Option<&str>,
    ) -> Vec<TriggerMatch> {
        let prefix = first_segment(event_type);
        let candidate_indices = match self.prefix_index.get(&prefix) {
            Some(indices) => indices.as_slice(),
            None => return Vec::new(),
        };

        let mut matches = Vec::new();
        for &idx in candidate_indices {
            let pattern = &self.patterns[idx];
            if pattern_matches(pattern, event_type, payload, source_node) {
                let correlation_value = pattern
                    .correlation_key
                    .as_ref()
                    .and_then(|key| extract_field(payload, key))
                    .map(|v| value_to_string(&v));

                let bindings = extract_all_fields(payload);

                matches.push(TriggerMatch {
                    workflow_id: pattern.workflow_id.clone(),
                    transition: pattern.transition.clone(),
                    creates_instance: pattern.creates_instance,
                    correlation_value,
                    bindings,
                });
            }
        }
        matches
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Extract the first segment of a dotted event type string.
/// For `"node.joined"` returns `"node"`. For `"ping"` returns `"ping"`.
fn first_segment(event_type: &str) -> String {
    event_type
        .split('.')
        .next()
        .unwrap_or(event_type)
        .to_string()
}

/// Check whether a pattern's event type matches the given event type.
///
/// - Exact match: `"node.joined"` matches `"node.joined"`.
/// - Wildcard: `"node.*"` matches any event starting with `"node."`.
fn type_matches(pattern: &EventPattern, event_type: &str) -> bool {
    if pattern.event_type.ends_with(".*") {
        let prefix = &pattern.event_type[..pattern.event_type.len() - 2];
        event_type.starts_with(prefix)
            && event_type.len() > prefix.len()
            && event_type.as_bytes()[prefix.len()] == b'.'
    } else {
        pattern.event_type == event_type
    }
}

/// Full pattern match: type, source node, and all field predicates.
fn pattern_matches(
    pattern: &EventPattern,
    event_type: &str,
    payload: &Value,
    source_node: Option<&str>,
) -> bool {
    // 1. Type match
    if !type_matches(pattern, event_type) {
        return false;
    }

    // 2. Source node filter
    if let Some(ref required_source) = pattern.source_node {
        match source_node {
            Some(actual) if actual == required_source.as_str() => {}
            _ => return false,
        }
    }

    // 3. Field predicates (AND logic)
    for (path, expected) in &pattern.field_predicates {
        match extract_field(payload, path) {
            Some(actual) if actual == *expected => {}
            _ => return false,
        }
    }

    true
}

/// Extract a value from a JSON payload using dot-notation path traversal.
///
/// For path `"metadata.region"`, navigates `payload["metadata"]["region"]`.
fn extract_field(payload: &Value, path: &str) -> Option<Value> {
    let mut current = payload;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current.clone())
}

/// Convert a `serde_json::Value` to a string representation.
///
/// Strings are returned without surrounding quotes. Numbers and booleans
/// use their natural representation. Other types use JSON serialization.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Flatten all top-level fields from a JSON object into a `HashMap`.
///
/// Non-object payloads return an empty map.
fn extract_all_fields(payload: &Value) -> HashMap<String, Value> {
    match payload {
        Value::Object(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        _ => HashMap::new(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_pattern(event_type: &str, workflow_id: &str, transition: &str) -> EventPattern {
        EventPattern {
            event_type: event_type.to_string(),
            field_predicates: HashMap::new(),
            source_node: None,
            workflow_id: workflow_id.to_string(),
            transition: transition.to_string(),
            creates_instance: false,
            correlation_key: None,
        }
    }

    #[test]
    fn test_exact_event_type_match() {
        let mut engine = TriggerEngine::new();
        engine.register_pattern(make_pattern("document.updated", "wf1", "submit"));

        let payload = json!({"doc_id": "d-123"});
        let matches = engine.match_event("document.updated", &payload, None);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].workflow_id, "wf1");
        assert_eq!(matches[0].transition, "submit");
    }

    #[test]
    fn test_wildcard_event_type_match() {
        let mut engine = TriggerEngine::new();
        engine.register_pattern(make_pattern("node.*", "wf2", "handle_node_event"));

        let payload = json!({});
        let matches = engine.match_event("node.joined", &payload, None);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].transition, "handle_node_event");

        let matches2 = engine.match_event("node.left", &payload, None);
        assert_eq!(matches2.len(), 1);
    }

    #[test]
    fn test_wildcard_non_match() {
        let mut engine = TriggerEngine::new();
        engine.register_pattern(make_pattern("node.*", "wf2", "handle_node_event"));

        let payload = json!({});
        // "node" alone should not match "node.*" — must have a segment after the dot.
        let matches = engine.match_event("node", &payload, None);
        assert_eq!(matches.len(), 0);

        // Completely different prefix.
        let matches2 = engine.match_event("document.updated", &payload, None);
        assert_eq!(matches2.len(), 0);
    }

    #[test]
    fn test_field_predicate_match() {
        let mut engine = TriggerEngine::new();
        let mut pattern = make_pattern("order.placed", "wf3", "process");
        pattern
            .field_predicates
            .insert("region".to_string(), json!("us-east"));
        engine.register_pattern(pattern);

        let payload = json!({"region": "us-east", "amount": 42});
        let matches = engine.match_event("order.placed", &payload, None);
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_field_predicate_mismatch() {
        let mut engine = TriggerEngine::new();
        let mut pattern = make_pattern("order.placed", "wf3", "process");
        pattern
            .field_predicates
            .insert("region".to_string(), json!("us-east"));
        engine.register_pattern(pattern);

        let payload = json!({"region": "eu-west", "amount": 42});
        let matches = engine.match_event("order.placed", &payload, None);
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_source_node_filter() {
        let mut engine = TriggerEngine::new();
        let mut pattern = make_pattern("metric.reported", "wf4", "record");
        pattern.source_node = Some("node-alpha".to_string());
        engine.register_pattern(pattern);

        let payload = json!({"cpu": 0.85});

        // Matching source node.
        let matches = engine.match_event("metric.reported", &payload, Some("node-alpha"));
        assert_eq!(matches.len(), 1);

        // Wrong source node.
        let matches2 = engine.match_event("metric.reported", &payload, Some("node-beta"));
        assert_eq!(matches2.len(), 0);

        // No source node provided.
        let matches3 = engine.match_event("metric.reported", &payload, None);
        assert_eq!(matches3.len(), 0);
    }

    #[test]
    fn test_correlation_key_extraction() {
        let mut engine = TriggerEngine::new();
        let mut pattern = make_pattern("ticket.updated", "wf5", "review");
        pattern.correlation_key = Some("ticket_id".to_string());
        engine.register_pattern(pattern);

        let payload = json!({"ticket_id": "T-999", "status": "open"});
        let matches = engine.match_event("ticket.updated", &payload, None);
        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].correlation_value,
            Some("T-999".to_string())
        );
    }

    #[test]
    fn test_multiple_matches() {
        let mut engine = TriggerEngine::new();
        engine.register_pattern(make_pattern("task.completed", "wf-a", "finalize"));
        engine.register_pattern(make_pattern("task.completed", "wf-b", "audit"));
        engine.register_pattern(make_pattern("task.*", "wf-c", "log"));

        let payload = json!({"task_id": "t-1"});
        let matches = engine.match_event("task.completed", &payload, None);
        assert_eq!(matches.len(), 3);

        let workflow_ids: Vec<&str> = matches.iter().map(|m| m.workflow_id.as_str()).collect();
        assert!(workflow_ids.contains(&"wf-a"));
        assert!(workflow_ids.contains(&"wf-b"));
        assert!(workflow_ids.contains(&"wf-c"));
    }
}
