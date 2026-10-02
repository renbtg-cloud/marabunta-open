// Marabunta - Licensed under the MIT License.
//! Core types for the three-tier storage subsystem.
//!
//! Defines the foundational enums, structs, and error types that all
//! storage adapters, the router, and upper layers depend on.

use serde::{Deserialize, Serialize};

use super::StorageTier;

// ---------------------------------------------------------------------------
// HealthStatus
// ---------------------------------------------------------------------------

/// Health status of a storage tier or adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Tier responding normally, latency within bounds.
    Healthy,
    /// Tier responding but slow or partially available.
    Degraded,
    /// Tier not responding, operations will fail.
    Unavailable,
    /// Health check has not yet run or timed out.
    Unknown,
}

impl HealthStatus {
    /// Returns true if operations can be attempted against this tier.
    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "Healthy"),
            Self::Degraded => write!(f, "Degraded"),
            Self::Unavailable => write!(f, "Unavailable"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

// ---------------------------------------------------------------------------
// Value
// ---------------------------------------------------------------------------

/// Generic value type for SQL arguments and result rows.
/// Intentionally simple -- adapters convert to/from native types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(u64),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<u64> {
        match self {
            Self::Timestamp(t) => Some(*t),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Bool(b) => write!(f, "{}", b),
            Self::Int(i) => write!(f, "{}", i),
            Self::Float(v) => write!(f, "{}", v),
            Self::Text(s) => write!(f, "{}", s),
            Self::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Self::Timestamp(t) => write!(f, "ts:{}", t),
        }
    }
}

// ---------------------------------------------------------------------------
// AuditEvent (placeholder until W1A delivers the full type)
// ---------------------------------------------------------------------------

/// Placeholder for AuditEvent until W1A delivers the full type.
/// Fields chosen to match the minimum interface needed by SwarmStore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: String,
    pub timestamp: u64,
    pub actor_id: String,
    pub action: String,
    pub target: String,
    /// String placeholder for CriticalityLevel (will be replaced when W1A integrates).
    pub criticality: String,
    pub node_id: String,
    pub payload: Option<String>,
}

// ---------------------------------------------------------------------------
// EventFilter
// ---------------------------------------------------------------------------

/// Filter for querying audit events via `SwarmStore::query_events()`.
/// All fields are `Option` -- unset means "no filter on this dimension".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventFilter {
    /// Time range: (start_inclusive, end_exclusive). Both optional.
    pub time_range: Option<(Option<u64>, Option<u64>)>,

    /// Filter to events originating from these nodes.
    pub node_ids: Option<Vec<String>>,

    /// Filter to events by these actor identifiers.
    pub actor_ids: Option<Vec<String>>,

    /// Filter to these action type strings (e.g., "job.submit", "node.join").
    pub action_types: Option<Vec<String>>,

    /// Minimum criticality level (string placeholder until W1A CriticalityLevel integrates).
    pub criticality_min: Option<String>,

    /// Glob/regex pattern matching on event target field.
    pub target_pattern: Option<String>,

    /// Maximum number of results to return.
    pub limit: Option<usize>,

    /// Offset for pagination.
    pub offset: Option<usize>,
}

// ---------------------------------------------------------------------------
// ResultSet
// ---------------------------------------------------------------------------

/// Generic tabular result returned by `SwarmStore::query_sql()`.
/// Compatible with any tier's query output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSet {
    /// Column names in order.
    pub columns: Vec<String>,

    /// Rows of values, each row aligned with columns.
    pub rows: Vec<Vec<Value>>,

    /// Total rows matched (may exceed rows.len() if truncated).
    pub row_count: usize,

    /// True if results were truncated due to limit.
    pub truncated: bool,
}

impl ResultSet {
    /// Create an empty result set with no columns.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
            truncated: false,
        }
    }

    /// Create a result set with the given columns and no rows.
    pub fn with_columns(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            row_count: 0,
            truncated: false,
        }
    }
}

// ---------------------------------------------------------------------------
// StorageError
// ---------------------------------------------------------------------------

/// Errors that can occur during storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("tier unavailable: {tier}")]
    TierUnavailable { tier: StorageTier },

    #[error("query failed: {reason}")]
    QueryFailed { reason: String },

    #[error("replication failed: replicated to {achieved} of {required} nodes")]
    ReplicationFailed { achieved: usize, required: usize },

    #[error("operation timed out after {ms}ms")]
    Timeout { ms: u64 },

    #[error("connection failed: {reason}")]
    ConnectionFailed { reason: String },

    #[error("serialization error: {reason}")]
    SerializationError { reason: String },

    #[error("fragment not found: {id}")]
    FragmentNotFound { id: String },

    #[error("deployment failed: {reason}")]
    DeploymentFailed { reason: String },

    #[error("permission denied: {reason}")]
    PermissionDenied { reason: String },

    #[error("internal error: {reason}")]
    Internal { reason: String },
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_usable() {
        assert!(HealthStatus::Healthy.is_usable());
        assert!(HealthStatus::Degraded.is_usable());
        assert!(!HealthStatus::Unavailable.is_usable());
        assert!(!HealthStatus::Unknown.is_usable());
    }

    #[test]
    fn test_event_filter_default() {
        let f = EventFilter::default();
        assert!(f.time_range.is_none());
        assert!(f.node_ids.is_none());
        assert!(f.actor_ids.is_none());
        assert!(f.action_types.is_none());
        assert!(f.criticality_min.is_none());
        assert!(f.target_pattern.is_none());
        assert!(f.limit.is_none());
        assert!(f.offset.is_none());
    }

    #[test]
    fn test_event_filter_builder() {
        let f = EventFilter {
            time_range: Some((Some(1000), Some(2000))),
            node_ids: Some(vec!["node-1".to_string()]),
            actor_ids: None,
            action_types: Some(vec!["job.submit".to_string(), "node.join".to_string()]),
            criticality_min: Some("high".to_string()),
            target_pattern: Some("cluster-*".to_string()),
            limit: Some(100),
            offset: Some(0),
        };
        assert_eq!(f.time_range, Some((Some(1000), Some(2000))));
        assert_eq!(f.node_ids.as_ref().unwrap().len(), 1);
        assert!(f.actor_ids.is_none());
        assert_eq!(f.action_types.as_ref().unwrap().len(), 2);
        assert_eq!(f.criticality_min, Some("high".to_string()));
        assert_eq!(f.target_pattern, Some("cluster-*".to_string()));
        assert_eq!(f.limit, Some(100));
        assert_eq!(f.offset, Some(0));
    }

    #[test]
    fn test_result_set_empty() {
        let rs = ResultSet::empty();
        assert!(rs.columns.is_empty());
        assert!(rs.rows.is_empty());
        assert_eq!(rs.row_count, 0);
        assert!(!rs.truncated);
    }

    #[test]
    fn test_result_set_with_columns() {
        let rs = ResultSet::with_columns(vec![
            "id".to_string(),
            "name".to_string(),
            "value".to_string(),
        ]);
        assert_eq!(rs.columns.len(), 3);
        assert_eq!(rs.columns[0], "id");
        assert_eq!(rs.columns[1], "name");
        assert_eq!(rs.columns[2], "value");
        assert!(rs.rows.is_empty());
        assert_eq!(rs.row_count, 0);
        assert!(!rs.truncated);
    }

    #[test]
    fn test_storage_error_display() {
        let e = StorageError::TierUnavailable {
            tier: StorageTier::Tier1PreExisting,
        };
        assert_eq!(e.to_string(), "tier unavailable: Tier 1 (Pre-Existing)");

        let e = StorageError::QueryFailed {
            reason: "syntax error".to_string(),
        };
        assert_eq!(e.to_string(), "query failed: syntax error");

        let e = StorageError::ReplicationFailed {
            achieved: 1,
            required: 3,
        };
        assert_eq!(
            e.to_string(),
            "replication failed: replicated to 1 of 3 nodes"
        );

        let e = StorageError::Timeout { ms: 5000 };
        assert_eq!(e.to_string(), "operation timed out after 5000ms");

        let e = StorageError::ConnectionFailed {
            reason: "refused".to_string(),
        };
        assert_eq!(e.to_string(), "connection failed: refused");

        let e = StorageError::FragmentNotFound {
            id: "frag-42".to_string(),
        };
        assert_eq!(e.to_string(), "fragment not found: frag-42");

        let e = StorageError::DeploymentFailed {
            reason: "out of memory".to_string(),
        };
        assert_eq!(e.to_string(), "deployment failed: out of memory");

        let e = StorageError::PermissionDenied {
            reason: "not authorized".to_string(),
        };
        assert_eq!(e.to_string(), "permission denied: not authorized");

        let e = StorageError::Internal {
            reason: "unexpected state".to_string(),
        };
        assert_eq!(e.to_string(), "internal error: unexpected state");
    }

    #[test]
    fn test_value_constructors_and_accessors() {
        assert!(Value::Null.is_null());
        assert!(!Value::Bool(true).is_null());

        assert_eq!(Value::Text("hello".into()).as_text(), Some("hello"));
        assert_eq!(Value::Int(42).as_text(), None);

        assert_eq!(Value::Int(42).as_int(), Some(42));
        assert_eq!(Value::Text("x".into()).as_int(), None);

        assert_eq!(Value::Float(3.14).as_float(), Some(3.14));
        assert_eq!(Value::Int(1).as_float(), None);

        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Null.as_bool(), None);

        assert_eq!(Value::Bytes(vec![1, 2, 3]).as_bytes(), Some(&[1u8, 2, 3][..]));
        assert_eq!(Value::Null.as_bytes(), None);

        assert_eq!(Value::Timestamp(1234567890).as_timestamp(), Some(1234567890));
        assert_eq!(Value::Null.as_timestamp(), None);
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::Int(42).to_string(), "42");
        assert_eq!(Value::Text("hello".into()).to_string(), "hello");
        assert_eq!(Value::Bytes(vec![1, 2, 3]).to_string(), "<3 bytes>");
    }
}
