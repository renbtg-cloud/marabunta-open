// Marabunta - Licensed under the MIT License.
//! REST API endpoints for audit trail access.
//!
//! Provides query, verification, filtering, and export endpoints for the
//! [`UnifiedAuditStore`]. All endpoints require the Auditor security_domain role
//! or Admin permission (enforced by external auth middleware).

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Json;
use axum::{Router, routing::get};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::swarm::unified_audit::{UnifiedAuditEvent, UnifiedAuditStore};

// ============================================================================
// Shared State
// ============================================================================

/// Shared application state for audit API endpoints.
#[derive(Clone)]
pub struct AuditApiState {
    pub unified_audit: Arc<UnifiedAuditStore>,
}

// ============================================================================
// Query / Response types
// ============================================================================

/// Query parameters for filtering audit events.
#[derive(Debug, Deserialize)]
pub struct AuditQueryParams {
    /// Start of sequence range (inclusive).
    pub from_seq: Option<u64>,
    /// End of sequence range (inclusive).
    pub to_seq: Option<u64>,
    /// Filter by event type (exact match).
    pub event_type: Option<String>,
    /// Filter by node ID (exact match).
    pub node_id: Option<String>,
    /// Filter by zone ID (exact match).
    pub zone_id: Option<String>,
    /// Filter by event source ("swarm", "highestsec", "blind").
    pub source: Option<String>,
    /// Maximum number of events to return.
    pub limit: Option<usize>,
}

/// Serialized representation of a single audit event for API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventResponse {
    pub event_source: String,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub node_id: Option<String>,
    pub zone_id: Option<String>,
    pub event_hash: String,
    pub prev_hash: String,
    pub sequence: u64,
    pub payload: serde_json::Value,
    pub classification: Option<String>,
    pub sanitized: bool,
}

impl From<UnifiedAuditEvent> for AuditEventResponse {
    fn from(e: UnifiedAuditEvent) -> Self {
        Self {
            event_source: e.event_source,
            event_type: e.event_type,
            timestamp: e.timestamp,
            node_id: e.node_id,
            zone_id: e.zone_id,
            event_hash: hex::encode(e.event_hash),
            prev_hash: hex::encode(e.prev_hash),
            sequence: e.sequence,
            payload: e.payload,
            classification: e.classification,
            sanitized: e.sanitized,
        }
    }
}

/// Response for chain verification.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChainVerifyResponse {
    pub valid: bool,
    pub checked_events: usize,
    pub last_verified_seq: u64,
}

/// Response for audit export.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditExportResponse {
    pub events: Vec<AuditEventResponse>,
    pub exported_at: DateTime<Utc>,
    pub total_count: usize,
}

// ============================================================================
// Helpers
// ============================================================================

/// Apply query filters to a list of events.
fn apply_filters(
    events: Vec<UnifiedAuditEvent>,
    params: &AuditQueryParams,
) -> Vec<AuditEventResponse> {
    let mut filtered: Vec<UnifiedAuditEvent> = events
        .into_iter()
        .filter(|e| {
            if let Some(ref et) = params.event_type {
                if e.event_type != *et {
                    return false;
                }
            }
            if let Some(ref nid) = params.node_id {
                if e.node_id.as_deref() != Some(nid.as_str()) {
                    return false;
                }
            }
            if let Some(ref zid) = params.zone_id {
                if e.zone_id.as_deref() != Some(zid.as_str()) {
                    return false;
                }
            }
            if let Some(ref src) = params.source {
                if e.event_source != *src {
                    return false;
                }
            }
            true
        })
        .collect();

    filtered.sort_by_key(|e| e.sequence);

    let limit = params.limit.unwrap_or(1000).min(10_000);
    filtered.truncate(limit);

    filtered.into_iter().map(AuditEventResponse::from).collect()
}

/// Resolve the query range, defaulting to [1, current_max].
fn resolve_range(store: &UnifiedAuditStore, params: &AuditQueryParams) -> (u64, u64) {
    let from = params.from_seq.unwrap_or(1);
    let to = params.to_seq.unwrap_or_else(|| store.len() as u64);
    (from, to.max(from))
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /api/v1/audit/events
///
/// Query audit events with optional filters.
pub async fn get_audit_events(
    Query(params): Query<AuditQueryParams>,
    State(state): State<Arc<AuditApiState>>,
) -> Json<Vec<AuditEventResponse>> {
    let (from, to) = resolve_range(&state.unified_audit, &params);
    let events = state.unified_audit.query_range(from, to);
    Json(apply_filters(events, &params))
}

/// GET /api/v1/audit/events/:seq
///
/// Retrieve a single audit event by sequence number.
pub async fn get_audit_event(
    Path(seq): Path<u64>,
    State(state): State<Arc<AuditApiState>>,
) -> Result<Json<AuditEventResponse>, StatusCode> {
    state
        .unified_audit
        .get(seq)
        .map(|e| Json(AuditEventResponse::from(e)))
        .ok_or(StatusCode::NOT_FOUND)
}

/// GET /api/v1/audit/verify
///
/// Verify the hash chain integrity of the entire audit trail.
pub async fn verify_chain(
    State(state): State<Arc<AuditApiState>>,
) -> Json<ChainVerifyResponse> {
    let valid = state.unified_audit.verify_chain();
    let count = state.unified_audit.len();
    Json(ChainVerifyResponse {
        valid,
        checked_events: count,
        last_verified_seq: count as u64,
    })
}

/// GET /api/v1/audit/highestsec
///
/// Query only highestsec-source audit events.
pub async fn get_highestsec_events(
    Query(mut params): Query<AuditQueryParams>,
    State(state): State<Arc<AuditApiState>>,
) -> Json<Vec<AuditEventResponse>> {
    params.source = Some("highestsec".to_string());
    let (from, to) = resolve_range(&state.unified_audit, &params);
    let events = state.unified_audit.query_range(from, to);
    Json(apply_filters(events, &params))
}

/// GET /api/v1/audit/gdpr
///
/// Query only GDPR / blind-source audit events.
pub async fn get_gdpr_events(
    Query(mut params): Query<AuditQueryParams>,
    State(state): State<Arc<AuditApiState>>,
) -> Json<Vec<AuditEventResponse>> {
    // GDPR events may originate from "blind" or have event_type containing "gdpr".
    // We filter by source="blind" as the primary selector; the caller can further
    // filter by event_type via the query parameter.
    params.source = Some("blind".to_string());
    let (from, to) = resolve_range(&state.unified_audit, &params);
    let events = state.unified_audit.query_range(from, to);
    Json(apply_filters(events, &params))
}

/// GET /api/v1/audit/export
///
/// Export filtered audit events with metadata.
pub async fn export_audit(
    Query(params): Query<AuditQueryParams>,
    State(state): State<Arc<AuditApiState>>,
) -> Json<AuditExportResponse> {
    let (from, to) = resolve_range(&state.unified_audit, &params);
    let events = state.unified_audit.query_range(from, to);
    let filtered = apply_filters(events, &params);
    let total = filtered.len();

    Json(AuditExportResponse {
        events: filtered,
        exported_at: Utc::now(),
        total_count: total,
    })
}

// ============================================================================
// Router
// ============================================================================

/// Build the audit API router with all 6 endpoints.
///
/// Mount at `/api/v1/audit` in the main application router.
pub fn audit_routes() -> Router<Arc<AuditApiState>> {
    Router::new()
        .route("/events", get(get_audit_events))
        .route("/events/:seq", get(get_audit_event))
        .route("/verify", get(verify_chain))
        .route("/highestsec", get(get_highestsec_events))
        .route("/gdpr", get(get_gdpr_events))
        .route("/export", get(export_audit))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_audit_event_response_from_unified() {
        let store = UnifiedAuditStore::new();
        let event = store.append(
            "highestsec",
            "zone.created",
            json!({"zone": "eu-west"}),
            Some("node-1".to_string()),
            Some("zone-eu".to_string()),
            Some("RESTRICTED".to_string()),
        );

        let response = AuditEventResponse::from(event.clone());
        assert_eq!(response.event_source, "highestsec");
        assert_eq!(response.event_type, "zone.created");
        assert_eq!(response.sequence, 1);
        assert_eq!(response.node_id, Some("node-1".to_string()));
        assert_eq!(response.zone_id, Some("zone-eu".to_string()));
        assert_eq!(response.classification, Some("RESTRICTED".to_string()));
        assert!(!response.sanitized);
        // Hash should be hex-encoded (64 chars).
        assert_eq!(response.event_hash.len(), 64);
        assert_eq!(response.prev_hash.len(), 64);
    }

    #[test]
    fn test_chain_verify_response_serializes() {
        let resp = ChainVerifyResponse {
            valid: true,
            checked_events: 42,
            last_verified_seq: 42,
        };
        let json = serde_json::to_value(&resp).expect("serialize");
        assert_eq!(json["valid"], true);
        assert_eq!(json["checked_events"], 42);
    }

    #[test]
    fn test_audit_export_response_serializes() {
        let resp = AuditExportResponse {
            events: vec![],
            exported_at: Utc::now(),
            total_count: 0,
        };
        let json = serde_json::to_value(&resp).expect("serialize");
        assert_eq!(json["total_count"], 0);
        assert!(json["events"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_query_params_deserialize_defaults() {
        let params: AuditQueryParams =
            serde_json::from_value(json!({})).expect("deserialize");
        assert!(params.from_seq.is_none());
        assert!(params.to_seq.is_none());
        assert!(params.event_type.is_none());
        assert!(params.limit.is_none());
    }

    #[test]
    fn test_query_params_deserialize_full() {
        let params: AuditQueryParams = serde_json::from_value(json!({
            "from_seq": 10,
            "to_seq": 50,
            "event_type": "job.submit",
            "node_id": "node-abc",
            "zone_id": "zone-eu",
            "source": "highestsec",
            "limit": 100
        }))
        .expect("deserialize");

        assert_eq!(params.from_seq, Some(10));
        assert_eq!(params.to_seq, Some(50));
        assert_eq!(params.event_type, Some("job.submit".to_string()));
        assert_eq!(params.limit, Some(100));
    }

    #[test]
    fn test_apply_filters_by_source() {
        let store = UnifiedAuditStore::new();
        store.append("swarm", "node.join", json!({}), None, None, None);
        store.append("highestsec", "zone.created", json!({}), None, None, None);
        store.append("swarm", "node.leave", json!({}), None, None, None);

        let events = store.query_range(1, 3);
        let params = AuditQueryParams {
            from_seq: None,
            to_seq: None,
            event_type: None,
            node_id: None,
            zone_id: None,
            source: Some("swarm".to_string()),
            limit: None,
        };
        let filtered = apply_filters(events, &params);
        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().all(|e| e.event_source == "swarm"));
    }

    #[test]
    fn test_apply_filters_by_event_type() {
        let store = UnifiedAuditStore::new();
        store.append("swarm", "node.join", json!({}), None, None, None);
        store.append("swarm", "job.submit", json!({}), None, None, None);

        let events = store.query_range(1, 2);
        let params = AuditQueryParams {
            from_seq: None,
            to_seq: None,
            event_type: Some("job.submit".to_string()),
            node_id: None,
            zone_id: None,
            source: None,
            limit: None,
        };
        let filtered = apply_filters(events, &params);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].event_type, "job.submit");
    }

    #[test]
    fn test_apply_filters_limit() {
        let store = UnifiedAuditStore::new();
        for i in 0..10 {
            store.append("swarm", &format!("event_{}", i), json!({}), None, None, None);
        }

        let events = store.query_range(1, 10);
        let params = AuditQueryParams {
            from_seq: None,
            to_seq: None,
            event_type: None,
            node_id: None,
            zone_id: None,
            source: None,
            limit: Some(3),
        };
        let filtered = apply_filters(events, &params);
        assert_eq!(filtered.len(), 3);
    }
}
