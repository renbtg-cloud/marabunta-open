// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;

use crate::swarm::api::ApiState;
use super::operator::{Operator, OperatorStore, ProfilePreset};
use super::session::{IncidentSession, SessionStore};
use super::types::*;

// ---- Operator Endpoints ----

#[derive(Deserialize)]
pub struct CreateOperatorRequest {
    pub name: String,
    pub preset: String,
    pub team: Option<String>,
}

/// POST /api/v1/operator
pub async fn create_operator(
    State(state): State<ApiState>,
    Json(req): Json<CreateOperatorRequest>,
) -> Result<(StatusCode, Json<Operator>), (StatusCode, Json<serde_json::Value>)> {
    let store = require_operator_store(&state)?;

    let preset = parse_preset(&req.preset).ok_or_else(|| {
        error_response(
            StatusCode::BAD_REQUEST,
            "unknown preset. Valid: spectator, researcher, engineer, platform_admin, security_ops, unrestricted",
        )
    })?;

    let id = OperatorId::new(format!("op-{}", uuid::Uuid::new_v4().as_simple()));
    let mut op = store.register_from_preset(id, req.name, preset);
    if let Some(team) = req.team {
        op.team = Some(team);
        store.register(op.clone());
    }

    Ok((StatusCode::CREATED, Json(op)))
}

/// GET /api/v1/operator/whoami
pub async fn operator_whoami(
    State(state): State<ApiState>,
) -> Result<Json<Operator>, (StatusCode, Json<serde_json::Value>)> {
    let store = require_operator_store(&state)?;
    let op_id = resolve_current_operator();

    let op = store
        .get(&op_id)
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, "operator not found"))?;

    Ok(Json(op))
}

/// GET /api/v1/operator/:id
pub async fn get_operator(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<Operator>, (StatusCode, Json<serde_json::Value>)> {
    let store = require_operator_store(&state)?;
    let op = store
        .get(&OperatorId::new(id))
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, "operator not found"))?;
    Ok(Json(op))
}

/// GET /api/v1/operator
pub async fn list_operators(
    State(state): State<ApiState>,
) -> Result<Json<Vec<Operator>>, (StatusCode, Json<serde_json::Value>)> {
    let store = require_operator_store(&state)?;
    Ok(Json(store.list()))
}

/// GET /api/v1/operator/audit
#[derive(Deserialize)]
pub struct AuditQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn operator_audit(
    State(state): State<ApiState>,
    axum::extract::Query(q): axum::extract::Query<AuditQuery>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, Json<serde_json::Value>)> {
    let audit_log = state.audit_log.as_ref().ok_or_else(|| {
        error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "audit log not available",
        )
    })?;

    let op_id = resolve_current_operator();

    let filter = crate::swarm::audit::AuditFilter {
        actor_id: Some(op_id.0.clone()),
        ..Default::default()
    };

    let limit = q.limit.unwrap_or(50).min(500);
    let mut full_filter = filter;
    full_filter.limit = Some(limit);

    let entries = audit_log.query(&full_filter);
    let json_entries: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| serde_json::to_value(e).unwrap_or_default())
        .collect();

    Ok(Json(json_entries))
}

// ---- Session Endpoints ----

#[derive(Deserialize)]
pub struct CreateSessionRequest {
    pub title: String,
    pub description: Option<String>,
}

/// POST /api/v1/operator/session
pub async fn create_session(
    State(state): State<ApiState>,
    Json(req): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<IncidentSession>), (StatusCode, Json<serde_json::Value>)> {
    let store = require_session_store(&state)?;
    let op_id = resolve_current_operator();
    let session = store.create(op_id, req.title, req.description);
    Ok((StatusCode::CREATED, Json(session)))
}

/// POST /api/v1/operator/session/:id/end
pub async fn end_session(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<IncidentSession>, (StatusCode, Json<serde_json::Value>)> {
    let store = require_session_store(&state)?;
    let session = store
        .end_session(&SessionId(id))
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, "session not found"))?;
    Ok(Json(session))
}

/// GET /api/v1/operator/session/:id
pub async fn get_session(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<IncidentSession>, (StatusCode, Json<serde_json::Value>)> {
    let store = require_session_store(&state)?;
    let session = store
        .get(&SessionId(id))
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, "session not found"))?;
    Ok(Json(session))
}

/// GET /api/v1/operator/sessions
pub async fn list_sessions(
    State(state): State<ApiState>,
) -> Result<Json<Vec<IncidentSession>>, (StatusCode, Json<serde_json::Value>)> {
    let store = require_session_store(&state)?;
    Ok(Json(store.list()))
}

// ---- Helpers ----

fn require_operator_store(
    state: &ApiState,
) -> Result<&Arc<OperatorStore>, (StatusCode, Json<serde_json::Value>)> {
    state.operator_store.as_ref().ok_or_else(|| {
        error_response(StatusCode::SERVICE_UNAVAILABLE, "OAI not enabled")
    })
}

fn require_session_store(
    state: &ApiState,
) -> Result<&Arc<SessionStore>, (StatusCode, Json<serde_json::Value>)> {
    state.session_store.as_ref().ok_or_else(|| {
        error_response(StatusCode::SERVICE_UNAVAILABLE, "OAI not enabled")
    })
}

fn resolve_current_operator() -> OperatorId {
    OperatorId::new("default-operator")
}

fn parse_preset(s: &str) -> Option<ProfilePreset> {
    match s {
        "spectator" => Some(ProfilePreset::Spectator),
        "researcher" => Some(ProfilePreset::Researcher),
        "engineer" => Some(ProfilePreset::Engineer),
        "platform_admin" => Some(ProfilePreset::PlatformAdmin),
        "security_ops" => Some(ProfilePreset::SecurityOps),
        "unrestricted" => Some(ProfilePreset::Unrestricted),
        _ => None,
    }
}

fn error_response(
    status: StatusCode,
    msg: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({ "error": msg })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_preset_valid() {
        assert!(parse_preset("spectator").is_some());
        assert!(parse_preset("researcher").is_some());
        assert!(parse_preset("engineer").is_some());
        assert!(parse_preset("platform_admin").is_some());
        assert!(parse_preset("security_ops").is_some());
        assert!(parse_preset("unrestricted").is_some());
    }

    #[test]
    fn test_parse_preset_invalid() {
        assert!(parse_preset("admin").is_none());
        assert!(parse_preset("").is_none());
        assert!(parse_preset("root").is_none());
    }

    #[test]
    fn test_create_operator_request_deserialize() {
        let json = serde_json::json!({
            "name": "Alice",
            "preset": "engineer",
            "team": "infra"
        });
        let req: CreateOperatorRequest =
            serde_json::from_value(json).expect("deserialize");
        assert_eq!(req.name, "Alice");
        assert_eq!(req.preset, "engineer");
        assert_eq!(req.team, Some("infra".to_string()));
    }

    #[test]
    fn test_create_session_request_deserialize() {
        let json = serde_json::json!({
            "title": "Incident 42",
            "description": "Node failure investigation"
        });
        let req: CreateSessionRequest =
            serde_json::from_value(json).expect("deserialize");
        assert_eq!(req.title, "Incident 42");
        assert!(req.description.is_some());
    }

    #[test]
    fn test_audit_query_defaults() {
        let q: AuditQuery =
            serde_json::from_value(serde_json::json!({})).expect("deserialize");
        assert!(q.limit.is_none());
    }

    #[test]
    fn test_error_response_format() {
        let (status, Json(body)) = error_response(StatusCode::BAD_REQUEST, "bad request");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "bad request");
    }
}
