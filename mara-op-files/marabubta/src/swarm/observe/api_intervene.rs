// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::swarm::api::ApiState;
use super::engine::{InterventionEngine, InterventionRequest, InterventionResult};
use super::guard::ConditionGuard;
use super::types::*;

// ---- Request/Response Types ----

#[derive(Deserialize)]
pub struct InterventionApiRequest {
    pub target_type: String,
    pub target_id: String,
    pub guard: Option<ConditionGuard>,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub skip_guard: bool,
    #[serde(default)]
    pub force: bool,
    pub session_id: Option<String>,
}

#[derive(Serialize)]
pub struct InterventionApiResponse {
    pub intervention_id: String,
    pub guard_evaluation: Option<serde_json::Value>,
    pub impact: Option<serde_json::Value>,
    pub executed: bool,
    pub result: serde_json::Value,
    pub audit_entry_id: Option<u64>,
    pub rollback_info: Option<String>,
}

impl From<InterventionResult> for InterventionApiResponse {
    fn from(r: InterventionResult) -> Self {
        Self {
            intervention_id: r.intervention_id.0,
            guard_evaluation: r
                .guard_evaluation
                .map(|g| serde_json::to_value(g).unwrap_or_default()),
            impact: r
                .impact
                .map(|i| serde_json::to_value(i).unwrap_or_default()),
            executed: r.executed,
            result: r.result,
            audit_entry_id: r.audit_entry_id,
            rollback_info: r.rollback_info,
        }
    }
}

// ---- Intervention Endpoints ----

/// POST /api/v1/intervene/:tier/:action
pub async fn execute_intervention(
    State(state): State<ApiState>,
    Path((tier_str, action_str)): Path<(String, String)>,
    Json(req): Json<InterventionApiRequest>,
) -> Result<Json<InterventionApiResponse>, (StatusCode, Json<serde_json::Value>)> {
    let engine = require_engine(&state)?;

    let tier = parse_tier(&tier_str).ok_or_else(|| {
        oai_error_response(
            StatusCode::BAD_REQUEST,
            &format!(
                "unknown tier: {}. Valid: node_lifecycle, work_control, organic, neuromancer, plugin, configuration",
                tier_str
            ),
        )
    })?;

    let target_type = super::api_observe::parse_entity_type(&req.target_type)
        .ok_or_else(|| oai_error_response(StatusCode::BAD_REQUEST, "unknown target type"))?;

    let operator_id = resolve_operator_from_auth();

    let guard = if let Some(g) = req.guard {
        Some(g)
    } else if !req.skip_guard {
        auto_generate_guard(&state, &target_type, &req.target_id)
    } else {
        None
    };

    let request = InterventionRequest {
        id: InterventionId::new(),
        action: InterventionAction {
            tier,
            name: action_str,
        },
        target: EntityRef {
            entity_type: target_type,
            id: req.target_id,
        },
        guard,
        urgency: InterventionUrgency::Operational,
        operator_id,
        session_id: req.session_id.map(SessionId),
        skip_guard: req.skip_guard,
        force: req.force,
        dry_run: false,
        params: req.params,
    };

    let result = engine.execute(request).map_err(|e| {
        let status = StatusCode::from_u16(e.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (
            status,
            Json(serde_json::to_value(&e).unwrap_or_default()),
        )
    })?;

    Ok(Json(result.into()))
}

/// POST /api/v1/intervene/dry-run/:tier/:action
pub async fn dry_run_intervention(
    State(state): State<ApiState>,
    Path((tier_str, action_str)): Path<(String, String)>,
    Json(req): Json<InterventionApiRequest>,
) -> Result<Json<InterventionApiResponse>, (StatusCode, Json<serde_json::Value>)> {
    let engine = require_engine(&state)?;

    let tier = parse_tier(&tier_str)
        .ok_or_else(|| oai_error_response(StatusCode::BAD_REQUEST, "unknown tier"))?;

    let target_type = super::api_observe::parse_entity_type(&req.target_type)
        .ok_or_else(|| oai_error_response(StatusCode::BAD_REQUEST, "unknown target type"))?;

    let operator_id = resolve_operator_from_auth();

    let guard = if let Some(g) = req.guard {
        Some(g)
    } else if !req.skip_guard {
        auto_generate_guard(&state, &target_type, &req.target_id)
    } else {
        None
    };

    let request = InterventionRequest {
        id: InterventionId::new(),
        action: InterventionAction {
            tier,
            name: action_str,
        },
        target: EntityRef {
            entity_type: target_type,
            id: req.target_id,
        },
        guard,
        urgency: InterventionUrgency::Operational,
        operator_id,
        session_id: req.session_id.map(SessionId),
        skip_guard: req.skip_guard,
        force: req.force,
        dry_run: true,
        params: req.params,
    };

    let result = engine.execute(request).map_err(|e| {
        let status = StatusCode::from_u16(e.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (
            status,
            Json(serde_json::to_value(&e).unwrap_or_default()),
        )
    })?;

    Ok(Json(result.into()))
}

/// GET /api/v1/intervene/locks
pub async fn list_locks(
    State(state): State<ApiState>,
) -> Result<Json<Vec<super::lock::InterventionLock>>, (StatusCode, Json<serde_json::Value>)> {
    let lock_mgr = state.lock_manager.as_ref().ok_or_else(|| {
        oai_error_response(StatusCode::SERVICE_UNAVAILABLE, "OAI not enabled")
    })?;
    Ok(Json(lock_mgr.list_active()))
}

// ---- Helpers ----

fn require_engine(
    state: &ApiState,
) -> Result<&Arc<InterventionEngine>, (StatusCode, Json<serde_json::Value>)> {
    state.intervention_engine.as_ref().ok_or_else(|| {
        oai_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "observe_and_interfere not enabled",
        )
    })
}

fn parse_tier(s: &str) -> Option<InterventionTier> {
    match s {
        "node_lifecycle" | "tier1" => Some(InterventionTier::NodeLifecycle),
        "work_control" | "tier2" => Some(InterventionTier::WorkControl),
        "organic" | "tier3" => Some(InterventionTier::Organic),
        "neuromancer" | "tier4" => Some(InterventionTier::Neuromancer),
        "plugin" | "tier5" => Some(InterventionTier::Plugin),
        "configuration" | "tier6" => Some(InterventionTier::Configuration),
        _ => None,
    }
}

fn resolve_operator_from_auth() -> OperatorId {
    OperatorId::new("default-operator")
}

fn auto_generate_guard(
    state: &ApiState,
    entity_type: &EntityType,
    entity_id: &str,
) -> Option<ConditionGuard> {
    let guard_evaluator = state.guard_evaluator.as_ref()?;
    match entity_type {
        EntityType::Node => {
            let node_id = Uuid::parse_str(entity_id)
                .map(crate::swarm::types::NodeId)
                .ok()?;
            guard_evaluator.auto_guard_node(&node_id)
        }
        EntityType::Job => {
            let job_id = Uuid::parse_str(entity_id)
                .map(crate::common::types::JobId)
                .ok()?;
            guard_evaluator.auto_guard_job(&job_id)
        }
        EntityType::Chunk => {
            let chunk_id = Uuid::parse_str(entity_id)
                .map(crate::swarm::types::ChunkId)
                .ok()?;
            guard_evaluator.auto_guard_chunk(&chunk_id)
        }
        _ => None,
    }
}

fn oai_error_response(
    status: StatusCode,
    msg: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({ "error": msg })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tier_named() {
        assert_eq!(
            parse_tier("node_lifecycle"),
            Some(InterventionTier::NodeLifecycle)
        );
        assert_eq!(
            parse_tier("work_control"),
            Some(InterventionTier::WorkControl)
        );
        assert_eq!(parse_tier("organic"), Some(InterventionTier::Organic));
        assert_eq!(
            parse_tier("neuromancer"),
            Some(InterventionTier::Neuromancer)
        );
        assert_eq!(parse_tier("plugin"), Some(InterventionTier::Plugin));
        assert_eq!(
            parse_tier("configuration"),
            Some(InterventionTier::Configuration)
        );
    }

    #[test]
    fn test_parse_tier_numbered() {
        assert_eq!(
            parse_tier("tier1"),
            Some(InterventionTier::NodeLifecycle)
        );
        assert_eq!(
            parse_tier("tier2"),
            Some(InterventionTier::WorkControl)
        );
        assert_eq!(parse_tier("tier6"), Some(InterventionTier::Configuration));
    }

    #[test]
    fn test_parse_tier_unknown() {
        assert_eq!(parse_tier("unknown"), None);
        assert_eq!(parse_tier(""), None);
        assert_eq!(parse_tier("tier7"), None);
    }

    #[test]
    fn test_intervention_api_response_from_result() {
        let result = InterventionResult {
            intervention_id: InterventionId::new(),
            guard_evaluation: None,
            impact: None,
            executed: true,
            result: serde_json::json!({"status": "ok"}),
            audit_entry_id: Some(42),
            rollback_info: None,
        };
        let resp: InterventionApiResponse = result.into();
        assert!(resp.executed);
        assert_eq!(resp.audit_entry_id, Some(42));
        assert_eq!(resp.result["status"], "ok");
    }

    #[test]
    fn test_intervention_api_request_deserialize() {
        let json = serde_json::json!({
            "target_type": "node",
            "target_id": "abc-123",
            "params": {"timeout_secs": 60}
        });
        let req: InterventionApiRequest =
            serde_json::from_value(json).expect("deserialize");
        assert_eq!(req.target_type, "node");
        assert!(!req.force);
        assert!(!req.skip_guard);
        assert!(req.session_id.is_none());
    }

    #[test]
    fn test_intervention_api_request_with_force() {
        let json = serde_json::json!({
            "target_type": "node",
            "target_id": "abc-123",
            "force": true,
            "skip_guard": true
        });
        let req: InterventionApiRequest =
            serde_json::from_value(json).expect("deserialize");
        assert!(req.force);
        assert!(req.skip_guard);
    }
}
