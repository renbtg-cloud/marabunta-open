// Marabunta - Licensed under the MIT License.
//! REST API endpoints for the L4 state machine workflow engine (W4A).
//!
//! Provides 10 HTTP endpoints that expose the full workflow lifecycle:
//! listing definitions, deploying workflows, creating/querying instances,
//! firing transitions, inspecting history, and plugin introspection.
//!
//! All handlers are thin translation layers between HTTP and the
//! [`WorkflowRuntime`]. Business logic lives entirely in the runtime;
//! the API layer never manipulates workflow state directly.

use std::sync::Arc;

use axum::extract::{Extension, Path, Query};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use super::parser::{parse_and_validate_json, parse_and_validate_yaml};
use super::runtime::{TransitionRequest, TransitionResult, WorkflowRuntime};
use super::types::{ActorInfo, TransitionDefinition};

// ============================================================================
// Shared State
// ============================================================================

/// Shared state injected into all workflow API handlers via axum Extension.
#[derive(Clone)]
pub struct WorkflowApiState {
    /// The workflow execution engine (W3A).
    pub runtime: Arc<WorkflowRuntime>,
}

// ============================================================================
// Error Response
// ============================================================================

/// Standardized error response body for workflow API endpoints.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkflowApiError {
    /// Machine-readable error code (e.g., "NOT_FOUND", "CONFLICT").
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Optional structured details (guard failures, field errors, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

// ============================================================================
// Request Types
// ============================================================================

/// Request body for POST /api/workflows/{name}/instances.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateInstanceRequest {
    /// Initial context object to seed the workflow instance.
    pub context: serde_json::Value,
    /// Actor ID of the person/system creating the instance.
    pub created_by: String,
}

/// Request body for POST /api/instances/{id}/transitions/{name}.
#[derive(Debug, Serialize, Deserialize)]
pub struct FireTransitionRequest {
    /// Actor performing the transition.
    pub actor_id: String,
    /// Expected current state for stale-state detection.
    pub expected_state: String,
    /// Optional parameters passed to guards and actions.
    #[serde(default)]
    pub params: Option<serde_json::Value>,
}

/// Request body for POST /api/workflows (deploy).
#[derive(Debug, Serialize, Deserialize)]
pub struct DeployWorkflowRequest {
    /// Workflow definition as a string (YAML or JSON).
    pub definition: String,
    /// Format: "yaml" or "json". Defaults to "yaml".
    #[serde(default = "default_format")]
    pub format: String,
    /// Actor who is deploying.
    #[serde(default)]
    pub deployed_by: String,
}

fn default_format() -> String {
    "yaml".into()
}

// ============================================================================
// Response Types
// ============================================================================

/// Response for GET /api/workflows.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkflowListResponse {
    pub workflows: Vec<WorkflowSummary>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
}

/// Summary metadata for a deployed workflow definition.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkflowSummary {
    pub name: String,
    pub version: u32,
    pub description: String,
    pub state_count: usize,
    pub transition_count: usize,
    pub active_instances: usize,
    pub deployed_at: String,
    pub deployed_by: String,
}

/// Response for GET /api/workflows/{name}/instances.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstanceListResponse {
    pub instances: Vec<InstanceSummary>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
}

/// Condensed instance info for list views.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstanceSummary {
    pub id: String,
    pub workflow_name: String,
    pub current_state: String,
    pub status: String,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
    pub version: u32,
}

/// Full instance detail for GET /api/instances/{id}.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstanceDetailResponse {
    pub id: String,
    pub workflow_name: String,
    pub definition_version: u32,
    pub current_state: String,
    pub status: String,
    pub context: serde_json::Value,
    pub version: u32,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
    pub recent_history: Vec<HistoryEntrySummary>,
    pub active_timers: Vec<TimerInfo>,
    pub transition_count: usize,
}

/// Condensed history entry for inclusion in instance detail.
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryEntrySummary {
    pub transition_name: String,
    pub from_state: String,
    pub to_state: String,
    pub actor_id: String,
    pub timestamp: String,
    pub audit_event_id: String,
}

/// Active timer info.
#[derive(Debug, Serialize, Deserialize)]
pub struct TimerInfo {
    pub timer_name: String,
    pub state_name: String,
    pub fires_at: String,
    pub action: String,
}

/// Response for a successful transition (HTTP 200).
#[derive(Debug, Serialize, Deserialize)]
pub struct TransitionSuccessResponse {
    pub instance: InstanceDetailResponse,
    pub audit_event_id: String,
    pub previous_state: String,
    pub new_state: String,
}

/// Response for GET /api/instances/{id}/available-transitions.
#[derive(Debug, Serialize, Deserialize)]
pub struct AvailableTransitionsResponse {
    pub instance_id: String,
    pub current_state: String,
    pub transitions: Vec<TransitionInfo>,
}

/// Information about a single available transition.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransitionInfo {
    pub name: String,
    pub to_state: String,
    pub guards: Vec<String>,
    pub required_roles: Vec<String>,
    pub min_approvers: usize,
    pub description: String,
}

/// Response for GET /api/instances/{id}/history.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstanceHistoryResponse {
    pub instance_id: String,
    pub entries: Vec<HistoryEntryDetail>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
}

/// Full history entry with audit link.
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryEntryDetail {
    pub sequence: usize,
    pub transition_name: String,
    pub from_state: String,
    pub to_state: String,
    pub actor_id: String,
    pub timestamp: String,
    pub audit_event_id: String,
    pub audit_event_url: String,
    pub context_snapshot: Option<serde_json::Value>,
}

/// Response for GET /api/plugins (stub).
#[derive(Debug, Serialize, Deserialize)]
pub struct PluginListResponse {
    pub plugins: Vec<PluginSummary>,
    pub total: usize,
}

/// Summary of a loaded plugin (stub).
#[derive(Debug, Serialize, Deserialize)]
pub struct PluginSummary {
    pub name: String,
    pub version: String,
    pub description: String,
    pub action_count: usize,
    pub condition_count: usize,
    pub status: String,
}

/// Response for GET /api/plugins/{name}/describe (stub).
#[derive(Debug, Serialize, Deserialize)]
pub struct PluginDescribeResponse {
    pub name: String,
    pub version: String,
    pub description: String,
    pub actions: Vec<ActionSchema>,
    pub conditions: Vec<ConditionSchema>,
}

/// Schema for a plugin-provided action.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActionSchema {
    pub name: String,
    pub description: String,
    pub params_schema: serde_json::Value,
}

/// Schema for a plugin-provided condition.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConditionSchema {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

// ============================================================================
// Query Parameter Structs
// ============================================================================

/// Query parameters for GET /api/workflows.
#[derive(Debug, Deserialize)]
pub struct WorkflowListParams {
    #[serde(default = "default_page")]
    pub page: usize,
    #[serde(default = "default_per_page")]
    pub per_page: usize,
    #[serde(default = "default_sort_name")]
    pub sort: String,
}

/// Query parameters for GET /api/workflows/{name}/instances.
#[derive(Debug, Deserialize)]
pub struct InstanceListParams {
    pub state: Option<String>,
    pub actor: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub status: Option<String>,
    #[serde(default = "default_page")]
    pub page: usize,
    #[serde(default = "default_per_page")]
    pub per_page: usize,
    #[serde(default = "default_sort_updated")]
    pub sort: String,
}

/// Query parameters for GET /api/instances/{id}/history.
#[derive(Debug, Deserialize)]
pub struct HistoryParams {
    #[serde(default = "default_page")]
    pub page: usize,
    #[serde(default = "default_history_per_page")]
    pub per_page: usize,
    #[serde(default = "default_sort_desc")]
    pub sort: String,
    #[serde(default)]
    pub include_context: bool,
}

/// Query parameters for GET /api/instances/{id}/available-transitions.
#[derive(Debug, Deserialize)]
pub struct AvailableTransitionsParams {
    pub actor_id: Option<String>,
}

fn default_page() -> usize {
    1
}
fn default_per_page() -> usize {
    20
}
fn default_sort_name() -> String {
    "name".into()
}
fn default_sort_updated() -> String {
    "updated_at".into()
}
fn default_history_per_page() -> usize {
    50
}
fn default_sort_desc() -> String {
    "desc".into()
}

/// Clamp per_page to [1, max]. Treats 0 as default.
fn clamp_per_page(value: usize, max: usize, default: usize) -> usize {
    if value == 0 {
        return default;
    }
    value.min(max).max(1)
}

// ============================================================================
// RBAC Helpers
// ============================================================================

/// Check if the actor has the required role.
fn require_role(
    actor: &ActorInfo,
    required: &str,
) -> Result<(), (StatusCode, Json<WorkflowApiError>)> {
    if actor.roles.iter().any(|r| r == required || r == "admin") {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            Json(WorkflowApiError {
                code: "FORBIDDEN".into(),
                message: format!(
                    "Actor '{}' lacks required role '{}'",
                    actor.actor_id, required
                ),
                details: None,
            }),
        ))
    }
}

/// Check if the actor can fire a specific transition.
#[allow(dead_code)]
fn check_transition_rbac(
    actor: &ActorInfo,
    transition: &TransitionDefinition,
) -> Result<(), (StatusCode, Json<WorkflowApiError>)> {
    // TransitionDefinition in types.rs does not have required_roles field,
    // so all transitions are open to any actor for now.
    let _ = actor;
    let _ = transition;
    Ok(())
}

// ============================================================================
// Instance Detail Builder
// ============================================================================

/// Build an InstanceDetailResponse from a WorkflowInstance.
fn build_instance_detail(
    instance: &super::types::WorkflowInstance,
) -> InstanceDetailResponse {
    let recent_history: Vec<HistoryEntrySummary> = instance
        .history
        .iter()
        .rev()
        .take(10)
        .map(|h| HistoryEntrySummary {
            transition_name: h.transition_name.clone(),
            from_state: h.from_state.clone(),
            to_state: h.to_state.clone(),
            actor_id: h.actor_id.clone(),
            timestamp: h.timestamp.to_rfc3339(),
            audit_event_id: h.audit_event_id.to_string(),
        })
        .collect();

    let active_timers: Vec<TimerInfo> = instance
        .timers
        .iter()
        .filter(|t| t.status == super::types::TimerStatus::Pending)
        .map(|t| TimerInfo {
            timer_name: t.trigger_name.clone(),
            state_name: instance.current_state.clone(),
            fires_at: t.fire_at.to_rfc3339(),
            action: format!("fire_transition:{}", t.transition),
        })
        .collect();

    InstanceDetailResponse {
        id: instance.instance_id.to_string(),
        workflow_name: instance.workflow_name.clone(),
        definition_version: instance.workflow_version,
        current_state: instance.current_state.clone(),
        status: instance.status.to_string(),
        context: instance.context.clone(),
        version: instance.workflow_version,
        created_by: instance.created_by.clone(),
        created_at: instance.created_at.to_rfc3339(),
        updated_at: instance.updated_at.to_rfc3339(),
        recent_history,
        active_timers,
        transition_count: instance.history.len(),
    }
}

// ============================================================================
// Handler Functions
// ============================================================================

/// GET /api/workflows -- List all deployed workflow definitions.
async fn list_workflows(
    Extension(state): Extension<WorkflowApiState>,
    Query(params): Query<WorkflowListParams>,
) -> Result<Json<WorkflowListResponse>, (StatusCode, Json<WorkflowApiError>)> {
    let per_page = clamp_per_page(params.per_page, 100, 20);
    let page = if params.page == 0 { 1 } else { params.page };

    let defs = state.runtime.list_definitions();
    let all_instances = state.runtime.list_instances();

    let mut summaries: Vec<WorkflowSummary> = defs
        .iter()
        .map(|def| {
            let active_count = all_instances
                .iter()
                .filter(|i| {
                    i.workflow_name == def.name
                        && i.status == super::types::InstanceStatus::Active
                })
                .count();

            WorkflowSummary {
                name: def.name.clone(),
                version: def.version,
                description: def.description.clone(),
                state_count: def.states.len(),
                transition_count: def.transitions.len(),
                active_instances: active_count,
                deployed_at: Utc::now().to_rfc3339(),
                deployed_by: "system".to_string(),
            }
        })
        .collect();

    // Sort
    match params.sort.as_str() {
        "created_at" => summaries.sort_by(|a, b| a.deployed_at.cmp(&b.deployed_at)),
        _ => summaries.sort_by(|a, b| a.name.cmp(&b.name)),
    }

    let total = summaries.len();
    let start = (page - 1) * per_page;
    let paginated: Vec<WorkflowSummary> = summaries
        .into_iter()
        .skip(start)
        .take(per_page)
        .collect();

    Ok(Json(WorkflowListResponse {
        workflows: paginated,
        total,
        page,
        per_page,
    }))
}

/// POST /api/workflows -- Deploy a new workflow definition.
async fn deploy_workflow(
    Extension(state): Extension<WorkflowApiState>,
    Json(body): Json<DeployWorkflowRequest>,
) -> Result<(StatusCode, Json<WorkflowSummary>), (StatusCode, Json<WorkflowApiError>)> {
    let parse_result = match body.format.as_str() {
        "json" => parse_and_validate_json(&body.definition),
        _ => parse_and_validate_yaml(&body.definition),
    };

    let (def, _warnings) = parse_result.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(WorkflowApiError {
                code: "BAD_REQUEST".into(),
                message: format!("Failed to parse workflow definition: {}", e),
                details: None,
            }),
        )
    })?;

    let all_instances = state.runtime.list_instances();
    let active_count = all_instances
        .iter()
        .filter(|i| {
            i.workflow_name == def.name
                && i.status == super::types::InstanceStatus::Active
        })
        .count();

    let summary = WorkflowSummary {
        name: def.name.clone(),
        version: def.version,
        description: def.description.clone(),
        state_count: def.states.len(),
        transition_count: def.transitions.len(),
        active_instances: active_count,
        deployed_at: Utc::now().to_rfc3339(),
        deployed_by: body.deployed_by.clone(),
    };

    let _key = state.runtime.register_definition(def);

    info!(
        name = %summary.name,
        version = %summary.version,
        "deployed workflow definition via REST API"
    );

    Ok((StatusCode::CREATED, Json(summary)))
}

/// GET /api/workflows/:name/instances -- List instances of a named workflow.
async fn list_instances(
    Extension(state): Extension<WorkflowApiState>,
    Path(name): Path<String>,
    Query(params): Query<InstanceListParams>,
) -> Result<Json<InstanceListResponse>, (StatusCode, Json<WorkflowApiError>)> {
    // Verify workflow exists
    if state.runtime.get_definition_by_name(&name).is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(WorkflowApiError {
                code: "NOT_FOUND".into(),
                message: format!("Workflow definition '{}' not found", name),
                details: None,
            }),
        ));
    }

    let per_page = clamp_per_page(params.per_page, 100, 20);
    let page = if params.page == 0 { 1 } else { params.page };

    let all_instances = state.runtime.list_instances();
    let mut filtered: Vec<_> = all_instances
        .into_iter()
        .filter(|i| i.workflow_name == name)
        .filter(|i| {
            if let Some(ref state_filter) = params.state {
                i.current_state == *state_filter
            } else {
                true
            }
        })
        .filter(|i| {
            if let Some(ref actor_filter) = params.actor {
                i.created_by == *actor_filter
            } else {
                true
            }
        })
        .filter(|i| {
            if let Some(ref status_filter) = params.status {
                i.status.to_string() == *status_filter.to_lowercase()
            } else {
                true
            }
        })
        .filter(|i| {
            if let Some(ref since) = params.since {
                if let Ok(since_dt) = chrono::DateTime::parse_from_rfc3339(since) {
                    i.created_at >= since_dt.with_timezone(&Utc)
                } else {
                    true
                }
            } else {
                true
            }
        })
        .filter(|i| {
            if let Some(ref until) = params.until {
                if let Ok(until_dt) = chrono::DateTime::parse_from_rfc3339(until) {
                    i.created_at <= until_dt.with_timezone(&Utc)
                } else {
                    true
                }
            } else {
                true
            }
        })
        .collect();

    // Sort
    match params.sort.as_str() {
        "created_at" => filtered.sort_by(|a, b| b.created_at.cmp(&a.created_at)),
        _ => filtered.sort_by(|a, b| b.updated_at.cmp(&a.updated_at)),
    }

    let total = filtered.len();
    let start = (page - 1) * per_page;
    let paginated: Vec<InstanceSummary> = filtered
        .into_iter()
        .skip(start)
        .take(per_page)
        .map(|i| InstanceSummary {
            id: i.instance_id.to_string(),
            workflow_name: i.workflow_name.clone(),
            current_state: i.current_state.clone(),
            status: i.status.to_string(),
            created_by: i.created_by.clone(),
            created_at: i.created_at.to_rfc3339(),
            updated_at: i.updated_at.to_rfc3339(),
            version: i.workflow_version,
        })
        .collect();

    Ok(Json(InstanceListResponse {
        instances: paginated,
        total,
        page,
        per_page,
    }))
}

/// POST /api/workflows/:name/instances -- Create a new workflow instance.
async fn create_instance(
    Extension(state): Extension<WorkflowApiState>,
    Path(name): Path<String>,
    Json(body): Json<CreateInstanceRequest>,
) -> Result<(StatusCode, Json<InstanceDetailResponse>), (StatusCode, Json<WorkflowApiError>)> {
    // Validate context is a JSON object
    if !body.context.is_object() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(WorkflowApiError {
                code: "BAD_REQUEST".into(),
                message: "Context must be a JSON object".into(),
                details: None,
            }),
        ));
    }

    // Find the latest version of this workflow definition
    let def = state.runtime.get_definition_by_name(&name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(WorkflowApiError {
                code: "NOT_FOUND".into(),
                message: format!("Workflow definition '{}' not found", name),
                details: None,
            }),
        )
    })?;

    let def_key = format!("{}@v{}", def.name, def.version);
    let instance = state
        .runtime
        .create_instance(&def_key, body.context, body.created_by)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WorkflowApiError {
                    code: "INTERNAL_ERROR".into(),
                    message: format!("Failed to create instance: {}", e),
                    details: None,
                }),
            )
        })?;

    info!(
        instance_id = %instance.instance_id,
        workflow = %name,
        "created workflow instance via REST API"
    );

    let detail = build_instance_detail(&instance);
    Ok((StatusCode::CREATED, Json(detail)))
}

/// GET /api/instances/:id -- Get full instance detail.
async fn get_instance(
    Extension(state): Extension<WorkflowApiState>,
    Path(id): Path<String>,
) -> Result<Json<InstanceDetailResponse>, (StatusCode, Json<WorkflowApiError>)> {
    let instance_id = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(WorkflowApiError {
                code: "BAD_REQUEST".into(),
                message: format!("Invalid instance ID: {}", id),
                details: None,
            }),
        )
    })?;

    let instance = state.runtime.get_instance(&instance_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(WorkflowApiError {
                code: "NOT_FOUND".into(),
                message: format!("Instance '{}' not found", id),
                details: None,
            }),
        )
    })?;

    Ok(Json(build_instance_detail(&instance)))
}

/// POST /api/instances/:id/transitions/:name -- Fire a named transition.
async fn fire_transition(
    Extension(state): Extension<WorkflowApiState>,
    Path((id, transition_name)): Path<(String, String)>,
    Json(body): Json<FireTransitionRequest>,
) -> Result<Json<TransitionSuccessResponse>, (StatusCode, Json<WorkflowApiError>)> {
    let instance_id = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(WorkflowApiError {
                code: "BAD_REQUEST".into(),
                message: format!("Invalid instance ID: {}", id),
                details: None,
            }),
        )
    })?;

    // Build the actor info (roles are not available from the request body alone).
    let actor = ActorInfo {
        actor_id: body.actor_id.clone(),
        roles: vec![],
        metadata: std::collections::HashMap::new(),
    };

    let req = TransitionRequest {
        instance_id,
        transition_name: transition_name.clone(),
        actor,
        expected_state: Some(body.expected_state.clone()),
        params: body.params.unwrap_or(serde_json::Value::Null),
    };

    let result = state.runtime.execute_transition(req).await;
    transition_result_to_response(result, &body.expected_state)
}

/// Map TransitionResult to an axum response.
fn transition_result_to_response(
    result: TransitionResult,
    previous_state: &str,
) -> Result<Json<TransitionSuccessResponse>, (StatusCode, Json<WorkflowApiError>)> {
    match result {
        TransitionResult::Success {
            instance,
            audit_event_id,
        } => {
            let new_state = instance.current_state.clone();
            let detail = build_instance_detail(&instance);
            Ok(Json(TransitionSuccessResponse {
                instance: detail,
                audit_event_id: audit_event_id.to_string(),
                previous_state: previous_state.to_string(),
                new_state,
            }))
        }
        TransitionResult::Blocked { reasons } => Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(WorkflowApiError {
                code: "GUARD_FAILED".into(),
                message: "Transition blocked by guard conditions".into(),
                details: Some(serde_json::json!({ "reasons": reasons })),
            }),
        )),
        TransitionResult::Conflict { current_state } => Err((
            StatusCode::CONFLICT,
            Json(WorkflowApiError {
                code: "CONFLICT".into(),
                message: format!(
                    "Stale state: expected state does not match current state '{}'",
                    current_state
                ),
                details: Some(serde_json::json!({ "current_state": current_state })),
            }),
        )),
        TransitionResult::LockTimeout => Err((
            StatusCode::LOCKED,
            Json(WorkflowApiError {
                code: "LOCKED".into(),
                message: "Could not acquire instance lock; another transition is in progress"
                    .into(),
                details: None,
            }),
        )),
        TransitionResult::NotFound { detail } => Err((
            StatusCode::NOT_FOUND,
            Json(WorkflowApiError {
                code: "NOT_FOUND".into(),
                message: detail,
                details: None,
            }),
        )),
    }
}

/// GET /api/instances/:id/available-transitions -- List available transitions.
async fn available_transitions(
    Extension(state): Extension<WorkflowApiState>,
    Path(id): Path<String>,
    Query(_params): Query<AvailableTransitionsParams>,
) -> Result<Json<AvailableTransitionsResponse>, (StatusCode, Json<WorkflowApiError>)> {
    let instance_id = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(WorkflowApiError {
                code: "BAD_REQUEST".into(),
                message: format!("Invalid instance ID: {}", id),
                details: None,
            }),
        )
    })?;

    let instance = state.runtime.get_instance(&instance_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(WorkflowApiError {
                code: "NOT_FOUND".into(),
                message: format!("Instance '{}' not found", id),
                details: None,
            }),
        )
    })?;

    let transitions = state
        .runtime
        .available_transitions(&instance_id)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WorkflowApiError {
                    code: "INTERNAL_ERROR".into(),
                    message: format!("Failed to get available transitions: {}", e),
                    details: None,
                }),
            )
        })?;

    let transition_infos: Vec<TransitionInfo> = transitions
        .iter()
        .map(|t| TransitionInfo {
            name: t.name.clone(),
            to_state: t.to.clone(),
            guards: t.guards.iter().map(|g| g.condition.clone()).collect(),
            required_roles: vec![],
            min_approvers: 1,
            description: format!("{} -> {}", t.from, t.to),
        })
        .collect();

    Ok(Json(AvailableTransitionsResponse {
        instance_id: id,
        current_state: instance.current_state.clone(),
        transitions: transition_infos,
    }))
}

/// GET /api/instances/:id/history -- Full transition history.
async fn instance_history(
    Extension(state): Extension<WorkflowApiState>,
    Path(id): Path<String>,
    Query(params): Query<HistoryParams>,
) -> Result<Json<InstanceHistoryResponse>, (StatusCode, Json<WorkflowApiError>)> {
    let instance_id = Uuid::parse_str(&id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(WorkflowApiError {
                code: "BAD_REQUEST".into(),
                message: format!("Invalid instance ID: {}", id),
                details: None,
            }),
        )
    })?;

    let instance = state.runtime.get_instance(&instance_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(WorkflowApiError {
                code: "NOT_FOUND".into(),
                message: format!("Instance '{}' not found", id),
                details: None,
            }),
        )
    })?;

    let per_page = clamp_per_page(params.per_page, 200, 50);
    let page = if params.page == 0 { 1 } else { params.page };

    let mut history: Vec<(usize, &super::types::TransitionRecord)> = instance
        .history
        .iter()
        .enumerate()
        .collect();

    // Sort
    match params.sort.as_str() {
        "asc" => history.sort_by_key(|(seq, _)| *seq),
        _ => history.sort_by(|(a, _), (b, _)| b.cmp(a)),
    }

    let total = history.len();
    let start = (page - 1) * per_page;
    let paginated: Vec<HistoryEntryDetail> = history
        .into_iter()
        .skip(start)
        .take(per_page)
        .map(|(seq, record)| HistoryEntryDetail {
            sequence: seq + 1,
            transition_name: record.transition_name.clone(),
            from_state: record.from_state.clone(),
            to_state: record.to_state.clone(),
            actor_id: record.actor_id.clone(),
            timestamp: record.timestamp.to_rfc3339(),
            audit_event_id: record.audit_event_id.to_string(),
            audit_event_url: format!(
                "/api/v1/audit/events/{}",
                record.audit_event_id
            ),
            context_snapshot: if params.include_context {
                Some(record.context_diff.clone())
            } else {
                None
            },
        })
        .collect();

    Ok(Json(InstanceHistoryResponse {
        instance_id: id,
        entries: paginated,
        total,
        page,
        per_page,
    }))
}

/// GET /api/plugins -- List all loaded plugins (stub).
async fn list_plugins() -> Json<PluginListResponse> {
    // Stub: returns empty list until W4B provides PluginRegistry.
    Json(PluginListResponse {
        plugins: vec![],
        total: 0,
    })
}

/// GET /api/plugins/:name/describe -- Describe a plugin (stub).
async fn describe_plugin(
    Path(name): Path<String>,
) -> Result<Json<PluginDescribeResponse>, (StatusCode, Json<WorkflowApiError>)> {
    // Stub: always returns 404 until W4B provides PluginRegistry.
    Err((
        StatusCode::NOT_FOUND,
        Json(WorkflowApiError {
            code: "NOT_FOUND".into(),
            message: format!("Plugin '{}' not found or not loaded", name),
            details: None,
        }),
    ))
}

// ============================================================================
// Route Builder
// ============================================================================

/// Build the workflow API router.
///
/// All routes are prefixed with `/api` and share WorkflowApiState via Extension.
pub fn workflow_api_routes(state: WorkflowApiState) -> Router {
    Router::new()
        // Workflow definitions
        .route("/api/workflows", get(list_workflows).post(deploy_workflow))
        // Workflow instances (scoped by workflow name)
        .route(
            "/api/workflows/:name/instances",
            get(list_instances).post(create_instance),
        )
        // Instance detail
        .route("/api/instances/:id", get(get_instance))
        // Transitions
        .route(
            "/api/instances/:id/transitions/:name",
            post(fire_transition),
        )
        .route(
            "/api/instances/:id/available-transitions",
            get(available_transitions),
        )
        // History
        .route("/api/instances/:id/history", get(instance_history))
        // Plugins (stubs until W4B)
        .route("/api/plugins", get(list_plugins))
        .route("/api/plugins/:name/describe", get(describe_plugin))
        // Shared state
        .layer(Extension(state))
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::workflow::types::*;
    use std::collections::HashMap;

    // ========================================================================
    // TransitionResult-to-response mapping tests
    // ========================================================================

    #[test]
    fn test_transition_result_to_response_success() {
        let now = Utc::now();
        let instance = WorkflowInstance {
            instance_id: Uuid::now_v7(),
            workflow_name: "test-wf".to_string(),
            workflow_version: 1,
            current_state: "in_review".to_string(),
            context: serde_json::json!({}),
            history: vec![],
            created_at: now,
            updated_at: now,
            created_by: "alice".to_string(),
            status: InstanceStatus::Active,
            assigned_actors: HashMap::new(),
            timers: vec![],
            locked_by: None,
        };

        let result = TransitionResult::Success {
            instance,
            audit_event_id: Uuid::now_v7(),
        };

        let resp = transition_result_to_response(result, "draft");
        assert!(resp.is_ok());
        let body = resp.unwrap().0;
        assert_eq!(body.previous_state, "draft");
        assert_eq!(body.new_state, "in_review");
    }

    #[test]
    fn test_transition_result_to_response_conflict() {
        let result = TransitionResult::Conflict {
            current_state: "in_review".to_string(),
        };
        let resp = transition_result_to_response(result, "draft");
        assert!(resp.is_err());
        let (status, body) = resp.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.code, "CONFLICT");
        assert!(body.details.is_some());
    }

    #[test]
    fn test_transition_result_to_response_blocked() {
        let result = TransitionResult::Blocked {
            reasons: vec!["guard 'has_content' not satisfied".to_string()],
        };
        let resp = transition_result_to_response(result, "draft");
        assert!(resp.is_err());
        let (status, body) = resp.unwrap_err();
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body.code, "GUARD_FAILED");
        assert!(body.details.is_some());
    }

    #[test]
    fn test_transition_result_to_response_locked() {
        let result = TransitionResult::LockTimeout;
        let resp = transition_result_to_response(result, "draft");
        assert!(resp.is_err());
        let (status, body) = resp.unwrap_err();
        assert_eq!(status, StatusCode::LOCKED);
        assert_eq!(body.code, "LOCKED");
    }

    #[test]
    fn test_transition_result_to_response_not_found() {
        let result = TransitionResult::NotFound {
            detail: "instance xyz not found".to_string(),
        };
        let resp = transition_result_to_response(result, "draft");
        assert!(resp.is_err());
        let (status, body) = resp.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.code, "NOT_FOUND");
    }

    // ========================================================================
    // RBAC helper tests
    // ========================================================================

    #[test]
    fn test_require_role_success() {
        let actor = ActorInfo {
            actor_id: "alice".to_string(),
            roles: vec!["workflow:deploy".to_string()],
            metadata: HashMap::new(),
        };
        assert!(require_role(&actor, "workflow:deploy").is_ok());
    }

    #[test]
    fn test_require_role_admin_bypass() {
        let actor = ActorInfo {
            actor_id: "superadmin".to_string(),
            roles: vec!["admin".to_string()],
            metadata: HashMap::new(),
        };
        assert!(require_role(&actor, "workflow:deploy").is_ok());
        assert!(require_role(&actor, "workflow:create").is_ok());
        assert!(require_role(&actor, "anything").is_ok());
    }

    #[test]
    fn test_require_role_forbidden() {
        let actor = ActorInfo {
            actor_id: "bob".to_string(),
            roles: vec!["viewer".to_string()],
            metadata: HashMap::new(),
        };
        let result = require_role(&actor, "workflow:deploy");
        assert!(result.is_err());
        let (status, body) = result.unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body.code, "FORBIDDEN");
        assert!(body.message.contains("bob"));
    }

    // ========================================================================
    // Pagination helper tests
    // ========================================================================

    #[test]
    fn test_clamp_per_page_within_bounds() {
        assert_eq!(clamp_per_page(10, 100, 20), 10);
        assert_eq!(clamp_per_page(50, 100, 20), 50);
        assert_eq!(clamp_per_page(100, 100, 20), 100);
        assert_eq!(clamp_per_page(1, 100, 20), 1);
    }

    #[test]
    fn test_clamp_per_page_exceeds_max() {
        assert_eq!(clamp_per_page(200, 100, 20), 100);
        assert_eq!(clamp_per_page(999, 100, 20), 100);
        assert_eq!(clamp_per_page(500, 200, 50), 200);
    }

    #[test]
    fn test_clamp_per_page_zero_returns_default() {
        assert_eq!(clamp_per_page(0, 100, 20), 20);
        assert_eq!(clamp_per_page(0, 200, 50), 50);
    }

    // ========================================================================
    // Request/Response serde tests
    // ========================================================================

    #[test]
    fn test_create_instance_request_deser() {
        let json = serde_json::json!({
            "context": {"document_id": "doc-123", "author": "alice"},
            "created_by": "alice"
        });
        let req: CreateInstanceRequest = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(req.created_by, "alice");
        assert!(req.context.is_object());
        assert_eq!(
            req.context.get("document_id").unwrap().as_str().unwrap(),
            "doc-123"
        );
        // Round-trip
        let serialized = serde_json::to_value(&req).unwrap();
        assert_eq!(serialized["created_by"], "alice");
    }

    #[test]
    fn test_fire_transition_request_deser() {
        let json = serde_json::json!({
            "actor_id": "bob",
            "expected_state": "draft",
            "params": {"comment": "looks good"}
        });
        let req: FireTransitionRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.actor_id, "bob");
        assert_eq!(req.expected_state, "draft");
        assert!(req.params.is_some());
        assert_eq!(
            req.params.as_ref().unwrap()["comment"].as_str().unwrap(),
            "looks good"
        );
    }

    #[test]
    fn test_fire_transition_request_no_params() {
        let json = serde_json::json!({
            "actor_id": "carol",
            "expected_state": "in_review"
        });
        let req: FireTransitionRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.actor_id, "carol");
        assert!(req.params.is_none());
    }

    #[test]
    fn test_workflow_api_error_serde() {
        let err = WorkflowApiError {
            code: "NOT_FOUND".into(),
            message: "Instance not found".into(),
            details: None,
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["code"], "NOT_FOUND");
        assert_eq!(json["message"], "Instance not found");
        // details should be skipped when None
        assert!(json.get("details").is_none());
    }

    #[test]
    fn test_workflow_api_error_with_details() {
        let err = WorkflowApiError {
            code: "GUARD_FAILED".into(),
            message: "Blocked".into(),
            details: Some(serde_json::json!({"reasons": ["role check failed"]})),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert!(json.get("details").is_some());
        assert_eq!(json["details"]["reasons"][0], "role check failed");
    }

    #[test]
    fn test_deploy_workflow_request_defaults() {
        let json = serde_json::json!({
            "definition": "name: test\nversion: 1"
        });
        let req: DeployWorkflowRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.format, "yaml");
        assert_eq!(req.deployed_by, "");
    }

    #[test]
    fn test_instance_detail_response_serde() {
        let detail = InstanceDetailResponse {
            id: "abc-123".into(),
            workflow_name: "test-wf".into(),
            definition_version: 2,
            current_state: "draft".into(),
            status: "active".into(),
            context: serde_json::json!({}),
            version: 2,
            created_by: "alice".into(),
            created_at: "2025-01-01T00:00:00Z".into(),
            updated_at: "2025-01-01T00:00:00Z".into(),
            recent_history: vec![],
            active_timers: vec![],
            transition_count: 0,
        };
        let json = serde_json::to_value(&detail).unwrap();
        assert_eq!(json["id"], "abc-123");
        assert_eq!(json["definition_version"], 2);
        assert_eq!(json["transition_count"], 0);
    }

    #[test]
    fn test_build_instance_detail() {
        let now = Utc::now();
        let audit_id = Uuid::now_v7();
        let instance = WorkflowInstance {
            instance_id: Uuid::now_v7(),
            workflow_name: "doc-approval".to_string(),
            workflow_version: 1,
            current_state: "in_review".to_string(),
            context: serde_json::json!({"doc": "test"}),
            history: vec![TransitionRecord {
                transition_name: "submit".to_string(),
                from_state: "draft".to_string(),
                to_state: "in_review".to_string(),
                actor_id: "alice".to_string(),
                timestamp: now,
                node_id: "node-1".to_string(),
                audit_event_id: audit_id,
                context_diff: serde_json::json!({}),
            }],
            created_at: now,
            updated_at: now,
            created_by: "alice".to_string(),
            status: InstanceStatus::Active,
            assigned_actors: HashMap::new(),
            timers: vec![],
            locked_by: None,
        };

        let detail = build_instance_detail(&instance);
        assert_eq!(detail.workflow_name, "doc-approval");
        assert_eq!(detail.current_state, "in_review");
        assert_eq!(detail.transition_count, 1);
        assert_eq!(detail.recent_history.len(), 1);
        assert_eq!(detail.recent_history[0].transition_name, "submit");
        assert_eq!(detail.recent_history[0].audit_event_id, audit_id.to_string());
        assert_eq!(detail.active_timers.len(), 0);
    }

    #[test]
    fn test_transition_info_serde() {
        let info = TransitionInfo {
            name: "submit".into(),
            to_state: "in_review".into(),
            guards: vec!["has_content".into()],
            required_roles: vec![],
            min_approvers: 1,
            description: "draft -> in_review".into(),
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["name"], "submit");
        assert_eq!(json["guards"][0], "has_content");
        assert_eq!(json["min_approvers"], 1);
    }

    #[test]
    fn test_history_entry_detail_serde() {
        let entry = HistoryEntryDetail {
            sequence: 1,
            transition_name: "submit".into(),
            from_state: "draft".into(),
            to_state: "in_review".into(),
            actor_id: "alice".into(),
            timestamp: "2025-01-01T00:00:00Z".into(),
            audit_event_id: "abc-123".into(),
            audit_event_url: "/api/v1/audit/events/abc-123".into(),
            context_snapshot: None,
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["sequence"], 1);
        assert_eq!(json["audit_event_url"], "/api/v1/audit/events/abc-123");
    }

    #[test]
    fn test_plugin_list_response_serde() {
        let resp = PluginListResponse {
            plugins: vec![],
            total: 0,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["total"], 0);
        assert!(json["plugins"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_workflow_list_response_serde() {
        let resp = WorkflowListResponse {
            workflows: vec![WorkflowSummary {
                name: "test".into(),
                version: 1,
                description: "A test workflow".into(),
                state_count: 3,
                transition_count: 2,
                active_instances: 0,
                deployed_at: "2025-01-01T00:00:00Z".into(),
                deployed_by: "admin".into(),
            }],
            total: 1,
            page: 1,
            per_page: 20,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["total"], 1);
        assert_eq!(json["workflows"][0]["name"], "test");
        assert_eq!(json["workflows"][0]["state_count"], 3);
    }

    #[test]
    fn test_instance_list_params_defaults() {
        let json = serde_json::json!({});
        let params: InstanceListParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.page, 1);
        assert_eq!(params.per_page, 20);
        assert_eq!(params.sort, "updated_at");
        assert!(params.state.is_none());
        assert!(params.actor.is_none());
        assert!(params.status.is_none());
    }

    #[test]
    fn test_history_params_defaults() {
        let json = serde_json::json!({});
        let params: HistoryParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.page, 1);
        assert_eq!(params.per_page, 50);
        assert_eq!(params.sort, "desc");
        assert!(!params.include_context);
    }

    #[test]
    fn test_workflow_list_params_defaults() {
        let json = serde_json::json!({});
        let params: WorkflowListParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.page, 1);
        assert_eq!(params.per_page, 20);
        assert_eq!(params.sort, "name");
    }

    #[test]
    fn test_available_transitions_params_empty() {
        let json = serde_json::json!({});
        let params: AvailableTransitionsParams = serde_json::from_value(json).unwrap();
        assert!(params.actor_id.is_none());
    }

    #[test]
    fn test_available_transitions_params_with_actor() {
        let json = serde_json::json!({"actor_id": "alice"});
        let params: AvailableTransitionsParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.actor_id.as_deref(), Some("alice"));
    }

    #[test]
    fn test_instance_summary_serde() {
        let summary = InstanceSummary {
            id: "inst-1".into(),
            workflow_name: "invoice-approval".into(),
            current_state: "draft".into(),
            status: "active".into(),
            created_by: "alice".into(),
            created_at: "2025-06-01T00:00:00Z".into(),
            updated_at: "2025-06-01T00:00:00Z".into(),
            version: 1,
        };
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["id"], "inst-1");
        assert_eq!(json["workflow_name"], "invoice-approval");
        assert_eq!(json["version"], 1);
    }

    #[test]
    fn test_require_role_empty_roles() {
        let actor = ActorInfo {
            actor_id: "anon".to_string(),
            roles: vec![],
            metadata: HashMap::new(),
        };
        let result = require_role(&actor, "workflow:deploy");
        assert!(result.is_err());
    }
}
