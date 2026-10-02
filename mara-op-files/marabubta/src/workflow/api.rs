// Marabunta - Licensed under the MIT License.
//! Workflow REST API
//!
//! HTTP endpoints for workflow management and execution.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

use crate::workflow::engine::{JobSubmitter, WorkflowExecutor};
use crate::workflow::persistence::{RunSummary, WorkflowStore, WorkflowSummary};
use crate::workflow::types::*;

// ============================================================================
// API State
// ============================================================================

/// State for the workflow API
pub struct WorkflowApiState<S: WorkflowStore + 'static, J: JobSubmitter + 'static> {
    /// Workflow executor
    pub executor: Arc<WorkflowExecutor<S, J>>,

    /// Workflow store (for direct access)
    pub store: Arc<S>,
}

impl<S: WorkflowStore + 'static, J: JobSubmitter + 'static> Clone for WorkflowApiState<S, J> {
    fn clone(&self) -> Self {
        Self {
            executor: Arc::clone(&self.executor),
            store: Arc::clone(&self.store),
        }
    }
}

// ============================================================================
// Router
// ============================================================================

/// Create the workflow API router
pub fn create_workflow_router<S: WorkflowStore, J: JobSubmitter + 'static>(
    state: WorkflowApiState<S, J>,
) -> Router {
    Router::new()
        // Workflow definition endpoints
        .route("/api/workflows", get(list_workflows::<S, J>))
        .route("/api/workflows", post(create_workflow::<S, J>))
        .route("/api/workflows/:id", get(get_workflow::<S, J>))
        .route("/api/workflows/:id", delete(delete_workflow::<S, J>))
        // Workflow execution endpoints
        .route("/api/workflows/:id/run", post(run_workflow::<S, J>))
        .route("/api/workflows/:id/runs", get(list_workflow_runs::<S, J>))
        // Run management endpoints
        .route("/api/runs/:id/status", get(get_run_status::<S, J>))
        .route("/api/runs/:id/cancel", post(cancel_run::<S, J>))
        .route("/api/runs", get(list_active_runs::<S, J>))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to create a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWorkflowRequest {
    /// Workflow definition (can be full JSON or YAML string)
    #[serde(flatten)]
    pub workflow: Workflow,
}

/// Response after creating a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWorkflowResponse {
    pub id: WorkflowId,
    pub name: String,
    pub message: String,
}

/// Request to run a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunWorkflowRequest {
    /// Override variables for this run
    #[serde(default)]
    pub variables: Option<HashMap<String, serde_json::Value>>,

    /// Run metadata
    #[serde(default)]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

/// Response after starting a workflow run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunWorkflowResponse {
    pub run_id: WorkflowRunId,
    pub workflow_id: WorkflowId,
    pub status: WorkflowStatus,
    pub message: String,
}

/// Query parameters for listing workflows
#[derive(Debug, Clone, Deserialize)]
pub struct ListWorkflowsQuery {
    /// Filter by name (partial match)
    pub name: Option<String>,
    /// Filter by tag
    pub tag: Option<String>,
    /// Pagination limit
    pub limit: Option<usize>,
    /// Pagination offset
    pub offset: Option<usize>,
}

/// Query parameters for listing runs
#[derive(Debug, Clone, Deserialize)]
pub struct ListRunsQuery {
    /// Filter by status
    pub status: Option<WorkflowStatus>,
    /// Pagination limit
    pub limit: Option<usize>,
    /// Pagination offset
    pub offset: Option<usize>,
}

/// Workflow list response
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowListResponse {
    pub workflows: Vec<WorkflowSummary>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

/// Run list response
#[derive(Debug, Clone, Serialize)]
pub struct RunListResponse {
    pub runs: Vec<RunSummary>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

/// Detailed run status response
#[derive(Debug, Clone, Serialize)]
pub struct RunStatusResponse {
    pub id: WorkflowRunId,
    pub workflow_id: WorkflowId,
    pub workflow_version: String,
    pub status: WorkflowStatus,
    pub progress: f64,
    pub variables: HashMap<String, serde_json::Value>,
    pub step_states: HashMap<String, StepState>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

impl From<WorkflowRun> for RunStatusResponse {
    fn from(run: WorkflowRun) -> Self {
        let progress = run.progress();
        Self {
            id: run.id,
            workflow_id: run.workflow_id,
            workflow_version: run.workflow_version,
            status: run.status,
            progress,
            variables: run.variables,
            step_states: run.step_states,
            started_at: run.started_at,
            completed_at: run.completed_at,
            error: run.error,
        }
    }
}

// ============================================================================
// API Error
// ============================================================================

/// API error type
#[derive(Debug, thiserror::Error)]
pub enum WorkflowApiError {
    #[error("Workflow not found: {0}")]
    WorkflowNotFound(String),

    #[error("Run not found: {0}")]
    RunNotFound(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Bad request: {0}")]
    BadRequest(String),
}

impl IntoResponse for WorkflowApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match &self {
            WorkflowApiError::WorkflowNotFound(_) | WorkflowApiError::RunNotFound(_) => {
                (StatusCode::NOT_FOUND, self.to_string())
            }
            WorkflowApiError::Validation(_) | WorkflowApiError::BadRequest(_) => {
                (StatusCode::BAD_REQUEST, self.to_string())
            }
            WorkflowApiError::Execution(_) | WorkflowApiError::Storage(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
        };

        let body = Json(serde_json::json!({
            "error": message,
            "status": status.as_u16(),
        }));

        (status, body).into_response()
    }
}

// ============================================================================
// Handlers
// ============================================================================

/// POST /api/workflows - Create a new workflow
async fn create_workflow<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Json(mut request): Json<CreateWorkflowRequest>,
) -> Result<Json<CreateWorkflowResponse>, WorkflowApiError> {
    // Assign ID if not set
    if request.workflow.id.0 == uuid::Uuid::nil() {
        request.workflow.id = WorkflowId::new();
    }

    // Validate
    request
        .workflow
        .validate()
        .map_err(|e| WorkflowApiError::Validation(e.to_string()))?;

    // Save
    state
        .store
        .save_workflow(&request.workflow)
        .await
        .map_err(|e| {
            error!("Failed to save workflow: {}", e);
            WorkflowApiError::Storage(e.to_string())
        })?;

    info!(
        "Created workflow: {} ({})",
        request.workflow.name, request.workflow.id
    );

    Ok(Json(CreateWorkflowResponse {
        id: request.workflow.id,
        name: request.workflow.name.clone(),
        message: "Workflow created successfully".to_string(),
    }))
}

/// GET /api/workflows - List workflows
async fn list_workflows<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Query(query): Query<ListWorkflowsQuery>,
) -> Result<Json<WorkflowListResponse>, WorkflowApiError> {
    let workflows = if let Some(name) = &query.name {
        state.store.find_workflows_by_name(name).await
    } else if let Some(tag) = &query.tag {
        state.store.find_workflows_by_tag(tag).await
    } else {
        state.store.list_workflows().await
    }
    .map_err(|e| WorkflowApiError::Storage(e.to_string()))?;

    let total = workflows.len();
    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);

    let workflows: Vec<_> = workflows.into_iter().skip(offset).take(limit).collect();

    Ok(Json(WorkflowListResponse {
        workflows,
        total,
        limit,
        offset,
    }))
}

/// GET /api/workflows/:id - Get workflow details
async fn get_workflow<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Path(id): Path<String>,
) -> Result<Json<Workflow>, WorkflowApiError> {
    let workflow_id: WorkflowId = parse_workflow_id(&id)?;

    let workflow = state
        .store
        .get_workflow(workflow_id)
        .await
        .map_err(|e| WorkflowApiError::Storage(e.to_string()))?
        .ok_or(WorkflowApiError::WorkflowNotFound(id))?;

    Ok(Json(workflow))
}

/// DELETE /api/workflows/:id - Delete a workflow
async fn delete_workflow<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, WorkflowApiError> {
    let workflow_id = parse_workflow_id(&id)?;

    let deleted = state
        .store
        .delete_workflow(workflow_id)
        .await
        .map_err(|e| WorkflowApiError::Storage(e.to_string()))?;

    if deleted {
        info!("Deleted workflow: {}", id);
        Ok(Json(serde_json::json!({
            "success": true,
            "message": "Workflow deleted"
        })))
    } else {
        Err(WorkflowApiError::WorkflowNotFound(id))
    }
}

/// POST /api/workflows/:id/run - Start a workflow run
async fn run_workflow<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Path(id): Path<String>,
    Json(request): Json<RunWorkflowRequest>,
) -> Result<Json<RunWorkflowResponse>, WorkflowApiError> {
    let workflow_id = parse_workflow_id(&id)?;

    // Start the workflow
    let run_id = state
        .executor
        .start_workflow(workflow_id, request.variables)
        .await
        .map_err(|e| {
            error!("Failed to start workflow {}: {}", id, e);
            WorkflowApiError::Execution(e.to_string())
        })?;

    info!("Started workflow run: {} for workflow {}", run_id, id);

    Ok(Json(RunWorkflowResponse {
        run_id,
        workflow_id,
        status: WorkflowStatus::Running,
        message: "Workflow started successfully".to_string(),
    }))
}

/// GET /api/workflows/:id/runs - List runs for a workflow
async fn list_workflow_runs<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Path(id): Path<String>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<RunListResponse>, WorkflowApiError> {
    let workflow_id = parse_workflow_id(&id)?;

    let runs = state
        .store
        .list_runs_for_workflow(workflow_id)
        .await
        .map_err(|e| WorkflowApiError::Storage(e.to_string()))?;

    let total = runs.len();
    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);

    let runs: Vec<_> = runs
        .into_iter()
        .filter(|r| query.status.map(|s| r.status == s).unwrap_or(true))
        .skip(offset)
        .take(limit)
        .collect();

    Ok(Json(RunListResponse {
        runs,
        total,
        limit,
        offset,
    }))
}

/// GET /api/runs/:id/status - Get run status
async fn get_run_status<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Path(id): Path<String>,
) -> Result<Json<RunStatusResponse>, WorkflowApiError> {
    let run_id = parse_run_id(&id)?;

    let run = state.executor.get_run_status(run_id).await.map_err(|e| {
        if e.to_string().contains("not found") {
            WorkflowApiError::RunNotFound(id.clone())
        } else {
            WorkflowApiError::Storage(e.to_string())
        }
    })?;

    Ok(Json(RunStatusResponse::from(run)))
}

/// POST /api/runs/:id/cancel - Cancel a run
async fn cancel_run<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, WorkflowApiError> {
    let run_id = parse_run_id(&id)?;

    state
        .executor
        .cancel_workflow(run_id)
        .await
        .map_err(|e| WorkflowApiError::Execution(e.to_string()))?;

    info!("Cancelled workflow run: {}", id);

    Ok(Json(serde_json::json!({
        "success": true,
        "message": "Workflow run cancelled"
    })))
}

/// GET /api/runs - List active runs
async fn list_active_runs<S: WorkflowStore + 'static, J: JobSubmitter + 'static>(
    State(state): State<WorkflowApiState<S, J>>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<RunListResponse>, WorkflowApiError> {
    let runs = if let Some(status) = query.status {
        state.store.get_runs_by_status(status).await
    } else {
        state.store.list_active_runs().await
    }
    .map_err(|e| WorkflowApiError::Storage(e.to_string()))?;

    let total = runs.len();
    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);

    let runs: Vec<_> = runs.into_iter().skip(offset).take(limit).collect();

    Ok(Json(RunListResponse {
        runs,
        total,
        limit,
        offset,
    }))
}

// ============================================================================
// Helpers
// ============================================================================

/// Parse a workflow ID from string
fn parse_workflow_id(s: &str) -> Result<WorkflowId, WorkflowApiError> {
    // Try parsing as UUID directly
    if let Ok(uuid) = uuid::Uuid::parse_str(s) {
        return Ok(WorkflowId(uuid));
    }

    // Try stripping "wf-" prefix
    if let Some(stripped) = s.strip_prefix("wf-") {
        if let Ok(uuid) = uuid::Uuid::parse_str(stripped) {
            return Ok(WorkflowId(uuid));
        }
    }

    Err(WorkflowApiError::BadRequest(format!(
        "Invalid workflow ID: {}",
        s
    )))
}

/// Parse a run ID from string
fn parse_run_id(s: &str) -> Result<WorkflowRunId, WorkflowApiError> {
    // Try parsing as UUID directly
    if let Ok(uuid) = uuid::Uuid::parse_str(s) {
        return Ok(WorkflowRunId(uuid));
    }

    // Try stripping "run-" prefix
    if let Some(stripped) = s.strip_prefix("run-") {
        if let Ok(uuid) = uuid::Uuid::parse_str(stripped) {
            return Ok(WorkflowRunId(uuid));
        }
    }

    Err(WorkflowApiError::BadRequest(format!(
        "Invalid run ID: {}",
        s
    )))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::engine::mock::MockJobSubmitter;
    use crate::workflow::engine::WorkflowExecutorConfig;
    use crate::workflow::persistence::MemoryWorkflowStore;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn create_test_state() -> WorkflowApiState<MemoryWorkflowStore, MockJobSubmitter> {
        let store = Arc::new(MemoryWorkflowStore::new());
        let submitter = Arc::new(MockJobSubmitter::new());
        let config = WorkflowExecutorConfig::default();
        let executor = Arc::new(WorkflowExecutor::new(Arc::clone(&store), submitter, config));

        WorkflowApiState { executor, store }
    }

    #[tokio::test]
    async fn test_create_workflow_api() {
        let state = create_test_state();
        let app = create_workflow_router(state);

        let workflow_json = serde_json::json!({
            "name": "test-workflow",
            "steps": [
                {
                    "id": "step1",
                    "type": "job",
                    "job": {
                        "name": "Test Job",
                        "runtime": "python3",
                        "script": "print('hello')"
                    }
                }
            ]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/workflows")
                    .header("content-type", "application/json")
                    .body(Body::from(workflow_json.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_workflows_api() {
        let state = create_test_state();

        // Add a workflow
        let mut workflow = Workflow::new("test");
        workflow.add_step(WorkflowStep::job(
            "s1",
            JobStepConfig::new("Job", "py").with_script("x"),
        ));
        state.store.save_workflow(&workflow).await.unwrap();

        let app = create_workflow_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/workflows")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_workflow_not_found() {
        let state = create_test_state();
        let app = create_workflow_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/workflows/00000000-0000-0000-0000-000000000000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_parse_workflow_id() {
        // Full UUID
        let id = parse_workflow_id("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(id.0.to_string(), "550e8400-e29b-41d4-a716-446655440000");

        // With prefix (note: this expects the full UUID after prefix)
        // For short IDs, we'd need a different lookup mechanism
    }
}
