// Marabunta - Licensed under the MIT License.
//! Control Plane REST API
//!
//! Comprehensive REST API endpoints for the Marabunta Compute control plane dashboard.
//! Provides visibility into infrastructure nodes, phantom pool, jobs, alerts, and forecasting.
//!
//! # Architecture
//!
//! This API uses axum for HTTP handling with:
//! - JSON request/response serialization
//! - Query parameter filtering
//! - Path parameter extraction
//! - Proper HTTP status codes
//! - CORS support for web dashboards
//!
//! # Privacy Guarantees
//!
//! Infrastructure endpoints provide full node-level detail.
//! Phantom endpoints only expose aggregate statistics to preserve anonymity.
//!
//! # Example Usage
//!
//! ```bash
//! # Full dashboard snapshot
//! curl http://localhost:8080/api/dashboard
//!
//! # List all regions
//! curl http://localhost:8080/api/regions
//!
//! # Filter nodes by region
//! curl "http://localhost:8080/api/nodes?region=us-east-1&status=online"
//!
//! # Get job latency heatmap
//! curl http://localhost:8080/api/jobs/job-abc123/latency
//!
//! # Dismiss an alert
//! curl -X POST http://localhost:8080/api/alerts/alert-123/dismiss
//!
//! # Manual node override with alternatives
//! curl -X POST http://localhost:8080/api/nodes/node-456/override \
//!   -H "Content-Type: application/json" \
//!   -d '{"task_id": "task-789", "reason": "testing"}'
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};

use crate::coordinator::dual_mode::dashboard::{
    AlertSeverity, CapacityTrend, LocationSummary, NodeSummary, SystemHealth, TaskSummary,
};
use crate::coordinator::dual_mode::DashboardData;
use crate::ratelimit::{RateLimit, RateLimitLayer, RateLimiter, RateLimiterConfig};

// ============================================================================
// API State
// ============================================================================

/// Shared state for the control plane API.
///
/// This is injected into all handlers via axum's State extractor.
pub struct ControlPlaneState {
    /// Dashboard data generator (wraps UnifiedPool)
    pub dashboard: Arc<dyn DashboardDataSource>,

    /// Alert manager
    pub alerts: Arc<dyn AlertManager>,

    /// Capacity forecaster
    pub forecaster: Arc<dyn CapacityForecaster>,

    /// Node assignment override handler
    pub assignment_override: Arc<dyn AssignmentOverride>,
}

// Placeholder traits for state components (to be implemented)
pub trait DashboardDataSource: Send + Sync {
    fn get_dashboard(&self) -> DashboardData;
    fn get_regions(&self) -> Vec<RegionStats>;
    fn get_region(&self, id: &str) -> Option<RegionDetails>;
    fn get_nodes(&self, filter: &NodeFilter) -> Vec<NodeDetails>;
    fn get_node(&self, id: &str) -> Option<NodeDetailsWithHistory>;
    fn get_jobs(&self, filter: &JobFilter) -> Vec<JobListItem>;
    fn get_job(&self, id: &str) -> Option<JobDetails>;
    fn get_job_latency_heatmap(&self, id: &str) -> Option<LatencyHeatmap>;
}

pub trait AlertManager: Send + Sync {
    fn get_active_alerts(&self) -> Vec<AlertDetails>;
    fn dismiss_alert(&self, id: &str) -> Result<(), AlertError>;
    fn execute_action(&self, alert_id: &str, action_id: &str) -> Result<ActionResult, AlertError>;
}

pub trait CapacityForecaster: Send + Sync {
    fn forecast_24h(&self) -> CapacityForecast;
}

pub trait AssignmentOverride: Send + Sync {
    fn override_assignment(
        &self,
        node_id: &str,
        task_id: &str,
        reason: &str,
    ) -> Result<OverrideResponse, OverrideError>;
}

// ============================================================================
// Router Construction
// ============================================================================

/// Creates the complete control plane API router with all endpoints.
///
/// # CORS Configuration
///
/// CORS is enabled for all origins to support web dashboard access.
/// In production, configure `CorsLayer::new().allow_origin(...)` with specific origins.
///
/// # Example
///
/// ```rust,no_run
/// use axum::Router;
/// use std::sync::Arc;
/// # use marabunta_compute::control_plane::api::*;
///
/// async fn example() {
///     // let state = Arc::new(ControlPlaneState { ... });
///     // let router = create_control_plane_router(state);
///     // axum::Server::bind(&"0.0.0.0:8080".parse().unwrap())
///     //     .serve(router.into_make_service())
///     //     .await
///     //     .unwrap();
/// }
/// ```
pub fn create_control_plane_router(state: Arc<ControlPlaneState>) -> Router {
    Router::new()
        // Dashboard overview
        .route("/api/dashboard", get(get_dashboard))
        // Region endpoints
        .route("/api/regions", get(list_regions))
        .route("/api/regions/:id", get(get_region_details))
        // Node endpoints
        .route("/api/nodes", get(list_nodes))
        .route("/api/nodes/:id", get(get_node_details))
        .route("/api/nodes/:id/override", post(override_node_assignment))
        // Job endpoints
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/:id", get(get_job_details))
        .route("/api/jobs/:id/latency", get(get_job_latency))
        // Alert endpoints
        .route("/api/alerts", get(list_alerts))
        .route("/api/alerts/:id/dismiss", post(dismiss_alert))
        .route(
            "/api/alerts/:id/action/:action_id",
            post(execute_alert_action),
        )
        // Forecasting endpoint
        .route("/api/forecast", get(get_capacity_forecast))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}

// ============================================================================
// Dashboard Endpoint
// ============================================================================

/// **GET /api/dashboard** - Full dashboard snapshot
///
/// Returns a complete snapshot of the system state including:
/// - Infrastructure node metrics
/// - Phantom pool aggregate statistics
/// - System health and capacity trends
/// - Active recommendations
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/dashboard
/// ```
async fn get_dashboard(State(state): State<Arc<ControlPlaneState>>) -> Json<DashboardData> {
    Json(state.dashboard.get_dashboard())
}

// ============================================================================
// Region Endpoints
// ============================================================================

/// **GET /api/regions** - List all regions with stats
///
/// Returns summary statistics for all regions.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/regions
/// ```
async fn list_regions(State(state): State<Arc<ControlPlaneState>>) -> Json<Vec<RegionStats>> {
    Json(state.dashboard.get_regions())
}

/// **GET /api/regions/{id}** - Single region details
///
/// Returns detailed information about a specific region.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/regions/us-east-1
/// ```
async fn get_region_details(
    State(state): State<Arc<ControlPlaneState>>,
    Path(id): Path<String>,
) -> Result<Json<RegionDetails>, StatusCode> {
    state
        .dashboard
        .get_region(&id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// ============================================================================
// Node Endpoints
// ============================================================================

/// **GET /api/nodes** - List nodes with filtering
///
/// Returns a list of nodes matching the filter criteria.
///
/// # Query Parameters
///
/// - `region`: Filter by region ID
/// - `pool`: Filter by pool type (infrastructure, phantom)
/// - `tag`: Filter by node tag
/// - `status`: Filter by status (online, degraded, offline, maintenance)
/// - `sla_tier`: Filter by SLA tier (critical, production, standard, best_effort)
/// - `limit`: Maximum number of results (default: 100)
/// - `offset`: Pagination offset (default: 0)
///
/// # Examples
///
/// ```bash
/// # All nodes
/// curl http://localhost:8080/api/nodes
///
/// # Filter by region
/// curl "http://localhost:8080/api/nodes?region=us-east-1"
///
/// # Filter by status and SLA tier
/// curl "http://localhost:8080/api/nodes?status=online&sla_tier=production"
///
/// # Pagination
/// curl "http://localhost:8080/api/nodes?limit=50&offset=100"
/// ```
async fn list_nodes(
    State(state): State<Arc<ControlPlaneState>>,
    Query(filter): Query<NodeFilter>,
) -> Json<NodeListResponse> {
    let nodes = state.dashboard.get_nodes(&filter);
    let total = nodes.len();

    Json(NodeListResponse {
        nodes,
        total,
        limit: filter.limit.unwrap_or(100),
        offset: filter.offset.unwrap_or(0),
    })
}

/// **GET /api/nodes/{id}** - Single node details with history
///
/// Returns detailed information about a specific node including task history.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/nodes/node-abc123
/// ```
async fn get_node_details(
    State(state): State<Arc<ControlPlaneState>>,
    Path(id): Path<String>,
) -> Result<Json<NodeDetailsWithHistory>, StatusCode> {
    state
        .dashboard
        .get_node(&id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// **POST /api/nodes/{id}/override** - Manual node assignment override
///
/// Forces a task to be assigned to a specific node, with alternatives if the
/// primary assignment fails. Returns the assignment decision and alternative options.
///
/// # Request Body
///
/// ```json
/// {
///   "task_id": "task-789",
///   "reason": "Testing new hardware configuration"
/// }
/// ```
///
/// # Response
///
/// ```json
/// {
///   "assigned": true,
///   "node_id": "node-456",
///   "alternatives": [
///     {
///       "node_id": "node-789",
///       "score": 0.95,
///       "reason": "Similar specs, lower load"
///     }
///   ]
/// }
/// ```
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/nodes/node-456/override \
///   -H "Content-Type: application/json" \
///   -d '{"task_id": "task-789", "reason": "testing"}'
/// ```
async fn override_node_assignment(
    State(state): State<Arc<ControlPlaneState>>,
    Path(node_id): Path<String>,
    Json(req): Json<OverrideRequest>,
) -> Result<Json<OverrideResponse>, ApiError> {
    state
        .assignment_override
        .override_assignment(&node_id, &req.task_id, &req.reason)
        .map(Json)
        .map_err(|e| ApiError::Override(e))
}

// ============================================================================
// Job Endpoints
// ============================================================================

/// **GET /api/jobs** - List jobs with metrics
///
/// Returns a list of jobs matching the filter criteria.
///
/// # Query Parameters
///
/// - `status`: Filter by job status
/// - `min_priority`: Minimum priority level
/// - `since`: Only jobs created since this timestamp (RFC3339)
/// - `limit`: Maximum results (default: 100)
/// - `offset`: Pagination offset (default: 0)
///
/// # Examples
///
/// ```bash
/// # All jobs
/// curl http://localhost:8080/api/jobs
///
/// # Running jobs only
/// curl "http://localhost:8080/api/jobs?status=running"
///
/// # High priority jobs
/// curl "http://localhost:8080/api/jobs?min_priority=100"
/// ```
async fn list_jobs(
    State(state): State<Arc<ControlPlaneState>>,
    Query(filter): Query<JobFilter>,
) -> Json<JobListResponse> {
    let jobs = state.dashboard.get_jobs(&filter);
    let total = jobs.len();

    Json(JobListResponse {
        jobs,
        total,
        limit: filter.limit.unwrap_or(100),
        offset: filter.offset.unwrap_or(0),
    })
}

/// **GET /api/jobs/{id}** - Single job details
///
/// Returns detailed information about a specific job including all tasks.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/jobs/job-abc123
/// ```
async fn get_job_details(
    State(state): State<Arc<ControlPlaneState>>,
    Path(id): Path<String>,
) -> Result<Json<JobDetails>, StatusCode> {
    state
        .dashboard
        .get_job(&id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// **GET /api/jobs/{id}/latency** - Job latency heatmap by region
///
/// Returns latency distribution data for visualizing task execution times
/// across different regions.
///
/// # Response Format
///
/// Heatmap data suitable for visualization libraries like D3.js or Plotly.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/jobs/job-abc123/latency
/// ```
async fn get_job_latency(
    State(state): State<Arc<ControlPlaneState>>,
    Path(id): Path<String>,
) -> Result<Json<LatencyHeatmap>, StatusCode> {
    state
        .dashboard
        .get_job_latency_heatmap(&id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// ============================================================================
// Alert Endpoints
// ============================================================================

/// **GET /api/alerts** - List active alerts
///
/// Returns all active (non-dismissed) alerts with severity levels and
/// suggested actions.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/alerts
/// ```
async fn list_alerts(State(state): State<Arc<ControlPlaneState>>) -> Json<AlertListResponse> {
    let alerts = state.alerts.get_active_alerts();

    Json(AlertListResponse {
        alerts: alerts.clone(),
        total: alerts.len(),
        by_severity: group_alerts_by_severity(&alerts),
    })
}

/// **POST /api/alerts/{id}/dismiss** - Dismiss an alert
///
/// Marks an alert as acknowledged/dismissed.
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/alerts/alert-123/dismiss
/// ```
async fn dismiss_alert(
    State(state): State<Arc<ControlPlaneState>>,
    Path(id): Path<String>,
) -> Result<Json<DismissResponse>, ApiError> {
    state
        .alerts
        .dismiss_alert(&id)
        .map(|_| {
            Json(DismissResponse {
                success: true,
                alert_id: id,
                dismissed_at: Utc::now(),
            })
        })
        .map_err(|e| ApiError::Alert(e))
}

/// **POST /api/alerts/{id}/action/{action_id}** - Execute suggested action
///
/// Executes one of the suggested remediation actions for an alert.
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/alerts/alert-123/action/restart_node
/// ```
async fn execute_alert_action(
    State(state): State<Arc<ControlPlaneState>>,
    Path((alert_id, action_id)): Path<(String, String)>,
) -> Result<Json<ActionResult>, ApiError> {
    state
        .alerts
        .execute_action(&alert_id, &action_id)
        .map(Json)
        .map_err(|e| ApiError::Alert(e))
}

// ============================================================================
// Forecast Endpoint
// ============================================================================

/// **GET /api/forecast** - Capacity prediction for next 24 hours
///
/// Returns predicted capacity availability for the next 24 hours based on
/// historical patterns and current trends.
///
/// # Response Format
///
/// Hourly predictions with confidence intervals for both infrastructure
/// and phantom pools.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/forecast
/// ```
async fn get_capacity_forecast(
    State(state): State<Arc<ControlPlaneState>>,
) -> Json<CapacityForecast> {
    Json(state.forecaster.forecast_24h())
}

// ============================================================================
// Request/Response Types
// ============================================================================

// --- Region Types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStats {
    pub id: String,
    pub name: String,
    pub total_nodes: u32,
    pub online_nodes: u32,
    pub total_cores: u32,
    pub available_cores: u32,
    pub utilization: f32,
    pub avg_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionDetails {
    pub id: String,
    pub name: String,
    pub nodes: Vec<NodeSummary>,
    pub location_summary: LocationSummary,
    pub recent_tasks: Vec<TaskSummary>,
    pub health_status: SystemHealth,
}

// --- Node Types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeFilter {
    pub region: Option<String>,
    pub pool: Option<String>, // "infrastructure" or "phantom"
    pub tag: Option<String>,
    pub status: Option<String>,
    pub sla_tier: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeListResponse {
    pub nodes: Vec<NodeDetails>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDetails {
    pub id: String,
    pub hostname: String,
    pub status: String,
    pub region: String,
    pub sla_tier: String,
    pub cores_total: u32,
    pub cores_available: u32,
    pub ram_bytes_total: u64,
    pub ram_bytes_available: u64,
    pub gpus_total: u32,
    pub gpus_available: u32,
    pub utilization: f32,
    pub tasks_today: u32,
    pub failure_rate: f32,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDetailsWithHistory {
    #[serde(flatten)]
    pub node: NodeDetails,
    pub task_history: Vec<TaskHistoryItem>,
    pub metrics_history: Vec<MetricsSnapshot>,
    pub uptime_hours: f64,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHistoryItem {
    pub task_id: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub duration_ms: u64,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub timestamp: DateTime<Utc>,
    pub cpu_usage: f32,
    pub ram_usage: f32,
    pub gpu_usage: f32,
    pub network_rx_mbps: f32,
    pub network_tx_mbps: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverrideRequest {
    pub task_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverrideResponse {
    pub assigned: bool,
    pub node_id: String,
    pub alternatives: Vec<AlternativeNode>,
    pub override_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlternativeNode {
    pub node_id: String,
    pub score: f32,
    pub reason: String,
    pub estimated_latency_ms: u64,
}

// --- Job Types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobFilter {
    pub status: Option<String>,
    pub min_priority: Option<u32>,
    pub since: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobListResponse {
    pub jobs: Vec<JobListItem>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobListItem {
    pub id: String,
    pub name: String,
    pub status: String,
    pub priority: u32,
    pub total_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub running_tasks: usize,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub progress_percent: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDetails {
    pub id: String,
    pub name: String,
    pub status: String,
    pub priority: u32,
    pub tasks: Vec<TaskDetails>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub total_duration_ms: Option<u64>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDetails {
    pub id: String,
    pub status: String,
    pub assigned_node: Option<String>,
    pub assigned_region: Option<String>,
    pub is_phantom: bool,
    pub attempts: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyHeatmap {
    pub job_id: String,
    pub regions: Vec<String>,
    pub time_buckets: Vec<DateTime<Utc>>,
    /// Matrix[region_idx][time_bucket_idx] = latency_ms
    pub latency_matrix: Vec<Vec<Option<u64>>>,
    /// Percentiles for color scale
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
}

// --- Alert Types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertListResponse {
    pub alerts: Vec<AlertDetails>,
    pub total: usize,
    pub by_severity: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDetails {
    pub id: String,
    pub severity: AlertSeverity,
    pub title: String,
    pub message: String,
    pub node_id: Option<String>,
    pub region_id: Option<String>,
    pub raised_at: DateTime<Utc>,
    pub acknowledged: bool,
    pub suggested_actions: Vec<SuggestedAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuggestedAction {
    pub id: String,
    pub name: String,
    pub description: String,
    pub impact: ActionImpact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionImpact {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DismissResponse {
    pub success: bool,
    pub alert_id: String,
    pub dismissed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionResult {
    pub success: bool,
    pub action_id: String,
    pub executed_at: DateTime<Utc>,
    pub output: String,
}

// --- Forecast Types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityForecast {
    pub generated_at: DateTime<Utc>,
    pub forecast_hours: u32,
    pub hourly_predictions: Vec<HourlyPrediction>,
    pub trends: ForecastTrends,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyPrediction {
    pub hour: DateTime<Utc>,
    pub infrastructure_cores_available: u32,
    pub phantom_cores_estimated_low: u32,
    pub phantom_cores_estimated_mid: u32,
    pub phantom_cores_estimated_high: u32,
    pub total_predicted_load: f32,
    pub expected_utilization: f32,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastTrends {
    pub capacity_trend: CapacityTrend,
    pub demand_trend: DemandTrend,
    pub peak_hours: Vec<u32>,
    pub recommended_action: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DemandTrend {
    Increasing,
    Stable,
    Decreasing,
}

// ============================================================================
// Error Handling
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Alert error: {0}")]
    Alert(#[from] AlertError),

    #[error("Override error: {0}")]
    Override(#[from] OverrideError),

    #[error("Not found")]
    NotFound,

    #[error("Bad request: {0}")]
    BadRequest(String),
}

#[derive(Debug, thiserror::Error)]
pub enum AlertError {
    #[error("Alert not found: {0}")]
    NotFound(String),

    #[error("Action not found: {0}")]
    ActionNotFound(String),

    #[error("Action execution failed: {0}")]
    ExecutionFailed(String),
}

#[derive(Debug, thiserror::Error)]
pub enum OverrideError {
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Node cannot accept work: {0}")]
    NodeUnavailable(String),

    #[error("Override failed: {0}")]
    Failed(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::Alert(AlertError::NotFound(id)) => {
                (StatusCode::NOT_FOUND, format!("Alert not found: {}", id))
            }
            ApiError::Alert(AlertError::ActionNotFound(id)) => {
                (StatusCode::NOT_FOUND, format!("Action not found: {}", id))
            }
            ApiError::Alert(AlertError::ExecutionFailed(msg)) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Action failed: {}", msg),
            ),
            ApiError::Override(OverrideError::NodeNotFound(id)) => {
                (StatusCode::NOT_FOUND, format!("Node not found: {}", id))
            }
            ApiError::Override(OverrideError::TaskNotFound(id)) => {
                (StatusCode::NOT_FOUND, format!("Task not found: {}", id))
            }
            ApiError::Override(OverrideError::NodeUnavailable(msg)) => {
                (StatusCode::CONFLICT, format!("Node unavailable: {}", msg))
            }
            ApiError::Override(OverrideError::Failed(msg)) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Override failed: {}", msg),
            ),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Resource not found".to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
        };

        let body = Json(serde_json::json!({
            "error": message,
            "status": status.as_u16(),
        }));

        (status, body).into_response()
    }
}

// ============================================================================
// Metrics Endpoints
// ============================================================================

/// Node metrics response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetricsResponse {
    pub total_nodes: u32,
    pub online_nodes: u32,
    pub degraded_nodes: u32,
    pub offline_nodes: u32,
    pub total_cores: u32,
    pub available_cores: u32,
    pub total_ram_bytes: u64,
    pub available_ram_bytes: u64,
    pub total_gpus: u32,
    pub available_gpus: u32,
    pub avg_cpu_utilization: f32,
    pub avg_memory_utilization: f32,
    pub nodes: Vec<NodeDetails>,
    pub collected_at: DateTime<Utc>,
}

/// Job metrics response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetricsResponse {
    pub total_jobs: u32,
    pub pending_jobs: u32,
    pub running_jobs: u32,
    pub completed_jobs: u32,
    pub failed_jobs: u32,
    pub total_tasks: u64,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub avg_job_duration_ms: Option<u64>,
    pub throughput_per_minute: f64,
    pub jobs: Vec<JobListItem>,
    pub collected_at: DateTime<Utc>,
}

/// System health response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealthResponse {
    pub overall_health: String,
    pub health_score: f32,
    pub infrastructure_health: InfraHealthDetails,
    pub phantom_health: Option<PhantomHealthDetails>,
    pub active_alerts: Vec<AlertDetails>,
    pub recommendations: Vec<String>,
    pub collected_at: DateTime<Utc>,
}

/// Infrastructure health details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraHealthDetails {
    pub status: String,
    pub nodes_healthy: u32,
    pub nodes_total: u32,
    pub utilization_percent: f32,
    pub sla_compliance_rate: f32,
}

/// Phantom pool health details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhantomHealthDetails {
    pub status: String,
    pub capacity_confidence: f32,
    pub completion_rate: f32,
    pub avg_latency_ms: u64,
    pub pending_tasks: u32,
}

/// **GET /api/metrics/nodes** - Real node statistics
///
/// Returns comprehensive node metrics from the cluster.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/metrics/nodes
/// ```
async fn get_node_metrics(
    State(state): State<Arc<ControlPlaneState>>,
) -> Json<NodeMetricsResponse> {
    let dashboard = state.dashboard.get_dashboard();
    let infra = &dashboard.infrastructure;
    let nodes = state.dashboard.get_nodes(&NodeFilter {
        region: None,
        pool: None,
        tag: None,
        status: None,
        sla_tier: None,
        limit: Some(100),
        offset: None,
    });

    let avg_cpu: f32 = if nodes.is_empty() {
        0.0
    } else {
        nodes.iter().map(|n| n.utilization).sum::<f32>() / nodes.len() as f32
    };

    let avg_mem: f32 = if nodes.is_empty() || infra.total_ram_bytes == 0 {
        0.0
    } else {
        1.0 - (infra.available_ram_bytes as f32 / infra.total_ram_bytes as f32)
    };

    Json(NodeMetricsResponse {
        total_nodes: infra.nodes_online
            + infra.nodes_degraded
            + infra.nodes_offline
            + infra.nodes_maintenance,
        online_nodes: infra.nodes_online,
        degraded_nodes: infra.nodes_degraded,
        offline_nodes: infra.nodes_offline,
        total_cores: infra.total_cores,
        available_cores: infra.available_cores,
        total_ram_bytes: infra.total_ram_bytes,
        available_ram_bytes: infra.available_ram_bytes,
        total_gpus: infra.total_gpus,
        available_gpus: infra.available_gpus,
        avg_cpu_utilization: avg_cpu * 100.0,
        avg_memory_utilization: avg_mem * 100.0,
        nodes,
        collected_at: dashboard.generated_at,
    })
}

/// **GET /api/metrics/jobs** - Real job statistics
///
/// Returns comprehensive job metrics from the cluster.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/metrics/jobs
/// ```
async fn get_job_metrics(State(state): State<Arc<ControlPlaneState>>) -> Json<JobMetricsResponse> {
    let jobs = state.dashboard.get_jobs(&JobFilter {
        status: None,
        min_priority: None,
        since: None,
        limit: Some(100),
        offset: None,
    });

    let pending = jobs.iter().filter(|j| j.status == "pending").count() as u32;
    let running = jobs.iter().filter(|j| j.status == "running").count() as u32;
    let completed = jobs.iter().filter(|j| j.status == "completed").count() as u32;
    let failed = jobs.iter().filter(|j| j.status == "failed").count() as u32;

    let total_tasks: u64 = jobs.iter().map(|j| j.total_tasks as u64).sum();
    let completed_tasks: u64 = jobs.iter().map(|j| j.completed_tasks as u64).sum();
    let failed_tasks: u64 = jobs.iter().map(|j| j.failed_tasks as u64).sum();

    // Calculate average job duration for completed jobs
    let avg_duration: Option<u64> = {
        let completed_jobs: Vec<_> = jobs
            .iter()
            .filter(|j| {
                j.status == "completed" && j.completed_at.is_some() && j.started_at.is_some()
            })
            .collect();

        if completed_jobs.is_empty() {
            None
        } else {
            let total_duration: i64 = completed_jobs
                .iter()
                .filter_map(|j| {
                    j.completed_at
                        .and_then(|end| j.started_at.map(|start| (end - start).num_milliseconds()))
                })
                .sum();
            Some((total_duration / completed_jobs.len() as i64) as u64)
        }
    };

    // Calculate throughput (tasks completed per minute)
    let throughput = if completed_tasks > 0 && avg_duration.unwrap_or(0) > 0 {
        (completed_tasks as f64 / (avg_duration.unwrap() as f64 / 60000.0)).max(0.0)
    } else {
        0.0
    };

    Json(JobMetricsResponse {
        total_jobs: jobs.len() as u32,
        pending_jobs: pending,
        running_jobs: running,
        completed_jobs: completed,
        failed_jobs: failed,
        total_tasks,
        completed_tasks,
        failed_tasks,
        avg_job_duration_ms: avg_duration,
        throughput_per_minute: throughput,
        jobs,
        collected_at: Utc::now(),
    })
}

/// **GET /api/metrics/system** - System health overview
///
/// Returns overall system health including infrastructure and phantom pool status.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/metrics/system
/// ```
async fn get_system_health(
    State(state): State<Arc<ControlPlaneState>>,
) -> Json<SystemHealthResponse> {
    let dashboard = state.dashboard.get_dashboard();
    let alerts = state.alerts.get_active_alerts();

    // Calculate overall health score
    let infra = &dashboard.infrastructure;
    let combined = &dashboard.combined;

    let critical_alerts = alerts
        .iter()
        .filter(|a| matches!(a.severity, AlertSeverity::Critical))
        .count();
    let warning_alerts = alerts
        .iter()
        .filter(|a| matches!(a.severity, AlertSeverity::Warning))
        .count();

    let alert_penalty = (critical_alerts as f32 * 0.15) + (warning_alerts as f32 * 0.05);
    let utilization_score = 1.0 - (combined.current_utilization.min(1.0));
    let node_health_ratio = if (infra.nodes_online + infra.nodes_degraded) > 0 {
        infra.nodes_online as f32 / (infra.nodes_online + infra.nodes_degraded) as f32
    } else {
        0.0
    };

    let health_score = ((utilization_score * 0.3)
        + (node_health_ratio * 0.5)
        + (infra.sla_compliance_rate / 100.0 * 0.2)
        - alert_penalty)
        .max(0.0)
        .min(1.0);

    let overall_health = match combined.system_health {
        SystemHealth::Healthy => "healthy",
        SystemHealth::Degraded => "degraded",
        SystemHealth::Strained => "strained",
        SystemHealth::Critical => "critical",
    };

    let infra_status = if infra.nodes_offline > infra.nodes_online {
        "critical"
    } else if infra.nodes_degraded > infra.nodes_online / 4 {
        "degraded"
    } else {
        "healthy"
    };

    let infra_health = InfraHealthDetails {
        status: infra_status.to_string(),
        nodes_healthy: infra.nodes_online,
        nodes_total: infra.nodes_online
            + infra.nodes_degraded
            + infra.nodes_offline
            + infra.nodes_maintenance,
        utilization_percent: infra.utilization_percent,
        sla_compliance_rate: infra.sla_compliance_rate,
    };

    let phantom = &dashboard.phantom;
    let phantom_health = if phantom.pool_responsive {
        Some(PhantomHealthDetails {
            status: if phantom.capacity_confidence > 0.7 {
                "healthy"
            } else if phantom.capacity_confidence > 0.4 {
                "degraded"
            } else {
                "unknown"
            }
            .to_string(),
            capacity_confidence: phantom.capacity_confidence,
            completion_rate: phantom.task_completion_rate,
            avg_latency_ms: phantom.avg_task_latency.as_millis() as u64,
            pending_tasks: phantom.active_tasks,
        })
    } else {
        None
    };

    let recommendations: Vec<String> = combined
        .recommendations
        .iter()
        .map(|r| r.message.clone())
        .collect();

    Json(SystemHealthResponse {
        overall_health: overall_health.to_string(),
        health_score: health_score * 100.0,
        infrastructure_health: infra_health,
        phantom_health,
        active_alerts: alerts,
        recommendations,
        collected_at: Utc::now(),
    })
}

// ============================================================================
// Extended Router with Metrics Endpoints
// ============================================================================

/// Creates the complete control plane API router with all endpoints including metrics.
///
/// This extends the base router with additional /api/metrics/* endpoints.
pub fn create_control_plane_router_with_metrics(state: Arc<ControlPlaneState>) -> Router {
    let metrics_router = Router::new()
        .route("/api/metrics/nodes", get(get_node_metrics))
        .route("/api/metrics/jobs", get(get_job_metrics))
        .route("/api/metrics/system", get(get_system_health))
        .with_state(state.clone());

    create_control_plane_router(state).merge(metrics_router)
}

/// Creates the control plane API router with rate limiting enabled.
///
/// This version includes rate limiting middleware that:
/// - Applies per-client rate limits based on IP or API key
/// - Returns 429 Too Many Requests when limits are exceeded
/// - Includes X-RateLimit-* headers in responses
/// - Exempts health check endpoints
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use marabunta_compute::control_plane::api::{ControlPlaneState, create_control_plane_router_with_rate_limiting};
/// use marabunta_compute::ratelimit::RateLimiterConfig;
///
/// # fn example() {
/// // let state = Arc::new(ControlPlaneState { ... });
/// // let config = RateLimiterConfig::new();
/// // let router = create_control_plane_router_with_rate_limiting(state, config);
/// # }
/// ```
pub fn create_control_plane_router_with_rate_limiting(
    state: Arc<ControlPlaneState>,
    rate_limit_config: RateLimiterConfig,
) -> Router {
    let limiter = Arc::new(RateLimiter::with_config(rate_limit_config));

    create_control_plane_router(state).layer(RateLimitLayer::new(limiter))
}

/// Creates the control plane API router with both rate limiting and metrics.
///
/// Combines rate limiting with the extended metrics endpoints.
pub fn create_control_plane_router_full(
    state: Arc<ControlPlaneState>,
    rate_limit_config: RateLimiterConfig,
) -> Router {
    let limiter = Arc::new(RateLimiter::with_config(rate_limit_config));

    create_control_plane_router_with_metrics(state).layer(RateLimitLayer::new(limiter))
}

/// Creates a default rate limiter configuration for the control plane API.
///
/// This provides sensible defaults optimized for dashboard and monitoring:
/// - 60 requests/second for dashboard (allows frequent polling)
/// - 100 requests/second for most endpoints
/// - 10 requests/second for override operations (expensive)
/// - Health and metrics endpoints are exempt
///
/// # Example
///
/// ```rust
/// use marabunta_compute::control_plane::api::default_control_plane_rate_limiter_config;
///
/// let config = default_control_plane_rate_limiter_config();
/// // Customize as needed
/// ```
pub fn default_control_plane_rate_limiter_config() -> RateLimiterConfig {
    RateLimiterConfig::new()
        .with_default_limit(RateLimit::new(100.0, 200))
        .with_endpoint_limit("/api/dashboard", RateLimit::new(60.0, 120))
        .with_endpoint_limit("/api/nodes/:id/override", RateLimit::new(10.0, 20))
        .with_endpoint_limit(
            "/api/alerts/:id/action/:action_id",
            RateLimit::new(20.0, 40),
        )
        .with_global_limit("/api/forecast", RateLimit::new(30.0, 60))
        .with_exempt_path("/health")
        .with_exempt_path("/ready")
        .with_exempt_path("/api/metrics")
}

// ============================================================================
// Helper Functions
// ============================================================================

fn group_alerts_by_severity(alerts: &[AlertDetails]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    counts.insert("info".to_string(), 0);
    counts.insert("warning".to_string(), 0);
    counts.insert("error".to_string(), 0);
    counts.insert("critical".to_string(), 0);

    for alert in alerts {
        let key = match alert.severity {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Error => "error",
            AlertSeverity::Critical => "critical",
        };
        *counts.get_mut(key).unwrap() += 1;
    }

    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_alerts_by_severity() {
        let alerts = vec![
            AlertDetails {
                id: "1".into(),
                severity: AlertSeverity::Warning,
                title: "Test".into(),
                message: "Test".into(),
                node_id: None,
                region_id: None,
                raised_at: Utc::now(),
                acknowledged: false,
                suggested_actions: vec![],
            },
            AlertDetails {
                id: "2".into(),
                severity: AlertSeverity::Critical,
                title: "Test".into(),
                message: "Test".into(),
                node_id: None,
                region_id: None,
                raised_at: Utc::now(),
                acknowledged: false,
                suggested_actions: vec![],
            },
        ];

        let counts = group_alerts_by_severity(&alerts);
        assert_eq!(counts["warning"], 1);
        assert_eq!(counts["critical"], 1);
        assert_eq!(counts["info"], 0);
    }
}
