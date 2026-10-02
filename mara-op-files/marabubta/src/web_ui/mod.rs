// Marabunta - Licensed under the MIT License.
//! Web UI Module for Marabunta Compute
//!
//! Provides a simple web-based dashboard for monitoring and managing
//! the Marabunta Compute cluster. Serves static HTML/CSS/JS files and
//! provides API endpoints for the dashboard.
//!
//! # Features
//!
//! - Dashboard overview with cluster statistics
//! - Job listing, submission, and monitoring
//! - Node status and health information
//! - Real-time updates via polling
//!
//! # Usage
//!
//! ```rust,no_run
//! use marabunta_compute::web_ui;
//! use std::sync::Arc;
//!
//! async fn start_server() {
//!     let state = web_ui::WebUiState::new();
//!     let router = web_ui::create_router(Arc::new(state));
//!     // Mount on your axum server
//! }
//! ```

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

// ============================================================================
// Shared State Types
// ============================================================================

/// Information about a compute job, as exposed by the web UI API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub id: String,
    pub name: String,
    /// One of: "pending", "running", "completed", "failed", "cancelled"
    pub status: String,
    pub progress: f64,
    pub tasks_total: u32,
    pub tasks_completed: u32,
    pub submitted_at: String,
    pub completed_at: Option<String>,
}

/// Information about a worker node, as exposed by the web UI API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    /// One of: "server", "desktop", "android", "raspberry_pi"
    pub node_type: String,
    /// One of: "ready", "busy", "draining", "offline"
    pub status: String,
    pub cores: u32,
    pub memory_bytes: u64,
    pub current_load: f64,
    pub thermal_state: String,
    pub last_seen: String,
    pub region: String,
}

/// An activity event for the dashboard event feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEvent {
    pub timestamp: String,
    pub event_type: String,
    pub description: String,
}

/// Information about a distributed PostgreSQL instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub name: String,
    pub port: u16,
    pub shard_count: u32,
    pub replication_factor: u32,
    pub consistency_mode: String,
    pub status: String,
    /// Number of nodes currently hosting shards for this instance
    pub node_count: u32,
    pub created_at: String,
}

/// Maximum number of activity events to keep in memory.
const MAX_ACTIVITY_EVENTS: usize = 100;

/// Dashboard cache TTL.
const DASHBOARD_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(10);

/// Pagination query parameters accepted by list endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    /// Maximum items per page (default 100, max 500).
    #[serde(default = "default_page_limit")]
    pub limit: usize,
    /// Offset for pagination.
    #[serde(default)]
    pub offset: usize,
    /// Optional status filter (e.g. "running", "completed").
    #[serde(default)]
    pub status: Option<String>,
}

fn default_page_limit() -> usize {
    100
}

/// Paginated response wrapper.
#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T: Serialize> {
    pub items: Vec<T>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

/// State shared by web UI handlers
pub struct WebUiState {
    /// Start time for uptime calculation
    pub start_time: std::time::Instant,
    /// All known jobs, keyed by job ID string
    pub jobs: Arc<DashMap<String, JobInfo>>,
    /// All known worker nodes, keyed by node ID string
    pub nodes: Arc<DashMap<String, NodeInfo>>,
    /// Recent activity events (most recent first)
    pub events: Arc<RwLock<VecDeque<ActivityEvent>>>,
    /// Distributed PostgreSQL instances, keyed by instance name
    pub instances: Arc<DashMap<String, InstanceInfo>>,
    /// Next port to assign to new instances
    next_instance_port: std::sync::atomic::AtomicU16,
    /// Optional reference to the alerting subsystem
    pub alert_manager: Option<Arc<crate::alerting::AlertManager>>,
    /// Cached dashboard stats with timestamp.
    dashboard_cache: RwLock<Option<(std::time::Instant, DashboardStats)>>,
}

impl WebUiState {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            jobs: Arc::new(DashMap::new()),
            nodes: Arc::new(DashMap::new()),
            events: Arc::new(RwLock::new(VecDeque::new())),
            instances: Arc::new(DashMap::new()),
            next_instance_port: std::sync::atomic::AtomicU16::new(15432),
            alert_manager: None,
            dashboard_cache: RwLock::new(None),
        }
    }

    /// Create a new state with an attached alert manager.
    pub fn with_alert_manager(mut self, manager: Arc<crate::alerting::AlertManager>) -> Self {
        self.alert_manager = Some(manager);
        self
    }

    /// Push a new activity event. Old events beyond MAX_ACTIVITY_EVENTS are
    /// discarded automatically.
    pub async fn push_event(&self, event: ActivityEvent) {
        let mut events = self.events.write().await;
        events.push_front(event);
        while events.len() > MAX_ACTIVITY_EVENTS {
            events.pop_back();
        }
    }
}

impl Default for WebUiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Create the web UI router
pub fn create_router(state: Arc<WebUiState>) -> Router {
    Router::new()
        // Static file routes
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/jobs.html", get(serve_jobs))
        .route("/nodes.html", get(serve_nodes))
        .route("/instances.html", get(serve_instances_page))
        .route("/compute.html", get(serve_compute_page))
        .route("/equations.html", get(serve_equations_page))
        .route("/static/*path", get(serve_static))
        // Dashboard API routes
        .route("/api/dashboard/stats", get(dashboard_stats))
        .route("/api/dashboard/activity", get(recent_activity))
        // Jobs API routes
        .route("/api/jobs", get(list_jobs).post(create_job))
        .route("/api/jobs/:id/cancel", post(cancel_job))
        // Nodes API routes
        .route("/api/nodes", get(list_nodes))
        // Instances API routes
        .route("/api/instances", get(list_instances).post(create_instance))
        // Alerts API route
        .route("/api/alerts", get(list_alerts))
        .with_state(state)
}

// ============================================================================
// Static File Handlers
// ============================================================================

async fn serve_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn serve_jobs() -> Html<&'static str> {
    Html(JOBS_HTML)
}

async fn serve_nodes() -> Html<&'static str> {
    Html(NODES_HTML)
}

async fn serve_instances_page() -> Html<&'static str> {
    Html(INSTANCES_HTML)
}

async fn serve_compute_page() -> Html<&'static str> {
    Html(COMPUTE_HTML)
}

async fn serve_equations_page() -> Html<&'static str> {
    Html(EQUATIONS_HTML)
}

async fn serve_static(uri: Uri) -> Response {
    let path = uri.path().strip_prefix("/static/").unwrap_or("");

    match path {
        "style.css" => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/css")],
            STYLE_CSS,
        )
            .into_response(),
        "app.js" => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/javascript")],
            APP_JS,
        )
            .into_response(),
        "compute.js" => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/javascript")],
            COMPUTE_JS,
        )
            .into_response(),
        "worker-exec.js" => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/javascript")],
            WORKER_EXEC_JS,
        )
            .into_response(),
        "equations.js" => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/javascript")],
            EQUATIONS_JS,
        )
            .into_response(),
        _ => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

// ============================================================================
// Dashboard API Handlers
// ============================================================================

/// Dashboard statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardStats {
    pub total_jobs: u64,
    pub running_jobs: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub total_nodes: u64,
    pub healthy_nodes: u64,
    pub total_tasks: u64,
    pub tasks_per_second: f64,
    pub uptime_secs: u64,
    pub cluster_utilization: f64,
    /// Number of nodes with thermal state "high" or "critical"
    pub thermal_warning_nodes: u64,
}

async fn dashboard_stats(State(state): State<Arc<WebUiState>>) -> Json<DashboardStats> {
    // Check cache first.
    {
        let cache = state.dashboard_cache.read().await;
        if let Some((ts, ref stats)) = *cache {
            if ts.elapsed() < DASHBOARD_CACHE_TTL {
                return Json(stats.clone());
            }
        }
    }

    // Compute fresh stats.
    let total_jobs = state.jobs.len() as u64;
    let running_jobs = state
        .jobs
        .iter()
        .filter(|entry| entry.value().status == "running")
        .count() as u64;
    let completed_jobs = state
        .jobs
        .iter()
        .filter(|entry| entry.value().status == "completed")
        .count() as u64;
    let failed_jobs = state
        .jobs
        .iter()
        .filter(|entry| entry.value().status == "failed")
        .count() as u64;

    let total_nodes = state.nodes.len() as u64;
    let healthy_nodes = state
        .nodes
        .iter()
        .filter(|entry| entry.value().status != "offline")
        .count() as u64;

    let total_tasks: u64 = state
        .jobs
        .iter()
        .map(|entry| entry.value().tasks_total as u64)
        .sum();

    let cluster_utilization = if total_nodes > 0 {
        let total_load: f64 = state
            .nodes
            .iter()
            .map(|entry| entry.value().current_load)
            .sum();
        total_load / total_nodes as f64
    } else {
        0.0
    };

    let uptime_secs = state.start_time.elapsed().as_secs();
    let completed_tasks: u64 = state
        .jobs
        .iter()
        .map(|entry| entry.value().tasks_completed as u64)
        .sum();
    let tasks_per_second = if uptime_secs > 0 {
        completed_tasks as f64 / uptime_secs as f64
    } else {
        0.0
    };

    let thermal_warning_nodes = state
        .nodes
        .iter()
        .filter(|entry| {
            let ts = &entry.value().thermal_state;
            ts == "high" || ts == "critical"
        })
        .count() as u64;

    let stats = DashboardStats {
        total_jobs,
        running_jobs,
        completed_jobs,
        failed_jobs,
        total_nodes,
        healthy_nodes,
        total_tasks,
        tasks_per_second,
        uptime_secs,
        cluster_utilization,
        thermal_warning_nodes,
    };

    // Update cache.
    {
        let mut cache = state.dashboard_cache.write().await;
        *cache = Some((std::time::Instant::now(), stats.clone()));
    }

    Json(stats)
}

/// Recent activity item — kept for backwards compatibility with the
/// existing `ActivityItem` shape that the frontend already understands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityItem {
    pub timestamp: String,
    pub event_type: String,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

async fn recent_activity(State(state): State<Arc<WebUiState>>) -> Json<Vec<ActivityItem>> {
    let events = state.events.read().await;
    let items: Vec<ActivityItem> = events
        .iter()
        .take(20)
        .map(|evt| ActivityItem {
            timestamp: evt.timestamp.clone(),
            event_type: evt.event_type.clone(),
            description: evt.description.clone(),
            job_id: None,
            node_id: None,
        })
        .collect();
    Json(items)
}

// ============================================================================
// Jobs API Handlers
// ============================================================================

async fn list_jobs(
    State(state): State<Arc<WebUiState>>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<JobInfo>> {
    let limit = params.limit.min(500);

    let all_jobs: Vec<JobInfo> = state
        .jobs
        .iter()
        .map(|entry| entry.value().clone())
        .filter(|j| {
            params.status.as_ref().map_or(true, |s| &j.status == s)
        })
        .collect();

    let total = all_jobs.len();
    let items: Vec<JobInfo> = all_jobs
        .into_iter()
        .skip(params.offset)
        .take(limit)
        .collect();

    Json(PaginatedResponse {
        items,
        total,
        limit,
        offset: params.offset,
    })
}

/// Request body for creating a new job.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateJobRequest {
    pub name: String,
    #[serde(default)]
    pub tasks: Option<serde_json::Value>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default = "default_task_count")]
    pub tasks_total: u32,
}

fn default_task_count() -> u32 {
    1
}

async fn create_job(
    State(state): State<Arc<WebUiState>>,
    Json(req): Json<CreateJobRequest>,
) -> (StatusCode, Json<JobInfo>) {
    let job_id = format!("job-{}", uuid::Uuid::new_v4().to_string().split('-').next().unwrap_or("0000"));
    let now = chrono::Utc::now().to_rfc3339();

    let job = JobInfo {
        id: job_id.clone(),
        name: req.name.clone(),
        status: "pending".to_string(),
        progress: 0.0,
        tasks_total: req.tasks_total,
        tasks_completed: 0,
        submitted_at: now.clone(),
        completed_at: None,
    };

    state.jobs.insert(job_id.clone(), job.clone());

    // Record the event
    state
        .push_event(ActivityEvent {
            timestamp: now,
            event_type: "job_submitted".to_string(),
            description: format!("Job '{}' submitted", req.name),
        })
        .await;

    (StatusCode::CREATED, Json(job))
}

async fn cancel_job(
    State(state): State<Arc<WebUiState>>,
    Path(id): Path<String>,
) -> StatusCode {
    if let Some(mut entry) = state.jobs.get_mut(&id) {
        let job = entry.value_mut();
        // Only allow cancellation if not already in a terminal state
        if job.status == "completed" || job.status == "failed" || job.status == "cancelled" {
            return StatusCode::CONFLICT;
        }
        job.status = "cancelled".to_string();
        let now = chrono::Utc::now().to_rfc3339();
        job.completed_at = Some(now.clone());

        let name = job.name.clone();
        drop(entry);

        // Record the event
        state
            .push_event(ActivityEvent {
                timestamp: now,
                event_type: "job_cancelled".to_string(),
                description: format!("Job '{}' cancelled", name),
            })
            .await;

        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

// ============================================================================
// Nodes API Handlers
// ============================================================================

async fn list_nodes(
    State(state): State<Arc<WebUiState>>,
    Query(params): Query<PaginationParams>,
) -> Json<PaginatedResponse<NodeInfo>> {
    let limit = params.limit.min(500);

    let all_nodes: Vec<NodeInfo> = state
        .nodes
        .iter()
        .map(|entry| entry.value().clone())
        .filter(|n| {
            params.status.as_ref().map_or(true, |s| &n.status == s)
        })
        .collect();

    let total = all_nodes.len();
    let items: Vec<NodeInfo> = all_nodes
        .into_iter()
        .skip(params.offset)
        .take(limit)
        .collect();

    Json(PaginatedResponse {
        items,
        total,
        limit,
        offset: params.offset,
    })
}

// ============================================================================
// Instances API Handlers
// ============================================================================

async fn list_instances(State(state): State<Arc<WebUiState>>) -> Json<Vec<InstanceInfo>> {
    let instances: Vec<InstanceInfo> = state
        .instances
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    Json(instances)
}

/// Request body for creating a new PostgreSQL instance.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateInstanceRequest {
    pub name: String,
    #[serde(default = "default_shard_count")]
    pub shard_count: u32,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    #[serde(default = "default_consistency_mode")]
    pub consistency_mode: String,
}

fn default_shard_count() -> u32 {
    16
}

fn default_replication_factor() -> u32 {
    1
}

fn default_consistency_mode() -> String {
    "eventual".to_string()
}

async fn create_instance(
    State(state): State<Arc<WebUiState>>,
    Json(req): Json<CreateInstanceRequest>,
) -> (StatusCode, Json<InstanceInfo>) {
    let port = state
        .next_instance_port
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let now = chrono::Utc::now().to_rfc3339();

    let instance = InstanceInfo {
        name: req.name.clone(),
        port,
        shard_count: req.shard_count,
        replication_factor: req.replication_factor,
        consistency_mode: req.consistency_mode,
        status: "running".to_string(),
        node_count: 0,
        created_at: now.clone(),
    };

    state.instances.insert(req.name.clone(), instance.clone());

    // Record the event
    state
        .push_event(ActivityEvent {
            timestamp: now,
            event_type: "instance_created".to_string(),
            description: format!(
                "PostgreSQL instance '{}' created with {} shards",
                req.name, instance.shard_count
            ),
        })
        .await;

    (StatusCode::CREATED, Json(instance))
}

// ============================================================================
// Alerts API Handler
// ============================================================================

/// GET /api/alerts
///
/// Returns active alerts from the AlertManager if one is attached to the
/// web UI state. Otherwise returns an empty array so the frontend never
/// breaks.
async fn list_alerts(State(state): State<Arc<WebUiState>>) -> Json<Vec<serde_json::Value>> {
    if let Some(ref manager) = state.alert_manager {
        let active = manager.get_active_alerts();
        let items: Vec<serde_json::Value> = active
            .into_iter()
            .map(|alert| {
                serde_json::json!({
                    "id": alert.id.to_string(),
                    "severity": alert.severity.as_str(),
                    "message": alert.message,
                    "source": alert.source,
                    "timestamp": alert.timestamp.to_rfc3339(),
                    "labels": alert.labels,
                })
            })
            .collect();
        Json(items)
    } else {
        Json(vec![])
    }
}

// ============================================================================
// Embedded Static Files
// ============================================================================

const INDEX_HTML: &str = include_str!("static/index.html");
const JOBS_HTML: &str = include_str!("static/jobs.html");
const NODES_HTML: &str = include_str!("static/nodes.html");
const INSTANCES_HTML: &str = include_str!("static/instances.html");
const COMPUTE_HTML: &str = include_str!("static/compute.html");
const STYLE_CSS: &str = include_str!("static/style.css");
const APP_JS: &str = include_str!("static/app.js");
const COMPUTE_JS: &str = include_str!("static/compute.js");
const WORKER_EXEC_JS: &str = include_str!("static/worker-exec.js");
const EQUATIONS_HTML: &str = include_str!("static/equations.html");
const EQUATIONS_JS: &str = include_str!("static/equations.js");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_web_ui_state_creation() {
        let state = WebUiState::new();
        assert!(state.start_time.elapsed().as_secs() < 1);
        assert_eq!(state.jobs.len(), 0);
        assert_eq!(state.nodes.len(), 0);
        assert_eq!(state.instances.len(), 0);
        assert!(state.alert_manager.is_none());
    }

    #[test]
    fn test_static_files_embedded() {
        assert!(!INDEX_HTML.is_empty());
        assert!(!JOBS_HTML.is_empty());
        assert!(!NODES_HTML.is_empty());
        assert!(!INSTANCES_HTML.is_empty());
        assert!(!STYLE_CSS.is_empty());
        assert!(!APP_JS.is_empty());
    }

    #[tokio::test]
    async fn test_push_event_limits() {
        let state = WebUiState::new();
        for i in 0..150 {
            state
                .push_event(ActivityEvent {
                    timestamp: format!("2024-01-01T00:00:{:02}Z", i % 60),
                    event_type: "test".to_string(),
                    description: format!("Event {}", i),
                })
                .await;
        }
        let events = state.events.read().await;
        assert_eq!(events.len(), MAX_ACTIVITY_EVENTS);
    }
}
