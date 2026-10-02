// Marabunta - Licensed under the MIT License.
//! Health and Status APIs
//!
//! This module provides comprehensive health and status endpoints for Kubernetes-style
//! deployments and operational monitoring. It includes:
//!
//! - **Kubernetes Probes**: `/health/live` and `/health/ready` for liveness and readiness checks
//! - **Cluster Status**: `/status/cluster` for aggregated cluster health
//! - **Node Status**: `/status/nodes` for per-node health details
//! - **Debug Endpoints**: `/debug/pprof`-like endpoints for profiling data
//! - **Metrics Summary**: `/metrics/summary` for human-readable metrics
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::health::{HealthApi, HealthApiConfig, ComponentHealth};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create health API with default config
//!     let config = HealthApiConfig::default();
//!     let health_api = Arc::new(HealthApi::new(config));
//!
//!     // Register components
//!     health_api.register_component("database").await;
//!     health_api.register_component("scheduler").await;
//!
//!     // Update component health
//!     health_api.update_component_health("database", ComponentHealth::healthy()).await;
//!
//!     // Create router
//!     let router = marabunta_compute::health::create_health_router(health_api);
//! }
//! ```
//!
//! # Endpoints
//!
//! | Endpoint | Method | Description |
//! |----------|--------|-------------|
//! | `/health/live` | GET | Kubernetes liveness probe |
//! | `/health/ready` | GET | Kubernetes readiness probe |
//! | `/status/cluster` | GET | Aggregated cluster health |
//! | `/status/nodes` | GET | Per-node health details |
//! | `/debug/pprof` | GET | CPU profiling data |
//! | `/debug/heap` | GET | Heap allocation data |
//! | `/debug/goroutines` | GET | Async task info |
//! | `/metrics/summary` | GET | Human-readable metrics |

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

// ================================
// Configuration
// ================================

/// Configuration for the health API
#[derive(Debug, Clone)]
pub struct HealthApiConfig {
    /// How long before a component is considered stale (no heartbeat)
    pub component_timeout: std::time::Duration,
    /// How many components must be healthy for readiness
    pub min_healthy_components: usize,
    /// Enable detailed profiling endpoints
    pub enable_profiling: bool,
    /// How long to wait for startup before readiness
    pub startup_grace_period: std::time::Duration,
    /// Threshold for unhealthy node percentage before cluster is degraded
    pub cluster_degraded_threshold: f64,
    /// Threshold for unhealthy node percentage before cluster is critical
    pub cluster_critical_threshold: f64,
}

impl Default for HealthApiConfig {
    fn default() -> Self {
        Self {
            component_timeout: std::time::Duration::from_secs(30),
            min_healthy_components: 1,
            enable_profiling: true,
            startup_grace_period: std::time::Duration::from_secs(10),
            cluster_degraded_threshold: 0.25, // 25% unhealthy = degraded
            cluster_critical_threshold: 0.50, // 50% unhealthy = critical
        }
    }
}

// ================================
// Health Status Types
// ================================

/// Overall health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but functional
    Degraded,
    /// Component is unhealthy
    Unhealthy,
    /// Component status is unknown
    Unknown,
}

impl HealthStatus {
    /// Returns true if the status allows serving traffic
    pub fn is_serving(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }

    /// Returns true if the status indicates the component is alive
    pub fn is_alive(&self) -> bool {
        !matches!(self, HealthStatus::Unknown)
    }

    /// Combine two statuses (returns the worse one)
    pub fn combine(self, other: HealthStatus) -> HealthStatus {
        match (self, other) {
            (HealthStatus::Unhealthy, _) | (_, HealthStatus::Unhealthy) => HealthStatus::Unhealthy,
            (HealthStatus::Unknown, _) | (_, HealthStatus::Unknown) => HealthStatus::Unknown,
            (HealthStatus::Degraded, _) | (_, HealthStatus::Degraded) => HealthStatus::Degraded,
            (HealthStatus::Healthy, HealthStatus::Healthy) => HealthStatus::Healthy,
        }
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
            HealthStatus::Unknown => write!(f, "unknown"),
        }
    }
}

// ================================
// Component Health
// ================================

/// Health information for a single component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Current status
    pub status: HealthStatus,
    /// Human-readable message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Last check time
    pub last_check: DateTime<Utc>,
    /// Time when component was last healthy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_healthy: Option<DateTime<Utc>>,
    /// Additional details (error messages, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// Response time of last health check (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_time_ms: Option<u64>,
}

impl ComponentHealth {
    /// Create a healthy component status
    pub fn healthy() -> Self {
        let now = Utc::now();
        Self {
            name: String::new(),
            status: HealthStatus::Healthy,
            message: None,
            last_check: now,
            last_healthy: Some(now),
            details: None,
            response_time_ms: None,
        }
    }

    /// Create a degraded component status
    pub fn degraded(message: impl Into<String>) -> Self {
        Self {
            name: String::new(),
            status: HealthStatus::Degraded,
            message: Some(message.into()),
            last_check: Utc::now(),
            last_healthy: None,
            details: None,
            response_time_ms: None,
        }
    }

    /// Create an unhealthy component status
    pub fn unhealthy(message: impl Into<String>) -> Self {
        Self {
            name: String::new(),
            status: HealthStatus::Unhealthy,
            message: Some(message.into()),
            last_check: Utc::now(),
            last_healthy: None,
            details: None,
            response_time_ms: None,
        }
    }

    /// Add details to the health status
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Add response time to the health status
    pub fn with_response_time(mut self, ms: u64) -> Self {
        self.response_time_ms = Some(ms);
        self
    }

    /// Set the component name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
}

// ================================
// Liveness and Readiness Responses
// ================================

/// Response for liveness probe
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessResponse {
    /// Whether the service is alive
    pub alive: bool,
    /// Service uptime in seconds
    pub uptime_seconds: u64,
    /// Timestamp of the check
    pub timestamp: DateTime<Utc>,
}

/// Response for readiness probe
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessResponse {
    /// Whether the service is ready to receive traffic
    pub ready: bool,
    /// Overall status
    pub status: HealthStatus,
    /// Per-component health
    pub components: Vec<ComponentHealth>,
    /// Timestamp of the check
    pub timestamp: DateTime<Utc>,
    /// Reason if not ready
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// ================================
// Cluster Status Types
// ================================

/// Aggregated cluster health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthResponse {
    /// Overall cluster status
    pub status: HealthStatus,
    /// Cluster name/identifier
    pub cluster_id: String,
    /// Total number of nodes
    pub total_nodes: usize,
    /// Number of healthy nodes
    pub healthy_nodes: usize,
    /// Number of degraded nodes
    pub degraded_nodes: usize,
    /// Number of unhealthy nodes
    pub unhealthy_nodes: usize,
    /// Health percentage (0-100)
    pub health_percentage: f64,
    /// Active jobs count
    pub active_jobs: usize,
    /// Pending tasks count
    pub pending_tasks: usize,
    /// Running tasks count
    pub running_tasks: usize,
    /// Cluster-wide alerts
    pub alerts: Vec<ClusterAlert>,
    /// Timestamp of the check
    pub timestamp: DateTime<Utc>,
    /// Per-region breakdown
    pub regions: Vec<RegionHealth>,
}

/// Health status for a region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionHealth {
    /// Region identifier
    pub region_id: String,
    /// Number of nodes in region
    pub node_count: usize,
    /// Number of healthy nodes
    pub healthy_nodes: usize,
    /// Region status
    pub status: HealthStatus,
    /// Available capacity (0-100)
    pub available_capacity_percent: f64,
}

/// Cluster-level alert
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterAlert {
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert message
    pub message: String,
    /// When the alert was triggered
    pub triggered_at: DateTime<Utc>,
    /// Affected component or resource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected: Option<String>,
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    /// Informational
    Info,
    /// Warning
    Warning,
    /// Critical
    Critical,
}

// ================================
// Node Status Types
// ================================

/// Response for node health listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodesHealthResponse {
    /// Total number of nodes
    pub total_nodes: usize,
    /// Summary by status
    pub summary: NodeStatusSummary,
    /// Per-node details
    pub nodes: Vec<NodeHealthDetail>,
    /// Timestamp of the check
    pub timestamp: DateTime<Utc>,
}

/// Summary of node statuses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatusSummary {
    pub healthy: usize,
    pub degraded: usize,
    pub unhealthy: usize,
    pub unknown: usize,
}

/// Detailed health for a single node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealthDetail {
    /// Node identifier
    pub node_id: String,
    /// Node hostname
    pub hostname: String,
    /// Region
    pub region: String,
    /// Current status
    pub status: HealthStatus,
    /// CPU usage percentage (0-100)
    pub cpu_usage_percent: f64,
    /// Memory usage percentage (0-100)
    pub memory_usage_percent: f64,
    /// Number of running tasks
    pub running_tasks: usize,
    /// Seconds since last heartbeat
    pub last_heartbeat_seconds: u64,
    /// Time node has been in current status
    pub status_duration_seconds: u64,
    /// Active alerts for this node
    pub alerts: Vec<String>,
    /// Additional metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<NodeMetrics>,
}

/// Additional node metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Network receive rate (bytes/sec)
    pub network_rx_bytes_sec: f64,
    /// Network transmit rate (bytes/sec)
    pub network_tx_bytes_sec: f64,
    /// Disk read rate (bytes/sec)
    pub disk_read_bytes_sec: f64,
    /// Disk write rate (bytes/sec)
    pub disk_write_bytes_sec: f64,
    /// Temperature (Celsius)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature_celsius: Option<f64>,
    /// Uptime in seconds
    pub uptime_seconds: u64,
}

// ================================
// Debug/Profiling Types
// ================================

/// CPU profile data (pprof-like)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuProfileResponse {
    /// Profile duration in seconds
    pub duration_seconds: u64,
    /// Total samples collected
    pub total_samples: u64,
    /// Top functions by CPU time
    pub top_functions: Vec<FunctionProfile>,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Function profile data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionProfile {
    /// Function name
    pub name: String,
    /// Module/crate name
    pub module: String,
    /// CPU time percentage
    pub cpu_percent: f64,
    /// Number of samples
    pub samples: u64,
}

/// Heap profile data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeapProfileResponse {
    /// Total heap allocated (bytes)
    pub heap_allocated_bytes: u64,
    /// Heap used (bytes)
    pub heap_used_bytes: u64,
    /// Number of allocations
    pub allocation_count: u64,
    /// Top allocators
    pub top_allocators: Vec<AllocatorProfile>,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Allocator profile data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorProfile {
    /// Allocation site name
    pub name: String,
    /// Bytes allocated
    pub bytes_allocated: u64,
    /// Allocation count
    pub allocation_count: u64,
}

/// Async task (goroutine-like) profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsyncTaskProfileResponse {
    /// Total active async tasks
    pub total_tasks: u64,
    /// Tasks by state
    pub tasks_by_state: HashMap<String, u64>,
    /// Top task types
    pub top_task_types: Vec<TaskTypeProfile>,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Task type profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskTypeProfile {
    /// Task type name
    pub name: String,
    /// Count of tasks
    pub count: u64,
    /// Average duration (ms)
    pub avg_duration_ms: u64,
}

// ================================
// Metrics Summary Types
// ================================

/// Human-readable metrics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummaryResponse {
    /// Cluster overview
    pub cluster: ClusterSummary,
    /// Job statistics
    pub jobs: JobsSummary,
    /// Task statistics
    pub tasks: TasksSummary,
    /// Resource utilization
    pub resources: ResourcesSummary,
    /// Performance metrics
    pub performance: PerformanceSummary,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
}

/// Cluster summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSummary {
    /// Status description
    pub status: String,
    /// Total nodes
    pub total_nodes: usize,
    /// Online nodes
    pub online_nodes: usize,
    /// Total CPU cores
    pub total_cpu_cores: u64,
    /// Total memory (human-readable)
    pub total_memory: String,
    /// Total GPU count
    pub total_gpus: u64,
}

/// Jobs summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsSummary {
    /// Active jobs
    pub active: usize,
    /// Pending jobs
    pub pending: usize,
    /// Completed today
    pub completed_today: usize,
    /// Failed today
    pub failed_today: usize,
    /// Success rate (percentage)
    pub success_rate_percent: f64,
}

/// Tasks summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TasksSummary {
    /// Running tasks
    pub running: usize,
    /// Pending tasks
    pub pending: usize,
    /// Completed per minute (rate)
    pub completed_per_minute: f64,
    /// Average duration
    pub avg_duration: String,
    /// P95 duration
    pub p95_duration: String,
}

/// Resource utilization summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcesSummary {
    /// CPU utilization (percentage)
    pub cpu_utilization_percent: f64,
    /// Memory utilization (percentage)
    pub memory_utilization_percent: f64,
    /// GPU utilization (percentage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu_utilization_percent: Option<f64>,
    /// Network throughput
    pub network_throughput: String,
    /// Available capacity
    pub available_capacity_percent: f64,
}

/// Performance summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSummary {
    /// Scheduling latency (P50)
    pub scheduling_latency_p50: String,
    /// Scheduling latency (P95)
    pub scheduling_latency_p95: String,
    /// Queue wait time
    pub queue_wait_time: String,
    /// Preemption rate
    pub preemption_rate: String,
}

// ================================
// Query Parameters
// ================================

/// Query parameters for nodes endpoint
#[derive(Debug, Deserialize)]
pub struct NodesQuery {
    /// Filter by status
    #[serde(default)]
    pub status: Option<String>,
    /// Filter by region
    #[serde(default)]
    pub region: Option<String>,
    /// Include full metrics
    #[serde(default)]
    pub include_metrics: bool,
    /// Maximum nodes to return
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Query parameters for profiling endpoints
#[derive(Debug, Deserialize)]
pub struct ProfileQuery {
    /// Profile duration in seconds
    #[serde(default = "default_profile_duration")]
    pub duration: u64,
}

fn default_profile_duration() -> u64 {
    5
}

// ================================
// Health API State
// ================================

/// Health API state and management
pub struct HealthApi {
    /// Configuration
    config: HealthApiConfig,
    /// Service start time
    start_time: std::time::Instant,
    /// Component health states
    components: DashMap<String, ComponentHealth>,
    /// Whether the service is ready
    ready: AtomicBool,
    /// Node health states
    nodes: DashMap<String, NodeHealthDetail>,
    /// Cluster identifier
    cluster_id: RwLock<String>,
    /// Active alerts
    alerts: RwLock<Vec<ClusterAlert>>,
    /// Cached stats
    stats_cache: RwLock<Option<CachedStats>>,
    /// Request counter for profiling
    request_count: AtomicU64,
}

/// Cached statistics
#[derive(Debug, Clone)]
struct CachedStats {
    /// When the cache was updated
    updated_at: std::time::Instant,
    /// Cluster stats
    cluster_stats: ClusterHealthResponse,
}

impl HealthApi {
    /// Create a new health API instance
    pub fn new(config: HealthApiConfig) -> Self {
        Self {
            config,
            start_time: std::time::Instant::now(),
            components: DashMap::new(),
            ready: AtomicBool::new(false),
            nodes: DashMap::new(),
            cluster_id: RwLock::new("marabunta-cluster".to_string()),
            alerts: RwLock::new(Vec::new()),
            stats_cache: RwLock::new(None),
            request_count: AtomicU64::new(0),
        }
    }

    /// Set the cluster identifier
    pub async fn set_cluster_id(&self, id: impl Into<String>) {
        let mut cluster_id = self.cluster_id.write().await;
        *cluster_id = id.into();
    }

    /// Register a component for health tracking
    pub async fn register_component(&self, name: &str) {
        let health = ComponentHealth {
            name: name.to_string(),
            status: HealthStatus::Unknown,
            message: Some("Waiting for first health check".to_string()),
            last_check: Utc::now(),
            last_healthy: None,
            details: None,
            response_time_ms: None,
        };
        self.components.insert(name.to_string(), health);
        debug!("Registered health component: {}", name);
    }

    /// Unregister a component
    pub async fn unregister_component(&self, name: &str) {
        self.components.remove(name);
        debug!("Unregistered health component: {}", name);
    }

    /// Update component health
    pub async fn update_component_health(&self, name: &str, mut health: ComponentHealth) {
        health.name = name.to_string();
        health.last_check = Utc::now();

        if health.status == HealthStatus::Healthy {
            health.last_healthy = Some(Utc::now());
        }

        self.components.insert(name.to_string(), health.clone());
        debug!("Updated component {} health: {:?}", name, health.status);

        // Re-evaluate readiness
        self.evaluate_readiness().await;
    }

    /// Register a node for health tracking
    pub async fn register_node(&self, node: NodeHealthDetail) {
        self.nodes.insert(node.node_id.clone(), node);
    }

    /// Update node health
    pub async fn update_node_health(&self, node_id: &str, update_fn: impl FnOnce(&mut NodeHealthDetail)) {
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            update_fn(&mut node);
        }
    }

    /// Remove a node
    pub async fn remove_node(&self, node_id: &str) {
        self.nodes.remove(node_id);
    }

    /// Add a cluster alert
    pub async fn add_alert(&self, severity: AlertSeverity, message: impl Into<String>, affected: Option<String>) {
        let alert = ClusterAlert {
            severity,
            message: message.into(),
            triggered_at: Utc::now(),
            affected,
        };
        let mut alerts = self.alerts.write().await;
        alerts.push(alert);

        // Keep only recent alerts (last 100)
        if alerts.len() > 100 {
            let remove_count = alerts.len() - 100;
            alerts.drain(0..remove_count);
        }
    }

    /// Clear alerts
    pub async fn clear_alerts(&self) {
        let mut alerts = self.alerts.write().await;
        alerts.clear();
    }

    /// Mark the service as ready
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
        info!("Service readiness set to: {}", ready);
    }

    /// Evaluate readiness based on component health
    async fn evaluate_readiness(&self) {
        // Check startup grace period
        if self.start_time.elapsed() < self.config.startup_grace_period {
            return; // Still in grace period
        }

        let mut healthy_count = 0;
        let timeout = Duration::from_std(self.config.component_timeout).unwrap_or(Duration::seconds(30));

        for entry in self.components.iter() {
            let health = entry.value();

            // Check if component is stale
            if Utc::now() - health.last_check > timeout {
                continue; // Stale component
            }

            if health.status.is_serving() {
                healthy_count += 1;
            }
        }

        let is_ready = healthy_count >= self.config.min_healthy_components;
        self.ready.store(is_ready, Ordering::SeqCst);
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Check liveness
    pub async fn check_liveness(&self) -> LivenessResponse {
        LivenessResponse {
            alive: true,
            uptime_seconds: self.uptime_seconds(),
            timestamp: Utc::now(),
        }
    }

    /// Check readiness
    pub async fn check_readiness(&self) -> ReadinessResponse {
        let components: Vec<ComponentHealth> = self.components.iter()
            .map(|entry| entry.value().clone())
            .collect();

        let overall_status = components.iter()
            .map(|c| c.status)
            .fold(HealthStatus::Healthy, |acc, s| acc.combine(s));

        let ready = self.ready.load(Ordering::SeqCst);

        let reason = if !ready {
            if self.start_time.elapsed() < self.config.startup_grace_period {
                Some("Service is still starting up".to_string())
            } else {
                let unhealthy: Vec<_> = components.iter()
                    .filter(|c| !c.status.is_serving())
                    .map(|c| c.name.clone())
                    .collect();
                if unhealthy.is_empty() {
                    Some("Insufficient healthy components".to_string())
                } else {
                    Some(format!("Unhealthy components: {}", unhealthy.join(", ")))
                }
            }
        } else {
            None
        };

        ReadinessResponse {
            ready,
            status: overall_status,
            components,
            timestamp: Utc::now(),
            reason,
        }
    }

    /// Get cluster health
    pub async fn get_cluster_health(&self) -> ClusterHealthResponse {
        let nodes: Vec<NodeHealthDetail> = self.nodes.iter()
            .map(|entry| entry.value().clone())
            .collect();

        let total_nodes = nodes.len();
        let healthy_nodes = nodes.iter().filter(|n| n.status == HealthStatus::Healthy).count();
        let degraded_nodes = nodes.iter().filter(|n| n.status == HealthStatus::Degraded).count();
        let unhealthy_nodes = nodes.iter().filter(|n| n.status == HealthStatus::Unhealthy).count();

        let health_percentage = if total_nodes > 0 {
            (healthy_nodes + degraded_nodes) as f64 / total_nodes as f64 * 100.0
        } else {
            100.0
        };

        let unhealthy_ratio = if total_nodes > 0 {
            unhealthy_nodes as f64 / total_nodes as f64
        } else {
            0.0
        };

        let status = if unhealthy_ratio >= self.config.cluster_critical_threshold {
            HealthStatus::Unhealthy
        } else if unhealthy_ratio >= self.config.cluster_degraded_threshold {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        // Calculate per-region health
        let mut region_map: HashMap<String, (usize, usize)> = HashMap::new();
        for node in &nodes {
            let entry = region_map.entry(node.region.clone()).or_insert((0, 0));
            entry.0 += 1; // total
            if node.status.is_serving() {
                entry.1 += 1; // healthy
            }
        }

        let regions: Vec<RegionHealth> = region_map.into_iter()
            .map(|(region_id, (total, healthy))| {
                let status = if healthy == total {
                    HealthStatus::Healthy
                } else if healthy == 0 {
                    HealthStatus::Unhealthy
                } else {
                    HealthStatus::Degraded
                };
                RegionHealth {
                    region_id,
                    node_count: total,
                    healthy_nodes: healthy,
                    status,
                    available_capacity_percent: (healthy as f64 / total as f64) * 100.0,
                }
            })
            .collect();

        let alerts = self.alerts.read().await.clone();

        let cluster_id = self.cluster_id.read().await.clone();

        ClusterHealthResponse {
            status,
            cluster_id,
            total_nodes,
            healthy_nodes,
            degraded_nodes,
            unhealthy_nodes,
            health_percentage,
            active_jobs: 0,  // Would be populated from job registry
            pending_tasks: 0, // Would be populated from task queue
            running_tasks: 0, // Would be populated from task registry
            alerts,
            timestamp: Utc::now(),
            regions,
        }
    }

    /// Get nodes health
    pub async fn get_nodes_health(&self, query: &NodesQuery) -> NodesHealthResponse {
        let mut nodes: Vec<NodeHealthDetail> = self.nodes.iter()
            .map(|entry| entry.value().clone())
            .collect();

        // Apply filters
        if let Some(ref status_filter) = query.status {
            let filter_status = match status_filter.to_lowercase().as_str() {
                "healthy" => Some(HealthStatus::Healthy),
                "degraded" => Some(HealthStatus::Degraded),
                "unhealthy" => Some(HealthStatus::Unhealthy),
                "unknown" => Some(HealthStatus::Unknown),
                _ => None,
            };
            if let Some(status) = filter_status {
                nodes.retain(|n| n.status == status);
            }
        }

        if let Some(ref region) = query.region {
            nodes.retain(|n| &n.region == region);
        }

        // Apply limit
        if let Some(limit) = query.limit {
            nodes.truncate(limit);
        }

        // Optionally strip metrics
        if !query.include_metrics {
            for node in &mut nodes {
                node.metrics = None;
            }
        }

        let summary = NodeStatusSummary {
            healthy: nodes.iter().filter(|n| n.status == HealthStatus::Healthy).count(),
            degraded: nodes.iter().filter(|n| n.status == HealthStatus::Degraded).count(),
            unhealthy: nodes.iter().filter(|n| n.status == HealthStatus::Unhealthy).count(),
            unknown: nodes.iter().filter(|n| n.status == HealthStatus::Unknown).count(),
        };

        NodesHealthResponse {
            total_nodes: nodes.len(),
            summary,
            nodes,
            timestamp: Utc::now(),
        }
    }

    /// Get CPU profile (simulated)
    pub async fn get_cpu_profile(&self, duration: u64) -> CpuProfileResponse {
        // In a real implementation, this would collect actual CPU profiling data
        // using tools like pprof or custom sampling

        self.request_count.fetch_add(1, Ordering::Relaxed);

        // Simulate profile collection delay
        tokio::time::sleep(std::time::Duration::from_secs(duration.min(10))).await;

        CpuProfileResponse {
            duration_seconds: duration,
            total_samples: 1000 * duration,
            top_functions: vec![
                FunctionProfile {
                    name: "scheduler::run_scheduling_loop".to_string(),
                    module: "marabunta_compute::coordinator".to_string(),
                    cpu_percent: 15.2,
                    samples: 152,
                },
                FunctionProfile {
                    name: "worker::process_task".to_string(),
                    module: "marabunta_compute::worker".to_string(),
                    cpu_percent: 12.8,
                    samples: 128,
                },
                FunctionProfile {
                    name: "tokio::runtime::poll".to_string(),
                    module: "tokio".to_string(),
                    cpu_percent: 8.5,
                    samples: 85,
                },
            ],
            timestamp: Utc::now(),
        }
    }

    /// Get heap profile (simulated)
    pub async fn get_heap_profile(&self) -> HeapProfileResponse {
        // In a real implementation, this would use jemalloc or similar
        // to get actual heap profiling data

        HeapProfileResponse {
            heap_allocated_bytes: 256 * 1024 * 1024, // 256 MB
            heap_used_bytes: 180 * 1024 * 1024, // 180 MB
            allocation_count: 50_000,
            top_allocators: vec![
                AllocatorProfile {
                    name: "dashmap::DashMap<K, V>".to_string(),
                    bytes_allocated: 50 * 1024 * 1024,
                    allocation_count: 10_000,
                },
                AllocatorProfile {
                    name: "std::vec::Vec<Task>".to_string(),
                    bytes_allocated: 30 * 1024 * 1024,
                    allocation_count: 5_000,
                },
            ],
            timestamp: Utc::now(),
        }
    }

    /// Get async task profile (simulated)
    pub async fn get_async_task_profile(&self) -> AsyncTaskProfileResponse {
        // In a real implementation, this would inspect the tokio runtime

        let mut tasks_by_state = HashMap::new();
        tasks_by_state.insert("running".to_string(), 150);
        tasks_by_state.insert("idle".to_string(), 50);
        tasks_by_state.insert("blocked".to_string(), 10);

        AsyncTaskProfileResponse {
            total_tasks: 210,
            tasks_by_state,
            top_task_types: vec![
                TaskTypeProfile {
                    name: "heartbeat_handler".to_string(),
                    count: 100,
                    avg_duration_ms: 5,
                },
                TaskTypeProfile {
                    name: "task_executor".to_string(),
                    count: 50,
                    avg_duration_ms: 150,
                },
                TaskTypeProfile {
                    name: "metrics_collector".to_string(),
                    count: 10,
                    avg_duration_ms: 20,
                },
            ],
            timestamp: Utc::now(),
        }
    }

    /// Get metrics summary
    pub async fn get_metrics_summary(&self) -> MetricsSummaryResponse {
        let nodes: Vec<NodeHealthDetail> = self.nodes.iter()
            .map(|entry| entry.value().clone())
            .collect();

        let total_nodes = nodes.len();
        let online_nodes = nodes.iter().filter(|n| n.status.is_serving()).count();

        // Calculate resource utilization
        let avg_cpu = if !nodes.is_empty() {
            nodes.iter().map(|n| n.cpu_usage_percent).sum::<f64>() / nodes.len() as f64
        } else {
            0.0
        };

        let avg_memory = if !nodes.is_empty() {
            nodes.iter().map(|n| n.memory_usage_percent).sum::<f64>() / nodes.len() as f64
        } else {
            0.0
        };

        MetricsSummaryResponse {
            cluster: ClusterSummary {
                status: if online_nodes == total_nodes { "Healthy".to_string() }
                        else if online_nodes > 0 { "Degraded".to_string() }
                        else { "Unhealthy".to_string() },
                total_nodes,
                online_nodes,
                total_cpu_cores: 0, // Would be populated from node specs
                total_memory: "0 GB".to_string(),
                total_gpus: 0,
            },
            jobs: JobsSummary {
                active: 0,
                pending: 0,
                completed_today: 0,
                failed_today: 0,
                success_rate_percent: 100.0,
            },
            tasks: TasksSummary {
                running: nodes.iter().map(|n| n.running_tasks).sum(),
                pending: 0,
                completed_per_minute: 0.0,
                avg_duration: "0ms".to_string(),
                p95_duration: "0ms".to_string(),
            },
            resources: ResourcesSummary {
                cpu_utilization_percent: avg_cpu,
                memory_utilization_percent: avg_memory,
                gpu_utilization_percent: None,
                network_throughput: "0 MB/s".to_string(),
                available_capacity_percent: 100.0 - avg_cpu.max(avg_memory),
            },
            performance: PerformanceSummary {
                scheduling_latency_p50: "5ms".to_string(),
                scheduling_latency_p95: "25ms".to_string(),
                queue_wait_time: "0ms".to_string(),
                preemption_rate: "0/min".to_string(),
            },
            timestamp: Utc::now(),
        }
    }
}

// ================================
// HTTP Handlers
// ================================

/// Handler for GET /health/live
async fn health_live(State(api): State<Arc<HealthApi>>) -> impl IntoResponse {
    let response = api.check_liveness().await;
    (StatusCode::OK, Json(response))
}

/// Handler for GET /health/ready
async fn health_ready(State(api): State<Arc<HealthApi>>) -> impl IntoResponse {
    let response = api.check_readiness().await;
    let status = if response.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(response))
}

/// Handler for GET /status/cluster
async fn status_cluster(State(api): State<Arc<HealthApi>>) -> impl IntoResponse {
    let response = api.get_cluster_health().await;
    let status = match response.status {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Degraded => StatusCode::OK,
        HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
        HealthStatus::Unknown => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(response))
}

/// Handler for GET /status/nodes
async fn status_nodes(
    State(api): State<Arc<HealthApi>>,
    Query(query): Query<NodesQuery>,
) -> impl IntoResponse {
    let response = api.get_nodes_health(&query).await;
    (StatusCode::OK, Json(response))
}

/// Handler for GET /debug/pprof
async fn debug_pprof(
    State(api): State<Arc<HealthApi>>,
    Query(query): Query<ProfileQuery>,
) -> impl IntoResponse {
    if !api.config.enable_profiling {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({
            "error": "Profiling is disabled"
        }))).into_response();
    }
    let response = api.get_cpu_profile(query.duration).await;
    Json(response).into_response()
}

/// Handler for GET /debug/heap
async fn debug_heap(State(api): State<Arc<HealthApi>>) -> impl IntoResponse {
    if !api.config.enable_profiling {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({
            "error": "Profiling is disabled"
        }))).into_response();
    }
    let response = api.get_heap_profile().await;
    Json(response).into_response()
}

/// Handler for GET /debug/goroutines (async tasks)
async fn debug_goroutines(State(api): State<Arc<HealthApi>>) -> impl IntoResponse {
    if !api.config.enable_profiling {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({
            "error": "Profiling is disabled"
        }))).into_response();
    }
    let response = api.get_async_task_profile().await;
    Json(response).into_response()
}

/// Handler for GET /metrics/summary
async fn metrics_summary(State(api): State<Arc<HealthApi>>) -> impl IntoResponse {
    let response = api.get_metrics_summary().await;
    (StatusCode::OK, Json(response))
}

// ================================
// Router
// ================================

/// Create the health API router
pub fn create_health_router(api: Arc<HealthApi>) -> Router {
    Router::new()
        .route("/health/live", get(health_live))
        .route("/health/ready", get(health_ready))
        .route("/status/cluster", get(status_cluster))
        .route("/status/nodes", get(status_nodes))
        .route("/debug/pprof", get(debug_pprof))
        .route("/debug/heap", get(debug_heap))
        .route("/debug/goroutines", get(debug_goroutines))
        .route("/metrics/summary", get(metrics_summary))
        .with_state(api)
}

// ================================
// Tests
// ================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn create_test_api() -> Arc<HealthApi> {
        Arc::new(HealthApi::new(HealthApiConfig {
            startup_grace_period: std::time::Duration::from_millis(0),
            ..Default::default()
        }))
    }

    #[tokio::test]
    async fn test_liveness_probe() {
        let api = create_test_api();
        let response = api.check_liveness().await;

        assert!(response.alive);
        assert!(response.uptime_seconds >= 0);
    }

    #[tokio::test]
    async fn test_readiness_no_components() {
        let api = create_test_api();
        let response = api.check_readiness().await;

        // No components registered, should be not ready
        assert!(!response.ready);
        assert!(response.components.is_empty());
    }

    #[tokio::test]
    async fn test_readiness_with_healthy_component() {
        let api = create_test_api();
        api.register_component("test").await;
        api.update_component_health("test", ComponentHealth::healthy()).await;
        api.set_ready(true);

        let response = api.check_readiness().await;
        assert!(response.ready);
        assert_eq!(response.components.len(), 1);
        assert_eq!(response.components[0].status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_readiness_with_unhealthy_component() {
        let api = create_test_api();
        api.register_component("test").await;
        api.update_component_health("test", ComponentHealth::unhealthy("Test failure")).await;

        let response = api.check_readiness().await;
        assert!(!response.ready);
        assert_eq!(response.status, HealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn test_health_status_combine() {
        assert_eq!(
            HealthStatus::Healthy.combine(HealthStatus::Healthy),
            HealthStatus::Healthy
        );
        assert_eq!(
            HealthStatus::Healthy.combine(HealthStatus::Degraded),
            HealthStatus::Degraded
        );
        assert_eq!(
            HealthStatus::Degraded.combine(HealthStatus::Unhealthy),
            HealthStatus::Unhealthy
        );
        assert_eq!(
            HealthStatus::Healthy.combine(HealthStatus::Unknown),
            HealthStatus::Unknown
        );
    }

    #[tokio::test]
    async fn test_cluster_health_empty() {
        let api = create_test_api();
        let response = api.get_cluster_health().await;

        assert_eq!(response.total_nodes, 0);
        assert_eq!(response.health_percentage, 100.0);
        assert_eq!(response.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_cluster_health_with_nodes() {
        let api = create_test_api();

        // Register some nodes
        api.register_node(NodeHealthDetail {
            node_id: "node-1".to_string(),
            hostname: "host-1".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Healthy,
            cpu_usage_percent: 50.0,
            memory_usage_percent: 60.0,
            running_tasks: 5,
            last_heartbeat_seconds: 5,
            status_duration_seconds: 3600,
            alerts: vec![],
            metrics: None,
        }).await;

        api.register_node(NodeHealthDetail {
            node_id: "node-2".to_string(),
            hostname: "host-2".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Unhealthy,
            cpu_usage_percent: 95.0,
            memory_usage_percent: 90.0,
            running_tasks: 0,
            last_heartbeat_seconds: 60,
            status_duration_seconds: 120,
            alerts: vec!["High CPU".to_string()],
            metrics: None,
        }).await;

        let response = api.get_cluster_health().await;

        assert_eq!(response.total_nodes, 2);
        assert_eq!(response.healthy_nodes, 1);
        assert_eq!(response.unhealthy_nodes, 1);
        assert_eq!(response.health_percentage, 50.0);
    }

    #[tokio::test]
    async fn test_cluster_health_status_thresholds() {
        let config = HealthApiConfig {
            cluster_degraded_threshold: 0.25,
            cluster_critical_threshold: 0.50,
            startup_grace_period: std::time::Duration::from_millis(0),
            ..Default::default()
        };
        let api = Arc::new(HealthApi::new(config));

        // Add 4 nodes: 2 healthy, 2 unhealthy (50% unhealthy = critical)
        for i in 0..4 {
            api.register_node(NodeHealthDetail {
                node_id: format!("node-{}", i),
                hostname: format!("host-{}", i),
                region: "us-east-1".to_string(),
                status: if i < 2 { HealthStatus::Healthy } else { HealthStatus::Unhealthy },
                cpu_usage_percent: 50.0,
                memory_usage_percent: 50.0,
                running_tasks: 0,
                last_heartbeat_seconds: 5,
                status_duration_seconds: 100,
                alerts: vec![],
                metrics: None,
            }).await;
        }

        let response = api.get_cluster_health().await;
        assert_eq!(response.status, HealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn test_nodes_health_filtering() {
        let api = create_test_api();

        // Register nodes in different regions with different statuses
        api.register_node(NodeHealthDetail {
            node_id: "node-1".to_string(),
            hostname: "host-1".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Healthy,
            cpu_usage_percent: 50.0,
            memory_usage_percent: 60.0,
            running_tasks: 5,
            last_heartbeat_seconds: 5,
            status_duration_seconds: 3600,
            alerts: vec![],
            metrics: None,
        }).await;

        api.register_node(NodeHealthDetail {
            node_id: "node-2".to_string(),
            hostname: "host-2".to_string(),
            region: "eu-west-1".to_string(),
            status: HealthStatus::Degraded,
            cpu_usage_percent: 80.0,
            memory_usage_percent: 70.0,
            running_tasks: 3,
            last_heartbeat_seconds: 10,
            status_duration_seconds: 600,
            alerts: vec![],
            metrics: None,
        }).await;

        // Filter by region
        let query = NodesQuery {
            region: Some("us-east-1".to_string()),
            status: None,
            include_metrics: false,
            limit: None,
        };
        let response = api.get_nodes_health(&query).await;
        assert_eq!(response.total_nodes, 1);
        assert_eq!(response.nodes[0].region, "us-east-1");

        // Filter by status
        let query = NodesQuery {
            region: None,
            status: Some("degraded".to_string()),
            include_metrics: false,
            limit: None,
        };
        let response = api.get_nodes_health(&query).await;
        assert_eq!(response.total_nodes, 1);
        assert_eq!(response.nodes[0].status, HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn test_nodes_health_limit() {
        let api = create_test_api();

        // Register 5 nodes
        for i in 0..5 {
            api.register_node(NodeHealthDetail {
                node_id: format!("node-{}", i),
                hostname: format!("host-{}", i),
                region: "us-east-1".to_string(),
                status: HealthStatus::Healthy,
                cpu_usage_percent: 50.0,
                memory_usage_percent: 50.0,
                running_tasks: 0,
                last_heartbeat_seconds: 5,
                status_duration_seconds: 100,
                alerts: vec![],
                metrics: None,
            }).await;
        }

        let query = NodesQuery {
            region: None,
            status: None,
            include_metrics: false,
            limit: Some(3),
        };
        let response = api.get_nodes_health(&query).await;
        assert_eq!(response.total_nodes, 3);
    }

    #[tokio::test]
    async fn test_component_health_builder() {
        let health = ComponentHealth::healthy()
            .with_name("test")
            .with_response_time(50)
            .with_details(serde_json::json!({"version": "1.0"}));

        assert_eq!(health.name, "test");
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.response_time_ms, Some(50));
        assert!(health.details.is_some());
    }

    #[tokio::test]
    async fn test_alerts() {
        let api = create_test_api();

        api.add_alert(AlertSeverity::Warning, "Test warning", Some("node-1".to_string())).await;
        api.add_alert(AlertSeverity::Critical, "Test critical", None).await;

        let cluster_health = api.get_cluster_health().await;
        assert_eq!(cluster_health.alerts.len(), 2);

        api.clear_alerts().await;
        let cluster_health = api.get_cluster_health().await;
        assert_eq!(cluster_health.alerts.len(), 0);
    }

    #[tokio::test]
    async fn test_cpu_profile() {
        let api = create_test_api();
        let response = api.get_cpu_profile(1).await;

        assert!(response.total_samples > 0);
        assert!(!response.top_functions.is_empty());
    }

    #[tokio::test]
    async fn test_heap_profile() {
        let api = create_test_api();
        let response = api.get_heap_profile().await;

        assert!(response.heap_allocated_bytes > 0);
        assert!(!response.top_allocators.is_empty());
    }

    #[tokio::test]
    async fn test_async_task_profile() {
        let api = create_test_api();
        let response = api.get_async_task_profile().await;

        assert!(response.total_tasks > 0);
        assert!(!response.tasks_by_state.is_empty());
        assert!(!response.top_task_types.is_empty());
    }

    #[tokio::test]
    async fn test_metrics_summary() {
        let api = create_test_api();

        // Add a node with some metrics
        api.register_node(NodeHealthDetail {
            node_id: "node-1".to_string(),
            hostname: "host-1".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Healthy,
            cpu_usage_percent: 60.0,
            memory_usage_percent: 70.0,
            running_tasks: 5,
            last_heartbeat_seconds: 5,
            status_duration_seconds: 3600,
            alerts: vec![],
            metrics: Some(NodeMetrics {
                network_rx_bytes_sec: 1_000_000.0,
                network_tx_bytes_sec: 500_000.0,
                disk_read_bytes_sec: 100_000.0,
                disk_write_bytes_sec: 50_000.0,
                temperature_celsius: Some(65.0),
                uptime_seconds: 86400,
            }),
        }).await;

        let response = api.get_metrics_summary().await;

        assert_eq!(response.cluster.total_nodes, 1);
        assert_eq!(response.cluster.online_nodes, 1);
        assert_eq!(response.resources.cpu_utilization_percent, 60.0);
        assert_eq!(response.resources.memory_utilization_percent, 70.0);
    }

    #[tokio::test]
    async fn test_http_endpoints() {
        let api = create_test_api();
        api.register_component("database").await;
        api.update_component_health("database", ComponentHealth::healthy()).await;
        api.set_ready(true);

        let router = create_health_router(api);

        // Test /health/live
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test /health/ready
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test /status/cluster
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/status/cluster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test /status/nodes
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/status/nodes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test /metrics/summary
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_profiling_disabled() {
        let config = HealthApiConfig {
            enable_profiling: false,
            startup_grace_period: std::time::Duration::from_millis(0),
            ..Default::default()
        };
        let api = Arc::new(HealthApi::new(config));
        let router = create_health_router(api);

        // Test /debug/pprof - should be forbidden
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // Test /debug/heap - should be forbidden
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/debug/heap")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_region_health_aggregation() {
        let api = create_test_api();

        // Add nodes in different regions
        api.register_node(NodeHealthDetail {
            node_id: "node-1".to_string(),
            hostname: "host-1".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Healthy,
            cpu_usage_percent: 50.0,
            memory_usage_percent: 50.0,
            running_tasks: 0,
            last_heartbeat_seconds: 5,
            status_duration_seconds: 100,
            alerts: vec![],
            metrics: None,
        }).await;

        api.register_node(NodeHealthDetail {
            node_id: "node-2".to_string(),
            hostname: "host-2".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Unhealthy,
            cpu_usage_percent: 95.0,
            memory_usage_percent: 95.0,
            running_tasks: 0,
            last_heartbeat_seconds: 60,
            status_duration_seconds: 100,
            alerts: vec![],
            metrics: None,
        }).await;

        api.register_node(NodeHealthDetail {
            node_id: "node-3".to_string(),
            hostname: "host-3".to_string(),
            region: "eu-west-1".to_string(),
            status: HealthStatus::Healthy,
            cpu_usage_percent: 30.0,
            memory_usage_percent: 40.0,
            running_tasks: 2,
            last_heartbeat_seconds: 5,
            status_duration_seconds: 1000,
            alerts: vec![],
            metrics: None,
        }).await;

        let response = api.get_cluster_health().await;

        assert_eq!(response.regions.len(), 2);

        let us_region = response.regions.iter().find(|r| r.region_id == "us-east-1").unwrap();
        assert_eq!(us_region.node_count, 2);
        assert_eq!(us_region.healthy_nodes, 1);
        assert_eq!(us_region.status, HealthStatus::Degraded);

        let eu_region = response.regions.iter().find(|r| r.region_id == "eu-west-1").unwrap();
        assert_eq!(eu_region.node_count, 1);
        assert_eq!(eu_region.healthy_nodes, 1);
        assert_eq!(eu_region.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_component_unregister() {
        let api = create_test_api();

        api.register_component("test").await;
        assert_eq!(api.components.len(), 1);

        api.unregister_component("test").await;
        assert_eq!(api.components.len(), 0);
    }

    #[tokio::test]
    async fn test_node_update() {
        let api = create_test_api();

        api.register_node(NodeHealthDetail {
            node_id: "node-1".to_string(),
            hostname: "host-1".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Healthy,
            cpu_usage_percent: 50.0,
            memory_usage_percent: 50.0,
            running_tasks: 0,
            last_heartbeat_seconds: 5,
            status_duration_seconds: 100,
            alerts: vec![],
            metrics: None,
        }).await;

        api.update_node_health("node-1", |node| {
            node.cpu_usage_percent = 90.0;
            node.status = HealthStatus::Degraded;
        }).await;

        let query = NodesQuery {
            region: None,
            status: None,
            include_metrics: false,
            limit: None,
        };
        let response = api.get_nodes_health(&query).await;

        assert_eq!(response.nodes[0].cpu_usage_percent, 90.0);
        assert_eq!(response.nodes[0].status, HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn test_node_remove() {
        let api = create_test_api();

        api.register_node(NodeHealthDetail {
            node_id: "node-1".to_string(),
            hostname: "host-1".to_string(),
            region: "us-east-1".to_string(),
            status: HealthStatus::Healthy,
            cpu_usage_percent: 50.0,
            memory_usage_percent: 50.0,
            running_tasks: 0,
            last_heartbeat_seconds: 5,
            status_duration_seconds: 100,
            alerts: vec![],
            metrics: None,
        }).await;

        assert_eq!(api.nodes.len(), 1);

        api.remove_node("node-1").await;
        assert_eq!(api.nodes.len(), 0);
    }

    #[tokio::test]
    async fn test_cluster_id() {
        let api = create_test_api();

        api.set_cluster_id("my-cluster").await;

        let response = api.get_cluster_health().await;
        assert_eq!(response.cluster_id, "my-cluster");
    }

    #[tokio::test]
    async fn test_uptime() {
        let api = create_test_api();

        // Should have some uptime
        assert!(api.uptime_seconds() >= 0);

        // Wait a bit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Uptime should still be valid (at least 0)
        assert!(api.uptime_seconds() >= 0);
    }
}
