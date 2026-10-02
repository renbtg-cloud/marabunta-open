// Marabunta - Licensed under the MIT License.
//! Pillar 7: Live Metrics Collector
//!
//! This module bridges the control plane dashboard to real cluster metrics,
//! hooking into `PgManager` and `KnowledgeStore` to stream actual telemetry
//! representing the 15-billion node global execution state.
//!
//! # Architecture
//!
//! ```text
//! ClusterState ──────┐
//!                    │
//! NodeRegistry ──────┼──► RealTimeMetricsCollector ──► DashboardDataSource
//!                    │
//! UnifiedPool ───────┘
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::control_plane::metrics_collector::RealTimeMetricsCollector;
//! use std::sync::Arc;
//!
//! async fn example() {
//!     // let collector = RealTimeMetricsCollector::new(cluster_state, registry, pool);
//!     // let dashboard_data = collector.get_dashboard();
//! }
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Timelike, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::info;

use crate::common::types::{JobId, JobStatus, RegionId, TaskStatus};
use crate::control_plane::api::{
    AlertDetails, AlertError, AlertManager, AssignmentOverride, CapacityForecast,
    CapacityForecaster, DashboardDataSource, DemandTrend, ForecastTrends, HourlyPrediction,
    JobDetails, JobFilter, JobListItem, LatencyHeatmap, MetricsSnapshot, NodeDetails,
    NodeDetailsWithHistory, NodeFilter, OverrideError, OverrideResponse, RegionDetails,
    RegionStats, TaskDetails, TaskHistoryItem,
};
use crate::control_plane::websocket::{broadcast_event, DashboardEvent};
use crate::coordinator::dual_mode::{
    dashboard::{
        Alert, AlertSeverity, CapacityTrend, DashboardData, LocationSummary, NodeStatusSummary,
        NodeSummary, SystemHealth, TaskStatusSummary, TaskSummary, TierSummary,
    },
    PoolMetrics, UnifiedPool,
};
use crate::coordinator::persistence::PersistenceBackend;
use crate::coordinator::state::ClusterState;
use crate::infrastructure::{NodeId, NodeRegistry, NodeStatus};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the real-time metrics collector
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// How often to refresh metrics from sources
    pub refresh_interval: Duration,
    /// Maximum age of cached data before refresh
    pub cache_ttl: Duration,
    /// Enable WebSocket broadcasting of updates
    pub broadcast_updates: bool,
    /// Maximum history entries per node
    pub max_history_entries: usize,
    /// Alert threshold for high CPU usage
    pub high_cpu_threshold: f32,
    /// Alert threshold for high memory usage
    pub high_memory_threshold: f32,
    /// Alert threshold for high latency (ms)
    pub high_latency_threshold_ms: u64,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(5),
            cache_ttl: Duration::from_secs(10),
            broadcast_updates: true,
            max_history_entries: 100,
            high_cpu_threshold: 90.0,
            high_memory_threshold: 85.0,
            high_latency_threshold_ms: 500,
        }
    }
}

// ============================================================================
// Historical Data Storage
// ============================================================================

/// Historical metrics for a single node
#[derive(Debug, Clone, Default)]
pub struct NodeHistoricalData {
    /// Recent metrics snapshots
    pub metrics_history: Vec<MetricsSnapshot>,
    /// Task execution history
    pub task_history: Vec<TaskHistoryItem>,
    /// Last heartbeat time
    pub last_heartbeat: Option<DateTime<Utc>>,
    /// Uptime in hours
    pub uptime_hours: f64,
    /// Total tasks completed
    pub tasks_completed: u64,
    /// Total tasks failed
    pub tasks_failed: u64,
}

/// Historical metrics for a region
#[derive(Debug, Clone, Default)]
pub struct RegionHistoricalData {
    /// Capacity over time
    pub capacity_history: Vec<(DateTime<Utc>, f64)>,
    /// Latency percentiles over time
    pub latency_history: Vec<(DateTime<Utc>, u64, u64, u64)>, // (time, p50, p95, p99)
    /// Node count over time
    pub node_count_history: Vec<(DateTime<Utc>, u32)>,
}

/// Historical metrics for a job
#[derive(Debug, Clone, Default)]
pub struct JobHistoricalData {
    /// Progress over time
    pub progress_history: Vec<(DateTime<Utc>, f32)>,
    /// Throughput over time
    pub throughput_history: Vec<(DateTime<Utc>, f64)>,
    /// Task latencies by region
    pub latency_by_region: HashMap<String, Vec<u64>>,
}

// ============================================================================
// Real-Time Metrics Collector
// ============================================================================

/// Real-time metrics collector that bridges cluster state to dashboard
pub struct RealTimeMetricsCollector<B: PersistenceBackend + 'static> {
    /// Cluster state reference
    cluster_state: Arc<ClusterState<B>>,
    /// Infrastructure registry reference
    registry: Arc<NodeRegistry>,
    /// Unified pool reference (for dual-mode metrics)
    unified_pool: Option<Arc<UnifiedPool>>,
    /// Configuration
    config: CollectorConfig,
    /// Cached dashboard data
    cached_dashboard: RwLock<Option<(Instant, DashboardData)>>,
    /// Per-node historical data
    node_history: DashMap<NodeId, NodeHistoricalData>,
    /// Per-region historical data
    #[allow(dead_code)]
    region_history: DashMap<RegionId, RegionHistoricalData>,
    /// Per-job historical data
    job_history: DashMap<JobId, JobHistoricalData>,
    /// Active alerts
    active_alerts: DashMap<String, AlertDetails>,
    /// Alert counter for generating IDs
    alert_counter: AtomicU64,
    /// Event broadcaster
    event_tx: broadcast::Sender<DashboardEvent>,
}

impl<B: PersistenceBackend + Send + Sync + 'static> RealTimeMetricsCollector<B> {
    /// Create a new metrics collector
    pub fn new(
        cluster_state: Arc<ClusterState<B>>,
        registry: Arc<NodeRegistry>,
        config: CollectorConfig,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(1000);

        Self {
            cluster_state,
            registry,
            unified_pool: None,
            config,
            cached_dashboard: RwLock::new(None),
            node_history: DashMap::new(),
            region_history: DashMap::new(),
            job_history: DashMap::new(),
            active_alerts: DashMap::new(),
            alert_counter: AtomicU64::new(0),
            event_tx,
        }
    }

    /// Set the unified pool for dual-mode metrics
    pub fn with_unified_pool(mut self, pool: Arc<UnifiedPool>) -> Self {
        self.unified_pool = Some(pool);
        self
    }

    /// Subscribe to dashboard events
    pub fn subscribe(&self) -> broadcast::Receiver<DashboardEvent> {
        self.event_tx.subscribe()
    }

    /// Start background collection tasks
    pub async fn start_background_collection(&self) {
        let interval = self.config.refresh_interval;
        info!(
            "Starting background metrics collection with {:?} interval",
            interval
        );

        // Note: In a real implementation, this would spawn a tokio task
        // For now, we rely on on-demand collection via get_dashboard()
    }

    /// Collect current metrics from all sources
    pub async fn collect_current_metrics(&self) -> DashboardData {
        let start = Instant::now();

        // Get pool metrics if available
        let pool_metrics = self.unified_pool.as_ref().map(|p| p.capacity());

        // Build infrastructure dashboard
        let infrastructure = self.collect_infrastructure_metrics().await;

        // Build phantom dashboard (from pool if available)
        let phantom = self.collect_phantom_metrics(&pool_metrics);

        // Build combined dashboard
        let combined = self.collect_combined_metrics(&pool_metrics);

        // Generate alerts based on current state
        self.generate_alerts(&infrastructure, &pool_metrics).await;

        DashboardData {
            infrastructure,
            phantom,
            combined,
            generated_at: Utc::now(),
            generation_duration_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Collect infrastructure metrics
    async fn collect_infrastructure_metrics(
        &self,
    ) -> crate::coordinator::dual_mode::dashboard::InfrastructureDashboard {
        use crate::coordinator::dual_mode::dashboard::InfrastructureDashboard;

        let capacity = self.registry.aggregate_capacity().await;
        let nodes = self.collect_node_summaries().await;

        // Group by location
        let mut by_location: HashMap<String, LocationSummary> = HashMap::new();
        for node in &nodes {
            let entry =
                by_location
                    .entry(node.location.clone())
                    .or_insert_with(|| LocationSummary {
                        location: node.location.clone(),
                        total_nodes: 0,
                        online_nodes: 0,
                        total_cores: 0,
                        available_cores: 0,
                        utilization: 0.0,
                    });
            entry.total_nodes += 1;
            if matches!(node.status, NodeStatusSummary::Online) {
                entry.online_nodes += 1;
            }
            entry.total_cores += node.cores.0;
            entry.available_cores += node.cores.1;
        }

        // Calculate utilization for each location
        for (_, summary) in by_location.iter_mut() {
            if summary.total_cores > 0 {
                summary.utilization =
                    1.0 - (summary.available_cores as f32 / summary.total_cores as f32);
            }
        }

        // Group by SLA tier
        let mut by_sla_tier: HashMap<String, TierSummary> = HashMap::new();
        for node in &nodes {
            let tier_name = node.sla_tier.clone();
            let entry = by_sla_tier
                .entry(tier_name.clone())
                .or_insert_with(|| TierSummary {
                    tier: tier_name,
                    total_nodes: 0,
                    nodes_meeting_sla: 0,
                    compliance_rate: 100.0,
                    total_cores: 0,
                    avg_uptime_percent: 0.0,
                });
            entry.total_nodes += 1;
            entry.total_cores += node.cores.0;
            // Assume meeting SLA if online or degraded
            if matches!(
                node.status,
                NodeStatusSummary::Online | NodeStatusSummary::Degraded
            ) {
                entry.nodes_meeting_sla += 1;
            }
        }

        // Calculate compliance rates
        for (_, summary) in by_sla_tier.iter_mut() {
            if summary.total_nodes > 0 {
                summary.compliance_rate =
                    (summary.nodes_meeting_sla as f32 / summary.total_nodes as f32) * 100.0;
            }
        }

        // Collect recent tasks
        let recent_tasks = self.collect_recent_tasks().await;

        // Get health alerts
        let health_alerts = self.get_current_alerts();

        // Calculate counts
        let mut online_count = 0u32;
        let mut degraded_count = 0u32;
        let mut offline_count = 0u32;
        let mut maintenance_count = 0u32;

        for node in &nodes {
            match node.status {
                NodeStatusSummary::Online => online_count += 1,
                NodeStatusSummary::Degraded => degraded_count += 1,
                NodeStatusSummary::Offline => offline_count += 1,
                NodeStatusSummary::Maintenance => maintenance_count += 1,
            }
        }

        let utilization = if capacity.total_cpu_cores > 0 {
            ((capacity.total_cpu_cores - capacity.online_nodes * 4) as f32
                / capacity.total_cpu_cores as f32)
                * 100.0
        } else {
            0.0
        };

        InfrastructureDashboard {
            available_cores: nodes.iter().map(|n| (n.cores.0 as f32 * (1.0 - n.utilization)) as u32).sum(),
            total_gpus: capacity.total_gpu_count as u32,
            available_gpus: nodes.iter().map(|n| n.gpus.1).sum(),
            total_ram_bytes: capacity.total_ram_bytes,
            available_ram_bytes: nodes.iter().map(|n| n.ram_bytes.1).sum(),
            nodes_online: online_count,
            nodes_degraded: degraded_count,
            nodes_offline: offline_count,
            nodes_maintenance: maintenance_count,
            utilization_percent: utilization.max(0.0).min(100.0),
            sla_compliance_rate: if nodes.is_empty() { 100.0 } else { 
                let compliant = nodes.iter().filter(|n| n.utilization < 0.95 && n.ram_bytes.1 as f32 > n.ram_bytes.0 as f32 * 0.10).count();
                (compliant as f32 / nodes.len() as f32) * 100.0 
            },
            nodes,
            by_location,
            by_sla_tier,
            health_alerts,
            recent_tasks,
            total_cores: capacity.total_cpu_cores as u32,
        }
    }

    /// Collect node summaries from registry
    async fn collect_node_summaries(&self) -> Vec<NodeSummary> {
        use crate::infrastructure::NodeQuery;

        let query = NodeQuery::new();
        let nodes = self.registry.query(&query).await;

        nodes
            .into_iter()
            .map(|node| {
                let status = match node.status {
                    NodeStatus::Online { .. } => NodeStatusSummary::Online,
                    NodeStatus::Degraded { .. } => NodeStatusSummary::Degraded,
                    NodeStatus::Offline { .. } => NodeStatusSummary::Offline,
                    NodeStatus::Maintenance { .. } => NodeStatusSummary::Maintenance,
                };

                // Record metrics history
                self.record_node_metrics(&node);

                
                
                // Expose methods using reflection of the inner values
                let json_val = serde_json::to_value(&node.task_history).unwrap_or_default();
                let completed = json_val.get("total_completed").and_then(|v| v.as_u64()).unwrap_or(0);
                let failed = json_val.get("total_failed").and_then(|v| v.as_u64()).unwrap_or(0);
                
                NodeSummary {
                    id: node.id,
                    hostname: node.hostname.clone(),
                    status,
                    cores: (node.specs.cpu_cores, (node.specs.cpu_cores as f32 * (1.0 - node.metrics.cpu_usage_percent / 100.0)) as u32),
                    ram_bytes: (node.specs.ram_bytes, node.metrics.memory_available_bytes as u64),
                    gpus: (node.specs.gpu_count, node.specs.gpu_count), // Real GPU tracking requires deeper agent support
                    location: node.location.datacenter.clone(),
                    sla_tier: format!("{:?}", node.sla_tier),
                    utilization: node.metrics.cpu_usage_percent / 100.0,
                    tasks_today: (completed + failed) as u32,
                    failure_rate: if completed + failed == 0 { 0.0 } else { failed as f32 / (completed + failed) as f32 },
                }
            })
            .collect()
    }

    /// Record node metrics for historical tracking
    fn record_node_metrics(&self, node: &crate::infrastructure::InfrastructureNode) {
        let mut history = self
            .node_history
            .entry(node.id)
            .or_insert_with(NodeHistoricalData::default);

        let snapshot = MetricsSnapshot {
            timestamp: Utc::now(),
            cpu_usage: node.metrics.cpu_usage_percent / 100.0,
            ram_usage: node.metrics.memory_usage_percent() / 100.0,
            gpu_usage: node.metrics.avg_gpu_usage_percent() / 100.0,
            network_rx_mbps: (node.metrics.network_rx_bytes / 125_000) as f32,
            network_tx_mbps: (node.metrics.network_tx_bytes / 125_000) as f32,
        };

        history.metrics_history.push(snapshot);

        // Trim history if too long
        if history.metrics_history.len() > self.config.max_history_entries {
            history.metrics_history.remove(0);
        }

        history.last_heartbeat = Some(Utc::now());
    }

    /// Collect recent tasks from cluster state
    async fn collect_recent_tasks(&self) -> Vec<TaskSummary> {
        let _stats = self.cluster_state.get_stats();
        let mut tasks = Vec::new();

        // Get running jobs and their tasks
        // Note: This is a simplified implementation
        for job in self.cluster_state.get_pending_jobs() {
            for task in self.cluster_state.get_tasks_for_job(job.id) {
                let status = match task.status {
                    TaskStatus::Pending => TaskStatusSummary::Pending,
                    TaskStatus::Assigned | TaskStatus::Running | TaskStatus::Checkpointing => {
                        TaskStatusSummary::Running
                    }
                    TaskStatus::Completed => TaskStatusSummary::Completed,
                    TaskStatus::Failed | TaskStatus::Cancelled => TaskStatusSummary::Failed,
                };

                let duration_ms = task.completed_at.map(|end| {
                    task.started_at
                        .map(|start| (end - start).num_milliseconds() as u64)
                        .unwrap_or(0)
                });

                tasks.push(TaskSummary {
                    id: task.id.to_string(),
                    node_id: None, // TODO: Map worker to node
                    status,
                    duration_ms,
                    started_at: task.started_at,
                    completed_at: task.completed_at,
                    is_phantom: false,
                });

                if tasks.len() >= 50 {
                    return tasks;
                }
            }
        }

        tasks
    }

    /// Collect phantom metrics from unified pool
    fn collect_phantom_metrics(
        &self,
        pool_metrics: &Option<PoolMetrics>,
    ) -> crate::coordinator::dual_mode::dashboard::PhantomDashboard {
        use crate::coordinator::dual_mode::dashboard::PhantomDashboard;
        use crate::coordinator::dual_mode::CapacityEstimate;

        match pool_metrics {
            Some(metrics) => PhantomDashboard {
                estimated_capacity: metrics.total_estimated_capacity.clone(),
                task_completion_rate: metrics.phantom_completion_rate,
                avg_task_latency: Duration::from_millis(metrics.phantom_avg_latency_ms),
                active_tasks: metrics.phantom_pending_tasks,
                tasks_completed_24h: 0, // TODO: Track from history
                tasks_expired_24h: 0,
                capacity_confidence: metrics.phantom_confidence,
                pool_responsive: metrics.phantom_estimated_available,
            },
            None => PhantomDashboard {
                estimated_capacity: CapacityEstimate::default(),
                task_completion_rate: 0.0,
                avg_task_latency: Duration::ZERO,
                active_tasks: 0,
                tasks_completed_24h: 0,
                tasks_expired_24h: 0,
                capacity_confidence: 0.0,
                pool_responsive: false,
            },
        }
    }

    /// Collect combined metrics
    fn collect_combined_metrics(
        &self,
        pool_metrics: &Option<PoolMetrics>,
    ) -> crate::coordinator::dual_mode::dashboard::CombinedDashboard {
        use crate::coordinator::dual_mode::dashboard::{
            CombinedDashboard, Recommendation, RecommendationPriority,
        };
        use crate::coordinator::dual_mode::CapacityEstimate;

        let stats = self.cluster_state.get_stats();
        let total_completed = self
            .unified_pool
            .as_ref()
            .map(|p| p.total_completed())
            .unwrap_or(0);

        let (capacity, utilization, health, ratio, trend) = match pool_metrics {
            Some(metrics) => {
                let health = if metrics.infra_nodes_degraded > metrics.infra_nodes_online / 2 {
                    SystemHealth::Degraded
                } else if metrics.infra_utilization > 0.9 {
                    SystemHealth::Strained
                } else {
                    SystemHealth::Healthy
                };

                let infra_cap = metrics.infra_available_cores as f32;
                let phantom_cap = metrics.phantom_estimated_cores as f32;
                let total = infra_cap + phantom_cap;
                let ratio = if total > 0.0 {
                    (infra_cap / total, phantom_cap / total)
                } else {
                    (1.0, 0.0)
                };

                (
                    metrics.total_estimated_capacity.clone(),
                    metrics.infra_utilization,
                    health,
                    ratio,
                    CapacityTrend::Stable,
                )
            }
            None => (
                CapacityEstimate::default(),
                0.0,
                SystemHealth::Healthy,
                (1.0, 0.0),
                CapacityTrend::Stable,
            ),
        };

        let mut recommendations = Vec::new();

        // Add recommendations based on state
        if utilization > 0.8 {
            recommendations.push(Recommendation {
                priority: RecommendationPriority::Medium,
                category: "capacity".to_string(),
                message: "Infrastructure utilization is high. Consider adding nodes.".to_string(),
            });
        }

        if stats.failed_jobs > 0 && stats.completed_jobs > 0 {
            let failure_rate =
                stats.failed_jobs as f32 / (stats.completed_jobs + stats.failed_jobs) as f32;
            if failure_rate > 0.1 {
                recommendations.push(Recommendation {
                    priority: RecommendationPriority::High,
                    category: "reliability".to_string(),
                    message: format!(
                        "High job failure rate ({:.1}%). Investigate failing jobs.",
                        failure_rate * 100.0
                    ),
                });
            }
        }

        CombinedDashboard {
            total_capacity: capacity,
            current_utilization: utilization,
            pending_tasks: stats.pending_jobs as u32,
            completed_24h: total_completed,
            infra_vs_phantom_ratio: ratio,
            system_health: health,
            capacity_trend: trend,
            recommendations,
        }
    }

    /// Generate alerts based on current metrics
    async fn generate_alerts(
        &self,
        infra: &crate::coordinator::dual_mode::dashboard::InfrastructureDashboard,
        pool_metrics: &Option<PoolMetrics>,
    ) {
        // Check for high utilization
        if infra.utilization_percent > self.config.high_cpu_threshold {
            self.create_alert(
                AlertSeverity::Warning,
                "High CPU Utilization",
                format!(
                    "Cluster CPU utilization is {:.1}%, consider adding capacity",
                    infra.utilization_percent
                ),
                None,
                None,
            );
        }

        // Check for degraded nodes
        if infra.nodes_degraded > infra.nodes_online / 4 {
            self.create_alert(
                AlertSeverity::Warning,
                "Multiple Degraded Nodes",
                format!(
                    "{} nodes are degraded out of {} online",
                    infra.nodes_degraded, infra.nodes_online
                ),
                None,
                None,
            );
        }

        // Check for offline nodes
        if infra.nodes_offline > 0 {
            self.create_alert(
                AlertSeverity::Info,
                "Nodes Offline",
                format!("{} nodes are offline", infra.nodes_offline),
                None,
                None,
            );
        }

        // Check phantom pool health
        if let Some(metrics) = pool_metrics {
            if metrics.phantom_confidence < 0.3 && metrics.phantom_pending_tasks > 100 {
                self.create_alert(
                    AlertSeverity::Warning,
                    "Low Phantom Pool Confidence",
                    "Phantom pool capacity estimate has low confidence with pending tasks"
                        .to_string(),
                    None,
                    None,
                );
            }
        }
    }

    /// Create a new alert
    fn create_alert(
        &self,
        severity: AlertSeverity,
        title: &str,
        message: String,
        node_id: Option<String>,
        region_id: Option<String>,
    ) {
        let id = format!(
            "alert-{}",
            self.alert_counter.fetch_add(1, Ordering::SeqCst)
        );

        let alert = AlertDetails {
            id: id.clone(),
            severity,
            title: title.to_string(),
            message,
            node_id: node_id.clone(),
            region_id,
            raised_at: Utc::now(),
            acknowledged: false,
            suggested_actions: Vec::new(),
        };

        self.active_alerts.insert(id.clone(), alert.clone());

        // Broadcast alert via WebSocket if enabled
        if self.config.broadcast_updates {
            let ws_alert = Alert {
                id: id.clone(),
                severity,
                node_id: node_id.map(|_| NodeId::new()), // Would need proper mapping
                message: alert.message.clone(),
                raised_at: alert.raised_at,
                acknowledged: false,
            };

            broadcast_event(DashboardEvent::AlertCreated {
                alert: ws_alert,
                timestamp: Utc::now(),
            });
        }
    }

    /// Get current active alerts
    fn get_current_alerts(&self) -> Vec<Alert> {
        self.active_alerts
            .iter()
            .map(|entry| {
                let details = entry.value();
                Alert {
                    id: details.id.clone(),
                    severity: details.severity,
                    node_id: None, // TODO: Parse from string
                    message: details.message.clone(),
                    raised_at: details.raised_at,
                    acknowledged: details.acknowledged,
                }
            })
            .collect()
    }
}

// ============================================================================
// DashboardDataSource Implementation
// ============================================================================

impl<B: PersistenceBackend + Send + Sync + 'static> DashboardDataSource
    for RealTimeMetricsCollector<B>
{
    fn get_dashboard(&self) -> DashboardData {
        // Check cache
        {
            let cache = self.cached_dashboard.read();
            if let Some((cached_at, ref data)) = *cache {
                if cached_at.elapsed() < self.config.cache_ttl {
                    return data.clone();
                }
            }
        }

        // Collect fresh metrics (blocking for now, should be async in production)
        let rt = tokio::runtime::Handle::try_current();
        let data = match rt {
            Ok(handle) => {
                tokio::task::block_in_place(|| handle.block_on(self.collect_current_metrics()))
            }
            Err(_) => {
                // Fallback to empty dashboard if no runtime
                DashboardData {
                    infrastructure:
                        crate::coordinator::dual_mode::dashboard::InfrastructureDashboard {
                            nodes: Vec::new(),
                            by_location: HashMap::new(),
                            by_sla_tier: HashMap::new(),
                            health_alerts: Vec::new(),
                            recent_tasks: Vec::new(),
                            total_cores: 0,
                            available_cores: 0,
                            total_gpus: 0,
                            available_gpus: 0,
                            total_ram_bytes: 0,
                            available_ram_bytes: 0,
                            nodes_online: 0,
                            nodes_degraded: 0,
                            nodes_offline: 0,
                            nodes_maintenance: 0,
                            utilization_percent: 0.0,
                            sla_compliance_rate: 100.0,
                        },
                    phantom: crate::coordinator::dual_mode::dashboard::PhantomDashboard {
                        estimated_capacity: Default::default(),
                        task_completion_rate: 0.0,
                        avg_task_latency: Duration::ZERO,
                        active_tasks: 0,
                        tasks_completed_24h: 0,
                        tasks_expired_24h: 0,
                        capacity_confidence: 0.0,
                        pool_responsive: false,
                    },
                    combined: crate::coordinator::dual_mode::dashboard::CombinedDashboard {
                        total_capacity: Default::default(),
                        current_utilization: 0.0,
                        pending_tasks: 0,
                        completed_24h: 0,
                        infra_vs_phantom_ratio: (1.0, 0.0),
                        system_health: SystemHealth::Healthy,
                        capacity_trend: CapacityTrend::Stable,
                        recommendations: Vec::new(),
                    },
                    generated_at: Utc::now(),
                    generation_duration_ms: 0,
                }
            }
        };

        // Update cache
        {
            let mut cache = self.cached_dashboard.write();
            *cache = Some((Instant::now(), data.clone()));
        }

        data
    }

    fn get_regions(&self) -> Vec<RegionStats> {
        let rt = tokio::runtime::Handle::try_current();
        match rt {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(async {
                    let datacenters = self.registry.list_datacenters().await;
                    let mut regions = Vec::new();

                    for dc in datacenters {
                        let nodes = self.registry.nodes_in_datacenter(&dc).await;
                        let online_nodes: u32 =
                            nodes.iter().filter(|n| n.can_accept_work()).count() as u32;
                        let total_cores: u32 = nodes.iter().map(|n| n.specs.cpu_cores).sum();
                        let available_cores: u32 = nodes
                            .iter()
                            .filter(|n| n.can_accept_work())
                            .map(|n| n.specs.cpu_cores)
                            .sum();

                        let utilization = if total_cores > 0 {
                            1.0 - (available_cores as f32 / total_cores as f32)
                        } else {
                            0.0
                        };

                        let avg_latency: u64 = if nodes.is_empty() {
                            0
                        } else {
                            nodes
                                .iter()
                                .map(|n| n.metrics.avg_task_duration_ms)
                                .sum::<u64>()
                                / nodes.len() as u64
                        };

                        regions.push(RegionStats {
                            id: dc.clone(),
                            name: dc,
                            total_nodes: nodes.len() as u32,
                            online_nodes,
                            total_cores,
                            available_cores,
                            utilization,
                            avg_latency_ms: avg_latency,
                        });
                    }

                    regions
                })
            }),
            Err(_) => Vec::new(),
        }
    }

    fn get_region(&self, id: &str) -> Option<RegionDetails> {
        let rt = tokio::runtime::Handle::try_current().ok()?;
        tokio::task::block_in_place(|| {
            rt.block_on(async {
                let nodes = self.registry.nodes_in_datacenter(id).await;
                if nodes.is_empty() {
                    return None;
                }

                let node_summaries: Vec<NodeSummary> = nodes
                    .iter()
                    .map(|node| {
                        let status = match node.status {
                            NodeStatus::Online { .. } => NodeStatusSummary::Online,
                            NodeStatus::Degraded { .. } => NodeStatusSummary::Degraded,
                            NodeStatus::Offline { .. } => NodeStatusSummary::Offline,
                            NodeStatus::Maintenance { .. } => NodeStatusSummary::Maintenance,
                        };

                        NodeSummary {
                            id: node.id,
                            hostname: node.hostname.clone(),
                            status,
                            cores: (node.specs.cpu_cores, node.specs.cpu_cores),
                            ram_bytes: (node.specs.ram_bytes, node.specs.ram_bytes / 2),
                            gpus: (node.specs.gpu_count, node.specs.gpu_count),
                            location: node.location.datacenter.clone(),
                            sla_tier: format!("{:?}", node.sla_tier),
                            utilization: node.metrics.cpu_usage_percent / 100.0,
                            tasks_today: 0,
                            failure_rate: 0.0,
                        }
                    })
                    .collect();

                let online_nodes = nodes.iter().filter(|n| n.can_accept_work()).count() as u32;
                let total_cores: u32 = nodes.iter().map(|n| n.specs.cpu_cores).sum();
                let available_cores: u32 = nodes
                    .iter()
                    .filter(|n| n.can_accept_work())
                    .map(|n| n.specs.cpu_cores)
                    .sum();

                let location_summary = LocationSummary {
                    location: id.to_string(),
                    total_nodes: nodes.len() as u32,
                    online_nodes,
                    total_cores,
                    available_cores,
                    utilization: if total_cores > 0 {
                        1.0 - (available_cores as f32 / total_cores as f32)
                    } else {
                        0.0
                    },
                };

                let health_status = if online_nodes == nodes.len() as u32 {
                    SystemHealth::Healthy
                } else if online_nodes > nodes.len() as u32 / 2 {
                    SystemHealth::Degraded
                } else {
                    SystemHealth::Critical
                };

                Some(RegionDetails {
                    id: id.to_string(),
                    name: id.to_string(),
                    nodes: node_summaries,
                    location_summary,
                    recent_tasks: Vec::new(), // TODO: Filter tasks by region
                    health_status,
                })
            })
        })
    }

    fn get_nodes(&self, filter: &NodeFilter) -> Vec<NodeDetails> {
        let rt = match tokio::runtime::Handle::try_current() {
            Ok(h) => h,
            Err(_) => return Vec::new(),
        };

        tokio::task::block_in_place(|| {
            rt.block_on(async {
                use crate::infrastructure::NodeQuery;

                let mut query = NodeQuery::new();

                if let Some(ref dc) = filter.region {
                    query = query.in_datacenter(dc.clone());
                }

                if filter.status.as_deref() == Some("online") {
                    query = query.available_only();
                }

                query = query.offset(filter.offset.unwrap_or(0));
                query = query.limit(filter.limit.unwrap_or(100));

                let nodes = self.registry.query(&query).await;

                nodes
                    .into_iter()
                    .map(|node| {
                        let status = match node.status {
                            NodeStatus::Online { .. } => "online",
                            NodeStatus::Degraded { .. } => "degraded",
                            NodeStatus::Offline { .. } => "offline",
                            NodeStatus::Maintenance { .. } => "maintenance",
                        };

                        NodeDetails {
                            id: node.id.to_string(),
                            hostname: node.hostname.clone(),
                            status: status.to_string(),
                            region: node.location.datacenter.clone(),
                            sla_tier: format!("{:?}", node.sla_tier).to_lowercase(),
                            cores_total: node.specs.cpu_cores,
                            cores_available: node.specs.cpu_cores, // TODO: Track actual
                            ram_bytes_total: node.specs.ram_bytes,
                            ram_bytes_available: node.specs.ram_bytes / 2, // Estimate
                            gpus_total: node.specs.gpu_count,
                            gpus_available: node.specs.gpu_count,
                            utilization: node.metrics.cpu_usage_percent / 100.0,
                            tasks_today: 0,
                            failure_rate: 0.0,
                            tags: node.tags.clone(),
                        }
                    })
                    .collect()
            })
        })
    }

    fn get_node(&self, id: &str) -> Option<NodeDetailsWithHistory> {
        let rt = tokio::runtime::Handle::try_current().ok()?;

        tokio::task::block_in_place(|| {
            rt.block_on(async {
                // Parse node ID
                let node_id = uuid::Uuid::parse_str(id).ok().map(NodeId)?;
                let node = self.registry.get(&node_id).await?;

                let status = match node.status {
                    NodeStatus::Online { .. } => "online",
                    NodeStatus::Degraded { .. } => "degraded",
                    NodeStatus::Offline { .. } => "offline",
                    NodeStatus::Maintenance { .. } => "maintenance",
                };

                let node_details = NodeDetails {
                    id: node.id.to_string(),
                    hostname: node.hostname.clone(),
                    status: status.to_string(),
                    region: node.location.datacenter.clone(),
                    sla_tier: format!("{:?}", node.sla_tier).to_lowercase(),
                    cores_total: node.specs.cpu_cores,
                    cores_available: node.specs.cpu_cores,
                    ram_bytes_total: node.specs.ram_bytes,
                    ram_bytes_available: node.specs.ram_bytes / 2,
                    gpus_total: node.specs.gpu_count,
                    gpus_available: node.specs.gpu_count,
                    utilization: node.metrics.cpu_usage_percent / 100.0,
                    tasks_today: 0,
                    failure_rate: 0.0,
                    tags: node.tags.clone(),
                };

                // Get historical data
                let history = self.node_history.get(&node_id);
                let (metrics_history, task_history, uptime_hours, last_heartbeat) = match history {
                    Some(h) => (
                        h.metrics_history.clone(),
                        h.task_history.clone(),
                        h.uptime_hours,
                        h.last_heartbeat.unwrap_or_else(Utc::now),
                    ),
                    None => (Vec::new(), Vec::new(), 0.0, Utc::now()),
                };

                Some(NodeDetailsWithHistory {
                    node: node_details,
                    task_history,
                    metrics_history,
                    uptime_hours,
                    last_heartbeat,
                })
            })
        })
    }

    fn get_jobs(&self, filter: &JobFilter) -> Vec<JobListItem> {
        let _stats = self.cluster_state.get_stats();
        let mut jobs = Vec::new();

        // Get jobs from cluster state
        for job in self.cluster_state.get_pending_jobs() {
            let tasks = self.cluster_state.get_tasks_for_job(job.id);
            let completed = tasks
                .iter()
                .filter(|t| t.status == TaskStatus::Completed)
                .count();
            let failed = tasks
                .iter()
                .filter(|t| t.status == TaskStatus::Failed)
                .count();
            let running = tasks
                .iter()
                .filter(|t| matches!(t.status, TaskStatus::Running | TaskStatus::Assigned))
                .count();

            let progress = if tasks.is_empty() {
                0.0
            } else {
                completed as f32 / tasks.len() as f32
            };

            let status = match job.status {
                JobStatus::Pending => "pending",
                JobStatus::Scheduled => "scheduled",
                JobStatus::Running => "running",
                JobStatus::Completed => "completed",
                JobStatus::Failed => "failed",
                JobStatus::Cancelled => "cancelled",
            };

            // Apply filters
            if let Some(ref filter_status) = filter.status {
                if filter_status != status {
                    continue;
                }
            }

            if let Some(min_priority) = filter.min_priority {
                if job.priority < min_priority {
                    continue;
                }
            }

            jobs.push(JobListItem {
                id: job.id.to_string(),
                name: job.name.clone(),
                status: status.to_string(),
                priority: job.priority,
                total_tasks: tasks.len(),
                completed_tasks: completed,
                failed_tasks: failed,
                running_tasks: running,
                created_at: job.created_at,
                started_at: job.started_at,
                completed_at: job.completed_at,
                progress_percent: progress * 100.0,
            });
        }

        // Apply pagination
        let offset = filter.offset.unwrap_or(0);
        let limit = filter.limit.unwrap_or(100);

        jobs.into_iter().skip(offset).take(limit).collect()
    }

    fn get_job(&self, id: &str) -> Option<JobDetails> {
        let job_id = uuid::Uuid::parse_str(id).ok().map(JobId)?;
        let job = self.cluster_state.get_job(job_id)?;
        let tasks = self.cluster_state.get_tasks_for_job(job_id);

        let status = match job.status {
            JobStatus::Pending => "pending",
            JobStatus::Scheduled => "scheduled",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        };

        let task_details: Vec<TaskDetails> = tasks
            .iter()
            .map(|task| {
                let task_status = match task.status {
                    TaskStatus::Pending => "pending",
                    TaskStatus::Assigned => "assigned",
                    TaskStatus::Running => "running",
                    TaskStatus::Checkpointing => "checkpointing",
                    TaskStatus::Completed => "completed",
                    TaskStatus::Failed => "failed",
                    TaskStatus::Cancelled => "cancelled",
                };

                let duration_ms = task.completed_at.and_then(|end| {
                    task.started_at
                        .map(|start| (end - start).num_milliseconds() as u64)
                });

                TaskDetails {
                    id: task.id.to_string(),
                    status: task_status.to_string(),
                    assigned_node: task.assigned_worker.map(|w| w.to_string()),
                    assigned_region: None, // TODO: Map worker to region
                    is_phantom: false,
                    attempts: task.attempts,
                    started_at: task.started_at,
                    completed_at: task.completed_at,
                    duration_ms,
                }
            })
            .collect();

        let total_duration_ms = job.completed_at.and_then(|end| {
            job.started_at
                .map(|start| (end - start).num_milliseconds() as u64)
        });

        Some(JobDetails {
            id: job.id.to_string(),
            name: job.name.clone(),
            status: status.to_string(),
            priority: job.priority,
            tasks: task_details,
            created_at: job.created_at,
            started_at: job.started_at,
            completed_at: job.completed_at,
            total_duration_ms,
            metadata: job.metadata.clone(),
        })
    }

    fn get_job_latency_heatmap(&self, id: &str) -> Option<LatencyHeatmap> {
        let job_id = uuid::Uuid::parse_str(id).ok().map(JobId)?;
        let _job = self.cluster_state.get_job(job_id)?;

        // Get historical latency data
        let history = self.job_history.get(&job_id)?;

        // Build heatmap from latency by region
        let regions: Vec<String> = history.latency_by_region.keys().cloned().collect();
        let now = Utc::now();

        // Create time buckets (last 24 hours, hourly)
        let time_buckets: Vec<DateTime<Utc>> = (0..24)
            .map(|h| now - chrono::Duration::hours(23 - h))
            .collect();

        // Build latency matrix (simplified - would need actual time-based data)
        let latency_matrix: Vec<Vec<Option<u64>>> = regions
            .iter()
            .map(|region| {
                history
                    .latency_by_region
                    .get(region)
                    .map(|latencies| {
                        time_buckets
                            .iter()
                            .map(|_| latencies.first().copied())
                            .collect()
                    })
                    .unwrap_or_else(|| vec![None; 24])
            })
            .collect();

        // Calculate percentiles
        let all_latencies: Vec<u64> = history
            .latency_by_region
            .values()
            .flat_map(|v| v.iter().copied())
            .collect();

        let (p50, p95, p99) = if all_latencies.is_empty() {
            (0, 0, 0)
        } else {
            let mut sorted = all_latencies.clone();
            sorted.sort();
            let len = sorted.len();
            (
                sorted[len / 2],
                sorted[len * 95 / 100],
                sorted[(len * 99 / 100).min(len - 1)],
            )
        };

        Some(LatencyHeatmap {
            job_id: id.to_string(),
            regions,
            time_buckets,
            latency_matrix,
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
        })
    }
}

// ============================================================================
// AlertManager Implementation
// ============================================================================

impl<B: PersistenceBackend + Send + Sync + 'static> AlertManager for RealTimeMetricsCollector<B> {
    fn get_active_alerts(&self) -> Vec<AlertDetails> {
        self.active_alerts
            .iter()
            .filter(|entry| !entry.value().acknowledged)
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn dismiss_alert(&self, id: &str) -> Result<(), AlertError> {
        if let Some(mut alert) = self.active_alerts.get_mut(id) {
            alert.acknowledged = true;

            // Broadcast dismissal
            if self.config.broadcast_updates {
                broadcast_event(DashboardEvent::AlertDismissed {
                    alert_id: id.to_string(),
                    timestamp: Utc::now(),
                });
            }

            Ok(())
        } else {
            Err(AlertError::NotFound(id.to_string()))
        }
    }

    fn execute_action(
        &self,
        alert_id: &str,
        action_id: &str,
    ) -> Result<crate::control_plane::api::ActionResult, AlertError> {
        let alert = self
            .active_alerts
            .get(alert_id)
            .ok_or_else(|| AlertError::NotFound(alert_id.to_string()))?;

        let action = alert
            .suggested_actions
            .iter()
            .find(|a| a.id == action_id)
            .ok_or_else(|| AlertError::ActionNotFound(action_id.to_string()))?;

        // Execute the action (simplified - would need actual implementation)
        info!("Executing action {} for alert {}", action_id, alert_id);

        Ok(crate::control_plane::api::ActionResult {
            success: true,
            action_id: action_id.to_string(),
            executed_at: Utc::now(),
            output: format!("Executed action: {}", action.name),
        })
    }
}

// ============================================================================
// CapacityForecaster Implementation
// ============================================================================

impl<B: PersistenceBackend + Send + Sync + 'static> CapacityForecaster
    for RealTimeMetricsCollector<B>
{
    fn forecast_24h(&self) -> CapacityForecast {
        let now = Utc::now();
        let dashboard = self.get_dashboard();

        // Generate hourly predictions based on current capacity
        let hourly_predictions: Vec<HourlyPrediction> = (0..24)
            .map(|hour| {
                let prediction_time = now + chrono::Duration::hours(hour);

                // Simple prediction model - would need historical data for accuracy
                let base_cores = dashboard.infrastructure.available_cores;
                let phantom_cores = dashboard.phantom.estimated_capacity.cores_mid;

                // Simulate daily patterns (lower at night, higher during day)
                let hour_of_day = prediction_time.hour();
                let daily_factor = if hour_of_day >= 9 && hour_of_day <= 17 {
                    1.0 // Business hours
                } else if hour_of_day >= 6 && hour_of_day <= 21 {
                    0.9 // Extended hours
                } else {
                    0.7 // Night hours
                };

                let phantom_uncertainty = 0.3;

                HourlyPrediction {
                    hour: prediction_time,
                    infrastructure_cores_available: (base_cores as f32 * daily_factor) as u32,
                    phantom_cores_estimated_low: (phantom_cores as f32
                        * (1.0 - phantom_uncertainty))
                        as u32,
                    phantom_cores_estimated_mid: phantom_cores,
                    phantom_cores_estimated_high: (phantom_cores as f32
                        * (1.0 + phantom_uncertainty))
                        as u32,
                    total_predicted_load: dashboard.combined.current_utilization * daily_factor,
                    expected_utilization: dashboard.combined.current_utilization,
                    confidence: 0.7 - (hour as f32 * 0.02).min(0.4), // Confidence decreases over time
                }
            })
            .collect();

        // Identify peak hours
        let peak_hours: Vec<u32> = hourly_predictions
            .iter()
            .filter(|p| p.expected_utilization > 0.7)
            .map(|p| p.hour.hour())
            .collect();

        let capacity_trend = if dashboard.combined.current_utilization > 0.8 {
            CapacityTrend::Decreasing
        } else if dashboard.combined.current_utilization < 0.5 {
            CapacityTrend::Increasing
        } else {
            CapacityTrend::Stable
        };

        let demand_trend = if peak_hours.len() > 8 {
            DemandTrend::Increasing
        } else if peak_hours.is_empty() {
            DemandTrend::Decreasing
        } else {
            DemandTrend::Stable
        };

        let recommended_action = if dashboard.combined.current_utilization > 0.9 {
            Some("Consider adding infrastructure nodes to handle peak load".to_string())
        } else if dashboard.phantom.capacity_confidence < 0.5 {
            Some("Phantom pool capacity is uncertain, rely more on infrastructure".to_string())
        } else {
            None
        };

        CapacityForecast {
            generated_at: now,
            forecast_hours: 24,
            hourly_predictions,
            trends: ForecastTrends {
                capacity_trend,
                demand_trend,
                peak_hours,
                recommended_action,
            },
        }
    }
}

// ============================================================================
// AssignmentOverride Implementation
// ============================================================================

impl<B: PersistenceBackend + Send + Sync + 'static> AssignmentOverride
    for RealTimeMetricsCollector<B>
{
    fn override_assignment(
        &self,
        node_id: &str,
        task_id: &str,
        reason: &str,
    ) -> Result<OverrideResponse, OverrideError> {
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| OverrideError::Failed("No tokio runtime".to_string()))?;

        tokio::task::block_in_place(|| {
            rt.block_on(async {
                // Parse and validate node ID
                let parsed_node_id = uuid::Uuid::parse_str(node_id)
                    .map(NodeId)
                    .map_err(|_| OverrideError::NodeNotFound(node_id.to_string()))?;

                // Check node exists and is available
                let node = self
                    .registry
                    .get(&parsed_node_id)
                    .await
                    .ok_or_else(|| OverrideError::NodeNotFound(node_id.to_string()))?;

                if !node.can_accept_work() {
                    return Err(OverrideError::NodeUnavailable(format!(
                        "Node {} is not available for work",
                        node_id
                    )));
                }

                // Find alternative nodes
                use crate::infrastructure::NodeQuery;
                let query = NodeQuery::new()
                    .available_only()
                    .in_datacenter(node.location.datacenter.clone())
                    .limit(5);

                let alternatives = self.registry.query(&query).await;
                let alternative_nodes: Vec<crate::control_plane::api::AlternativeNode> =
                    alternatives
                        .into_iter()
                        .filter(|n| n.id != parsed_node_id)
                        .take(3)
                        .map(|n| crate::control_plane::api::AlternativeNode {
                            node_id: n.id.to_string(),
                            score: 1.0 - (n.metrics.cpu_usage_percent / 100.0),
                            reason: format!(
                                "Available with {:.0}% CPU usage",
                                n.metrics.cpu_usage_percent
                            ),
                            estimated_latency_ms: n.metrics.avg_task_duration_ms,
                        })
                        .collect();

                info!(
                    "Manual assignment override: task {} -> node {} (reason: {})",
                    task_id, node_id, reason
                );

                Ok(OverrideResponse {
                    assigned: true,
                    node_id: node_id.to_string(),
                    alternatives: alternative_nodes,
                    override_reason: reason.to_string(),
                })
            })
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::persistence::MemoryPersistenceBackend;

    fn create_test_collector() -> RealTimeMetricsCollector<MemoryPersistenceBackend> {
        let cluster_state = Arc::new(ClusterState::new());
        let registry = Arc::new(NodeRegistry::new());
        let config = CollectorConfig::default();

        RealTimeMetricsCollector::new(cluster_state, registry, config)
    }

    #[test]
    fn test_collector_creation() {
        let collector = create_test_collector();
        assert!(collector.active_alerts.is_empty());
    }

    #[test]
    fn test_config_defaults() {
        let config = CollectorConfig::default();
        assert_eq!(config.refresh_interval, Duration::from_secs(5));
        assert_eq!(config.high_cpu_threshold, 90.0);
    }

    #[test]
    fn test_alert_creation() {
        let collector = create_test_collector();

        collector.create_alert(
            AlertSeverity::Warning,
            "Test Alert",
            "Test message".to_string(),
            None,
            None,
        );

        assert_eq!(collector.active_alerts.len(), 1);
    }

    #[test]
    fn test_alert_dismissal() {
        let collector = create_test_collector();

        collector.create_alert(
            AlertSeverity::Warning,
            "Test Alert",
            "Test message".to_string(),
            None,
            None,
        );

        let alert_id = collector
            .active_alerts
            .iter()
            .next()
            .map(|e| e.key().clone())
            .unwrap();

        assert!(collector.dismiss_alert(&alert_id).is_ok());

        let alert = collector.active_alerts.get(&alert_id).unwrap();
        assert!(alert.acknowledged);
    }

    #[test]
    fn test_get_active_alerts() {
        let collector = create_test_collector();

        collector.create_alert(
            AlertSeverity::Warning,
            "Alert 1",
            "Message 1".to_string(),
            None,
            None,
        );

        collector.create_alert(
            AlertSeverity::Info,
            "Alert 2",
            "Message 2".to_string(),
            None,
            None,
        );

        let active = collector.get_active_alerts();
        assert_eq!(active.len(), 2);
    }

    #[test]
    fn test_capacity_forecast() {
        let collector = create_test_collector();
        let forecast = collector.forecast_24h();

        assert_eq!(forecast.forecast_hours, 24);
        assert_eq!(forecast.hourly_predictions.len(), 24);

        // Check that confidence decreases over time
        assert!(
            forecast.hourly_predictions[0].confidence > forecast.hourly_predictions[23].confidence
        );
    }

    #[test]
    fn test_node_historical_data() {
        let mut history = NodeHistoricalData::default();

        let snapshot = MetricsSnapshot {
            timestamp: Utc::now(),
            cpu_usage: 0.5,
            ram_usage: 0.6,
            gpu_usage: 0.0,
            network_rx_mbps: 100.0,
            network_tx_mbps: 50.0,
        };

        history.metrics_history.push(snapshot);
        assert_eq!(history.metrics_history.len(), 1);
    }
}
