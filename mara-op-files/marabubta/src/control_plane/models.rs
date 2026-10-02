// Marabunta - Licensed under the MIT License.
//! Control Plane Data Models
//!
//! Comprehensive data models for the Marabunta Compute control plane, providing
//! monitoring, metrics, and observability across the distributed system.

use crate::common::{JobId, RegionId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Unique identifier for a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub uuid::Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node-{}", &self.0.to_string()[..8])
    }
}

/// Per-node statistics and metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Unique identifier for this node
    pub node_id: NodeId,

    /// Region this node belongs to
    pub region: RegionId,

    /// Resource pool assignment (e.g., "default", "high-priority", "gpu")
    pub pool: String,

    /// Custom tags for filtering and organization
    pub tags: HashMap<String, String>,

    /// CPU compute capacity in FLOPS (floating point operations per second)
    pub cpu_flops: f64,

    /// Total memory in megabytes
    pub memory_mb: u64,

    /// Total disk space in megabytes
    pub disk_mb: u64,

    /// Current load (0.0 to 1.0, where 1.0 is fully loaded)
    pub current_load: f64,

    /// CPU temperature in Celsius (if available)
    pub temperature: Option<f32>,

    /// Network latency to coordinator in milliseconds
    pub latency_ms: f64,

    /// Timestamp of last heartbeat
    pub last_heartbeat: DateTime<Utc>,

    /// Node uptime in seconds
    pub uptime_secs: u64,

    /// Number of work units successfully completed
    pub work_units_completed: u64,

    /// Number of work units that failed
    pub work_units_failed: u64,

    /// Battery level (0.0 to 1.0) for mobile devices
    pub battery_level: Option<f32>,

    /// Reason for throttling (if any)
    pub throttling_reason: Option<ThrottlingReason>,
}

impl NodeMetrics {
    /// Create new node metrics with default values
    pub fn new(node_id: NodeId, region: RegionId, pool: String) -> Self {
        Self {
            node_id,
            region,
            pool,
            tags: HashMap::new(),
            cpu_flops: 0.0,
            memory_mb: 0,
            disk_mb: 0,
            current_load: 0.0,
            temperature: None,
            latency_ms: 0.0,
            last_heartbeat: Utc::now(),
            uptime_secs: 0,
            work_units_completed: 0,
            work_units_failed: 0,
            battery_level: None,
            throttling_reason: None,
        }
    }

    /// Check if node is currently healthy
    pub fn is_healthy(&self) -> bool {
        let heartbeat_age = Utc::now().signed_duration_since(self.last_heartbeat);
        heartbeat_age.num_seconds() < 60 && self.throttling_reason.is_none()
    }

    /// Check if node is available for work
    pub fn is_available(&self) -> bool {
        self.is_healthy() && self.current_load < 0.9
    }
}

/// Reasons why a node might be throttled
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThrottlingReason {
    /// CPU temperature too high
    Overheating,

    /// Battery level too low
    LowBattery,

    /// Network latency too high
    HighLatency,

    /// Memory pressure
    MemoryPressure,

    /// Manual throttling by operator
    Manual,

    /// Cost optimization (e.g., metered network)
    CostOptimization,
}

/// Aggregated statistics for a region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStats {
    /// Unique identifier for this region
    pub region_id: RegionId,

    /// Human-readable region name
    pub name: String,

    /// Total compute capacity in FLOPS
    pub total_flops: f64,

    /// Available compute capacity in FLOPS
    pub available_flops: f64,

    /// Total number of nodes in region
    pub node_count: u32,

    /// Number of nodes actively processing work
    pub active_count: u32,

    /// Number of idle nodes (healthy but not working)
    pub idle_count: u32,

    /// Average network latency to coordinator in milliseconds
    pub avg_latency_ms: f64,

    /// 95th percentile latency in milliseconds
    pub p95_latency_ms: f64,

    /// 99th percentile latency in milliseconds
    pub p99_latency_ms: f64,

    /// Predicted capacity over the next 24 hours (hourly buckets)
    /// Each entry represents expected available FLOPS for that hour
    pub predicted_capacity_curve: Vec<f64>,
}

impl RegionStats {
    /// Create new region stats with default values
    pub fn new(region_id: RegionId, name: String) -> Self {
        Self {
            region_id,
            name,
            total_flops: 0.0,
            available_flops: 0.0,
            node_count: 0,
            active_count: 0,
            idle_count: 0,
            avg_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            predicted_capacity_curve: vec![0.0; 24],
        }
    }

    /// Get current utilization percentage (0.0 to 1.0)
    pub fn utilization(&self) -> f64 {
        if self.total_flops > 0.0 {
            1.0 - (self.available_flops / self.total_flops)
        } else {
            0.0
        }
    }

    /// Get percentage of nodes that are idle
    pub fn idle_percentage(&self) -> f64 {
        if self.node_count > 0 {
            self.idle_count as f64 / self.node_count as f64
        } else {
            0.0
        }
    }
}

/// Latency statistics for a specific context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    /// Minimum latency observed
    pub min_ms: f64,

    /// Maximum latency observed
    pub max_ms: f64,

    /// Average latency
    pub avg_ms: f64,

    /// Median latency (50th percentile)
    pub p50_ms: f64,

    /// 95th percentile latency
    pub p95_ms: f64,

    /// 99th percentile latency
    pub p99_ms: f64,

    /// Number of samples
    pub sample_count: u64,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            min_ms: 0.0,
            max_ms: 0.0,
            avg_ms: 0.0,
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            sample_count: 0,
        }
    }
}

/// Per-job metrics and statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetrics {
    /// Job identifier
    pub job_id: JobId,

    /// Human-readable job name
    pub name: String,

    /// Current job status
    pub status: JobStatus,

    /// Total number of work units in this job
    pub total_work_units: u64,

    /// Number of work units completed
    pub completed: u64,

    /// Number of work units that failed
    pub failed: u64,

    /// Number of work units currently being processed
    pub in_flight: u64,

    /// Work units completed per minute
    pub throughput_per_minute: f64,

    /// Latency statistics broken down by region
    pub latency_by_region: HashMap<RegionId, LatencyStats>,

    /// Estimated completion time
    pub estimated_completion: Option<DateTime<Utc>>,

    /// Identified bottlenecks affecting job performance
    pub bottlenecks: Vec<Bottleneck>,
}

impl JobMetrics {
    /// Create new job metrics
    pub fn new(job_id: JobId, name: String, total_work_units: u64) -> Self {
        Self {
            job_id,
            name,
            status: JobStatus::Pending,
            total_work_units,
            completed: 0,
            failed: 0,
            in_flight: 0,
            throughput_per_minute: 0.0,
            latency_by_region: HashMap::new(),
            estimated_completion: None,
            bottlenecks: Vec::new(),
        }
    }

    /// Get job completion percentage (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        if self.total_work_units > 0 {
            self.completed as f64 / self.total_work_units as f64
        } else {
            0.0
        }
    }

    /// Get failure rate (0.0 to 1.0)
    pub fn failure_rate(&self) -> f64 {
        let total_processed = self.completed + self.failed;
        if total_processed > 0 {
            self.failed as f64 / total_processed as f64
        } else {
            0.0
        }
    }
}

/// Job status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Job is queued but not yet started
    Pending,

    /// Job is currently running
    Running,

    /// Job completed successfully
    Completed,

    /// Job failed
    Failed,

    /// Job was cancelled by user
    Cancelled,

    /// Job is paused
    Paused,
}

/// Identified bottleneck affecting job performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bottleneck {
    /// Type of bottleneck
    pub bottleneck_type: BottleneckType,

    /// Severity score (0.0 to 1.0, where 1.0 is most severe)
    pub severity: f64,

    /// Human-readable description
    pub description: String,

    /// Affected region (if region-specific)
    pub affected_region: Option<RegionId>,

    /// When this bottleneck was first detected
    pub detected_at: DateTime<Utc>,
}

/// Types of bottlenecks that can affect job performance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BottleneckType {
    /// Insufficient compute capacity
    ComputeCapacity,

    /// High network latency
    NetworkLatency,

    /// Memory constraints
    Memory,

    /// Storage I/O limitations
    Storage,

    /// Task scheduling inefficiency
    Scheduling,

    /// Data transfer bottleneck
    DataTransfer,
}

/// System alert for operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Unique alert identifier
    pub alert_id: String,

    /// Alert severity level
    pub severity: AlertSeverity,

    /// Type of alert
    pub alert_type: AlertType,

    /// Human-readable alert message
    pub message: String,

    /// When the alert was triggered
    pub timestamp: DateTime<Utc>,

    /// Suggested actions to resolve the alert
    pub suggested_actions: Vec<SuggestedAction>,

    /// Whether the alert has been dismissed by an operator
    pub dismissed: bool,
}

impl Alert {
    /// Create a new alert
    pub fn new(severity: AlertSeverity, alert_type: AlertType, message: String) -> Self {
        Self {
            alert_id: uuid::Uuid::new_v4().to_string(),
            severity,
            alert_type,
            message,
            timestamp: Utc::now(),
            suggested_actions: Vec::new(),
            dismissed: false,
        }
    }

    /// Add a suggested action to this alert
    pub fn with_action(mut self, action: SuggestedAction) -> Self {
        self.suggested_actions.push(action);
        self
    }
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    /// Informational message
    Info,

    /// Warning that should be investigated
    Warning,

    /// Critical issue requiring immediate attention
    Critical,
}

/// Types of alerts that can be triggered
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertType {
    /// Multiple nodes in a cluster are going offline
    ClusterGoingOffline,

    /// Node is idle when work is available
    NodeIdle,

    /// Sudden spike in network latency
    LatencySpike,

    /// High failure rate for tasks
    HighFailureRate,

    /// Region capacity is critically low
    LowCapacity,

    /// Node is overheating
    Overheating,

    /// Job is taking longer than expected
    JobStalled,

    /// Coordinator failover occurred
    CoordinatorFailover,

    /// Storage capacity warning
    StorageFull,

    /// Network partition detected
    NetworkPartition,

    /// Security alert
    SecurityAlert,

    /// Custom alert type
    Custom,
}

/// Suggested action for alert resolution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuggestedAction {
    /// Unique action identifier
    pub action_id: String,

    /// Short label for UI button
    pub label: String,

    /// Detailed description of what this action does
    pub description: String,

    /// CLI command equivalent
    pub command: String,

    /// Whether this is the recommended default action
    pub is_default: bool,
}

impl SuggestedAction {
    /// Create a new suggested action
    pub fn new(label: String, description: String, command: String) -> Self {
        Self {
            action_id: uuid::Uuid::new_v4().to_string(),
            label,
            description,
            command,
            is_default: false,
        }
    }

    /// Mark this action as the default
    pub fn as_default(mut self) -> Self {
        self.is_default = true;
        self
    }
}

/// Complete snapshot of the system state for dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    /// When this snapshot was taken
    pub timestamp: DateTime<Utc>,

    /// Total compute capacity across all regions (FLOPS)
    pub total_flops: f64,

    /// Total number of nodes across all regions
    pub total_nodes: u32,

    /// Statistics for each region
    pub regions: Vec<RegionStats>,

    /// Metrics for currently active jobs
    pub active_jobs: Vec<JobMetrics>,

    /// Active alerts
    pub alerts: Vec<Alert>,
}

impl DashboardSnapshot {
    /// Create a new empty dashboard snapshot
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now(),
            total_flops: 0.0,
            total_nodes: 0,
            regions: Vec::new(),
            active_jobs: Vec::new(),
            alerts: Vec::new(),
        }
    }

    /// Get overall system health score (0.0 to 1.0)
    pub fn health_score(&self) -> f64 {
        if self.total_nodes == 0 {
            return 0.0;
        }

        // Deduct points for critical alerts
        let critical_alerts = self
            .alerts
            .iter()
            .filter(|a| a.severity == AlertSeverity::Critical && !a.dismissed)
            .count();

        let warning_alerts = self
            .alerts
            .iter()
            .filter(|a| a.severity == AlertSeverity::Warning && !a.dismissed)
            .count();

        let alert_penalty = (critical_alerts as f64 * 0.1) + (warning_alerts as f64 * 0.05);

        // Base health on average region utilization
        let avg_utilization = if !self.regions.is_empty() {
            self.regions.iter().map(|r| r.utilization()).sum::<f64>() / self.regions.len() as f64
        } else {
            0.0
        };

        let base_health = (avg_utilization * 0.5) + 0.5; // Normalize to 0.5-1.0 range
        (base_health - alert_penalty).max(0.0).min(1.0)
    }

    /// Count alerts by severity
    pub fn alert_counts(&self) -> AlertCounts {
        let mut counts = AlertCounts::default();
        for alert in &self.alerts {
            if !alert.dismissed {
                match alert.severity {
                    AlertSeverity::Info => counts.info += 1,
                    AlertSeverity::Warning => counts.warning += 1,
                    AlertSeverity::Critical => counts.critical += 1,
                }
            }
        }
        counts
    }
}

impl Default for DashboardSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// Alert counts by severity
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlertCounts {
    pub info: u32,
    pub warning: u32,
    pub critical: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_metrics_serialization() {
        let node_id = NodeId::new();
        let region_id = RegionId::new();
        let mut metrics = NodeMetrics::new(node_id, region_id, "default".to_string());

        metrics.cpu_flops = 1_000_000_000.0;
        metrics.memory_mb = 8192;
        metrics.disk_mb = 102400;
        metrics.current_load = 0.5;
        metrics.temperature = Some(45.5);
        metrics.battery_level = Some(0.85);

        // Serialize to JSON
        let json = serde_json::to_string(&metrics).expect("Failed to serialize");

        // Deserialize back
        let deserialized: NodeMetrics = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(deserialized.node_id, node_id);
        assert_eq!(deserialized.region, region_id);
        assert_eq!(deserialized.cpu_flops, 1_000_000_000.0);
        assert_eq!(deserialized.memory_mb, 8192);
        assert_eq!(deserialized.temperature, Some(45.5));
        assert_eq!(deserialized.battery_level, Some(0.85));
    }

    #[test]
    fn test_region_stats_serialization() {
        let region_id = RegionId::new();
        let mut stats = RegionStats::new(region_id, "us-west".to_string());

        stats.total_flops = 10_000_000_000.0;
        stats.available_flops = 7_000_000_000.0;
        stats.node_count = 100;
        stats.active_count = 70;
        stats.idle_count = 30;

        let json = serde_json::to_string(&stats).expect("Failed to serialize");
        let deserialized: RegionStats = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(deserialized.region_id, region_id);
        assert_eq!(deserialized.name, "us-west");
        assert_eq!(deserialized.total_flops, 10_000_000_000.0);
        assert!((deserialized.utilization() - 0.3).abs() < 1e-10);
    }

    #[test]
    fn test_job_metrics_serialization() {
        let job_id = JobId::new();
        let mut metrics = JobMetrics::new(job_id, "test-job".to_string(), 1000);

        metrics.completed = 750;
        metrics.failed = 10;
        metrics.in_flight = 50;
        metrics.throughput_per_minute = 125.5;

        let json = serde_json::to_string(&metrics).expect("Failed to serialize");
        let deserialized: JobMetrics = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(deserialized.job_id, job_id);
        assert_eq!(deserialized.name, "test-job");
        assert_eq!(deserialized.progress(), 0.75);
        assert!((deserialized.failure_rate() - 0.0131578947).abs() < 0.0001);
    }

    #[test]
    fn test_alert_serialization() {
        let mut alert = Alert::new(
            AlertSeverity::Critical,
            AlertType::ClusterGoingOffline,
            "Region us-east is losing nodes".to_string(),
        );

        alert = alert.with_action(
            SuggestedAction::new(
                "Restart Nodes".to_string(),
                "Attempt to restart offline nodes".to_string(),
                "marabunta nodes restart --region us-east".to_string(),
            )
            .as_default(),
        );

        let json = serde_json::to_string(&alert).expect("Failed to serialize");
        let deserialized: Alert = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(deserialized.severity, AlertSeverity::Critical);
        assert_eq!(deserialized.alert_type, AlertType::ClusterGoingOffline);
        assert_eq!(deserialized.suggested_actions.len(), 1);
        assert!(deserialized.suggested_actions[0].is_default);
    }

    #[test]
    fn test_dashboard_snapshot_serialization() {
        let mut snapshot = DashboardSnapshot::new();

        snapshot.total_flops = 50_000_000_000.0;
        snapshot.total_nodes = 500;

        let region1 = RegionStats::new(RegionId::new(), "us-west".to_string());
        let region2 = RegionStats::new(RegionId::new(), "us-east".to_string());
        snapshot.regions.push(region1);
        snapshot.regions.push(region2);

        let job = JobMetrics::new(JobId::new(), "benchmark".to_string(), 10000);
        snapshot.active_jobs.push(job);

        let alert = Alert::new(
            AlertSeverity::Warning,
            AlertType::HighFailureRate,
            "Failure rate above 5%".to_string(),
        );
        snapshot.alerts.push(alert);

        let json = serde_json::to_string(&snapshot).expect("Failed to serialize");
        let deserialized: DashboardSnapshot =
            serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(deserialized.total_nodes, 500);
        assert_eq!(deserialized.regions.len(), 2);
        assert_eq!(deserialized.active_jobs.len(), 1);
        assert_eq!(deserialized.alerts.len(), 1);
    }

    #[test]
    fn test_throttling_reason_serialization() {
        let reasons = vec![
            ThrottlingReason::Overheating,
            ThrottlingReason::LowBattery,
            ThrottlingReason::HighLatency,
            ThrottlingReason::MemoryPressure,
            ThrottlingReason::Manual,
            ThrottlingReason::CostOptimization,
        ];

        for reason in reasons {
            let json = serde_json::to_string(&reason).expect("Failed to serialize");
            let deserialized: ThrottlingReason =
                serde_json::from_str(&json).expect("Failed to deserialize");
            assert_eq!(deserialized, reason);
        }
    }

    #[test]
    fn test_bottleneck_type_serialization() {
        let types = vec![
            BottleneckType::ComputeCapacity,
            BottleneckType::NetworkLatency,
            BottleneckType::Memory,
            BottleneckType::Storage,
            BottleneckType::Scheduling,
            BottleneckType::DataTransfer,
        ];

        for btype in types {
            let json = serde_json::to_string(&btype).expect("Failed to serialize");
            let deserialized: BottleneckType =
                serde_json::from_str(&json).expect("Failed to deserialize");
            assert_eq!(deserialized, btype);
        }
    }

    #[test]
    fn test_node_metrics_health_check() {
        let node_id = NodeId::new();
        let region_id = RegionId::new();
        let mut metrics = NodeMetrics::new(node_id, region_id, "default".to_string());

        // Fresh node should be healthy
        assert!(metrics.is_healthy());
        assert!(metrics.is_available());

        // Old heartbeat makes it unhealthy
        metrics.last_heartbeat = Utc::now() - chrono::Duration::seconds(120);
        assert!(!metrics.is_healthy());

        // Throttled node is unhealthy
        metrics.last_heartbeat = Utc::now();
        metrics.throttling_reason = Some(ThrottlingReason::Overheating);
        assert!(!metrics.is_healthy());

        // High load makes it unavailable
        metrics.throttling_reason = None;
        metrics.current_load = 0.95;
        assert!(metrics.is_healthy());
        assert!(!metrics.is_available());
    }

    #[test]
    fn test_dashboard_health_score() {
        let mut snapshot = DashboardSnapshot::new();
        snapshot.total_nodes = 100;

        // Empty system
        assert_eq!(snapshot.health_score(), 0.5);

        // Add critical alert
        snapshot.alerts.push(Alert::new(
            AlertSeverity::Critical,
            AlertType::ClusterGoingOffline,
            "Test".to_string(),
        ));

        let score_with_alert = snapshot.health_score();
        assert!(score_with_alert < 0.5);

        // Dismiss alert
        snapshot.alerts[0].dismissed = true;
        assert_eq!(snapshot.health_score(), 0.5);
    }

    #[test]
    fn test_alert_counts() {
        let mut snapshot = DashboardSnapshot::new();

        snapshot.alerts.push(Alert::new(
            AlertSeverity::Info,
            AlertType::NodeIdle,
            "Node idle".to_string(),
        ));

        snapshot.alerts.push(Alert::new(
            AlertSeverity::Warning,
            AlertType::HighFailureRate,
            "High failures".to_string(),
        ));

        snapshot.alerts.push(Alert::new(
            AlertSeverity::Critical,
            AlertType::ClusterGoingOffline,
            "Cluster offline".to_string(),
        ));

        let counts = snapshot.alert_counts();
        assert_eq!(counts.info, 1);
        assert_eq!(counts.warning, 1);
        assert_eq!(counts.critical, 1);

        // Dismissed alerts shouldn't count
        snapshot.alerts[0].dismissed = true;
        let counts = snapshot.alert_counts();
        assert_eq!(counts.info, 0);
    }
}
