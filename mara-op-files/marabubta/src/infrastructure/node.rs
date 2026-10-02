// Marabunta - Licensed under the MIT License.
//! Infrastructure Node Representation
//!
//! This module defines the core data structures for representing infrastructure nodes,
//! including hardware specifications, location information, SLA tiers, and task history.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::time::Instant;
use uuid::Uuid;

use crate::common::TaskId;

use super::metrics::NodeMetrics;

/// Unique identifier for an infrastructure node.
///
/// Wraps a UUID for type safety and provides display formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    /// Creates a new random NodeId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a NodeId from a deterministic name using UUID v5.
    ///
    /// This is useful for creating stable IDs from hostnames or other identifiers.
    pub fn from_name(name: &str) -> Self {
        Self(Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes()))
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", &self.0.to_string()[..8])
    }
}

impl From<Uuid> for NodeId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Department or team that owns the node.
///
/// Used for accounting, chargeback, and access control.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Department {
    /// Unique identifier for the department.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Cost center for chargeback.
    pub cost_center: Option<String>,
    /// Contact email for alerts.
    pub contact_email: Option<String>,
}

impl Department {
    /// Creates a new department with the given ID and name.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            cost_center: None,
            contact_email: None,
        }
    }

    /// Sets the cost center for chargeback.
    pub fn with_cost_center(mut self, cost_center: impl Into<String>) -> Self {
        self.cost_center = Some(cost_center.into());
        self
    }

    /// Sets the contact email for alerts.
    pub fn with_contact_email(mut self, email: impl Into<String>) -> Self {
        self.contact_email = Some(email.into());
        self
    }
}

/// Physical and logical location of an infrastructure node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    /// Datacenter or facility name.
    pub datacenter: String,
    /// Rack identifier within the datacenter.
    pub rack: Option<String>,
    /// Position within the rack (e.g., "U1-U4").
    pub position: Option<String>,
    /// Network zone for firewall and routing purposes.
    pub network_zone: String,
}

impl Location {
    /// Creates a new location with required fields.
    pub fn new(datacenter: impl Into<String>, network_zone: impl Into<String>) -> Self {
        Self {
            datacenter: datacenter.into(),
            rack: None,
            position: None,
            network_zone: network_zone.into(),
        }
    }

    /// Sets the rack identifier.
    pub fn with_rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }

    /// Sets the position within the rack.
    pub fn with_position(mut self, position: impl Into<String>) -> Self {
        self.position = Some(position.into());
        self
    }

    /// Returns the full location path (datacenter/rack/position).
    pub fn full_path(&self) -> String {
        let mut path = self.datacenter.clone();
        if let Some(ref rack) = self.rack {
            path.push('/');
            path.push_str(rack);
            if let Some(ref pos) = self.position {
                path.push('/');
                path.push_str(pos);
            }
        }
        path
    }
}

/// Hardware specifications of an infrastructure node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HardwareSpecs {
    /// Number of CPU cores available.
    pub cpu_cores: u32,
    /// CPU model string (e.g., "Intel Xeon E5-2690 v4").
    pub cpu_model: String,
    /// Total RAM in bytes.
    pub ram_bytes: u64,
    /// Number of GPUs installed.
    pub gpu_count: u32,
    /// GPU model strings.
    pub gpu_models: Vec<String>,
    /// VRAM per GPU in bytes.
    pub gpu_vram_bytes: Vec<u64>,
    /// Total storage capacity in bytes.
    pub storage_bytes: u64,
    /// Network bandwidth in Mbps.
    pub network_bandwidth_mbps: u32,
}

impl HardwareSpecs {
    /// Creates a new HardwareSpecs with CPU-only configuration.
    pub fn cpu_only(cores: u32, model: impl Into<String>, ram_bytes: u64) -> Self {
        Self {
            cpu_cores: cores,
            cpu_model: model.into(),
            ram_bytes,
            gpu_count: 0,
            gpu_models: Vec::new(),
            gpu_vram_bytes: Vec::new(),
            storage_bytes: 0,
            network_bandwidth_mbps: 1000, // Default 1Gbps
        }
    }

    /// Adds GPU configuration.
    pub fn with_gpus(mut self, models: Vec<String>, vram_bytes: Vec<u64>) -> Self {
        assert_eq!(
            models.len(),
            vram_bytes.len(),
            "GPU models and VRAM counts must match"
        );
        self.gpu_count = models.len() as u32;
        self.gpu_models = models;
        self.gpu_vram_bytes = vram_bytes;
        self
    }

    /// Sets storage capacity.
    pub fn with_storage(mut self, bytes: u64) -> Self {
        self.storage_bytes = bytes;
        self
    }

    /// Sets network bandwidth.
    pub fn with_network(mut self, bandwidth_mbps: u32) -> Self {
        self.network_bandwidth_mbps = bandwidth_mbps;
        self
    }

    /// Returns total GPU VRAM in bytes.
    pub fn total_gpu_vram(&self) -> u64 {
        self.gpu_vram_bytes.iter().sum()
    }

    /// Returns RAM in gigabytes.
    pub fn ram_gb(&self) -> f64 {
        self.ram_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    /// Returns storage in terabytes.
    pub fn storage_tb(&self) -> f64 {
        self.storage_bytes as f64 / (1024.0 * 1024.0 * 1024.0 * 1024.0)
    }

    /// Validates that hardware specs are sensible.
    pub fn validate(&self) -> Result<(), HardwareSpecsError> {
        if self.cpu_cores == 0 {
            return Err(HardwareSpecsError::InvalidCpuCores);
        }
        if self.ram_bytes == 0 {
            return Err(HardwareSpecsError::InvalidRam);
        }
        if self.gpu_models.len() != self.gpu_vram_bytes.len() {
            return Err(HardwareSpecsError::GpuMismatch);
        }
        if self.gpu_count != self.gpu_models.len() as u32 {
            return Err(HardwareSpecsError::GpuMismatch);
        }
        Ok(())
    }
}

/// Errors from hardware specification validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum HardwareSpecsError {
    #[error("CPU cores must be greater than 0")]
    InvalidCpuCores,
    #[error("RAM must be greater than 0")]
    InvalidRam,
    #[error("GPU model count must match VRAM count and gpu_count")]
    GpuMismatch,
}

/// Status of an infrastructure node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum NodeStatus {
    /// Node is online and accepting work.
    Online {
        /// When the node came online.
        since: DateTime<Utc>,
    },
    /// Node is online but experiencing issues.
    Degraded {
        /// Description of the degradation.
        reason: String,
        /// When degradation was detected.
        since: DateTime<Utc>,
    },
    /// Node is offline and not available.
    Offline {
        /// Reason for being offline.
        reason: String,
        /// When the node went offline.
        since: DateTime<Utc>,
    },
    /// Node is in scheduled maintenance.
    Maintenance {
        /// When maintenance is expected to end.
        scheduled_end: DateTime<Utc>,
        /// Description of maintenance work.
        reason: String,
    },
}

impl NodeStatus {
    /// Creates a new Online status.
    pub fn online() -> Self {
        Self::Online { since: Utc::now() }
    }

    /// Creates a new Degraded status.
    pub fn degraded(reason: impl Into<String>) -> Self {
        Self::Degraded {
            reason: reason.into(),
            since: Utc::now(),
        }
    }

    /// Creates a new Offline status.
    pub fn offline(reason: impl Into<String>) -> Self {
        Self::Offline {
            reason: reason.into(),
            since: Utc::now(),
        }
    }

    /// Creates a new Maintenance status.
    pub fn maintenance(scheduled_end: DateTime<Utc>, reason: impl Into<String>) -> Self {
        Self::Maintenance {
            scheduled_end,
            reason: reason.into(),
        }
    }

    /// Returns true if the node can accept work.
    pub fn can_accept_work(&self) -> bool {
        matches!(self, NodeStatus::Online { .. })
    }

    /// Returns true if the node is healthy (online or degraded).
    pub fn is_healthy(&self) -> bool {
        matches!(
            self,
            NodeStatus::Online { .. } | NodeStatus::Degraded { .. }
        )
    }

    /// Returns the status name as a string.
    pub fn name(&self) -> &'static str {
        match self {
            NodeStatus::Online { .. } => "online",
            NodeStatus::Degraded { .. } => "degraded",
            NodeStatus::Offline { .. } => "offline",
            NodeStatus::Maintenance { .. } => "maintenance",
        }
    }
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeStatus::Online { since } => {
                write!(f, "Online since {}", since.format("%Y-%m-%d %H:%M:%S"))
            }
            NodeStatus::Degraded { reason, since } => {
                write!(
                    f,
                    "Degraded: {} (since {})",
                    reason,
                    since.format("%Y-%m-%d %H:%M:%S")
                )
            }
            NodeStatus::Offline { reason, since } => {
                write!(
                    f,
                    "Offline: {} (since {})",
                    reason,
                    since.format("%Y-%m-%d %H:%M:%S")
                )
            }
            NodeStatus::Maintenance {
                scheduled_end,
                reason,
            } => {
                write!(
                    f,
                    "Maintenance: {} (until {})",
                    reason,
                    scheduled_end.format("%Y-%m-%d %H:%M:%S")
                )
            }
        }
    }
}

/// Service Level Agreement tier for the node.
///
/// Determines uptime requirements and failover behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum SlaTier {
    /// 99.99% uptime, immediate failover.
    Critical,
    /// 99.9% uptime, fast failover.
    Production,
    /// 99% uptime, standard handling.
    #[default]
    Standard,
    /// No uptime guarantees.
    BestEffort,
}

impl SlaTier {
    /// Returns the required uptime percentage.
    pub fn required_uptime(&self) -> f64 {
        match self {
            SlaTier::Critical => 99.99,
            SlaTier::Production => 99.9,
            SlaTier::Standard => 99.0,
            SlaTier::BestEffort => 0.0,
        }
    }

    /// Returns the maximum allowed downtime per month in seconds.
    pub fn max_monthly_downtime_seconds(&self) -> u64 {
        const SECONDS_PER_MONTH: f64 = 30.0 * 24.0 * 60.0 * 60.0; // ~2,592,000
        let allowed_fraction = (100.0 - self.required_uptime()) / 100.0;
        (SECONDS_PER_MONTH * allowed_fraction) as u64
    }

    /// Returns the heartbeat timeout for this SLA tier.
    pub fn heartbeat_timeout_seconds(&self) -> u64 {
        match self {
            SlaTier::Critical => 5,
            SlaTier::Production => 15,
            SlaTier::Standard => 30,
            SlaTier::BestEffort => 60,
        }
    }

    /// Returns the priority level (higher = more important).
    pub fn priority(&self) -> u32 {
        match self {
            SlaTier::Critical => 1000,
            SlaTier::Production => 100,
            SlaTier::Standard => 10,
            SlaTier::BestEffort => 1,
        }
    }
}

impl fmt::Display for SlaTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SlaTier::Critical => write!(f, "Critical (99.99%)"),
            SlaTier::Production => write!(f, "Production (99.9%)"),
            SlaTier::Standard => write!(f, "Standard (99%)"),
            SlaTier::BestEffort => write!(f, "Best Effort"),
        }
    }
}


/// Record of a completed task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    /// Task identifier.
    pub task_id: TaskId,
    /// When the task started.
    pub started_at: DateTime<Utc>,
    /// When the task completed.
    pub completed_at: DateTime<Utc>,
    /// Whether the task succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

/// Task execution history for a node.
///
/// Maintains a rolling window of recent tasks for performance analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHistory {
    /// Maximum number of records to keep.
    #[serde(skip)]
    max_records: usize,
    /// Recent task records.
    records: VecDeque<TaskRecord>,
    /// Total tasks completed (including those no longer in records).
    total_completed: u64,
    /// Total tasks failed (including those no longer in records).
    total_failed: u64,
    /// Sum of all task durations in milliseconds.
    total_duration_ms: u64,
}

impl TaskHistory {
    /// Creates a new TaskHistory with the specified maximum record count.
    pub fn new(max_records: usize) -> Self {
        Self {
            max_records,
            records: VecDeque::with_capacity(max_records),
            total_completed: 0,
            total_failed: 0,
            total_duration_ms: 0,
        }
    }

    /// Records a task completion.
    pub fn record_task(&mut self, record: TaskRecord) {
        self.total_duration_ms += record.duration_ms;
        if record.success {
            self.total_completed += 1;
        } else {
            self.total_failed += 1;
        }

        self.records.push_back(record);
        while self.records.len() > self.max_records {
            self.records.pop_front();
        }
    }

    /// Returns the total number of completed tasks.
    pub fn completed_count(&self) -> u64 {
        self.total_completed
    }

    /// Returns the total number of failed tasks.
    pub fn failed_count(&self) -> u64 {
        self.total_failed
    }

    /// Returns the failure rate (0.0 - 1.0).
    pub fn failure_rate(&self) -> f64 {
        let total = self.total_completed + self.total_failed;
        if total == 0 {
            0.0
        } else {
            self.total_failed as f64 / total as f64
        }
    }

    /// Returns the average task duration in milliseconds.
    pub fn average_duration_ms(&self) -> u64 {
        let total = self.total_completed + self.total_failed;
        if total == 0 {
            0
        } else {
            self.total_duration_ms / total
        }
    }

    /// Returns recent failure rate based on the rolling window.
    pub fn recent_failure_rate(&self) -> f64 {
        if self.records.is_empty() {
            return 0.0;
        }
        let failed = self.records.iter().filter(|r| !r.success).count();
        failed as f64 / self.records.len() as f64
    }

    /// Returns recent task records.
    pub fn recent_records(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.iter()
    }

    /// Returns the number of records in the rolling window.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

impl Default for TaskHistory {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// Represents a fully-monitored infrastructure node.
///
/// Infrastructure nodes are accountable compute resources with:
/// - Known hardware specifications
/// - Physical/logical location
/// - Department ownership
/// - SLA requirements
/// - Comprehensive metrics tracking
/// - Full audit trail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfrastructureNode {
    /// Unique identifier for this node.
    pub id: NodeId,
    /// Physical and logical location.
    pub location: Location,
    /// Hardware specifications.
    pub specs: HardwareSpecs,
    /// Owning department.
    pub owner: Department,
    /// Current status.
    pub status: NodeStatus,
    /// Latest metrics.
    pub metrics: NodeMetrics,
    /// SLA tier.
    pub sla_tier: SlaTier,
    /// Time of last heartbeat.
    #[serde(skip)]
    pub last_heartbeat: Option<Instant>,
    /// Task execution history.
    pub task_history: TaskHistory,
    /// Human-readable hostname or label.
    pub hostname: String,
    /// Optional tags for filtering.
    pub tags: Vec<String>,
    /// When the node was registered.
    pub registered_at: DateTime<Utc>,
    /// Additional metadata.
    pub metadata: serde_json::Value,
}

impl InfrastructureNode {
    /// Creates a new infrastructure node.
    pub fn new(
        hostname: impl Into<String>,
        location: Location,
        specs: HardwareSpecs,
        owner: Department,
        sla_tier: SlaTier,
    ) -> Self {
        Self {
            id: NodeId::new(),
            location,
            specs,
            owner,
            status: NodeStatus::offline("Not yet started"),
            metrics: NodeMetrics::default(),
            sla_tier,
            last_heartbeat: None,
            task_history: TaskHistory::default(),
            hostname: hostname.into(),
            tags: Vec::new(),
            registered_at: Utc::now(),
            metadata: serde_json::Value::Null,
        }
    }

    /// Creates a node with a specific ID (for deterministic testing).
    pub fn with_id(mut self, id: NodeId) -> Self {
        self.id = id;
        self
    }

    /// Adds tags to the node.
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Sets metadata on the node.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Updates the node status.
    pub fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
    }

    /// Updates the heartbeat timestamp.
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
        if let NodeStatus::Offline { .. } = self.status {
            self.status = NodeStatus::online();
        }
    }

    /// Updates metrics from a new sample.
    pub fn update_metrics(&mut self, metrics: NodeMetrics) {
        self.metrics = metrics;
    }

    /// Records a completed task.
    pub fn record_task(&mut self, record: TaskRecord) {
        self.task_history.record_task(record);
    }

    /// Returns true if the node can accept new work.
    pub fn can_accept_work(&self) -> bool {
        self.status.can_accept_work()
    }

    /// Returns the time since last heartbeat.
    pub fn time_since_heartbeat(&self) -> Option<std::time::Duration> {
        self.last_heartbeat.map(|t| t.elapsed())
    }

    /// Validates the node configuration.
    pub fn validate(&self) -> Result<(), NodeValidationError> {
        self.specs
            .validate()
            .map_err(|e| NodeValidationError::InvalidSpecs(e.to_string()))?;

        if self.hostname.is_empty() {
            return Err(NodeValidationError::EmptyHostname);
        }

        if self.location.datacenter.is_empty() {
            return Err(NodeValidationError::EmptyDatacenter);
        }

        if self.location.network_zone.is_empty() {
            return Err(NodeValidationError::EmptyNetworkZone);
        }

        Ok(())
    }
}

/// Errors from node validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum NodeValidationError {
    #[error("Invalid hardware specs: {0}")]
    InvalidSpecs(String),
    #[error("Hostname cannot be empty")]
    EmptyHostname,
    #[error("Datacenter cannot be empty")]
    EmptyDatacenter,
    #[error("Network zone cannot be empty")]
    EmptyNetworkZone,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_creation() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();
        assert_ne!(id1, id2);

        let id3 = NodeId::from_name("server-001");
        let id4 = NodeId::from_name("server-001");
        assert_eq!(id3, id4);
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId::new();
        let display = format!("{}", id);
        assert!(display.starts_with("node-"));
        assert_eq!(display.len(), 13); // "node-" + 8 hex chars
    }

    #[test]
    fn test_hardware_specs_validation() {
        let valid = HardwareSpecs::cpu_only(8, "Intel Xeon", 16 * 1024 * 1024 * 1024);
        assert!(valid.validate().is_ok());

        let invalid_cores = HardwareSpecs::cpu_only(0, "Intel Xeon", 16 * 1024 * 1024 * 1024);
        assert!(invalid_cores.validate().is_err());

        let invalid_ram = HardwareSpecs::cpu_only(8, "Intel Xeon", 0);
        assert!(invalid_ram.validate().is_err());
    }

    #[test]
    fn test_hardware_specs_with_gpus() {
        let specs = HardwareSpecs::cpu_only(32, "AMD EPYC", 128 * 1024 * 1024 * 1024)
            .with_gpus(
                vec!["NVIDIA A100".to_string(), "NVIDIA A100".to_string()],
                vec![40 * 1024 * 1024 * 1024, 40 * 1024 * 1024 * 1024],
            )
            .with_storage(10 * 1024 * 1024 * 1024 * 1024)
            .with_network(25000);

        assert_eq!(specs.gpu_count, 2);
        assert_eq!(specs.total_gpu_vram(), 80 * 1024 * 1024 * 1024);
        assert!(specs.validate().is_ok());
    }

    #[test]
    fn test_location_full_path() {
        let loc = Location::new("us-east-1", "prod")
            .with_rack("rack-42")
            .with_position("U1-U4");
        assert_eq!(loc.full_path(), "us-east-1/rack-42/U1-U4");

        let simple = Location::new("eu-west-1", "dev");
        assert_eq!(simple.full_path(), "eu-west-1");
    }

    #[test]
    fn test_node_status_transitions() {
        let online = NodeStatus::online();
        assert!(online.can_accept_work());
        assert!(online.is_healthy());

        let degraded = NodeStatus::degraded("High CPU usage");
        assert!(!degraded.can_accept_work());
        assert!(degraded.is_healthy());

        let offline = NodeStatus::offline("Network failure");
        assert!(!offline.can_accept_work());
        assert!(!offline.is_healthy());
    }

    #[test]
    fn test_sla_tier_properties() {
        assert_eq!(SlaTier::Critical.required_uptime(), 99.99);
        assert_eq!(SlaTier::Production.required_uptime(), 99.9);
        assert_eq!(SlaTier::Standard.required_uptime(), 99.0);
        assert_eq!(SlaTier::BestEffort.required_uptime(), 0.0);

        // Critical allows ~4.3 minutes downtime per month
        assert!(SlaTier::Critical.max_monthly_downtime_seconds() < 300);
        // Best effort has no limit
        assert!(SlaTier::BestEffort.max_monthly_downtime_seconds() > 2_500_000);
    }

    #[test]
    fn test_task_history() {
        let mut history = TaskHistory::new(10);

        for i in 0..15 {
            let record = TaskRecord {
                task_id: TaskId::new(),
                started_at: Utc::now(),
                completed_at: Utc::now(),
                success: i % 3 != 0, // 2/3 success rate
                error: if i % 3 == 0 {
                    Some("test error".into())
                } else {
                    None
                },
                duration_ms: 100,
            };
            history.record_task(record);
        }

        // Should only keep last 10 records
        assert_eq!(history.record_count(), 10);
        // But totals should reflect all 15
        assert_eq!(history.completed_count(), 10);
        assert_eq!(history.failed_count(), 5);
        assert_eq!(history.average_duration_ms(), 100);
    }

    #[test]
    fn test_infrastructure_node_creation() {
        let node = InfrastructureNode::new(
            "compute-001",
            Location::new("dc-1", "prod"),
            HardwareSpecs::cpu_only(16, "Intel Xeon E5", 64 * 1024 * 1024 * 1024),
            Department::new("eng", "Engineering"),
            SlaTier::Production,
        )
        .with_tags(vec!["gpu".into(), "high-memory".into()]);

        assert!(node.validate().is_ok());
        assert!(!node.can_accept_work()); // Starts offline
        assert_eq!(node.hostname, "compute-001");
        assert_eq!(node.tags.len(), 2);
    }

    #[test]
    fn test_node_heartbeat() {
        let mut node = InfrastructureNode::new(
            "compute-001",
            Location::new("dc-1", "prod"),
            HardwareSpecs::cpu_only(16, "Intel Xeon E5", 64 * 1024 * 1024 * 1024),
            Department::new("eng", "Engineering"),
            SlaTier::Production,
        );

        assert!(node.last_heartbeat.is_none());
        assert!(!node.can_accept_work());

        node.heartbeat();
        assert!(node.last_heartbeat.is_some());
        assert!(node.can_accept_work()); // Now online

        let elapsed = node.time_since_heartbeat();
        assert!(elapsed.is_some());
        assert!(elapsed.unwrap().as_millis() < 100);
    }
}
