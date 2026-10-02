// Marabunta - Licensed under the MIT License.
//! Maintenance window types and definitions
//!
//! Defines the core types for scheduled maintenance windows, including
//! maintenance types, states, and window configurations.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use uuid::Uuid;

use crate::common::types::WorkerId;

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE WINDOW ID
// ─────────────────────────────────────────────────────────────────────────────

/// Unique identifier for a maintenance window
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MaintenanceWindowId(pub Uuid);

impl MaintenanceWindowId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MaintenanceWindowId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MaintenanceWindowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "maint-{}", &self.0.to_string()[..8])
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE TYPE
// ─────────────────────────────────────────────────────────────────────────────

/// Type of maintenance being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceType {
    /// Planned maintenance with advance notice
    /// Allows time for graceful task migration
    Planned,

    /// Emergency maintenance requiring immediate action
    /// May force-stop running tasks if necessary
    Emergency,

    /// Rolling maintenance affecting one node at a time
    /// Minimizes cluster impact by maintaining capacity
    Rolling,

    /// Hardware upgrade or replacement
    Hardware,

    /// Software/OS update
    Software,

    /// Security patch application
    Security,
}

impl fmt::Display for MaintenanceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MaintenanceType::Planned => write!(f, "Planned"),
            MaintenanceType::Emergency => write!(f, "Emergency"),
            MaintenanceType::Rolling => write!(f, "Rolling"),
            MaintenanceType::Hardware => write!(f, "Hardware"),
            MaintenanceType::Software => write!(f, "Software"),
            MaintenanceType::Security => write!(f, "Security"),
        }
    }
}

impl MaintenanceType {
    /// Returns the default drain timeout for this maintenance type
    pub fn default_drain_timeout(&self) -> Duration {
        match self {
            MaintenanceType::Emergency | MaintenanceType::Security => Duration::minutes(5),
            MaintenanceType::Rolling => Duration::minutes(15),
            MaintenanceType::Planned | MaintenanceType::Hardware | MaintenanceType::Software => {
                Duration::minutes(30)
            }
        }
    }

    /// Returns whether this maintenance type allows task preemption
    pub fn allows_force_drain(&self) -> bool {
        matches!(self, MaintenanceType::Emergency | MaintenanceType::Security)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE STATE
// ─────────────────────────────────────────────────────────────────────────────

/// Current state of a maintenance window
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceState {
    /// Maintenance is scheduled but not yet started
    Scheduled,

    /// Nodes are being drained (tasks migrating)
    Draining,

    /// Maintenance is actively in progress
    InProgress,

    /// Maintenance completed successfully
    Completed,

    /// Maintenance was cancelled before or during execution
    Cancelled,

    /// Maintenance failed or was aborted
    Failed,
}

impl fmt::Display for MaintenanceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MaintenanceState::Scheduled => write!(f, "Scheduled"),
            MaintenanceState::Draining => write!(f, "Draining"),
            MaintenanceState::InProgress => write!(f, "In Progress"),
            MaintenanceState::Completed => write!(f, "Completed"),
            MaintenanceState::Cancelled => write!(f, "Cancelled"),
            MaintenanceState::Failed => write!(f, "Failed"),
        }
    }
}

impl MaintenanceState {
    /// Check if this is a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            MaintenanceState::Completed | MaintenanceState::Cancelled | MaintenanceState::Failed
        )
    }

    /// Check if this state allows cancellation
    pub fn can_cancel(&self) -> bool {
        matches!(
            self,
            MaintenanceState::Scheduled | MaintenanceState::Draining
        )
    }

    /// Check if nodes are actively unavailable during this state
    pub fn nodes_unavailable(&self) -> bool {
        matches!(
            self,
            MaintenanceState::Draining | MaintenanceState::InProgress
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE WINDOW
// ─────────────────────────────────────────────────────────────────────────────

/// A scheduled maintenance window affecting one or more nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceWindow {
    /// Unique identifier
    pub id: MaintenanceWindowId,

    /// Human-readable name/description
    pub name: String,

    /// Detailed description of maintenance work
    pub description: Option<String>,

    /// Type of maintenance
    pub maintenance_type: MaintenanceType,

    /// Current state
    pub state: MaintenanceState,

    /// Scheduled start time
    pub scheduled_start: DateTime<Utc>,

    /// Scheduled end time
    pub scheduled_end: DateTime<Utc>,

    /// Actual start time (when draining began)
    pub actual_start: Option<DateTime<Utc>>,

    /// Actual end time (when maintenance completed)
    pub actual_end: Option<DateTime<Utc>>,

    /// Nodes affected by this maintenance window
    pub affected_nodes: HashSet<WorkerId>,

    /// Nodes that have been successfully drained
    pub drained_nodes: HashSet<WorkerId>,

    /// Nodes currently being drained
    pub draining_nodes: HashSet<WorkerId>,

    /// For rolling maintenance, how many nodes at a time
    pub rolling_batch_size: usize,

    /// Timeout for draining operations
    pub drain_timeout: Duration,

    /// Whether to force-stop tasks if drain timeout is exceeded
    pub force_drain_on_timeout: bool,

    /// User who scheduled the maintenance
    pub created_by: Option<String>,

    /// When the maintenance was scheduled
    pub created_at: DateTime<Utc>,

    /// Last update time
    pub updated_at: DateTime<Utc>,

    /// Reason for cancellation or failure
    pub failure_reason: Option<String>,

    /// Metadata for custom attributes
    pub metadata: serde_json::Value,
}

impl MaintenanceWindow {
    /// Create a new maintenance window
    pub fn new(
        name: impl Into<String>,
        maintenance_type: MaintenanceType,
        scheduled_start: DateTime<Utc>,
        scheduled_end: DateTime<Utc>,
        affected_nodes: HashSet<WorkerId>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: MaintenanceWindowId::new(),
            name: name.into(),
            description: None,
            maintenance_type,
            state: MaintenanceState::Scheduled,
            scheduled_start,
            scheduled_end,
            actual_start: None,
            actual_end: None,
            affected_nodes,
            drained_nodes: HashSet::new(),
            draining_nodes: HashSet::new(),
            rolling_batch_size: 1,
            drain_timeout: maintenance_type.default_drain_timeout(),
            force_drain_on_timeout: maintenance_type.allows_force_drain(),
            created_by: None,
            created_at: now,
            updated_at: now,
            failure_reason: None,
            metadata: serde_json::Value::Null,
        }
    }

    /// Create an emergency maintenance window starting immediately
    pub fn emergency(
        name: impl Into<String>,
        affected_nodes: HashSet<WorkerId>,
        duration: Duration,
    ) -> Self {
        let now = Utc::now();
        let end = now + duration;

        let mut window = Self::new(name, MaintenanceType::Emergency, now, end, affected_nodes);
        window.force_drain_on_timeout = true;
        window
    }

    /// Create a rolling maintenance window
    pub fn rolling(
        name: impl Into<String>,
        scheduled_start: DateTime<Utc>,
        scheduled_end: DateTime<Utc>,
        affected_nodes: HashSet<WorkerId>,
        batch_size: usize,
    ) -> Self {
        let mut window = Self::new(
            name,
            MaintenanceType::Rolling,
            scheduled_start,
            scheduled_end,
            affected_nodes,
        );
        window.rolling_batch_size = batch_size.max(1);
        window
    }

    /// Check if the maintenance window is active (draining or in progress)
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            MaintenanceState::Draining | MaintenanceState::InProgress
        )
    }

    /// Check if a node is affected by this maintenance
    pub fn affects_node(&self, node_id: &WorkerId) -> bool {
        self.affected_nodes.contains(node_id)
    }

    /// Check if it's time to start the maintenance
    pub fn should_start(&self) -> bool {
        self.state == MaintenanceState::Scheduled && Utc::now() >= self.scheduled_start
    }

    /// Check if the maintenance is overdue to complete
    pub fn is_overdue(&self) -> bool {
        self.is_active() && Utc::now() > self.scheduled_end
    }

    /// Get the expected duration
    pub fn expected_duration(&self) -> Duration {
        self.scheduled_end - self.scheduled_start
    }

    /// Get the actual duration (if completed)
    pub fn actual_duration(&self) -> Option<Duration> {
        match (self.actual_start, self.actual_end) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }

    /// Get pending nodes (affected but not yet drained)
    pub fn pending_nodes(&self) -> HashSet<WorkerId> {
        self.affected_nodes
            .difference(&self.drained_nodes)
            .cloned()
            .collect()
    }

    /// Get progress as a fraction (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        if self.affected_nodes.is_empty() {
            return 1.0;
        }
        self.drained_nodes.len() as f64 / self.affected_nodes.len() as f64
    }

    /// Transition to draining state
    pub fn start_draining(&mut self) -> Result<(), MaintenanceError> {
        if self.state != MaintenanceState::Scheduled {
            return Err(MaintenanceError::InvalidStateTransition {
                from: self.state,
                to: MaintenanceState::Draining,
            });
        }

        self.state = MaintenanceState::Draining;
        self.actual_start = Some(Utc::now());
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Transition to in-progress state (all nodes drained)
    pub fn start_maintenance(&mut self) -> Result<(), MaintenanceError> {
        if self.state != MaintenanceState::Draining {
            return Err(MaintenanceError::InvalidStateTransition {
                from: self.state,
                to: MaintenanceState::InProgress,
            });
        }

        self.state = MaintenanceState::InProgress;
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Mark maintenance as completed
    pub fn complete(&mut self) -> Result<(), MaintenanceError> {
        if !matches!(
            self.state,
            MaintenanceState::Draining | MaintenanceState::InProgress
        ) {
            return Err(MaintenanceError::InvalidStateTransition {
                from: self.state,
                to: MaintenanceState::Completed,
            });
        }

        self.state = MaintenanceState::Completed;
        self.actual_end = Some(Utc::now());
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Cancel the maintenance window
    pub fn cancel(&mut self, reason: impl Into<String>) -> Result<(), MaintenanceError> {
        if !self.state.can_cancel() {
            return Err(MaintenanceError::CannotCancel(self.state));
        }

        self.state = MaintenanceState::Cancelled;
        self.failure_reason = Some(reason.into());
        self.actual_end = Some(Utc::now());
        self.updated_at = Utc::now();
        Ok(())
    }

    /// Mark maintenance as failed
    pub fn fail(&mut self, reason: impl Into<String>) {
        self.state = MaintenanceState::Failed;
        self.failure_reason = Some(reason.into());
        self.actual_end = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    /// Mark a node as drained
    pub fn mark_node_drained(&mut self, node_id: WorkerId) {
        self.draining_nodes.remove(&node_id);
        self.drained_nodes.insert(node_id);
        self.updated_at = Utc::now();
    }

    /// Start draining a node
    pub fn start_node_drain(&mut self, node_id: WorkerId) {
        self.draining_nodes.insert(node_id);
        self.updated_at = Utc::now();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DRAIN STATUS
// ─────────────────────────────────────────────────────────────────────────────

/// Status of a node drain operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DrainStatus {
    /// Node is not being drained
    NotDraining,

    /// Drain requested, waiting to start
    Pending,

    /// Tasks are being migrated off the node
    Draining,

    /// Waiting for running tasks to complete
    WaitingForTasks,

    /// Drain completed, node is empty
    Drained,

    /// Drain was force-stopped
    ForceDrained,

    /// Drain was cancelled
    Cancelled,
}

impl fmt::Display for DrainStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DrainStatus::NotDraining => write!(f, "Not Draining"),
            DrainStatus::Pending => write!(f, "Pending"),
            DrainStatus::Draining => write!(f, "Draining"),
            DrainStatus::WaitingForTasks => write!(f, "Waiting for Tasks"),
            DrainStatus::Drained => write!(f, "Drained"),
            DrainStatus::ForceDrained => write!(f, "Force Drained"),
            DrainStatus::Cancelled => write!(f, "Cancelled"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DRAIN INFO
// ─────────────────────────────────────────────────────────────────────────────

/// Information about a node's drain operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainInfo {
    /// Node being drained
    pub node_id: WorkerId,

    /// Associated maintenance window
    pub maintenance_id: MaintenanceWindowId,

    /// Current drain status
    pub status: DrainStatus,

    /// When drain was requested
    pub requested_at: DateTime<Utc>,

    /// When drain started
    pub started_at: Option<DateTime<Utc>>,

    /// When drain completed
    pub completed_at: Option<DateTime<Utc>>,

    /// Tasks that were on the node when drain started
    pub initial_task_count: usize,

    /// Tasks successfully migrated
    pub migrated_task_count: usize,

    /// Tasks that completed during drain
    pub completed_task_count: usize,

    /// Tasks that were force-stopped
    pub force_stopped_count: usize,

    /// Tasks remaining on node
    pub remaining_task_count: usize,

    /// Drain timeout
    pub timeout: Duration,

    /// Whether force drain is allowed
    pub force_on_timeout: bool,
}

impl DrainInfo {
    /// Create new drain info
    pub fn new(
        node_id: WorkerId,
        maintenance_id: MaintenanceWindowId,
        initial_task_count: usize,
        timeout: Duration,
        force_on_timeout: bool,
    ) -> Self {
        Self {
            node_id,
            maintenance_id,
            status: DrainStatus::Pending,
            requested_at: Utc::now(),
            started_at: None,
            completed_at: None,
            initial_task_count,
            migrated_task_count: 0,
            completed_task_count: 0,
            force_stopped_count: 0,
            remaining_task_count: initial_task_count,
            timeout,
            force_on_timeout,
        }
    }

    /// Check if drain has timed out
    pub fn is_timed_out(&self) -> bool {
        if let Some(started) = self.started_at {
            Utc::now() > started + self.timeout
        } else {
            false
        }
    }

    /// Get progress as a fraction
    pub fn progress(&self) -> f64 {
        if self.initial_task_count == 0 {
            return 1.0;
        }
        1.0 - (self.remaining_task_count as f64 / self.initial_task_count as f64)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE ERROR
// ─────────────────────────────────────────────────────────────────────────────

/// Errors that can occur during maintenance operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum MaintenanceError {
    #[error("Maintenance window not found: {0}")]
    NotFound(MaintenanceWindowId),

    #[error("Invalid state transition from {from} to {to}")]
    InvalidStateTransition {
        from: MaintenanceState,
        to: MaintenanceState,
    },

    #[error("Cannot cancel maintenance in state: {0}")]
    CannotCancel(MaintenanceState),

    #[error("Node {0} is not part of this maintenance window")]
    NodeNotInWindow(WorkerId),

    #[error("Node {0} is already in maintenance")]
    NodeAlreadyInMaintenance(WorkerId),

    #[error("Overlapping maintenance window exists: {0}")]
    OverlappingWindow(MaintenanceWindowId),

    #[error("Drain timeout exceeded for node: {0}")]
    DrainTimeout(WorkerId),

    #[error("Invalid maintenance window: {0}")]
    InvalidWindow(String),

    #[error("Scheduler error: {0}")]
    SchedulerError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE SUMMARY
// ─────────────────────────────────────────────────────────────────────────────

/// Summary information about a maintenance window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceSummary {
    pub id: MaintenanceWindowId,
    pub name: String,
    pub maintenance_type: MaintenanceType,
    pub state: MaintenanceState,
    pub scheduled_start: DateTime<Utc>,
    pub scheduled_end: DateTime<Utc>,
    pub affected_node_count: usize,
    pub drained_node_count: usize,
    pub progress: f64,
}

impl From<&MaintenanceWindow> for MaintenanceSummary {
    fn from(window: &MaintenanceWindow) -> Self {
        Self {
            id: window.id,
            name: window.name.clone(),
            maintenance_type: window.maintenance_type,
            state: window.state,
            scheduled_start: window.scheduled_start,
            scheduled_end: window.scheduled_end,
            affected_node_count: window.affected_nodes.len(),
            drained_node_count: window.drained_nodes.len(),
            progress: window.progress(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_nodes() -> HashSet<WorkerId> {
        let mut nodes = HashSet::new();
        nodes.insert(WorkerId::new());
        nodes.insert(WorkerId::new());
        nodes.insert(WorkerId::new());
        nodes
    }

    #[test]
    fn test_maintenance_window_creation() {
        let nodes = create_test_nodes();
        let start = Utc::now() + Duration::hours(1);
        let end = start + Duration::hours(2);

        let window = MaintenanceWindow::new(
            "Test Maintenance",
            MaintenanceType::Planned,
            start,
            end,
            nodes.clone(),
        );

        assert_eq!(window.state, MaintenanceState::Scheduled);
        assert_eq!(window.affected_nodes.len(), 3);
        assert_eq!(window.maintenance_type, MaintenanceType::Planned);
    }

    #[test]
    fn test_emergency_maintenance() {
        let nodes = create_test_nodes();
        let window =
            MaintenanceWindow::emergency("Emergency Fix", nodes.clone(), Duration::hours(1));

        assert_eq!(window.maintenance_type, MaintenanceType::Emergency);
        assert!(window.force_drain_on_timeout);
        assert!(window.should_start()); // Emergency starts immediately
    }

    #[test]
    fn test_rolling_maintenance() {
        let nodes = create_test_nodes();
        let start = Utc::now() + Duration::hours(1);
        let end = start + Duration::hours(4);

        let window = MaintenanceWindow::rolling("Rolling Update", start, end, nodes.clone(), 2);

        assert_eq!(window.maintenance_type, MaintenanceType::Rolling);
        assert_eq!(window.rolling_batch_size, 2);
    }

    #[test]
    fn test_state_transitions() {
        let nodes = create_test_nodes();
        let start = Utc::now();
        let end = start + Duration::hours(1);

        let mut window =
            MaintenanceWindow::new("Test", MaintenanceType::Planned, start, end, nodes.clone());

        // Valid transitions
        assert!(window.start_draining().is_ok());
        assert_eq!(window.state, MaintenanceState::Draining);

        assert!(window.start_maintenance().is_ok());
        assert_eq!(window.state, MaintenanceState::InProgress);

        assert!(window.complete().is_ok());
        assert_eq!(window.state, MaintenanceState::Completed);
    }

    #[test]
    fn test_invalid_state_transition() {
        let nodes = create_test_nodes();
        let start = Utc::now();
        let end = start + Duration::hours(1);

        let mut window =
            MaintenanceWindow::new("Test", MaintenanceType::Planned, start, end, nodes.clone());

        // Can't go directly to in-progress
        assert!(window.start_maintenance().is_err());

        // Can't complete from scheduled
        assert!(window.complete().is_err());
    }

    #[test]
    fn test_cancellation() {
        let nodes = create_test_nodes();
        let start = Utc::now() + Duration::hours(1);
        let end = start + Duration::hours(2);

        let mut window =
            MaintenanceWindow::new("Test", MaintenanceType::Planned, start, end, nodes.clone());

        // Can cancel when scheduled
        assert!(window.cancel("Changed plans").is_ok());
        assert_eq!(window.state, MaintenanceState::Cancelled);
        assert!(window.failure_reason.is_some());
    }

    #[test]
    fn test_progress_tracking() {
        let nodes = create_test_nodes();
        let node_list: Vec<_> = nodes.iter().cloned().collect();
        let start = Utc::now();
        let end = start + Duration::hours(1);

        let mut window =
            MaintenanceWindow::new("Test", MaintenanceType::Planned, start, end, nodes.clone());

        assert_eq!(window.progress(), 0.0);

        window.mark_node_drained(node_list[0]);
        assert!((window.progress() - 0.333).abs() < 0.01);

        window.mark_node_drained(node_list[1]);
        assert!((window.progress() - 0.666).abs() < 0.01);

        window.mark_node_drained(node_list[2]);
        assert_eq!(window.progress(), 1.0);
    }

    #[test]
    fn test_drain_info() {
        let node_id = WorkerId::new();
        let maint_id = MaintenanceWindowId::new();

        let mut drain = DrainInfo::new(node_id, maint_id, 10, Duration::minutes(30), false);

        assert_eq!(drain.progress(), 0.0);
        assert!(!drain.is_timed_out());

        drain.remaining_task_count = 5;
        drain.migrated_task_count = 5;
        assert_eq!(drain.progress(), 0.5);
    }

    #[test]
    fn test_maintenance_type_defaults() {
        assert!(MaintenanceType::Emergency.allows_force_drain());
        assert!(MaintenanceType::Security.allows_force_drain());
        assert!(!MaintenanceType::Planned.allows_force_drain());
        assert!(!MaintenanceType::Rolling.allows_force_drain());

        assert!(
            MaintenanceType::Emergency.default_drain_timeout()
                < MaintenanceType::Planned.default_drain_timeout()
        );
    }

    #[test]
    fn test_maintenance_state_properties() {
        assert!(MaintenanceState::Completed.is_terminal());
        assert!(MaintenanceState::Cancelled.is_terminal());
        assert!(MaintenanceState::Failed.is_terminal());
        assert!(!MaintenanceState::InProgress.is_terminal());

        assert!(MaintenanceState::Scheduled.can_cancel());
        assert!(MaintenanceState::Draining.can_cancel());
        assert!(!MaintenanceState::InProgress.can_cancel());

        assert!(MaintenanceState::InProgress.nodes_unavailable());
        assert!(MaintenanceState::Draining.nodes_unavailable());
        assert!(!MaintenanceState::Scheduled.nodes_unavailable());
    }
}
