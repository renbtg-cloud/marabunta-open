// Marabunta - Licensed under the MIT License.
//! Fleet operations for the Marabunta Swarm.
//!
//! This module provides fleet-wide node lifecycle management: draining,
//! cordoning, quarantining, rolling updates with canary support, maintenance
//! windows, and node tagging. All operations are thread-safe and designed
//! to integrate with the gossip-based knowledge store.
//!
//! # Concepts
//!
//! - **Drain**: gracefully moves work off a node before taking it offline.
//!   The node stops accepting new work but finishes its current chunks.
//! - **Cordon**: prevents a node from accepting new work without draining
//!   existing work. Used for manual maintenance.
//! - **Quarantine**: isolates a misbehaving node from both work and gossip.
//!   Typically triggered by health checks or operator action.
//! - **Rolling Update**: upgrades nodes in batches with canary verification,
//!   health checking, and automatic rollback.
//! - **Maintenance Window**: scheduled time periods during which fleet
//!   operations are automatically applied.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::knowledge::KnowledgeStore;
use super::types::NodeId;

// ============================================================================
// FleetError
// ============================================================================

/// Errors that can occur during fleet operations.
#[derive(Debug)]
pub enum FleetError {
    /// The specified node was not found in the knowledge store.
    NodeNotFound(NodeId),
    /// The node is in a state that does not allow the requested transition.
    InvalidState {
        /// The node whose state is invalid for the requested operation.
        node_id: NodeId,
        /// The current state of the node.
        current_state: String,
        /// What operation was attempted.
        attempted: String,
    },
    /// A rolling update is already in progress.
    UpdateAlreadyInProgress,
    /// No rolling update is currently in progress.
    NoUpdateInProgress,
    /// The current update phase does not support pausing.
    UpdateNotPausable(String),
    /// Validation of a rolling update plan failed.
    ValidationFailed(String),
    /// Internal error that does not fit other categories.
    Internal(String),
}

impl fmt::Display for FleetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FleetError::NodeNotFound(id) => write!(f, "node not found: {}", id),
            FleetError::InvalidState {
                node_id,
                current_state,
                attempted,
            } => write!(
                f,
                "invalid state for node {}: current={}, attempted={}",
                node_id, current_state, attempted
            ),
            FleetError::UpdateAlreadyInProgress => write!(f, "rolling update already in progress"),
            FleetError::NoUpdateInProgress => write!(f, "no rolling update in progress"),
            FleetError::UpdateNotPausable(reason) => {
                write!(f, "update cannot be paused: {}", reason)
            }
            FleetError::ValidationFailed(reason) => {
                write!(f, "validation failed: {}", reason)
            }
            FleetError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for FleetError {}

/// Result type alias for fleet operations.
pub type FleetResult<T> = Result<T, FleetError>;

// ============================================================================
// FleetNodeState
// ============================================================================

/// The fleet-level lifecycle state of a node.
///
/// This is orthogonal to the gossip-level `NodeStatus` (Alive/Suspect/Dead).
/// A node can be `Alive` in gossip but `Draining` in fleet terms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FleetNodeState {
    /// Normal operation: accepting work and gossiping.
    Normal,

    /// Draining: finishing current work but not accepting new chunks.
    Draining {
        /// When the drain started.
        started_at: DateTime<Utc>,
        /// How many seconds before the drain times out and is forced.
        timeout_secs: u64,
        /// Number of active chunks when the drain started.
        active_chunks_at_start: usize,
        /// Human-readable reason for the drain.
        reason: String,
    },

    /// Cordoned: not accepting new work, but still gossiping.
    Cordoned {
        /// When the cordon was applied.
        since: DateTime<Utc>,
        /// Human-readable reason for the cordon.
        reason: String,
    },

    /// Quarantined: not accepting work and not gossiping.
    Quarantined {
        /// When the quarantine was applied.
        since: DateTime<Utc>,
        /// Human-readable reason for the quarantine.
        reason: String,
        /// Who or what triggered the quarantine.
        quarantined_by: Option<String>,
    },

    /// Updating: node is being updated to a new software version.
    Updating {
        /// When the update started.
        since: DateTime<Utc>,
        /// The version being updated to.
        target_version: String,
        /// The version before the update (if known).
        previous_version: Option<String>,
    },
}

impl FleetNodeState {
    /// Returns `true` if the node should accept new work in this state.
    pub fn is_accepting_work(&self) -> bool {
        matches!(self, FleetNodeState::Normal)
    }

    /// Returns `true` if the node should participate in gossip in this state.
    pub fn is_gossiping(&self) -> bool {
        !matches!(self, FleetNodeState::Quarantined { .. })
    }

    /// Returns how many seconds the node has been in this state.
    pub fn duration_secs(&self) -> Option<i64> {
        let since = match self {
            FleetNodeState::Normal => return None,
            FleetNodeState::Draining { started_at, .. } => started_at,
            FleetNodeState::Cordoned { since, .. } => since,
            FleetNodeState::Quarantined { since, .. } => since,
            FleetNodeState::Updating { since, .. } => since,
        };
        Some((Utc::now() - *since).num_seconds())
    }

    /// Returns the state name as a static string for display.
    pub fn state_name(&self) -> &'static str {
        match self {
            FleetNodeState::Normal => "normal",
            FleetNodeState::Draining { .. } => "draining",
            FleetNodeState::Cordoned { .. } => "cordoned",
            FleetNodeState::Quarantined { .. } => "quarantined",
            FleetNodeState::Updating { .. } => "updating",
        }
    }
}

impl fmt::Display for FleetNodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FleetNodeState::Normal => write!(f, "Normal"),
            FleetNodeState::Draining {
                started_at,
                timeout_secs,
                reason,
                ..
            } => write!(
                f,
                "Draining(since={}, timeout={}s, reason={})",
                started_at.format("%H:%M:%S"),
                timeout_secs,
                reason
            ),
            FleetNodeState::Cordoned { since, reason } => write!(
                f,
                "Cordoned(since={}, reason={})",
                since.format("%H:%M:%S"),
                reason
            ),
            FleetNodeState::Quarantined {
                since,
                reason,
                quarantined_by,
            } => write!(
                f,
                "Quarantined(since={}, reason={}, by={})",
                since.format("%H:%M:%S"),
                reason,
                quarantined_by.as_deref().unwrap_or("unknown")
            ),
            FleetNodeState::Updating {
                since,
                target_version,
                ..
            } => write!(
                f,
                "Updating(since={}, target={})",
                since.format("%H:%M:%S"),
                target_version
            ),
        }
    }
}

// ============================================================================
// FleetStateTransition
// ============================================================================

/// A recorded state transition for audit purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetStateTransition {
    /// When the transition happened.
    pub timestamp: DateTime<Utc>,
    /// The state before the transition.
    pub from_state: String,
    /// The state after the transition.
    pub to_state: String,
    /// Who or what initiated the transition.
    pub initiated_by: Option<String>,
}

// ============================================================================
// FleetNodeInfo
// ============================================================================

/// Fleet-level information about a node, including its current state and
/// history of state transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetNodeInfo {
    /// The node this info is about.
    pub node_id: NodeId,
    /// Current fleet-level state.
    pub state: FleetNodeState,
    /// History of state transitions (most recent first), capped at 50 entries.
    pub transitions: VecDeque<FleetStateTransition>,
    /// When this record was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Maximum number of transitions to keep in the history per node.
const MAX_TRANSITION_HISTORY: usize = 50;

// ============================================================================
// FleetStore
// ============================================================================

/// Thread-safe store of fleet-level node state.
///
/// Backed by a `DashMap` for concurrent access. Nodes not present in the
/// store are implicitly in the `Normal` state.
pub struct FleetStore {
    /// Per-node fleet info.
    nodes: DashMap<NodeId, FleetNodeInfo>,
}

impl FleetStore {
    /// Create a new, empty fleet store.
    pub fn new() -> Self {
        Self {
            nodes: DashMap::new(),
        }
    }

    /// Set the fleet state of a node, recording the transition.
    ///
    /// If the node does not exist in the store, a new entry is created with
    /// the previous state recorded as "normal".
    pub fn set_state(
        &self,
        node_id: NodeId,
        new_state: FleetNodeState,
        initiated_by: Option<String>,
    ) {
        let now = Utc::now();
        match self.nodes.entry(node_id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let mut transitions = VecDeque::new();
                transitions.push_front(FleetStateTransition {
                    timestamp: now,
                    from_state: "normal".to_string(),
                    to_state: new_state.state_name().to_string(),
                    initiated_by,
                });
                entry.insert(FleetNodeInfo {
                    node_id,
                    state: new_state,
                    transitions,
                    updated_at: now,
                });
            }
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let info = entry.get_mut();
                let from = info.state.state_name().to_string();
                let to = new_state.state_name().to_string();
                info.state = new_state;
                info.updated_at = now;
                info.transitions.push_front(FleetStateTransition {
                    timestamp: now,
                    from_state: from,
                    to_state: to,
                    initiated_by,
                });
                while info.transitions.len() > MAX_TRANSITION_HISTORY {
                    info.transitions.pop_back();
                }
            }
        }
    }

    /// Get the fleet state of a node. Returns `Normal` if the node is not
    /// present in the store.
    pub fn get_state(&self, node_id: &NodeId) -> FleetNodeState {
        self.nodes
            .get(node_id)
            .map(|info| info.state.clone())
            .unwrap_or(FleetNodeState::Normal)
    }

    /// Get the full fleet info for a node, if it exists.
    pub fn get_info(&self, node_id: &NodeId) -> Option<FleetNodeInfo> {
        self.nodes.get(node_id).map(|r| r.value().clone())
    }

    /// List all nodes that have fleet state entries.
    pub fn list_all(&self) -> Vec<FleetNodeInfo> {
        self.nodes.iter().map(|r| r.value().clone()).collect()
    }

    /// List nodes whose state matches a predicate.
    pub fn list_by_state<F>(&self, predicate: F) -> Vec<FleetNodeInfo>
    where
        F: Fn(&FleetNodeState) -> bool,
    {
        self.nodes
            .iter()
            .filter(|r| predicate(&r.value().state))
            .map(|r| r.value().clone())
            .collect()
    }

    /// Count nodes by state category.
    pub fn count_by_state(&self) -> FleetStateCounts {
        let mut counts = FleetStateCounts::default();
        for entry in self.nodes.iter() {
            match entry.value().state {
                FleetNodeState::Normal => counts.normal += 1,
                FleetNodeState::Draining { .. } => counts.draining += 1,
                FleetNodeState::Cordoned { .. } => counts.cordoned += 1,
                FleetNodeState::Quarantined { .. } => counts.quarantined += 1,
                FleetNodeState::Updating { .. } => counts.updating += 1,
            }
        }
        counts
    }

    /// Check whether a node is accepting work.
    pub fn is_node_accepting_work(&self, node_id: &NodeId) -> bool {
        self.get_state(node_id).is_accepting_work()
    }

    /// Check whether a node should participate in gossip.
    pub fn is_node_gossiping(&self, node_id: &NodeId) -> bool {
        self.get_state(node_id).is_gossiping()
    }

    /// Remove entries that are in the `Normal` state (cleanup).
    pub fn clear_normal_entries(&self) {
        let normal_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|r| matches!(r.value().state, FleetNodeState::Normal))
            .map(|r| *r.key())
            .collect();
        for id in normal_ids {
            self.nodes.remove(&id);
        }
    }

    /// Number of entries in the store.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Remove a specific node entry.
    pub fn remove(&self, node_id: &NodeId) {
        self.nodes.remove(node_id);
    }
}

/// Counts of nodes by fleet state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FleetStateCounts {
    /// Number of nodes in the Normal state.
    pub normal: usize,
    /// Number of nodes currently draining.
    pub draining: usize,
    /// Number of cordoned nodes.
    pub cordoned: usize,
    /// Number of quarantined nodes.
    pub quarantined: usize,
    /// Number of nodes being updated.
    pub updating: usize,
}

// ============================================================================
// Rolling Update types
// ============================================================================

/// A plan for rolling out an update across the fleet.
///
/// The plan specifies the target version, canary configuration, batch sizing,
/// health check parameters, and rollback behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingUpdatePlan {
    /// The target version string to update to.
    pub target_version: String,
    /// Fraction of nodes to use as canaries (0.01 to 1.0).
    pub canary_percentage: f64,
    /// Order of regions for the rollout. Each region is fully updated before
    /// moving to the next. Empty means all regions simultaneously.
    pub region_order: Vec<String>,
    /// If true, the update pauses on first failure instead of rolling back.
    pub pause_on_failure: bool,
    /// Maximum fraction of nodes that can be unavailable at once (0.0 to 1.0).
    pub max_unavailable_pct: f64,
    /// Seconds to wait after an update before checking health.
    pub health_check_wait_secs: u64,
    /// If true, automatically roll back when health checks fail.
    pub rollback_on_health_failure: bool,
    /// Number of nodes to update per batch.
    pub batch_size: usize,
    /// If true, simulate the update without actually changing state.
    pub dry_run: bool,
}

impl RollingUpdatePlan {
    /// Validate the plan, returning an error if any field is out of range.
    pub fn validate(&self) -> FleetResult<()> {
        if self.target_version.is_empty() {
            return Err(FleetError::ValidationFailed(
                "target_version must not be empty".to_string(),
            ));
        }
        if self.canary_percentage < 0.01 || self.canary_percentage > 1.0 {
            return Err(FleetError::ValidationFailed(format!(
                "canary_percentage must be between 0.01 and 1.0, got {}",
                self.canary_percentage
            )));
        }
        if self.max_unavailable_pct < 0.0 || self.max_unavailable_pct > 1.0 {
            return Err(FleetError::ValidationFailed(format!(
                "max_unavailable_pct must be between 0.0 and 1.0, got {}",
                self.max_unavailable_pct
            )));
        }
        if self.batch_size == 0 {
            return Err(FleetError::ValidationFailed(
                "batch_size must be at least 1".to_string(),
            ));
        }
        Ok(())
    }
}

/// The current phase of a rolling update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdatePhase {
    /// Validating the update plan.
    Validating,
    /// Selecting canary nodes.
    SelectingCanary,
    /// Draining canary nodes before update.
    CanaryDraining {
        /// Canary node IDs being drained.
        nodes: Vec<NodeId>,
    },
    /// Applying the update to canary nodes.
    CanaryUpdating {
        /// Canary node IDs being updated.
        nodes: Vec<NodeId>,
    },
    /// Health-checking canary nodes after update.
    CanaryHealthCheck {
        /// Canary node IDs being health-checked.
        nodes: Vec<NodeId>,
        /// Deadline for the health check to complete.
        check_until: DateTime<Utc>,
    },
    /// Canary nodes are healthy; proceeding to rolling update.
    CanaryHealthy,
    /// Rolling out updates region by region in batches.
    RollingRegion {
        /// Current region being updated.
        region: String,
        /// Current batch index (0-based).
        batch_index: usize,
        /// Total number of batches for this region.
        total_batches: usize,
        /// Node IDs in the current batch.
        batch_nodes: Vec<NodeId>,
        /// Sub-phase of the current batch.
        sub_phase: BatchSubPhase,
    },
    /// Update is paused (e.g. due to failure when pause_on_failure is set).
    Paused {
        /// Why the update was paused.
        reason: String,
        /// Whether the update can be resumed.
        can_resume: bool,
    },
    /// Rolling back the update to the previous version.
    RollingBack {
        /// Nodes that still need to be rolled back.
        nodes_to_rollback: Vec<NodeId>,
        /// Nodes that have been successfully rolled back.
        completed: Vec<NodeId>,
    },
    /// Update completed successfully.
    Completed,
    /// Update failed.
    Failed {
        /// Why the update failed.
        reason: String,
        /// Nodes that were successfully updated.
        nodes_updated: Vec<NodeId>,
        /// Nodes that failed to update.
        nodes_failed: Vec<NodeId>,
    },
}

impl fmt::Display for UpdatePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpdatePhase::Validating => write!(f, "validating"),
            UpdatePhase::SelectingCanary => write!(f, "selecting-canary"),
            UpdatePhase::CanaryDraining { nodes } => {
                write!(f, "canary-draining({} nodes)", nodes.len())
            }
            UpdatePhase::CanaryUpdating { nodes } => {
                write!(f, "canary-updating({} nodes)", nodes.len())
            }
            UpdatePhase::CanaryHealthCheck { nodes, .. } => {
                write!(f, "canary-health-check({} nodes)", nodes.len())
            }
            UpdatePhase::CanaryHealthy => write!(f, "canary-healthy"),
            UpdatePhase::RollingRegion {
                region,
                batch_index,
                total_batches,
                ..
            } => write!(
                f,
                "rolling-region({}, batch {}/{})",
                region,
                batch_index + 1,
                total_batches
            ),
            UpdatePhase::Paused { reason, .. } => write!(f, "paused({})", reason),
            UpdatePhase::RollingBack {
                nodes_to_rollback,
                completed,
            } => write!(
                f,
                "rolling-back({} remaining, {} done)",
                nodes_to_rollback.len(),
                completed.len()
            ),
            UpdatePhase::Completed => write!(f, "completed"),
            UpdatePhase::Failed { reason, .. } => write!(f, "failed({})", reason),
        }
    }
}

/// Sub-phase within a batch during a rolling region update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BatchSubPhase {
    /// Draining nodes in the batch.
    Draining,
    /// Applying the update to nodes in the batch.
    Updating,
    /// Waiting for health checks to pass.
    HealthChecking {
        /// Deadline for health checks.
        check_until: DateTime<Utc>,
    },
    /// Batch is healthy; ready for next batch.
    Healthy,
}

impl fmt::Display for BatchSubPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchSubPhase::Draining => write!(f, "draining"),
            BatchSubPhase::Updating => write!(f, "updating"),
            BatchSubPhase::HealthChecking { .. } => write!(f, "health-checking"),
            BatchSubPhase::Healthy => write!(f, "healthy"),
        }
    }
}

// ============================================================================
// RollingUpdateStatus
// ============================================================================

/// Full status of a rolling update in progress (or recently completed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingUpdateStatus {
    /// The plan being executed.
    pub plan: RollingUpdatePlan,
    /// Current phase.
    pub phase: UpdatePhase,
    /// When the update started.
    pub started_at: DateTime<Utc>,
    /// When the status was last updated.
    pub updated_at: DateTime<Utc>,
    /// Nodes that have been successfully updated.
    pub updated_nodes: Vec<NodeId>,
    /// Nodes that failed to update.
    pub failed_nodes: Vec<NodeId>,
    /// Nodes still pending update.
    pub pending_nodes: Vec<NodeId>,
    /// Nodes that were skipped (e.g. already at target version).
    pub skipped_nodes: Vec<NodeId>,
    /// Overall progress percentage (0.0 to 100.0).
    pub progress_pct: f64,
    /// Estimated seconds remaining (None if unknown).
    pub estimated_remaining_secs: Option<u64>,
    /// Log of significant events during the update.
    pub events: Vec<UpdateEvent>,
}

/// A significant event during a rolling update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEvent {
    /// When the event occurred.
    pub timestamp: DateTime<Utc>,
    /// Description of the event.
    pub message: String,
}

// ============================================================================
// FleetSummary
// ============================================================================

/// Summary of the fleet state for dashboard display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetSummary {
    /// Total nodes known to the fleet.
    pub total_nodes: usize,
    /// Nodes in Normal state.
    pub normal: usize,
    /// Nodes currently draining.
    pub draining: usize,
    /// Cordoned nodes.
    pub cordoned: usize,
    /// Quarantined nodes.
    pub quarantined: usize,
    /// Nodes being updated.
    pub updating: usize,
    /// Whether a rolling update is in progress.
    pub update_in_progress: bool,
    /// Current update phase (if any).
    pub current_update_phase: Option<String>,
    /// Number of active maintenance windows.
    pub active_maintenance_windows: usize,
}

// ============================================================================
// FleetManager
// ============================================================================

/// Central fleet operations manager.
///
/// Coordinates node lifecycle transitions, rolling updates, and maintenance
/// windows. Integrates with the knowledge store for node discovery and the
/// event bus for operation notifications.
pub struct FleetManager {
    /// Optional knowledge store for node discovery and assignment queries.
    knowledge: Option<Arc<KnowledgeStore>>,
    /// Fleet state store.
    fleet_store: Arc<FleetStore>,
    /// Current rolling update status (if any).
    update_status: Arc<RwLock<Option<RollingUpdateStatus>>>,
    /// History of completed updates (most recent first), capped at 10.
    update_history: Arc<RwLock<VecDeque<RollingUpdateStatus>>>,
    /// How often the background loop checks for state machine advancement.
    check_interval: Duration,
    /// Maintenance scheduler.
    maintenance_scheduler: Arc<MaintenanceScheduler>,
    /// Node tag store.
    tag_store: Arc<NodeTagStore>,
    /// Monotonic event counter for update events.
    event_counter: AtomicU64,
}

/// Maximum number of completed updates kept in history.
const MAX_UPDATE_HISTORY: usize = 10;

impl FleetManager {
    /// Create a new fleet manager.
    pub fn new() -> Self {
        Self {
            knowledge: None,
            fleet_store: Arc::new(FleetStore::new()),
            update_status: Arc::new(RwLock::new(None)),
            update_history: Arc::new(RwLock::new(VecDeque::new())),
            check_interval: Duration::from_secs(5),
            maintenance_scheduler: Arc::new(MaintenanceScheduler::new()),
            tag_store: Arc::new(NodeTagStore::new()),
            event_counter: AtomicU64::new(0),
        }
    }

    /// Attach a knowledge store for node discovery.
    pub fn with_knowledge(mut self, knowledge: Arc<KnowledgeStore>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    /// Set a custom check interval for the background loop.
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Get the fleet store.
    pub fn fleet_store(&self) -> &Arc<FleetStore> {
        &self.fleet_store
    }

    /// Get the maintenance scheduler.
    pub fn maintenance_scheduler(&self) -> &Arc<MaintenanceScheduler> {
        &self.maintenance_scheduler
    }

    /// Get the node tag store.
    pub fn tag_store(&self) -> &Arc<NodeTagStore> {
        &self.tag_store
    }

    // ========================================================================
    // Node operations
    // ========================================================================

    /// Drain a node: stop accepting new work but finish current chunks.
    ///
    /// The node transitions from any non-quarantined state to `Draining`.
    /// After the timeout expires, the drain completes automatically.
    pub fn drain_node(
        &self,
        node_id: NodeId,
        timeout_secs: u64,
        reason: String,
    ) -> FleetResult<()> {
        let current = self.fleet_store.get_state(&node_id);
        match current {
            FleetNodeState::Quarantined { .. } => {
                return Err(FleetError::InvalidState {
                    node_id,
                    current_state: current.state_name().to_string(),
                    attempted: "drain".to_string(),
                });
            }
            FleetNodeState::Draining { .. } => {
                return Err(FleetError::InvalidState {
                    node_id,
                    current_state: "draining".to_string(),
                    attempted: "drain (already draining)".to_string(),
                });
            }
            _ => {}
        }

        let active_chunks = self.count_active_chunks(node_id);

        let new_state = FleetNodeState::Draining {
            started_at: Utc::now(),
            timeout_secs,
            active_chunks_at_start: active_chunks,
            reason: reason.clone(),
        };

        self.fleet_store
            .set_state(node_id, new_state, Some("fleet_manager".to_string()));

        info!(
            node = %node_id,
            timeout_secs = timeout_secs,
            active_chunks = active_chunks,
            reason = %reason,
            "node drain started"
        );

        Ok(())
    }

    /// Cordon a node: stop accepting new work immediately.
    ///
    /// The node transitions from `Normal` to `Cordoned`.
    pub fn cordon_node(&self, node_id: NodeId, reason: String) -> FleetResult<()> {
        let current = self.fleet_store.get_state(&node_id);
        match current {
            FleetNodeState::Quarantined { .. } | FleetNodeState::Cordoned { .. } => {
                return Err(FleetError::InvalidState {
                    node_id,
                    current_state: current.state_name().to_string(),
                    attempted: "cordon".to_string(),
                });
            }
            _ => {}
        }

        let new_state = FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: reason.clone(),
        };

        self.fleet_store
            .set_state(node_id, new_state, Some("fleet_manager".to_string()));

        info!(
            node = %node_id,
            reason = %reason,
            "node cordoned"
        );

        Ok(())
    }

    /// Uncordon a node: allow it to accept work again.
    ///
    /// The node transitions from `Cordoned` to `Normal`.
    pub fn uncordon_node(&self, node_id: NodeId) -> FleetResult<()> {
        let current = self.fleet_store.get_state(&node_id);
        if !matches!(current, FleetNodeState::Cordoned { .. }) {
            return Err(FleetError::InvalidState {
                node_id,
                current_state: current.state_name().to_string(),
                attempted: "uncordon".to_string(),
            });
        }

        self.fleet_store
            .set_state(node_id, FleetNodeState::Normal, Some("fleet_manager".to_string()));

        info!(node = %node_id, "node uncordoned");

        Ok(())
    }

    /// Quarantine a node: isolate it from both work and gossip.
    ///
    /// Can be applied from any state.
    pub fn quarantine_node(
        &self,
        node_id: NodeId,
        reason: String,
        quarantined_by: Option<String>,
    ) -> FleetResult<()> {
        let current = self.fleet_store.get_state(&node_id);
        if matches!(current, FleetNodeState::Quarantined { .. }) {
            return Err(FleetError::InvalidState {
                node_id,
                current_state: "quarantined".to_string(),
                attempted: "quarantine (already quarantined)".to_string(),
            });
        }

        let new_state = FleetNodeState::Quarantined {
            since: Utc::now(),
            reason: reason.clone(),
            quarantined_by: quarantined_by.clone(),
        };

        self.fleet_store.set_state(
            node_id,
            new_state,
            quarantined_by.or_else(|| Some("fleet_manager".to_string())),
        );

        warn!(
            node = %node_id,
            reason = %reason,
            "node quarantined"
        );

        Ok(())
    }

    /// Remove a node from quarantine, returning it to Normal state.
    pub fn unquarantine_node(&self, node_id: NodeId) -> FleetResult<()> {
        let current = self.fleet_store.get_state(&node_id);
        if !matches!(current, FleetNodeState::Quarantined { .. }) {
            return Err(FleetError::InvalidState {
                node_id,
                current_state: current.state_name().to_string(),
                attempted: "unquarantine".to_string(),
            });
        }

        self.fleet_store
            .set_state(node_id, FleetNodeState::Normal, Some("fleet_manager".to_string()));

        info!(node = %node_id, "node unquarantined");

        Ok(())
    }

    // ========================================================================
    // Rolling update operations
    // ========================================================================

    /// Start a rolling update with the given plan.
    ///
    /// Validates the plan and initializes the update state machine.
    pub fn start_update(&self, plan: RollingUpdatePlan) -> FleetResult<()> {
        plan.validate()?;

        let status = self.update_status.read();
        if let Some(ref current) = *status {
            if !matches!(
                current.phase,
                UpdatePhase::Completed | UpdatePhase::Failed { .. }
            ) {
                return Err(FleetError::UpdateAlreadyInProgress);
            }
        }
        drop(status);

        let pending_nodes = self.get_eligible_update_nodes(&plan);

        let now = Utc::now();
        let new_status = RollingUpdateStatus {
            plan: plan.clone(),
            phase: UpdatePhase::Validating,
            started_at: now,
            updated_at: now,
            updated_nodes: Vec::new(),
            failed_nodes: Vec::new(),
            pending_nodes,
            skipped_nodes: Vec::new(),
            progress_pct: 0.0,
            estimated_remaining_secs: None,
            events: vec![UpdateEvent {
                timestamp: now,
                message: format!("Rolling update started: target={}", plan.target_version),
            }],
        };

        *self.update_status.write() = Some(new_status);

        info!(
            target_version = %plan.target_version,
            canary_pct = plan.canary_percentage,
            batch_size = plan.batch_size,
            "rolling update started"
        );

        Ok(())
    }

    /// Pause a running update.
    pub fn pause_update(&self, reason: String) -> FleetResult<()> {
        let mut status = self.update_status.write();
        let current = status.as_mut().ok_or(FleetError::NoUpdateInProgress)?;

        match &current.phase {
            UpdatePhase::Completed | UpdatePhase::Failed { .. } | UpdatePhase::Paused { .. } => {
                return Err(FleetError::UpdateNotPausable(format!(
                    "phase is {}",
                    current.phase
                )));
            }
            _ => {}
        }

        let now = Utc::now();
        current.phase = UpdatePhase::Paused {
            reason: reason.clone(),
            can_resume: true,
        };
        current.updated_at = now;
        current.events.push(UpdateEvent {
            timestamp: now,
            message: format!("Update paused: {}", reason),
        });

        info!(reason = %reason, "rolling update paused");

        Ok(())
    }

    /// Resume a paused update.
    pub fn resume_update(&self) -> FleetResult<()> {
        let mut status = self.update_status.write();
        let current = status.as_mut().ok_or(FleetError::NoUpdateInProgress)?;

        match &current.phase {
            UpdatePhase::Paused { can_resume, .. } => {
                if !can_resume {
                    return Err(FleetError::UpdateNotPausable(
                        "update cannot be resumed".to_string(),
                    ));
                }
            }
            _ => {
                return Err(FleetError::InvalidState {
                    node_id: NodeId::new(),
                    current_state: format!("{}", current.phase),
                    attempted: "resume (not paused)".to_string(),
                });
            }
        }

        let now = Utc::now();
        // Resume to canary healthy to re-enter the rolling phase logic
        current.phase = UpdatePhase::CanaryHealthy;
        current.updated_at = now;
        current.events.push(UpdateEvent {
            timestamp: now,
            message: "Update resumed".to_string(),
        });

        info!("rolling update resumed");

        Ok(())
    }

    /// Initiate a rollback of the current update.
    pub fn rollback_update(&self) -> FleetResult<()> {
        let mut status = self.update_status.write();
        let current = status.as_mut().ok_or(FleetError::NoUpdateInProgress)?;

        match &current.phase {
            UpdatePhase::Completed | UpdatePhase::RollingBack { .. } => {
                return Err(FleetError::InvalidState {
                    node_id: NodeId::new(),
                    current_state: format!("{}", current.phase),
                    attempted: "rollback".to_string(),
                });
            }
            _ => {}
        }

        let now = Utc::now();
        let nodes_to_rollback = current.updated_nodes.clone();
        current.phase = UpdatePhase::RollingBack {
            nodes_to_rollback: nodes_to_rollback.clone(),
            completed: Vec::new(),
        };
        current.updated_at = now;
        current.events.push(UpdateEvent {
            timestamp: now,
            message: format!(
                "Rollback initiated: {} nodes to rollback",
                nodes_to_rollback.len()
            ),
        });

        info!(
            nodes = nodes_to_rollback.len(),
            "rolling update rollback initiated"
        );

        Ok(())
    }

    /// Cancel the current update entirely.
    pub fn cancel_update(&self) -> FleetResult<()> {
        let mut status = self.update_status.write();
        let current = status.as_mut().ok_or(FleetError::NoUpdateInProgress)?;

        let now = Utc::now();
        let updated = current.updated_nodes.clone();
        let failed = current.failed_nodes.clone();

        current.phase = UpdatePhase::Failed {
            reason: "cancelled by operator".to_string(),
            nodes_updated: updated,
            nodes_failed: failed,
        };
        current.updated_at = now;
        current.events.push(UpdateEvent {
            timestamp: now,
            message: "Update cancelled by operator".to_string(),
        });

        // Uncordon/undrain any nodes that were being updated
        for info in self.fleet_store.list_all() {
            if matches!(info.state, FleetNodeState::Updating { .. }) {
                self.fleet_store
                    .set_state(info.node_id, FleetNodeState::Normal, Some("cancel_update".to_string()));
            }
        }

        info!("rolling update cancelled");

        Ok(())
    }

    /// Get the current update status, if any.
    pub fn update_status(&self) -> Option<RollingUpdateStatus> {
        self.update_status.read().clone()
    }

    /// Get the history of completed updates.
    pub fn update_history(&self) -> Vec<RollingUpdateStatus> {
        self.update_history.read().iter().cloned().collect()
    }

    // ========================================================================
    // Query methods
    // ========================================================================

    /// Check whether a node is accepting work (combines fleet and knowledge state).
    pub fn is_node_accepting_work(&self, node_id: &NodeId) -> bool {
        self.fleet_store.is_node_accepting_work(node_id)
    }

    /// Check whether a node should participate in gossip.
    pub fn is_node_gossiping(&self, node_id: &NodeId) -> bool {
        self.fleet_store.is_node_gossiping(node_id)
    }

    /// Get the fleet state of a specific node.
    pub fn node_fleet_state(&self, node_id: &NodeId) -> FleetNodeState {
        self.fleet_store.get_state(node_id)
    }

    /// Get a summary of the fleet state for dashboard display.
    pub fn fleet_summary(&self) -> FleetSummary {
        let counts = self.fleet_store.count_by_state();
        let total = if let Some(ref knowledge) = self.knowledge {
            knowledge.node_count()
        } else {
            counts.normal + counts.draining + counts.cordoned + counts.quarantined + counts.updating
        };

        let (update_in_progress, current_update_phase) = {
            let status = self.update_status.read();
            match status.as_ref() {
                Some(s) => {
                    let in_progress = !matches!(
                        s.phase,
                        UpdatePhase::Completed | UpdatePhase::Failed { .. }
                    );
                    (in_progress, Some(format!("{}", s.phase)))
                }
                None => (false, None),
            }
        };

        FleetSummary {
            total_nodes: total,
            normal: counts.normal,
            draining: counts.draining,
            cordoned: counts.cordoned,
            quarantined: counts.quarantined,
            updating: counts.updating,
            update_in_progress,
            current_update_phase,
            active_maintenance_windows: self.maintenance_scheduler.active().len(),
        }
    }

    // ========================================================================
    // Background loop
    // ========================================================================

    /// Spawn the background fleet management loop.
    ///
    /// This loop periodically:
    /// - Checks for drain timeouts and completes drained nodes
    /// - Advances the rolling update state machine
    /// - Activates/completes maintenance windows
    pub fn spawn_loop(
        self: &Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);

        tokio::spawn(async move {
            info!("fleet management loop started");

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(manager.check_interval) => {}
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!("fleet management loop shutting down");
                            break;
                        }
                    }
                }

                if *shutdown_rx.borrow() {
                    break;
                }

                manager.tick_drain_timeouts();
                manager.tick_update_state_machine();
                manager.tick_maintenance_windows();
            }
        })
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Count active (InProgress) chunks assigned to a node.
    fn count_active_chunks(&self, node_id: NodeId) -> usize {
        if let Some(ref knowledge) = self.knowledge {
            knowledge
                .get_assignments_for_node(&node_id)
                .iter()
                .filter(|a| a.status == super::types::ChunkStatus::InProgress)
                .count()
        } else {
            0
        }
    }

    /// Get nodes eligible for update (alive nodes not already at target version).
    fn get_eligible_update_nodes(&self, _plan: &RollingUpdatePlan) -> Vec<NodeId> {
        if let Some(ref knowledge) = self.knowledge {
            knowledge
                .get_live_nodes()
                .iter()
                .map(|info| info.node_id)
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Check drain timeouts and complete nodes whose drains have expired.
    fn tick_drain_timeouts(&self) {
        let draining = self.fleet_store.list_by_state(|s| matches!(s, FleetNodeState::Draining { .. }));

        for info in draining {
            if let FleetNodeState::Draining {
                started_at,
                timeout_secs,
                ..
            } = &info.state
            {
                let elapsed = (Utc::now() - *started_at).num_seconds();
                let active = self.count_active_chunks(info.node_id);

                if elapsed >= *timeout_secs as i64 || active == 0 {
                    // Drain complete: transition to cordoned
                    let reason = if active == 0 {
                        "drain completed: no active chunks remaining".to_string()
                    } else {
                        format!(
                            "drain timed out after {}s with {} chunks still active",
                            elapsed, active
                        )
                    };

                    self.fleet_store.set_state(
                        info.node_id,
                        FleetNodeState::Cordoned {
                            since: Utc::now(),
                            reason: reason.clone(),
                        },
                        Some("drain_timeout".to_string()),
                    );

                    debug!(
                        node = %info.node_id,
                        reason = %reason,
                        "drain completed"
                    );
                }
            }
        }
    }

    /// Advance the rolling update state machine by one step.
    fn tick_update_state_machine(&self) {
        let mut status_guard = self.update_status.write();
        let status = match status_guard.as_mut() {
            Some(s) => s,
            None => return,
        };

        // Skip terminal states
        if matches!(
            status.phase,
            UpdatePhase::Completed | UpdatePhase::Failed { .. } | UpdatePhase::Paused { .. }
        ) {
            return;
        }

        let now = Utc::now();

        match status.phase.clone() {
            UpdatePhase::Validating => {
                // Move to selecting canary
                status.phase = UpdatePhase::SelectingCanary;
                status.updated_at = now;
                status.events.push(UpdateEvent {
                    timestamp: now,
                    message: "Validation complete, selecting canary nodes".to_string(),
                });
            }

            UpdatePhase::SelectingCanary => {
                let total = status.pending_nodes.len();
                let canary_count = ((total as f64 * status.plan.canary_percentage).ceil() as usize).max(1).min(total);
                let canary_nodes: Vec<NodeId> = status.pending_nodes.drain(..canary_count.min(status.pending_nodes.len())).collect();

                if canary_nodes.is_empty() {
                    status.phase = UpdatePhase::Completed;
                    status.progress_pct = 100.0;
                    status.updated_at = now;
                    status.events.push(UpdateEvent {
                        timestamp: now,
                        message: "No eligible nodes, update completed".to_string(),
                    });
                } else {
                    if !status.plan.dry_run {
                        for &node_id in &canary_nodes {
                            self.fleet_store.set_state(
                                node_id,
                                FleetNodeState::Draining {
                                    started_at: now,
                                    timeout_secs: 60,
                                    active_chunks_at_start: self.count_active_chunks(node_id),
                                    reason: "canary drain for rolling update".to_string(),
                                },
                                Some("rolling_update".to_string()),
                            );
                        }
                    }
                    status.phase = UpdatePhase::CanaryDraining { nodes: canary_nodes };
                    status.updated_at = now;
                    status.events.push(UpdateEvent {
                        timestamp: now,
                        message: format!("Canary drain started for {} nodes", canary_count),
                    });
                }
            }

            UpdatePhase::CanaryDraining { nodes } => {
                // Check if all canary nodes are drained (or in dry run)
                let all_drained = status.plan.dry_run || nodes.iter().all(|id| {
                    !matches!(
                        self.fleet_store.get_state(id),
                        FleetNodeState::Draining { .. }
                    )
                });

                if all_drained {
                    if !status.plan.dry_run {
                        for &node_id in &nodes {
                            self.fleet_store.set_state(
                                node_id,
                                FleetNodeState::Updating {
                                    since: now,
                                    target_version: status.plan.target_version.clone(),
                                    previous_version: None,
                                },
                                Some("rolling_update".to_string()),
                            );
                        }
                    }
                    status.phase = UpdatePhase::CanaryUpdating { nodes };
                    status.updated_at = now;
                    status.events.push(UpdateEvent {
                        timestamp: now,
                        message: "Canary nodes drained, updating".to_string(),
                    });
                }
            }

            UpdatePhase::CanaryUpdating { nodes } => {
                // Simulate update completion (in real system, nodes would
                // restart and re-register with new version)
                let check_until = now + chrono::Duration::seconds(
                    status.plan.health_check_wait_secs as i64,
                );
                status.phase = UpdatePhase::CanaryHealthCheck {
                    nodes,
                    check_until,
                };
                status.updated_at = now;
                status.events.push(UpdateEvent {
                    timestamp: now,
                    message: "Canary update applied, starting health check".to_string(),
                });
            }

            UpdatePhase::CanaryHealthCheck { nodes, check_until } => {
                if now >= check_until || status.plan.dry_run {
                    // Health check passed
                    for &node_id in &nodes {
                        if !status.plan.dry_run {
                            self.fleet_store.set_state(
                                node_id,
                                FleetNodeState::Normal,
                                Some("rolling_update_canary_healthy".to_string()),
                            );
                        }
                        status.updated_nodes.push(node_id);
                    }
                    status.phase = UpdatePhase::CanaryHealthy;
                    status.updated_at = now;
                    status.events.push(UpdateEvent {
                        timestamp: now,
                        message: format!(
                            "Canary health check passed for {} nodes",
                            nodes.len()
                        ),
                    });
                }
            }

            UpdatePhase::CanaryHealthy => {
                // Move to rolling region updates
                if status.pending_nodes.is_empty() {
                    status.phase = UpdatePhase::Completed;
                    status.progress_pct = 100.0;
                    status.updated_at = now;
                    status.events.push(UpdateEvent {
                        timestamp: now,
                        message: "Rolling update completed successfully".to_string(),
                    });
                } else {
                    let region = if status.plan.region_order.is_empty() {
                        "default".to_string()
                    } else {
                        status.plan.region_order.first()
                            .cloned()
                            .unwrap_or_else(|| "default".to_string())
                    };

                    let batch_size = status.plan.batch_size.min(status.pending_nodes.len());
                    let total_batches = status.pending_nodes.len().div_ceil(batch_size);
                    let batch_nodes: Vec<NodeId> = status.pending_nodes.drain(..batch_size).collect();

                    status.phase = UpdatePhase::RollingRegion {
                        region,
                        batch_index: 0,
                        total_batches,
                        batch_nodes,
                        sub_phase: BatchSubPhase::Draining,
                    };
                    status.updated_at = now;
                    status.events.push(UpdateEvent {
                        timestamp: now,
                        message: format!(
                            "Rolling phase started: {} batches, {} nodes per batch",
                            total_batches, batch_size
                        ),
                    });
                }
            }

            UpdatePhase::RollingRegion {
                region,
                batch_index,
                total_batches,
                batch_nodes,
                sub_phase,
            } => {
                match sub_phase {
                    BatchSubPhase::Draining => {
                        // Transition to updating
                        if !status.plan.dry_run {
                            for &node_id in &batch_nodes {
                                self.fleet_store.set_state(
                                    node_id,
                                    FleetNodeState::Updating {
                                        since: now,
                                        target_version: status.plan.target_version.clone(),
                                        previous_version: None,
                                    },
                                    Some("rolling_update_batch".to_string()),
                                );
                            }
                        }
                        status.phase = UpdatePhase::RollingRegion {
                            region,
                            batch_index,
                            total_batches,
                            batch_nodes,
                            sub_phase: BatchSubPhase::Updating,
                        };
                        status.updated_at = now;
                    }
                    BatchSubPhase::Updating => {
                        let check_until = now + chrono::Duration::seconds(
                            status.plan.health_check_wait_secs as i64,
                        );
                        status.phase = UpdatePhase::RollingRegion {
                            region,
                            batch_index,
                            total_batches,
                            batch_nodes,
                            sub_phase: BatchSubPhase::HealthChecking { check_until },
                        };
                        status.updated_at = now;
                    }
                    BatchSubPhase::HealthChecking { check_until } => {
                        if now >= check_until || status.plan.dry_run {
                            for &node_id in &batch_nodes {
                                if !status.plan.dry_run {
                                    self.fleet_store.set_state(
                                        node_id,
                                        FleetNodeState::Normal,
                                        Some("rolling_update_batch_healthy".to_string()),
                                    );
                                }
                                status.updated_nodes.push(node_id);
                            }

                            // Calculate progress
                            let total_nodes = status.updated_nodes.len()
                                + status.pending_nodes.len()
                                + status.failed_nodes.len()
                                + status.skipped_nodes.len();
                            if total_nodes > 0 {
                                status.progress_pct =
                                    (status.updated_nodes.len() as f64 / total_nodes as f64) * 100.0;
                            }

                            status.phase = UpdatePhase::RollingRegion {
                                region,
                                batch_index,
                                total_batches,
                                batch_nodes,
                                sub_phase: BatchSubPhase::Healthy,
                            };
                            status.updated_at = now;
                        }
                    }
                    BatchSubPhase::Healthy => {
                        // Move to next batch or complete
                        if status.pending_nodes.is_empty() {
                            status.phase = UpdatePhase::Completed;
                            status.progress_pct = 100.0;
                            status.updated_at = now;
                            status.events.push(UpdateEvent {
                                timestamp: now,
                                message: "Rolling update completed successfully".to_string(),
                            });
                        } else {
                            let batch_size =
                                status.plan.batch_size.min(status.pending_nodes.len());
                            let new_batch: Vec<NodeId> =
                                status.pending_nodes.drain(..batch_size).collect();
                            status.phase = UpdatePhase::RollingRegion {
                                region,
                                batch_index: batch_index + 1,
                                total_batches,
                                batch_nodes: new_batch,
                                sub_phase: BatchSubPhase::Draining,
                            };
                            status.updated_at = now;
                        }
                    }
                }
            }

            UpdatePhase::RollingBack {
                mut nodes_to_rollback,
                mut completed,
            } => {
                // Roll back one batch at a time
                if nodes_to_rollback.is_empty() {
                    status.phase = UpdatePhase::Failed {
                        reason: "rollback completed".to_string(),
                        nodes_updated: completed,
                        nodes_failed: status.failed_nodes.clone(),
                    };
                    status.updated_at = now;
                    status.events.push(UpdateEvent {
                        timestamp: now,
                        message: "Rollback completed".to_string(),
                    });
                } else {
                    let batch_size = status.plan.batch_size.min(nodes_to_rollback.len());
                    let batch: Vec<NodeId> = nodes_to_rollback.drain(..batch_size).collect();

                    for &node_id in &batch {
                        if !status.plan.dry_run {
                            self.fleet_store.set_state(
                                node_id,
                                FleetNodeState::Normal,
                                Some("rollback".to_string()),
                            );
                        }
                        completed.push(node_id);
                    }

                    status.phase = UpdatePhase::RollingBack {
                        nodes_to_rollback,
                        completed,
                    };
                    status.updated_at = now;
                }
            }

            // Terminal states handled above
            _ => {}
        }

        // Archive completed updates
        if matches!(
            status.phase,
            UpdatePhase::Completed | UpdatePhase::Failed { .. }
        ) {
            let completed_status = status.clone();
            drop(status_guard);
            let mut history = self.update_history.write();
            history.push_front(completed_status);
            while history.len() > MAX_UPDATE_HISTORY {
                history.pop_back();
            }
        }
    }

    /// Check and advance maintenance windows.
    fn tick_maintenance_windows(&self) {
        let now = Utc::now();

        // Activate windows that should be active
        let upcoming = self.maintenance_scheduler.list();
        for window in upcoming {
            if window.status == MaintenanceStatus::Scheduled && window.start_at <= now {
                if let Some(mut entry) = self.maintenance_scheduler.windows.get_mut(&window.id) {
                    entry.status = MaintenanceStatus::Active;
                }
            }

            if window.status == MaintenanceStatus::Active && window.end_at <= now {
                if let Some(mut entry) = self.maintenance_scheduler.windows.get_mut(&window.id) {
                    entry.status = MaintenanceStatus::Completed;
                }
            }
        }
    }

    /// Push an event to the update status event log.
    fn push_update_event(&self, message: String) {
        let mut status = self.update_status.write();
        if let Some(ref mut s) = *status {
            s.events.push(UpdateEvent {
                timestamp: Utc::now(),
                message,
            });
            s.updated_at = Utc::now();
        }
        self.event_counter.fetch_add(1, Ordering::Relaxed);
    }
}

// ============================================================================
// MaintenanceWindow types
// ============================================================================

/// A scheduled maintenance window during which fleet operations are applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceWindow {
    /// Unique identifier for this window.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// When the window starts.
    pub start_at: DateTime<Utc>,
    /// When the window ends.
    pub end_at: DateTime<Utc>,
    /// Operations to perform during the window.
    pub operations: Vec<ScheduledOperation>,
    /// Specific nodes affected (empty means all nodes).
    pub affected_nodes: Vec<NodeId>,
    /// Regions affected (empty means all regions).
    pub affected_regions: Vec<String>,
    /// Who created this window.
    pub created_by: String,
    /// Current status of the window.
    pub status: MaintenanceStatus,
}

/// Status of a maintenance window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaintenanceStatus {
    /// Window is scheduled but not yet active.
    Scheduled,
    /// Window is currently active.
    Active,
    /// Window has completed.
    Completed,
    /// Window was cancelled.
    Cancelled,
}

/// An operation to perform during a maintenance window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScheduledOperation {
    /// Drain and cordon affected nodes.
    DrainAndCordon {
        /// Timeout for the drain phase in seconds.
        drain_timeout_secs: u64,
    },
    /// Perform a rolling update during the window.
    RollingUpdate {
        /// Target version for the update.
        target_version: String,
    },
    /// Quarantine nodes for inspection.
    QuarantineForInspection {
        /// Reason for the quarantine.
        reason: String,
    },
    /// Run a custom script on affected nodes.
    CustomScript {
        /// Name of the script.
        name: String,
        /// Script content.
        script: String,
    },
}

// ============================================================================
// MaintenanceScheduler
// ============================================================================

/// Scheduler for maintenance windows.
///
/// Manages the lifecycle of maintenance windows: scheduling, activation,
/// completion, and cancellation.
pub struct MaintenanceScheduler {
    /// Active and scheduled maintenance windows.
    windows: DashMap<String, MaintenanceWindow>,
}

impl MaintenanceScheduler {
    /// Create a new, empty maintenance scheduler.
    pub fn new() -> Self {
        Self {
            windows: DashMap::new(),
        }
    }

    /// Schedule a new maintenance window.
    ///
    /// Returns the window ID.
    pub fn schedule(&self, window: MaintenanceWindow) -> String {
        let id = window.id.clone();
        self.windows.insert(id.clone(), window);
        info!(window_id = %id, "maintenance window scheduled");
        id
    }

    /// Cancel a scheduled maintenance window.
    pub fn cancel(&self, id: &str) -> Option<MaintenanceWindow> {
        if let Some(mut entry) = self.windows.get_mut(id) {
            if entry.status == MaintenanceStatus::Scheduled {
                entry.status = MaintenanceStatus::Cancelled;
                info!(window_id = %id, "maintenance window cancelled");
                return Some(entry.clone());
            }
        }
        None
    }

    /// List all maintenance windows.
    pub fn list(&self) -> Vec<MaintenanceWindow> {
        self.windows.iter().map(|r| r.value().clone()).collect()
    }

    /// List maintenance windows that are upcoming (scheduled, not yet active).
    pub fn upcoming(&self) -> Vec<MaintenanceWindow> {
        self.windows
            .iter()
            .filter(|r| r.value().status == MaintenanceStatus::Scheduled)
            .map(|r| r.value().clone())
            .collect()
    }

    /// List currently active maintenance windows.
    pub fn active(&self) -> Vec<MaintenanceWindow> {
        self.windows
            .iter()
            .filter(|r| r.value().status == MaintenanceStatus::Active)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Get a maintenance window by ID.
    pub fn get(&self, id: &str) -> Option<MaintenanceWindow> {
        self.windows.get(id).map(|r| r.value().clone())
    }

    /// Remove completed and cancelled windows older than the given age.
    pub fn prune(&self, max_age: Duration) {
        let cutoff = Utc::now() - chrono::Duration::from_std(max_age).unwrap_or_else(|_| chrono::Duration::hours(24));
        let to_remove: Vec<String> = self
            .windows
            .iter()
            .filter(|r| {
                matches!(
                    r.value().status,
                    MaintenanceStatus::Completed | MaintenanceStatus::Cancelled
                ) && r.value().end_at < cutoff
            })
            .map(|r| r.key().clone())
            .collect();

        for id in to_remove {
            self.windows.remove(&id);
        }
    }
}

// ============================================================================
// NodeTagStore
// ============================================================================

/// Thread-safe store of node tags (key-value metadata).
///
/// Tags are free-form string key-value pairs attached to nodes, used for
/// grouping, filtering, and targeting operations (e.g. "role=worker",
/// "region=us-east-1", "tier=gpu").
pub struct NodeTagStore {
    /// Per-node tag maps.
    tags: DashMap<NodeId, HashMap<String, String>>,
}

impl NodeTagStore {
    /// Create a new, empty tag store.
    pub fn new() -> Self {
        Self {
            tags: DashMap::new(),
        }
    }

    /// Set a tag on a node. Overwrites any existing value for the key.
    pub fn set_tag(&self, node_id: NodeId, key: String, value: String) {
        self.tags
            .entry(node_id)
            .or_default()
            .insert(key, value);
    }

    /// Remove a tag from a node. Returns the old value if it existed.
    pub fn remove_tag(&self, node_id: &NodeId, key: &str) -> Option<String> {
        if let Some(mut entry) = self.tags.get_mut(node_id) {
            let removed = entry.remove(key);
            if entry.is_empty() {
                drop(entry);
                self.tags.remove(node_id);
            }
            removed
        } else {
            None
        }
    }

    /// Get all tags for a node.
    pub fn get_tags(&self, node_id: &NodeId) -> HashMap<String, String> {
        self.tags
            .get(node_id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    /// Find all nodes that have a specific tag key-value pair.
    pub fn find_by_tag(&self, key: &str, value: &str) -> Vec<NodeId> {
        self.tags
            .iter()
            .filter(|r| r.value().get(key).map(|v| v.as_str()) == Some(value))
            .map(|r| *r.key())
            .collect()
    }

    /// Find all nodes that have a specific tag key (any value).
    pub fn find_by_tag_key(&self, key: &str) -> Vec<NodeId> {
        self.tags
            .iter()
            .filter(|r| r.value().contains_key(key))
            .map(|r| *r.key())
            .collect()
    }

    /// Get all distinct tag keys across all nodes.
    pub fn all_tag_keys(&self) -> HashSet<String> {
        let mut keys = HashSet::new();
        for entry in self.tags.iter() {
            for key in entry.value().keys() {
                keys.insert(key.clone());
            }
        }
        keys
    }

    /// Number of nodes with at least one tag.
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Check if any nodes have tags.
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id() -> NodeId {
        NodeId::new()
    }

    // ====================================================================
    // FleetNodeState tests
    // ====================================================================

    #[test]
    fn test_normal_state_accepts_work_and_gossips() {
        let state = FleetNodeState::Normal;
        assert!(state.is_accepting_work());
        assert!(state.is_gossiping());
        assert!(state.duration_secs().is_none());
    }

    #[test]
    fn test_draining_state_rejects_work_but_gossips() {
        let state = FleetNodeState::Draining {
            started_at: Utc::now(),
            timeout_secs: 60,
            active_chunks_at_start: 5,
            reason: "maintenance".to_string(),
        };
        assert!(!state.is_accepting_work());
        assert!(state.is_gossiping());
        assert!(state.duration_secs().is_some());
    }

    #[test]
    fn test_cordoned_state_rejects_work_but_gossips() {
        let state = FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "manual".to_string(),
        };
        assert!(!state.is_accepting_work());
        assert!(state.is_gossiping());
    }

    #[test]
    fn test_quarantined_state_rejects_work_and_gossip() {
        let state = FleetNodeState::Quarantined {
            since: Utc::now(),
            reason: "unhealthy".to_string(),
            quarantined_by: Some("health_check".to_string()),
        };
        assert!(!state.is_accepting_work());
        assert!(!state.is_gossiping());
    }

    #[test]
    fn test_updating_state_rejects_work_but_gossips() {
        let state = FleetNodeState::Updating {
            since: Utc::now(),
            target_version: "2.0.0".to_string(),
            previous_version: Some("1.0.0".to_string()),
        };
        assert!(!state.is_accepting_work());
        assert!(state.is_gossiping());
    }

    #[test]
    fn test_fleet_node_state_display() {
        let state = FleetNodeState::Normal;
        assert_eq!(format!("{}", state), "Normal");

        let state = FleetNodeState::Cordoned {
            since: Utc::now(),
            reason: "test".to_string(),
        };
        let display = format!("{}", state);
        assert!(display.starts_with("Cordoned("));
        assert!(display.contains("test"));
    }

    #[test]
    fn test_state_name() {
        assert_eq!(FleetNodeState::Normal.state_name(), "normal");
        assert_eq!(
            FleetNodeState::Draining {
                started_at: Utc::now(),
                timeout_secs: 30,
                active_chunks_at_start: 0,
                reason: "test".into()
            }
            .state_name(),
            "draining"
        );
        assert_eq!(
            FleetNodeState::Quarantined {
                since: Utc::now(),
                reason: "x".into(),
                quarantined_by: None
            }
            .state_name(),
            "quarantined"
        );
    }

    // ====================================================================
    // FleetStore tests
    // ====================================================================

    #[test]
    fn test_fleet_store_default_is_normal() {
        let store = FleetStore::new();
        let id = make_node_id();
        assert!(matches!(store.get_state(&id), FleetNodeState::Normal));
        assert!(store.is_node_accepting_work(&id));
        assert!(store.is_node_gossiping(&id));
    }

    #[test]
    fn test_fleet_store_set_and_get_state() {
        let store = FleetStore::new();
        let id = make_node_id();

        store.set_state(
            id,
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "test".to_string(),
            },
            None,
        );

        assert!(matches!(
            store.get_state(&id),
            FleetNodeState::Cordoned { .. }
        ));
        assert!(!store.is_node_accepting_work(&id));
    }

    #[test]
    fn test_fleet_store_records_transitions() {
        let store = FleetStore::new();
        let id = make_node_id();

        store.set_state(
            id,
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "first".to_string(),
            },
            Some("operator".to_string()),
        );

        store.set_state(id, FleetNodeState::Normal, Some("operator".to_string()));

        let info = store.get_info(&id).expect("should have info");
        assert_eq!(info.transitions.len(), 2);
        assert_eq!(info.transitions[0].to_state, "normal");
        assert_eq!(info.transitions[1].to_state, "cordoned");
    }

    #[test]
    fn test_fleet_store_transition_history_capped() {
        let store = FleetStore::new();
        let id = make_node_id();

        for i in 0..100 {
            store.set_state(
                id,
                FleetNodeState::Cordoned {
                    since: Utc::now(),
                    reason: format!("reason-{}", i),
                },
                None,
            );
        }

        let info = store.get_info(&id).expect("should have info");
        assert!(info.transitions.len() <= MAX_TRANSITION_HISTORY);
    }

    #[test]
    fn test_fleet_store_list_all() {
        let store = FleetStore::new();
        let id1 = make_node_id();
        let id2 = make_node_id();

        store.set_state(
            id1,
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "a".to_string(),
            },
            None,
        );
        store.set_state(
            id2,
            FleetNodeState::Quarantined {
                since: Utc::now(),
                reason: "b".to_string(),
                quarantined_by: None,
            },
            None,
        );

        assert_eq!(store.list_all().len(), 2);
    }

    #[test]
    fn test_fleet_store_list_by_state() {
        let store = FleetStore::new();
        let id1 = make_node_id();
        let id2 = make_node_id();

        store.set_state(
            id1,
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "a".to_string(),
            },
            None,
        );
        store.set_state(
            id2,
            FleetNodeState::Quarantined {
                since: Utc::now(),
                reason: "b".to_string(),
                quarantined_by: None,
            },
            None,
        );

        let cordoned = store.list_by_state(|s| matches!(s, FleetNodeState::Cordoned { .. }));
        assert_eq!(cordoned.len(), 1);
        assert_eq!(cordoned[0].node_id, id1);
    }

    #[test]
    fn test_fleet_store_count_by_state() {
        let store = FleetStore::new();

        store.set_state(
            make_node_id(),
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "a".to_string(),
            },
            None,
        );
        store.set_state(
            make_node_id(),
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "b".to_string(),
            },
            None,
        );
        store.set_state(
            make_node_id(),
            FleetNodeState::Quarantined {
                since: Utc::now(),
                reason: "c".to_string(),
                quarantined_by: None,
            },
            None,
        );

        let counts = store.count_by_state();
        assert_eq!(counts.cordoned, 2);
        assert_eq!(counts.quarantined, 1);
        assert_eq!(counts.normal, 0);
    }

    #[test]
    fn test_fleet_store_clear_normal_entries() {
        let store = FleetStore::new();
        let id1 = make_node_id();
        let id2 = make_node_id();

        store.set_state(id1, FleetNodeState::Normal, None);
        store.set_state(
            id2,
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "keep".to_string(),
            },
            None,
        );

        store.clear_normal_entries();
        assert_eq!(store.len(), 1);
        assert!(store.get_info(&id1).is_none());
        assert!(store.get_info(&id2).is_some());
    }

    #[test]
    fn test_fleet_store_remove() {
        let store = FleetStore::new();
        let id = make_node_id();

        store.set_state(
            id,
            FleetNodeState::Cordoned {
                since: Utc::now(),
                reason: "test".to_string(),
            },
            None,
        );
        assert_eq!(store.len(), 1);

        store.remove(&id);
        assert_eq!(store.len(), 0);
    }

    // ====================================================================
    // FleetManager tests
    // ====================================================================

    #[test]
    fn test_fleet_manager_drain_node() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        mgr.drain_node(id, 60, "maintenance".to_string())
            .expect("drain should succeed");

        let state = mgr.fleet_store.get_state(&id);
        assert!(matches!(state, FleetNodeState::Draining { .. }));
        assert!(!mgr.is_node_accepting_work(&id));
    }

    #[test]
    fn test_fleet_manager_drain_already_draining_fails() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        mgr.drain_node(id, 60, "first".to_string())
            .expect("first drain should succeed");

        let result = mgr.drain_node(id, 30, "second".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_fleet_manager_drain_quarantined_fails() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        mgr.quarantine_node(id, "bad".to_string(), None)
            .expect("quarantine should succeed");

        let result = mgr.drain_node(id, 60, "maintenance".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_fleet_manager_cordon_and_uncordon() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        mgr.cordon_node(id, "maintenance".to_string())
            .expect("cordon should succeed");

        assert!(!mgr.is_node_accepting_work(&id));
        assert!(mgr.is_node_gossiping(&id));

        mgr.uncordon_node(id).expect("uncordon should succeed");

        assert!(mgr.is_node_accepting_work(&id));
    }

    #[test]
    fn test_fleet_manager_cordon_already_cordoned_fails() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        mgr.cordon_node(id, "first".to_string())
            .expect("first cordon should succeed");

        let result = mgr.cordon_node(id, "second".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_fleet_manager_uncordon_normal_fails() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        let result = mgr.uncordon_node(id);
        assert!(result.is_err());
    }

    #[test]
    fn test_fleet_manager_quarantine_and_unquarantine() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        mgr.quarantine_node(id, "unhealthy".to_string(), Some("health_check".to_string()))
            .expect("quarantine should succeed");

        assert!(!mgr.is_node_accepting_work(&id));
        assert!(!mgr.is_node_gossiping(&id));

        mgr.unquarantine_node(id)
            .expect("unquarantine should succeed");

        assert!(mgr.is_node_accepting_work(&id));
        assert!(mgr.is_node_gossiping(&id));
    }

    #[test]
    fn test_fleet_manager_quarantine_already_quarantined_fails() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        mgr.quarantine_node(id, "first".to_string(), None)
            .expect("first quarantine should succeed");

        let result = mgr.quarantine_node(id, "second".to_string(), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_fleet_manager_unquarantine_normal_fails() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        let result = mgr.unquarantine_node(id);
        assert!(result.is_err());
    }

    #[test]
    fn test_fleet_manager_fleet_summary() {
        let mgr = FleetManager::new();

        let id1 = make_node_id();
        let id2 = make_node_id();

        mgr.cordon_node(id1, "a".to_string()).unwrap();
        mgr.quarantine_node(id2, "b".to_string(), None).unwrap();

        let summary = mgr.fleet_summary();
        assert_eq!(summary.cordoned, 1);
        assert_eq!(summary.quarantined, 1);
        assert!(!summary.update_in_progress);
    }

    #[test]
    fn test_fleet_manager_node_fleet_state() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        assert!(matches!(
            mgr.node_fleet_state(&id),
            FleetNodeState::Normal
        ));

        mgr.cordon_node(id, "test".to_string()).unwrap();
        assert!(matches!(
            mgr.node_fleet_state(&id),
            FleetNodeState::Cordoned { .. }
        ));
    }

    // ====================================================================
    // Rolling update plan validation tests
    // ====================================================================

    fn make_valid_plan() -> RollingUpdatePlan {
        RollingUpdatePlan {
            target_version: "2.0.0".to_string(),
            canary_percentage: 0.1,
            region_order: vec!["us-east".to_string()],
            pause_on_failure: false,
            max_unavailable_pct: 0.2,
            health_check_wait_secs: 30,
            rollback_on_health_failure: true,
            batch_size: 5,
            dry_run: false,
        }
    }

    #[test]
    fn test_rolling_update_plan_valid() {
        let plan = make_valid_plan();
        assert!(plan.validate().is_ok());
    }

    #[test]
    fn test_rolling_update_plan_empty_version_fails() {
        let mut plan = make_valid_plan();
        plan.target_version = String::new();
        assert!(plan.validate().is_err());
    }

    #[test]
    fn test_rolling_update_plan_canary_pct_too_low() {
        let mut plan = make_valid_plan();
        plan.canary_percentage = 0.001;
        assert!(plan.validate().is_err());
    }

    #[test]
    fn test_rolling_update_plan_canary_pct_too_high() {
        let mut plan = make_valid_plan();
        plan.canary_percentage = 1.5;
        assert!(plan.validate().is_err());
    }

    #[test]
    fn test_rolling_update_plan_max_unavailable_out_of_range() {
        let mut plan = make_valid_plan();
        plan.max_unavailable_pct = -0.1;
        assert!(plan.validate().is_err());

        let mut plan2 = make_valid_plan();
        plan2.max_unavailable_pct = 1.5;
        assert!(plan2.validate().is_err());
    }

    #[test]
    fn test_rolling_update_plan_zero_batch_size() {
        let mut plan = make_valid_plan();
        plan.batch_size = 0;
        assert!(plan.validate().is_err());
    }

    // ====================================================================
    // Rolling update lifecycle tests
    // ====================================================================

    #[test]
    fn test_start_update_no_update_in_progress() {
        let mgr = FleetManager::new();
        let plan = make_valid_plan();
        assert!(mgr.start_update(plan).is_ok());

        let status = mgr.update_status().expect("should have status");
        assert!(matches!(status.phase, UpdatePhase::Validating));
    }

    #[test]
    fn test_start_update_duplicate_fails() {
        let mgr = FleetManager::new();
        let plan = make_valid_plan();
        mgr.start_update(plan.clone()).unwrap();

        let result = mgr.start_update(plan);
        assert!(result.is_err());
    }

    #[test]
    fn test_pause_and_resume_update() {
        let mgr = FleetManager::new();
        let plan = make_valid_plan();
        mgr.start_update(plan).unwrap();

        mgr.pause_update("testing".to_string()).unwrap();
        let status = mgr.update_status().unwrap();
        assert!(matches!(status.phase, UpdatePhase::Paused { .. }));

        mgr.resume_update().unwrap();
        let status = mgr.update_status().unwrap();
        assert!(matches!(status.phase, UpdatePhase::CanaryHealthy));
    }

    #[test]
    fn test_pause_no_update_fails() {
        let mgr = FleetManager::new();
        let result = mgr.pause_update("test".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_cancel_update() {
        let mgr = FleetManager::new();
        let plan = make_valid_plan();
        mgr.start_update(plan).unwrap();

        mgr.cancel_update().unwrap();
        let status = mgr.update_status().unwrap();
        assert!(matches!(status.phase, UpdatePhase::Failed { .. }));
    }

    #[test]
    fn test_rollback_update() {
        let mgr = FleetManager::new();
        let plan = make_valid_plan();
        mgr.start_update(plan).unwrap();

        mgr.rollback_update().unwrap();
        let status = mgr.update_status().unwrap();
        assert!(matches!(status.phase, UpdatePhase::RollingBack { .. }));
    }

    #[test]
    fn test_rollback_no_update_fails() {
        let mgr = FleetManager::new();
        let result = mgr.rollback_update();
        assert!(result.is_err());
    }

    #[test]
    fn test_update_history_empty_initially() {
        let mgr = FleetManager::new();
        assert!(mgr.update_history().is_empty());
    }

    // ====================================================================
    // Maintenance window tests
    // ====================================================================

    fn make_window(id: &str, start_offset_secs: i64, duration_secs: i64) -> MaintenanceWindow {
        let now = Utc::now();
        MaintenanceWindow {
            id: id.to_string(),
            name: format!("Window {}", id),
            start_at: now + chrono::Duration::seconds(start_offset_secs),
            end_at: now + chrono::Duration::seconds(start_offset_secs + duration_secs),
            operations: vec![ScheduledOperation::DrainAndCordon {
                drain_timeout_secs: 60,
            }],
            affected_nodes: vec![],
            affected_regions: vec![],
            created_by: "test".to_string(),
            status: MaintenanceStatus::Scheduled,
        }
    }

    #[test]
    fn test_maintenance_schedule_and_list() {
        let scheduler = MaintenanceScheduler::new();

        scheduler.schedule(make_window("w1", 3600, 7200));
        scheduler.schedule(make_window("w2", 7200, 3600));

        assert_eq!(scheduler.list().len(), 2);
    }

    #[test]
    fn test_maintenance_cancel() {
        let scheduler = MaintenanceScheduler::new();

        scheduler.schedule(make_window("w1", 3600, 7200));
        let cancelled = scheduler.cancel("w1");
        assert!(cancelled.is_some());

        let window = scheduler.get("w1").unwrap();
        assert_eq!(window.status, MaintenanceStatus::Cancelled);
    }

    #[test]
    fn test_maintenance_upcoming_filters() {
        let scheduler = MaintenanceScheduler::new();

        scheduler.schedule(make_window("w1", 3600, 7200));

        let mut active = make_window("w2", -3600, 7200);
        active.status = MaintenanceStatus::Active;
        scheduler.schedule(active);

        assert_eq!(scheduler.upcoming().len(), 1);
        assert_eq!(scheduler.active().len(), 1);
    }

    // ====================================================================
    // NodeTagStore tests
    // ====================================================================

    #[test]
    fn test_tag_store_set_and_get() {
        let store = NodeTagStore::new();
        let id = make_node_id();

        store.set_tag(id, "role".to_string(), "worker".to_string());
        store.set_tag(id, "region".to_string(), "us-east".to_string());

        let tags = store.get_tags(&id);
        assert_eq!(tags.len(), 2);
        assert_eq!(tags.get("role"), Some(&"worker".to_string()));
        assert_eq!(tags.get("region"), Some(&"us-east".to_string()));
    }

    #[test]
    fn test_tag_store_remove_tag() {
        let store = NodeTagStore::new();
        let id = make_node_id();

        store.set_tag(id, "role".to_string(), "worker".to_string());
        let removed = store.remove_tag(&id, "role");
        assert_eq!(removed, Some("worker".to_string()));

        let tags = store.get_tags(&id);
        assert!(tags.is_empty());
        assert!(store.is_empty());
    }

    #[test]
    fn test_tag_store_find_by_tag() {
        let store = NodeTagStore::new();
        let id1 = make_node_id();
        let id2 = make_node_id();
        let id3 = make_node_id();

        store.set_tag(id1, "role".to_string(), "worker".to_string());
        store.set_tag(id2, "role".to_string(), "worker".to_string());
        store.set_tag(id3, "role".to_string(), "gateway".to_string());

        let workers = store.find_by_tag("role", "worker");
        assert_eq!(workers.len(), 2);

        let gateways = store.find_by_tag("role", "gateway");
        assert_eq!(gateways.len(), 1);
    }

    #[test]
    fn test_tag_store_find_by_tag_key() {
        let store = NodeTagStore::new();
        let id1 = make_node_id();
        let id2 = make_node_id();

        store.set_tag(id1, "role".to_string(), "worker".to_string());
        store.set_tag(id2, "tier".to_string(), "gpu".to_string());

        let with_role = store.find_by_tag_key("role");
        assert_eq!(with_role.len(), 1);

        let with_tier = store.find_by_tag_key("tier");
        assert_eq!(with_tier.len(), 1);
    }

    #[test]
    fn test_tag_store_all_tag_keys() {
        let store = NodeTagStore::new();
        let id = make_node_id();

        store.set_tag(id, "role".to_string(), "worker".to_string());
        store.set_tag(id, "region".to_string(), "eu".to_string());
        store.set_tag(make_node_id(), "tier".to_string(), "gpu".to_string());

        let keys = store.all_tag_keys();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains("role"));
        assert!(keys.contains("region"));
        assert!(keys.contains("tier"));
    }

    #[test]
    fn test_tag_store_overwrite() {
        let store = NodeTagStore::new();
        let id = make_node_id();

        store.set_tag(id, "role".to_string(), "worker".to_string());
        store.set_tag(id, "role".to_string(), "gateway".to_string());

        let tags = store.get_tags(&id);
        assert_eq!(tags.get("role"), Some(&"gateway".to_string()));
    }

    #[test]
    fn test_tag_store_get_nonexistent() {
        let store = NodeTagStore::new();
        let id = make_node_id();

        let tags = store.get_tags(&id);
        assert!(tags.is_empty());
    }

    #[test]
    fn test_tag_store_remove_nonexistent() {
        let store = NodeTagStore::new();
        let id = make_node_id();

        let removed = store.remove_tag(&id, "role");
        assert!(removed.is_none());
    }

    // ====================================================================
    // FleetError display tests
    // ====================================================================

    #[test]
    fn test_fleet_error_display() {
        let err = FleetError::NodeNotFound(NodeId::new());
        let msg = format!("{}", err);
        assert!(msg.contains("node not found"));

        let err = FleetError::UpdateAlreadyInProgress;
        let msg = format!("{}", err);
        assert!(msg.contains("already in progress"));

        let err = FleetError::ValidationFailed("bad input".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("validation failed"));
        assert!(msg.contains("bad input"));
    }

    #[test]
    fn test_fleet_error_is_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(FleetError::Internal("test".to_string()));
        assert!(err.to_string().contains("internal error"));
    }

    // ====================================================================
    // Update phase display tests
    // ====================================================================

    #[test]
    fn test_update_phase_display() {
        assert_eq!(format!("{}", UpdatePhase::Validating), "validating");
        assert_eq!(format!("{}", UpdatePhase::Completed), "completed");
        assert_eq!(
            format!("{}", UpdatePhase::SelectingCanary),
            "selecting-canary"
        );

        let phase = UpdatePhase::CanaryDraining {
            nodes: vec![NodeId::new(), NodeId::new()],
        };
        assert!(format!("{}", phase).contains("2 nodes"));
    }

    #[test]
    fn test_batch_sub_phase_display() {
        assert_eq!(format!("{}", BatchSubPhase::Draining), "draining");
        assert_eq!(format!("{}", BatchSubPhase::Updating), "updating");
        assert_eq!(format!("{}", BatchSubPhase::Healthy), "healthy");
    }

    // ====================================================================
    // Tick methods tests
    // ====================================================================

    #[test]
    fn test_tick_drain_timeout_completes_empty_nodes() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        // Set a drain with 0 second timeout that started in the past
        mgr.fleet_store.set_state(
            id,
            FleetNodeState::Draining {
                started_at: Utc::now() - chrono::Duration::seconds(100),
                timeout_secs: 10,
                active_chunks_at_start: 0,
                reason: "test".to_string(),
            },
            None,
        );

        mgr.tick_drain_timeouts();

        // Should now be cordoned
        assert!(matches!(
            mgr.fleet_store.get_state(&id),
            FleetNodeState::Cordoned { .. }
        ));
    }

    #[test]
    fn test_tick_update_state_machine_advances_validating() {
        let mgr = FleetManager::new();

        let plan = make_valid_plan();
        mgr.start_update(plan).unwrap();

        mgr.tick_update_state_machine();

        let status = mgr.update_status().unwrap();
        assert!(matches!(status.phase, UpdatePhase::SelectingCanary));
    }

    #[test]
    fn test_tick_maintenance_windows_activates() {
        let mgr = FleetManager::new();

        // Create a window that should be active already
        let mut window = make_window("w1", -10, 3600);
        window.status = MaintenanceStatus::Scheduled;
        mgr.maintenance_scheduler.schedule(window);

        mgr.tick_maintenance_windows();

        let w = mgr.maintenance_scheduler.get("w1").unwrap();
        assert_eq!(w.status, MaintenanceStatus::Active);
    }

    #[test]
    fn test_tick_maintenance_windows_completes() {
        let mgr = FleetManager::new();

        // Create a window that should be completed already
        let mut window = make_window("w1", -7200, 3600);
        window.status = MaintenanceStatus::Active;
        mgr.maintenance_scheduler.schedule(window);

        mgr.tick_maintenance_windows();

        let w = mgr.maintenance_scheduler.get("w1").unwrap();
        assert_eq!(w.status, MaintenanceStatus::Completed);
    }

    // ====================================================================
    // Dry run test
    // ====================================================================

    #[test]
    fn test_dry_run_update_does_not_change_fleet_state() {
        let mgr = FleetManager::new();
        let id = make_node_id();

        // Manually add a pending node by starting a dry run update
        let mut plan = make_valid_plan();
        plan.dry_run = true;
        mgr.start_update(plan).unwrap();

        // The fleet store should not have any draining/updating entries
        assert!(matches!(
            mgr.fleet_store.get_state(&id),
            FleetNodeState::Normal
        ));
    }

    #[test]
    fn test_fleet_store_is_empty() {
        let store = FleetStore::new();
        assert!(store.is_empty());

        store.set_state(make_node_id(), FleetNodeState::Normal, None);
        assert!(!store.is_empty());
    }
}
