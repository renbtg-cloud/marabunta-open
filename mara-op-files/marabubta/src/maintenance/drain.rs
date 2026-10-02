// Marabunta - Licensed under the MIT License.
//! Node draining functionality for maintenance windows
//!
//! Provides graceful task migration and node draining capabilities
//! to prepare nodes for maintenance without losing task progress.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::common::types::{Task, TaskId, TaskStatus, WorkerId};
use crate::maintenance::types::{DrainInfo, DrainStatus, MaintenanceError, MaintenanceWindowId};

// ─────────────────────────────────────────────────────────────────────────────
// DRAIN EVENT
// ─────────────────────────────────────────────────────────────────────────────

/// Events emitted during drain operations
#[derive(Debug, Clone)]
pub enum DrainEvent {
    /// Drain started for a node
    DrainStarted {
        node_id: WorkerId,
        maintenance_id: MaintenanceWindowId,
        task_count: usize,
    },

    /// Task migrated from draining node
    TaskMigrated {
        node_id: WorkerId,
        task_id: TaskId,
        target_node: Option<WorkerId>,
    },

    /// Task completed on draining node
    TaskCompleted { node_id: WorkerId, task_id: TaskId },

    /// Task force-stopped due to timeout
    TaskForceStopped { node_id: WorkerId, task_id: TaskId },

    /// Drain completed for a node
    DrainCompleted {
        node_id: WorkerId,
        maintenance_id: MaintenanceWindowId,
        migrated_count: usize,
        completed_count: usize,
        force_stopped_count: usize,
    },

    /// Drain timed out
    DrainTimeout {
        node_id: WorkerId,
        maintenance_id: MaintenanceWindowId,
    },

    /// Drain cancelled
    DrainCancelled {
        node_id: WorkerId,
        maintenance_id: MaintenanceWindowId,
    },

    /// Drain failed
    DrainFailed {
        node_id: WorkerId,
        maintenance_id: MaintenanceWindowId,
        reason: String,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// DRAIN POLICY
// ─────────────────────────────────────────────────────────────────────────────

/// Policy for handling tasks during drain
#[derive(Debug, Clone)]
pub struct DrainPolicy {
    /// Default timeout for drain operations
    pub default_timeout: chrono::Duration,

    /// Whether to wait for running tasks to complete
    pub wait_for_running: bool,

    /// Whether to migrate pending tasks
    pub migrate_pending: bool,

    /// Whether to checkpoint running tasks before migration
    pub checkpoint_before_migrate: bool,

    /// Maximum time to wait for a task to checkpoint
    pub checkpoint_timeout: Duration,

    /// Whether to force-stop tasks on timeout
    pub force_on_timeout: bool,

    /// Grace period before force-stopping
    pub grace_period: Duration,

    /// Minimum node availability after drain (for rolling)
    pub min_remaining_nodes: usize,
}

impl Default for DrainPolicy {
    fn default() -> Self {
        Self {
            default_timeout: chrono::Duration::minutes(30),
            wait_for_running: true,
            migrate_pending: true,
            checkpoint_before_migrate: true,
            checkpoint_timeout: Duration::from_secs(60),
            force_on_timeout: false,
            grace_period: Duration::from_secs(30),
            min_remaining_nodes: 1,
        }
    }
}

impl DrainPolicy {
    /// Policy for emergency maintenance
    pub fn emergency() -> Self {
        Self {
            default_timeout: chrono::Duration::minutes(5),
            wait_for_running: false,
            migrate_pending: true,
            checkpoint_before_migrate: true,
            checkpoint_timeout: Duration::from_secs(30),
            force_on_timeout: true,
            grace_period: Duration::from_secs(10),
            min_remaining_nodes: 0,
        }
    }

    /// Policy for rolling maintenance
    pub fn rolling() -> Self {
        Self {
            default_timeout: chrono::Duration::minutes(15),
            wait_for_running: true,
            migrate_pending: true,
            checkpoint_before_migrate: true,
            checkpoint_timeout: Duration::from_secs(60),
            force_on_timeout: false,
            grace_period: Duration::from_secs(60),
            min_remaining_nodes: 1,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DRAIN REQUEST
// ─────────────────────────────────────────────────────────────────────────────

/// Request to drain a node
#[derive(Debug, Clone)]
pub struct DrainRequest {
    /// Node to drain
    pub node_id: WorkerId,

    /// Associated maintenance window
    pub maintenance_id: MaintenanceWindowId,

    /// Drain policy
    pub policy: DrainPolicy,

    /// Specific tasks to migrate (None = all tasks)
    pub tasks_to_migrate: Option<Vec<TaskId>>,

    /// Target nodes for migration (None = let scheduler decide)
    pub migration_targets: Option<Vec<WorkerId>>,
}

impl DrainRequest {
    /// Create a new drain request
    pub fn new(
        node_id: WorkerId,
        maintenance_id: MaintenanceWindowId,
        policy: DrainPolicy,
    ) -> Self {
        Self {
            node_id,
            maintenance_id,
            policy,
            tasks_to_migrate: None,
            migration_targets: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NODE DRAIN MANAGER
// ─────────────────────────────────────────────────────────────────────────────

/// Manages node drain operations
pub struct NodeDrainManager {
    /// Active drain operations by node
    active_drains: DashMap<WorkerId, DrainInfo>,

    /// Node statuses (for tracking)
    node_statuses: DashMap<WorkerId, DrainStatus>,

    /// Task assignments for tracking
    task_assignments: Arc<RwLock<HashMap<TaskId, WorkerId>>>,

    /// Event sender
    event_tx: mpsc::Sender<DrainEvent>,

    /// Default drain policy
    default_policy: DrainPolicy,

    /// Nodes excluded from scheduling (draining or drained)
    excluded_nodes: DashMap<WorkerId, MaintenanceWindowId>,
}

impl NodeDrainManager {
    /// Create a new drain manager
    pub fn new(event_tx: mpsc::Sender<DrainEvent>) -> Self {
        Self {
            active_drains: DashMap::new(),
            node_statuses: DashMap::new(),
            task_assignments: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            default_policy: DrainPolicy::default(),
            excluded_nodes: DashMap::new(),
        }
    }

    /// Create with custom default policy
    pub fn with_policy(event_tx: mpsc::Sender<DrainEvent>, policy: DrainPolicy) -> Self {
        Self {
            active_drains: DashMap::new(),
            node_statuses: DashMap::new(),
            task_assignments: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            default_policy: policy,
            excluded_nodes: DashMap::new(),
        }
    }

    /// Check if a node is being drained
    pub fn is_draining(&self, node_id: &WorkerId) -> bool {
        self.node_statuses
            .get(node_id)
            .map(|s| {
                matches!(
                    *s,
                    DrainStatus::Pending | DrainStatus::Draining | DrainStatus::WaitingForTasks
                )
            })
            .unwrap_or(false)
    }

    /// Check if a node is excluded from scheduling
    pub fn is_excluded(&self, node_id: &WorkerId) -> bool {
        self.excluded_nodes.contains_key(node_id)
    }

    /// Get drain status for a node
    pub fn get_drain_status(&self, node_id: &WorkerId) -> DrainStatus {
        self.node_statuses
            .get(node_id)
            .map(|s| *s)
            .unwrap_or(DrainStatus::NotDraining)
    }

    /// Get drain info for a node
    pub fn get_drain_info(&self, node_id: &WorkerId) -> Option<DrainInfo> {
        self.active_drains.get(node_id).map(|d| d.clone())
    }

    /// Get all actively draining nodes
    pub fn get_draining_nodes(&self) -> Vec<WorkerId> {
        self.active_drains
            .iter()
            .filter(|d| {
                matches!(
                    d.status,
                    DrainStatus::Draining | DrainStatus::WaitingForTasks
                )
            })
            .map(|d| d.node_id)
            .collect()
    }

    /// Get all excluded nodes
    pub fn get_excluded_nodes(&self) -> Vec<WorkerId> {
        self.excluded_nodes.iter().map(|e| *e.key()).collect()
    }

    /// Start draining a node
    pub async fn start_drain(
        &self,
        request: DrainRequest,
        current_tasks: Vec<TaskId>,
    ) -> Result<DrainInfo, MaintenanceError> {
        let node_id = request.node_id;
        let maintenance_id = request.maintenance_id;

        // Check if already draining
        if self.is_draining(&node_id) {
            return Err(MaintenanceError::NodeAlreadyInMaintenance(node_id));
        }

        info!(
            "Starting drain for node {} (maintenance: {}), {} tasks to handle",
            node_id,
            maintenance_id,
            current_tasks.len()
        );

        // Create drain info
        let drain_info = DrainInfo::new(
            node_id,
            maintenance_id,
            current_tasks.len(),
            request.policy.default_timeout,
            request.policy.force_on_timeout,
        );

        // Mark node as draining
        self.node_statuses.insert(node_id, DrainStatus::Pending);
        self.active_drains.insert(node_id, drain_info.clone());
        self.excluded_nodes.insert(node_id, maintenance_id);

        // Update task assignments
        {
            let mut assignments = self.task_assignments.write();
            for task_id in &current_tasks {
                assignments.insert(*task_id, node_id);
            }
        }

        // Send event
        let _ = self
            .event_tx
            .send(DrainEvent::DrainStarted {
                node_id,
                maintenance_id,
                task_count: current_tasks.len(),
            })
            .await;

        Ok(drain_info)
    }

    /// Update drain progress
    pub async fn update_drain_progress(
        &self,
        node_id: WorkerId,
        remaining_tasks: usize,
        migrated: usize,
        completed: usize,
    ) {
        if let Some(mut drain) = self.active_drains.get_mut(&node_id) {
            drain.remaining_task_count = remaining_tasks;
            drain.migrated_task_count = migrated;
            drain.completed_task_count = completed;

            if remaining_tasks == 0 {
                drain.status = DrainStatus::Drained;
                drain.completed_at = Some(Utc::now());
                self.node_statuses.insert(node_id, DrainStatus::Drained);

                info!("Node {} drain completed", node_id);

                let _ = self
                    .event_tx
                    .send(DrainEvent::DrainCompleted {
                        node_id,
                        maintenance_id: drain.maintenance_id,
                        migrated_count: drain.migrated_task_count,
                        completed_count: drain.completed_task_count,
                        force_stopped_count: drain.force_stopped_count,
                    })
                    .await;
            } else if drain.status == DrainStatus::Pending {
                drain.status = DrainStatus::Draining;
                drain.started_at = Some(Utc::now());
                self.node_statuses.insert(node_id, DrainStatus::Draining);
            }
        }
    }

    /// Record task migration
    pub async fn record_task_migrated(
        &self,
        node_id: WorkerId,
        task_id: TaskId,
        target_node: Option<WorkerId>,
    ) {
        debug!(
            "Task {} migrated from node {} to {:?}",
            task_id, node_id, target_node
        );

        // Update assignments
        {
            let mut assignments = self.task_assignments.write();
            if let Some(target) = target_node {
                assignments.insert(task_id, target);
            } else {
                assignments.remove(&task_id);
            }
        }

        let _ = self
            .event_tx
            .send(DrainEvent::TaskMigrated {
                node_id,
                task_id,
                target_node,
            })
            .await;
    }

    /// Record task completion on draining node
    pub async fn record_task_completed(&self, node_id: WorkerId, task_id: TaskId) {
        debug!("Task {} completed on draining node {}", task_id, node_id);

        {
            let mut assignments = self.task_assignments.write();
            assignments.remove(&task_id);
        }

        let _ = self
            .event_tx
            .send(DrainEvent::TaskCompleted { node_id, task_id })
            .await;
    }

    /// Force stop a task
    pub async fn force_stop_task(&self, node_id: WorkerId, task_id: TaskId) {
        warn!("Force stopping task {} on node {}", task_id, node_id);

        if let Some(mut drain) = self.active_drains.get_mut(&node_id) {
            drain.force_stopped_count += 1;
        }

        {
            let mut assignments = self.task_assignments.write();
            assignments.remove(&task_id);
        }

        let _ = self
            .event_tx
            .send(DrainEvent::TaskForceStopped { node_id, task_id })
            .await;
    }

    /// Cancel drain for a node
    pub async fn cancel_drain(&self, node_id: WorkerId) -> Result<(), MaintenanceError> {
        if let Some(drain) = self.active_drains.get(&node_id) {
            let maintenance_id = drain.maintenance_id;

            info!("Cancelling drain for node {}", node_id);

            self.node_statuses.insert(node_id, DrainStatus::Cancelled);
            self.excluded_nodes.remove(&node_id);

            let _ = self
                .event_tx
                .send(DrainEvent::DrainCancelled {
                    node_id,
                    maintenance_id,
                })
                .await;

            Ok(())
        } else {
            Err(MaintenanceError::NodeNotInWindow(node_id))
        }
    }

    /// Handle drain timeout
    pub async fn handle_drain_timeout(&self, node_id: WorkerId, force: bool) {
        if let Some(mut drain) = self.active_drains.get_mut(&node_id) {
            let maintenance_id = drain.maintenance_id;

            warn!("Drain timeout for node {} (force={})", node_id, force);

            if force {
                drain.status = DrainStatus::ForceDrained;
                self.node_statuses
                    .insert(node_id, DrainStatus::ForceDrained);
            }

            let _ = self
                .event_tx
                .send(DrainEvent::DrainTimeout {
                    node_id,
                    maintenance_id,
                })
                .await;
        }
    }

    /// Complete drain and clean up
    pub fn complete_drain(&self, node_id: WorkerId) {
        self.active_drains.remove(&node_id);
        // Note: Keep in excluded_nodes until maintenance completes
    }

    /// Remove exclusion after maintenance completes
    pub fn remove_exclusion(&self, node_id: WorkerId) {
        self.excluded_nodes.remove(&node_id);
        self.node_statuses.remove(&node_id);
        self.active_drains.remove(&node_id);
    }

    /// Get default policy
    pub fn default_policy(&self) -> &DrainPolicy {
        &self.default_policy
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DRAIN COORDIGLOBAL_ALLIANCE_T1R
// ─────────────────────────────────────────────────────────────────────────────

/// Coordinates drain operations across multiple nodes
pub struct DrainCoordinator {
    /// Drain manager
    manager: Arc<NodeDrainManager>,

    /// Event receiver for monitoring
    event_rx: Option<mpsc::Receiver<DrainEvent>>,

    /// Check interval for drain progress
    check_interval: Duration,
}

impl DrainCoordinator {
    /// Create a new drain coordinator
    pub fn new(buffer_size: usize) -> (Self, mpsc::Sender<DrainEvent>) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let manager = Arc::new(NodeDrainManager::new(tx.clone()));

        (
            Self {
                manager,
                event_rx: Some(rx),
                check_interval: Duration::from_secs(5),
            },
            tx,
        )
    }

    /// Get the drain manager
    pub fn manager(&self) -> Arc<NodeDrainManager> {
        self.manager.clone()
    }

    /// Take the event receiver
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<DrainEvent>> {
        self.event_rx.take()
    }

    /// Drain multiple nodes sequentially (for rolling maintenance)
    pub async fn drain_nodes_sequential(
        &self,
        nodes: Vec<(WorkerId, Vec<TaskId>)>,
        maintenance_id: MaintenanceWindowId,
        policy: DrainPolicy,
    ) -> Result<Vec<DrainInfo>, MaintenanceError> {
        let mut results = Vec::new();

        for (node_id, tasks) in nodes {
            let request = DrainRequest::new(node_id, maintenance_id, policy.clone());
            let info = self.manager.start_drain(request, tasks).await?;
            results.push(info);

            // Wait for drain to complete before moving to next node
            self.wait_for_drain(node_id, policy.default_timeout).await?;
        }

        Ok(results)
    }

    /// Drain multiple nodes in parallel
    pub async fn drain_nodes_parallel(
        &self,
        nodes: Vec<(WorkerId, Vec<TaskId>)>,
        maintenance_id: MaintenanceWindowId,
        policy: DrainPolicy,
    ) -> Result<Vec<DrainInfo>, MaintenanceError> {
        let mut results = Vec::new();

        // Start all drains
        for (node_id, tasks) in &nodes {
            let request = DrainRequest::new(*node_id, maintenance_id, policy.clone());
            let info = self.manager.start_drain(request, tasks.clone()).await?;
            results.push(info);
        }

        // Wait for all drains to complete
        for (node_id, _) in &nodes {
            self.wait_for_drain(*node_id, policy.default_timeout)
                .await?;
        }

        Ok(results)
    }

    /// Wait for a specific node to finish draining
    async fn wait_for_drain(
        &self,
        node_id: WorkerId,
        timeout: chrono::Duration,
    ) -> Result<(), MaintenanceError> {
        let deadline = Utc::now() + timeout;
        let mut interval = interval(self.check_interval);

        loop {
            interval.tick().await;

            let status = self.manager.get_drain_status(&node_id);

            match status {
                DrainStatus::Drained | DrainStatus::ForceDrained => {
                    return Ok(());
                }
                DrainStatus::Cancelled => {
                    return Err(MaintenanceError::Internal("Drain was cancelled".into()));
                }
                _ => {
                    if Utc::now() >= deadline {
                        // Check if we should force drain
                        if let Some(info) = self.manager.get_drain_info(&node_id) {
                            if info.force_on_timeout {
                                self.manager.handle_drain_timeout(node_id, true).await;
                                return Ok(());
                            }
                        }
                        return Err(MaintenanceError::DrainTimeout(node_id));
                    }
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TASK MIGRATOR
// ─────────────────────────────────────────────────────────────────────────────

/// Handles task migration during drain operations
pub struct TaskMigrator {
    /// Drain manager reference
    drain_manager: Arc<NodeDrainManager>,

    /// Callback for checkpointing tasks
    checkpoint_fn: Option<Box<dyn Fn(TaskId) -> bool + Send + Sync>>,

    /// Callback for reassigning tasks
    reassign_fn: Option<Box<dyn Fn(TaskId, Option<WorkerId>) -> Option<WorkerId> + Send + Sync>>,
}

impl TaskMigrator {
    /// Create a new task migrator
    pub fn new(drain_manager: Arc<NodeDrainManager>) -> Self {
        Self {
            drain_manager,
            checkpoint_fn: None,
            reassign_fn: None,
        }
    }

    /// Set checkpoint function
    pub fn with_checkpoint_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(TaskId) -> bool + Send + Sync + 'static,
    {
        self.checkpoint_fn = Some(Box::new(f));
        self
    }

    /// Set reassign function
    pub fn with_reassign_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(TaskId, Option<WorkerId>) -> Option<WorkerId> + Send + Sync + 'static,
    {
        self.reassign_fn = Some(Box::new(f));
        self
    }

    /// Migrate a task from a draining node
    pub async fn migrate_task(
        &self,
        task: &Task,
        source_node: WorkerId,
        policy: &DrainPolicy,
    ) -> Result<Option<WorkerId>, MaintenanceError> {
        let task_id = task.id;

        // Checkpoint if needed
        if policy.checkpoint_before_migrate {
            if let Some(ref checkpoint_fn) = self.checkpoint_fn {
                if !checkpoint_fn(task_id) {
                    warn!("Failed to checkpoint task {} before migration", task_id);
                }
            }
        }

        // Find target node
        let target = if let Some(ref reassign_fn) = self.reassign_fn {
            reassign_fn(task_id, None)
        } else {
            None
        };

        // Record migration
        self.drain_manager
            .record_task_migrated(source_node, task_id, target)
            .await;

        Ok(target)
    }

    /// Migrate all pending tasks from a node
    pub async fn migrate_pending_tasks(
        &self,
        tasks: Vec<Task>,
        source_node: WorkerId,
        policy: &DrainPolicy,
    ) -> Vec<(TaskId, Option<WorkerId>)> {
        let mut migrations = Vec::new();

        for task in tasks {
            if task.status == TaskStatus::Pending {
                match self.migrate_task(&task, source_node, policy).await {
                    Ok(target) => migrations.push((task.id, target)),
                    Err(e) => {
                        warn!("Failed to migrate task {}: {}", task.id, e);
                    }
                }
            }
        }

        migrations
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_drain_manager_creation() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = NodeDrainManager::new(tx);

        let node_id = WorkerId::new();
        assert!(!manager.is_draining(&node_id));
        assert!(!manager.is_excluded(&node_id));
    }

    #[tokio::test]
    async fn test_start_drain() {
        let (tx, mut rx) = mpsc::channel(100);
        let manager = NodeDrainManager::new(tx);

        let node_id = WorkerId::new();
        let maintenance_id = MaintenanceWindowId::new();
        let tasks = vec![TaskId::new(), TaskId::new()];

        let request = DrainRequest::new(node_id, maintenance_id, DrainPolicy::default());
        let info = manager.start_drain(request, tasks.clone()).await.unwrap();

        assert_eq!(info.node_id, node_id);
        assert_eq!(info.initial_task_count, 2);
        assert!(manager.is_draining(&node_id));
        assert!(manager.is_excluded(&node_id));

        // Check event was sent
        let event = rx.recv().await.unwrap();
        match event {
            DrainEvent::DrainStarted {
                node_id: n,
                task_count,
                ..
            } => {
                assert_eq!(n, node_id);
                assert_eq!(task_count, 2);
            }
            _ => panic!("Expected DrainStarted event"),
        }
    }

    #[tokio::test]
    async fn test_drain_progress() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = NodeDrainManager::new(tx);

        let node_id = WorkerId::new();
        let maintenance_id = MaintenanceWindowId::new();
        let tasks = vec![TaskId::new(), TaskId::new(), TaskId::new()];

        let request = DrainRequest::new(node_id, maintenance_id, DrainPolicy::default());
        manager.start_drain(request, tasks.clone()).await.unwrap();

        // Simulate progress
        manager.update_drain_progress(node_id, 2, 1, 0).await;
        let info = manager.get_drain_info(&node_id).unwrap();
        assert_eq!(info.remaining_task_count, 2);
        assert_eq!(info.migrated_task_count, 1);

        // Complete drain
        manager.update_drain_progress(node_id, 0, 2, 1).await;
        let status = manager.get_drain_status(&node_id);
        assert_eq!(status, DrainStatus::Drained);
    }

    #[tokio::test]
    async fn test_cancel_drain() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = NodeDrainManager::new(tx);

        let node_id = WorkerId::new();
        let maintenance_id = MaintenanceWindowId::new();
        let tasks = vec![TaskId::new()];

        let request = DrainRequest::new(node_id, maintenance_id, DrainPolicy::default());
        manager.start_drain(request, tasks).await.unwrap();

        assert!(manager.cancel_drain(node_id).await.is_ok());
        assert_eq!(manager.get_drain_status(&node_id), DrainStatus::Cancelled);
        assert!(!manager.is_excluded(&node_id));
    }

    #[test]
    fn test_drain_policy_defaults() {
        let default = DrainPolicy::default();
        assert!(default.wait_for_running);
        assert!(!default.force_on_timeout);

        let emergency = DrainPolicy::emergency();
        assert!(!emergency.wait_for_running);
        assert!(emergency.force_on_timeout);

        let rolling = DrainPolicy::rolling();
        assert!(rolling.wait_for_running);
        assert!(!rolling.force_on_timeout);
    }

    #[tokio::test]
    async fn test_drain_coordinator() {
        let (coordinator, _tx) = DrainCoordinator::new(100);
        let manager = coordinator.manager();

        let node_id = WorkerId::new();
        assert!(!manager.is_draining(&node_id));
    }

    #[tokio::test]
    async fn test_excluded_nodes() {
        let (tx, _rx) = mpsc::channel(100);
        let manager = NodeDrainManager::new(tx);

        let node_id = WorkerId::new();
        let maintenance_id = MaintenanceWindowId::new();

        let request = DrainRequest::new(node_id, maintenance_id, DrainPolicy::default());
        manager.start_drain(request, vec![]).await.unwrap();

        assert!(manager.is_excluded(&node_id));
        let excluded = manager.get_excluded_nodes();
        assert!(excluded.contains(&node_id));

        manager.remove_exclusion(node_id);
        assert!(!manager.is_excluded(&node_id));
    }
}
