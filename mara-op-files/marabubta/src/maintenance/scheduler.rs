// Marabunta - Licensed under the MIT License.
//! Maintenance window scheduler
//!
//! Manages scheduling, tracking, and execution of maintenance windows.
//! Coordinates with the drain manager to prepare nodes for maintenance.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::common::types::{TaskId, WorkerId};
use crate::maintenance::drain::{
    DrainCoordinator, DrainEvent, DrainPolicy, DrainRequest, NodeDrainManager,
};
use crate::maintenance::types::{
    MaintenanceError, MaintenanceState, MaintenanceSummary, MaintenanceType, MaintenanceWindow,
    MaintenanceWindowId,
};

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE EVENT
// ─────────────────────────────────────────────────────────────────────────────

/// Events emitted by the maintenance scheduler
#[derive(Debug, Clone)]
pub enum MaintenanceEvent {
    /// Maintenance window scheduled
    WindowScheduled {
        id: MaintenanceWindowId,
        name: String,
        start: DateTime<Utc>,
        affected_nodes: usize,
    },

    /// Maintenance window starting (entering drain phase)
    WindowStarting {
        id: MaintenanceWindowId,
        name: String,
    },

    /// All nodes drained, maintenance in progress
    WindowInProgress {
        id: MaintenanceWindowId,
        name: String,
    },

    /// Maintenance window completed
    WindowCompleted {
        id: MaintenanceWindowId,
        name: String,
        duration_secs: u64,
    },

    /// Maintenance window cancelled
    WindowCancelled {
        id: MaintenanceWindowId,
        name: String,
        reason: String,
    },

    /// Maintenance window failed
    WindowFailed {
        id: MaintenanceWindowId,
        name: String,
        reason: String,
    },

    /// Rolling maintenance progress
    RollingProgress {
        id: MaintenanceWindowId,
        completed_nodes: usize,
        total_nodes: usize,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE SCHEDULER CONFIG
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the maintenance scheduler
#[derive(Debug, Clone)]
pub struct MaintenanceSchedulerConfig {
    /// How far in advance to start drain before maintenance
    pub drain_lead_time: chrono::Duration,

    /// Check interval for scheduled maintenance
    pub check_interval: Duration,

    /// Maximum concurrent maintenance windows
    pub max_concurrent_windows: usize,

    /// Default rolling batch size
    pub default_rolling_batch_size: usize,

    /// Minimum time between maintenance on same node
    pub min_node_interval: chrono::Duration,

    /// Whether to allow overlapping maintenance on different nodes
    pub allow_overlapping: bool,
}

impl Default for MaintenanceSchedulerConfig {
    fn default() -> Self {
        Self {
            drain_lead_time: chrono::Duration::minutes(15),
            check_interval: Duration::from_secs(30),
            max_concurrent_windows: 3,
            default_rolling_batch_size: 1,
            min_node_interval: chrono::Duration::hours(24),
            allow_overlapping: true,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE SCHEDULER
// ─────────────────────────────────────────────────────────────────────────────

/// Manages maintenance window scheduling and execution
pub struct MaintenanceScheduler {
    /// All maintenance windows by ID
    windows: DashMap<MaintenanceWindowId, MaintenanceWindow>,

    /// Windows indexed by affected node
    node_windows: DashMap<WorkerId, HashSet<MaintenanceWindowId>>,

    /// Active windows (currently draining or in progress)
    active_windows: RwLock<HashSet<MaintenanceWindowId>>,

    /// Drain coordinator
    #[allow(dead_code)]
    drain_coordinator: Arc<DrainCoordinator>,

    /// Drain manager
    drain_manager: Arc<NodeDrainManager>,

    /// Configuration
    config: MaintenanceSchedulerConfig,

    /// Event sender
    event_tx: mpsc::Sender<MaintenanceEvent>,

    /// Callback to get tasks for a node
    get_node_tasks: RwLock<Option<Arc<dyn Fn(WorkerId) -> Vec<TaskId> + Send + Sync>>>,

    /// Callback to notify node status change
    on_node_status_change: RwLock<Option<Arc<dyn Fn(WorkerId, bool) + Send + Sync>>>,
}

impl MaintenanceScheduler {
    /// Create a new maintenance scheduler
    pub fn new(
        config: MaintenanceSchedulerConfig,
        event_tx: mpsc::Sender<MaintenanceEvent>,
    ) -> (Self, mpsc::Receiver<DrainEvent>) {
        let (drain_coordinator, _drain_tx) = DrainCoordinator::new(1000);
        let (_drain_event_tx, drain_event_rx) = mpsc::channel(1000);

        // Create drain manager with event forwarding
        let drain_manager = drain_coordinator.manager();

        let scheduler = Self {
            windows: DashMap::new(),
            node_windows: DashMap::new(),
            active_windows: RwLock::new(HashSet::new()),
            drain_coordinator: Arc::new(drain_coordinator),
            drain_manager,
            config,
            event_tx,
            get_node_tasks: RwLock::new(None),
            on_node_status_change: RwLock::new(None),
        };

        (scheduler, drain_event_rx)
    }

    /// Set callback for getting node tasks
    pub fn set_get_node_tasks<F>(&self, f: F)
    where
        F: Fn(WorkerId) -> Vec<TaskId> + Send + Sync + 'static,
    {
        *self.get_node_tasks.write() = Some(Arc::new(f));
    }

    /// Set callback for node status changes
    pub fn set_on_node_status_change<F>(&self, f: F)
    where
        F: Fn(WorkerId, bool) + Send + Sync + 'static,
    {
        *self.on_node_status_change.write() = Some(Arc::new(f));
    }

    /// Schedule a new maintenance window
    pub async fn schedule_maintenance(
        &self,
        window: MaintenanceWindow,
    ) -> Result<MaintenanceWindowId, MaintenanceError> {
        // Validate the window
        self.validate_window(&window)?;

        let id = window.id;
        let name = window.name.clone();
        let start = window.scheduled_start;
        let affected_nodes = window.affected_nodes.len();

        // Check for overlapping windows if not allowed
        if !self.config.allow_overlapping {
            self.check_overlapping(&window)?;
        }

        // Index by affected nodes
        for node_id in &window.affected_nodes {
            self.node_windows.entry(*node_id).or_default().insert(id);
        }

        // Store the window
        self.windows.insert(id, window);

        info!(
            "Scheduled maintenance window {} '{}' starting at {}",
            id, name, start
        );

        // Send event
        let _ = self
            .event_tx
            .send(MaintenanceEvent::WindowScheduled {
                id,
                name,
                start,
                affected_nodes,
            })
            .await;

        Ok(id)
    }

    /// Schedule emergency maintenance
    pub async fn schedule_emergency(
        &self,
        name: impl Into<String>,
        affected_nodes: HashSet<WorkerId>,
        duration: chrono::Duration,
    ) -> Result<MaintenanceWindowId, MaintenanceError> {
        let window = MaintenanceWindow::emergency(name, affected_nodes, duration);
        let id = self.schedule_maintenance(window).await?;

        // Start immediately
        self.start_maintenance(id).await?;

        Ok(id)
    }

    /// Get maintenance window by ID
    pub fn get_window(&self, id: MaintenanceWindowId) -> Option<MaintenanceWindow> {
        self.windows.get(&id).map(|w| w.clone())
    }

    /// Get all maintenance windows
    pub fn list_windows(&self) -> Vec<MaintenanceSummary> {
        self.windows
            .iter()
            .map(|w| MaintenanceSummary::from(w.value()))
            .collect()
    }

    /// Get scheduled (future) maintenance windows
    pub fn get_scheduled_windows(&self) -> Vec<MaintenanceSummary> {
        self.windows
            .iter()
            .filter(|w| w.state == MaintenanceState::Scheduled)
            .map(|w| MaintenanceSummary::from(w.value()))
            .collect()
    }

    /// Get active maintenance windows
    pub fn get_active_windows(&self) -> Vec<MaintenanceSummary> {
        self.windows
            .iter()
            .filter(|w| w.is_active())
            .map(|w| MaintenanceSummary::from(w.value()))
            .collect()
    }

    /// Get maintenance windows affecting a node
    pub fn get_node_maintenance(&self, node_id: WorkerId) -> Vec<MaintenanceSummary> {
        if let Some(window_ids) = self.node_windows.get(&node_id) {
            window_ids
                .iter()
                .filter_map(|id| {
                    self.windows
                        .get(id)
                        .map(|w| MaintenanceSummary::from(w.value()))
                })
                .collect()
        } else {
            vec![]
        }
    }

    /// Check if a node has upcoming or active maintenance
    pub fn has_upcoming_maintenance(&self, node_id: WorkerId, within: chrono::Duration) -> bool {
        let deadline = Utc::now() + within;

        if let Some(window_ids) = self.node_windows.get(&node_id) {
            for id in window_ids.iter() {
                if let Some(window) = self.windows.get(id) {
                    if !window.state.is_terminal() && window.scheduled_start <= deadline {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if a node is currently unavailable due to maintenance
    pub fn is_node_in_maintenance(&self, node_id: &WorkerId) -> bool {
        if let Some(window_ids) = self.node_windows.get(node_id) {
            for id in window_ids.iter() {
                if let Some(window) = self.windows.get(id) {
                    if window.state.nodes_unavailable() {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Start a maintenance window (begin draining)
    pub async fn start_maintenance(&self, id: MaintenanceWindowId) -> Result<(), MaintenanceError> {
        let mut window = self
            .windows
            .get_mut(&id)
            .ok_or(MaintenanceError::NotFound(id))?;

        // Transition to draining
        window.start_draining()?;

        let name = window.name.clone();
        let affected_nodes: Vec<_> = window.affected_nodes.iter().cloned().collect();
        let maintenance_type = window.maintenance_type;
        let drain_timeout = window.drain_timeout;
        let force_drain = window.force_drain_on_timeout;
        let batch_size = window.rolling_batch_size;

        drop(window);

        // Add to active windows
        self.active_windows.write().insert(id);

        info!("Starting maintenance window {} '{}'", id, name);

        // Send event
        let _ = self
            .event_tx
            .send(MaintenanceEvent::WindowStarting {
                id,
                name: name.clone(),
            })
            .await;

        // Notify nodes that they're being taken offline
        if let Some(callback) = self.on_node_status_change.read().as_ref() {
            for node_id in &affected_nodes {
                callback(*node_id, false); // Node becoming unavailable
            }
        }

        // Start draining based on maintenance type
        let policy = match maintenance_type {
            MaintenanceType::Emergency | MaintenanceType::Security => DrainPolicy::emergency(),
            MaintenanceType::Rolling => DrainPolicy::rolling(),
            _ => {
                let mut policy = DrainPolicy::default();
                policy.default_timeout = drain_timeout;
                policy.force_on_timeout = force_drain;
                policy
            }
        };

        // Get tasks for each node and start drains
        if maintenance_type == MaintenanceType::Rolling {
            self.start_rolling_drain(id, affected_nodes, policy, batch_size)
                .await?;
        } else {
            self.start_parallel_drain(id, affected_nodes, policy)
                .await?;
        }

        Ok(())
    }

    /// Start rolling drain (one batch at a time)
    async fn start_rolling_drain(
        &self,
        maintenance_id: MaintenanceWindowId,
        nodes: Vec<WorkerId>,
        policy: DrainPolicy,
        batch_size: usize,
    ) -> Result<(), MaintenanceError> {
        let mut completed = 0;
        let total = nodes.len();

        for batch in nodes.chunks(batch_size) {
            let nodes_with_tasks: Vec<_> = batch
                .iter()
                .map(|node_id| {
                    let tasks = self.get_tasks_for_node(*node_id);
                    (*node_id, tasks)
                })
                .collect();

            for (node_id, tasks) in nodes_with_tasks {
                let request = DrainRequest::new(node_id, maintenance_id, policy.clone());
                self.drain_manager.start_drain(request, tasks).await?;

                // Mark node as draining in window
                if let Some(mut window) = self.windows.get_mut(&maintenance_id) {
                    window.start_node_drain(node_id);
                }
            }

            // Wait for this batch to complete
            for node_id in batch {
                self.wait_for_node_drained(*node_id).await;

                if let Some(mut window) = self.windows.get_mut(&maintenance_id) {
                    window.mark_node_drained(*node_id);
                }

                completed += 1;

                // Send progress event
                let _ = self
                    .event_tx
                    .send(MaintenanceEvent::RollingProgress {
                        id: maintenance_id,
                        completed_nodes: completed,
                        total_nodes: total,
                    })
                    .await;
            }
        }

        // Transition to in-progress
        self.transition_to_in_progress(maintenance_id).await
    }

    /// Start parallel drain (all nodes at once)
    async fn start_parallel_drain(
        &self,
        maintenance_id: MaintenanceWindowId,
        nodes: Vec<WorkerId>,
        policy: DrainPolicy,
    ) -> Result<(), MaintenanceError> {
        let nodes_with_tasks: Vec<_> = nodes
            .iter()
            .map(|node_id| {
                let tasks = self.get_tasks_for_node(*node_id);
                (*node_id, tasks)
            })
            .collect();

        // Start all drains
        for (node_id, tasks) in &nodes_with_tasks {
            let request = DrainRequest::new(*node_id, maintenance_id, policy.clone());
            self.drain_manager
                .start_drain(request, tasks.clone())
                .await?;

            if let Some(mut window) = self.windows.get_mut(&maintenance_id) {
                window.start_node_drain(*node_id);
            }
        }

        // Wait for all to complete
        for (node_id, _) in &nodes_with_tasks {
            self.wait_for_node_drained(*node_id).await;

            if let Some(mut window) = self.windows.get_mut(&maintenance_id) {
                window.mark_node_drained(*node_id);
            }
        }

        // Transition to in-progress
        self.transition_to_in_progress(maintenance_id).await
    }

    /// Wait for a node to be drained
    async fn wait_for_node_drained(&self, node_id: WorkerId) {
        use crate::maintenance::types::DrainStatus;

        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let status = self.drain_manager.get_drain_status(&node_id);
            if matches!(
                status,
                DrainStatus::Drained | DrainStatus::ForceDrained | DrainStatus::Cancelled
            ) {
                break;
            }
        }
    }

    /// Transition maintenance to in-progress
    async fn transition_to_in_progress(
        &self,
        id: MaintenanceWindowId,
    ) -> Result<(), MaintenanceError> {
        if let Some(mut window) = self.windows.get_mut(&id) {
            window.start_maintenance()?;

            let name = window.name.clone();
            drop(window);

            info!("Maintenance window {} '{}' now in progress", id, name);

            let _ = self
                .event_tx
                .send(MaintenanceEvent::WindowInProgress { id, name })
                .await;
        }

        Ok(())
    }

    /// Complete a maintenance window
    pub async fn complete_maintenance(
        &self,
        id: MaintenanceWindowId,
    ) -> Result<(), MaintenanceError> {
        let mut window = self
            .windows
            .get_mut(&id)
            .ok_or(MaintenanceError::NotFound(id))?;

        window.complete()?;

        let name = window.name.clone();
        let affected_nodes: Vec<_> = window.affected_nodes.iter().cloned().collect();
        let duration_secs = window
            .actual_duration()
            .map(|d| d.num_seconds() as u64)
            .unwrap_or(0);

        drop(window);

        // Remove from active windows
        self.active_windows.write().remove(&id);

        info!(
            "Maintenance window {} '{}' completed ({}s)",
            id, name, duration_secs
        );

        // Remove node exclusions
        for node_id in &affected_nodes {
            self.drain_manager.remove_exclusion(*node_id);
        }

        // Notify nodes they're back online
        if let Some(callback) = self.on_node_status_change.read().as_ref() {
            for node_id in &affected_nodes {
                callback(*node_id, true); // Node becoming available
            }
        }

        // Send event
        let _ = self
            .event_tx
            .send(MaintenanceEvent::WindowCompleted {
                id,
                name,
                duration_secs,
            })
            .await;

        Ok(())
    }

    /// Cancel a maintenance window
    pub async fn cancel_maintenance(
        &self,
        id: MaintenanceWindowId,
        reason: impl Into<String>,
    ) -> Result<(), MaintenanceError> {
        let reason = reason.into();

        let mut window = self
            .windows
            .get_mut(&id)
            .ok_or(MaintenanceError::NotFound(id))?;

        window.cancel(reason.clone())?;

        let name = window.name.clone();
        let affected_nodes: Vec<_> = window.affected_nodes.iter().cloned().collect();

        drop(window);

        // Remove from active windows
        self.active_windows.write().remove(&id);

        info!("Maintenance window {} '{}' cancelled: {}", id, name, reason);

        // Cancel any active drains
        for node_id in &affected_nodes {
            let _ = self.drain_manager.cancel_drain(*node_id).await;
            self.drain_manager.remove_exclusion(*node_id);
        }

        // Notify nodes they're back online
        if let Some(callback) = self.on_node_status_change.read().as_ref() {
            for node_id in &affected_nodes {
                callback(*node_id, true);
            }
        }

        // Send event
        let _ = self
            .event_tx
            .send(MaintenanceEvent::WindowCancelled { id, name, reason })
            .await;

        Ok(())
    }

    /// Check for and start due maintenance windows
    pub async fn process_scheduled_windows(&self) {
        let now = Utc::now();
        let drain_start_time = now + self.config.drain_lead_time;

        for window in self.windows.iter() {
            if window.state == MaintenanceState::Scheduled {
                // Start draining if we're within the lead time
                if window.scheduled_start <= drain_start_time {
                    let id = window.id;
                    drop(window);

                    if let Err(e) = self.start_maintenance(id).await {
                        error!("Failed to start maintenance {}: {}", id, e);
                    }
                }
            }
        }
    }

    /// Run the scheduler loop
    pub async fn run(&self) {
        let mut interval = interval(self.config.check_interval);

        loop {
            interval.tick().await;
            self.process_scheduled_windows().await;
        }
    }

    /// Get the drain manager
    pub fn drain_manager(&self) -> Arc<NodeDrainManager> {
        self.drain_manager.clone()
    }

    /// Validate a maintenance window
    fn validate_window(&self, window: &MaintenanceWindow) -> Result<(), MaintenanceError> {
        if window.scheduled_end <= window.scheduled_start {
            return Err(MaintenanceError::InvalidWindow(
                "End time must be after start time".into(),
            ));
        }

        if window.affected_nodes.is_empty() {
            return Err(MaintenanceError::InvalidWindow(
                "Must specify at least one affected node".into(),
            ));
        }

        // Check for recent maintenance on nodes
        for node_id in &window.affected_nodes {
            if let Some(window_ids) = self.node_windows.get(node_id) {
                for id in window_ids.iter() {
                    if let Some(existing) = self.windows.get(id) {
                        if existing.state == MaintenanceState::Completed {
                            if let Some(end) = existing.actual_end {
                                if window.scheduled_start < end + self.config.min_node_interval {
                                    warn!(
                                        "Node {} had maintenance recently (ended {})",
                                        node_id, end
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check for overlapping maintenance windows
    fn check_overlapping(&self, window: &MaintenanceWindow) -> Result<(), MaintenanceError> {
        for node_id in &window.affected_nodes {
            if let Some(window_ids) = self.node_windows.get(node_id) {
                for id in window_ids.iter() {
                    if let Some(existing) = self.windows.get(id) {
                        if !existing.state.is_terminal() {
                            // Check for time overlap
                            if window.scheduled_start < existing.scheduled_end
                                && window.scheduled_end > existing.scheduled_start
                            {
                                return Err(MaintenanceError::OverlappingWindow(*id));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get tasks for a node (using callback or returning empty)
    fn get_tasks_for_node(&self, node_id: WorkerId) -> Vec<TaskId> {
        if let Some(callback) = self.get_node_tasks.read().as_ref() {
            callback(node_id)
        } else {
            vec![]
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE SCHEDULER HANDLE
// ─────────────────────────────────────────────────────────────────────────────

/// Handle for interacting with the maintenance scheduler
pub struct MaintenanceSchedulerHandle {
    scheduler: Arc<MaintenanceScheduler>,
}

impl MaintenanceSchedulerHandle {
    /// Create a new handle
    pub fn new(scheduler: Arc<MaintenanceScheduler>) -> Self {
        Self { scheduler }
    }

    /// Schedule maintenance
    pub async fn schedule(
        &self,
        window: MaintenanceWindow,
    ) -> Result<MaintenanceWindowId, MaintenanceError> {
        self.scheduler.schedule_maintenance(window).await
    }

    /// Get window
    pub fn get(&self, id: MaintenanceWindowId) -> Option<MaintenanceWindow> {
        self.scheduler.get_window(id)
    }

    /// List all windows
    pub fn list(&self) -> Vec<MaintenanceSummary> {
        self.scheduler.list_windows()
    }

    /// Cancel maintenance
    pub async fn cancel(
        &self,
        id: MaintenanceWindowId,
        reason: impl Into<String>,
    ) -> Result<(), MaintenanceError> {
        self.scheduler.cancel_maintenance(id, reason).await
    }

    /// Complete maintenance
    pub async fn complete(&self, id: MaintenanceWindowId) -> Result<(), MaintenanceError> {
        self.scheduler.complete_maintenance(id).await
    }

    /// Check if node is in maintenance
    pub fn is_node_in_maintenance(&self, node_id: &WorkerId) -> bool {
        self.scheduler.is_node_in_maintenance(node_id)
    }

    /// Check if node is excluded from scheduling
    pub fn is_node_excluded(&self, node_id: &WorkerId) -> bool {
        self.scheduler.drain_manager().is_excluded(node_id)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_window() -> MaintenanceWindow {
        let mut nodes = HashSet::new();
        nodes.insert(WorkerId::new());
        nodes.insert(WorkerId::new());

        MaintenanceWindow::new(
            "Test Maintenance",
            MaintenanceType::Planned,
            Utc::now() + chrono::Duration::hours(1),
            Utc::now() + chrono::Duration::hours(3),
            nodes,
        )
    }

    #[tokio::test]
    async fn test_schedule_maintenance() {
        let (tx, _rx) = mpsc::channel(100);
        let (scheduler, _drain_rx) =
            MaintenanceScheduler::new(MaintenanceSchedulerConfig::default(), tx);

        let window = create_test_window();
        let id = scheduler.schedule_maintenance(window).await.unwrap();

        assert!(scheduler.get_window(id).is_some());
        assert_eq!(scheduler.list_windows().len(), 1);
    }

    #[tokio::test]
    async fn test_get_scheduled_windows() {
        let (tx, _rx) = mpsc::channel(100);
        let (scheduler, _drain_rx) =
            MaintenanceScheduler::new(MaintenanceSchedulerConfig::default(), tx);

        let window = create_test_window();
        scheduler.schedule_maintenance(window).await.unwrap();

        let scheduled = scheduler.get_scheduled_windows();
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].state, MaintenanceState::Scheduled);
    }

    #[tokio::test]
    async fn test_cancel_maintenance() {
        let (tx, _rx) = mpsc::channel(100);
        let (scheduler, _drain_rx) =
            MaintenanceScheduler::new(MaintenanceSchedulerConfig::default(), tx);

        let window = create_test_window();
        let id = scheduler.schedule_maintenance(window).await.unwrap();

        scheduler
            .cancel_maintenance(id, "Test cancellation")
            .await
            .unwrap();

        let window = scheduler.get_window(id).unwrap();
        assert_eq!(window.state, MaintenanceState::Cancelled);
    }

    #[tokio::test]
    async fn test_invalid_window_times() {
        let (tx, _rx) = mpsc::channel(100);
        let (scheduler, _drain_rx) =
            MaintenanceScheduler::new(MaintenanceSchedulerConfig::default(), tx);

        let mut nodes = HashSet::new();
        nodes.insert(WorkerId::new());

        // End before start
        let window = MaintenanceWindow::new(
            "Invalid",
            MaintenanceType::Planned,
            Utc::now() + chrono::Duration::hours(2),
            Utc::now() + chrono::Duration::hours(1),
            nodes,
        );

        let result = scheduler.schedule_maintenance(window).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_empty_nodes() {
        let (tx, _rx) = mpsc::channel(100);
        let (scheduler, _drain_rx) =
            MaintenanceScheduler::new(MaintenanceSchedulerConfig::default(), tx);

        let window = MaintenanceWindow::new(
            "Empty",
            MaintenanceType::Planned,
            Utc::now() + chrono::Duration::hours(1),
            Utc::now() + chrono::Duration::hours(2),
            HashSet::new(),
        );

        let result = scheduler.schedule_maintenance(window).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_node_maintenance_lookup() {
        let (tx, _rx) = mpsc::channel(100);
        let (scheduler, _drain_rx) =
            MaintenanceScheduler::new(MaintenanceSchedulerConfig::default(), tx);

        let node_id = WorkerId::new();
        let mut nodes = HashSet::new();
        nodes.insert(node_id);

        let window = MaintenanceWindow::new(
            "Test",
            MaintenanceType::Planned,
            Utc::now() + chrono::Duration::hours(1),
            Utc::now() + chrono::Duration::hours(2),
            nodes,
        );

        scheduler.schedule_maintenance(window).await.unwrap();

        let maintenance = scheduler.get_node_maintenance(node_id);
        assert_eq!(maintenance.len(), 1);

        assert!(scheduler.has_upcoming_maintenance(node_id, chrono::Duration::hours(2)));
        assert!(!scheduler.has_upcoming_maintenance(node_id, chrono::Duration::minutes(30)));
    }

    #[tokio::test]
    async fn test_overlapping_detection() {
        let (tx, _rx) = mpsc::channel(100);
        let mut config = MaintenanceSchedulerConfig::default();
        config.allow_overlapping = false;

        let (scheduler, _drain_rx) = MaintenanceScheduler::new(config, tx);

        let node_id = WorkerId::new();
        let mut nodes = HashSet::new();
        nodes.insert(node_id);

        let window1 = MaintenanceWindow::new(
            "First",
            MaintenanceType::Planned,
            Utc::now() + chrono::Duration::hours(1),
            Utc::now() + chrono::Duration::hours(3),
            nodes.clone(),
        );

        scheduler.schedule_maintenance(window1).await.unwrap();

        // Overlapping window
        let window2 = MaintenanceWindow::new(
            "Overlapping",
            MaintenanceType::Planned,
            Utc::now() + chrono::Duration::hours(2),
            Utc::now() + chrono::Duration::hours(4),
            nodes,
        );

        let result = scheduler.schedule_maintenance(window2).await;
        assert!(result.is_err());
    }
}
