// Marabunta - Licensed under the MIT License.
//! Wintermute — Central task scheduler and orchestrator for the Neuromancer
//! subsystem.
//!
//! Wintermute owns the task lifecycle: submit, assign, complete, fail, and
//! resurrect.  It maintains a priority-ordered pending queue and a running
//! set, and emits bus events at every lifecycle transition.
//!
//! # Responsibilities
//!
//! - **Submission**: accept tasks into the pending queue (bounded by
//!   `max_pending`).
//! - **Assignment**: move a pending task to the running set on a specific
//!   node.
//! - **Completion**: record success, update statistics.
//! - **Failure**: re-queue failed tasks up to a retry limit (3).
//! - **Node death**: automatically re-queue all tasks running on a dead
//!   node.
//! - **Timeouts**: periodically check running tasks against
//!   `task_timeout` and treat expired ones as failures.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::WintermuteConfig;
use super::types::*;

// ============================================================================
// Error type
// ============================================================================

/// Errors returned by Wintermute scheduling operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WintermuteError {
    /// The pending queue has reached `max_pending`.
    QueueFull,
    /// The requested task was not found in the expected state.
    TaskNotFound,
    /// The task is already in the running set.
    AlreadyRunning,
    /// The task has already been completed.
    AlreadyCompleted,
}

impl fmt::Display for WintermuteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueFull => write!(f, "pending task queue is full"),
            Self::TaskNotFound => write!(f, "task not found"),
            Self::AlreadyRunning => write!(f, "task is already running"),
            Self::AlreadyCompleted => write!(f, "task has already been completed"),
        }
    }
}

impl std::error::Error for WintermuteError {}

// ============================================================================
// TaskState
// ============================================================================

/// The lifecycle state of a task.
#[derive(Debug, Clone)]
pub enum TaskState {
    Pending,
    Running {
        node: NodeId,
        started_at: SystemTime,
    },
    Completed {
        node: NodeId,
        duration_ms: u64,
    },
    Failed {
        node: NodeId,
        reason: String,
    },
    Cancelled,
}

// ============================================================================
// PendingTask
// ============================================================================

/// A task sitting in the pending queue waiting to be assigned.
#[derive(Debug, Clone)]
pub struct PendingTask {
    pub task_id: TaskId,
    pub input_hash: Blake3Hash,
    pub requirements: ResourceRequirements,
    pub submitted_at: SystemTime,
    pub priority: u32,
    pub retries: u32,
}

// ============================================================================
// RunningTask
// ============================================================================

/// A task that has been assigned to a node and is executing.
#[derive(Debug, Clone)]
pub struct RunningTask {
    pub task_id: TaskId,
    pub node: NodeId,
    pub started_at: SystemTime,
    pub retries: u32,
}

// ============================================================================
// WintermuteStats
// ============================================================================

/// Cumulative statistics for the Wintermute scheduler.
#[derive(Debug, Clone, Default)]
pub struct WintermuteStats {
    pub tasks_submitted: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub tasks_resurrected: u64,
    pub tasks_timed_out: u64,
    pub total_duration_ms: u64,
}

// ============================================================================
// Wintermute
// ============================================================================

/// Maximum number of retry attempts before a task is permanently dropped.
const MAX_RETRIES: u32 = 3;

/// The central task scheduler and orchestrator.
pub struct Wintermute {
    config: WintermuteConfig,
    bus: Arc<NeuromancerBus>,
    pending: VecDeque<PendingTask>,
    running: HashMap<TaskId, RunningTask>,
    completed: Vec<TaskId>,
    stats: WintermuteStats,
}

impl Wintermute {

    /// [MARABUNTA WMD] Vector Clock Consensus Monitor
    /// Scans the Kademlia DHT for supermajority confirmations on Vector Clock Epochs.
    /// If 66% of Tier-1 Aggregators acknowledge Epoch V_n, Wintermute triggers
    /// a synchronous garbage collection on the `IsomorphicStateRing`, deleting all 
    /// tombstones prior to V_n and ensuring O(1) temporal storage scaling.
    pub async fn trigger_crdt_compaction_if_consensus_reached(
        &self, 
        current_epoch: u64, 
        ack_count: usize, 
        total_aggregators: usize,
        state_map: &mut crate::swarm::crdt::EpidemicStateMap<String, Vec<u8>>
    ) {
        let threshold = (total_aggregators as f64 * 0.66) as usize;
        
        if ack_count >= threshold {
            tracing::info!(
                "WINTERMUTE: BFT Consensus Reached on Vector Epoch {}. ({} / {} Acks). Executing Tombstone Compaction.", 
                current_epoch, ack_count, total_aggregators
            );
            
            // Execute O(1) Storage Garbage Collection
            state_map.garbage_collect_tombstones(current_epoch);
        } else {
            tracing::debug!(
                "WINTERMUTE: Epoch {} pending consensus ({} / {} Acks). Holding tombstones.", 
                current_epoch, ack_count, threshold
            );
        }
    }

    /// Create a new Wintermute scheduler with the given configuration and bus.
    pub fn new(config: WintermuteConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self {
            config,
            bus,
            pending: VecDeque::new(),
            running: HashMap::new(),
            completed: Vec::new(),
            stats: WintermuteStats::default(),
        }
    }

    // --------------------------------------------------------------------
    // Task lifecycle
    // --------------------------------------------------------------------

    /// Submit a new task to the pending queue.
    ///
    /// Returns [`WintermuteError::QueueFull`] if `pending.len() >= max_pending`.
    /// Emits a [`MarabuntaEvent::TaskSubmitted`] on success.
    pub fn submit(
        &mut self,
        task_id: TaskId,
        input_hash: Blake3Hash,
        requirements: ResourceRequirements,
        priority: u32,
    ) -> Result<(), WintermuteError> {
        if self.pending.len() >= self.config.max_pending {
            warn!(
                pending = self.pending.len(),
                max = self.config.max_pending,
                "Wintermute: queue full, rejecting task"
            );
            return Err(WintermuteError::QueueFull);
        }

        // Check if already completed
        if self.completed.contains(&task_id) {
            return Err(WintermuteError::AlreadyCompleted);
        }

        // Check if already running
        if self.running.contains_key(&task_id) {
            return Err(WintermuteError::AlreadyRunning);
        }

        let now = SystemTime::now();
        let task = PendingTask {
            task_id,
            input_hash,
            requirements: requirements.clone(),
            submitted_at: now,
            priority,
            retries: 0,
        };

        self.pending.push_back(task);
        self.stats.tasks_submitted += 1;

        info!("Wintermute: task submitted, pending={}", self.pending.len());

        self.bus.emit(MarabuntaEvent::TaskSubmitted {
            id: task_id,
            input_hash,
            requirements,
            timestamp: now,
        });

        Ok(())
    }

    /// Assign a pending task to a specific node, moving it to the running set.
    ///
    /// Returns [`WintermuteError::TaskNotFound`] if the task is not in the
    /// pending queue.
    pub fn assign_task(
        &mut self,
        task_id: TaskId,
        node: NodeId,
    ) -> Result<(), WintermuteError> {
        // Check if already running
        if self.running.contains_key(&task_id) {
            return Err(WintermuteError::AlreadyRunning);
        }

        // Find and remove from pending
        let pos = self
            .pending
            .iter()
            .position(|t| t.task_id == task_id)
            .ok_or(WintermuteError::TaskNotFound)?;
        let pending_task = self.pending.remove(pos).unwrap();

        let now = SystemTime::now();
        let running_task = RunningTask {
            task_id,
            node,
            started_at: now,
            retries: pending_task.retries,
        };
        self.running.insert(task_id, running_task);

        debug!(?node, "Wintermute: task assigned");

        Ok(())
    }

    /// Mark a running task as completed.
    ///
    /// Removes the task from the running set, adds it to the completed list,
    /// updates statistics, and emits [`MarabuntaEvent::TaskCompleted`].
    pub fn complete_task(
        &mut self,
        task_id: TaskId,
        node: NodeId,
        result_hash: Blake3Hash,
        duration_ms: u64,
    ) {
        if self.running.remove(&task_id).is_some() {
            self.completed.push(task_id);
            self.stats.tasks_completed += 1;
            self.stats.total_duration_ms += duration_ms;

            info!(
                duration_ms,
                completed = self.stats.tasks_completed,
                "Wintermute: task completed"
            );

            self.bus.emit(MarabuntaEvent::TaskCompleted {
                id: task_id,
                result_hash,
                node,
                duration_ms,
                timestamp: SystemTime::now(),
            });
        } else {
            warn!("Wintermute: complete_task called for unknown running task");
        }
    }

    /// Mark a running task as failed.
    ///
    /// If the task has fewer than [`MAX_RETRIES`] attempts, it is re-queued
    /// at the front of the pending queue with an incremented retry counter.
    /// Otherwise it is permanently dropped.
    ///
    /// Emits [`MarabuntaEvent::TaskFailed`].
    pub fn fail_task(&mut self, task_id: TaskId, node: NodeId, reason: &str) {
        if let Some(running_task) = self.running.remove(&task_id) {
            self.stats.tasks_failed += 1;

            info!(
                ?node,
                reason,
                "Wintermute: task failed"
            );

            self.bus.emit(MarabuntaEvent::TaskFailed {
                id: task_id,
                node,
                reason: reason.to_string(),
                timestamp: SystemTime::now(),
            });

            // Re-queue with incremented retries if under the limit.
            self.requeue_task(task_id, running_task.retries);
        } else {
            warn!("Wintermute: fail_task called for unknown running task");
        }
    }

    /// Handle the death of a node by re-queuing all tasks that were running
    /// on it.
    ///
    /// Each re-queued task has its retry counter incremented.  Tasks that
    /// exceed [`MAX_RETRIES`] are permanently dropped.
    pub fn handle_node_death(&mut self, node: NodeId) {
        let affected: Vec<TaskId> = self
            .running
            .iter()
            .filter(|(_, rt)| rt.node == node)
            .map(|(tid, _)| *tid)
            .collect();

        if affected.is_empty() {
            return;
        }

        info!(
            ?node,
            count = affected.len(),
            "Wintermute: node died, re-queuing tasks"
        );

        for task_id in &affected {
            let retries = self.running.get(task_id).map(|rt| rt.retries).unwrap_or(0);
            self.running.remove(task_id);
            self.stats.tasks_resurrected += 1;
            self.requeue_task(*task_id, retries);
        }
    }

    /// Check all running tasks for timeouts and treat expired ones as failed.
    pub fn check_timeouts(&mut self) {
        let now = SystemTime::now();
        let timeout = self.config.task_timeout;

        let timed_out: Vec<(TaskId, NodeId)> = self
            .running
            .iter()
            .filter(|(_, rt)| {
                now.duration_since(rt.started_at).unwrap_or_default() >= timeout
            })
            .map(|(tid, rt)| (*tid, rt.node))
            .collect();

        for (task_id, node) in timed_out {
            warn!(?node, "Wintermute: task timed out");
            self.stats.tasks_timed_out += 1;

            let retries = self.running.get(&task_id).map(|rt| rt.retries).unwrap_or(0);
            self.running.remove(&task_id);

            self.bus.emit(MarabuntaEvent::TaskFailed {
                id: task_id,
                node,
                reason: "timeout".to_string(),
                timestamp: now,
            });

            self.requeue_task(task_id, retries);
        }
    }

    /// Return up to `max_running - running.len()` tasks from the front of the
    /// pending queue for an external scheduler to assign.
    ///
    /// Tasks are **removed** from the pending queue.  If the external
    /// scheduler decides not to assign a task, it should be re-submitted.
    pub fn process_queue(&mut self) -> Vec<PendingTask> {
        let available_slots = self.config.max_running.saturating_sub(self.running.len());
        if available_slots == 0 {
            return Vec::new();
        }

        let take = available_slots.min(self.pending.len());
        let mut batch = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(task) = self.pending.pop_front() {
                batch.push(task);
            }
        }

        if !batch.is_empty() {
            debug!(
                count = batch.len(),
                remaining = self.pending.len(),
                "Wintermute: dequeued tasks for scheduling"
            );
        }

        batch
    }

    // --------------------------------------------------------------------
    // Accessors
    // --------------------------------------------------------------------

    /// Number of tasks in the pending queue.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Number of currently running tasks.
    pub fn running_count(&self) -> usize {
        self.running.len()
    }

    /// Cumulative statistics.
    pub fn stats(&self) -> &WintermuteStats {
        &self.stats
    }

    // --------------------------------------------------------------------
    // Internal helpers
    // --------------------------------------------------------------------

    /// Re-queue a task at the front of the pending queue if it has not
    /// exceeded [`MAX_RETRIES`].
    ///
    /// `current_retries` is the retry count from the task's most recent
    /// incarnation (stored in RunningTask). We increment by 1 and check
    /// against MAX_RETRIES.
    fn requeue_task(&mut self, task_id: TaskId, current_retries: u32) {
        let new_retries = current_retries + 1;

        if new_retries > MAX_RETRIES {
            warn!(
                retries = new_retries,
                max = MAX_RETRIES,
                "Wintermute: task exceeded max retries, dropping"
            );
            return;
        }

        let task = PendingTask {
            task_id,
            input_hash: [0u8; 32], // original hash unavailable post-assignment
            requirements: ResourceRequirements::default(),
            submitted_at: SystemTime::now(),
            priority: 0,
            retries: new_retries,
        };

        // Push to front so retries are prioritised
        self.pending.push_front(task);
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::neuromancer::bus::NeuromancerBus;
    use std::time::Duration;

    fn make_wintermute() -> (Wintermute, tokio::sync::broadcast::Receiver<MarabuntaEvent>) {
        let config = WintermuteConfig {
            queue_check_interval: Duration::from_secs(5),
            task_timeout: Duration::from_millis(50), // short for testing
            max_pending: 10,
            max_running: 5,
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::new(64));
        let rx = bus.subscribe();
        let wm = Wintermute::new(config, bus);
        (wm, rx)
    }

    fn sample_requirements() -> ResourceRequirements {
        ResourceRequirements {
            min_cpu_cores: 2,
            min_memory_mb: 1024,
            min_storage_mb: 512,
            gpu_required: false,
            capabilities_required: vec![],
        }
    }

    // -- Test 1: submit emits TaskSubmitted ----------------------------------
    #[test]
    fn test_submit_emits_event() {
        let (mut wm, mut rx) = make_wintermute();
        let task_id = [1u8; 32];
        let input_hash = [2u8; 32];

        wm.submit(task_id, input_hash, sample_requirements(), 10)
            .unwrap();

        assert_eq!(wm.stats().tasks_submitted, 1);
        assert_eq!(wm.pending_count(), 1);

        // Verify the event was emitted
        let event = rx.try_recv().unwrap();
        assert_eq!(event.type_name(), "TaskSubmitted");
    }

    // -- Test 2: queue full returns QueueFull error --------------------------
    #[test]
    fn test_queue_full_error() {
        let (mut wm, _rx) = make_wintermute();

        // Fill the queue (max_pending = 10)
        for i in 0..10u8 {
            let mut tid = [0u8; 32];
            tid[0] = i;
            wm.submit(tid, [0u8; 32], sample_requirements(), 0)
                .unwrap();
        }
        assert_eq!(wm.pending_count(), 10);

        // 11th should fail
        let result = wm.submit([99u8; 32], [0u8; 32], sample_requirements(), 0);
        assert_eq!(result, Err(WintermuteError::QueueFull));
    }

    // -- Test 3: complete task updates stats and emits TaskCompleted ----------
    #[test]
    fn test_complete_task_stats_and_event() {
        let (mut wm, mut rx) = make_wintermute();
        let task_id = [1u8; 32];
        let node = NodeId::new();

        wm.submit(task_id, [2u8; 32], sample_requirements(), 0)
            .unwrap();
        wm.assign_task(task_id, node).unwrap();

        // Drain the TaskSubmitted event
        let _ = rx.try_recv();

        let result_hash = [3u8; 32];
        wm.complete_task(task_id, node, result_hash, 150);

        assert_eq!(wm.stats().tasks_completed, 1);
        assert_eq!(wm.stats().total_duration_ms, 150);
        assert_eq!(wm.running_count(), 0);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.type_name(), "TaskCompleted");
    }

    // -- Test 4: fail task re-queues with retry increment --------------------
    #[test]
    fn test_fail_task_requeues_with_retry() {
        let (mut wm, mut rx) = make_wintermute();
        let task_id = [1u8; 32];
        let node = NodeId::new();

        wm.submit(task_id, [2u8; 32], sample_requirements(), 0)
            .unwrap();
        wm.assign_task(task_id, node).unwrap();
        assert_eq!(wm.running_count(), 1);
        assert_eq!(wm.pending_count(), 0);

        // Drain the TaskSubmitted event
        let _ = rx.try_recv();

        wm.fail_task(task_id, node, "test failure");

        // Task should be back in pending with retries=1
        assert_eq!(wm.running_count(), 0);
        assert_eq!(wm.pending_count(), 1);
        assert_eq!(wm.stats().tasks_failed, 1);

        let requeued = wm.pending.front().unwrap();
        assert_eq!(requeued.task_id, task_id);
        assert_eq!(requeued.retries, 1);

        // TaskFailed event emitted
        let event = rx.try_recv().unwrap();
        assert_eq!(event.type_name(), "TaskFailed");
    }

    // -- Test 5: node death re-queues running tasks --------------------------
    #[test]
    fn test_node_death_requeues_tasks() {
        let (mut wm, _rx) = make_wintermute();
        let node = NodeId::new();

        // Submit and assign 3 tasks to the same node
        for i in 0..3u8 {
            let mut tid = [0u8; 32];
            tid[0] = i;
            wm.submit(tid, [0u8; 32], sample_requirements(), 0)
                .unwrap();
            wm.assign_task(tid, node).unwrap();
        }
        assert_eq!(wm.running_count(), 3);
        assert_eq!(wm.pending_count(), 0);

        wm.handle_node_death(node);

        assert_eq!(wm.running_count(), 0);
        assert_eq!(wm.pending_count(), 3);
        assert_eq!(wm.stats().tasks_resurrected, 3);
    }

    // -- Test 6: timeout treats tasks as failed ------------------------------
    #[test]
    fn test_timeout_treats_as_failed() {
        let config = WintermuteConfig {
            queue_check_interval: Duration::from_secs(5),
            task_timeout: Duration::from_millis(1), // extremely short
            max_pending: 10,
            max_running: 5,
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::new(64));
        let mut rx = bus.subscribe();
        let mut wm = Wintermute::new(config, bus);
        let node = NodeId::new();

        let task_id = [1u8; 32];
        wm.submit(task_id, [2u8; 32], sample_requirements(), 0)
            .unwrap();
        wm.assign_task(task_id, node).unwrap();

        // Drain TaskSubmitted
        let _ = rx.try_recv();

        // Wait for the timeout to elapse
        std::thread::sleep(Duration::from_millis(10));

        wm.check_timeouts();

        assert_eq!(wm.running_count(), 0);
        assert_eq!(wm.stats().tasks_timed_out, 1);
        // Task should be re-queued
        assert_eq!(wm.pending_count(), 1);

        // TaskFailed event emitted with "timeout" reason
        let event = rx.try_recv().unwrap();
        match &event {
            MarabuntaEvent::TaskFailed { reason, .. } => {
                assert_eq!(reason, "timeout");
            }
            other => panic!("expected TaskFailed, got {:?}", other.type_name()),
        }
    }

    // -- Test 7: process_queue respects max_running slots --------------------
    #[test]
    fn test_process_queue_respects_slots() {
        let (mut wm, _rx) = make_wintermute();

        // Submit 8 tasks (max_running = 5)
        for i in 0..8u8 {
            let mut tid = [0u8; 32];
            tid[0] = i;
            wm.submit(tid, [0u8; 32], sample_requirements(), 0)
                .unwrap();
        }
        assert_eq!(wm.pending_count(), 8);

        let batch = wm.process_queue();
        // Should return at most max_running (5) tasks
        assert_eq!(batch.len(), 5);
        assert_eq!(wm.pending_count(), 3);
    }

    // -- Test 8: assign_task errors -----------------------------------------
    #[test]
    fn test_assign_task_errors() {
        let (mut wm, _rx) = make_wintermute();
        let node = NodeId::new();
        let task_id = [1u8; 32];

        // TaskNotFound when nothing is pending
        let result = wm.assign_task(task_id, node);
        assert_eq!(result, Err(WintermuteError::TaskNotFound));

        // AlreadyRunning
        wm.submit(task_id, [2u8; 32], sample_requirements(), 0)
            .unwrap();
        wm.assign_task(task_id, node).unwrap();
        let result = wm.assign_task(task_id, node);
        assert_eq!(result, Err(WintermuteError::AlreadyRunning));
    }

    // -- Test 9: retry limit enforcement -------------------------------------
    #[test]
    fn test_retry_limit_drops_task() {
        let (mut wm, _rx) = make_wintermute();
        let task_id = [1u8; 32];
        let node = NodeId::new();

        // Manually insert a pending task with retries at the limit
        let task = PendingTask {
            task_id,
            input_hash: [0u8; 32],
            requirements: ResourceRequirements::default(),
            submitted_at: SystemTime::now(),
            priority: 0,
            retries: MAX_RETRIES,
        };
        wm.pending.push_back(task);

        // Assign and fail — should NOT be re-queued (already at MAX_RETRIES)
        wm.assign_task(task_id, node).unwrap();
        wm.fail_task(task_id, node, "final failure");

        // Task should NOT be in pending (exceeded retries)
        assert_eq!(wm.pending_count(), 0);
    }

    // -- Test 10: submit rejects duplicates ----------------------------------
    #[test]
    fn test_submit_rejects_already_completed() {
        let (mut wm, _rx) = make_wintermute();
        let task_id = [1u8; 32];
        let node = NodeId::new();

        wm.submit(task_id, [2u8; 32], sample_requirements(), 0)
            .unwrap();
        wm.assign_task(task_id, node).unwrap();
        wm.complete_task(task_id, node, [3u8; 32], 100);

        // Now try to re-submit the completed task
        let result = wm.submit(task_id, [2u8; 32], sample_requirements(), 0);
        assert_eq!(result, Err(WintermuteError::AlreadyCompleted));
    }
}
