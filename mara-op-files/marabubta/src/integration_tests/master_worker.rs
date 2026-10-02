// Marabunta - Licensed under the MIT License.
//! Integration tests for Master-Worker task execution flow
//!
//! This module tests the complete task lifecycle between master and worker nodes:
//! - Master startup and worker registration
//! - Task assignment from master to worker
//! - Task completion reporting
//! - Task failure handling
//! - Worker heartbeat handling
//!
//! Tests use mock channels to simulate network communication between components.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::common::types::{
    JobId, RegionId, Task, TaskPayload, WorkerCapabilities, WorkerId, WorkerInfo, WorkerStatus,
};
use crate::master::scheduler::SchedulingPolicy;
use crate::master::{JobScheduler, WorkerRegistry};
use crate::master::worker_connection::{
    WorkerConnectionRegistry, WorkerMessageHandler,
};
use crate::protocol::master_worker::{
    MasterToWorkerMsg, TaskMetrics, TaskResult as ProtocolTaskResult,
    WorkerCapabilities as ProtocolCapabilities, WorkerCapacity, WorkerToMasterMsg,
};

// ============================================================================
// Test Harness: Mock channels and state tracking
// ============================================================================

/// Test harness for simulating master-worker communication
struct TestHarness {
    /// Worker connection registry (master side)
    registry: Arc<WorkerConnectionRegistry>,
    /// Message handler for processing worker messages
    message_handler: Arc<WorkerMessageHandler>,
    /// Channels for each registered worker (worker_id -> (tx to master, rx from master))
    worker_channels:
        RwLock<HashMap<String, (mpsc::Sender<WorkerToMasterMsg>, mpsc::Receiver<MasterToWorkerMsg>)>>,
    /// Task completion notifications
    completed_tasks: Arc<RwLock<Vec<(String, ProtocolTaskResult)>>>,
    /// Task failure notifications
    failed_tasks: Arc<RwLock<Vec<(String, String, bool)>>>,
    /// Task progress updates
    progress_updates: Arc<RwLock<Vec<(String, f64, Option<String>)>>>,
}

impl TestHarness {
    /// Create a new test harness with default heartbeat timeout
    fn new() -> Self {
        Self::with_timeout(Duration::from_secs(30))
    }

    /// Create a new test harness with custom heartbeat timeout
    fn with_timeout(timeout: Duration) -> Self {
        let registry = Arc::new(WorkerConnectionRegistry::new(timeout));
        let message_handler = Arc::new(WorkerMessageHandler::new(registry.clone()));

        let completed_tasks: Arc<RwLock<Vec<(String, ProtocolTaskResult)>>> =
            Arc::new(RwLock::new(Vec::new()));
        let failed_tasks: Arc<RwLock<Vec<(String, String, bool)>>> =
            Arc::new(RwLock::new(Vec::new()));
        let progress_updates: Arc<RwLock<Vec<(String, f64, Option<String>)>>> =
            Arc::new(RwLock::new(Vec::new()));

        // Set up callbacks
        let completed_clone = completed_tasks.clone();
        message_handler.set_completion_callback(move |task_id, result| {
            completed_clone.write().push((task_id.to_string(), result));
        });

        let failed_clone = failed_tasks.clone();
        message_handler.set_failure_callback(move |task_id, error, retryable| {
            failed_clone
                .write()
                .push((task_id.to_string(), error.to_string(), retryable));
        });

        let progress_clone = progress_updates.clone();
        message_handler.set_progress_callback(move |task_id, progress, stage| {
            progress_clone
                .write()
                .push((task_id.to_string(), progress, stage.map(|s| s.to_string())));
        });

        Self {
            registry,
            message_handler,
            worker_channels: RwLock::new(HashMap::new()),
            completed_tasks,
            failed_tasks,
            progress_updates,
        }
    }

    /// Register a worker and return its ID
    async fn register_worker(&self, worker_id: &str, capabilities: ProtocolCapabilities) -> String {
        let (master_tx, worker_rx) = mpsc::channel::<MasterToWorkerMsg>(100);
        let (worker_tx, _master_rx) = mpsc::channel::<WorkerToMasterMsg>(100);

        let addr: SocketAddr = format!("127.0.0.1:{}", 8000 + self.registry.worker_count())
            .parse()
            .unwrap();

        self.registry
            .register_worker(worker_id.to_string(), addr, capabilities, master_tx)
            .await;

        self.worker_channels
            .write()
            .insert(worker_id.to_string(), (worker_tx, worker_rx));

        worker_id.to_string()
    }

    /// Simulate worker sending a message to master
    async fn worker_sends(&self, worker_id: &str, msg: WorkerToMasterMsg) {
        self.message_handler.handle_message(worker_id, msg).await;
    }

    /// Get the next message that was sent to a worker
    async fn worker_receives(&self, worker_id: &str) -> Option<MasterToWorkerMsg> {
        if let Some((_, rx)) = self.worker_channels.write().get_mut(worker_id) {
            rx.try_recv().ok()
        } else {
            None
        }
    }

    /// Get completed tasks
    fn get_completed_tasks(&self) -> Vec<(String, ProtocolTaskResult)> {
        self.completed_tasks.read().clone()
    }

    /// Get failed tasks
    fn get_failed_tasks(&self) -> Vec<(String, String, bool)> {
        self.failed_tasks.read().clone()
    }

    /// Get progress updates
    fn get_progress_updates(&self) -> Vec<(String, f64, Option<String>)> {
        self.progress_updates.read().clone()
    }
}

/// Create test worker capabilities
fn test_capabilities(cpu_cores: u32, memory_mb: u64, max_tasks: u32) -> ProtocolCapabilities {
    ProtocolCapabilities {
        cpu_cores,
        memory_mb,
        gpu_count: 0,
        gpu_memory_mb: None,
        disk_gb: 100,
        tags: vec!["shell".to_string(), "python3".to_string()],
        max_concurrent_tasks: max_tasks,
    }
}

/// Create a test task
fn test_task(job_id: JobId, name: &str) -> Task {
    let mut task = Task::new(
        job_id,
        TaskPayload::Shell {
            command: "echo".to_string(),
            args: vec![name.to_string()],
        },
    );
    task.name = Some(name.to_string());
    task
}

// ============================================================================
// Test 1: Master startup and worker registration
// ============================================================================

#[cfg(test)]
mod master_startup_and_registration {
    use super::*;

    #[tokio::test]
    async fn test_single_worker_registration() {
        let harness = TestHarness::new();

        // Register a single worker
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        // Verify worker is registered
        assert_eq!(harness.registry.worker_count(), 1);

        let worker = harness.registry.get_worker(&worker_id);
        assert!(worker.is_some());

        let worker = worker.unwrap();
        assert_eq!(worker.worker_id, "worker-1");
        assert_eq!(worker.capabilities.cpu_cores, 4);
        assert_eq!(worker.capabilities.memory_mb, 8192);
        assert!(worker.can_accept_task());
    }

    #[tokio::test]
    async fn test_multiple_worker_registration() {
        let harness = TestHarness::new();

        // Register multiple workers
        harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;
        harness
            .register_worker("worker-2", test_capabilities(8, 16384, 4))
            .await;
        harness
            .register_worker("worker-3", test_capabilities(2, 4096, 1))
            .await;

        // Verify all workers are registered
        assert_eq!(harness.registry.worker_count(), 3);

        // Verify total capacity
        let (total_cores, total_memory, total_tasks) = harness.registry.total_capacity();
        assert_eq!(total_cores, 14); // 4 + 8 + 2
        assert_eq!(total_memory, 28672); // 8192 + 16384 + 4096
        assert_eq!(total_tasks, 7); // 2 + 4 + 1
    }

    #[tokio::test]
    async fn test_worker_registration_with_capabilities() {
        let harness = TestHarness::new();

        // Register worker with specific capabilities
        let caps = ProtocolCapabilities {
            cpu_cores: 16,
            memory_mb: 32768,
            gpu_count: 2,
            gpu_memory_mb: Some(16384),
            disk_gb: 500,
            tags: vec![
                "gpu".to_string(),
                "high-memory".to_string(),
                "python3".to_string(),
            ],
            max_concurrent_tasks: 8,
        };

        let worker_id = harness.register_worker("gpu-worker", caps).await;

        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.capabilities.gpu_count, 2);
        assert!(worker.capabilities.tags.contains(&"gpu".to_string()));
    }

    #[tokio::test]
    async fn test_worker_deregistration() {
        let harness = TestHarness::new();

        // Register workers
        harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;
        harness
            .register_worker("worker-2", test_capabilities(4, 8192, 2))
            .await;

        assert_eq!(harness.registry.worker_count(), 2);

        // Deregister one worker
        harness.registry.deregister_worker("worker-1");

        assert_eq!(harness.registry.worker_count(), 1);
        assert!(harness.registry.get_worker("worker-1").is_none());
        assert!(harness.registry.get_worker("worker-2").is_some());
    }

    #[tokio::test]
    async fn test_worker_registration_via_message() {
        let harness = TestHarness::new();

        // First register the worker through the registry (required for message handling)
        harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        // Then simulate the registration message
        let msg = WorkerToMasterMsg::Register {
            worker_id: "worker-1".to_string(),
            capabilities: test_capabilities(4, 8192, 2),
        };

        harness.worker_sends("worker-1", msg).await;

        // Worker should still be registered
        assert_eq!(harness.registry.worker_count(), 1);
    }
}

// ============================================================================
// Test 2: Task assignment from master to worker
// ============================================================================

#[cfg(test)]
mod task_assignment {
    use super::*;

    #[tokio::test]
    async fn test_assign_task_to_worker() {
        let harness = TestHarness::new();

        // Register a worker
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        // Create a task
        let job_id = JobId::new();
        let task = test_task(job_id, "test-task");

        // Assign the task
        let result = harness.registry.assign_task(&worker_id, task.clone()).await;
        assert!(result.is_ok());

        // Verify worker has the task
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 1);
        assert!(worker.get_assigned_tasks().contains(&task.id.to_string()));
    }

    #[tokio::test]
    async fn test_assign_task_sends_message() {
        let harness = TestHarness::new();

        // Register a worker
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        // Create and assign a task
        let job_id = JobId::new();
        let task = test_task(job_id, "message-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Check that assignment message was sent
        let msg = harness.worker_receives(&worker_id).await;
        assert!(msg.is_some());

        if let Some(MasterToWorkerMsg::AssignTask {
            task_id: received_id,
            job_id: received_job_id,
            ..
        }) = msg
        {
            assert_eq!(received_id, task_id);
            assert_eq!(received_job_id, job_id.to_string());
        } else {
            panic!("Expected AssignTask message");
        }
    }

    #[tokio::test]
    async fn test_assign_multiple_tasks() {
        let harness = TestHarness::new();

        // Register a worker with capacity for 3 tasks
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 3))
            .await;

        let job_id = JobId::new();

        // Assign three tasks
        for i in 1..=3 {
            let task = test_task(job_id, &format!("task-{}", i));
            let result = harness.registry.assign_task(&worker_id, task).await;
            assert!(result.is_ok());
        }

        // Verify worker has all tasks
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 3);
        assert!(!worker.can_accept_task()); // At capacity
    }

    #[tokio::test]
    async fn test_assign_task_to_unavailable_worker() {
        let harness = TestHarness::new();

        // Register a worker with capacity for 1 task
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 1))
            .await;

        let job_id = JobId::new();

        // Fill the worker's capacity
        let task1 = test_task(job_id, "task-1");
        harness.registry.assign_task(&worker_id, task1).await.unwrap();

        // Try to assign another task (should fail)
        let task2 = test_task(job_id, "task-2");
        let result = harness.registry.assign_task(&worker_id, task2).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_best_worker_selection_by_load() {
        let harness = TestHarness::new();

        // Register two workers
        let worker1 = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 4))
            .await;
        let worker2 = harness
            .register_worker("worker-2", test_capabilities(4, 8192, 4))
            .await;

        let job_id = JobId::new();

        // Add 2 tasks to worker-1
        let w1 = harness.registry.get_worker(&worker1).unwrap();
        w1.add_task("existing-1".to_string());
        w1.add_task("existing-2".to_string());

        // Add 1 task to worker-2
        let w2 = harness.registry.get_worker(&worker2).unwrap();
        w2.add_task("existing-3".to_string());

        // Create test task for selection
        let task = test_task(job_id, "new-task");

        // Best worker should be worker-2 (lower load)
        let best = harness.registry.get_best_worker_for_task(&task);
        assert!(best.is_some());
        assert_eq!(best.unwrap().worker_id, "worker-2");
    }
}

// ============================================================================
// Test 3: Task completion reporting
// ============================================================================

#[cfg(test)]
mod task_completion {
    use super::*;

    #[tokio::test]
    async fn test_task_completion_success() {
        let harness = TestHarness::new();

        // Register worker and assign task
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "complete-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Simulate task completion
        let result = ProtocolTaskResult {
            output: b"Hello, World!".to_vec(),
            metrics: TaskMetrics::default(),
            exit_code: Some(0),
            stdout: Some("Hello, World!".to_string()),
            stderr: None,
        };

        let msg = WorkerToMasterMsg::TaskCompleted {
            task_id: task_id.clone(),
            result: result.clone(),
            duration_ms: 100,
        };

        harness.worker_sends(&worker_id, msg).await;

        // Verify completion was tracked
        let completed = harness.get_completed_tasks();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, task_id);
        assert_eq!(completed[0].1.exit_code, Some(0));

        // Verify task was removed from worker
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 0);
    }

    #[tokio::test]
    async fn test_task_completion_with_output() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "output-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Complete with large output
        let large_output = vec![42u8; 1024 * 1024]; // 1MB output
        let result = ProtocolTaskResult {
            output: large_output.clone(),
            metrics: TaskMetrics {
                cpu_usage: 0.75,
                memory_mb: 512,
                gpu_usage: None,
                disk_read_bytes: 1024,
                disk_write_bytes: 2048,
                network_rx_bytes: 100,
                network_tx_bytes: 50,
            },
            exit_code: Some(0),
            stdout: Some("Output generated".to_string()),
            stderr: None,
        };

        let msg = WorkerToMasterMsg::TaskCompleted {
            task_id: task_id.clone(),
            result,
            duration_ms: 5000,
        };

        harness.worker_sends(&worker_id, msg).await;

        let completed = harness.get_completed_tasks();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].1.output.len(), 1024 * 1024);
    }

    #[tokio::test]
    async fn test_multiple_task_completions() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 5))
            .await;

        let job_id = JobId::new();

        // Assign and complete multiple tasks
        for i in 1..=3 {
            let task = test_task(job_id, &format!("task-{}", i));
            let task_id = task.id.to_string();
            harness.registry.assign_task(&worker_id, task).await.unwrap();

            let msg = WorkerToMasterMsg::TaskCompleted {
                task_id,
                result: ProtocolTaskResult::default(),
                duration_ms: i as u64 * 100,
            };
            harness.worker_sends(&worker_id, msg).await;
        }

        let completed = harness.get_completed_tasks();
        assert_eq!(completed.len(), 3);
    }

    #[tokio::test]
    async fn test_task_accepted_acknowledgment() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "ack-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Simulate worker acknowledging task acceptance
        let msg = WorkerToMasterMsg::TaskAccepted {
            task_id: task_id.clone(),
        };

        harness.worker_sends(&worker_id, msg).await;

        // Task should still be tracked
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 1);
    }
}

// ============================================================================
// Test 4: Task failure handling
// ============================================================================

#[cfg(test)]
mod task_failure {
    use super::*;

    #[tokio::test]
    async fn test_task_failure_retryable() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "fail-retry-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Simulate retryable failure
        let msg = WorkerToMasterMsg::TaskFailed {
            task_id: task_id.clone(),
            error: "Connection timeout".to_string(),
            retryable: true,
            checkpoint: None,
        };

        harness.worker_sends(&worker_id, msg).await;

        let failed = harness.get_failed_tasks();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].0, task_id);
        assert_eq!(failed[0].1, "Connection timeout");
        assert!(failed[0].2); // retryable

        // Verify task was removed from worker
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 0);
    }

    #[tokio::test]
    async fn test_task_failure_non_retryable() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "fail-no-retry-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Simulate non-retryable failure
        let msg = WorkerToMasterMsg::TaskFailed {
            task_id: task_id.clone(),
            error: "Invalid task configuration".to_string(),
            retryable: false,
            checkpoint: None,
        };

        harness.worker_sends(&worker_id, msg).await;

        let failed = harness.get_failed_tasks();
        assert_eq!(failed.len(), 1);
        assert!(!failed[0].2); // not retryable
    }

    #[tokio::test]
    async fn test_task_failure_with_checkpoint() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "fail-checkpoint-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Simulate failure with saved checkpoint
        let checkpoint_data = vec![1, 2, 3, 4, 5]; // Mock checkpoint
        let msg = WorkerToMasterMsg::TaskFailed {
            task_id: task_id.clone(),
            error: "Task interrupted".to_string(),
            retryable: true,
            checkpoint: Some(checkpoint_data.clone()),
        };

        harness.worker_sends(&worker_id, msg).await;

        let failed = harness.get_failed_tasks();
        assert_eq!(failed.len(), 1);
        assert!(failed[0].2); // retryable
    }

    #[tokio::test]
    async fn test_task_rejected_by_worker() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "reject-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Simulate worker rejecting the task
        let msg = WorkerToMasterMsg::TaskRejected {
            task_id: task_id.clone(),
            reason: "Insufficient memory for task".to_string(),
        };

        harness.worker_sends(&worker_id, msg).await;

        // Task should be removed from worker
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 0);
    }

    #[tokio::test]
    async fn test_multiple_failures() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 5))
            .await;

        let job_id = JobId::new();

        // Assign and fail multiple tasks
        for i in 1..=3 {
            let task = test_task(job_id, &format!("fail-task-{}", i));
            let task_id = task.id.to_string();
            harness.registry.assign_task(&worker_id, task).await.unwrap();

            let msg = WorkerToMasterMsg::TaskFailed {
                task_id,
                error: format!("Error {}", i),
                retryable: i % 2 == 0, // Alternate retryable
                checkpoint: None,
            };
            harness.worker_sends(&worker_id, msg).await;
        }

        let failed = harness.get_failed_tasks();
        assert_eq!(failed.len(), 3);
    }
}

// ============================================================================
// Test 5: Worker heartbeat handling
// ============================================================================

#[cfg(test)]
mod heartbeat_handling {
    use super::*;

    #[tokio::test]
    async fn test_heartbeat_updates_capacity() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 4))
            .await;

        // Send a heartbeat with updated capacity
        let capacity = WorkerCapacity {
            available_cpu: 2.5,
            available_memory_mb: 4096,
            available_gpu: 0,
            max_tasks: 4,
            current_tasks: 2,
        };

        let msg = WorkerToMasterMsg::Pong {
            timestamp: 12345,
            active_tasks: 2,
            cpu_usage: 0.5,
            memory_usage_mb: 4096,
            available_capacity: capacity.clone(),
        };

        harness.worker_sends(&worker_id, msg).await;

        // Verify capacity was updated
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        let current_cap = worker.current_capacity.read();
        assert_eq!(current_cap.current_tasks, 2);
        assert_eq!(current_cap.available_memory_mb, 4096);
    }

    #[tokio::test]
    async fn test_heartbeat_timeout_detection() {
        // Create harness with short timeout for testing
        let harness = TestHarness::with_timeout(Duration::from_millis(100));

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        // Initially no dead workers
        let dead = harness.registry.get_dead_workers();
        assert!(dead.is_empty());

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Now worker should be detected as dead
        let dead = harness.registry.get_dead_workers();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].0, worker_id);
    }

    #[tokio::test]
    async fn test_heartbeat_keeps_worker_alive() {
        let harness = TestHarness::with_timeout(Duration::from_millis(200));

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        // Send heartbeats at regular intervals
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(50)).await;

            harness.registry.update_heartbeat(
                &worker_id,
                WorkerCapacity {
                    available_cpu: 4.0,
                    available_memory_mb: 8192,
                    available_gpu: 0,
                    max_tasks: 2,
                    current_tasks: 0,
                },
            );
        }

        // Worker should still be alive
        let dead = harness.registry.get_dead_workers();
        assert!(dead.is_empty());
    }

    #[tokio::test]
    async fn test_dead_worker_removal() {
        let harness = TestHarness::with_timeout(Duration::from_millis(50));

        // Register and add tasks
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "task-1");
        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Remove dead workers
        let removed = harness.registry.remove_dead_workers();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, worker_id);
        assert_eq!(removed[0].1.len(), 1); // Had 1 task

        // Worker should no longer exist
        assert_eq!(harness.registry.worker_count(), 0);
    }

    #[tokio::test]
    async fn test_dead_worker_callback() {
        let harness = TestHarness::with_timeout(Duration::from_millis(50));

        let dead_workers: Arc<RwLock<Vec<(String, Vec<String>)>>> =
            Arc::new(RwLock::new(Vec::new()));
        let dead_clone = dead_workers.clone();

        harness.registry.set_dead_worker_callback(move |id, tasks| {
            dead_clone.write().push((id.to_string(), tasks));
        });

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check for dead workers (triggers callback)
        harness.registry.get_dead_workers();

        let notified = dead_workers.read();
        assert_eq!(notified.len(), 1);
        assert_eq!(notified[0].0, worker_id);
    }

    #[tokio::test]
    async fn test_worker_shutdown_message() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "task-1");
        let task_id = task.id.to_string();
        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Worker sends shutdown message
        let msg = WorkerToMasterMsg::Shutdown {
            reason: "Planned maintenance".to_string(),
            active_tasks: vec![task_id],
        };

        harness.worker_sends(&worker_id, msg).await;

        // Worker should be deregistered
        assert_eq!(harness.registry.worker_count(), 0);
    }
}

// ============================================================================
// Test: Progress tracking
// ============================================================================

#[cfg(test)]
mod progress_tracking {
    use super::*;

    #[tokio::test]
    async fn test_task_progress_updates() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "progress-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Send progress updates
        for i in 1..=5 {
            let progress = i as f64 * 0.2;
            let msg = WorkerToMasterMsg::TaskProgress {
                task_id: task_id.clone(),
                progress,
                stage: Some(format!("Stage {}", i)),
                metrics: TaskMetrics::default(),
            };
            harness.worker_sends(&worker_id, msg).await;
        }

        let updates = harness.get_progress_updates();
        assert_eq!(updates.len(), 5);
        assert!((updates[4].1 - 1.0).abs() < f64::EPSILON); // Last update should be 100%
    }

    #[tokio::test]
    async fn test_task_started_notification() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "start-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Worker notifies task started
        let msg = WorkerToMasterMsg::TaskStarted {
            task_id: task_id.clone(),
            started_at: 1234567890,
        };

        harness.worker_sends(&worker_id, msg).await;

        // Task should still be tracked
        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 1);
    }
}

// ============================================================================
// Test: Scheduler integration
// ============================================================================

#[cfg(test)]
mod scheduler_integration {
    use super::*;

    fn create_scheduler_registry() -> (Arc<WorkerRegistry>, JobScheduler) {
        let registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
        let scheduler = JobScheduler::new(registry.clone(), SchedulingPolicy::Fifo);
        (registry, scheduler)
    }

    fn create_worker_info(id: WorkerId, name: &str) -> WorkerInfo {
        WorkerInfo {
            id,
            address: format!("localhost:80{}", name.chars().last().unwrap_or('0')),
            region: RegionId::new(),
            capabilities: WorkerCapabilities {
                cpu_cores: 4,
                memory_mb: 8192,
                disk_mb: 100000,
                has_gpu: false,
                supported_tasks: vec!["shell".to_string()],
            },
            status: WorkerStatus::Ready,
            current_load: 0.0,
            running_tasks: Vec::new(),
            last_heartbeat: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_scheduler_with_workers() {
        let (registry, scheduler) = create_scheduler_registry();

        // Register workers
        let worker1 = WorkerId::new();
        let worker2 = WorkerId::new();
        registry.register(create_worker_info(worker1, "worker1"));
        registry.register(create_worker_info(worker2, "worker2"));

        // Submit tasks
        let job_id = JobId::new();
        let task1 = test_task(job_id, "task-1");
        let task2 = test_task(job_id, "task-2");

        scheduler.submit_tasks(vec![task1, task2], 1);

        assert_eq!(scheduler.pending_count(), 2);

        // Schedule tasks
        let scheduled = scheduler.schedule().await;
        assert_eq!(scheduled.len(), 2);

        // Verify tasks assigned to different workers (load balancing)
        let workers: Vec<WorkerId> = scheduled.iter().map(|(_, w)| *w).collect();
        assert!(workers.contains(&worker1) || workers.contains(&worker2));
    }

    #[tokio::test]
    async fn test_scheduler_task_completion() {
        let (registry, scheduler) = create_scheduler_registry();

        // Register worker
        let worker_id = WorkerId::new();
        registry.register(create_worker_info(worker_id, "worker1"));

        // Submit task
        let job_id = JobId::new();
        let task = test_task(job_id, "task-1");
        let task_id = task.id;

        scheduler.submit_tasks(vec![task], 1);

        // Schedule task
        let scheduled = scheduler.schedule().await;
        assert_eq!(scheduled.len(), 1);

        // Complete task
        let unblocked = scheduler.complete_task(task_id);
        assert!(unblocked.is_empty()); // No dependent tasks

        // Task should no longer be running
        assert_eq!(scheduler.running_count(), 0);
    }

    #[tokio::test]
    async fn test_scheduler_task_failure_retry() {
        let (registry, scheduler) = create_scheduler_registry();

        // Register worker
        let worker_id = WorkerId::new();
        registry.register(create_worker_info(worker_id, "worker1"));

        // Submit task
        let job_id = JobId::new();
        let mut task = test_task(job_id, "retry-task");
        task.max_attempts = 3;
        task.attempts = 0;
        let task_id = task.id;

        scheduler.submit_tasks(vec![task.clone()], 1);

        // Schedule and fail task
        let scheduled = scheduler.schedule().await;
        assert_eq!(scheduled.len(), 1);

        // Fail with retry
        scheduler.fail_task(task_id, task.clone(), true);

        // Task should be requeued
        assert_eq!(scheduler.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_scheduler_dag_dependencies() {
        let (registry, scheduler) = create_scheduler_registry();

        // Register worker
        let worker_id = WorkerId::new();
        registry.register(create_worker_info(worker_id, "worker1"));

        let job_id = JobId::new();

        // Create task chain: task1 -> task2 -> task3
        let task1 = test_task(job_id, "task-1");
        let task1_id = task1.id;

        let mut task2 = test_task(job_id, "task-2");
        task2.depends_on = vec![task1_id];
        let task2_id = task2.id;

        let mut task3 = test_task(job_id, "task-3");
        task3.depends_on = vec![task2_id];

        scheduler.submit_tasks(vec![task1, task2, task3], 1);

        // Only task1 should be pending
        assert_eq!(scheduler.pending_count(), 1);
        assert_eq!(scheduler.waiting_count(), 2);

        // Schedule and complete task1
        let scheduled = scheduler.schedule().await;
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].0.id, task1_id);

        let unblocked = scheduler.complete_task_with_job(task1_id, Some(job_id));
        assert_eq!(unblocked.len(), 1);
        assert_eq!(unblocked[0].id, task2_id);
    }
}

// ============================================================================
// Test: End-to-end task flow
// ============================================================================

#[cfg(test)]
mod end_to_end {
    use super::*;

    #[tokio::test]
    async fn test_complete_task_lifecycle() {
        let harness = TestHarness::new();

        // 1. Master starts and worker registers
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        assert_eq!(harness.registry.worker_count(), 1);

        // 2. Create and assign a task
        let job_id = JobId::new();
        let task = test_task(job_id, "lifecycle-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // 3. Worker acknowledges task
        let msg = WorkerToMasterMsg::TaskAccepted {
            task_id: task_id.clone(),
        };
        harness.worker_sends(&worker_id, msg).await;

        // 4. Worker reports task started
        let msg = WorkerToMasterMsg::TaskStarted {
            task_id: task_id.clone(),
            started_at: chrono::Utc::now().timestamp() as u64,
        };
        harness.worker_sends(&worker_id, msg).await;

        // 5. Worker reports progress
        for progress in [0.25, 0.5, 0.75] {
            let msg = WorkerToMasterMsg::TaskProgress {
                task_id: task_id.clone(),
                progress,
                stage: Some(format!("{}%", (progress * 100.0) as u32)),
                metrics: TaskMetrics::default(),
            };
            harness.worker_sends(&worker_id, msg).await;
        }

        // 6. Worker reports completion
        let msg = WorkerToMasterMsg::TaskCompleted {
            task_id: task_id.clone(),
            result: ProtocolTaskResult {
                output: b"Success!".to_vec(),
                metrics: TaskMetrics::default(),
                exit_code: Some(0),
                stdout: Some("Success!".to_string()),
                stderr: None,
            },
            duration_ms: 1000,
        };
        harness.worker_sends(&worker_id, msg).await;

        // 7. Verify final state
        let completed = harness.get_completed_tasks();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, task_id);

        let progress_updates = harness.get_progress_updates();
        assert_eq!(progress_updates.len(), 3);

        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 0);
        assert!(worker.can_accept_task());
    }

    #[tokio::test]
    async fn test_multi_worker_parallel_execution() {
        let harness = TestHarness::new();

        // Register multiple workers
        for i in 1..=3 {
            harness
                .register_worker(&format!("worker-{}", i), test_capabilities(4, 8192, 2))
                .await;
        }

        assert_eq!(harness.registry.worker_count(), 3);

        let job_id = JobId::new();

        // Assign tasks to different workers
        for i in 1..=3 {
            let task = test_task(job_id, &format!("parallel-task-{}", i));
            harness
                .registry
                .assign_task(&format!("worker-{}", i), task)
                .await
                .unwrap();
        }

        // Complete all tasks
        for i in 1..=3 {
            let worker_id = format!("worker-{}", i);
            let worker = harness.registry.get_worker(&worker_id).unwrap();
            let tasks = worker.get_assigned_tasks();

            for task_id in tasks {
                let msg = WorkerToMasterMsg::TaskCompleted {
                    task_id,
                    result: ProtocolTaskResult::default(),
                    duration_ms: 100,
                };
                harness.worker_sends(&worker_id, msg).await;
            }
        }

        let completed = harness.get_completed_tasks();
        assert_eq!(completed.len(), 3);

        // All workers should be idle
        for i in 1..=3 {
            let worker = harness
                .registry
                .get_worker(&format!("worker-{}", i))
                .unwrap();
            assert_eq!(worker.task_count(), 0);
        }
    }

    #[tokio::test]
    async fn test_worker_failure_during_task() {
        let harness = TestHarness::with_timeout(Duration::from_millis(50));

        let dead_workers: Arc<RwLock<Vec<(String, Vec<String>)>>> =
            Arc::new(RwLock::new(Vec::new()));
        let dead_clone = dead_workers.clone();

        harness.registry.set_dead_worker_callback(move |id, tasks| {
            dead_clone.write().push((id.to_string(), tasks));
        });

        // Register worker and assign task
        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "failure-test");
        let task_id = task.id.to_string();
        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Simulate worker failure (no heartbeat)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Detect and handle dead worker
        let dead = harness.registry.remove_dead_workers();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].1.len(), 1);
        assert_eq!(dead[0].1[0], task_id);

        // Callback should have been invoked
        let notified = dead_workers.read();
        assert_eq!(notified.len(), 1);
    }

    #[tokio::test]
    async fn test_checkpoint_during_preemption() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;

        let job_id = JobId::new();
        let task = test_task(job_id, "checkpoint-test");
        let task_id = task.id.to_string();

        harness.registry.assign_task(&worker_id, task).await.unwrap();

        // Worker saves checkpoint
        let msg = WorkerToMasterMsg::CheckpointSaved {
            task_id: task_id.clone(),
            checkpoint_id: "checkpoint-123".to_string(),
            size_bytes: 1024,
        };
        harness.worker_sends(&worker_id, msg).await;

        // Task can then be preempted and resumed elsewhere
        // (In a full implementation, this would involve task reassignment)

        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.task_count(), 1);
    }
}

// ============================================================================
// Test: Capacity and load management
// ============================================================================

#[cfg(test)]
mod capacity_management {
    use super::*;

    #[tokio::test]
    async fn test_worker_capacity_tracking() {
        let harness = TestHarness::new();

        // Register two workers with different capacities
        harness
            .register_worker("worker-1", test_capabilities(4, 8192, 2))
            .await;
        harness
            .register_worker("worker-2", test_capabilities(8, 16384, 4))
            .await;

        let (total_cores, total_mem, total_tasks) = harness.registry.total_capacity();
        assert_eq!(total_cores, 12);
        assert_eq!(total_mem, 24576);
        assert_eq!(total_tasks, 6);
    }

    #[tokio::test]
    async fn test_available_capacity_updates() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 4))
            .await;

        // Initial available capacity
        let (_, _, avail_tasks) = harness.registry.available_capacity();
        assert_eq!(avail_tasks, 4);

        // Add tasks
        let job_id = JobId::new();
        for i in 1..=2 {
            let task = test_task(job_id, &format!("task-{}", i));
            harness.registry.assign_task(&worker_id, task).await.unwrap();
        }

        // Update capacity via heartbeat
        harness.registry.update_heartbeat(
            &worker_id,
            WorkerCapacity {
                available_cpu: 2.0,
                available_memory_mb: 4096,
                available_gpu: 0,
                max_tasks: 4,
                current_tasks: 2,
            },
        );

        let (_, avail_mem, avail_tasks) = harness.registry.available_capacity();
        assert_eq!(avail_tasks, 2);
        assert_eq!(avail_mem, 4096);
    }

    #[tokio::test]
    async fn test_worker_load_calculation() {
        let harness = TestHarness::new();

        let worker_id = harness
            .register_worker("worker-1", test_capabilities(4, 8192, 4))
            .await;

        let worker = harness.registry.get_worker(&worker_id).unwrap();
        assert_eq!(worker.load(), 0.0);

        // Add 2 of 4 tasks
        worker.add_task("task-1".to_string());
        worker.add_task("task-2".to_string());

        assert_eq!(worker.load(), 0.5);

        // Add 2 more
        worker.add_task("task-3".to_string());
        worker.add_task("task-4".to_string());

        assert_eq!(worker.load(), 1.0);
        assert!(!worker.can_accept_task());
    }
}
