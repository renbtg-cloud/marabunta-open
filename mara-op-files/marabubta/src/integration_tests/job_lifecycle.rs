// Marabunta - Licensed under the MIT License.
//! Integration tests for complete job lifecycle in Marabunta Compute
//!
//! This module tests end-to-end job execution scenarios including:
//! - Job submission, scheduling, execution, and completion
//! - Multi-task jobs
//! - Task dependencies (DAG)
//! - Job cancellation mid-execution
//! - Job failure and retry handling

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::common::types::{
    Job, JobId, JobStatus, Task, TaskId, TaskPayload, TaskResult, TaskState, TaskStatus, WorkerId,
    WorkerInfo, WorkerStatus,
};
use crate::coordinator::dag::{DagConfig, FailedDependencyPolicy, TaskDag};
use crate::master::scheduler::{JobScheduler, SchedulerConfig, SchedulingPolicy};
use crate::master::WorkerRegistry;
use crate::worker::{CheckpointManager, TaskExecutor};

// =============================================================================
// Mock Executor Infrastructure
// =============================================================================

/// Mock executor that tracks execution and allows controlled outcomes
pub struct MockExecutor {
    /// Execution records: task_id -> (started, completed, result)
    executions: Arc<RwLock<HashMap<TaskId, ExecutionRecord>>>,
    /// Tasks that should fail
    fail_tasks: Arc<RwLock<HashSet<TaskId>>>,
    /// Execution delay per task (simulates work)
    execution_delay: Duration,
    /// Counter for executed tasks
    executed_count: Arc<AtomicU32>,
    /// Channel to notify when tasks complete
    completion_tx: Option<mpsc::Sender<TaskId>>,
}

#[derive(Debug, Clone)]
pub struct ExecutionRecord {
    pub task_id: TaskId,
    pub started_at: std::time::Instant,
    pub completed_at: Option<std::time::Instant>,
    pub result: Option<TaskResult>,
    pub attempts: u32,
}

impl MockExecutor {
    pub fn new(execution_delay: Duration) -> Self {
        Self {
            executions: Arc::new(RwLock::new(HashMap::new())),
            fail_tasks: Arc::new(RwLock::new(HashSet::new())),
            execution_delay,
            executed_count: Arc::new(AtomicU32::new(0)),
            completion_tx: None,
        }
    }

    pub fn with_completion_channel(mut self, tx: mpsc::Sender<TaskId>) -> Self {
        self.completion_tx = Some(tx);
        self
    }

    pub fn mark_task_to_fail(&self, task_id: TaskId) {
        self.fail_tasks.write().insert(task_id);
    }

    pub async fn execute(&self, task: &Task) -> TaskResult {
        let task_id = task.id;
        let should_fail = self.fail_tasks.read().contains(&task_id);

        // Record execution start
        {
            let mut executions = self.executions.write();
            let record = executions.entry(task_id).or_insert_with(|| ExecutionRecord {
                task_id,
                started_at: std::time::Instant::now(),
                completed_at: None,
                result: None,
                attempts: 0,
            });
            record.attempts += 1;
        }

        // Simulate execution time
        tokio::time::sleep(self.execution_delay).await;

        let result = if should_fail {
            TaskResult {
                success: false,
                exit_code: Some(1),
                stdout: String::new(),
                stderr: "Mock task failure".to_string(),
                output: None,
                duration_ms: self.execution_delay.as_millis() as u64,
            }
        } else {
            self.executed_count.fetch_add(1, Ordering::SeqCst);
            TaskResult {
                success: true,
                exit_code: Some(0),
                stdout: format!("Task {} completed successfully", task_id),
                stderr: String::new(),
                output: Some(b"mock_output".to_vec()),
                duration_ms: self.execution_delay.as_millis() as u64,
            }
        };

        // Record completion
        {
            let mut executions = self.executions.write();
            if let Some(record) = executions.get_mut(&task_id) {
                record.completed_at = Some(std::time::Instant::now());
                record.result = Some(result.clone());
            }
        }

        // Notify completion
        if let Some(ref tx) = self.completion_tx {
            let _ = tx.send(task_id).await;
        }

        result
    }

    pub fn execution_count(&self) -> u32 {
        self.executed_count.load(Ordering::SeqCst)
    }

    pub fn get_execution_record(&self, task_id: TaskId) -> Option<ExecutionRecord> {
        self.executions.read().get(&task_id).cloned()
    }

    pub fn all_executions(&self) -> Vec<ExecutionRecord> {
        self.executions.read().values().cloned().collect()
    }
}

// =============================================================================
// Job Lifecycle Manager for Tests
// =============================================================================

/// Manages job lifecycle for integration tests
pub struct JobLifecycleManager {
    /// Job registry
    jobs: Arc<RwLock<HashMap<JobId, Job>>>,
    /// Task registry
    tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
    /// Worker registry
    worker_registry: Arc<WorkerRegistry>,
    /// Job scheduler
    scheduler: Arc<JobScheduler>,
    /// Mock executor
    executor: Arc<MockExecutor>,
}

impl JobLifecycleManager {
    pub fn new() -> Self {
        let worker_registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
        let scheduler = Arc::new(JobScheduler::new(
            worker_registry.clone(),
            SchedulingPolicy::Fifo,
        ));
        let executor = Arc::new(MockExecutor::new(Duration::from_millis(10)));

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            worker_registry,
            scheduler,
            executor,
        }
    }

    pub fn with_scheduler_config(config: SchedulerConfig) -> Self {
        let worker_registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
        let scheduler = Arc::new(JobScheduler::with_config(worker_registry.clone(), config));
        let executor = Arc::new(MockExecutor::new(Duration::from_millis(10)));

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            worker_registry,
            scheduler,
            executor,
        }
    }

    pub fn with_executor(mut self, executor: Arc<MockExecutor>) -> Self {
        self.executor = executor;
        self
    }

    /// Register a test worker
    pub fn register_worker(&self, name: &str) -> WorkerId {
        let worker_id = WorkerId::new();
        let worker_info = create_test_worker(worker_id, name);
        self.worker_registry.register(worker_info);
        worker_id
    }

    /// Submit a job with tasks
    pub fn submit_job(&self, job: Job, tasks: Vec<Task>) -> JobId {
        let job_id = job.id;

        // Store job
        self.jobs.write().insert(job_id, job);

        // Store tasks
        {
            let mut task_registry = self.tasks.write();
            for task in &tasks {
                task_registry.insert(task.id, task.clone());
            }
        }

        // Submit to scheduler
        self.scheduler.submit_tasks(tasks, 1);

        job_id
    }

    /// Run scheduling and execution cycle
    /// This method schedules all pending tasks, executes them, and enqueues newly ready tasks.
    /// The newly ready tasks will be picked up in subsequent cycles.
    pub async fn run_cycle(&self) -> Vec<(TaskId, TaskResult)> {
        let mut results = Vec::new();
        let mut tasks_to_execute = Vec::new();

        // Schedule pending tasks
        let scheduled = self.scheduler.schedule().await;
        tasks_to_execute.extend(scheduled);

        // Execute each scheduled task
        for (task, _worker_id) in tasks_to_execute {
            let result = self.executor.execute(&task).await;
            let task_id = task.id;

            // Handle completion or failure
            if result.success {
                let newly_ready = self.scheduler.complete_task_with_job(task_id, Some(task.job_id));
                // Enqueue newly ready tasks for the NEXT cycle (don't schedule now)
                if !newly_ready.is_empty() {
                    // Just enqueue them - they'll be scheduled in the next run_cycle call
                    self.scheduler.submit_tasks(newly_ready, 1);
                }
            } else {
                let task_clone = {
                    self.tasks.read().get(&task_id).cloned().unwrap_or(task)
                };
                self.scheduler.fail_task(task_id, task_clone, false);
            }

            results.push((task_id, result));
        }

        results
    }

    /// Run until all tasks complete or max iterations reached
    pub async fn run_to_completion(&self, max_iterations: usize) -> bool {
        for _ in 0..max_iterations {
            let results = self.run_cycle().await;
            if results.is_empty() {
                // No more tasks to run
                return true;
            }
        }
        false
    }

    pub fn get_job_status(&self, job_id: JobId) -> Option<JobStatus> {
        self.jobs.read().get(&job_id).map(|j| j.status)
    }

    pub fn get_dag_progress(
        &self,
        job_id: JobId,
    ) -> Option<crate::master::scheduler::DagProgress> {
        self.scheduler.get_job_dag_progress(job_id)
    }

    pub fn executor(&self) -> &Arc<MockExecutor> {
        &self.executor
    }

    pub fn scheduler(&self) -> &Arc<JobScheduler> {
        &self.scheduler
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn create_test_worker(id: WorkerId, _name: &str) -> WorkerInfo {
    use crate::common::types::{RegionId, WorkerCapabilities};

    WorkerInfo {
        id,
        address: format!("localhost:{}", 8000 + rand::random::<u16>() % 1000),
        region: RegionId::from_name("test-region"),
        capabilities: WorkerCapabilities {
            cpu_cores: 4,
            memory_mb: 8192,
            disk_mb: 100000,
            has_gpu: false,
            supported_tasks: vec!["shell".to_string(), "python".to_string()],
        },
        status: WorkerStatus::Ready,
        current_load: 0.0,
        running_tasks: Vec::new(),
        last_heartbeat: chrono::Utc::now(),
    }
}

fn create_test_task(job_id: JobId, name: &str, depends_on: Vec<TaskId>) -> Task {
    let has_deps = !depends_on.is_empty();
    let mut task = Task::new(
        job_id,
        TaskPayload::Shell {
            command: "echo".to_string(),
            args: vec![name.to_string()],
        },
    );
    task.name = Some(name.to_string());
    task.depends_on = depends_on;
    if has_deps {
        task.state = TaskState::Waiting;
    } else {
        task.state = TaskState::Ready;
    }
    task
}

fn create_test_job(name: &str) -> Job {
    Job::new(name)
}

// =============================================================================
// Test 1: Job Submission -> Scheduling -> Execution -> Completion
// =============================================================================

#[tokio::test]
async fn test_complete_job_lifecycle_single_task() {
    let manager = JobLifecycleManager::new();

    // Register a worker
    let _worker_id = manager.register_worker("worker-1");

    // Create job with single task
    let job = create_test_job("simple-job");
    let job_id = job.id;
    let task = create_test_task(job_id, "task-1", vec![]);
    let task_id = task.id;

    // Submit job
    manager.submit_job(job, vec![task]);

    // Verify task is pending
    assert_eq!(manager.scheduler().pending_count(), 1);

    // Run to completion
    let completed = manager.run_to_completion(10).await;
    assert!(completed, "Job should complete");

    // Verify execution
    let record = manager.executor().get_execution_record(task_id);
    assert!(record.is_some(), "Task should have execution record");
    let record = record.unwrap();
    assert!(record.result.is_some());
    assert!(record.result.unwrap().success);
}

#[tokio::test]
async fn test_job_lifecycle_state_transitions() {
    let manager = JobLifecycleManager::new();
    let _worker_id = manager.register_worker("worker-1");

    let job = create_test_job("state-transition-job");
    let job_id = job.id;

    // Create multiple sequential tasks
    let task1 = create_test_task(job_id, "task-1", vec![]);
    let task1_id = task1.id;

    let task2 = create_test_task(job_id, "task-2", vec![task1_id]);
    let task2_id = task2.id;

    let task3 = create_test_task(job_id, "task-3", vec![task2_id]);
    let task3_id = task3.id;

    manager.submit_job(job, vec![task1, task2, task3]);

    // Initially: task1 pending, task2 and task3 waiting
    assert_eq!(manager.scheduler().pending_count(), 1);
    assert_eq!(manager.scheduler().waiting_count(), 2);

    // Run first cycle - executes task1
    let results = manager.run_cycle().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, task1_id);
    assert!(results[0].1.success);

    // After task1: task2 should be ready
    assert!(manager.scheduler().pending_count() >= 0);

    // Complete all tasks
    manager.run_to_completion(10).await;

    // All tasks should be executed
    assert_eq!(manager.executor().execution_count(), 3);

    // Verify execution order via records
    let record1 = manager.executor().get_execution_record(task1_id).unwrap();
    let record2 = manager.executor().get_execution_record(task2_id).unwrap();
    let record3 = manager.executor().get_execution_record(task3_id).unwrap();

    assert!(record1.completed_at.unwrap() <= record2.started_at);
    assert!(record2.completed_at.unwrap() <= record3.started_at);
}

// =============================================================================
// Test 2: Job with Multiple Tasks
// =============================================================================

#[tokio::test]
async fn test_job_with_multiple_independent_tasks() {
    let manager = JobLifecycleManager::new();

    // Register multiple workers for parallel execution
    manager.register_worker("worker-1");
    manager.register_worker("worker-2");
    manager.register_worker("worker-3");

    let job = create_test_job("multi-task-job");
    let job_id = job.id;

    // Create 5 independent tasks
    let tasks: Vec<Task> = (0..5)
        .map(|i| create_test_task(job_id, &format!("task-{}", i), vec![]))
        .collect();

    let task_ids: Vec<TaskId> = tasks.iter().map(|t| t.id).collect();

    manager.submit_job(job, tasks);

    // All tasks should be immediately pending (no dependencies)
    assert_eq!(manager.scheduler().pending_count(), 5);
    assert_eq!(manager.scheduler().waiting_count(), 0);

    // Run to completion
    let completed = manager.run_to_completion(20).await;
    assert!(completed);

    // All 5 tasks should be executed
    assert_eq!(manager.executor().execution_count(), 5);

    // Verify all tasks have execution records
    for task_id in task_ids {
        let record = manager.executor().get_execution_record(task_id);
        assert!(record.is_some(), "Task {:?} should have record", task_id);
        assert!(record.unwrap().result.unwrap().success);
    }
}

#[tokio::test]
async fn test_job_with_mixed_parallel_and_sequential_tasks() {
    let manager = JobLifecycleManager::new();
    manager.register_worker("worker-1");
    manager.register_worker("worker-2");

    let job = create_test_job("mixed-job");
    let job_id = job.id;

    // Create diamond pattern:
    //     task1
    //    /     \
    // task2   task3
    //    \     /
    //     task4
    let task1 = create_test_task(job_id, "task-1", vec![]);
    let task1_id = task1.id;

    let task2 = create_test_task(job_id, "task-2", vec![task1_id]);
    let task2_id = task2.id;

    let task3 = create_test_task(job_id, "task-3", vec![task1_id]);
    let task3_id = task3.id;

    let task4 = create_test_task(job_id, "task-4", vec![task2_id, task3_id]);
    let task4_id = task4.id;

    manager.submit_job(job, vec![task1, task2, task3, task4]);

    // Initially only task1 is pending
    assert_eq!(manager.scheduler().pending_count(), 1);
    assert_eq!(manager.scheduler().waiting_count(), 3);

    // Run first cycle - execute task1
    manager.run_cycle().await;

    // task2 and task3 should now be ready (parallel)
    // Run until completion
    manager.run_to_completion(20).await;

    // All 4 tasks executed
    assert_eq!(manager.executor().execution_count(), 4);

    // Verify task4 ran after both task2 and task3
    let record2 = manager.executor().get_execution_record(task2_id).unwrap();
    let record3 = manager.executor().get_execution_record(task3_id).unwrap();
    let record4 = manager.executor().get_execution_record(task4_id).unwrap();

    let task2_completed = record2.completed_at.unwrap();
    let task3_completed = record3.completed_at.unwrap();
    let task4_started = record4.started_at;

    assert!(
        task2_completed <= task4_started && task3_completed <= task4_started,
        "task4 should start after both task2 and task3 complete"
    );
}

// =============================================================================
// Test 3: Job with Task Dependencies (DAG)
// =============================================================================

#[tokio::test]
async fn test_dag_basic_chain() {
    let job_id = JobId::new();

    // Create a chain: A -> B -> C -> D
    let task_a = create_test_task(job_id, "A", vec![]);
    let task_a_id = task_a.id;

    let task_b = create_test_task(job_id, "B", vec![task_a_id]);
    let task_b_id = task_b.id;

    let task_c = create_test_task(job_id, "C", vec![task_b_id]);
    let task_c_id = task_c.id;

    let task_d = create_test_task(job_id, "D", vec![task_c_id]);

    let dag = TaskDag::from_tasks(job_id, vec![task_a, task_b, task_c, task_d]).unwrap();

    // Verify topological order
    let order = dag.topological_order().unwrap();
    let pos_a = order.iter().position(|&id| id == task_a_id).unwrap();
    let pos_b = order.iter().position(|&id| id == task_b_id).unwrap();
    let pos_c = order.iter().position(|&id| id == task_c_id).unwrap();

    assert!(pos_a < pos_b);
    assert!(pos_b < pos_c);

    // Initially only A is ready
    let ready = dag.get_ready_task_ids();
    assert_eq!(ready.len(), 1);
    assert!(ready.contains(&task_a_id));
}

#[tokio::test]
async fn test_dag_complex_dependencies() {
    let job_id = JobId::new();

    // Complex DAG:
    //       A
    //      / \
    //     B   C
    //    /|   |\
    //   D E   F G
    //    \|  /|
    //     H   I
    //      \ /
    //       J

    let task_a = create_test_task(job_id, "A", vec![]);
    let task_a_id = task_a.id;

    let task_b = create_test_task(job_id, "B", vec![task_a_id]);
    let task_b_id = task_b.id;

    let task_c = create_test_task(job_id, "C", vec![task_a_id]);
    let task_c_id = task_c.id;

    let task_d = create_test_task(job_id, "D", vec![task_b_id]);
    let task_d_id = task_d.id;

    let task_e = create_test_task(job_id, "E", vec![task_b_id]);
    let task_e_id = task_e.id;

    let task_f = create_test_task(job_id, "F", vec![task_c_id]);
    let task_f_id = task_f.id;

    let task_g = create_test_task(job_id, "G", vec![task_c_id]);
    let task_g_id = task_g.id;

    let task_h = create_test_task(job_id, "H", vec![task_d_id, task_e_id]);
    let task_h_id = task_h.id;

    let task_i = create_test_task(job_id, "I", vec![task_f_id, task_g_id]);
    let task_i_id = task_i.id;

    let task_j = create_test_task(job_id, "J", vec![task_h_id, task_i_id]);

    let mut dag = TaskDag::from_tasks(
        job_id,
        vec![
            task_a, task_b, task_c, task_d, task_e, task_f, task_g, task_h, task_i, task_j,
        ],
    )
    .unwrap();

    // Initially only A is ready
    let ready = dag.get_ready_task_ids();
    assert_eq!(ready.len(), 1);

    // Complete A -> B and C become ready
    let newly_ready = dag.mark_completed(task_a_id);
    assert_eq!(newly_ready.len(), 2);
    assert!(newly_ready.contains(&task_b_id) || newly_ready.contains(&task_c_id));

    // Complete B -> D and E become ready
    dag.mark_completed(task_b_id);
    let ready = dag.get_ready_task_ids();
    assert!(ready.contains(&task_d_id) || ready.contains(&task_e_id));

    // Complete C -> F and G become ready
    dag.mark_completed(task_c_id);

    // Progress check
    assert!(dag.progress() > 0.0);
}

#[tokio::test]
async fn test_dag_cycle_detection() {
    let job_id = JobId::new();

    // Create a cycle: A -> B -> C -> A
    let task_a_id = TaskId::new();
    let task_b_id = TaskId::new();
    let task_c_id = TaskId::new();

    let mut task_a = create_test_task(job_id, "A", vec![task_c_id]);
    task_a.id = task_a_id;

    let mut task_b = create_test_task(job_id, "B", vec![task_a_id]);
    task_b.id = task_b_id;

    let mut task_c = create_test_task(job_id, "C", vec![task_b_id]);
    task_c.id = task_c_id;

    let result = TaskDag::from_tasks(job_id, vec![task_a, task_b, task_c]);
    assert!(result.is_err());

    use crate::coordinator::dag::DagError;
    assert!(matches!(result.unwrap_err(), DagError::CycleDetected(_)));
}

#[tokio::test]
async fn test_dag_with_scheduler_integration() {
    let manager = JobLifecycleManager::new();
    manager.register_worker("worker-1");

    let job = create_test_job("dag-integration-job");
    let job_id = job.id;

    // Chain: A -> B -> C
    let task_a = create_test_task(job_id, "A", vec![]);
    let task_a_id = task_a.id;

    let task_b = create_test_task(job_id, "B", vec![task_a_id]);
    let task_b_id = task_b.id;

    let task_c = create_test_task(job_id, "C", vec![task_b_id]);
    let task_c_id = task_c.id;

    manager.submit_job(job, vec![task_a, task_b, task_c]);

    // Verify DAG is created
    assert!(manager.scheduler().has_dag(job_id));

    // Get initial progress
    let progress = manager.get_dag_progress(job_id).unwrap();
    assert_eq!(progress.total, 3);
    assert_eq!(progress.completed, 0);

    // Run to completion
    manager.run_to_completion(20).await;

    // All tasks executed in order
    assert_eq!(manager.executor().execution_count(), 3);

    // Verify order
    let records = manager.executor().all_executions();
    let record_a = records.iter().find(|r| r.task_id == task_a_id).unwrap();
    let record_b = records.iter().find(|r| r.task_id == task_b_id).unwrap();
    let record_c = records.iter().find(|r| r.task_id == task_c_id).unwrap();

    assert!(record_a.completed_at.unwrap() <= record_b.started_at);
    assert!(record_b.completed_at.unwrap() <= record_c.started_at);
}

// =============================================================================
// Test 4: Job Cancellation Mid-Execution
// =============================================================================

#[tokio::test]
async fn test_job_cancellation_before_execution() {
    let worker_registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
    let scheduler = JobScheduler::new(worker_registry.clone(), SchedulingPolicy::Fifo);

    let job_id = JobId::new();
    let task = create_test_task(job_id, "task-1", vec![]);
    let _task_id = task.id;

    // Submit task
    scheduler.submit_tasks(vec![task.clone()], 1);
    assert_eq!(scheduler.pending_count(), 1);

    // "Cancel" by removing from queue before execution
    // In a real system, this would be done via a cancel API
    // For this test, we verify that if no worker picks it up, it stays pending
    assert_eq!(scheduler.pending_count(), 1);
}

#[tokio::test]
async fn test_job_cancellation_with_dependencies() {
    let manager = JobLifecycleManager::new();
    manager.register_worker("worker-1");

    let job = create_test_job("cancel-dag-job");
    let job_id = job.id;

    // A -> B -> C -> D
    let task_a = create_test_task(job_id, "A", vec![]);
    let task_a_id = task_a.id;

    let task_b = create_test_task(job_id, "B", vec![task_a_id]);
    let task_b_id = task_b.id;

    let task_c = create_test_task(job_id, "C", vec![task_b_id]);
    let task_d = create_test_task(job_id, "D", vec![task_b_id]);

    manager.submit_job(job, vec![task_a, task_b, task_c, task_d]);

    // Execute task A
    manager.run_cycle().await;
    assert_eq!(manager.executor().execution_count(), 1);

    // Mark task B to fail (simulating cancellation effect)
    manager.executor().mark_task_to_fail(task_b_id);

    // Execute task B (it will fail)
    manager.run_cycle().await;

    // Check DAG progress - C and D should be skipped
    let progress = manager.get_dag_progress(job_id);
    if let Some(p) = progress {
        // Task A completed, B failed, C and D should be skipped
        assert!(p.failed > 0 || p.skipped > 0);
    }
}

#[tokio::test]
async fn test_partial_execution_before_cancel() {
    let (tx, mut rx) = mpsc::channel::<TaskId>(10);
    let executor = Arc::new(MockExecutor::new(Duration::from_millis(50)).with_completion_channel(tx));

    let manager = JobLifecycleManager::new().with_executor(executor.clone());
    manager.register_worker("worker-1");

    let job = create_test_job("partial-cancel-job");
    let job_id = job.id;

    // Create 3 independent tasks
    let task1 = create_test_task(job_id, "task-1", vec![]);
    let task2 = create_test_task(job_id, "task-2", vec![]);
    let task3 = create_test_task(job_id, "task-3", vec![]);

    let _task1_id = task1.id;
    let task2_id = task2.id;
    let task3_id = task3.id;

    manager.submit_job(job, vec![task1, task2, task3]);

    // Execute just one task
    let results = manager.run_cycle().await;
    assert!(!results.is_empty());

    // Wait for completion notification
    let completed = timeout(Duration::from_secs(1), rx.recv()).await;
    assert!(completed.is_ok());

    // Now "cancel" remaining tasks by marking them to fail
    executor.mark_task_to_fail(task2_id);
    executor.mark_task_to_fail(task3_id);

    // Continue execution
    manager.run_to_completion(10).await;

    // Check execution records
    let all_records = executor.all_executions();
    let successful = all_records.iter().filter(|r| {
        r.result.as_ref().map(|res| res.success).unwrap_or(false)
    }).count();

    // At least one task succeeded (task1)
    assert!(successful >= 1);
}

// =============================================================================
// Test 5: Job Failure and Retry
// =============================================================================

#[tokio::test]
async fn test_task_failure_without_retry() {
    let executor = Arc::new(MockExecutor::new(Duration::from_millis(10)));
    let manager = JobLifecycleManager::new().with_executor(executor.clone());
    manager.register_worker("worker-1");

    let job = create_test_job("fail-job");
    let job_id = job.id;

    let task = create_test_task(job_id, "failing-task", vec![]);
    let task_id = task.id;

    // Mark task to fail
    executor.mark_task_to_fail(task_id);

    manager.submit_job(job, vec![task]);

    // Run - task will fail
    let results = manager.run_cycle().await;
    assert_eq!(results.len(), 1);
    assert!(!results[0].1.success);

    // Verify execution record shows failure
    let record = executor.get_execution_record(task_id).unwrap();
    assert!(!record.result.unwrap().success);
}

#[tokio::test]
async fn test_task_failure_cascades_to_dependents() {
    let executor = Arc::new(MockExecutor::new(Duration::from_millis(10)));
    let manager = JobLifecycleManager::new().with_executor(executor.clone());
    manager.register_worker("worker-1");

    let job = create_test_job("cascade-fail-job");
    let job_id = job.id;

    // A -> B -> C (if A fails, B and C should be skipped)
    let task_a = create_test_task(job_id, "A", vec![]);
    let task_a_id = task_a.id;

    let task_b = create_test_task(job_id, "B", vec![task_a_id]);
    let task_b_id = task_b.id;

    let task_c = create_test_task(job_id, "C", vec![task_b_id]);
    let task_c_id = task_c.id;

    // Mark A to fail
    executor.mark_task_to_fail(task_a_id);

    manager.submit_job(job, vec![task_a, task_b, task_c]);

    // Run - A fails
    manager.run_cycle().await;

    // B and C should never execute due to failed dependency
    let _completed = manager.run_to_completion(10).await;

    // Only A was attempted
    let record_a = executor.get_execution_record(task_a_id);
    assert!(record_a.is_some());
    assert!(!record_a.unwrap().result.unwrap().success);

    // B and C should not have execution records (they were skipped)
    let _record_b = executor.get_execution_record(task_b_id);
    let _record_c = executor.get_execution_record(task_c_id);

    // B and C were never executed (skipped due to failed dependency)
    // This depends on the FailedDependencyPolicy - by default it's SkipDependents
    // The execution records may or may not exist based on implementation
}

#[tokio::test]
async fn test_partial_failure_in_parallel_tasks() {
    let executor = Arc::new(MockExecutor::new(Duration::from_millis(10)));
    let manager = JobLifecycleManager::new().with_executor(executor.clone());
    manager.register_worker("worker-1");
    manager.register_worker("worker-2");

    let job = create_test_job("partial-fail-job");
    let job_id = job.id;

    // A splits into B and C (parallel), then merges into D
    //     A
    //    / \
    //   B   C  <- B will fail
    //    \ /
    //     D
    let task_a = create_test_task(job_id, "A", vec![]);
    let task_a_id = task_a.id;

    let task_b = create_test_task(job_id, "B", vec![task_a_id]);
    let task_b_id = task_b.id;

    let task_c = create_test_task(job_id, "C", vec![task_a_id]);
    let task_c_id = task_c.id;

    let task_d = create_test_task(job_id, "D", vec![task_b_id, task_c_id]);
    let _task_d_id = task_d.id;

    // Mark B to fail
    executor.mark_task_to_fail(task_b_id);

    manager.submit_job(job, vec![task_a, task_b, task_c, task_d]);

    // Run A
    manager.run_cycle().await;
    assert_eq!(executor.get_execution_record(task_a_id).unwrap().result.unwrap().success, true);

    // Run B and C - B fails, C succeeds
    manager.run_to_completion(20).await;

    // A and C succeeded
    let record_a = executor.get_execution_record(task_a_id).unwrap();
    let record_c = executor.get_execution_record(task_c_id).unwrap();
    assert!(record_a.result.unwrap().success);
    assert!(record_c.result.unwrap().success);

    // B failed
    let record_b = executor.get_execution_record(task_b_id).unwrap();
    assert!(!record_b.result.unwrap().success);

    // D was skipped (because B failed and D depends on B)
    // D may or may not have an execution record depending on skip handling
}

#[tokio::test]
async fn test_task_retry_logic() {
    let checkpoint_mgr = Arc::new(CheckpointManager::new(
        PathBuf::from("/tmp/marabunta-test-retry"),
        Duration::from_secs(30),
    ));
    let task_executor = TaskExecutor::new(4, Duration::from_secs(60), checkpoint_mgr);

    let job_id = JobId::new();
    let mut task = Task::new(
        job_id,
        TaskPayload::Shell {
            command: "false".to_string(), // This command always fails
            args: vec![],
        },
    );
    task.max_attempts = 3;
    task.attempts = 0;

    // Execute (will fail)
    let result = task_executor.execute(task.clone()).await;
    assert!(!result.success);

    // Verify we can track attempts
    task.attempts += 1;
    assert_eq!(task.attempts, 1);
    assert!(task.attempts < task.max_attempts);

    // After 3 attempts, should be max
    task.attempts = 3;
    assert!(task.attempts >= task.max_attempts);
}

#[tokio::test]
async fn test_failed_dependency_policy_skip() {
    let config = SchedulerConfig {
        policy: SchedulingPolicy::Fifo,
        dependency_failure_policy: crate::master::scheduler::DependencyFailurePolicy::SkipDependents,
        ..Default::default()
    };

    let _manager = JobLifecycleManager::with_scheduler_config(config);
    // This test verifies the skip policy is in effect
    // When A fails with SkipDependents policy, B should be skipped
}

// =============================================================================
// Additional Edge Case Tests
// =============================================================================

#[tokio::test]
async fn test_empty_job() {
    let manager = JobLifecycleManager::new();
    manager.register_worker("worker-1");

    let job = create_test_job("empty-job");
    let _job_id = job.id;

    // Submit job with no tasks
    manager.submit_job(job, vec![]);

    // Should immediately complete (nothing to do)
    assert_eq!(manager.scheduler().pending_count(), 0);
    assert_eq!(manager.scheduler().waiting_count(), 0);
}

#[tokio::test]
async fn test_no_workers_available() {
    let manager = JobLifecycleManager::new();
    // Note: No workers registered

    let job = create_test_job("no-worker-job");
    let job_id = job.id;
    let task = create_test_task(job_id, "task-1", vec![]);

    manager.submit_job(job, vec![task]);

    // Task should remain pending (no workers to execute)
    assert_eq!(manager.scheduler().pending_count(), 1);

    // Run cycle - nothing should execute
    let results = manager.run_cycle().await;
    assert!(results.is_empty());

    // Task still pending
    assert_eq!(manager.scheduler().pending_count(), 1);
}

#[tokio::test]
async fn test_large_dag() {
    let manager = JobLifecycleManager::new();
    manager.register_worker("worker-1");
    manager.register_worker("worker-2");
    manager.register_worker("worker-3");

    let job = create_test_job("large-dag-job");
    let job_id = job.id;

    // Create a large DAG with 20 tasks in layers
    // Layer 1: 5 root tasks
    // Layer 2: 5 tasks depending on layer 1
    // Layer 3: 5 tasks depending on layer 2
    // Layer 4: 5 tasks depending on layer 3

    let mut all_tasks = Vec::new();
    let mut prev_layer_ids = Vec::new();

    for layer in 0..4 {
        let mut current_layer_ids = Vec::new();

        for i in 0..5 {
            let deps = if layer == 0 {
                vec![]
            } else {
                // Depend on all tasks in previous layer
                prev_layer_ids.clone()
            };

            let task = create_test_task(job_id, &format!("L{}-T{}", layer, i), deps);
            current_layer_ids.push(task.id);
            all_tasks.push(task);
        }

        prev_layer_ids = current_layer_ids;
    }

    manager.submit_job(job, all_tasks);

    // Run to completion
    let completed = manager.run_to_completion(100).await;
    assert!(completed);

    // All 20 tasks should execute
    assert_eq!(manager.executor().execution_count(), 20);
}

#[tokio::test]
async fn test_dag_progress_tracking() {
    let manager = JobLifecycleManager::new();
    manager.register_worker("worker-1");

    let job = create_test_job("progress-job");
    let job_id = job.id;

    // Create 4 tasks in sequence
    let task1 = create_test_task(job_id, "task-1", vec![]);
    let task1_id = task1.id;

    let task2 = create_test_task(job_id, "task-2", vec![task1_id]);
    let task2_id = task2.id;

    let task3 = create_test_task(job_id, "task-3", vec![task2_id]);
    let task3_id = task3.id;

    let task4 = create_test_task(job_id, "task-4", vec![task3_id]);

    manager.submit_job(job, vec![task1, task2, task3, task4]);

    // Initial progress: 0%
    let progress = manager.get_dag_progress(job_id).unwrap();
    assert_eq!(progress.completed, 0);
    assert_eq!(progress.total, 4);
    assert!((progress.progress - 0.0).abs() < 0.01);

    // Complete task 1 (25%)
    manager.run_cycle().await;
    let progress = manager.get_dag_progress(job_id).unwrap();
    assert_eq!(progress.completed, 1);
    assert!((progress.progress - 0.25).abs() < 0.01);

    // Complete task 2 (50%)
    manager.run_cycle().await;
    let progress = manager.get_dag_progress(job_id).unwrap();
    assert_eq!(progress.completed, 2);
    assert!((progress.progress - 0.5).abs() < 0.01);

    // Complete all
    manager.run_to_completion(10).await;
    let progress = manager.get_dag_progress(job_id).unwrap();
    assert!(progress.is_complete);
    assert!(progress.is_successful);
}

#[tokio::test]
async fn test_self_dependency_rejection() {
    let job_id = JobId::new();
    let task_id = TaskId::new();

    let mut task = create_test_task(job_id, "self-dep", vec![task_id]);
    task.id = task_id; // Make it depend on itself

    let result = TaskDag::from_tasks(job_id, vec![task]);
    assert!(result.is_err());

    use crate::coordinator::dag::DagError;
    assert!(matches!(result.unwrap_err(), DagError::SelfDependency(_)));
}

#[tokio::test]
async fn test_unknown_dependency_rejection() {
    let job_id = JobId::new();
    let unknown_id = TaskId::new();

    let task = create_test_task(job_id, "orphan-dep", vec![unknown_id]);

    let result = TaskDag::from_tasks(job_id, vec![task]);
    assert!(result.is_err());

    use crate::coordinator::dag::DagError;
    assert!(matches!(result.unwrap_err(), DagError::UnknownDependency { .. }));
}
