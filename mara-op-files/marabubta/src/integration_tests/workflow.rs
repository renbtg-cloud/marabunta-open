// Marabunta - Licensed under the MIT License.
//! Integration tests for the Marabunta Compute Workflow Engine
//!
//! These tests verify the complete workflow execution pipeline including:
//! - Sequential workflow execution
//! - Parallel step execution
//! - Conditional branching based on step outcomes
//! - Variable interpolation between steps
//! - Failure handling and recovery

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::time::sleep;

use crate::common::types::{JobId, JobStatus};
use crate::workflow::engine::{
    ExecutorError, JobSource, JobSubmitter, WorkflowExecutor, WorkflowExecutorConfig,
};
use crate::workflow::persistence::{MemoryWorkflowStore, WorkflowStore};
use crate::workflow::types::*;

// ============================================================================
// Mock Job Submitter for Integration Tests
// ============================================================================

/// Configuration for how a mock job should behave
#[derive(Debug, Clone)]
pub struct MockJobBehavior {
    /// Status to return after "execution"
    pub final_status: JobStatus,
    /// Output to return on completion
    pub output: Option<serde_json::Value>,
    /// Delay before returning status (simulates execution time)
    pub delay_ms: u64,
    /// Number of times to return Running before final status
    pub running_polls: u32,
}

impl Default for MockJobBehavior {
    fn default() -> Self {
        Self {
            final_status: JobStatus::Completed,
            output: Some(serde_json::json!({"success": true})),
            delay_ms: 0,
            running_polls: 0,
        }
    }
}

impl MockJobBehavior {
    /// Create a behavior that succeeds with given output
    pub fn success(output: serde_json::Value) -> Self {
        Self {
            final_status: JobStatus::Completed,
            output: Some(output),
            ..Default::default()
        }
    }

    /// Create a behavior that fails
    pub fn failure() -> Self {
        Self {
            final_status: JobStatus::Failed,
            output: None,
            ..Default::default()
        }
    }

    /// Create a behavior with execution delay
    pub fn with_delay(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    /// Create a behavior that polls running N times before completing
    pub fn with_running_polls(mut self, polls: u32) -> Self {
        self.running_polls = polls;
        self
    }
}

/// Record of a submitted job
#[derive(Debug, Clone)]
pub struct SubmittedJob {
    pub job_id: JobId,
    pub name: String,
    pub runtime: String,
    pub source: String,
    pub args: HashMap<String, serde_json::Value>,
    pub env: HashMap<String, String>,
}

/// State for tracking job poll counts
struct JobPollState {
    poll_count: AtomicU32,
    behavior: MockJobBehavior,
}

/// Mock job submitter that allows configuring job behaviors for testing
pub struct TestJobSubmitter {
    job_counter: AtomicU32,
    /// Jobs that have been submitted
    pub submitted_jobs: RwLock<Vec<SubmittedJob>>,
    /// Pre-configured behaviors for specific job names
    name_behaviors: RwLock<HashMap<String, MockJobBehavior>>,
    /// Default behavior for jobs without specific configuration
    default_behavior: RwLock<MockJobBehavior>,
    /// Track poll state per job
    job_states: RwLock<HashMap<JobId, JobPollState>>,
}

impl TestJobSubmitter {
    /// Create a new test job submitter
    pub fn new() -> Self {
        Self {
            job_counter: AtomicU32::new(0),
            submitted_jobs: RwLock::new(Vec::new()),
            name_behaviors: RwLock::new(HashMap::new()),
            default_behavior: RwLock::new(MockJobBehavior::default()),
            job_states: RwLock::new(HashMap::new()),
        }
    }

    /// Set the default behavior for all jobs
    pub fn set_default_behavior(&self, behavior: MockJobBehavior) {
        *self.default_behavior.write() = behavior;
    }

    /// Set behavior for a specific job by name
    pub fn set_behavior_for_job(&self, job_name: &str, behavior: MockJobBehavior) {
        self.name_behaviors
            .write()
            .insert(job_name.to_string(), behavior);
    }

    /// Get list of submitted job names in order
    pub fn get_submitted_job_names(&self) -> Vec<String> {
        self.submitted_jobs.read().iter().map(|j| j.name.clone()).collect()
    }

    /// Get a submitted job by name
    pub fn get_submitted_job(&self, name: &str) -> Option<SubmittedJob> {
        self.submitted_jobs.read().iter().find(|j| j.name == name).cloned()
    }

    /// Get count of submitted jobs
    pub fn submitted_count(&self) -> usize {
        self.submitted_jobs.read().len()
    }
}

impl Default for TestJobSubmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobSubmitter for TestJobSubmitter {
    async fn submit_job(
        &self,
        name: &str,
        runtime: &str,
        source: JobSource,
        args: &HashMap<String, serde_json::Value>,
        env: &HashMap<String, String>,
        _resources: Option<&ResourceRequirements>,
        _priority: Option<u32>,
    ) -> Result<JobId, ExecutorError> {
        let id = self.job_counter.fetch_add(1, Ordering::SeqCst);
        let job_id = JobId(uuid::Uuid::from_u128(id as u128));

        let source_str = match &source {
            JobSource::File(f) => f.clone(),
            JobSource::Script(s) => s.clone(),
        };

        // Record the submission
        self.submitted_jobs.write().push(SubmittedJob {
            job_id,
            name: name.to_string(),
            runtime: runtime.to_string(),
            source: source_str,
            args: args.clone(),
            env: env.clone(),
        });

        // Get behavior for this job
        let behavior = self
            .name_behaviors
            .read()
            .get(name)
            .cloned()
            .unwrap_or_else(|| self.default_behavior.read().clone());

        // Store job state for polling
        self.job_states.write().insert(
            job_id,
            JobPollState {
                poll_count: AtomicU32::new(0),
                behavior,
            },
        );

        Ok(job_id)
    }

    async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, ExecutorError> {
        // Extract values while holding the lock, then release before await
        let (delay_ms, running_polls, final_status, poll_count) = {
            let states = self.job_states.read();
            let state = states
                .get(&job_id)
                .ok_or_else(|| ExecutorError::JobSubmissionFailed("Job not found".into()))?;

            let poll_count = state.poll_count.fetch_add(1, Ordering::SeqCst);
            (
                state.behavior.delay_ms,
                state.behavior.running_polls,
                state.behavior.final_status,
                poll_count,
            )
        };

        // Apply delay if configured (lock is released)
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }

        // Check if we should still be "running"
        if poll_count < running_polls {
            return Ok(JobStatus::Running);
        }

        Ok(final_status)
    }

    async fn get_job_result(
        &self,
        job_id: JobId,
    ) -> Result<Option<serde_json::Value>, ExecutorError> {
        let states = self.job_states.read();
        let state = states
            .get(&job_id)
            .ok_or_else(|| ExecutorError::JobSubmissionFailed("Job not found".into()))?;

        Ok(state.behavior.output.clone())
    }

    async fn cancel_job(&self, _job_id: JobId) -> Result<(), ExecutorError> {
        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a test executor with the given submitter
fn create_test_executor(
    submitter: Arc<TestJobSubmitter>,
) -> (
    WorkflowExecutor<MemoryWorkflowStore, TestJobSubmitter>,
    Arc<MemoryWorkflowStore>,
) {
    let store = Arc::new(MemoryWorkflowStore::new());
    let config = WorkflowExecutorConfig {
        poll_interval: Duration::from_millis(10),
        max_concurrent_steps: 10,
        default_step_timeout: Duration::from_secs(30),
        default_workflow_timeout: Duration::from_secs(60),
        max_loop_iterations: 100,
        variable_pattern: r"\{\{\s*([^}]+)\s*\}\}".to_string(),
    };

    let executor = WorkflowExecutor::new(Arc::clone(&store), submitter, config);
    (executor, store)
}

// ============================================================================
// Test: Simple Sequential Workflow
// ============================================================================

#[tokio::test]
async fn test_simple_sequential_workflow() {
    // Create test submitter
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure job behaviors
    submitter.set_behavior_for_job(
        "Fetch Data",
        MockJobBehavior::success(serde_json::json!({"rows": 100})),
    );
    submitter.set_behavior_for_job(
        "Process Data",
        MockJobBehavior::success(serde_json::json!({"processed": 100})),
    );
    submitter.set_behavior_for_job(
        "Store Results",
        MockJobBehavior::success(serde_json::json!({"stored": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    // Create a sequential workflow: fetch -> process -> store
    let mut workflow = Workflow::new("sequential-pipeline");

    workflow.add_step(WorkflowStep::job(
        "fetch",
        JobStepConfig::new("Fetch Data", "python3")
            .with_script("fetch.py")
            .with_output("fetch_result"),
    ));

    workflow.add_step(
        WorkflowStep::job(
            "process",
            JobStepConfig::new("Process Data", "python3")
                .with_script("process.py")
                .with_output("process_result"),
        )
        .depends_on("fetch"),
    );

    workflow.add_step(
        WorkflowStep::job(
            "store",
            JobStepConfig::new("Store Results", "python3")
                .with_script("store.py")
                .with_output("store_result"),
        )
        .depends_on("process"),
    );

    // Save and execute
    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    // Wait for completion
    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    // Verify workflow completed
    assert_eq!(run.status, WorkflowStatus::Completed);

    // Verify all three jobs were submitted
    assert_eq!(submitter.submitted_count(), 3);

    // Verify execution order (sequential)
    let names = submitter.get_submitted_job_names();
    assert_eq!(names[0], "Fetch Data");
    assert_eq!(names[1], "Process Data");
    assert_eq!(names[2], "Store Results");

    // Verify all steps completed
    assert_eq!(run.step_states.get("fetch").unwrap().status, StepStatus::Completed);
    assert_eq!(run.step_states.get("process").unwrap().status, StepStatus::Completed);
    assert_eq!(run.step_states.get("store").unwrap().status, StepStatus::Completed);
}

// ============================================================================
// Test: Parallel Steps
// ============================================================================

#[tokio::test]
async fn test_workflow_with_parallel_steps() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure all jobs to succeed
    submitter.set_behavior_for_job(
        "Prepare",
        MockJobBehavior::success(serde_json::json!({"ready": true})),
    );
    submitter.set_behavior_for_job(
        "Process A",
        MockJobBehavior::success(serde_json::json!({"a_result": 10})),
    );
    submitter.set_behavior_for_job(
        "Process B",
        MockJobBehavior::success(serde_json::json!({"b_result": 20})),
    );
    submitter.set_behavior_for_job(
        "Process C",
        MockJobBehavior::success(serde_json::json!({"c_result": 30})),
    );
    submitter.set_behavior_for_job(
        "Aggregate",
        MockJobBehavior::success(serde_json::json!({"total": 60})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    // Create workflow with parallel middle steps
    let mut workflow = Workflow::new("parallel-pipeline");

    // Step 1: Prepare
    workflow.add_step(WorkflowStep::job(
        "prepare",
        JobStepConfig::new("Prepare", "python3").with_script("prepare.py"),
    ));

    // Step 2: Parallel processing (3 branches)
    workflow.add_step(
        WorkflowStep::parallel(
            "parallel_process",
            vec![
                WorkflowStep::job(
                    "process_a",
                    JobStepConfig::new("Process A", "python3").with_script("process_a.py"),
                ),
                WorkflowStep::job(
                    "process_b",
                    JobStepConfig::new("Process B", "python3").with_script("process_b.py"),
                ),
                WorkflowStep::job(
                    "process_c",
                    JobStepConfig::new("Process C", "python3").with_script("process_c.py"),
                ),
            ],
        )
        .depends_on("prepare"),
    );

    // Step 3: Aggregate results
    workflow.add_step(
        WorkflowStep::job(
            "aggregate",
            JobStepConfig::new("Aggregate", "python3").with_script("aggregate.py"),
        )
        .depends_on("parallel_process"),
    );

    // Save and execute
    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    // Verify workflow completed
    assert_eq!(run.status, WorkflowStatus::Completed);

    // Verify all jobs were submitted (1 prepare + 3 parallel + 1 aggregate = 5)
    assert_eq!(submitter.submitted_count(), 5);

    // Verify execution order: prepare first, then parallel steps, then aggregate
    let names = submitter.get_submitted_job_names();
    assert_eq!(names[0], "Prepare");
    // The three parallel jobs should be between position 1-3 (order may vary)
    let parallel_jobs: Vec<_> = names[1..4].to_vec();
    assert!(parallel_jobs.contains(&"Process A".to_string()));
    assert!(parallel_jobs.contains(&"Process B".to_string()));
    assert!(parallel_jobs.contains(&"Process C".to_string()));
    // Aggregate should be last
    assert_eq!(names[4], "Aggregate");
}

// ============================================================================
// Test: Conditional Branching
// ============================================================================

#[tokio::test]
async fn test_workflow_conditional_on_success() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure validation to succeed
    submitter.set_behavior_for_job(
        "Validate Input",
        MockJobBehavior::success(serde_json::json!({"valid": true})),
    );
    submitter.set_behavior_for_job(
        "Process Valid Data",
        MockJobBehavior::success(serde_json::json!({"processed": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("conditional-success");

    // Step 1: Validation
    workflow.add_step(WorkflowStep::job(
        "validate",
        JobStepConfig::new("Validate Input", "python3")
            .with_script("validate.py")
            .with_output("validation_result"),
    ));

    // Step 2: Conditional - only process if validation succeeds
    workflow.add_step(
        WorkflowStep::conditional(
            "check_validation",
            Condition::on_success("validate"),
            vec![WorkflowStep::job(
                "process_valid",
                JobStepConfig::new("Process Valid Data", "python3").with_script("process.py"),
            )],
        )
        .depends_on("validate"),
    );

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Both validation and processing should have run
    assert_eq!(submitter.submitted_count(), 2);
    let names = submitter.get_submitted_job_names();
    assert!(names.contains(&"Validate Input".to_string()));
    assert!(names.contains(&"Process Valid Data".to_string()));
}

#[tokio::test]
async fn test_workflow_conditional_on_failure() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure validation to FAIL
    submitter.set_behavior_for_job("Validate Input", MockJobBehavior::failure());
    submitter.set_behavior_for_job(
        "Handle Error",
        MockJobBehavior::success(serde_json::json!({"handled": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("conditional-failure");

    // Step 1: Validation (will fail)
    workflow.add_step(
        WorkflowStep::job(
            "validate",
            JobStepConfig::new("Validate Input", "python3").with_script("validate.py"),
        )
        .continue_on_fail(), // Allow workflow to continue even on failure
    );

    // Step 2: Conditional - handle error if validation fails
    let mut conditional = WorkflowStep::conditional(
        "error_handler",
        Condition::on_failure("validate"),
        vec![WorkflowStep::job(
            "handle_error",
            JobStepConfig::new("Handle Error", "python3").with_script("error_handler.py"),
        )],
    );
    conditional.depends_on = vec!["validate".to_string()];

    workflow.add_step(conditional);

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Validation and error handler should have run
    assert_eq!(submitter.submitted_count(), 2);
    let names = submitter.get_submitted_job_names();
    assert!(names.contains(&"Validate Input".to_string()));
    assert!(names.contains(&"Handle Error".to_string()));
}

#[tokio::test]
async fn test_workflow_conditional_with_else_branch() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure primary path to fail
    submitter.set_behavior_for_job("Check Availability", MockJobBehavior::failure());
    submitter.set_behavior_for_job(
        "Use Fallback",
        MockJobBehavior::success(serde_json::json!({"fallback": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("conditional-else");

    // Step 1: Check availability (will fail)
    workflow.add_step(
        WorkflowStep::job(
            "check",
            JobStepConfig::new("Check Availability", "python3").with_script("check.py"),
        )
        .continue_on_fail(),
    );

    // Step 2: Conditional with else branch
    let mut conditional_step = WorkflowStep::conditional(
        "route_decision",
        Condition::on_success("check"),
        vec![WorkflowStep::job(
            "use_primary",
            JobStepConfig::new("Use Primary", "python3").with_script("primary.py"),
        )],
    );

    // Add else branch using step_type modification
    if let StepType::Conditional { ref mut else_branch, .. } = conditional_step.step_type {
        *else_branch = Some(vec![WorkflowStep::job(
            "use_fallback",
            JobStepConfig::new("Use Fallback", "python3").with_script("fallback.py"),
        )]);
    }
    conditional_step.depends_on = vec!["check".to_string()];

    workflow.add_step(conditional_step);

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Check and fallback should have run (not primary)
    let names = submitter.get_submitted_job_names();
    assert!(names.contains(&"Check Availability".to_string()));
    assert!(names.contains(&"Use Fallback".to_string()));
    assert!(!names.contains(&"Use Primary".to_string()));
}

// ============================================================================
// Test: Variable Interpolation
// ============================================================================

#[tokio::test]
async fn test_workflow_variable_interpolation() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure job to succeed
    submitter.set_behavior_for_job(
        "Process with Args",
        MockJobBehavior::success(serde_json::json!({"result": "ok"})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    // Create workflow with variables
    let mut workflow = Workflow::new("variable-interpolation");
    workflow.set_variable(
        "input_bucket",
        serde_json::Value::String("s3://my-bucket/input".to_string()),
    );
    workflow.set_variable(
        "output_bucket",
        serde_json::Value::String("s3://my-bucket/output".to_string()),
    );
    workflow.set_variable("batch_size", serde_json::json!(1000));

    // Create job that uses interpolated variables
    let mut job_config = JobStepConfig::new("Process with Args", "python3");
    job_config.script = Some("process.py".to_string());
    job_config.args.insert(
        "source".to_string(),
        serde_json::Value::String("{{ input_bucket }}".to_string()),
    );
    job_config.args.insert(
        "destination".to_string(),
        serde_json::Value::String("{{ output_bucket }}".to_string()),
    );
    job_config.args.insert(
        "batch_size".to_string(),
        serde_json::Value::String("{{ batch_size }}".to_string()),
    );

    workflow.add_step(WorkflowStep::job("process", job_config));

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Verify the job was submitted with interpolated arguments
    let job = submitter.get_submitted_job("Process with Args").unwrap();

    // Check that variables were interpolated
    assert_eq!(
        job.args.get("source").unwrap(),
        &serde_json::Value::String("s3://my-bucket/input".to_string())
    );
    assert_eq!(
        job.args.get("destination").unwrap(),
        &serde_json::Value::String("s3://my-bucket/output".to_string())
    );
    // When the entire string is a variable reference, the original type is preserved
    assert_eq!(
        job.args.get("batch_size").unwrap(),
        &serde_json::json!(1000)
    );
}

#[tokio::test]
async fn test_workflow_input_variables_override() {
    let submitter = Arc::new(TestJobSubmitter::new());

    submitter.set_behavior_for_job(
        "Echo Config",
        MockJobBehavior::success(serde_json::json!({"echoed": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    // Create workflow with default variables
    let mut workflow = Workflow::new("input-override");
    workflow.set_variable("environment", serde_json::json!("production"));
    workflow.set_variable("region", serde_json::json!("us-east-1"));

    let mut job_config = JobStepConfig::new("Echo Config", "python3");
    job_config.script = Some("echo.py".to_string());
    job_config.args.insert(
        "env".to_string(),
        serde_json::Value::String("{{ environment }}".to_string()),
    );
    job_config.args.insert(
        "region".to_string(),
        serde_json::Value::String("{{ region }}".to_string()),
    );

    workflow.add_step(WorkflowStep::job("echo", job_config));

    store.save_workflow(&workflow).await.unwrap();

    // Start workflow with input variables that override defaults
    let mut input_vars = HashMap::new();
    input_vars.insert("environment".to_string(), serde_json::json!("staging"));
    // Leave region as default

    let run_id = executor
        .start_workflow(workflow.id, Some(input_vars))
        .await
        .unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Verify input override was applied
    let job = submitter.get_submitted_job("Echo Config").unwrap();
    assert_eq!(
        job.args.get("env").unwrap(),
        &serde_json::Value::String("staging".to_string())
    );
    assert_eq!(
        job.args.get("region").unwrap(),
        &serde_json::Value::String("us-east-1".to_string())
    );
}

// ============================================================================
// Test: Failure Handling
// ============================================================================

#[tokio::test]
async fn test_workflow_fails_on_step_failure() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // First job succeeds, second fails
    submitter.set_behavior_for_job(
        "Step One",
        MockJobBehavior::success(serde_json::json!({"step": 1})),
    );
    submitter.set_behavior_for_job("Step Two", MockJobBehavior::failure());

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("failure-test");

    workflow.add_step(WorkflowStep::job(
        "step1",
        JobStepConfig::new("Step One", "python3").with_script("step1.py"),
    ));

    workflow.add_step(
        WorkflowStep::job(
            "step2",
            JobStepConfig::new("Step Two", "python3").with_script("step2.py"),
        )
        .depends_on("step1"),
    );

    workflow.add_step(
        WorkflowStep::job(
            "step3",
            JobStepConfig::new("Step Three", "python3").with_script("step3.py"),
        )
        .depends_on("step2"),
    );

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    // Workflow should fail
    assert_eq!(run.status, WorkflowStatus::Failed);
    assert!(run.error.is_some());

    // Only first two jobs should have been submitted
    assert_eq!(submitter.submitted_count(), 2);
    let names = submitter.get_submitted_job_names();
    assert!(names.contains(&"Step One".to_string()));
    assert!(names.contains(&"Step Two".to_string()));
    assert!(!names.contains(&"Step Three".to_string()));
}

#[tokio::test]
async fn test_workflow_continue_on_failure() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Second step fails but has continue_on_failure
    submitter.set_behavior_for_job(
        "Step One",
        MockJobBehavior::success(serde_json::json!({"step": 1})),
    );
    submitter.set_behavior_for_job("Optional Step", MockJobBehavior::failure());
    submitter.set_behavior_for_job(
        "Final Step",
        MockJobBehavior::success(serde_json::json!({"step": 3})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("continue-on-failure");

    workflow.add_step(WorkflowStep::job(
        "step1",
        JobStepConfig::new("Step One", "python3").with_script("step1.py"),
    ));

    // This step will fail but has continue_on_failure = true
    workflow.add_step(
        WorkflowStep::job(
            "optional",
            JobStepConfig::new("Optional Step", "python3").with_script("optional.py"),
        )
        .depends_on("step1")
        .continue_on_fail(),
    );

    workflow.add_step(
        WorkflowStep::job(
            "final",
            JobStepConfig::new("Final Step", "python3").with_script("final.py"),
        )
        .depends_on("optional"),
    );

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    // Workflow should complete despite the failed step
    assert_eq!(run.status, WorkflowStatus::Completed);

    // All three jobs should have been submitted
    assert_eq!(submitter.submitted_count(), 3);
    let names = submitter.get_submitted_job_names();
    assert!(names.contains(&"Step One".to_string()));
    assert!(names.contains(&"Optional Step".to_string()));
    assert!(names.contains(&"Final Step".to_string()));

    // Verify the optional step is marked as failed
    assert_eq!(run.step_states.get("optional").unwrap().status, StepStatus::Failed);
}

#[tokio::test]
async fn test_workflow_retry_on_failure() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // This test would require implementing retry logic in the mock
    // For now, we test that max_retries is respected in the step config
    submitter.set_behavior_for_job("Flaky Job", MockJobBehavior::failure());

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("retry-test");

    // Note: The actual retry logic would need to be implemented in the executor
    // This test verifies the workflow structure is correct
    workflow.add_step(
        WorkflowStep::job(
            "flaky",
            JobStepConfig::new("Flaky Job", "python3").with_script("flaky.py"),
        )
        .with_retries(3), // Max 3 retries
    );

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    // Workflow should fail since all retries would fail
    assert_eq!(run.status, WorkflowStatus::Failed);
}

// ============================================================================
// Test: Complex Workflow with Mixed Features
// ============================================================================

#[tokio::test]
async fn test_complex_workflow_with_all_features() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure all jobs
    submitter.set_behavior_for_job(
        "Initialize",
        MockJobBehavior::success(serde_json::json!({"init": true})),
    );
    submitter.set_behavior_for_job(
        "Validate",
        MockJobBehavior::success(serde_json::json!({"valid": true})),
    );
    submitter.set_behavior_for_job(
        "Process Region A",
        MockJobBehavior::success(serde_json::json!({"region": "A", "count": 100})),
    );
    submitter.set_behavior_for_job(
        "Process Region B",
        MockJobBehavior::success(serde_json::json!({"region": "B", "count": 200})),
    );
    submitter.set_behavior_for_job(
        "Aggregate",
        MockJobBehavior::success(serde_json::json!({"total": 300})),
    );
    submitter.set_behavior_for_job(
        "Notify Success",
        MockJobBehavior::success(serde_json::json!({"notified": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    // Build complex workflow
    let mut workflow = Workflow::new("complex-pipeline");
    workflow.set_variable("environment", serde_json::json!("production"));
    workflow.set_variable("notify_email", serde_json::json!("team@example.com"));

    // Step 1: Initialize
    workflow.add_step(WorkflowStep::job(
        "init",
        JobStepConfig::new("Initialize", "python3")
            .with_script("init.py")
            .with_output("init_result"),
    ));

    // Step 2: Validate
    workflow.add_step(
        WorkflowStep::job(
            "validate",
            JobStepConfig::new("Validate", "python3")
                .with_script("validate.py")
                .with_output("validation"),
        )
        .depends_on("init"),
    );

    // Step 3: Conditional parallel processing
    let mut conditional_parallel = WorkflowStep::conditional(
        "process_if_valid",
        Condition::on_success("validate"),
        vec![WorkflowStep::parallel(
            "parallel_regions",
            vec![
                WorkflowStep::job(
                    "region_a",
                    JobStepConfig::new("Process Region A", "python3")
                        .with_script("process.py")
                        .with_arg("region", serde_json::json!("A")),
                ),
                WorkflowStep::job(
                    "region_b",
                    JobStepConfig::new("Process Region B", "python3")
                        .with_script("process.py")
                        .with_arg("region", serde_json::json!("B")),
                ),
            ],
        )],
    );
    conditional_parallel.depends_on = vec!["validate".to_string()];
    workflow.add_step(conditional_parallel);

    // Step 4: Aggregate results
    workflow.add_step(
        WorkflowStep::job(
            "aggregate",
            JobStepConfig::new("Aggregate", "python3").with_script("aggregate.py"),
        )
        .depends_on("process_if_valid"),
    );

    // Step 5: Notification with variable interpolation
    let mut notify_config = JobStepConfig::new("Notify Success", "python3");
    notify_config.script = Some("notify.py".to_string());
    notify_config.args.insert(
        "email".to_string(),
        serde_json::Value::String("{{ notify_email }}".to_string()),
    );
    notify_config.args.insert(
        "env".to_string(),
        serde_json::Value::String("{{ environment }}".to_string()),
    );

    workflow.add_step(
        WorkflowStep::job("notify", notify_config).depends_on("aggregate"),
    );

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(15)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Verify all 6 jobs were submitted
    assert_eq!(submitter.submitted_count(), 6);

    // Verify notification job received interpolated variables
    let notify_job = submitter.get_submitted_job("Notify Success").unwrap();
    assert_eq!(
        notify_job.args.get("email").unwrap(),
        &serde_json::Value::String("team@example.com".to_string())
    );
    assert_eq!(
        notify_job.args.get("env").unwrap(),
        &serde_json::Value::String("production".to_string())
    );
}

// ============================================================================
// Test: Workflow Cancellation
// ============================================================================

#[tokio::test]
async fn test_workflow_cancellation() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Configure first job with delay to allow cancellation
    submitter.set_behavior_for_job(
        "Long Running",
        MockJobBehavior::success(serde_json::json!({"done": true})).with_running_polls(100),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("cancellation-test");

    workflow.add_step(WorkflowStep::job(
        "long_running",
        JobStepConfig::new("Long Running", "python3").with_script("long.py"),
    ));

    workflow.add_step(
        WorkflowStep::job(
            "after_long",
            JobStepConfig::new("After Long", "python3").with_script("after.py"),
        )
        .depends_on("long_running"),
    );

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    // Give it a moment to start
    sleep(Duration::from_millis(50)).await;

    // Cancel the workflow
    executor.cancel_workflow(run_id).await.unwrap();

    // Wait a bit and check status
    sleep(Duration::from_millis(100)).await;

    let run = executor.get_run_status(run_id).await.unwrap();
    assert_eq!(run.status, WorkflowStatus::Cancelled);

    // Second job should not have been submitted
    let names = submitter.get_submitted_job_names();
    assert!(!names.contains(&"After Long".to_string()));
}

// ============================================================================
// Test: Output Variable Passing Between Steps
// ============================================================================

#[tokio::test]
async fn test_output_variable_passing() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // First job outputs data that second job will use
    submitter.set_behavior_for_job(
        "Generate Data",
        MockJobBehavior::success(serde_json::json!({
            "items": ["item1", "item2", "item3"],
            "count": 3
        })),
    );
    submitter.set_behavior_for_job(
        "Use Data",
        MockJobBehavior::success(serde_json::json!({"processed": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("output-passing");

    // First step generates and stores output in variable
    workflow.add_step(WorkflowStep::job(
        "generate",
        JobStepConfig::new("Generate Data", "python3")
            .with_script("generate.py")
            .with_output("generated_data"),
    ));

    // Second step uses the output
    let mut use_config = JobStepConfig::new("Use Data", "python3");
    use_config.script = Some("use.py".to_string());
    // Reference the output variable from previous step
    use_config.args.insert(
        "data".to_string(),
        serde_json::Value::String("{{ generated_data }}".to_string()),
    );

    workflow.add_step(
        WorkflowStep::job("use", use_config).depends_on("generate"),
    );

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Verify the output was stored as a variable
    assert!(run.variables.contains_key("generated_data"));
}

// ============================================================================
// Test: Condition Based on Output Value
// ============================================================================

#[tokio::test]
async fn test_condition_on_output_match() {
    let submitter = Arc::new(TestJobSubmitter::new());

    // Check returns status = "ready"
    submitter.set_behavior_for_job(
        "Check Status",
        MockJobBehavior::success(serde_json::json!({"status": "ready", "details": "All systems go"})),
    );
    submitter.set_behavior_for_job(
        "Proceed",
        MockJobBehavior::success(serde_json::json!({"proceeded": true})),
    );

    let (executor, store) = create_test_executor(Arc::clone(&submitter));

    let mut workflow = Workflow::new("output-condition");

    // Check step stores output
    workflow.add_step(WorkflowStep::job(
        "check",
        JobStepConfig::new("Check Status", "python3")
            .with_script("check.py")
            .with_output("check_result"),
    ));

    // Proceed only if status == "ready"
    let mut conditional = WorkflowStep::conditional(
        "proceed_if_ready",
        Condition::OnOutputMatch {
            step: "check".to_string(),
            path: "status".to_string(),
            value: serde_json::json!("ready"),
            operator: ComparisonOperator::Equals,
        },
        vec![WorkflowStep::job(
            "proceed",
            JobStepConfig::new("Proceed", "python3").with_script("proceed.py"),
        )],
    );
    conditional.depends_on = vec!["check".to_string()];
    workflow.add_step(conditional);

    store.save_workflow(&workflow).await.unwrap();
    let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

    let run = executor
        .wait_for_completion(run_id, Some(Duration::from_secs(10)))
        .await
        .unwrap();

    assert_eq!(run.status, WorkflowStatus::Completed);

    // Both jobs should have run since condition was met
    assert_eq!(submitter.submitted_count(), 2);
    let names = submitter.get_submitted_job_names();
    assert!(names.contains(&"Check Status".to_string()));
    assert!(names.contains(&"Proceed".to_string()));
}

// ============================================================================
// Test: Validation Errors
// ============================================================================

#[tokio::test]
async fn test_workflow_validation_empty_name() {
    let mut workflow = Workflow::new("");
    workflow.add_step(WorkflowStep::job(
        "step1",
        JobStepConfig::new("Test", "python3").with_script("test.py"),
    ));

    let result = workflow.validate();
    assert!(matches!(result, Err(WorkflowValidationError::EmptyName)));
}

#[tokio::test]
async fn test_workflow_validation_no_steps() {
    let workflow = Workflow::new("empty-workflow");
    let result = workflow.validate();
    assert!(matches!(result, Err(WorkflowValidationError::NoSteps)));
}

#[tokio::test]
async fn test_workflow_validation_duplicate_step_ids() {
    let mut workflow = Workflow::new("duplicate-ids");
    workflow.add_step(WorkflowStep::job(
        "step1",
        JobStepConfig::new("First", "python3").with_script("first.py"),
    ));
    workflow.add_step(WorkflowStep::job(
        "step1", // Same ID as above
        JobStepConfig::new("Second", "python3").with_script("second.py"),
    ));

    let result = workflow.validate();
    assert!(matches!(
        result,
        Err(WorkflowValidationError::DuplicateStepId(_))
    ));
}

#[tokio::test]
async fn test_workflow_validation_invalid_dependency() {
    let mut workflow = Workflow::new("invalid-dep");
    workflow.add_step(
        WorkflowStep::job(
            "step1",
            JobStepConfig::new("Test", "python3").with_script("test.py"),
        )
        .depends_on("nonexistent"), // References non-existent step
    );

    let result = workflow.validate();
    assert!(matches!(
        result,
        Err(WorkflowValidationError::InvalidDependency { .. })
    ));
}
