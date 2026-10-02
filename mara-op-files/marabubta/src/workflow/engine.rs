// Marabunta - Licensed under the MIT License.
//! Workflow Execution Engine
//!
//! Orchestrates workflow execution with a state machine approach.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use parking_lot::RwLock;
use regex::Regex;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::common::types::{JobId, JobStatus};
use crate::workflow::persistence::WorkflowStore;
use crate::workflow::types::*;

// ============================================================================
// Executor Configuration
// ============================================================================

/// Configuration for the workflow executor
#[derive(Debug, Clone)]
pub struct WorkflowExecutorConfig {
    /// How often to poll for job status updates
    pub poll_interval: Duration,

    /// Maximum concurrent steps to execute
    pub max_concurrent_steps: usize,

    /// Default timeout for steps without explicit timeout
    pub default_step_timeout: Duration,

    /// Default timeout for entire workflow
    pub default_workflow_timeout: Duration,

    /// Maximum loop iterations (safety limit)
    pub max_loop_iterations: u32,

    /// Variable interpolation pattern
    pub variable_pattern: String,
}

impl Default for WorkflowExecutorConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            max_concurrent_steps: 10,
            default_step_timeout: Duration::from_secs(3600), // 1 hour
            default_workflow_timeout: Duration::from_secs(86400), // 24 hours
            max_loop_iterations: 10000,
            variable_pattern: r"\{\{\s*([^}]+)\s*\}\}".to_string(),
        }
    }
}

// ============================================================================
// Executor Errors
// ============================================================================

/// Errors that can occur during workflow execution
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Workflow not found: {0}")]
    WorkflowNotFound(WorkflowId),

    #[error("Run not found: {0}")]
    RunNotFound(WorkflowRunId),

    #[error("Step not found: {0}")]
    StepNotFound(String),

    #[error("Job submission failed: {0}")]
    JobSubmissionFailed(String),

    #[error("Job execution failed: {0}")]
    JobFailed(String),

    #[error("Condition evaluation failed: {0}")]
    ConditionError(String),

    #[error("Variable interpolation failed: {0}")]
    InterpolationError(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Workflow validation failed: {0}")]
    ValidationError(#[from] WorkflowValidationError),

    #[error("Workflow cancelled")]
    Cancelled,

    #[error("Maximum iterations exceeded")]
    MaxIterationsExceeded,
}

// ============================================================================
// Job Submitter Trait
// ============================================================================

/// Trait for submitting jobs to the Marabunta cluster
///
/// This abstraction allows the executor to work with different job submission
/// mechanisms (direct API calls, mock for testing, etc.)
#[async_trait::async_trait]
pub trait JobSubmitter: Send + Sync {
    /// Submit a job and return its ID
    async fn submit_job(
        &self,
        name: &str,
        runtime: &str,
        file_or_script: JobSource,
        args: &HashMap<String, serde_json::Value>,
        env: &HashMap<String, String>,
        resources: Option<&ResourceRequirements>,
        priority: Option<u32>,
    ) -> Result<JobId, ExecutorError>;

    /// Get the current status of a job
    async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, ExecutorError>;

    /// Get the job result/output
    async fn get_job_result(
        &self,
        job_id: JobId,
    ) -> Result<Option<serde_json::Value>, ExecutorError>;

    /// Cancel a job
    async fn cancel_job(&self, job_id: JobId) -> Result<(), ExecutorError>;
}

/// Source for job code
#[derive(Debug, Clone)]
pub enum JobSource {
    /// File path
    File(String),
    /// Inline script content
    Script(String),
}

// ============================================================================
// Workflow Executor
// ============================================================================

/// The workflow execution engine
///
/// Orchestrates the execution of workflow steps, managing dependencies,
/// conditions, parallelism, and loops.
pub struct WorkflowExecutor<S: WorkflowStore, J: JobSubmitter> {
    /// Configuration
    config: WorkflowExecutorConfig,

    /// Workflow storage
    store: Arc<S>,

    /// Job submitter
    job_submitter: Arc<J>,

    /// Active runs
    active_runs: RwLock<HashMap<WorkflowRunId, RunContext>>,

    /// Variable interpolation regex
    var_regex: Regex,
}

/// Context for an active workflow run
struct RunContext {
    /// Cancellation signal sender
    cancel_tx: mpsc::Sender<()>,
}

impl<S: WorkflowStore + 'static, J: JobSubmitter + 'static> WorkflowExecutor<S, J> {
    /// Create a new workflow executor
    pub fn new(store: Arc<S>, job_submitter: Arc<J>, config: WorkflowExecutorConfig) -> Self {
        let var_regex = Regex::new(&config.variable_pattern).expect("Invalid variable pattern");

        Self {
            config,
            store,
            job_submitter,
            active_runs: RwLock::new(HashMap::new()),
            var_regex,
        }
    }

    /// Start executing a workflow
    ///
    /// Returns the run ID immediately. Use `wait_for_completion` or poll
    /// `get_run_status` to track progress.
    pub async fn start_workflow(
        &self,
        workflow_id: WorkflowId,
        input_variables: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<WorkflowRunId, ExecutorError> {
        // Load the workflow
        let workflow = self
            .store
            .get_workflow(workflow_id)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or(ExecutorError::WorkflowNotFound(workflow_id))?;

        // Validate
        workflow.validate()?;

        // Create run instance
        let mut run = WorkflowRun::new(&workflow);

        // Merge input variables
        if let Some(vars) = input_variables {
            for (k, v) in vars {
                run.variables.insert(k, v);
            }
        }

        // Initialize step states
        self.initialize_step_states(&workflow.steps, &mut run);

        // Save initial run state
        self.store
            .save_run(&run)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        let run_id = run.id;

        // Create cancellation channel
        let (cancel_tx, cancel_rx) = mpsc::channel(1);

        // Store run context
        {
            let mut active = self.active_runs.write();
            active.insert(run_id, RunContext { cancel_tx });
        }

        // Spawn execution task
        let executor = self.clone_inner();
        let workflow_clone = workflow.clone();
        tokio::spawn(async move {
            executor
                .execute_workflow(workflow_clone, run_id, cancel_rx)
                .await;
        });

        Ok(run_id)
    }

    /// Cancel a running workflow
    pub async fn cancel_workflow(&self, run_id: WorkflowRunId) -> Result<(), ExecutorError> {
        // Send cancellation signal
        let cancel_tx = {
            let active = self.active_runs.read();
            active.get(&run_id).map(|ctx| ctx.cancel_tx.clone())
        };

        if let Some(tx) = cancel_tx {
            let _ = tx.send(()).await;
        }

        // Update run status
        if let Ok(Some(mut run)) = self.store.get_run(run_id).await {
            run.status = WorkflowStatus::Cancelled;
            run.completed_at = Some(Utc::now());
            let _ = self.store.save_run(&run).await;
        }

        // Remove from active runs
        {
            let mut active = self.active_runs.write();
            active.remove(&run_id);
        }

        Ok(())
    }

    /// Get the current status of a workflow run
    pub async fn get_run_status(
        &self,
        run_id: WorkflowRunId,
    ) -> Result<WorkflowRun, ExecutorError> {
        self.store
            .get_run(run_id)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or(ExecutorError::RunNotFound(run_id))
    }

    /// Wait for a workflow run to complete
    pub async fn wait_for_completion(
        &self,
        run_id: WorkflowRunId,
        timeout: Option<Duration>,
    ) -> Result<WorkflowRun, ExecutorError> {
        let start = std::time::Instant::now();
        let timeout = timeout.unwrap_or(self.config.default_workflow_timeout);

        loop {
            let run = self.get_run_status(run_id).await?;

            if run.is_terminal() {
                return Ok(run);
            }

            if start.elapsed() > timeout {
                return Err(ExecutorError::Timeout(format!(
                    "Workflow run {} timed out after {:?}",
                    run_id, timeout
                )));
            }

            sleep(self.config.poll_interval).await;
        }
    }

    // ========================================================================
    // Internal Execution Logic
    // ========================================================================

    /// Clone inner state for spawned tasks
    fn clone_inner(&self) -> ExecutorInner<S, J> {
        ExecutorInner {
            config: self.config.clone(),
            store: Arc::clone(&self.store),
            job_submitter: Arc::clone(&self.job_submitter),
            var_regex: self.var_regex.clone(),
        }
    }

    /// Initialize step states for all steps in the workflow
    fn initialize_step_states(&self, steps: &[WorkflowStep], run: &mut WorkflowRun) {
        for step in steps {
            run.step_states
                .insert(step.id.clone(), StepState::new(&step.id));

            // Initialize nested steps
            match &step.step_type {
                StepType::Conditional {
                    then, else_branch, ..
                } => {
                    self.initialize_step_states(then, run);
                    if let Some(else_steps) = else_branch {
                        self.initialize_step_states(else_steps, run);
                    }
                }
                StepType::Parallel { branches } => {
                    self.initialize_step_states(branches, run);
                }
                StepType::Loop { body, .. } => {
                    self.initialize_step_states(body, run);
                }
                _ => {}
            }
        }
    }
}

/// Inner executor state for async tasks
struct ExecutorInner<S: WorkflowStore + 'static, J: JobSubmitter + 'static> {
    config: WorkflowExecutorConfig,
    store: Arc<S>,
    job_submitter: Arc<J>,
    var_regex: Regex,
}

impl<S: WorkflowStore + 'static, J: JobSubmitter + 'static> ExecutorInner<S, J> {
    /// Main workflow execution loop
    async fn execute_workflow(
        &self,
        workflow: Workflow,
        run_id: WorkflowRunId,
        mut cancel_rx: mpsc::Receiver<()>,
    ) {
        info!(
            "Starting workflow execution: {} ({})",
            workflow.name, run_id
        );

        // Update status to running
        if let Ok(Some(mut run)) = self.store.get_run(run_id).await {
            run.status = WorkflowStatus::Running;
            let _ = self.store.save_run(&run).await;
        }

        // Execute steps
        let result = tokio::select! {
            result = self.execute_steps(&workflow.steps, run_id) => result,
            _ = cancel_rx.recv() => {
                warn!("Workflow {} cancelled", run_id);
                Err(ExecutorError::Cancelled)
            }
        };

        // Update final status
        if let Ok(Some(mut run)) = self.store.get_run(run_id).await {
            match result {
                Ok(()) => {
                    run.status = WorkflowStatus::Completed;
                    info!("Workflow {} completed successfully", run_id);
                }
                Err(ExecutorError::Cancelled) => {
                    run.status = WorkflowStatus::Cancelled;
                }
                Err(e) => {
                    run.status = WorkflowStatus::Failed;
                    run.error = Some(e.to_string());
                    error!("Workflow {} failed: {}", run_id, e);
                }
            }
            run.completed_at = Some(Utc::now());
            let _ = self.store.save_run(&run).await;
        }
    }

    /// Execute a list of steps
    fn execute_steps<'a>(
        &'a self,
        steps: &'a [WorkflowStep],
        run_id: WorkflowRunId,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutorError>> + Send + 'a>> {
        Box::pin(async move {
        // Build dependency graph
        let mut completed: HashSet<String> = HashSet::new();
        let mut pending: Vec<&WorkflowStep> = steps.iter().collect();

        while !pending.is_empty() {
            // Find steps ready to execute (dependencies satisfied)
            let (ready, not_ready): (Vec<_>, Vec<_>) = pending
                .into_iter()
                .partition(|s| s.depends_on.iter().all(|dep| completed.contains(dep)));

            pending = not_ready;

            if ready.is_empty() && !pending.is_empty() {
                return Err(ExecutorError::ConditionError(
                    "Circular dependency or unsatisfied dependencies".to_string(),
                ));
            }

            // Execute ready steps (up to max concurrent)
            let mut handles = Vec::new();
            for step in ready.into_iter().take(self.config.max_concurrent_steps) {
                let step_clone = step.clone();
                let store = Arc::clone(&self.store);
                let submitter = Arc::clone(&self.job_submitter);
                let config = self.config.clone();
                let var_regex = self.var_regex.clone();

                let handle = tokio::spawn(async move {
                    let inner = ExecutorInner {
                        config,
                        store,
                        job_submitter: submitter,
                        var_regex,
                    };
                    inner.execute_step(&step_clone, run_id).await
                });
                handles.push((step.id.clone(), handle));
            }

            // Wait for all concurrent steps
            for (step_id, handle) in handles {
                match handle.await {
                    Ok(Ok(())) => {
                        completed.insert(step_id);
                    }
                    Ok(Err(e)) => {
                        // Check if step allows continue_on_failure
                        let step = steps.iter().find(|s| s.id == step_id);
                        if step.map(|s| s.continue_on_failure).unwrap_or(false) {
                            warn!("Step {} failed but continuing: {}", step_id, e);
                            completed.insert(step_id);
                        } else {
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        return Err(ExecutorError::JobSubmissionFailed(format!(
                            "Step task panicked: {}",
                            e
                        )));
                    }
                }
            }
        }

        Ok(())
        })
    }

    /// Execute a single step
    fn execute_step(
        &self,
        step: &WorkflowStep,
        run_id: WorkflowRunId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ExecutorError>> + Send + '_>> {
        let step = step.clone();
        let store = Arc::clone(&self.store);
        let job_submitter = Arc::clone(&self.job_submitter);
        let config = self.config.clone();
        let var_regex = self.var_regex.clone();

        Box::pin(async move {
            let inner = ExecutorInner {
                config,
                store,
                job_submitter,
                var_regex,
            };

            debug!("Executing step: {}", step.id);

            // Mark step as running
            inner.update_step_status(run_id, &step.id, |state| {
                state.start();
            })
            .await?;

            let result = match &step.step_type {
                StepType::Job { job } => inner.execute_job_step(&step, job, run_id).await,
                StepType::Conditional {
                    condition,
                    then,
                    else_branch,
                } => {
                    inner.execute_conditional_step(&step, condition, then, else_branch.as_deref(), run_id)
                        .await
                }
                StepType::Parallel { branches } => {
                    inner.execute_parallel_step(&step, branches, run_id).await
                }
                StepType::Loop { condition, body } => {
                    inner.execute_loop_step(&step, condition, body, run_id).await
                }
            };

            // Update step status based on result
            match &result {
                Ok(()) => {
                    inner.update_step_status(run_id, &step.id, |state| {
                        if state.status != StepStatus::Completed {
                            state.complete(None);
                        }
                    })
                    .await?;
                }
                Err(e) => {
                    inner.update_step_status(run_id, &step.id, |state| {
                        state.fail(e.to_string());
                    })
                    .await?;
                }
            }

            result
        })
    }

    /// Execute a job step
    async fn execute_job_step(
        &self,
        step: &WorkflowStep,
        job_config: &JobStepConfig,
        run_id: WorkflowRunId,
    ) -> Result<(), ExecutorError> {
        // Get current run for variable interpolation
        let run = self
            .store
            .get_run(run_id)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or(ExecutorError::RunNotFound(run_id))?;

        // Interpolate variables in args
        let interpolated_args = self.interpolate_args(&job_config.args, &run.variables)?;

        // Determine job source
        let source = if let Some(script) = &job_config.script {
            JobSource::Script(self.interpolate_string(script, &run.variables)?)
        } else if let Some(file) = &job_config.file {
            JobSource::File(self.interpolate_string(file, &run.variables)?)
        } else {
            return Err(ExecutorError::ValidationError(
                WorkflowValidationError::JobMissingSource(step.id.clone()),
            ));
        };

        // Submit job
        let job_id = self
            .job_submitter
            .submit_job(
                &job_config.name,
                &job_config.runtime,
                source,
                &interpolated_args,
                &job_config.env,
                job_config.resources.as_ref(),
                job_config.priority,
            )
            .await?;

        // Update step with job ID
        self.update_step_status(run_id, &step.id, |state| {
            state.job_id = Some(job_id);
            state.status = StepStatus::WaitingForJob;
        })
        .await?;

        // Poll for job completion
        let timeout = step
            .timeout_secs
            .map(Duration::from_secs)
            .unwrap_or(self.config.default_step_timeout);
        let start = std::time::Instant::now();

        loop {
            let status = self.job_submitter.get_job_status(job_id).await?;

            match status {
                JobStatus::Completed => {
                    // Get job output
                    let output = self.job_submitter.get_job_result(job_id).await?;

                    // Store output in variable if configured
                    if let Some(var_name) = &job_config.output_var {
                        if let Some(output_value) = &output {
                            self.set_variable(run_id, var_name, output_value.clone())
                                .await?;
                        }
                    }

                    // Update step state
                    self.update_step_status(run_id, &step.id, |state| {
                        state.complete(output);
                    })
                    .await?;

                    return Ok(());
                }
                JobStatus::Failed => {
                    return Err(ExecutorError::JobFailed(format!(
                        "Job {} failed for step {}",
                        job_id, step.id
                    )));
                }
                JobStatus::Cancelled => {
                    return Err(ExecutorError::Cancelled);
                }
                _ => {
                    // Still running
                    if start.elapsed() > timeout {
                        self.job_submitter.cancel_job(job_id).await?;
                        return Err(ExecutorError::Timeout(format!(
                            "Step {} timed out after {:?}",
                            step.id, timeout
                        )));
                    }
                    sleep(self.config.poll_interval).await;
                }
            }
        }
    }

    /// Execute a conditional step
    async fn execute_conditional_step(
        &self,
        step: &WorkflowStep,
        condition: &Condition,
        then_steps: &[WorkflowStep],
        else_steps: Option<&[WorkflowStep]>,
        run_id: WorkflowRunId,
    ) -> Result<(), ExecutorError> {
        let run = self
            .store
            .get_run(run_id)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or(ExecutorError::RunNotFound(run_id))?;

        let condition_result = self.evaluate_condition(condition, &run)?;
        debug!(
            "Condition for step {} evaluated to: {}",
            step.id, condition_result
        );

        if condition_result {
            self.execute_steps(then_steps, run_id).await
        } else if let Some(else_steps) = else_steps {
            self.execute_steps(else_steps, run_id).await
        } else {
            // No else branch, skip
            Ok(())
        }
    }

    /// Execute a parallel step
    fn execute_parallel_step(
        &self,
        _step: &WorkflowStep,
        branches: &[WorkflowStep],
        run_id: WorkflowRunId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ExecutorError>> + Send + '_>> {
        let branches = branches.to_vec();
        let store = Arc::clone(&self.store);
        let submitter = Arc::clone(&self.job_submitter);
        let config = self.config.clone();
        let var_regex = self.var_regex.clone();

        Box::pin(async move {
            // Execute all branches concurrently
            let mut handles = Vec::new();

            for branch in branches {
                let branch_clone = branch.clone();
                let store = Arc::clone(&store);
                let submitter = Arc::clone(&submitter);
                let config = config.clone();
                let var_regex = var_regex.clone();

                let handle = tokio::spawn(async move {
                    let inner = ExecutorInner {
                        config,
                        store,
                        job_submitter: submitter,
                        var_regex,
                    };
                    inner.execute_step(&branch_clone, run_id).await
                });
                handles.push(handle);
            }

            // Wait for all branches
            let mut errors = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => errors.push(e),
                    Err(e) => errors.push(ExecutorError::JobSubmissionFailed(format!(
                        "Branch task panicked: {}",
                        e
                    ))),
                }
            }

            if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.remove(0))
            }
        })
    }

    /// Execute a loop step
    async fn execute_loop_step(
        &self,
        step: &WorkflowStep,
        condition: &LoopCondition,
        body: &[WorkflowStep],
        run_id: WorkflowRunId,
    ) -> Result<(), ExecutorError> {
        match condition {
            LoopCondition::Count { count, iterator } => {
                for i in 0..*count {
                    self.set_variable(run_id, iterator, serde_json::json!(i))
                        .await?;
                    self.update_step_status(run_id, &step.id, |state| {
                        state.iteration = Some(i);
                    })
                    .await?;
                    self.execute_steps(body, run_id).await?;
                }
            }
            LoopCondition::ForEach { items, item, index } => {
                let run = self
                    .store
                    .get_run(run_id)
                    .await
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?
                    .ok_or(ExecutorError::RunNotFound(run_id))?;

                let items_value = run
                    .variables
                    .get(items)
                    .ok_or_else(|| {
                        ExecutorError::InterpolationError(format!("Variable '{}' not found", items))
                    })?
                    .clone();

                let items_array = items_value.as_array().ok_or_else(|| {
                    ExecutorError::ConditionError(format!("Variable '{}' is not an array", items))
                })?;

                for (i, item_value) in items_array.iter().enumerate() {
                    self.set_variable(run_id, item, item_value.clone()).await?;
                    self.set_variable(run_id, index, serde_json::json!(i))
                        .await?;
                    self.update_step_status(run_id, &step.id, |state| {
                        state.iteration = Some(i as u32);
                    })
                    .await?;
                    self.execute_steps(body, run_id).await?;
                }
            }
            LoopCondition::While {
                condition: cond,
                max_iterations,
            } => {
                let mut iterations = 0u32;
                loop {
                    if iterations >= *max_iterations {
                        return Err(ExecutorError::MaxIterationsExceeded);
                    }

                    let run = self
                        .store
                        .get_run(run_id)
                        .await
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?
                        .ok_or(ExecutorError::RunNotFound(run_id))?;

                    if !self.evaluate_condition(cond, &run)? {
                        break;
                    }

                    self.update_step_status(run_id, &step.id, |state| {
                        state.iteration = Some(iterations);
                    })
                    .await?;
                    self.execute_steps(body, run_id).await?;
                    iterations += 1;
                }
            }
            LoopCondition::Until {
                condition: cond,
                max_iterations,
            } => {
                let mut iterations = 0u32;
                loop {
                    if iterations >= *max_iterations {
                        return Err(ExecutorError::MaxIterationsExceeded);
                    }

                    self.update_step_status(run_id, &step.id, |state| {
                        state.iteration = Some(iterations);
                    })
                    .await?;
                    self.execute_steps(body, run_id).await?;
                    iterations += 1;

                    let run = self
                        .store
                        .get_run(run_id)
                        .await
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?
                        .ok_or(ExecutorError::RunNotFound(run_id))?;

                    if self.evaluate_condition(cond, &run)? {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    // ========================================================================
    // Condition Evaluation
    // ========================================================================

    /// Evaluate a condition
    fn evaluate_condition(
        &self,
        condition: &Condition,
        run: &WorkflowRun,
    ) -> Result<bool, ExecutorError> {
        match condition {
            Condition::OnSuccess { step } => {
                let state = run
                    .step_states
                    .get(step)
                    .ok_or_else(|| ExecutorError::StepNotFound(step.clone()))?;
                Ok(state.status == StepStatus::Completed)
            }
            Condition::OnFailure { step } => {
                let state = run
                    .step_states
                    .get(step)
                    .ok_or_else(|| ExecutorError::StepNotFound(step.clone()))?;
                Ok(state.status == StepStatus::Failed)
            }
            Condition::OnOutputMatch {
                step,
                path,
                value,
                operator,
            } => {
                let state = run
                    .step_states
                    .get(step)
                    .ok_or_else(|| ExecutorError::StepNotFound(step.clone()))?;

                let output = state.output.as_ref().ok_or_else(|| {
                    ExecutorError::ConditionError(format!("Step '{}' has no output", step))
                })?;

                let actual = self.json_path_get(output, path)?;
                self.compare_values(&actual, value, *operator)
            }
            Condition::OnVariable {
                variable,
                value,
                operator,
            } => {
                let actual = run.variables.get(variable).ok_or_else(|| {
                    ExecutorError::ConditionError(format!("Variable '{}' not found", variable))
                })?;
                self.compare_values(actual, value, *operator)
            }
            Condition::And { conditions } => {
                for cond in conditions {
                    if !self.evaluate_condition(cond, run)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Condition::Or { conditions } => {
                for cond in conditions {
                    if self.evaluate_condition(cond, run)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Condition::Not { condition } => Ok(!self.evaluate_condition(condition, run)?),
            Condition::Always => Ok(true),
        }
    }

    /// Simple JSON path getter (supports dot notation)
    fn json_path_get(
        &self,
        value: &serde_json::Value,
        path: &str,
    ) -> Result<serde_json::Value, ExecutorError> {
        let mut current = value.clone();
        for part in path.split('.') {
            current = match current {
                serde_json::Value::Object(map) => {
                    map.get(part).cloned().unwrap_or(serde_json::Value::Null)
                }
                serde_json::Value::Array(arr) => {
                    let idx: usize = part.parse().map_err(|_| {
                        ExecutorError::ConditionError(format!("Invalid array index: {}", part))
                    })?;
                    arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => serde_json::Value::Null,
            };
        }
        Ok(current)
    }

    /// Compare two values with an operator
    fn compare_values(
        &self,
        actual: &serde_json::Value,
        expected: &serde_json::Value,
        operator: ComparisonOperator,
    ) -> Result<bool, ExecutorError> {
        match operator {
            ComparisonOperator::Equals => Ok(actual == expected),
            ComparisonOperator::NotEquals => Ok(actual != expected),
            ComparisonOperator::GreaterThan => {
                let a = self.to_f64(actual)?;
                let b = self.to_f64(expected)?;
                Ok(a > b)
            }
            ComparisonOperator::GreaterThanOrEquals => {
                let a = self.to_f64(actual)?;
                let b = self.to_f64(expected)?;
                Ok(a >= b)
            }
            ComparisonOperator::LessThan => {
                let a = self.to_f64(actual)?;
                let b = self.to_f64(expected)?;
                Ok(a < b)
            }
            ComparisonOperator::LessThanOrEquals => {
                let a = self.to_f64(actual)?;
                let b = self.to_f64(expected)?;
                Ok(a <= b)
            }
            ComparisonOperator::Contains => {
                let a = actual.as_str().unwrap_or_default();
                let b = expected.as_str().unwrap_or_default();
                Ok(a.contains(b))
            }
            ComparisonOperator::StartsWith => {
                let a = actual.as_str().unwrap_or_default();
                let b = expected.as_str().unwrap_or_default();
                Ok(a.starts_with(b))
            }
            ComparisonOperator::EndsWith => {
                let a = actual.as_str().unwrap_or_default();
                let b = expected.as_str().unwrap_or_default();
                Ok(a.ends_with(b))
            }
            ComparisonOperator::Matches => {
                let a = actual.as_str().unwrap_or_default();
                let pattern = expected.as_str().unwrap_or_default();
                let re = Regex::new(pattern).map_err(|e| {
                    ExecutorError::ConditionError(format!("Invalid regex pattern: {}", e))
                })?;
                Ok(re.is_match(a))
            }
        }
    }

    fn to_f64(&self, value: &serde_json::Value) -> Result<f64, ExecutorError> {
        match value {
            serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| {
                ExecutorError::ConditionError("Cannot convert number to f64".to_string())
            }),
            serde_json::Value::String(s) => s.parse().map_err(|_| {
                ExecutorError::ConditionError(format!("Cannot parse '{}' as number", s))
            }),
            _ => Err(ExecutorError::ConditionError(format!(
                "Cannot compare non-numeric value: {:?}",
                value
            ))),
        }
    }

    // ========================================================================
    // Variable Interpolation
    // ========================================================================

    /// Interpolate variables in a string
    fn interpolate_string(
        &self,
        template: &str,
        variables: &HashMap<String, serde_json::Value>,
    ) -> Result<String, ExecutorError> {
        let mut result = template.to_string();

        for cap in self.var_regex.captures_iter(template) {
            let full_match = cap.get(0).unwrap().as_str();
            let var_path = cap.get(1).unwrap().as_str().trim();

            let value = self.resolve_variable_path(var_path, variables)?;
            let replacement = match value {
                serde_json::Value::String(s) => s,
                serde_json::Value::Null => "".to_string(),
                other => other.to_string(),
            };

            result = result.replace(full_match, &replacement);
        }

        Ok(result)
    }

    /// Interpolate variables in arguments
    fn interpolate_args(
        &self,
        args: &HashMap<String, serde_json::Value>,
        variables: &HashMap<String, serde_json::Value>,
    ) -> Result<HashMap<String, serde_json::Value>, ExecutorError> {
        let mut result = HashMap::new();

        for (key, value) in args {
            let interpolated = self.interpolate_value(value, variables)?;
            result.insert(key.clone(), interpolated);
        }

        Ok(result)
    }

    /// Interpolate variables in a JSON value
    fn interpolate_value(
        &self,
        value: &serde_json::Value,
        variables: &HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, ExecutorError> {
        match value {
            serde_json::Value::String(s) => {
                // Check if the entire string is a variable reference
                if let Some(caps) = self.var_regex.captures(s) {
                    if caps.get(0).unwrap().as_str() == s {
                        // Entire string is a variable, return the value directly
                        let var_path = caps.get(1).unwrap().as_str().trim();
                        return self.resolve_variable_path(var_path, variables);
                    }
                }
                // Otherwise interpolate as string
                Ok(serde_json::Value::String(
                    self.interpolate_string(s, variables)?,
                ))
            }
            serde_json::Value::Object(map) => {
                let mut result = serde_json::Map::new();
                for (k, v) in map {
                    result.insert(k.clone(), self.interpolate_value(v, variables)?);
                }
                Ok(serde_json::Value::Object(result))
            }
            serde_json::Value::Array(arr) => {
                let result: Result<Vec<_>, _> = arr
                    .iter()
                    .map(|v| self.interpolate_value(v, variables))
                    .collect();
                Ok(serde_json::Value::Array(result?))
            }
            other => Ok(other.clone()),
        }
    }

    /// Resolve a variable path like "variables.input_bucket" or "steps.fetch.output.data"
    fn resolve_variable_path(
        &self,
        path: &str,
        variables: &HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, ExecutorError> {
        let parts: Vec<&str> = path.split('.').collect();
        if parts.is_empty() {
            return Err(ExecutorError::InterpolationError(
                "Empty variable path".to_string(),
            ));
        }

        // First part determines the namespace
        let (namespace, rest) = if parts[0] == "variables" {
            ("variables", &parts[1..])
        } else {
            // Direct variable reference
            ("", parts.as_slice())
        };

        let var_name = if namespace == "variables" && !rest.is_empty() {
            rest[0]
        } else if !parts.is_empty() {
            parts[0]
        } else {
            return Err(ExecutorError::InterpolationError(format!(
                "Invalid variable path: {}",
                path
            )));
        };

        let mut value = variables.get(var_name).cloned().ok_or_else(|| {
            ExecutorError::InterpolationError(format!("Variable '{}' not found", var_name))
        })?;

        // Navigate nested path
        let remaining = if namespace == "variables" && rest.len() > 1 {
            &rest[1..]
        } else if namespace.is_empty() && parts.len() > 1 {
            &parts[1..]
        } else {
            &[]
        };

        for part in remaining {
            value = match value {
                serde_json::Value::Object(map) => {
                    map.get(*part).cloned().unwrap_or(serde_json::Value::Null)
                }
                serde_json::Value::Array(arr) => {
                    let idx: usize = part.parse().map_err(|_| {
                        ExecutorError::InterpolationError(format!("Invalid array index: {}", part))
                    })?;
                    arr.get(idx).cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => serde_json::Value::Null,
            };
        }

        Ok(value)
    }

    // ========================================================================
    // State Management
    // ========================================================================

    /// Update step status
    async fn update_step_status<F>(
        &self,
        run_id: WorkflowRunId,
        step_id: &str,
        f: F,
    ) -> Result<(), ExecutorError>
    where
        F: FnOnce(&mut StepState),
    {
        let mut run = self
            .store
            .get_run(run_id)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or(ExecutorError::RunNotFound(run_id))?;

        if let Some(state) = run.step_states.get_mut(step_id) {
            f(state);
        } else {
            let mut state = StepState::new(step_id);
            f(&mut state);
            run.step_states.insert(step_id.to_string(), state);
        }

        self.store
            .save_run(&run)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(())
    }

    /// Set a variable in the run
    async fn set_variable(
        &self,
        run_id: WorkflowRunId,
        name: &str,
        value: serde_json::Value,
    ) -> Result<(), ExecutorError> {
        let mut run = self
            .store
            .get_run(run_id)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?
            .ok_or(ExecutorError::RunNotFound(run_id))?;

        run.variables.insert(name.to_string(), value);

        self.store
            .save_run(&run)
            .await
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(())
    }
}

// ============================================================================
// Mock Job Submitter for Testing
// ============================================================================

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Mock job submitter for testing
    pub struct MockJobSubmitter {
        job_counter: AtomicU32,
        pub submitted_jobs: RwLock<Vec<MockSubmittedJob>>,
        pub job_results: RwLock<HashMap<JobId, MockJobResult>>,
    }

    #[derive(Debug, Clone)]
    pub struct MockSubmittedJob {
        pub job_id: JobId,
        pub name: String,
        pub runtime: String,
        pub args: HashMap<String, serde_json::Value>,
    }

    #[derive(Debug, Clone)]
    pub struct MockJobResult {
        pub status: JobStatus,
        pub output: Option<serde_json::Value>,
    }

    impl MockJobSubmitter {
        pub fn new() -> Self {
            Self {
                job_counter: AtomicU32::new(0),
                submitted_jobs: RwLock::new(Vec::new()),
                job_results: RwLock::new(HashMap::new()),
            }
        }

        /// Set up a job to succeed with given output
        pub fn expect_success(&self, job_id: JobId, output: serde_json::Value) {
            self.job_results.write().insert(
                job_id,
                MockJobResult {
                    status: JobStatus::Completed,
                    output: Some(output),
                },
            );
        }

        /// Set up a job to fail
        pub fn expect_failure(&self, job_id: JobId) {
            self.job_results.write().insert(
                job_id,
                MockJobResult {
                    status: JobStatus::Failed,
                    output: None,
                },
            );
        }
    }

    impl Default for MockJobSubmitter {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait::async_trait]
    impl JobSubmitter for MockJobSubmitter {
        async fn submit_job(
            &self,
            name: &str,
            runtime: &str,
            _source: JobSource,
            args: &HashMap<String, serde_json::Value>,
            _env: &HashMap<String, String>,
            _resources: Option<&ResourceRequirements>,
            _priority: Option<u32>,
        ) -> Result<JobId, ExecutorError> {
            let id = self.job_counter.fetch_add(1, Ordering::SeqCst);
            let job_id = JobId(uuid::Uuid::from_u128(id as u128));

            self.submitted_jobs.write().push(MockSubmittedJob {
                job_id,
                name: name.to_string(),
                runtime: runtime.to_string(),
                args: args.clone(),
            });

            // Default to completed if not set
            if !self.job_results.read().contains_key(&job_id) {
                self.job_results.write().insert(
                    job_id,
                    MockJobResult {
                        status: JobStatus::Completed,
                        output: Some(serde_json::json!({"success": true})),
                    },
                );
            }

            Ok(job_id)
        }

        async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, ExecutorError> {
            let results = self.job_results.read();
            let result = results
                .get(&job_id)
                .ok_or_else(|| ExecutorError::JobSubmissionFailed("Job not found".into()))?;
            Ok(result.status)
        }

        async fn get_job_result(
            &self,
            job_id: JobId,
        ) -> Result<Option<serde_json::Value>, ExecutorError> {
            let results = self.job_results.read();
            let result = results
                .get(&job_id)
                .ok_or_else(|| ExecutorError::JobSubmissionFailed("Job not found".into()))?;
            Ok(result.output.clone())
        }

        async fn cancel_job(&self, _job_id: JobId) -> Result<(), ExecutorError> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::persistence::MemoryWorkflowStore;

    #[tokio::test]
    async fn test_simple_workflow_execution() {
        let store = Arc::new(MemoryWorkflowStore::new());
        let submitter = Arc::new(mock::MockJobSubmitter::new());
        let config = WorkflowExecutorConfig::default();

        let executor = WorkflowExecutor::new(store.clone(), submitter.clone(), config);

        // Create a simple workflow
        let mut workflow = Workflow::new("test-workflow");
        workflow.add_step(WorkflowStep::job(
            "step1",
            JobStepConfig::new("Job 1", "python3").with_script("print('hello')"),
        ));

        store.save_workflow(&workflow).await.unwrap();

        // Execute
        let run_id = executor.start_workflow(workflow.id, None).await.unwrap();

        // Wait for completion
        let run = executor
            .wait_for_completion(run_id, Some(Duration::from_secs(10)))
            .await
            .unwrap();

        assert_eq!(run.status, WorkflowStatus::Completed);
    }

    #[tokio::test]
    async fn test_sequential_steps() {
        let store = Arc::new(MemoryWorkflowStore::new());
        let submitter = Arc::new(mock::MockJobSubmitter::new());
        let config = WorkflowExecutorConfig::default();

        let executor = WorkflowExecutor::new(store.clone(), submitter.clone(), config);

        let mut workflow = Workflow::new("sequential-test");
        workflow.add_step(WorkflowStep::job(
            "step1",
            JobStepConfig::new("Job 1", "python3").with_script("print(1)"),
        ));
        workflow.add_step(
            WorkflowStep::job(
                "step2",
                JobStepConfig::new("Job 2", "python3").with_script("print(2)"),
            )
            .depends_on("step1"),
        );

        store.save_workflow(&workflow).await.unwrap();

        let run_id = executor.start_workflow(workflow.id, None).await.unwrap();
        let run = executor
            .wait_for_completion(run_id, Some(Duration::from_secs(10)))
            .await
            .unwrap();

        assert_eq!(run.status, WorkflowStatus::Completed);

        // Verify both jobs were submitted
        let submitted = submitter.submitted_jobs.read();
        assert_eq!(submitted.len(), 2);
    }

    #[tokio::test]
    async fn test_variable_interpolation() {
        let inner: ExecutorInner<MemoryWorkflowStore, mock::MockJobSubmitter> = ExecutorInner {
            config: WorkflowExecutorConfig::default(),
            store: Arc::new(MemoryWorkflowStore::new()),
            job_submitter: Arc::new(mock::MockJobSubmitter::new()),
            var_regex: Regex::new(r"\{\{\s*([^}]+)\s*\}\}").unwrap(),
        };

        let mut vars = HashMap::new();
        vars.insert(
            "bucket".to_string(),
            serde_json::Value::String("s3://data".into()),
        );
        vars.insert("count".to_string(), serde_json::json!(42));

        let result = inner
            .interpolate_string("Source: {{ bucket }}, Count: {{ count }}", &vars)
            .unwrap();
        assert_eq!(result, "Source: s3://data, Count: 42");
    }

    #[tokio::test]
    async fn test_condition_evaluation() {
        let inner: ExecutorInner<MemoryWorkflowStore, mock::MockJobSubmitter> = ExecutorInner {
            config: WorkflowExecutorConfig::default(),
            store: Arc::new(MemoryWorkflowStore::new()),
            job_submitter: Arc::new(mock::MockJobSubmitter::new()),
            var_regex: Regex::new(r"\{\{\s*([^}]+)\s*\}\}").unwrap(),
        };

        let workflow = Workflow::new("test");
        let mut run = WorkflowRun::new(&workflow);

        // Add a completed step
        let mut step_state = StepState::new("step1");
        step_state.complete(Some(serde_json::json!({"status": "success"})));
        run.step_states.insert("step1".to_string(), step_state);

        // Test on_success
        let cond = Condition::on_success("step1");
        assert!(inner.evaluate_condition(&cond, &run).unwrap());

        // Test on_failure (should be false)
        let cond = Condition::on_failure("step1");
        assert!(!inner.evaluate_condition(&cond, &run).unwrap());

        // Test output match
        let cond = Condition::on_output_match("step1", "status", serde_json::json!("success"));
        assert!(inner.evaluate_condition(&cond, &run).unwrap());
    }
}
