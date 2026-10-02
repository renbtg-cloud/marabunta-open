// Marabunta - Licensed under the MIT License.
//! Workflow type definitions
//!
//! Defines the complete workflow model including steps, conditions, and variables.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

// ============================================================================
// Workflow Identifiers
// ============================================================================

/// Unique identifier for a workflow definition
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowId(pub Uuid);

impl WorkflowId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WorkflowId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "wf-{}", &self.0.to_string()[..8])
    }
}

impl std::str::FromStr for WorkflowId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Handle both "wf-xxxxxxxx" and raw UUID formats
        let uuid_str = s.strip_prefix("wf-").unwrap_or(s);
        // If it's a short format, we can't recover the full UUID
        // For now, parse full UUIDs only
        Uuid::parse_str(uuid_str).map(WorkflowId)
    }
}

/// Unique identifier for a workflow execution instance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowRunId(pub Uuid);

impl WorkflowRunId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WorkflowRunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkflowRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "run-{}", &self.0.to_string()[..8])
    }
}

// ============================================================================
// Workflow Definition
// ============================================================================

/// A complete workflow definition
///
/// Workflows consist of steps that can be jobs, conditionals, parallel branches,
/// or loops. Steps can depend on each other and pass data via variables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    /// Unique workflow identifier (assigned on creation)
    #[serde(default)]
    pub id: WorkflowId,

    /// Human-readable name
    pub name: String,

    /// Semantic version (e.g., "1.0.0")
    #[serde(default = "default_version")]
    pub version: String,

    /// Optional description
    #[serde(default)]
    pub description: Option<String>,

    /// Initial variables available to all steps
    #[serde(default)]
    pub variables: HashMap<String, serde_json::Value>,

    /// Workflow steps
    pub steps: Vec<WorkflowStep>,

    /// Workflow metadata
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,

    /// Creation timestamp
    #[serde(default = "Utc::now")]
    pub created_at: DateTime<Utc>,

    /// Last modification timestamp
    #[serde(default = "Utc::now")]
    pub updated_at: DateTime<Utc>,

    /// Optional timeout for the entire workflow
    #[serde(default)]
    pub timeout_secs: Option<u64>,

    /// Maximum number of retries for the workflow
    #[serde(default)]
    pub max_retries: Option<u32>,

    /// Tags for organization and filtering
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_version() -> String {
    "1.0".to_string()
}

impl Workflow {
    /// Create a new workflow with the given name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: WorkflowId::new(),
            name: name.into(),
            version: default_version(),
            description: None,
            variables: HashMap::new(),
            steps: Vec::new(),
            metadata: HashMap::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            timeout_secs: None,
            max_retries: None,
            tags: Vec::new(),
        }
    }

    /// Add a step to the workflow
    pub fn add_step(&mut self, step: WorkflowStep) -> &mut Self {
        self.steps.push(step);
        self.updated_at = Utc::now();
        self
    }

    /// Set a variable
    pub fn set_variable(&mut self, key: impl Into<String>, value: serde_json::Value) -> &mut Self {
        self.variables.insert(key.into(), value);
        self.updated_at = Utc::now();
        self
    }

    /// Validate the workflow structure
    pub fn validate(&self) -> Result<(), WorkflowValidationError> {
        if self.name.is_empty() {
            return Err(WorkflowValidationError::EmptyName);
        }

        if self.steps.is_empty() {
            return Err(WorkflowValidationError::NoSteps);
        }

        // Check for duplicate step IDs
        let mut step_ids = std::collections::HashSet::new();
        for step in &self.steps {
            self.validate_step_ids(step, &mut step_ids)?;
        }

        // Validate dependencies exist
        for step in &self.steps {
            self.validate_step_deps(step, &step_ids)?;
        }

        Ok(())
    }

    fn validate_step_ids(
        &self,
        step: &WorkflowStep,
        ids: &mut std::collections::HashSet<String>,
    ) -> Result<(), WorkflowValidationError> {
        if ids.contains(&step.id) {
            return Err(WorkflowValidationError::DuplicateStepId(step.id.clone()));
        }
        ids.insert(step.id.clone());

        // Recursively check nested steps
        match &step.step_type {
            StepType::Conditional { then, else_branch, condition: _ } => {
                for s in then {
                    self.validate_step_ids(s, ids)?;
                }
                if let Some(else_steps) = else_branch {
                    for s in else_steps {
                        self.validate_step_ids(s, ids)?;
                    }
                }
            }
            StepType::Parallel { branches } => {
                for branch in branches {
                    self.validate_step_ids(branch, ids)?;
                }
            }
            StepType::Loop { body, .. } => {
                for s in body {
                    self.validate_step_ids(s, ids)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn validate_step_deps(
        &self,
        step: &WorkflowStep,
        valid_ids: &std::collections::HashSet<String>,
    ) -> Result<(), WorkflowValidationError> {
        for dep in &step.depends_on {
            if !valid_ids.contains(dep) {
                return Err(WorkflowValidationError::InvalidDependency {
                    step_id: step.id.clone(),
                    dependency: dep.clone(),
                });
            }
        }

        // Recursively check nested steps
        match &step.step_type {
            StepType::Conditional { then, else_branch, condition: _ } => {
                for s in then {
                    self.validate_step_deps(s, valid_ids)?;
                }
                if let Some(else_steps) = else_branch {
                    for s in else_steps {
                        self.validate_step_deps(s, valid_ids)?;
                    }
                }
            }
            StepType::Parallel { branches } => {
                for branch in branches {
                    self.validate_step_deps(branch, valid_ids)?;
                }
            }
            StepType::Loop { body, .. } => {
                for s in body {
                    self.validate_step_deps(s, valid_ids)?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}

// ============================================================================
// Workflow Steps
// ============================================================================

/// A single step in a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStep {
    /// Unique step identifier within the workflow
    pub id: String,

    /// Optional human-readable name
    #[serde(default)]
    pub name: Option<String>,

    /// The step type and its configuration
    #[serde(flatten)]
    pub step_type: StepType,

    /// Steps that must complete before this one
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Optional timeout for this step
    #[serde(default)]
    pub timeout_secs: Option<u64>,

    /// Number of retries if the step fails
    #[serde(default)]
    pub max_retries: Option<u32>,

    /// Whether to continue workflow on step failure
    #[serde(default)]
    pub continue_on_failure: bool,

    /// Step-level metadata
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl WorkflowStep {
    /// Create a new job step
    pub fn job(id: impl Into<String>, job: JobStepConfig) -> Self {
        Self {
            id: id.into(),
            name: None,
            step_type: StepType::Job { job },
            depends_on: Vec::new(),
            timeout_secs: None,
            max_retries: None,
            continue_on_failure: false,
            metadata: HashMap::new(),
        }
    }

    /// Create a new conditional step
    pub fn conditional(
        id: impl Into<String>,
        condition: Condition,
        then: Vec<WorkflowStep>,
    ) -> Self {
        Self {
            id: id.into(),
            name: None,
            step_type: StepType::Conditional {
                condition,
                then,
                else_branch: None,
            },
            depends_on: Vec::new(),
            timeout_secs: None,
            max_retries: None,
            continue_on_failure: false,
            metadata: HashMap::new(),
        }
    }

    /// Create a new parallel step
    pub fn parallel(id: impl Into<String>, branches: Vec<WorkflowStep>) -> Self {
        Self {
            id: id.into(),
            name: None,
            step_type: StepType::Parallel { branches },
            depends_on: Vec::new(),
            timeout_secs: None,
            max_retries: None,
            continue_on_failure: false,
            metadata: HashMap::new(),
        }
    }

    /// Create a new loop step
    pub fn loop_step(
        id: impl Into<String>,
        loop_condition: LoopCondition,
        body: Vec<WorkflowStep>,
    ) -> Self {
        Self {
            id: id.into(),
            name: None,
            step_type: StepType::Loop {
                condition: loop_condition,
                body,
            },
            depends_on: Vec::new(),
            timeout_secs: None,
            max_retries: None,
            continue_on_failure: false,
            metadata: HashMap::new(),
        }
    }

    /// Add a dependency
    pub fn depends_on(mut self, step_id: impl Into<String>) -> Self {
        self.depends_on.push(step_id.into());
        self
    }

    /// Set the step name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = Some(secs);
        self
    }

    /// Set max retries
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Continue on failure
    pub fn continue_on_fail(mut self) -> Self {
        self.continue_on_failure = true;
        self
    }
}

/// Step type with its specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepType {
    /// Execute a Marabunta job
    Job { job: JobStepConfig },

    /// Conditional branching based on previous step outcomes
    Conditional {
        condition: Condition,
        then: Vec<WorkflowStep>,
        #[serde(rename = "else")]
        else_branch: Option<Vec<WorkflowStep>>,
    },

    /// Execute multiple branches in parallel
    Parallel { branches: Vec<WorkflowStep> },

    /// Loop with a condition
    Loop {
        condition: LoopCondition,
        body: Vec<WorkflowStep>,
    },
}

// ============================================================================
// Job Step Configuration
// ============================================================================

/// Configuration for a job step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStepConfig {
    /// Job name
    pub name: String,

    /// Runtime to use (python3, wasm, etc.)
    pub runtime: String,

    /// Job file path (relative to workflow or absolute)
    #[serde(default)]
    pub file: Option<String>,

    /// Inline script content (alternative to file)
    #[serde(default)]
    pub script: Option<String>,

    /// Job arguments (supports variable interpolation)
    #[serde(default)]
    pub args: HashMap<String, serde_json::Value>,

    /// Environment variables
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Resource requirements
    #[serde(default)]
    pub resources: Option<ResourceRequirements>,

    /// Priority level
    #[serde(default)]
    pub priority: Option<u32>,

    /// Output variable name to store job result
    #[serde(default)]
    pub output_var: Option<String>,
}

impl JobStepConfig {
    /// Create a new job step config
    pub fn new(name: impl Into<String>, runtime: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            runtime: runtime.into(),
            file: None,
            script: None,
            args: HashMap::new(),
            env: HashMap::new(),
            resources: None,
            priority: None,
            output_var: None,
        }
    }

    /// Set the job file
    pub fn with_file(mut self, file: impl Into<String>) -> Self {
        self.file = Some(file.into());
        self
    }

    /// Set an inline script
    pub fn with_script(mut self, script: impl Into<String>) -> Self {
        self.script = Some(script.into());
        self
    }

    /// Add an argument
    pub fn with_arg(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.args.insert(key.into(), value.into());
        self
    }

    /// Set output variable name
    pub fn with_output(mut self, var: impl Into<String>) -> Self {
        self.output_var = Some(var.into());
        self
    }
}

/// Resource requirements for a job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequirements {
    /// Minimum memory in bytes
    #[serde(default)]
    pub memory_min: Option<u64>,

    /// Minimum disk space in bytes
    #[serde(default)]
    pub disk_min: Option<u64>,

    /// Minimum CPU cores
    #[serde(default)]
    pub cpu_min: Option<u32>,

    /// Whether GPU is required
    #[serde(default)]
    pub gpu_required: bool,
}

// ============================================================================
// Conditions
// ============================================================================

/// Condition for conditional steps
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    /// Previous step succeeded
    OnSuccess {
        /// Step ID to check
        step: String,
    },

    /// Previous step failed
    OnFailure {
        /// Step ID to check
        step: String,
    },

    /// Check if output matches a pattern
    OnOutputMatch {
        /// Step ID whose output to check
        step: String,
        /// JSON path to value (e.g., "result.status")
        path: String,
        /// Expected value
        value: serde_json::Value,
        /// Comparison operator
        #[serde(default)]
        operator: ComparisonOperator,
    },

    /// Check a variable value
    OnVariable {
        /// Variable name
        variable: String,
        /// Expected value
        value: serde_json::Value,
        /// Comparison operator
        #[serde(default)]
        operator: ComparisonOperator,
    },

    /// Logical AND of multiple conditions
    And { conditions: Vec<Condition> },

    /// Logical OR of multiple conditions
    Or { conditions: Vec<Condition> },

    /// Logical NOT of a condition
    Not { condition: Box<Condition> },

    /// Always true (for unconditional else branches)
    Always,
}

impl Condition {
    /// Create an on_success condition
    pub fn on_success(step: impl Into<String>) -> Self {
        Condition::OnSuccess { step: step.into() }
    }

    /// Create an on_failure condition
    pub fn on_failure(step: impl Into<String>) -> Self {
        Condition::OnFailure { step: step.into() }
    }

    /// Create an output match condition
    pub fn on_output_match(
        step: impl Into<String>,
        path: impl Into<String>,
        value: serde_json::Value,
    ) -> Self {
        Condition::OnOutputMatch {
            step: step.into(),
            path: path.into(),
            value,
            operator: ComparisonOperator::default(),
        }
    }

    /// Create a variable condition
    pub fn on_variable(variable: impl Into<String>, value: serde_json::Value) -> Self {
        Condition::OnVariable {
            variable: variable.into(),
            value,
            operator: ComparisonOperator::default(),
        }
    }

    /// Combine with AND
    pub fn and(self, other: Condition) -> Self {
        match self {
            Condition::And { mut conditions } => {
                conditions.push(other);
                Condition::And { conditions }
            }
            _ => Condition::And {
                conditions: vec![self, other],
            },
        }
    }

    /// Combine with OR
    pub fn or(self, other: Condition) -> Self {
        match self {
            Condition::Or { mut conditions } => {
                conditions.push(other);
                Condition::Or { conditions }
            }
            _ => Condition::Or {
                conditions: vec![self, other],
            },
        }
    }

    /// Negate the condition
    pub fn not(self) -> Self {
        Condition::Not {
            condition: Box::new(self),
        }
    }
}

/// Comparison operators for conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOperator {
    #[default]
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEquals,
    LessThan,
    LessThanOrEquals,
    Contains,
    StartsWith,
    EndsWith,
    Matches, // Regex match
}

// ============================================================================
// Loop Conditions
// ============================================================================

/// Loop condition for loop steps
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LoopCondition {
    /// Loop a fixed number of times
    Count {
        /// Number of iterations
        count: u32,
        /// Variable name for current iteration index
        #[serde(default = "default_iterator")]
        iterator: String,
    },

    /// Loop over items in a variable
    ForEach {
        /// Variable containing items to iterate
        items: String,
        /// Variable name for current item
        #[serde(default = "default_item")]
        item: String,
        /// Variable name for current index
        #[serde(default = "default_index")]
        index: String,
    },

    /// Loop while condition is true
    While {
        /// Condition to check each iteration
        condition: Box<Condition>,
        /// Maximum iterations (safety limit)
        #[serde(default = "default_max_iterations")]
        max_iterations: u32,
    },

    /// Loop until condition becomes true
    Until {
        /// Condition to check each iteration
        condition: Box<Condition>,
        /// Maximum iterations (safety limit)
        #[serde(default = "default_max_iterations")]
        max_iterations: u32,
    },
}

fn default_iterator() -> String {
    "i".to_string()
}

fn default_item() -> String {
    "item".to_string()
}

fn default_index() -> String {
    "index".to_string()
}

fn default_max_iterations() -> u32 {
    1000
}

// ============================================================================
// Workflow Execution State
// ============================================================================

/// A workflow execution instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRun {
    /// Unique run identifier
    pub id: WorkflowRunId,

    /// Workflow being executed
    pub workflow_id: WorkflowId,

    /// Workflow version at time of execution
    pub workflow_version: String,

    /// Current execution status
    pub status: WorkflowStatus,

    /// Current variables (including outputs from steps)
    pub variables: HashMap<String, serde_json::Value>,

    /// Step execution states
    pub step_states: HashMap<String, StepState>,

    /// When the run started
    pub started_at: DateTime<Utc>,

    /// When the run completed (if finished)
    pub completed_at: Option<DateTime<Utc>>,

    /// Error message if failed
    pub error: Option<String>,

    /// Retry count
    pub retry_count: u32,

    /// Run metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl WorkflowRun {
    /// Create a new workflow run
    pub fn new(workflow: &Workflow) -> Self {
        Self {
            id: WorkflowRunId::new(),
            workflow_id: workflow.id,
            workflow_version: workflow.version.clone(),
            status: WorkflowStatus::Pending,
            variables: workflow.variables.clone(),
            step_states: HashMap::new(),
            started_at: Utc::now(),
            completed_at: None,
            error: None,
            retry_count: 0,
            metadata: HashMap::new(),
        }
    }

    /// Get the state of a specific step
    pub fn get_step_state(&self, step_id: &str) -> Option<&StepState> {
        self.step_states.get(step_id)
    }

    /// Check if the run is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
    }

    /// Calculate overall progress (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        let total = self.step_states.len();
        if total == 0 {
            return 0.0;
        }

        let completed = self
            .step_states
            .values()
            .filter(|s| {
                matches!(
                    s.status,
                    StepStatus::Completed | StepStatus::Skipped | StepStatus::Failed
                )
            })
            .count();

        completed as f64 / total as f64
    }
}

/// Workflow execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Waiting to start
    Pending,
    /// Currently executing
    Running,
    /// Successfully completed
    Completed,
    /// Failed with error
    Failed,
    /// Cancelled by user
    Cancelled,
    /// Paused (can be resumed)
    Paused,
}

impl fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowStatus::Pending => write!(f, "Pending"),
            WorkflowStatus::Running => write!(f, "Running"),
            WorkflowStatus::Completed => write!(f, "Completed"),
            WorkflowStatus::Failed => write!(f, "Failed"),
            WorkflowStatus::Cancelled => write!(f, "Cancelled"),
            WorkflowStatus::Paused => write!(f, "Paused"),
        }
    }
}

/// Execution state of a single step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepState {
    /// Step ID
    pub step_id: String,

    /// Current status
    pub status: StepStatus,

    /// Associated job ID (if this is a job step)
    pub job_id: Option<crate::common::types::JobId>,

    /// Step output (if completed)
    pub output: Option<serde_json::Value>,

    /// Error message (if failed)
    pub error: Option<String>,

    /// When the step started
    pub started_at: Option<DateTime<Utc>>,

    /// When the step completed
    pub completed_at: Option<DateTime<Utc>>,

    /// Number of retries attempted
    pub retry_count: u32,

    /// For loop steps: current iteration
    pub iteration: Option<u32>,
}

impl StepState {
    /// Create a new pending step state
    pub fn new(step_id: impl Into<String>) -> Self {
        Self {
            step_id: step_id.into(),
            status: StepStatus::Pending,
            job_id: None,
            output: None,
            error: None,
            started_at: None,
            completed_at: None,
            retry_count: 0,
            iteration: None,
        }
    }

    /// Mark the step as running
    pub fn start(&mut self) {
        self.status = StepStatus::Running;
        self.started_at = Some(Utc::now());
    }

    /// Mark the step as completed with output
    pub fn complete(&mut self, output: Option<serde_json::Value>) {
        self.status = StepStatus::Completed;
        self.completed_at = Some(Utc::now());
        self.output = output;
    }

    /// Mark the step as failed
    pub fn fail(&mut self, error: impl Into<String>) {
        self.status = StepStatus::Failed;
        self.completed_at = Some(Utc::now());
        self.error = Some(error.into());
    }

    /// Mark the step as skipped
    pub fn skip(&mut self) {
        self.status = StepStatus::Skipped;
        self.completed_at = Some(Utc::now());
    }
}

/// Step execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Waiting for dependencies
    Pending,
    /// Dependencies met, waiting to run
    Ready,
    /// Currently executing
    Running,
    /// Waiting for job to complete
    WaitingForJob,
    /// Successfully completed
    Completed,
    /// Failed with error
    Failed,
    /// Skipped (condition not met or continue_on_failure)
    Skipped,
    /// Cancelled
    Cancelled,
}

impl fmt::Display for StepStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StepStatus::Pending => write!(f, "Pending"),
            StepStatus::Ready => write!(f, "Ready"),
            StepStatus::Running => write!(f, "Running"),
            StepStatus::WaitingForJob => write!(f, "Waiting for Job"),
            StepStatus::Completed => write!(f, "Completed"),
            StepStatus::Failed => write!(f, "Failed"),
            StepStatus::Skipped => write!(f, "Skipped"),
            StepStatus::Cancelled => write!(f, "Cancelled"),
        }
    }
}

// ============================================================================
// Validation Errors
// ============================================================================

/// Workflow validation error
#[derive(Debug, Clone, thiserror::Error)]
pub enum WorkflowValidationError {
    #[error("Workflow name cannot be empty")]
    EmptyName,

    #[error("Workflow must have at least one step")]
    NoSteps,

    #[error("Duplicate step ID: {0}")]
    DuplicateStepId(String),

    #[error("Step '{step_id}' has invalid dependency: '{dependency}'")]
    InvalidDependency { step_id: String, dependency: String },

    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("Job step '{0}' must have either 'file' or 'script'")]
    JobMissingSource(String),

    #[error("Invalid YAML: {0}")]
    InvalidYaml(String),
}

// ============================================================================
// YAML Parsing
// ============================================================================

impl Workflow {
    /// Parse a workflow from YAML
    pub fn from_yaml(yaml: &str) -> Result<Self, WorkflowValidationError> {
        // First, we need to add serde_yaml support
        // For now, we'll use a JSON-compatible subset approach
        let workflow: Workflow = serde_json::from_str(yaml).map_err(|e| {
            // Try YAML-style parsing hints
            WorkflowValidationError::InvalidYaml(e.to_string())
        })?;

        workflow.validate()?;
        Ok(workflow)
    }

    /// Serialize workflow to YAML
    pub fn to_yaml(&self) -> Result<String, WorkflowValidationError> {
        serde_json::to_string_pretty(self)
            .map_err(|e| WorkflowValidationError::InvalidYaml(e.to_string()))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_id_display() {
        let id = WorkflowId::new();
        let display = format!("{}", id);
        assert!(display.starts_with("wf-"));
        assert_eq!(display.len(), 11); // "wf-" + 8 chars
    }

    #[test]
    fn test_workflow_creation() {
        let mut workflow = Workflow::new("test-pipeline");
        workflow.add_step(WorkflowStep::job(
            "step1",
            JobStepConfig::new("Test Job", "python3").with_file("test.py"),
        ));

        assert_eq!(workflow.name, "test-pipeline");
        assert_eq!(workflow.steps.len(), 1);
    }

    #[test]
    fn test_workflow_validation_empty_name() {
        let workflow = Workflow::new("");
        assert!(matches!(
            workflow.validate(),
            Err(WorkflowValidationError::EmptyName)
        ));
    }

    #[test]
    fn test_workflow_validation_no_steps() {
        let workflow = Workflow::new("test");
        assert!(matches!(
            workflow.validate(),
            Err(WorkflowValidationError::NoSteps)
        ));
    }

    #[test]
    fn test_workflow_validation_duplicate_ids() {
        let mut workflow = Workflow::new("test");
        workflow.add_step(WorkflowStep::job(
            "step1",
            JobStepConfig::new("Job 1", "python3"),
        ));
        workflow.add_step(WorkflowStep::job(
            "step1",
            JobStepConfig::new("Job 2", "python3"),
        ));

        assert!(matches!(
            workflow.validate(),
            Err(WorkflowValidationError::DuplicateStepId(_))
        ));
    }

    #[test]
    fn test_workflow_validation_invalid_dependency() {
        let mut workflow = Workflow::new("test");
        workflow.add_step(
            WorkflowStep::job("step1", JobStepConfig::new("Job 1", "python3"))
                .depends_on("nonexistent"),
        );

        assert!(matches!(
            workflow.validate(),
            Err(WorkflowValidationError::InvalidDependency { .. })
        ));
    }

    #[test]
    fn test_condition_builder() {
        let cond = Condition::on_success("step1")
            .and(Condition::on_variable(
                "status",
                serde_json::Value::String("ready".into()),
            ))
            .or(Condition::on_failure("step0"));

        match cond {
            Condition::Or { conditions } => {
                assert_eq!(conditions.len(), 2);
            }
            _ => panic!("Expected Or condition"),
        }
    }

    #[test]
    fn test_step_state_transitions() {
        let mut state = StepState::new("test");
        assert_eq!(state.status, StepStatus::Pending);

        state.start();
        assert_eq!(state.status, StepStatus::Running);
        assert!(state.started_at.is_some());

        state.complete(Some(serde_json::json!({"result": 42})));
        assert_eq!(state.status, StepStatus::Completed);
        assert!(state.completed_at.is_some());
        assert!(state.output.is_some());
    }

    #[test]
    fn test_workflow_run_progress() {
        let workflow = Workflow::new("test");
        let mut run = WorkflowRun::new(&workflow);

        // Add some step states
        let mut s1 = StepState::new("step1");
        s1.complete(None);
        run.step_states.insert("step1".to_string(), s1);

        let s2 = StepState::new("step2");
        run.step_states.insert("step2".to_string(), s2);

        let progress = run.progress();
        assert!((progress - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_loop_condition_types() {
        let count_loop = LoopCondition::Count {
            count: 10,
            iterator: "i".to_string(),
        };

        let foreach_loop = LoopCondition::ForEach {
            items: "data".to_string(),
            item: "item".to_string(),
            index: "idx".to_string(),
        };

        let while_loop = LoopCondition::While {
            condition: Box::new(Condition::on_variable(
                "continue",
                serde_json::Value::Bool(true),
            )),
            max_iterations: 100,
        };

        // Verify serialization works
        let json = serde_json::to_string(&count_loop).unwrap();
        assert!(json.contains("count"));

        let json = serde_json::to_string(&foreach_loop).unwrap();
        assert!(json.contains("for_each"));

        let json = serde_json::to_string(&while_loop).unwrap();
        assert!(json.contains("while"));
    }
}
