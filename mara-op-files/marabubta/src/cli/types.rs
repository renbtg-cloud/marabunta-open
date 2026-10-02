// Marabunta - Licensed under the MIT License.
//! CLI-specific types and enums

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

// Note: JobId, TaskId, WorkerId may be used by callers via re-exports

/// Output format for CLI commands
#[derive(Debug, Clone, Copy, Default, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Human-readable output with colors and formatting
    #[default]
    Human,
    /// JSON output for scripting
    Json,
    /// CSV output for spreadsheets
    Csv,
    /// YAML output
    Yaml,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputFormat::Human => write!(f, "human"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Yaml => write!(f, "yaml"),
        }
    }
}

/// Job priority level
#[derive(Debug, Clone, Copy, Default, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    /// Low priority - runs when resources available
    Low,
    /// Normal priority
    #[default]
    Normal,
    /// High priority - preempts lower priority jobs
    High,
    /// Critical priority - maximum scheduling preference
    Critical,
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Priority::Low => write!(f, "low"),
            Priority::Normal => write!(f, "normal"),
            Priority::High => write!(f, "high"),
            Priority::Critical => write!(f, "critical"),
        }
    }
}

impl Priority {
    pub fn to_u32(&self) -> u32 {
        match self {
            Priority::Low => 0,
            Priority::Normal => 1,
            Priority::High => 2,
            Priority::Critical => 3,
        }
    }
}

/// Node quality preference for placement
#[derive(Debug, Clone, Copy, Default, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeQuality {
    /// Prefer fewer, more powerful nodes
    FewerBetter,
    /// Prefer more smaller nodes (better parallelism)
    MoreSmaller,
    /// Balance between node count and capability
    #[default]
    Balanced,
}

impl fmt::Display for NodeQuality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeQuality::FewerBetter => write!(f, "fewer_better"),
            NodeQuality::MoreSmaller => write!(f, "more_smaller"),
            NodeQuality::Balanced => write!(f, "balanced"),
        }
    }
}

/// Placement strategy for job distribution
#[derive(Debug, Clone, Copy, Default, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PlacementStrategy {
    /// Maximize throughput (tasks per second)
    #[default]
    Throughput,
    /// Minimize latency (time to first result)
    Latency,
    /// Minimize cost (token usage)
    Cost,
    /// Balance all factors
    Balanced,
}

impl fmt::Display for PlacementStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlacementStrategy::Throughput => write!(f, "throughput"),
            PlacementStrategy::Latency => write!(f, "latency"),
            PlacementStrategy::Cost => write!(f, "cost"),
            PlacementStrategy::Balanced => write!(f, "balanced"),
        }
    }
}

/// Preset configurations for common use cases
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Preset {
    /// High-end nodes, fewer but more powerful
    Beefy,
    /// Many small nodes, maximum parallelism
    Swarm,
    /// Balanced approach
    Balanced,
    /// Minimize token usage
    Cheap,
}

impl fmt::Display for Preset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Preset::Beefy => write!(f, "beefy"),
            Preset::Swarm => write!(f, "swarm"),
            Preset::Balanced => write!(f, "balanced"),
            Preset::Cheap => write!(f, "cheap"),
        }
    }
}

/// Job specification for submission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    /// Job name
    pub name: String,
    /// Runtime to use
    pub runtime: String,
    /// Job file content (base64 encoded for binary)
    pub file_content: String,
    /// File name
    pub file_name: String,
    /// Number of samples (for Monte Carlo)
    pub samples: Option<u64>,
    /// Samples per task
    pub samples_per_task: u32,
    /// Convergence threshold
    pub converge: Option<f64>,
    /// Hard constraints
    pub constraints: JobConstraints,
    /// Soft preferences
    pub preferences: JobPreferences,
    /// Placement strategy
    pub placement: PlacementConfig,
    /// Priority level
    pub priority: u32,
    /// Task timeout in seconds
    pub timeout_secs: u64,
}

/// Hard constraints for job placement
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobConstraints {
    /// Minimum memory per task in bytes
    pub memory_min: Option<u64>,
    /// Minimum disk space per task in bytes
    pub disk_min: Option<u64>,
    /// Required architectures
    pub architectures: Vec<String>,
    /// Excluded nodes
    pub excluded_nodes: Vec<String>,
    /// Minimum reliability score
    pub reliability_min: Option<f32>,
    /// Exclude battery-powered nodes
    pub exclude_battery: bool,
    /// Minimum memory to include node
    pub exclude_memory_below: Option<u64>,
}

/// Soft preferences for job placement
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobPreferences {
    /// Preferred memory per task in bytes
    pub memory_prefer: Option<u64>,
    /// Preferred cores per task
    pub cores_prefer: Option<u32>,
    /// Prefer nodes in same region
    pub prefer_local: bool,
    /// Preferred runtimes (in order)
    pub preferred_runtimes: Vec<String>,
}

/// Placement configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlacementConfig {
    /// Target total cores
    pub target_cores: Option<u32>,
    /// Target total memory in bytes
    pub target_memory: Option<u64>,
    /// Node quality preference
    pub node_quality: NodeQuality,
    /// Placement strategy
    pub strategy: PlacementStrategy,
}

/// Job status response from coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusResponse {
    /// Job ID
    pub job_id: String,
    /// Job name
    pub name: String,
    /// Current state
    pub state: JobState,
    /// Progress (0.0 to 1.0)
    pub progress: f64,
    /// Total tasks
    pub tasks_total: u32,
    /// Completed tasks
    pub tasks_completed: u32,
    /// Failed tasks
    pub tasks_failed: u32,
    /// Running tasks
    pub tasks_running: u32,
    /// Tasks waiting for dependencies
    #[serde(default)]
    pub tasks_waiting: u32,
    /// Tasks skipped due to failed dependencies
    #[serde(default)]
    pub tasks_skipped: u32,
    /// Active nodes
    pub nodes_active: u32,
    /// Start time
    pub started_at: Option<String>,
    /// Current runtime in seconds
    pub runtime_secs: u64,
    /// Estimated time to completion in seconds
    pub eta_secs: Option<u64>,
    /// Current estimate (for Monte Carlo jobs)
    pub current_estimate: Option<MonteCarloEstimate>,
    /// Whether job uses task dependencies (DAG)
    #[serde(default)]
    pub has_dependencies: bool,
}

impl JobStatusResponse {
    /// Check if job is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            JobState::Completed | JobState::Failed | JobState::Cancelled
        )
    }
}

/// Job state enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JobState::Pending => write!(f, "Pending"),
            JobState::Scheduled => write!(f, "Scheduled"),
            JobState::Running => write!(f, "Running"),
            JobState::Completed => write!(f, "Completed"),
            JobState::Failed => write!(f, "Failed"),
            JobState::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// Monte Carlo estimate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonteCarloEstimate {
    /// Current mean estimate
    pub mean: f64,
    /// Standard error
    pub std_error: f64,
    /// 95% confidence interval
    pub confidence_interval: (f64, f64),
    /// Whether convergence criteria met
    pub converged: Option<bool>,
    /// Total samples processed
    pub samples_processed: u64,
}

/// Job results response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResults {
    /// Job ID
    pub job_id: String,
    /// Job name
    pub name: String,
    /// Job state
    pub state: JobState,
    /// Final result value (if applicable)
    pub result: Option<serde_json::Value>,
    /// Task results
    pub tasks: Vec<TaskResult>,
    /// Statistics
    pub stats: JobStats,
    /// Partial results flag
    pub is_partial: bool,
}

/// Individual task result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Task ID
    pub task_id: String,
    /// Optional task name
    #[serde(default)]
    pub name: Option<String>,
    /// Task status
    pub status: String,
    /// Task state in DAG workflow
    #[serde(default)]
    pub state: Option<String>,
    /// Task output
    pub output: Option<serde_json::Value>,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Worker that executed the task
    pub worker_id: Option<String>,
    /// Tasks this task depends on
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// Task dependency information for display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependencyInfo {
    /// Task ID
    pub task_id: String,
    /// Optional task name
    pub name: Option<String>,
    /// Task state
    pub state: String,
    /// Tasks this task depends on
    pub depends_on: Vec<String>,
    /// Tasks that depend on this task
    pub dependents: Vec<String>,
    /// Whether all dependencies are satisfied
    pub dependencies_satisfied: bool,
    /// Whether any dependency has failed
    pub has_failed_dependency: bool,
}

/// DAG submission spec for CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagTaskSpec {
    /// Task name (for referencing in dependencies)
    pub name: String,
    /// Command to execute
    pub command: String,
    /// Command arguments
    #[serde(default)]
    pub args: Vec<String>,
    /// Names of tasks this task depends on
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// DAG job specification for YAML/JSON input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagJobSpec {
    /// Job name
    pub name: String,
    /// List of tasks with dependencies
    pub tasks: Vec<DagTaskSpec>,
    /// Job priority
    #[serde(default)]
    pub priority: Option<u32>,
    /// Task timeout in seconds
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// Job statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStats {
    /// Total tasks
    pub total_tasks: u32,
    /// Completed tasks
    pub completed_tasks: u32,
    /// Failed tasks
    pub failed_tasks: u32,
    /// Total runtime in seconds
    pub total_runtime_secs: u64,
    /// Average task duration in milliseconds
    pub avg_task_duration_ms: u64,
    /// Total nodes used
    pub nodes_used: u32,
    /// Tokens spent
    pub tokens_spent: f64,
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub id: String,
    /// Node type (e.g., "raspberry_pi", "desktop", "server")
    pub node_type: String,
    /// CPU cores
    pub cores: u32,
    /// Memory in bytes
    pub memory: u64,
    /// Available disk in bytes
    pub disk: u64,
    /// Node status
    pub status: String,
    /// Supported runtimes
    pub runtimes: Vec<String>,
    /// Region
    pub region: Option<String>,
    /// Architecture
    pub architecture: String,
    /// Reliability score (0.0 to 1.0)
    pub reliability: f32,
    /// Whether node is battery-powered
    pub battery_powered: bool,
    /// Current load (0.0 to 1.0)
    pub current_load: f64,
    /// Running tasks count
    pub running_tasks: u32,
}

/// Node filter for listing
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeFilter {
    /// Filter by runtime support
    pub runtime: Option<String>,
    /// Filter by minimum memory
    pub memory_min: Option<u64>,
    /// Filter by region
    pub region: Option<String>,
    /// Filter by architecture
    pub architecture: Option<String>,
    /// Filter by status
    pub status: Option<String>,
}

/// Placement plan from coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementPlan {
    /// Target cores requested
    pub target_cores: u32,
    /// Target memory requested
    pub target_memory: u64,
    /// Selected nodes
    pub nodes: Vec<PlacedNode>,
    /// Total cores allocated
    pub total_cores: u32,
    /// Total memory allocated
    pub total_memory: u64,
    /// Estimated completion time in seconds
    pub estimated_completion_secs: u64,
    /// Number of excluded nodes
    pub excluded_count: u32,
    /// Reason for exclusions
    pub excluded_reason: String,
}

/// Node in placement plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacedNode {
    /// Node ID
    pub node_id: String,
    /// Cores allocated
    pub cores: u32,
    /// Memory allocated
    pub memory: u64,
    /// Tasks assigned
    pub tasks_assigned: u32,
}

/// Token balance and history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalance {
    /// Current balance
    pub balance: f64,
    /// Pending rewards
    pub pending: f64,
    /// Total earned
    pub total_earned: f64,
    /// Total spent
    pub total_spent: f64,
}

/// Token transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTransaction {
    /// Transaction ID
    pub id: String,
    /// Transaction type
    pub tx_type: String,
    /// Amount
    pub amount: f64,
    /// Timestamp
    pub timestamp: String,
    /// Description
    pub description: String,
}

/// CLI error type with error codes and suggestions
#[derive(Debug)]
pub enum CliError {
    /// Configuration error (E001-E007)
    Config(String),
    /// Client/network error (E100-E107)
    Client(String),
    /// IO error (E004/E005)
    Io(std::io::Error),
    /// Parse error (E002/E500)
    Parse(String),
    /// Job not found (E200)
    NotFound(String),
    /// Invalid argument (E500)
    InvalidArgument(String),
    /// Serialization error (E902)
    Serialization(String),
    /// Quota exceeded (E300)
    QuotaExceeded {
        resource: String,
        requested: f64,
        available: f64,
    },
    /// Policy violation (E400)
    PolicyViolation { policy_id: String, reason: String },
    /// Scheduling error (E200-E207)
    Scheduling(String),
}

impl CliError {
    /// Get the error code for this error
    pub fn error_code(&self) -> &'static str {
        match self {
            CliError::Config(_) => "E001",
            CliError::Client(msg) => {
                if msg.contains("timeout") || msg.contains("Timeout") {
                    "E101"
                } else if msg.contains("refused") || msg.contains("Refused") {
                    "E103"
                } else if msg.contains("Connection") {
                    "E100"
                } else {
                    "E106"
                }
            }
            CliError::Io(_) => "E004",
            CliError::Parse(_) => "E500",
            CliError::NotFound(_) => "E200",
            CliError::InvalidArgument(_) => "E500",
            CliError::Serialization(_) => "E902",
            CliError::QuotaExceeded { .. } => "E300",
            CliError::PolicyViolation { .. } => "E400",
            CliError::Scheduling(_) => "E200",
        }
    }

    /// Get suggestions for resolving this error
    pub fn suggestions(&self) -> Vec<&'static str> {
        match self {
            CliError::Config(_) => vec![
                "Run 'marabunta config init' to set up configuration",
                "Check ~/.marabunta/config.toml for syntax errors",
                "Set MARABUNTA_COORDIGLOBAL_ALLIANCE_T1R_URL environment variable",
            ],
            CliError::Client(msg) => {
                if msg.contains("timeout") || msg.contains("Timeout") {
                    vec![
                        "The coordinator may be slow or overloaded - retry after a delay",
                        "Check network connectivity to the coordinator",
                        "Consider increasing the timeout with --timeout flag",
                    ]
                } else if msg.contains("refused") || msg.contains("Refused") {
                    vec![
                        "Verify the coordinator is running",
                        "Check the coordinator URL in your config",
                        "Ensure no firewall is blocking the connection",
                    ]
                } else if msg.contains("Connection") {
                    vec![
                        "Check that the coordinator is running",
                        "Verify the coordinator URL in your config",
                        "Test network connectivity with 'marabunta info'",
                    ]
                } else {
                    vec![
                        "Check network connectivity",
                        "Verify coordinator URL is correct",
                    ]
                }
            }
            CliError::Io(err) => match err.kind() {
                std::io::ErrorKind::NotFound => vec![
                    "Check that the file path is correct",
                    "Ensure the file exists and is readable",
                ],
                std::io::ErrorKind::PermissionDenied => vec![
                    "Check file permissions",
                    "Try running with elevated privileges if appropriate",
                ],
                _ => vec!["Check file access and disk space"],
            },
            CliError::Parse(_) => vec![
                "Check input format and syntax",
                "See 'marabunta <command> --help' for expected format",
            ],
            CliError::NotFound(msg) => {
                if msg.contains("job") || msg.contains("Job") {
                    vec![
                        "Verify the job ID is correct",
                        "List recent jobs with 'marabunta status --recent'",
                        "The job may have been cancelled or expired",
                    ]
                } else if msg.contains("file") || msg.contains("File") {
                    vec![
                        "Check that the file path is correct",
                        "Ensure the file exists and is readable",
                    ]
                } else {
                    vec!["Verify the identifier is correct"]
                }
            }
            CliError::InvalidArgument(_) => vec![
                "Review the command syntax with 'marabunta <command> --help'",
                "Check argument format and valid values",
            ],
            CliError::Serialization(_) => vec![
                "This may indicate corrupted data or a version mismatch",
                "Try with --format json to see raw output",
            ],
            CliError::QuotaExceeded { resource, .. } => {
                let base = vec![
                    "Wait for quota to replenish",
                    "Request a quota increase from your administrator",
                ];
                let specific: Vec<&'static str> = match resource.as_str() {
                    "cpu" | "cpu_hours" => vec!["Reduce CPU requirements for your jobs"],
                    "memory" | "memory_gb_hours" => {
                        vec!["Reduce memory requirements for your jobs"]
                    }
                    "gpu" | "gpu_hours" => vec!["Consider using CPU-only jobs if possible"],
                    _ => vec![],
                };
                [base, specific].concat()
            }
            CliError::PolicyViolation { .. } => vec![
                "Review the policy requirements",
                "Modify your request to comply with the policy",
                "Contact the policy owner for exceptions",
            ],
            CliError::Scheduling(_) => vec![
                "Wait for nodes to become available",
                "Check cluster health with 'marabunta nodes'",
                "Relax placement constraints if possible",
            ],
        }
    }

    /// Format the error with colored output for CLI display
    pub fn format_cli(&self, use_color: bool) -> String {
        let mut output = String::new();

        // Error header
        if use_color {
            output.push_str("\x1b[1;31m");
        }
        output.push_str(&format!("error[{}]", self.error_code()));
        if use_color {
            output.push_str("\x1b[0m");
        }
        output.push_str(": ");

        // Error message
        if use_color {
            output.push_str("\x1b[1m");
        }
        output.push_str(&self.to_string());
        if use_color {
            output.push_str("\x1b[0m");
        }
        output.push('\n');

        // Add quota details
        if let CliError::QuotaExceeded {
            resource,
            requested,
            available,
        } = self
        {
            output.push('\n');
            if use_color {
                output.push_str("\x1b[36m");
            }
            output.push_str(&format!(
                "  Resource:  {}\n  Requested: {:.2}\n  Available: {:.2}\n  Shortfall: {:.2}\n",
                resource,
                requested,
                available,
                requested - available
            ));
            if use_color {
                output.push_str("\x1b[0m");
            }
        }

        // Suggestions
        let suggestions = self.suggestions();
        if !suggestions.is_empty() {
            output.push('\n');
            if use_color {
                output.push_str("\x1b[1;32m");
            }
            output.push_str("help");
            if use_color {
                output.push_str("\x1b[0m");
            }
            output.push_str(": ");

            for (i, suggestion) in suggestions.iter().enumerate() {
                if i > 0 {
                    output.push_str("\n      ");
                }
                output.push_str(suggestion);
            }
            output.push('\n');
        }

        // Documentation link
        if use_color {
            output.push_str("\x1b[2m");
        }
        output.push_str(&format!(
            "\nFor more info, see https://marabunta-compute.io/docs/errors/{}\n",
            self.error_code()
        ));
        if use_color {
            output.push_str("\x1b[0m");
        }

        output
    }

    /// Create a quota exceeded error
    pub fn quota_exceeded(resource: impl Into<String>, requested: f64, available: f64) -> Self {
        CliError::QuotaExceeded {
            resource: resource.into(),
            requested,
            available,
        }
    }

    /// Create a policy violation error
    pub fn policy_violation(policy_id: impl Into<String>, reason: impl Into<String>) -> Self {
        CliError::PolicyViolation {
            policy_id: policy_id.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Config(msg) => write!(f, "Configuration error: {}", msg),
            CliError::Client(msg) => write!(f, "Client error: {}", msg),
            CliError::Io(err) => write!(f, "IO error: {}", err),
            CliError::Parse(msg) => write!(f, "Parse error: {}", msg),
            CliError::NotFound(msg) => write!(f, "Not found: {}", msg),
            CliError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            CliError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            CliError::QuotaExceeded {
                resource,
                requested,
                available,
            } => {
                write!(
                    f,
                    "Quota exceeded for {}: requested {:.2}, available {:.2}",
                    resource, requested, available
                )
            }
            CliError::PolicyViolation { policy_id, reason } => {
                write!(f, "Policy violation ({}): {}", policy_id, reason)
            }
            CliError::Scheduling(msg) => write!(f, "Scheduling error: {}", msg),
        }
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        CliError::Io(err)
    }
}

impl From<serde_json::Error> for CliError {
    fn from(err: serde_json::Error) -> Self {
        CliError::Serialization(err.to_string())
    }
}

impl From<toml::de::Error> for CliError {
    fn from(err: toml::de::Error) -> Self {
        CliError::Parse(err.to_string())
    }
}

impl From<toml::ser::Error> for CliError {
    fn from(err: toml::ser::Error) -> Self {
        CliError::Serialization(err.to_string())
    }
}

/// Parse a size string (e.g., "2GB", "512MB") into bytes
pub fn parse_size(s: &Option<String>) -> Result<Option<u64>, CliError> {
    let s = match s {
        Some(s) => s,
        None => return Ok(None),
    };

    let s = s.trim().to_uppercase();
    let (num_str, multiplier) = if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024u64)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024 * 1024u64)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else if s.ends_with('B') {
        (&s[..s.len() - 1], 1u64)
    } else {
        // Assume MB if no suffix
        (s.as_str(), 1024 * 1024u64)
    };

    let num: f64 = num_str
        .trim()
        .parse()
        .map_err(|_| CliError::Parse(format!("Invalid size: {}", s)))?;

    Ok(Some((num * multiplier as f64) as u64))
}

/// Parse a duration string (e.g., "5m", "1h30m") into Duration
pub fn parse_duration(s: &str) -> Result<Duration, CliError> {
    let s = s.trim().to_lowercase();
    let mut total_secs = 0u64;
    let mut current_num = String::new();

    for c in s.chars() {
        if c.is_ascii_digit() || c == '.' {
            current_num.push(c);
        } else {
            let num: f64 = current_num
                .parse()
                .map_err(|_| CliError::Parse(format!("Invalid duration: {}", s)))?;
            current_num.clear();

            let secs = match c {
                's' => num,
                'm' => num * 60.0,
                'h' => num * 3600.0,
                'd' => num * 86400.0,
                _ => return Err(CliError::Parse(format!("Invalid duration unit: {}", c))),
            };
            total_secs += secs as u64;
        }
    }

    // Handle case with no unit (assume seconds)
    if !current_num.is_empty() {
        let num: f64 = current_num
            .parse()
            .map_err(|_| CliError::Parse(format!("Invalid duration: {}", s)))?;
        total_secs += num as u64;
    }

    Ok(Duration::from_secs(total_secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(
            parse_size(&Some("2GB".to_string())).unwrap(),
            Some(2 * 1024 * 1024 * 1024)
        );
        assert_eq!(
            parse_size(&Some("512MB".to_string())).unwrap(),
            Some(512 * 1024 * 1024)
        );
        assert_eq!(
            parse_size(&Some("1024KB".to_string())).unwrap(),
            Some(1024 * 1024)
        );
        assert_eq!(parse_size(&None).unwrap(), None);
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
    }
}
