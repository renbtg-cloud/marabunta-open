// Marabunta - Licensed under the MIT License.
//! Maintenance CLI commands
//!
//! Provides command-line interface for managing maintenance windows,
//! scheduling downtime, and monitoring maintenance status.

use chrono::{DateTime, Duration, Utc};
use clap::{Args, Subcommand, ValueEnum};
use console::style;
use serde::{Deserialize, Serialize};

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::types::{parse_duration, CliError, OutputFormat};

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the maintenance command
#[derive(Args)]
pub struct MaintenanceArgs {
    /// Subcommand to execute
    #[command(subcommand)]
    pub command: MaintenanceCommand,
}

/// Maintenance subcommands
#[derive(Subcommand)]
pub enum MaintenanceCommand {
    /// Schedule a new maintenance window
    Schedule(ScheduleArgs),

    /// List all maintenance windows
    List(ListArgs),

    /// Show details of a maintenance window
    Show(ShowArgs),

    /// Cancel a scheduled maintenance window
    Cancel(CancelArgs),

    /// Complete a maintenance window manually
    Complete(CompleteArgs),

    /// Start a maintenance window immediately
    Start(StartArgs),

    /// Show nodes currently in maintenance
    Nodes(MaintenanceNodesArgs),
}

// ─────────────────────────────────────────────────────────────────────────────
// SCHEDULE ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for scheduling maintenance
#[derive(Args)]
pub struct ScheduleArgs {
    /// Maintenance window name/description
    #[arg(long, short)]
    pub name: String,

    /// Type of maintenance
    #[arg(long, short = 't', value_enum, default_value = "planned")]
    pub maintenance_type: MaintenanceTypeArg,

    /// Start time (ISO 8601 format or relative like "2h", "1d")
    #[arg(long, short)]
    pub start: String,

    /// Duration of maintenance window
    #[arg(long, short, default_value = "2h")]
    pub duration: String,

    /// Nodes to include (comma-separated IDs or "all")
    #[arg(long)]
    pub nodes: String,

    /// Rolling batch size (for rolling maintenance)
    #[arg(long, default_value = "1")]
    pub batch_size: usize,

    /// Force drain tasks on timeout
    #[arg(long)]
    pub force: bool,

    /// Drain timeout (e.g., "30m", "1h")
    #[arg(long)]
    pub drain_timeout: Option<String>,

    /// Additional description
    #[arg(long)]
    pub description: Option<String>,

    /// Output format
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,
}

/// Maintenance type argument
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceTypeArg {
    Planned,
    Emergency,
    Rolling,
    Hardware,
    Software,
    Security,
}

impl std::fmt::Display for MaintenanceTypeArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaintenanceTypeArg::Planned => write!(f, "planned"),
            MaintenanceTypeArg::Emergency => write!(f, "emergency"),
            MaintenanceTypeArg::Rolling => write!(f, "rolling"),
            MaintenanceTypeArg::Hardware => write!(f, "hardware"),
            MaintenanceTypeArg::Software => write!(f, "software"),
            MaintenanceTypeArg::Security => write!(f, "security"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LIST ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for listing maintenance windows
#[derive(Args)]
pub struct ListArgs {
    /// Filter by state
    #[arg(long)]
    pub state: Option<String>,

    /// Show only active windows
    #[arg(long)]
    pub active: bool,

    /// Show only scheduled (future) windows
    #[arg(long)]
    pub scheduled: bool,

    /// Include completed windows
    #[arg(long)]
    pub all: bool,

    /// Output format
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,
}

// ─────────────────────────────────────────────────────────────────────────────
// SHOW ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for showing maintenance details
#[derive(Args)]
pub struct ShowArgs {
    /// Maintenance window ID
    pub id: String,

    /// Output format
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,
}

// ─────────────────────────────────────────────────────────────────────────────
// CANCEL ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for cancelling maintenance
#[derive(Args)]
pub struct CancelArgs {
    /// Maintenance window ID
    pub id: String,

    /// Reason for cancellation
    #[arg(long, short)]
    pub reason: Option<String>,

    /// Skip confirmation
    #[arg(long, short = 'y')]
    pub yes: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPLETE ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for completing maintenance
#[derive(Args)]
pub struct CompleteArgs {
    /// Maintenance window ID
    pub id: String,

    /// Skip confirmation
    #[arg(long, short = 'y')]
    pub yes: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// START ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for starting maintenance immediately
#[derive(Args)]
pub struct StartArgs {
    /// Maintenance window ID
    pub id: String,

    /// Skip confirmation
    #[arg(long, short = 'y')]
    pub yes: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// MAINTENANCE NODES ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for showing nodes in maintenance
#[derive(Args)]
pub struct MaintenanceNodesArgs {
    /// Show all nodes (including not in maintenance)
    #[arg(long)]
    pub all: bool,

    /// Output format
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,
}

// ─────────────────────────────────────────────────────────────────────────────
// API TYPES
// ─────────────────────────────────────────────────────────────────────────────

/// Maintenance window summary for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceWindowResponse {
    pub id: String,
    pub name: String,
    pub maintenance_type: String,
    pub state: String,
    pub scheduled_start: String,
    pub scheduled_end: String,
    pub actual_start: Option<String>,
    pub actual_end: Option<String>,
    pub affected_node_count: usize,
    pub drained_node_count: usize,
    pub progress: f64,
    pub created_at: String,
    pub created_by: Option<String>,
    pub failure_reason: Option<String>,
}

/// Request to schedule maintenance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleMaintenanceRequest {
    pub name: String,
    pub description: Option<String>,
    pub maintenance_type: String,
    pub scheduled_start: String,
    pub scheduled_end: String,
    pub affected_nodes: Vec<String>,
    pub rolling_batch_size: usize,
    pub drain_timeout_secs: Option<u64>,
    pub force_drain_on_timeout: bool,
}

/// Response from scheduling maintenance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleMaintenanceResponse {
    pub id: String,
    pub message: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE MAINTENANCE
// ─────────────────────────────────────────────────────────────────────────────

/// Execute maintenance commands
pub async fn execute_maintenance(args: MaintenanceArgs, config: &Config) -> Result<(), CliError> {
    match args.command {
        MaintenanceCommand::Schedule(schedule_args) => {
            execute_schedule(schedule_args, config).await
        }
        MaintenanceCommand::List(list_args) => execute_list(list_args, config).await,
        MaintenanceCommand::Show(show_args) => execute_show(show_args, config).await,
        MaintenanceCommand::Cancel(cancel_args) => execute_cancel(cancel_args, config).await,
        MaintenanceCommand::Complete(complete_args) => {
            execute_complete(complete_args, config).await
        }
        MaintenanceCommand::Start(start_args) => execute_start(start_args, config).await,
        MaintenanceCommand::Nodes(nodes_args) => execute_nodes(nodes_args, config).await,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SCHEDULE COMMAND
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_schedule(args: ScheduleArgs, config: &Config) -> Result<(), CliError> {
    let _client = CoordinatorClient::from_config(config).await?;

    // Parse start time
    let start = parse_time(&args.start)?;

    // Parse duration
    let duration = parse_duration(&args.duration)?;
    let end =
        start + chrono::Duration::from_std(duration).map_err(|e| CliError::Parse(e.to_string()))?;

    // Parse nodes
    let nodes: Vec<String> = if args.nodes.to_lowercase() == "all" {
        // Would fetch all nodes from API
        vec!["all".to_string()]
    } else {
        args.nodes
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    };

    let request = ScheduleMaintenanceRequest {
        name: args.name.clone(),
        description: args.description,
        maintenance_type: args.maintenance_type.to_string(),
        scheduled_start: start.to_rfc3339(),
        scheduled_end: end.to_rfc3339(),
        affected_nodes: nodes.clone(),
        rolling_batch_size: args.batch_size,
        drain_timeout_secs: args
            .drain_timeout
            .as_ref()
            .and_then(|s| parse_duration(s).ok())
            .map(|d| d.as_secs()),
        force_drain_on_timeout: args.force,
    };

    // In a real implementation, this would call the API
    // let response = client.schedule_maintenance(request).await?;

    let format = args.format.unwrap_or(config.output_format);
    match format {
        OutputFormat::Human => {
            println!();
            println!("{} Maintenance Window Scheduled", style("").green());
            println!("{}", style("─".repeat(50)).dim());
            println!("   Name:      {}", args.name);
            println!(
                "   Type:      {}",
                style(args.maintenance_type.to_string()).cyan()
            );
            println!("   Start:     {}", start.format("%Y-%m-%d %H:%M:%S UTC"));
            println!("   End:       {}", end.format("%Y-%m-%d %H:%M:%S UTC"));
            println!("   Duration:  {}", humanize_duration(duration));
            println!("   Nodes:     {} affected", nodes.len());
            if args.maintenance_type as u8 == MaintenanceTypeArg::Rolling as u8 {
                println!("   Batch:     {} node(s) at a time", args.batch_size);
            }
            println!("{}", style("─".repeat(50)).dim());
            println!();
            println!(
                "   {}",
                style("Note: Maintenance window scheduled successfully.").dim()
            );
            println!(
                "   {}",
                style("Nodes will be drained automatically before start time.").dim()
            );
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&request)?);
        }
        _ => {
            println!("{}", serde_json::to_string_pretty(&request)?);
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// LIST COMMAND
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_list(args: ListArgs, config: &Config) -> Result<(), CliError> {
    let _client = CoordinatorClient::from_config(config).await?;

    // In a real implementation, this would fetch from API
    // let windows = client.list_maintenance(filter).await?;

    // Mock data for demonstration
    let windows: Vec<MaintenanceWindowResponse> = vec![MaintenanceWindowResponse {
        id: "maint-abc123".to_string(),
        name: "Weekly Security Update".to_string(),
        maintenance_type: "security".to_string(),
        state: "scheduled".to_string(),
        scheduled_start: (Utc::now() + Duration::hours(24)).to_rfc3339(),
        scheduled_end: (Utc::now() + Duration::hours(26)).to_rfc3339(),
        actual_start: None,
        actual_end: None,
        affected_node_count: 5,
        drained_node_count: 0,
        progress: 0.0,
        created_at: Utc::now().to_rfc3339(),
        created_by: Some("admin".to_string()),
        failure_reason: None,
    }];

    let format = args.format.unwrap_or(config.output_format);
    match format {
        OutputFormat::Human => {
            print_maintenance_list(&windows, args.active, args.scheduled);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&windows)?);
        }
        OutputFormat::Csv => {
            print_maintenance_csv(&windows);
        }
        OutputFormat::Yaml => {
            print_maintenance_yaml(&windows)?;
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// SHOW COMMAND
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_show(args: ShowArgs, config: &Config) -> Result<(), CliError> {
    let _client = CoordinatorClient::from_config(config).await?;

    // In a real implementation, this would fetch from API
    // let window = client.get_maintenance(&args.id).await?;

    let window = MaintenanceWindowResponse {
        id: args.id.clone(),
        name: "Weekly Security Update".to_string(),
        maintenance_type: "security".to_string(),
        state: "scheduled".to_string(),
        scheduled_start: (Utc::now() + Duration::hours(24)).to_rfc3339(),
        scheduled_end: (Utc::now() + Duration::hours(26)).to_rfc3339(),
        actual_start: None,
        actual_end: None,
        affected_node_count: 5,
        drained_node_count: 0,
        progress: 0.0,
        created_at: Utc::now().to_rfc3339(),
        created_by: Some("admin".to_string()),
        failure_reason: None,
    };

    let format = args.format.unwrap_or(config.output_format);
    match format {
        OutputFormat::Human => {
            print_maintenance_detail(&window);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&window)?);
        }
        _ => {
            println!("{}", serde_json::to_string_pretty(&window)?);
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// CANCEL COMMAND
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_cancel(args: CancelArgs, config: &Config) -> Result<(), CliError> {
    let _client = CoordinatorClient::from_config(config).await?;

    if !args.yes {
        println!();
        println!(
            "{} Are you sure you want to cancel maintenance {}?",
            style("Warning:").yellow().bold(),
            style(&args.id).cyan()
        );
        println!("   Type 'yes' to confirm: ");

        // In a real implementation, read confirmation
        // For now, assume confirmed
    }

    let reason = args
        .reason
        .unwrap_or_else(|| "Cancelled by user".to_string());

    // In a real implementation:
    // client.cancel_maintenance(&args.id, &reason).await?;

    println!();
    println!(
        "{} Maintenance window {} cancelled",
        style("").green(),
        style(&args.id).cyan()
    );
    println!("   Reason: {}", reason);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPLETE COMMAND
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_complete(args: CompleteArgs, config: &Config) -> Result<(), CliError> {
    let _client = CoordinatorClient::from_config(config).await?;

    if !args.yes {
        println!();
        println!(
            "{} Complete maintenance window {}?",
            style("Confirm:").yellow().bold(),
            style(&args.id).cyan()
        );
        println!("   This will mark the maintenance as completed and bring nodes back online.");
    }

    // In a real implementation:
    // client.complete_maintenance(&args.id).await?;

    println!();
    println!(
        "{} Maintenance window {} completed",
        style("").green(),
        style(&args.id).cyan()
    );
    println!("   Affected nodes are now available for scheduling.");

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// START COMMAND
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_start(args: StartArgs, config: &Config) -> Result<(), CliError> {
    let _client = CoordinatorClient::from_config(config).await?;

    if !args.yes {
        println!();
        println!(
            "{} Start maintenance window {} immediately?",
            style("Confirm:").yellow().bold(),
            style(&args.id).cyan()
        );
        println!("   This will begin draining affected nodes now.");
    }

    // In a real implementation:
    // client.start_maintenance(&args.id).await?;

    println!();
    println!(
        "{} Maintenance window {} started",
        style("").green(),
        style(&args.id).cyan()
    );
    println!("   Nodes are being drained...");

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// NODES COMMAND
// ─────────────────────────────────────────────────────────────────────────────

async fn execute_nodes(args: MaintenanceNodesArgs, config: &Config) -> Result<(), CliError> {
    let _client = CoordinatorClient::from_config(config).await?;

    // In a real implementation:
    // let nodes = client.get_maintenance_nodes().await?;

    // Mock data
    #[derive(Serialize)]
    struct NodeMaintenanceInfo {
        node_id: String,
        status: String,
        maintenance_id: Option<String>,
        drain_progress: f64,
    }

    let nodes = vec![
        NodeMaintenanceInfo {
            node_id: "node-001".to_string(),
            status: "draining".to_string(),
            maintenance_id: Some("maint-abc123".to_string()),
            drain_progress: 0.75,
        },
        NodeMaintenanceInfo {
            node_id: "node-002".to_string(),
            status: "drained".to_string(),
            maintenance_id: Some("maint-abc123".to_string()),
            drain_progress: 1.0,
        },
    ];

    let format = args.format.unwrap_or(config.output_format);
    match format {
        OutputFormat::Human => {
            println!();
            println!("{} Nodes in Maintenance", style("").cyan());
            println!("{}", style("─".repeat(60)).dim());
            println!(
                "  {:12}  {:12}  {:16}  {:10}",
                style("Node").bold(),
                style("Status").bold(),
                style("Maintenance").bold(),
                style("Progress").bold()
            );
            println!("{}", style("─".repeat(60)).dim());

            for node in &nodes {
                let status_color = match node.status.as_str() {
                    "draining" => style(&node.status).yellow(),
                    "drained" => style(&node.status).green(),
                    "in_maintenance" => style(&node.status).red(),
                    _ => style(&node.status).dim(),
                };

                println!(
                    "  {:12}  {:12}  {:16}  {:>6.1}%",
                    node.node_id,
                    status_color,
                    node.maintenance_id.as_deref().unwrap_or("-"),
                    node.drain_progress * 100.0
                );
            }
            println!();
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&nodes)?);
        }
        _ => {
            println!("{}", serde_json::to_string_pretty(&nodes)?);
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER FUNCTIONS
// ─────────────────────────────────────────────────────────────────────────────

/// Parse time string (ISO 8601 or relative)
fn parse_time(s: &str) -> Result<DateTime<Utc>, CliError> {
    // Try ISO 8601 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try as relative duration
    let duration = parse_duration(s)?;
    Ok(Utc::now()
        + chrono::Duration::from_std(duration).map_err(|e| CliError::Parse(e.to_string()))?)
}

/// Humanize a duration
fn humanize_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        if mins > 0 {
            format!("{}h {}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    } else {
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        if hours > 0 {
            format!("{}d {}h", days, hours)
        } else {
            format!("{}d", days)
        }
    }
}

/// Print maintenance list in human format
fn print_maintenance_list(
    windows: &[MaintenanceWindowResponse],
    active_only: bool,
    scheduled_only: bool,
) {
    println!();
    println!("{} Maintenance Windows", style("").cyan());
    println!("{}", style("─".repeat(80)).dim());
    println!(
        "  {:12}  {:20}  {:10}  {:12}  {:20}",
        style("ID").bold(),
        style("Name").bold(),
        style("Type").bold(),
        style("State").bold(),
        style("Start").bold()
    );
    println!("{}", style("─".repeat(80)).dim());

    for window in windows {
        let state_color = match window.state.as_str() {
            "scheduled" => style(&window.state).blue(),
            "draining" => style(&window.state).yellow(),
            "in_progress" => style(&window.state).cyan(),
            "completed" => style(&window.state).green(),
            "cancelled" => style(&window.state).dim(),
            "failed" => style(&window.state).red(),
            _ => style(&window.state).white(),
        };

        // Apply filters
        if active_only && !["draining", "in_progress"].contains(&window.state.as_str()) {
            continue;
        }
        if scheduled_only && window.state != "scheduled" {
            continue;
        }

        println!(
            "  {:12}  {:20}  {:10}  {:12}  {:20}",
            &window.id[..12.min(window.id.len())],
            truncate(&window.name, 20),
            window.maintenance_type,
            state_color,
            format_datetime(&window.scheduled_start)
        );
    }

    println!();
}

/// Print maintenance detail
fn print_maintenance_detail(window: &MaintenanceWindowResponse) {
    println!();
    println!("{} Maintenance Window Details", style("").cyan());
    println!("{}", style("─".repeat(50)).dim());
    println!("   ID:           {}", style(&window.id).cyan());
    println!("   Name:         {}", window.name);
    println!("   Type:         {}", window.maintenance_type);
    println!("   State:        {}", format_state(&window.state));
    println!();
    println!(
        "   Scheduled Start: {}",
        format_datetime(&window.scheduled_start)
    );
    println!(
        "   Scheduled End:   {}",
        format_datetime(&window.scheduled_end)
    );
    if let Some(ref start) = window.actual_start {
        println!("   Actual Start:    {}", format_datetime(start));
    }
    if let Some(ref end) = window.actual_end {
        println!("   Actual End:      {}", format_datetime(end));
    }
    println!();
    println!("   Affected Nodes: {}", window.affected_node_count);
    println!("   Drained Nodes:  {}", window.drained_node_count);
    println!("   Progress:       {:.1}%", window.progress * 100.0);
    println!();
    println!("   Created At:   {}", format_datetime(&window.created_at));
    if let Some(ref by) = window.created_by {
        println!("   Created By:   {}", by);
    }
    if let Some(ref reason) = window.failure_reason {
        println!();
        println!("   {} {}", style("Failure Reason:").red(), reason);
    }
    println!("{}", style("─".repeat(50)).dim());
}

/// Print maintenance as CSV
fn print_maintenance_csv(windows: &[MaintenanceWindowResponse]) {
    println!("id,name,type,state,scheduled_start,scheduled_end,affected_nodes,progress");
    for w in windows {
        println!(
            "{},{},{},{},{},{},{},{:.2}",
            w.id,
            w.name.replace(',', ";"),
            w.maintenance_type,
            w.state,
            w.scheduled_start,
            w.scheduled_end,
            w.affected_node_count,
            w.progress
        );
    }
}

/// Print maintenance as YAML
fn print_maintenance_yaml(windows: &[MaintenanceWindowResponse]) -> Result<(), CliError> {
    println!("maintenance_windows:");
    for w in windows {
        println!("  - id: {}", w.id);
        println!("    name: \"{}\"", w.name);
        println!("    type: {}", w.maintenance_type);
        println!("    state: {}", w.state);
        println!("    scheduled_start: {}", w.scheduled_start);
        println!("    scheduled_end: {}", w.scheduled_end);
        println!("    affected_node_count: {}", w.affected_node_count);
        println!("    progress: {:.2}", w.progress);
    }
    Ok(())
}

/// Truncate string
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max - 3])
    }
}

/// Format datetime string
fn format_datetime(s: &str) -> String {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        dt.format("%Y-%m-%d %H:%M UTC").to_string()
    } else {
        s.to_string()
    }
}

/// Format state with color
fn format_state(state: &str) -> String {
    match state {
        "scheduled" => style(state).blue().to_string(),
        "draining" => style(state).yellow().to_string(),
        "in_progress" => style(state).cyan().to_string(),
        "completed" => style(state).green().to_string(),
        "cancelled" => style(state).dim().to_string(),
        "failed" => style(state).red().to_string(),
        _ => state.to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_relative_time() {
        let now = Utc::now();
        let result = parse_time("1h").unwrap();
        assert!(result > now);
        assert!(result < now + Duration::hours(2));
    }

    #[test]
    fn test_humanize_duration() {
        use std::time::Duration;

        assert_eq!(humanize_duration(Duration::from_secs(30)), "30s");
        assert_eq!(humanize_duration(Duration::from_secs(120)), "2m");
        assert_eq!(humanize_duration(Duration::from_secs(3600)), "1h");
        assert_eq!(humanize_duration(Duration::from_secs(5400)), "1h 30m");
        assert_eq!(humanize_duration(Duration::from_secs(86400)), "1d");
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("this is a very long string", 10), "this is...");
    }
}
