// Marabunta - Licensed under the MIT License.
//! Workflow CLI commands
//!
//! Commands for submitting, monitoring, and managing workflows.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use console::style;
use tokio::time::sleep;

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::Spinner;
use crate::cli::types::CliError;

// ============================================================================
// CLI Arguments
// ============================================================================

/// Workflow management commands
#[derive(Args)]
pub struct WorkflowArgs {
    #[command(subcommand)]
    pub command: WorkflowCommand,
}

/// Workflow subcommands
#[derive(Subcommand)]
pub enum WorkflowCommand {
    /// Submit a workflow definition
    #[command(visible_alias = "sub")]
    Submit(WorkflowSubmitArgs),

    /// Get workflow execution status
    #[command(visible_alias = "stat")]
    Status(WorkflowStatusArgs),

    /// List workflows
    #[command(visible_alias = "ls")]
    List(WorkflowListArgs),

    /// Run a workflow
    Run(WorkflowRunArgs),

    /// Show workflow details
    Show(WorkflowShowArgs),

    /// Delete a workflow
    Delete(WorkflowDeleteArgs),

    /// Cancel a running workflow
    Cancel(WorkflowCancelArgs),

    /// List active runs
    Runs(WorkflowRunsArgs),
}

/// Arguments for workflow submit
#[derive(Args)]
pub struct WorkflowSubmitArgs {
    /// Path to the workflow YAML file
    pub file: PathBuf,

    /// Override workflow name
    #[arg(long)]
    pub name: Option<String>,

    /// Add tags (comma-separated)
    #[arg(long)]
    pub tags: Option<String>,

    /// Validate only, don't submit
    #[arg(long)]
    pub dry_run: bool,
}

/// Arguments for workflow status
#[derive(Args)]
pub struct WorkflowStatusArgs {
    /// Workflow run ID
    pub run_id: String,

    /// Watch for updates
    #[arg(long, short)]
    pub watch: bool,

    /// Output format (human, json)
    #[arg(long, default_value = "human")]
    pub format: String,
}

/// Arguments for workflow list
#[derive(Args)]
pub struct WorkflowListArgs {
    /// Filter by name
    #[arg(long)]
    pub name: Option<String>,

    /// Filter by tag
    #[arg(long)]
    pub tag: Option<String>,

    /// Maximum results
    #[arg(long, default_value = "20")]
    pub limit: usize,

    /// Output format (human, json)
    #[arg(long, default_value = "human")]
    pub format: String,
}

/// Arguments for workflow run
#[derive(Args)]
pub struct WorkflowRunArgs {
    /// Workflow ID or name
    pub workflow: String,

    /// Variable overrides (key=value format)
    #[arg(long, value_parser = parse_variable)]
    pub var: Vec<(String, String)>,

    /// Wait for completion
    #[arg(long)]
    pub wait: bool,

    /// Watch progress
    #[arg(long, short)]
    pub watch: bool,
}

/// Arguments for workflow show
#[derive(Args)]
pub struct WorkflowShowArgs {
    /// Workflow ID
    pub id: String,

    /// Output format (human, json, yaml)
    #[arg(long, default_value = "human")]
    pub format: String,
}

/// Arguments for workflow delete
#[derive(Args)]
pub struct WorkflowDeleteArgs {
    /// Workflow ID
    pub id: String,

    /// Force delete without confirmation
    #[arg(long, short)]
    pub force: bool,
}

/// Arguments for workflow cancel
#[derive(Args)]
pub struct WorkflowCancelArgs {
    /// Run ID to cancel
    pub run_id: String,
}

/// Arguments for workflow runs
#[derive(Args)]
pub struct WorkflowRunsArgs {
    /// Filter by workflow ID
    #[arg(long)]
    pub workflow: Option<String>,

    /// Filter by status
    #[arg(long)]
    pub status: Option<String>,

    /// Maximum results
    #[arg(long, default_value = "20")]
    pub limit: usize,

    /// Output format (human, json)
    #[arg(long, default_value = "human")]
    pub format: String,
}

// ============================================================================
// Command Execution
// ============================================================================

/// Execute workflow commands
pub async fn execute_workflow(args: WorkflowArgs, config: &Config) -> Result<(), CliError> {
    match args.command {
        WorkflowCommand::Submit(submit_args) => execute_submit(submit_args, config).await,
        WorkflowCommand::Status(status_args) => execute_status(status_args, config).await,
        WorkflowCommand::List(list_args) => execute_list(list_args, config).await,
        WorkflowCommand::Run(run_args) => execute_run(run_args, config).await,
        WorkflowCommand::Show(show_args) => execute_show(show_args, config).await,
        WorkflowCommand::Delete(delete_args) => execute_delete(delete_args, config).await,
        WorkflowCommand::Cancel(cancel_args) => execute_cancel(cancel_args, config).await,
        WorkflowCommand::Runs(runs_args) => execute_runs(runs_args, config).await,
    }
}

/// Submit a workflow definition
async fn execute_submit(args: WorkflowSubmitArgs, config: &Config) -> Result<(), CliError> {
    // Read workflow file
    if !args.file.exists() {
        return Err(CliError::NotFound(format!(
            "Workflow file not found: {}",
            args.file.display()
        )));
    }

    let content = std::fs::read_to_string(&args.file)?;

    // Parse workflow (try JSON first, then YAML-like structure)
    let mut workflow: serde_json::Value = if content.trim().starts_with('{') {
        serde_json::from_str(&content)?
    } else {
        // For YAML, we'd need serde_yaml - for now, indicate it's not supported
        return Err(CliError::Parse(
            "YAML parsing not yet supported. Please use JSON format.".to_string(),
        ));
    };

    // Override name if provided
    if let Some(name) = args.name {
        workflow["name"] = serde_json::Value::String(name);
    }

    // Add tags if provided
    if let Some(tags) = args.tags {
        let tag_list: Vec<_> = tags.split(',').map(|t| t.trim().to_string()).collect();
        workflow["tags"] = serde_json::json!(tag_list);
    }

    if args.dry_run {
        println!("\n{} Dry Run - Workflow Definition:", style("").yellow());
        println!("{}", style("-".repeat(60)).dim());
        println!("{}", serde_json::to_string_pretty(&workflow)?);
        println!("{}", style("-".repeat(60)).dim());
        return Ok(());
    }

    // Connect to coordinator
    let spinner = Spinner::new("Connecting to coordinator...");
    let client = CoordinatorClient::from_config(config).await.map_err(|e| {
        spinner.finish();
        CliError::Client(format!("Failed to connect: {}", e))
    })?;
    spinner.finish();

    // Submit workflow
    let spinner = Spinner::new("Submitting workflow...");
    let response = client.create_workflow(workflow).await.map_err(|e| {
        spinner.finish();
        CliError::Client(format!("Failed to submit workflow: {}", e))
    })?;
    spinner.finish();

    println!();
    println!("{} Workflow submitted!", style("Marabunta").yellow().bold());
    println!(
        "   ID:   {}",
        style(
            response
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
        )
        .cyan()
    );
    println!(
        "   Name: {}",
        response
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
    );
    println!();
    println!(
        "To run:  {} {}",
        style("marabunta workflow run").cyan(),
        response
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("<id>")
    );

    Ok(())
}

/// Get workflow run status
async fn execute_status(args: WorkflowStatusArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config)
        .await
        .map_err(|e| CliError::Client(format!("Failed to connect: {}", e)))?;

    if args.watch {
        // Watch mode - poll until complete
        loop {
            let status = client
                .get_run_status(&args.run_id)
                .await
                .map_err(|e| CliError::Client(format!("Failed to get status: {}", e)))?;

            // Clear screen and redraw
            print!("\x1B[2J\x1B[1;1H");
            print_run_status(&status, &args.format);

            // Check if terminal
            if is_terminal_status(&status) {
                break;
            }

            sleep(Duration::from_secs(2)).await;
        }
    } else {
        let status = client
            .get_run_status(&args.run_id)
            .await
            .map_err(|e| CliError::Client(format!("Failed to get status: {}", e)))?;

        print_run_status(&status, &args.format);
    }

    Ok(())
}

/// List workflows
async fn execute_list(args: WorkflowListArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config)
        .await
        .map_err(|e| CliError::Client(format!("Failed to connect: {}", e)))?;

    let workflows = client
        .list_workflows(args.name.as_deref(), args.tag.as_deref(), args.limit)
        .await
        .map_err(|e| CliError::Client(format!("Failed to list workflows: {}", e)))?;

    if args.format == "json" {
        println!("{}", serde_json::to_string_pretty(&workflows)?);
        return Ok(());
    }

    // Human-readable format
    println!();
    println!("{} Workflows", style("Marabunta").yellow().bold());
    println!("{}", style("-".repeat(80)).dim());

    let workflows_array = workflows.get("workflows").and_then(|v| v.as_array());

    if let Some(workflows) = workflows_array {
        if workflows.is_empty() {
            println!("  No workflows found.");
        } else {
            for wf in workflows {
                let id = wf.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                let name = wf.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                let version = wf.get("version").and_then(|v| v.as_str()).unwrap_or("?");
                let steps = wf.get("step_count").and_then(|v| v.as_u64()).unwrap_or(0);

                println!(
                    "  {} {} (v{}) - {} steps",
                    style(id).cyan(),
                    style(name).bold(),
                    version,
                    steps
                );
            }
        }
    } else {
        println!("  No workflows found.");
    }

    println!("{}", style("-".repeat(80)).dim());

    Ok(())
}

/// Run a workflow
async fn execute_run(args: WorkflowRunArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config)
        .await
        .map_err(|e| CliError::Client(format!("Failed to connect: {}", e)))?;

    // Build variables
    let variables: HashMap<String, serde_json::Value> = args
        .var
        .into_iter()
        .map(|(k, v)| {
            // Try to parse as JSON, fall back to string
            let value = serde_json::from_str(&v).unwrap_or(serde_json::Value::String(v));
            (k, value)
        })
        .collect();

    let spinner = Spinner::new("Starting workflow...");
    let response = client
        .run_workflow(
            &args.workflow,
            if variables.is_empty() {
                None
            } else {
                Some(variables)
            },
        )
        .await
        .map_err(|e| {
            spinner.finish();
            CliError::Client(format!("Failed to run workflow: {}", e))
        })?;
    spinner.finish();

    let run_id = response
        .get("run_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    println!();
    println!("{} Workflow started!", style("Marabunta").yellow().bold());
    println!("   Run ID: {}", style(run_id).cyan());

    if args.wait || args.watch {
        println!();
        println!("Monitoring progress...");

        loop {
            let status = client
                .get_run_status(run_id)
                .await
                .map_err(|e| CliError::Client(format!("Failed to get status: {}", e)))?;

            if args.watch {
                print!("\x1B[2J\x1B[1;1H");
                print_run_status(&status, "human");
            }

            if is_terminal_status(&status) {
                if !args.watch {
                    print_run_status(&status, "human");
                }
                break;
            }

            sleep(Duration::from_secs(2)).await;
        }
    } else {
        println!();
        println!(
            "Check status: {} {}",
            style("marabunta workflow status").cyan(),
            run_id
        );
    }

    Ok(())
}

/// Show workflow details
async fn execute_show(args: WorkflowShowArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config)
        .await
        .map_err(|e| CliError::Client(format!("Failed to connect: {}", e)))?;

    let workflow = client
        .get_workflow(&args.id)
        .await
        .map_err(|e| CliError::Client(format!("Failed to get workflow: {}", e)))?;

    match args.format.as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&workflow)?);
        }
        "yaml" => {
            // For YAML output, we'd need serde_yaml
            println!("{}", serde_json::to_string_pretty(&workflow)?);
        }
        _ => {
            // Human-readable
            println!();
            println!("{} Workflow Details", style("Marabunta").yellow().bold());
            println!("{}", style("-".repeat(60)).dim());

            let id = workflow.get("id").and_then(|v| v.as_str()).unwrap_or("?");
            let name = workflow.get("name").and_then(|v| v.as_str()).unwrap_or("?");
            let version = workflow
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let desc = workflow.get("description").and_then(|v| v.as_str());

            println!("  ID:          {}", style(id).cyan());
            println!("  Name:        {}", style(name).bold());
            println!("  Version:     {}", version);
            if let Some(d) = desc {
                println!("  Description: {}", d);
            }

            // Print variables
            if let Some(vars) = workflow.get("variables").and_then(|v| v.as_object()) {
                if !vars.is_empty() {
                    println!();
                    println!("  Variables:");
                    for (k, v) in vars {
                        println!("    {}: {}", style(k).cyan(), v);
                    }
                }
            }

            // Print steps
            if let Some(steps) = workflow.get("steps").and_then(|v| v.as_array()) {
                println!();
                println!("  Steps ({}):", steps.len());
                for step in steps {
                    let step_id = step.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                    let step_type = step.get("type").and_then(|v| v.as_str()).unwrap_or("?");
                    let deps = step
                        .get("depends_on")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_default();

                    print!("    {} [{}]", style(step_id).bold(), step_type);
                    if !deps.is_empty() {
                        print!(" (depends: {})", style(deps).dim());
                    }
                    println!();
                }
            }

            println!("{}", style("-".repeat(60)).dim());
        }
    }

    Ok(())
}

/// Delete a workflow
async fn execute_delete(args: WorkflowDeleteArgs, config: &Config) -> Result<(), CliError> {
    if !args.force {
        println!(
            "Are you sure you want to delete workflow {}? [y/N]",
            style(&args.id).cyan()
        );
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    let client = CoordinatorClient::from_config(config)
        .await
        .map_err(|e| CliError::Client(format!("Failed to connect: {}", e)))?;

    client
        .delete_workflow(&args.id)
        .await
        .map_err(|e| CliError::Client(format!("Failed to delete workflow: {}", e)))?;

    println!(
        "{} Workflow {} deleted.",
        style("Marabunta").yellow().bold(),
        style(&args.id).cyan()
    );

    Ok(())
}

/// Cancel a running workflow
async fn execute_cancel(args: WorkflowCancelArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config)
        .await
        .map_err(|e| CliError::Client(format!("Failed to connect: {}", e)))?;

    client
        .cancel_run(&args.run_id)
        .await
        .map_err(|e| CliError::Client(format!("Failed to cancel run: {}", e)))?;

    println!(
        "{} Workflow run {} cancelled.",
        style("Marabunta").yellow().bold(),
        style(&args.run_id).cyan()
    );

    Ok(())
}

/// List active runs
async fn execute_runs(args: WorkflowRunsArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config)
        .await
        .map_err(|e| CliError::Client(format!("Failed to connect: {}", e)))?;

    let runs = client
        .list_runs(args.workflow.as_deref(), args.status.as_deref(), args.limit)
        .await
        .map_err(|e| CliError::Client(format!("Failed to list runs: {}", e)))?;

    if args.format == "json" {
        println!("{}", serde_json::to_string_pretty(&runs)?);
        return Ok(());
    }

    // Human-readable format
    println!();
    println!("{} Workflow Runs", style("Marabunta").yellow().bold());
    println!("{}", style("-".repeat(90)).dim());

    let runs_array = runs.get("runs").and_then(|v| v.as_array());

    if let Some(runs) = runs_array {
        if runs.is_empty() {
            println!("  No runs found.");
        } else {
            for run in runs {
                let id = run.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                let workflow_id = run
                    .get("workflow_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                let progress = run.get("progress").and_then(|v| v.as_f64()).unwrap_or(0.0);

                let status_colored = match status {
                    "running" => style(status).yellow(),
                    "completed" => style(status).green(),
                    "failed" => style(status).red(),
                    "cancelled" => style(status).dim(),
                    _ => style(status).white(),
                };

                println!(
                    "  {} ({}) - {} ({:.0}%)",
                    style(id).cyan(),
                    workflow_id,
                    status_colored,
                    progress * 100.0
                );
            }
        }
    } else {
        println!("  No runs found.");
    }

    println!("{}", style("-".repeat(90)).dim());

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

/// Parse a variable argument (key=value)
fn parse_variable(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid variable format: {} (expected key=value)", s))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

/// Print run status
fn print_run_status(status: &serde_json::Value, format: &str) {
    if format == "json" {
        println!(
            "{}",
            serde_json::to_string_pretty(status).unwrap_or_default()
        );
        return;
    }

    let id = status.get("id").and_then(|v| v.as_str()).unwrap_or("?");
    let workflow_id = status
        .get("workflow_id")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let state = status.get("status").and_then(|v| v.as_str()).unwrap_or("?");
    let progress = status
        .get("progress")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let error = status.get("error").and_then(|v| v.as_str());

    println!();
    println!("{} Workflow Run Status", style("Marabunta").yellow().bold());
    println!("{}", style("-".repeat(60)).dim());

    println!("  Run ID:     {}", style(id).cyan());
    println!("  Workflow:   {}", workflow_id);

    let state_colored = match state {
        "running" => style(state).yellow().bold(),
        "completed" => style(state).green().bold(),
        "failed" => style(state).red().bold(),
        "cancelled" => style(state).dim().bold(),
        _ => style(state).white().bold(),
    };
    println!("  Status:     {}", state_colored);

    // Progress bar
    let bar_width = 40;
    let filled = (progress * bar_width as f64) as usize;
    let empty = bar_width - filled;
    let bar = format!(
        "[{}{}] {:.1}%",
        "=".repeat(filled),
        " ".repeat(empty),
        progress * 100.0
    );
    println!("  Progress:   {}", bar);

    // Step states
    if let Some(step_states) = status.get("step_states").and_then(|v| v.as_object()) {
        println!();
        println!("  Steps:");
        for (step_id, state) in step_states {
            let step_status = state.get("status").and_then(|v| v.as_str()).unwrap_or("?");
            let status_icon = match step_status {
                "completed" => style("").green(),
                "running" => style("").yellow(),
                "failed" => style("").red(),
                "pending" => style("").dim(),
                "skipped" => style("").dim(),
                _ => style(" ").white(),
            };
            println!("    {} {} ({})", status_icon, step_id, step_status);
        }
    }

    if let Some(err) = error {
        println!();
        println!("  {}: {}", style("Error").red().bold(), err);
    }

    println!("{}", style("-".repeat(60)).dim());
}

/// Check if status is terminal
fn is_terminal_status(status: &serde_json::Value) -> bool {
    let state = status.get("status").and_then(|v| v.as_str()).unwrap_or("");
    matches!(state, "completed" | "failed" | "cancelled")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_variable() {
        assert_eq!(
            parse_variable("key=value").unwrap(),
            ("key".to_string(), "value".to_string())
        );
        assert_eq!(
            parse_variable("key=value=with=equals").unwrap(),
            ("key".to_string(), "value=with=equals".to_string())
        );
        assert!(parse_variable("no-equals").is_err());
    }
}
