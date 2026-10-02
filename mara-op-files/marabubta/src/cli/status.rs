// Marabunta - Licensed under the MIT License.
//! Job status command
//!
//! Displays the current status of a job with optional watch mode.

use clap::Args;
use std::time::Duration;
use tokio::time::sleep;

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::{clear_screen, print_job_status};
use crate::cli::types::{CliError, JobStatusResponse, OutputFormat};

// ─────────────────────────────────────────────────────────────────────────────
// STATUS ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the status command
#[derive(Args)]
pub struct StatusArgs {
    /// Job ID
    pub job_id: String,

    /// Watch mode (continuous updates)
    #[arg(long, short)]
    pub watch: bool,

    /// Watch interval in seconds
    #[arg(long, default_value = "2")]
    pub interval: u64,

    /// Output format (human, json, csv, yaml)
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,

    /// Show extended details (task-level breakdown)
    #[arg(long, short)]
    pub extended: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE STATUS
// ─────────────────────────────────────────────────────────────────────────────

/// Execute the status command
pub async fn execute_status(args: StatusArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config).await?;

    let format = args.format.unwrap_or(config.output_format);

    loop {
        let status = client.get_job_status(&args.job_id).await?;

        // Clear screen if watching
        if args.watch {
            clear_screen();
        }

        // Display based on format
        match format {
            OutputFormat::Human => {
                print_job_status(&status);
                if args.extended {
                    print_extended_status(&status);
                }
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
            OutputFormat::Csv => {
                print_status_csv(&status);
            }
            OutputFormat::Yaml => {
                println!("{}", serde_yaml_to_string(&status)?);
            }
        }

        // Exit if not watching or job is terminal
        if !args.watch || status.is_terminal() {
            break;
        }

        sleep(Duration::from_secs(args.interval)).await;
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// EXTENDED STATUS
// ─────────────────────────────────────────────────────────────────────────────

/// Print extended status details
fn print_extended_status(status: &JobStatusResponse) {
    use console::style;

    println!();
    println!("   {} Task Breakdown:", style("Extended").cyan());
    println!("   {}", style("─".repeat(50)).dim());

    // Task distribution
    let total = status.tasks_total as f64;
    if total > 0.0 {
        let completed_pct = (status.tasks_completed as f64 / total) * 100.0;
        let running_pct = (status.tasks_running as f64 / total) * 100.0;
        let failed_pct = (status.tasks_failed as f64 / total) * 100.0;

        // Calculate pending taking into account waiting and skipped for DAG jobs
        let waiting = status.tasks_waiting;
        let skipped = status.tasks_skipped;
        let pending = status
            .tasks_total
            .saturating_sub(status.tasks_completed)
            .saturating_sub(status.tasks_running)
            .saturating_sub(status.tasks_failed)
            .saturating_sub(waiting)
            .saturating_sub(skipped);
        let pending_pct = (pending as f64 / total) * 100.0;
        let waiting_pct = (waiting as f64 / total) * 100.0;
        let skipped_pct = (skipped as f64 / total) * 100.0;

        println!(
            "   Completed: {} ({:.1}%)",
            style(status.tasks_completed).green(),
            completed_pct
        );
        println!(
            "   Running:   {} ({:.1}%)",
            style(status.tasks_running).cyan(),
            running_pct
        );
        println!(
            "   Failed:    {} ({:.1}%)",
            if status.tasks_failed > 0 {
                style(status.tasks_failed).red().to_string()
            } else {
                style("0").dim().to_string()
            },
            failed_pct
        );

        // Show DAG-specific status
        if status.has_dependencies {
            println!(
                "   Waiting:   {} ({:.1}%)",
                style(waiting).yellow(),
                waiting_pct
            );
            if skipped > 0 {
                println!(
                    "   Skipped:   {} ({:.1}%)",
                    style(skipped).magenta(),
                    skipped_pct
                );
            }
        }

        println!(
            "   Pending:   {} ({:.1}%)",
            style(pending).yellow(),
            pending_pct
        );
    }

    // Throughput
    if status.runtime_secs > 0 && status.tasks_completed > 0 {
        let throughput = status.tasks_completed as f64 / status.runtime_secs as f64;
        println!();
        println!(
            "   Throughput: {:.2} tasks/sec",
            style(format!("{:.2}", throughput)).yellow()
        );
    }

    // DAG info
    if status.has_dependencies {
        println!();
        println!(
            "   {} This job uses a DAG workflow (tasks have dependencies)",
            style("Info:").cyan()
        );
        println!("   Tasks are scheduled when all their dependencies complete.");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTPUT FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/// Print status as CSV
fn print_status_csv(status: &JobStatusResponse) {
    println!("job_id,name,state,progress,tasks_total,tasks_completed,tasks_failed,tasks_running,nodes_active,runtime_secs");
    println!(
        "{},{},{},{:.4},{},{},{},{},{},{}",
        status.job_id,
        status.name,
        format!("{:?}", status.state),
        status.progress,
        status.tasks_total,
        status.tasks_completed,
        status.tasks_failed,
        status.tasks_running,
        status.nodes_active,
        status.runtime_secs
    );
}

/// Serialize to YAML string (simple implementation)
fn serde_yaml_to_string(status: &JobStatusResponse) -> Result<String, CliError> {
    // Simple YAML serialization without external dependency
    let mut yaml = String::new();
    yaml.push_str(&format!("job_id: {}\n", status.job_id));
    yaml.push_str(&format!("name: {}\n", status.name));
    yaml.push_str(&format!("state: {:?}\n", status.state));
    yaml.push_str(&format!("progress: {:.4}\n", status.progress));
    yaml.push_str(&format!("tasks_total: {}\n", status.tasks_total));
    yaml.push_str(&format!("tasks_completed: {}\n", status.tasks_completed));
    yaml.push_str(&format!("tasks_failed: {}\n", status.tasks_failed));
    yaml.push_str(&format!("tasks_running: {}\n", status.tasks_running));
    yaml.push_str(&format!("nodes_active: {}\n", status.nodes_active));
    yaml.push_str(&format!("runtime_secs: {}\n", status.runtime_secs));

    if let Some(started_at) = &status.started_at {
        yaml.push_str(&format!("started_at: {}\n", started_at));
    }

    if let Some(eta) = status.eta_secs {
        yaml.push_str(&format!("eta_secs: {}\n", eta));
    }

    if let Some(estimate) = &status.current_estimate {
        yaml.push_str("current_estimate:\n");
        yaml.push_str(&format!("  mean: {}\n", estimate.mean));
        yaml.push_str(&format!("  std_error: {}\n", estimate.std_error));
        yaml.push_str(&format!(
            "  confidence_interval: [{}, {}]\n",
            estimate.confidence_interval.0, estimate.confidence_interval.1
        ));
        if let Some(converged) = estimate.converged {
            yaml.push_str(&format!("  converged: {}\n", converged));
        }
        yaml.push_str(&format!(
            "  samples_processed: {}\n",
            estimate.samples_processed
        ));
    }

    Ok(yaml)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::types::JobState;

    #[test]
    fn test_status_csv_format() {
        let status = JobStatusResponse {
            job_id: "test-123".to_string(),
            name: "test job".to_string(),
            state: JobState::Running,
            progress: 0.5,
            tasks_total: 100,
            tasks_completed: 50,
            tasks_failed: 2,
            tasks_running: 10,
            tasks_waiting: 20,
            tasks_skipped: 0,
            nodes_active: 5,
            started_at: Some("2024-01-01T00:00:00Z".to_string()),
            runtime_secs: 300,
            eta_secs: Some(300),
            current_estimate: None,
            has_dependencies: true,
        };

        // Just verify it doesn't panic
        print_status_csv(&status);
    }

    #[test]
    fn test_yaml_serialization() {
        let status = JobStatusResponse {
            job_id: "test-123".to_string(),
            name: "test job".to_string(),
            state: JobState::Running,
            progress: 0.5,
            tasks_total: 100,
            tasks_completed: 50,
            tasks_failed: 2,
            tasks_running: 10,
            tasks_waiting: 0,
            tasks_skipped: 0,
            nodes_active: 5,
            started_at: Some("2024-01-01T00:00:00Z".to_string()),
            runtime_secs: 300,
            eta_secs: Some(300),
            current_estimate: None,
            has_dependencies: false,
        };

        let yaml = serde_yaml_to_string(&status).unwrap();
        assert!(yaml.contains("job_id: test-123"));
        assert!(yaml.contains("progress: 0.5"));
    }
}
