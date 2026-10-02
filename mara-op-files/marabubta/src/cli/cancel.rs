// Marabunta - Licensed under the MIT License.
//! Job cancellation command
//!
//! Cancels running or pending jobs.

use clap::Args;
use dialoguer::{theme::ColorfulTheme, Confirm};

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::{print_error, print_success, print_warning};
use crate::cli::types::{CliError, JobState};

// ─────────────────────────────────────────────────────────────────────────────
// CANCEL ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the cancel command
#[derive(Args)]
pub struct CancelArgs {
    /// Job ID to cancel
    pub job_id: String,

    /// Skip confirmation prompt
    #[arg(long, short = 'y')]
    pub yes: bool,

    /// Force cancel even if job appears stuck
    #[arg(long)]
    pub force: bool,

    /// Cancel all tasks (not just pending)
    #[arg(long)]
    pub all_tasks: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE CANCEL
// ─────────────────────────────────────────────────────────────────────────────

/// Execute the cancel command
pub async fn execute_cancel(args: CancelArgs, config: &Config) -> Result<(), CliError> {
    use console::style;

    let client = CoordinatorClient::from_config(config).await?;

    // Get current job status
    let status = client.get_job_status(&args.job_id).await?;

    // Check if job can be cancelled
    if status.is_terminal() && !args.force {
        match status.state {
            JobState::Completed => {
                print_warning(&format!(
                    "Job {} is already completed. Nothing to cancel.",
                    args.job_id
                ));
                return Ok(());
            }
            JobState::Failed => {
                print_warning(&format!(
                    "Job {} has already failed. Nothing to cancel.",
                    args.job_id
                ));
                return Ok(());
            }
            JobState::Cancelled => {
                print_warning(&format!("Job {} is already cancelled.", args.job_id));
                return Ok(());
            }
            _ => {}
        }
    }

    // Show job info
    println!();
    println!("{} Cancel Job", style("Marabunta").yellow().bold());
    println!("{}", style("─".repeat(50)).dim());
    println!("   Job ID:     {}", style(&args.job_id).cyan());
    println!("   Name:       {}", status.name);
    println!("   Status:     {:?}", status.state);
    println!("   Progress:   {:.1}%", status.progress * 100.0);
    println!(
        "   Tasks:      {} / {} completed",
        status.tasks_completed, status.tasks_total
    );
    println!(
        "   Running:    {} tasks currently executing",
        status.tasks_running
    );
    println!("{}", style("─".repeat(50)).dim());

    // Warn about running tasks
    if status.tasks_running > 0 {
        print_warning(&format!(
            "{} tasks are currently running and will be terminated.",
            status.tasks_running
        ));
    }

    // Confirm unless --yes
    let confirmed = if args.yes || config.auto_confirm {
        true
    } else {
        let theme = ColorfulTheme::default();
        Confirm::with_theme(&theme)
            .with_prompt("Are you sure you want to cancel this job?")
            .default(false)
            .interact()
            .map_err(|e| CliError::Config(e.to_string()))?
    };

    if !confirmed {
        println!("Cancelled.");
        return Ok(());
    }

    // Cancel the job
    println!();
    println!("Cancelling job...");

    match client.cancel_job(&args.job_id).await {
        Ok(()) => {
            print_success(&format!("Job {} has been cancelled.", args.job_id));

            // Show final status
            if let Ok(final_status) = client.get_job_status(&args.job_id).await {
                println!();
                println!("Final Status:");
                println!("   Completed: {} tasks", final_status.tasks_completed);
                println!(
                    "   Cancelled: {} tasks",
                    final_status.tasks_total
                        - final_status.tasks_completed
                        - final_status.tasks_failed
                );
                println!("   Failed:    {} tasks", final_status.tasks_failed);
            }
        }
        Err(e) => {
            print_error(&format!("Failed to cancel job: {}", e));
            return Err(CliError::Client(e.to_string()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_args() {
        // Just verify the struct compiles correctly
        let args = CancelArgs {
            job_id: "test-123".to_string(),
            yes: true,
            force: false,
            all_tasks: false,
        };
        assert_eq!(args.job_id, "test-123");
        assert!(args.yes);
    }
}
