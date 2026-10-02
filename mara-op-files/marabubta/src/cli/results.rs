// Marabunta - Licensed under the MIT License.
//! Job results command
//!
//! Retrieves and displays job results in various formats.

use clap::Args;
use std::path::PathBuf;

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::{format_results_csv, format_results_human};
use crate::cli::types::{CliError, JobResults, OutputFormat};

// ─────────────────────────────────────────────────────────────────────────────
// RESULTS ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the results command
#[derive(Args)]
pub struct ResultsArgs {
    /// Job ID
    pub job_id: String,

    /// Get partial results (even if job not complete)
    #[arg(long)]
    pub partial: bool,

    /// Output format (human, json, csv, yaml)
    #[arg(long, value_enum)]
    pub format: Option<OutputFormat>,

    /// Save to file
    #[arg(long, short)]
    pub output: Option<PathBuf>,

    /// Only show the final result value
    #[arg(long)]
    pub value_only: bool,

    /// Show task-level details
    #[arg(long)]
    pub tasks: bool,

    /// Filter tasks by status (completed, failed)
    #[arg(long)]
    pub task_status: Option<String>,

    /// Limit number of tasks shown
    #[arg(long, default_value = "100")]
    pub limit: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE RESULTS
// ─────────────────────────────────────────────────────────────────────────────

/// Execute the results command
pub async fn execute_results(args: ResultsArgs, config: &Config) -> Result<(), CliError> {
    let client = CoordinatorClient::from_config(config).await?;

    // Fetch results
    let results = if args.partial {
        client.get_partial_results(&args.job_id).await?
    } else {
        client.get_results(&args.job_id).await?
    };

    // Filter tasks if requested
    let results = filter_results(results, &args);

    // Format output
    let format = args.format.unwrap_or(config.output_format);
    let output = format_output(&results, format, &args)?;

    // Write to file or stdout
    if let Some(path) = &args.output {
        std::fs::write(path, &output)?;
        println!("Results saved to {}", path.display());
    } else {
        println!("{}", output);
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// FILTERING
// ─────────────────────────────────────────────────────────────────────────────

/// Filter results based on arguments
fn filter_results(mut results: JobResults, args: &ResultsArgs) -> JobResults {
    // Filter by task status
    if let Some(status_filter) = &args.task_status {
        let status_filter = status_filter.to_lowercase();
        results
            .tasks
            .retain(|t| t.status.to_lowercase() == status_filter);
    }

    // Limit number of tasks
    if results.tasks.len() > args.limit {
        results.tasks.truncate(args.limit);
    }

    results
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTPUT FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/// Format output based on format type
fn format_output(
    results: &JobResults,
    format: OutputFormat,
    args: &ResultsArgs,
) -> Result<String, CliError> {
    // If value-only mode, just return the result value
    if args.value_only {
        return Ok(match &results.result {
            Some(value) => serde_json::to_string_pretty(value)?,
            None => "null".to_string(),
        });
    }

    Ok(match format {
        OutputFormat::Human => {
            let mut output = format_results_human(results);
            if args.tasks {
                output.push_str(&format_tasks_human(&results.tasks));
            }
            output
        }
        OutputFormat::Json => serde_json::to_string_pretty(results)?,
        OutputFormat::Csv => format_results_csv(results),
        OutputFormat::Yaml => format_results_yaml(results)?,
    })
}

/// Format tasks for human output
fn format_tasks_human(tasks: &[crate::cli::types::TaskResult]) -> String {
    use console::style;

    let mut output = String::new();
    output.push_str(&format!(
        "\n   {} Task Results:\n",
        style("Individual").cyan()
    ));
    output.push_str(&format!("   {}\n", style("─".repeat(70)).dim()));
    output.push_str(&format!(
        "   {:<12} {:<12} {:<12} {:<12} {}\n",
        style("Task ID").dim(),
        style("Status").dim(),
        style("Duration").dim(),
        style("Worker").dim(),
        style("Output").dim()
    ));
    output.push_str(&format!("   {}\n", "-".repeat(70)));

    for task in tasks {
        let task_id_short = if task.task_id.len() > 12 {
            &task.task_id[..12]
        } else {
            &task.task_id
        };

        let status_styled = match task.status.to_lowercase().as_str() {
            "completed" => style(&task.status).green().to_string(),
            "failed" => style(&task.status).red().to_string(),
            "running" => style(&task.status).cyan().to_string(),
            _ => task.status.clone(),
        };

        let duration = format!("{}ms", task.duration_ms);
        let worker = task
            .worker_id
            .as_ref()
            .map(|w| if w.len() > 12 { &w[..12] } else { w })
            .unwrap_or("-");

        let output_preview = task
            .output
            .as_ref()
            .map(|o| {
                let s = serde_json::to_string(o).unwrap_or_default();
                if s.len() > 20 {
                    format!("{}...", &s[..20])
                } else {
                    s
                }
            })
            .unwrap_or_else(|| "-".to_string());

        output.push_str(&format!(
            "   {:<12} {:<12} {:<12} {:<12} {}\n",
            task_id_short, status_styled, duration, worker, output_preview
        ));
    }

    output
}

/// Format results as YAML
fn format_results_yaml(results: &JobResults) -> Result<String, CliError> {
    let mut yaml = String::new();

    yaml.push_str(&format!("job_id: {}\n", results.job_id));
    yaml.push_str(&format!("name: {}\n", results.name));
    yaml.push_str(&format!("state: {:?}\n", results.state));
    yaml.push_str(&format!("is_partial: {}\n", results.is_partial));

    // Result
    if let Some(result) = &results.result {
        yaml.push_str("result:\n");
        let result_str = serde_json::to_string_pretty(result)?;
        for line in result_str.lines() {
            yaml.push_str(&format!("  {}\n", line));
        }
    }

    // Stats
    yaml.push_str("stats:\n");
    yaml.push_str(&format!("  total_tasks: {}\n", results.stats.total_tasks));
    yaml.push_str(&format!(
        "  completed_tasks: {}\n",
        results.stats.completed_tasks
    ));
    yaml.push_str(&format!("  failed_tasks: {}\n", results.stats.failed_tasks));
    yaml.push_str(&format!(
        "  total_runtime_secs: {}\n",
        results.stats.total_runtime_secs
    ));
    yaml.push_str(&format!(
        "  avg_task_duration_ms: {}\n",
        results.stats.avg_task_duration_ms
    ));
    yaml.push_str(&format!("  nodes_used: {}\n", results.stats.nodes_used));
    yaml.push_str(&format!("  tokens_spent: {}\n", results.stats.tokens_spent));

    // Tasks
    yaml.push_str("tasks:\n");
    for task in &results.tasks {
        yaml.push_str(&format!("  - task_id: {}\n", task.task_id));
        yaml.push_str(&format!("    status: {}\n", task.status));
        yaml.push_str(&format!("    duration_ms: {}\n", task.duration_ms));
        if let Some(worker) = &task.worker_id {
            yaml.push_str(&format!("    worker_id: {}\n", worker));
        }
        if let Some(output) = &task.output {
            let output_str = serde_json::to_string(output)?;
            yaml.push_str(&format!("    output: {}\n", output_str));
        }
    }

    Ok(yaml)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::types::{JobState, JobStats, TaskResult};

    fn create_test_results() -> JobResults {
        JobResults {
            job_id: "test-123".to_string(),
            name: "test job".to_string(),
            state: JobState::Completed,
            result: Some(serde_json::json!({"value": 3.14159})),
            tasks: vec![
                TaskResult {
                    task_id: "task-001".to_string(),
                    name: None,
                    status: "completed".to_string(),
                    state: None,
                    output: Some(serde_json::json!({"pi": 3.14})),
                    duration_ms: 100,
                    worker_id: Some("worker-abc".to_string()),
                    depends_on: vec![],
                },
                TaskResult {
                    task_id: "task-002".to_string(),
                    name: None,
                    status: "completed".to_string(),
                    state: None,
                    output: Some(serde_json::json!({"pi": 3.15})),
                    duration_ms: 120,
                    worker_id: Some("worker-def".to_string()),
                    depends_on: vec![],
                },
            ],
            stats: JobStats {
                total_tasks: 2,
                completed_tasks: 2,
                failed_tasks: 0,
                total_runtime_secs: 10,
                avg_task_duration_ms: 110,
                nodes_used: 2,
                tokens_spent: 0.5,
            },
            is_partial: false,
        }
    }

    #[test]
    fn test_filter_by_status() {
        let results = create_test_results();
        let args = ResultsArgs {
            job_id: "test".to_string(),
            partial: false,
            format: None,
            output: None,
            value_only: false,
            tasks: false,
            task_status: Some("completed".to_string()),
            limit: 100,
        };

        let filtered = filter_results(results, &args);
        assert_eq!(filtered.tasks.len(), 2);
    }

    #[test]
    fn test_filter_by_limit() {
        let results = create_test_results();
        let args = ResultsArgs {
            job_id: "test".to_string(),
            partial: false,
            format: None,
            output: None,
            value_only: false,
            tasks: false,
            task_status: None,
            limit: 1,
        };

        let filtered = filter_results(results, &args);
        assert_eq!(filtered.tasks.len(), 1);
    }

    #[test]
    fn test_yaml_format() {
        let results = create_test_results();
        let yaml = format_results_yaml(&results).unwrap();

        assert!(yaml.contains("job_id: test-123"));
        assert!(yaml.contains("total_tasks: 2"));
        assert!(yaml.contains("task_id: task-001"));
    }
}
