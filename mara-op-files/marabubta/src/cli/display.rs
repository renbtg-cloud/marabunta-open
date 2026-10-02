// Marabunta - Licensed under the MIT License.
//! Display and formatting helpers for CLI output

use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{self, Write};
use std::time::Duration;

use crate::cli::types::{
    JobResults, JobState, JobStatusResponse, MonteCarloEstimate, NodeInfo, PlacementPlan,
    TaskDependencyInfo, TokenBalance, TokenTransaction,
};

// ─────────────────────────────────────────────────────────────────────────────
// BYTE FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/// Format bytes into human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.1}TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DURATION FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/// Format duration into human-readable string
pub fn format_duration(dur: Duration) -> String {
    let secs = dur.as_secs();
    if secs >= 86400 {
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        format!("{}d {}h", days, hours)
    } else if secs >= 3600 {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    } else if secs >= 60 {
        let mins = secs / 60;
        let secs = secs % 60;
        format!("{}m {}s", mins, secs)
    } else {
        format!("{}s", secs)
    }
}

/// Format duration from seconds
pub fn format_duration_secs(secs: u64) -> String {
    format_duration(Duration::from_secs(secs))
}

// ─────────────────────────────────────────────────────────────────────────────
// TIME FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/// Format timestamp string into display format
pub fn format_time(timestamp: &Option<String>) -> String {
    match timestamp {
        Some(ts) => ts.clone(),
        None => "-".to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// STATUS FORMATTING
// ─────────────────────────────────────────────────────────────────────────────

/// Format job state with color
pub fn format_status(state: &JobState) -> String {
    match state {
        JobState::Pending => style("Pending").yellow().to_string(),
        JobState::Scheduled => style("Scheduled").cyan().to_string(),
        JobState::Running => style("Running").blue().bold().to_string(),
        JobState::Completed => style("Completed").green().bold().to_string(),
        JobState::Failed => style("Failed").red().bold().to_string(),
        JobState::Cancelled => style("Cancelled").dim().to_string(),
    }
}

/// Format node status with color
pub fn format_node_status(status: &str) -> String {
    match status.to_lowercase().as_str() {
        "ready" => style(status).green().to_string(),
        "busy" => style(status).yellow().to_string(),
        "starting" => style(status).cyan().to_string(),
        "draining" => style(status).magenta().to_string(),
        "offline" => style(status).red().to_string(),
        _ => status.to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PROGRESS BAR
// ─────────────────────────────────────────────────────────────────────────────

/// Create a progress bar for job progress
pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {msg} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%)")
            .unwrap()
            .progress_chars("=>-"),
    );
    pb.set_message(message.to_string());
    pb
}

/// Create a spinner for indefinite operations
pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Print a text-based progress bar (for non-interactive output)
pub fn print_progress_bar(progress: f64, width: usize) -> String {
    let pct = progress * 100.0;
    let filled = ((progress * width as f64) as usize).min(width);
    let empty = width.saturating_sub(filled);

    format!(
        "[{}{}] {:.1}%",
        style("=".repeat(filled)).cyan(),
        style("-".repeat(empty)).dim(),
        pct
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// JOB STATUS DISPLAY
// ─────────────────────────────────────────────────────────────────────────────

/// Print job status in human-readable format
pub fn print_job_status(status: &JobStatusResponse) {
    let bee = style("Marabunta").yellow().bold();
    println!("\n{} Job Status", bee);
    println!("{}", style("─".repeat(60)).dim());

    println!("   {}     {}", style("ID:").dim(), status.job_id);
    println!("   {}   {}", style("Name:").dim(), status.name);
    println!(
        "   {} {}",
        style("Status:").dim(),
        format_status(&status.state)
    );

    // Show DAG indicator if job has dependencies
    if status.has_dependencies {
        println!(
            "   {}    {}",
            style("Type:").dim(),
            style("DAG Workflow").cyan()
        );
    }
    println!();

    // Progress bar
    let pct = status.progress * 100.0;
    let filled = (pct / 2.0) as usize;
    let empty = 50usize.saturating_sub(filled);
    println!(
        "   {} [{}{}] {:.1}%",
        style("Progress:").dim(),
        style("=".repeat(filled)).cyan(),
        style("-".repeat(empty)).dim(),
        pct
    );
    println!();

    // Task stats
    println!(
        "   {}    {} / {} completed",
        style("Tasks:").dim(),
        style(status.tasks_completed).green(),
        status.tasks_total
    );
    println!(
        "   {}   {}",
        style("Failed:").dim(),
        if status.tasks_failed > 0 {
            style(status.tasks_failed).red().to_string()
        } else {
            style("0").dim().to_string()
        }
    );
    println!(
        "   {}  {}",
        style("Running:").dim(),
        style(status.tasks_running).cyan()
    );

    // Show waiting/skipped for DAG jobs
    if status.has_dependencies {
        println!(
            "   {}  {}",
            style("Waiting:").dim(),
            style(status.tasks_waiting).yellow()
        );
        if status.tasks_skipped > 0 {
            println!(
                "   {}  {}",
                style("Skipped:").dim(),
                style(status.tasks_skipped).magenta()
            );
        }
    }
    println!();

    // Nodes
    println!(
        "   {}    {} active",
        style("Nodes:").dim(),
        style(status.nodes_active).yellow()
    );
    println!();

    // Time
    println!(
        "   {}  {}",
        style("Started:").dim(),
        format_time(&status.started_at)
    );
    println!(
        "   {}  {}",
        style("Runtime:").dim(),
        format_duration_secs(status.runtime_secs)
    );
    if let Some(eta) = status.eta_secs {
        println!(
            "   {}      {}",
            style("ETA:").dim(),
            format_duration_secs(eta)
        );
    }
    println!();

    // Monte Carlo results
    if let Some(estimate) = &status.current_estimate {
        print_monte_carlo_estimate(estimate);
    }

    println!("{}", style("─".repeat(60)).dim());
}

/// Print task dependencies for a DAG job
pub fn print_task_dependencies(tasks: &[TaskDependencyInfo]) {
    println!();
    println!("   {} Task Dependencies:", style("DAG").cyan());
    println!("   {}", style("─".repeat(50)).dim());

    for task in tasks {
        let name_display = task
            .name
            .as_ref()
            .map(|n| format!(" ({})", n))
            .unwrap_or_default();

        let state_display = format_task_state(&task.state);

        // Task header
        println!(
            "   {} {}{}  [{}]",
            style(">").cyan(),
            &task.task_id[..12.min(task.task_id.len())],
            style(name_display).dim(),
            state_display
        );

        // Dependencies
        if !task.depends_on.is_empty() {
            let deps_str: Vec<String> = task
                .depends_on
                .iter()
                .map(|d| d[..12.min(d.len())].to_string())
                .collect();
            let satisfied_mark = if task.dependencies_satisfied {
                style("ok").green().to_string()
            } else if task.has_failed_dependency {
                style("blocked").red().to_string()
            } else {
                style("waiting").yellow().to_string()
            };
            println!(
                "      {} {} [{}]",
                style("depends on:").dim(),
                deps_str.join(", "),
                satisfied_mark
            );
        }

        // Dependents
        if !task.dependents.is_empty() {
            let deps_str: Vec<String> = task
                .dependents
                .iter()
                .map(|d| d[..12.min(d.len())].to_string())
                .collect();
            println!("      {} {}", style("blocks:").dim(), deps_str.join(", "));
        }
    }

    println!("   {}", style("─".repeat(50)).dim());
}

/// Format task state with color
pub fn format_task_state(state: &str) -> String {
    match state.to_lowercase().as_str() {
        "pending" => style(state).dim().to_string(),
        "waiting" => style(state).yellow().to_string(),
        "ready" => style(state).cyan().to_string(),
        "running" => style(state).blue().bold().to_string(),
        "completed" => style(state).green().to_string(),
        "failed" => style(state).red().bold().to_string(),
        "skipped" => style(state).magenta().to_string(),
        "cancelled" => style(state).dim().to_string(),
        _ => state.to_string(),
    }
}

/// Print Monte Carlo estimate
fn print_monte_carlo_estimate(estimate: &MonteCarloEstimate) {
    println!("   {} Monte Carlo Estimate:", style("Current").cyan());
    println!(
        "   {}     {} +/- {}",
        style("Mean:").dim(),
        style(format!("{:.6}", estimate.mean)).green(),
        style(format!("{:.6}", estimate.std_error)).yellow()
    );
    println!(
        "   {}   [{:.6}, {:.6}]",
        style("95% CI:").dim(),
        estimate.confidence_interval.0,
        estimate.confidence_interval.1
    );
    if let Some(converged) = estimate.converged {
        println!(
            "   {} {}",
            style("Converged:").dim(),
            if converged {
                style("Yes").green().bold().to_string()
            } else {
                style("Not yet").yellow().to_string()
            }
        );
    }
    println!(
        "   {}  {}",
        style("Samples:").dim(),
        format_number(estimate.samples_processed)
    );
    println!();
}

// ─────────────────────────────────────────────────────────────────────────────
// PLACEMENT PLAN DISPLAY
// ─────────────────────────────────────────────────────────────────────────────

/// Print placement plan
pub fn print_placement_plan(plan: &PlacementPlan) {
    println!();
    println!("   {} Placement Plan:", style("").yellow());
    println!(
        "   {}",
        style("┌────────────────────────────────────────────────────────────┐").dim()
    );
    println!(
        "   {}  Target: ~{} cores, ~{}",
        style("│").dim(),
        plan.target_cores,
        format_bytes(plan.target_memory)
    );
    println!("   {}", style("│").dim());
    println!(
        "   {}  Selected: {} nodes",
        style("│").dim(),
        style(plan.nodes.len()).green()
    );
    println!(
        "   {}  Total: {} cores, {}",
        style("│").dim(),
        style(plan.total_cores).cyan(),
        format_bytes(plan.total_memory)
    );
    println!(
        "   {}  Est. completion: {}",
        style("│").dim(),
        style(format_duration_secs(plan.estimated_completion_secs)).yellow()
    );
    println!("   {}", style("│").dim());
    if plan.excluded_count > 0 {
        println!(
            "   {}  Excluded: {} nodes ({})",
            style("│").dim(),
            style(plan.excluded_count).red(),
            plan.excluded_reason
        );
    }
    println!(
        "   {}",
        style("└────────────────────────────────────────────────────────────┘").dim()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// RESULTS DISPLAY
// ─────────────────────────────────────────────────────────────────────────────

/// Format results for human-readable output
pub fn format_results_human(results: &JobResults) -> String {
    let mut output = String::new();

    output.push_str(&format!(
        "\n{} Job Results\n",
        style("Marabunta").yellow().bold()
    ));
    output.push_str(&format!("{}\n", style("─".repeat(60)).dim()));

    output.push_str(&format!(
        "   {}     {}\n",
        style("ID:").dim(),
        results.job_id
    ));
    output.push_str(&format!("   {}   {}\n", style("Name:").dim(), results.name));
    output.push_str(&format!(
        "   {} {}\n",
        style("Status:").dim(),
        format_status(&results.state)
    ));

    if results.is_partial {
        output.push_str(&format!(
            "   {}  {}\n",
            style("Note:").dim(),
            style("Partial results").yellow()
        ));
    }
    output.push('\n');

    // Result value
    if let Some(result) = &results.result {
        output.push_str(&format!("   {} Result:\n", style("Final").cyan()));
        output.push_str(&format!(
            "   {}\n\n",
            serde_json::to_string_pretty(result).unwrap_or_default()
        ));
    }

    // Stats
    output.push_str(&format!("   {} Statistics:\n", style("Job").cyan()));
    output.push_str(&format!(
        "   Tasks:      {} / {} completed ({} failed)\n",
        results.stats.completed_tasks, results.stats.total_tasks, results.stats.failed_tasks
    ));
    output.push_str(&format!(
        "   Runtime:    {}\n",
        format_duration_secs(results.stats.total_runtime_secs)
    ));
    output.push_str(&format!(
        "   Avg Task:   {}ms\n",
        results.stats.avg_task_duration_ms
    ));
    output.push_str(&format!("   Nodes Used: {}\n", results.stats.nodes_used));
    output.push_str(&format!(
        "   Tokens:     {:.4} spent\n",
        results.stats.tokens_spent
    ));

    output.push_str(&format!("{}\n", style("─".repeat(60)).dim()));

    output
}

/// Format results as CSV
pub fn format_results_csv(results: &JobResults) -> String {
    let mut output = String::new();

    // Header
    output.push_str("task_id,status,duration_ms,worker_id,output\n");

    // Rows
    for task in &results.tasks {
        let output_str = task
            .output
            .as_ref()
            .map(|o| serde_json::to_string(o).unwrap_or_default())
            .unwrap_or_default();

        output.push_str(&format!(
            "{},{},{},{},{}\n",
            task.task_id,
            task.status,
            task.duration_ms,
            task.worker_id.as_deref().unwrap_or(""),
            output_str.replace(',', ";").replace('\n', " ")
        ));
    }

    output
}

// ─────────────────────────────────────────────────────────────────────────────
// NODES DISPLAY
// ─────────────────────────────────────────────────────────────────────────────

/// Print nodes summary
pub fn print_nodes_summary(nodes: &[NodeInfo]) {
    use std::collections::HashMap;

    let bee = style("Marabunta").yellow().bold();
    println!("\n{} Available Nodes: {}", bee, style(nodes.len()).green());
    println!("{}", style("─".repeat(60)).dim());

    // Summary by type
    let mut by_type: HashMap<&str, Vec<&NodeInfo>> = HashMap::new();
    for node in nodes {
        by_type.entry(&node.node_type).or_default().push(node);
    }

    for (node_type, type_nodes) in &by_type {
        let total_cores: u32 = type_nodes.iter().map(|n| n.cores).sum();
        let total_memory: u64 = type_nodes.iter().map(|n| n.memory).sum();

        println!(
            "   {}: {} nodes, {} cores, {} RAM",
            style(node_type).cyan(),
            type_nodes.len(),
            style(total_cores).yellow(),
            format_bytes(total_memory)
        );
    }

    println!("{}", style("─".repeat(60)).dim());
}

/// Print detailed nodes list
pub fn print_nodes_detail(nodes: &[NodeInfo]) {
    println!();
    println!(
        "   {:<12} {:<8} {:<10} {:<10} {:<20}",
        style("ID").dim(),
        style("Cores").dim(),
        style("Memory").dim(),
        style("Status").dim(),
        style("Runtimes").dim()
    );
    println!("   {}", "-".repeat(70));

    for node in nodes {
        let id_short = if node.id.len() > 12 {
            &node.id[..12]
        } else {
            &node.id
        };

        println!(
            "   {:<12} {:<8} {:<10} {:<10} {:<20}",
            id_short,
            node.cores,
            format_bytes(node.memory),
            format_node_status(&node.status),
            node.runtimes.join(", ")
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TOKEN DISPLAY
// ─────────────────────────────────────────────────────────────────────────────

/// Print token balance
pub fn print_token_balance(balance: &TokenBalance) {
    let bee = style("Marabunta").yellow().bold();
    println!("\n{} Token Balance", bee);
    println!("{}", style("─".repeat(40)).dim());

    println!(
        "   {}  {}",
        style("Current:").dim(),
        style(format!("{:.4}", balance.balance)).green().bold()
    );
    println!(
        "   {}  {}",
        style("Pending:").dim(),
        style(format!("+{:.4}", balance.pending)).yellow()
    );
    println!();
    println!(
        "   {}   {:.4}",
        style("Earned:").dim(),
        balance.total_earned
    );
    println!("   {}    {:.4}", style("Spent:").dim(), balance.total_spent);

    println!("{}", style("─".repeat(40)).dim());
}

/// Print token transactions
pub fn print_token_transactions(transactions: &[TokenTransaction]) {
    println!();
    println!(
        "   {:<20} {:<10} {:<12} {}",
        style("Timestamp").dim(),
        style("Type").dim(),
        style("Amount").dim(),
        style("Description").dim()
    );
    println!("   {}", "-".repeat(60));

    for tx in transactions {
        let amount_str = if tx.amount >= 0.0 {
            style(format!("+{:.4}", tx.amount)).green().to_string()
        } else {
            style(format!("{:.4}", tx.amount)).red().to_string()
        };

        println!(
            "   {:<20} {:<10} {:<12} {}",
            &tx.timestamp[..20.min(tx.timestamp.len())],
            tx.tx_type,
            amount_str,
            tx.description
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

/// Format large numbers with commas
pub fn format_number(n: u64) -> String {
    let s = n.to_string();
    let chars: Vec<char> = s.chars().rev().collect();
    let mut result = String::new();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result.chars().rev().collect()
}

/// Clear the terminal screen
pub fn clear_screen() {
    print!("\x1B[2J\x1B[1;1H");
    let _ = io::stdout().flush();
}

/// Print a success message
pub fn print_success(message: &str) {
    println!("{} {}", style("SUCCESS").green().bold(), message);
}

/// Print an error message
pub fn print_error(message: &str) {
    eprintln!("{} {}", style("ERROR").red().bold(), message);
}

/// Print a warning message
pub fn print_warning(message: &str) {
    println!("{} {}", style("WARNING").yellow().bold(), message);
}

/// Print an info message
pub fn print_info(message: &str) {
    println!("{} {}", style("INFO").cyan(), message);
}

/// Spinner helper for async operations
pub struct Spinner {
    pb: ProgressBar,
}

impl Spinner {
    pub fn new(message: &str) -> Self {
        Self {
            pb: create_spinner(message),
        }
    }

    pub fn set_message(&self, message: &str) {
        self.pb.set_message(message.to_string());
    }

    pub fn finish(&self) {
        self.pb.finish_and_clear();
    }

    pub fn finish_with_message(&self, message: &str) {
        self.pb.finish_with_message(message.to_string());
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.pb.finish_and_clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500B");
        assert_eq!(format_bytes(1024), "1.0KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0GB");
        assert_eq!(format_bytes(2 * 1024 * 1024 * 1024), "2.0GB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m 30s");
        assert_eq!(format_duration(Duration::from_secs(3600)), "1h 0m");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m");
        assert_eq!(format_duration(Duration::from_secs(86400)), "1d 0h");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(100), "100");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1000000), "1,000,000");
    }
}
