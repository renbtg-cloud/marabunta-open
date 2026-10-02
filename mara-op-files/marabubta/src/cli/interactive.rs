// Marabunta - Licensed under the MIT License.
//! Interactive REPL mode for job management
//!
//! Provides a shell-like interface for managing jobs, tasks, and nodes
//! with tab completion and command history.
//!
//! # Usage
//!
//! ```bash
//! marabunta shell
//! ```
//!
//! # Commands
//!
//! ```text
//! jobs                  List all jobs
//! job <id>              Show job details
//! job <id> status       Show job status
//! job <id> cancel       Cancel job
//! job <id> results      Get job results
//! tasks <job-id>        List tasks for a job
//! nodes                 List all nodes
//! node <id>             Show node details
//! workers               List all workers
//! submit <file>         Submit a new job
//! config                Show current config
//! help                  Show help
//! exit, quit, q         Exit shell
//! ```

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::Arc;

use console::style;
use tokio::sync::RwLock;

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::{
    format_bytes, format_duration_secs, format_node_status, format_status,
    print_job_status,
};
use crate::cli::table::{Column, Table, Alignment};
use crate::cli::types::{CliError, NodeFilter, OutputFormat};

// ─────────────────────────────────────────────────────────────────────────────
// REPL STATE
// ─────────────────────────────────────────────────────────────────────────────

/// REPL state containing client connection and history
pub struct ReplState {
    /// Coordinator client
    client: Option<CoordinatorClient>,
    /// Configuration
    config: Config,
    /// Command history
    history: VecDeque<String>,
    /// Maximum history size
    max_history: usize,
    /// Current job context (for short commands)
    current_job: Option<String>,
    /// Known job IDs for completion
    job_ids: Vec<String>,
    /// Known node IDs for completion
    node_ids: Vec<String>,
    /// Output format
    output_format: OutputFormat,
    /// Whether to use colors
    use_colors: bool,
}

impl ReplState {
    /// Create new REPL state
    pub fn new(config: Config) -> Self {
        Self {
            client: None,
            config,
            history: VecDeque::with_capacity(1000),
            max_history: 1000,
            current_job: None,
            job_ids: Vec::new(),
            node_ids: Vec::new(),
            output_format: OutputFormat::Human,
            use_colors: true,
        }
    }

    /// Add command to history
    fn add_to_history(&mut self, cmd: &str) {
        let cmd = cmd.trim().to_string();
        if !cmd.is_empty() {
            // Don't add duplicates of the last command
            if self.history.back().map(|s| s.as_str()) != Some(&cmd) {
                if self.history.len() >= self.max_history {
                    self.history.pop_front();
                }
                self.history.push_back(cmd);
            }
        }
    }

    /// Get completions for partial input
    pub fn get_completions(&self, partial: &str) -> Vec<String> {
        let parts: Vec<&str> = partial.split_whitespace().collect();

        if parts.is_empty() || (parts.len() == 1 && !partial.ends_with(' ')) {
            // Complete command
            let prefix = parts.first().copied().unwrap_or("");
            return COMMANDS
                .iter()
                .filter(|c| c.starts_with(prefix))
                .map(|s| s.to_string())
                .collect();
        }

        let cmd = parts[0];
        let arg_partial = if partial.ends_with(' ') {
            ""
        } else {
            parts.last().copied().unwrap_or("")
        };

        match cmd {
            "job" | "status" | "results" | "cancel" | "tasks" => {
                // Complete with job IDs
                self.job_ids
                    .iter()
                    .filter(|id| id.starts_with(arg_partial))
                    .cloned()
                    .collect()
            }
            "node" => {
                // Complete with node IDs
                self.node_ids
                    .iter()
                    .filter(|id| id.starts_with(arg_partial))
                    .cloned()
                    .collect()
            }
            "set" if parts.len() <= 2 => {
                // Complete with config keys
                CONFIG_KEYS
                    .iter()
                    .filter(|k| k.starts_with(arg_partial))
                    .map(|s| s.to_string())
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}

/// Available commands
static COMMANDS: &[&str] = &[
    "jobs", "job", "tasks", "task", "nodes", "node", "workers", "worker",
    "submit", "cancel", "status", "results", "config", "set", "connect",
    "help", "exit", "quit", "q", "clear", "history", "info",
];

/// Config keys for completion
static CONFIG_KEYS: &[&str] = &[
    "coordinator", "format", "color", "timeout", "region",
];

// ─────────────────────────────────────────────────────────────────────────────
// REPL EXECUTION
// ─────────────────────────────────────────────────────────────────────────────

/// Run the interactive REPL
pub async fn run_repl(config: &Config) -> Result<(), CliError> {
    let state = Arc::new(RwLock::new(ReplState::new(config.clone())));

    // Print welcome banner
    print_banner();

    // Try to connect initially
    {
        let mut state = state.write().await;
        match CoordinatorClient::from_config(&state.config).await {
            Ok(client) => {
                state.client = Some(client);
                println!(
                    "{}  Connected to {}",
                    style("").green(),
                    style(&state.config.coordinator_url).cyan()
                );
            }
            Err(e) => {
                println!(
                    "{}  Not connected: {}",
                    style("").yellow(),
                    e
                );
                println!("    Use 'connect <url>' to connect manually");
            }
        }
    }
    println!();

    // Main REPL loop
    loop {
        // Print prompt
        let prompt = {
            let state = state.read().await;
            if let Some(job) = &state.current_job {
                format!(
                    "{}{}{}> ",
                    style("marabunta").yellow().bold(),
                    style(":").dim(),
                    style(&job[..8.min(job.len())]).cyan()
                )
            } else {
                format!("{}> ", style("marabunta").yellow().bold())
            }
        };
        print!("{}", prompt);
        io::stdout().flush()?;

        // Read input
        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            break;
        }
        let input = input.trim();

        // Handle empty input
        if input.is_empty() {
            continue;
        }

        // Add to history
        {
            let mut state = state.write().await;
            state.add_to_history(input);
        }

        // Parse and execute command
        let result = execute_command(input, Arc::clone(&state)).await;

        match result {
            Ok(should_exit) if should_exit => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("{} {}", style("Error:").red().bold(), e);
            }
        }
    }

    println!("Goodbye!");
    Ok(())
}

/// Execute a single command, returns true if REPL should exit
async fn execute_command(input: &str, state: Arc<RwLock<ReplState>>) -> Result<bool, CliError> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(false);
    }

    let cmd = parts[0].to_lowercase();
    let args = &parts[1..];

    match cmd.as_str() {
        // Exit commands
        "exit" | "quit" | "q" => Ok(true),

        // Help
        "help" | "?" => {
            print_help();
            Ok(false)
        }

        // Clear screen
        "clear" | "cls" => {
            print!("\x1B[2J\x1B[1;1H");
            io::stdout().flush()?;
            Ok(false)
        }

        // History
        "history" => {
            let state = state.read().await;
            for (i, cmd) in state.history.iter().enumerate() {
                println!("{:4}  {}", i + 1, cmd);
            }
            Ok(false)
        }

        // Connection
        "connect" => {
            let url = args.first().copied();
            cmd_connect(state, url).await?;
            Ok(false)
        }

        // Info
        "info" => {
            cmd_info(state).await?;
            Ok(false)
        }

        // Config
        "config" => {
            let state = state.read().await;
            println!("{}", toml::to_string_pretty(&state.config)?);
            Ok(false)
        }

        // Set config
        "set" => {
            if args.len() < 2 {
                println!("Usage: set <key> <value>");
                println!("Keys: coordinator, format, color, timeout");
            } else {
                cmd_set(state, args[0], args[1]).await?;
            }
            Ok(false)
        }

        // List jobs
        "jobs" | "ls" => {
            cmd_jobs(state, args).await?;
            Ok(false)
        }

        // Job details/subcommands
        "job" => {
            if args.is_empty() {
                let state = state.read().await;
                if let Some(job) = &state.current_job {
                    println!("Current job: {}", style(job).cyan());
                } else {
                    println!("No job selected. Use 'job <id>' to select one.");
                }
            } else {
                cmd_job(state, args).await?;
            }
            Ok(false)
        }

        // Quick status
        "status" | "stat" => {
            if args.is_empty() {
                let current_job = {
                    let state_guard = state.read().await;
                    state_guard.current_job.clone()
                };
                if let Some(job) = current_job {
                    cmd_job_status(Arc::clone(&state), &job).await?;
                } else {
                    println!("No job selected. Use 'status <job-id>'.");
                }
            } else {
                cmd_job_status(state, args[0]).await?;
            }
            Ok(false)
        }

        // Results
        "results" | "res" => {
            if args.is_empty() {
                let current_job = {
                    let state_guard = state.read().await;
                    state_guard.current_job.clone()
                };
                if let Some(job) = current_job {
                    cmd_job_results(Arc::clone(&state), &job).await?;
                } else {
                    println!("No job selected. Use 'results <job-id>'.");
                }
            } else {
                cmd_job_results(state, args[0]).await?;
            }
            Ok(false)
        }

        // Cancel
        "cancel" => {
            if args.is_empty() {
                let current_job = {
                    let state_guard = state.read().await;
                    state_guard.current_job.clone()
                };
                if let Some(job) = current_job {
                    cmd_job_cancel(Arc::clone(&state), &job).await?;
                } else {
                    println!("No job selected. Use 'cancel <job-id>'.");
                }
            } else {
                cmd_job_cancel(state, args[0]).await?;
            }
            Ok(false)
        }

        // Tasks
        "tasks" => {
            if args.is_empty() {
                let current_job = {
                    let state_guard = state.read().await;
                    state_guard.current_job.clone()
                };
                if let Some(job) = current_job {
                    cmd_tasks(Arc::clone(&state), &job).await?;
                } else {
                    println!("No job selected. Use 'tasks <job-id>'.");
                }
            } else {
                cmd_tasks(state, args[0]).await?;
            }
            Ok(false)
        }

        // Nodes
        "nodes" => {
            cmd_nodes(state, args).await?;
            Ok(false)
        }

        // Node details
        "node" => {
            if args.is_empty() {
                println!("Usage: node <id>");
            } else {
                cmd_node(state, args[0]).await?;
            }
            Ok(false)
        }

        // Workers
        "workers" => {
            cmd_workers(state, args).await?;
            Ok(false)
        }

        // Unknown command
        _ => {
            println!("Unknown command: {}. Type 'help' for available commands.", cmd);
            Ok(false)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// COMMAND IMPLEMENTATIONS
// ─────────────────────────────────────────────────────────────────────────────

/// Connect to coordinator
async fn cmd_connect(state: Arc<RwLock<ReplState>>, url: Option<&str>) -> Result<(), CliError> {
    let mut state = state.write().await;

    let url = url
        .map(|s| s.to_string())
        .unwrap_or_else(|| state.config.coordinator_url.clone());

    println!("Connecting to {}...", style(&url).cyan());

    match CoordinatorClient::connect(&url).await {
        Ok(client) => {
            state.client = Some(client);
            state.config.coordinator_url = url.clone();
            println!("{}  Connected!", style("").green());
        }
        Err(e) => {
            println!("{}  Connection failed: {}", style("").red(), e);
        }
    }

    Ok(())
}

/// Show cluster info
async fn cmd_info(state: Arc<RwLock<ReplState>>) -> Result<(), CliError> {
    let state = state.read().await;

    println!();
    println!("{} Cluster Information", style("Marabunta").yellow().bold());
    println!("{}", style("-".repeat(50)).dim());

    if let Some(client) = &state.client {
        if let Ok(health) = client.health_check().await {
            println!("  Status:      {}", style(&health.status).green());
            if let Some(version) = &health.version {
                println!("  Version:     {}", version);
            }
            if let Some(uptime) = health.uptime_secs {
                println!("  Uptime:      {}", format_duration_secs(uptime));
            }
        }

        // Get node summary
        if let Ok(nodes) = client.list_nodes(NodeFilter::default()).await {
            let total_cores: u32 = nodes.iter().map(|n| n.cores).sum();
            let total_memory: u64 = nodes.iter().map(|n| n.memory).sum();
            let ready_count = nodes.iter().filter(|n| n.status == "ready").count();

            println!();
            println!("  Nodes:       {} ({} ready)", nodes.len(), ready_count);
            println!("  Total Cores: {}", total_cores);
            println!("  Total RAM:   {}", format_bytes(total_memory));
        }
    } else {
        println!("  Status:      {}", style("Not connected").red());
    }

    println!();
    println!("  Coordinator: {}", &state.config.coordinator_url);
    println!("{}", style("-".repeat(50)).dim());

    Ok(())
}

/// Set config value
async fn cmd_set(state: Arc<RwLock<ReplState>>, key: &str, value: &str) -> Result<(), CliError> {
    let mut state = state.write().await;

    match key {
        "coordinator" | "url" => {
            state.config.coordinator_url = value.to_string();
            println!("Set coordinator URL to {}", style(value).cyan());
        }
        "format" => {
            state.output_format = match value.to_lowercase().as_str() {
                "json" => OutputFormat::Json,
                "csv" => OutputFormat::Csv,
                "yaml" => OutputFormat::Yaml,
                _ => OutputFormat::Human,
            };
            println!("Set output format to {}", style(value).cyan());
        }
        "color" | "colors" => {
            state.use_colors = value.parse().unwrap_or(true);
            println!("Set colors to {}", style(value).cyan());
        }
        _ => {
            println!("Unknown config key: {}", key);
        }
    }

    Ok(())
}

/// List jobs
async fn cmd_jobs(state: Arc<RwLock<ReplState>>, _args: &[&str]) -> Result<(), CliError> {
    let state = state.read().await;

    let _client = state.client.as_ref()
        .ok_or_else(|| CliError::Client("Not connected".to_string()))?;

    // For now, we don't have a list jobs endpoint, so show a placeholder
    println!();
    println!("{} Recent Jobs", style("Marabunta").yellow().bold());
    println!("{}", style("-".repeat(80)).dim());
    println!("  (Job listing requires coordinator API support)");
    println!();
    println!("  Use 'job <id>' to view a specific job's status.");
    println!("{}", style("-".repeat(80)).dim());

    Ok(())
}

/// Job details/subcommands
async fn cmd_job(state: Arc<RwLock<ReplState>>, args: &[&str]) -> Result<(), CliError> {
    if args.is_empty() {
        return Ok(());
    }

    let job_id = args[0];
    let subcmd = args.get(1).copied();

    // Set current job context
    {
        let mut state_write = state.write().await;
        state_write.current_job = Some(job_id.to_string());

        // Add to known job IDs
        if !state_write.job_ids.contains(&job_id.to_string()) {
            state_write.job_ids.push(job_id.to_string());
        }
    }

    match subcmd {
        None | Some("status") | Some("stat") => {
            cmd_job_status(state, job_id).await
        }
        Some("results") | Some("res") => {
            cmd_job_results(state, job_id).await
        }
        Some("cancel") => {
            cmd_job_cancel(state, job_id).await
        }
        Some("tasks") => {
            cmd_tasks(state, job_id).await
        }
        Some(cmd) => {
            println!("Unknown job subcommand: {}. Use status, results, cancel, or tasks.", cmd);
            Ok(())
        }
    }
}

/// Show job status
async fn cmd_job_status(state: Arc<RwLock<ReplState>>, job_id: &str) -> Result<(), CliError> {
    let state = state.read().await;

    let client = state.client.as_ref()
        .ok_or_else(|| CliError::Client("Not connected".to_string()))?;

    let status = client.get_job_status(job_id).await
        .map_err(|e| CliError::Client(e.to_string()))?;

    if state.output_format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        print_job_status(&status);
    }

    Ok(())
}

/// Get job results
async fn cmd_job_results(state: Arc<RwLock<ReplState>>, job_id: &str) -> Result<(), CliError> {
    let state = state.read().await;

    let client = state.client.as_ref()
        .ok_or_else(|| CliError::Client("Not connected".to_string()))?;

    let results = client.get_results(job_id).await
        .map_err(|e| CliError::Client(e.to_string()))?;

    if state.output_format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        // Print summary
        println!();
        println!("{} Job Results", style("Marabunta").yellow().bold());
        println!("{}", style("-".repeat(60)).dim());
        println!("  Job ID:     {}", style(&results.job_id).cyan());
        println!("  Name:       {}", &results.name);
        println!("  Status:     {}", format_status(&results.state));
        println!();
        println!("  Tasks:      {} completed, {} failed",
            results.stats.completed_tasks, results.stats.failed_tasks);
        println!("  Runtime:    {}", format_duration_secs(results.stats.total_runtime_secs));

        if let Some(result) = &results.result {
            println!();
            println!("  Result:");
            println!("  {}", serde_json::to_string_pretty(result)?);
        }
        println!("{}", style("-".repeat(60)).dim());
    }

    Ok(())
}

/// Cancel job
async fn cmd_job_cancel(state: Arc<RwLock<ReplState>>, job_id: &str) -> Result<(), CliError> {
    let state = state.read().await;

    let client = state.client.as_ref()
        .ok_or_else(|| CliError::Client("Not connected".to_string()))?;

    client.cancel_job(job_id).await
        .map_err(|e| CliError::Client(e.to_string()))?;

    println!("{}  Job {} cancelled", style("").green(), style(job_id).cyan());

    Ok(())
}

/// List tasks for a job
async fn cmd_tasks(state: Arc<RwLock<ReplState>>, job_id: &str) -> Result<(), CliError> {
    let state = state.read().await;

    let client = state.client.as_ref()
        .ok_or_else(|| CliError::Client("Not connected".to_string()))?;

    let results = client.get_results(job_id).await
        .map_err(|e| CliError::Client(e.to_string()))?;

    if state.output_format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(&results.tasks)?);
    } else {
        // Build table
        let mut table = Table::new(vec![
            Column::new("Task ID").width(16),
            Column::new("Status").width(12),
            Column::new("Duration").width(10).align(Alignment::Right),
            Column::new("Worker").width(14),
        ]);

        for task in &results.tasks {
            let task_id = if task.task_id.len() > 14 {
                format!("{}...", &task.task_id[..12])
            } else {
                task.task_id.clone()
            };

            let status_colored = match task.status.to_lowercase().as_str() {
                "completed" => style(&task.status).green().to_string(),
                "failed" => style(&task.status).red().to_string(),
                "running" => style(&task.status).yellow().to_string(),
                _ => task.status.clone(),
            };

            let duration = format!("{}ms", task.duration_ms);
            let worker = task.worker_id.as_deref().unwrap_or("-").to_string();
            let worker = if worker.len() > 12 {
                format!("{}...", &worker[..10])
            } else {
                worker
            };

            table.add_row(vec![task_id, status_colored, duration, worker]);
        }

        println!();
        println!("{} Tasks for Job {}", style("Marabunta").yellow().bold(), style(job_id).cyan());
        table.print();
    }

    Ok(())
}

/// List nodes
async fn cmd_nodes(state: Arc<RwLock<ReplState>>, _args: &[&str]) -> Result<(), CliError> {
    let state_lock = state.read().await;

    let client = state_lock.client.as_ref()
        .ok_or_else(|| CliError::Client("Not connected".to_string()))?;

    let nodes = client.list_nodes(NodeFilter::default()).await
        .map_err(|e| CliError::Client(e.to_string()))?;

    // Update known node IDs
    drop(state_lock);
    {
        let mut state_write = state.write().await;
        state_write.node_ids = nodes.iter().map(|n| n.id.clone()).collect();
    }
    let state_lock = state.read().await;

    if state_lock.output_format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(&nodes)?);
    } else {
        // Build table
        let mut table = Table::new(vec![
            Column::new("ID").width(14),
            Column::new("Type").width(12),
            Column::new("Cores").width(6).align(Alignment::Right),
            Column::new("Memory").width(8).align(Alignment::Right),
            Column::new("Status").width(10),
            Column::new("Load").width(6).align(Alignment::Right),
            Column::new("Tasks").width(6).align(Alignment::Right),
        ]);

        for node in &nodes {
            let id = if node.id.len() > 12 {
                format!("{}...", &node.id[..10])
            } else {
                node.id.clone()
            };

            table.add_row(vec![
                id,
                node.node_type.clone(),
                node.cores.to_string(),
                format_bytes(node.memory),
                format_node_status(&node.status),
                format!("{:.0}%", node.current_load * 100.0),
                node.running_tasks.to_string(),
            ]);
        }

        println!();
        println!("{} Cluster Nodes", style("Marabunta").yellow().bold());
        table.print();

        // Print summary
        let total_cores: u32 = nodes.iter().map(|n| n.cores).sum();
        let total_memory: u64 = nodes.iter().map(|n| n.memory).sum();
        let ready_count = nodes.iter().filter(|n| n.status == "ready").count();

        println!();
        println!(
            "  {} nodes ({} ready), {} cores, {} RAM",
            style(nodes.len()).green(),
            ready_count,
            style(total_cores).yellow(),
            format_bytes(total_memory)
        );
    }

    Ok(())
}

/// Show node details
async fn cmd_node(state: Arc<RwLock<ReplState>>, node_id: &str) -> Result<(), CliError> {
    let state = state.read().await;

    let client = state.client.as_ref()
        .ok_or_else(|| CliError::Client("Not connected".to_string()))?;

    let node = client.get_node(node_id).await
        .map_err(|e| CliError::Client(e.to_string()))?;

    if state.output_format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(&node)?);
    } else {
        println!();
        println!("{} Node Details", style("Marabunta").yellow().bold());
        println!("{}", style("-".repeat(50)).dim());
        println!("  ID:           {}", style(&node.id).cyan());
        println!("  Type:         {}", &node.node_type);
        println!("  Status:       {}", format_node_status(&node.status));
        println!();
        println!("  Cores:        {}", node.cores);
        println!("  Memory:       {}", format_bytes(node.memory));
        println!("  Disk:         {}", format_bytes(node.disk));
        println!("  Architecture: {}", &node.architecture);
        println!();
        println!("  Reliability:  {:.1}%", node.reliability * 100.0);
        println!("  Current Load: {:.1}%", node.current_load * 100.0);
        println!("  Running Tasks: {}", node.running_tasks);
        println!();
        println!("  Runtimes:     {}", node.runtimes.join(", "));
        if let Some(region) = &node.region {
            println!("  Region:       {}", region);
        }
        println!("  Battery:      {}", if node.battery_powered { "Yes" } else { "No" });
        println!("{}", style("-".repeat(50)).dim());
    }

    Ok(())
}

/// List workers
async fn cmd_workers(state: Arc<RwLock<ReplState>>, _args: &[&str]) -> Result<(), CliError> {
    // Workers are essentially the same as nodes in this context
    cmd_nodes(state, &[]).await
}

// ─────────────────────────────────────────────────────────────────────────────
// HELP & BANNER
// ─────────────────────────────────────────────────────────────────────────────

/// Print welcome banner
fn print_banner() {
    println!();
    println!(
        "{}  Marabunta Compute Interactive Shell",
        style("").yellow()
    );
    println!("{}", style("-".repeat(50)).dim());
    println!("Type 'help' for available commands, 'exit' to quit.");
    println!();
}

/// Print help
fn print_help() {
    println!();
    println!("{} Available Commands", style("Marabunta").yellow().bold());
    println!("{}", style("-".repeat(60)).dim());
    println!();
    println!("  {}  ", style("Connection").cyan().bold());
    println!("    connect [url]      Connect to coordinator");
    println!("    info               Show cluster information");
    println!();
    println!("  {}  ", style("Jobs").cyan().bold());
    println!("    jobs               List recent jobs");
    println!("    job <id>           Select job and show status");
    println!("    job <id> status    Show job status");
    println!("    job <id> results   Get job results");
    println!("    job <id> cancel    Cancel job");
    println!("    job <id> tasks     List tasks");
    println!();
    println!("  {}  (use selected job)", style("Quick Commands").cyan().bold());
    println!("    status [id]        Show job status");
    println!("    results [id]       Get job results");
    println!("    cancel [id]        Cancel job");
    println!("    tasks [id]         List tasks");
    println!();
    println!("  {}  ", style("Nodes").cyan().bold());
    println!("    nodes              List all nodes");
    println!("    node <id>          Show node details");
    println!("    workers            List all workers");
    println!();
    println!("  {}  ", style("Configuration").cyan().bold());
    println!("    config             Show current config");
    println!("    set <key> <value>  Set config value");
    println!();
    println!("  {}  ", style("Shell").cyan().bold());
    println!("    help, ?            Show this help");
    println!("    history            Show command history");
    println!("    clear              Clear screen");
    println!("    exit, quit, q      Exit shell");
    println!();
    println!("{}", style("-".repeat(60)).dim());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repl_state_history() {
        let mut state = ReplState::new(Config::default());
        state.add_to_history("jobs");
        state.add_to_history("nodes");
        state.add_to_history("jobs"); // Should not duplicate

        assert_eq!(state.history.len(), 3);
    }

    #[test]
    fn test_completions() {
        let state = ReplState::new(Config::default());

        let completions = state.get_completions("jo");
        assert!(completions.contains(&"jobs".to_string()));
        assert!(completions.contains(&"job".to_string()));
    }
}
