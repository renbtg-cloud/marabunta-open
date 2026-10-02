// Marabunta - Licensed under the MIT License.
//! Job submission command
//!
//! Handles building job specifications and submitting them to the coordinator.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use clap::Args;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

use crate::cli::client::CoordinatorClient;
use crate::cli::config::Config;
use crate::cli::display::{
    clear_screen, print_job_status, print_placement_plan, print_success, Spinner,
};
use crate::cli::types::{
    parse_duration, parse_size, CliError, JobConstraints, JobPreferences, JobSpec, JobState,
    NodeQuality, PlacementConfig, PlacementStrategy, Preset, Priority,
};

// ─────────────────────────────────────────────────────────────────────────────
// SUBMIT ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the submit command
#[derive(Args)]
pub struct SubmitArgs {
    /// Path to the job file (Python, WASM, etc.)
    pub file: PathBuf,

    /// Job name
    #[arg(long)]
    pub name: Option<String>,

    // ─────────────────────────────────────────────────────────────────────────
    // RUNTIME
    // ─────────────────────────────────────────────────────────────────────────
    /// Runtime to use (auto-detected if not specified)
    #[arg(long)]
    pub runtime: Option<String>,

    // ─────────────────────────────────────────────────────────────────────────
    // MONTE CARLO OPTIONS
    // ─────────────────────────────────────────────────────────────────────────
    /// Total number of samples
    #[arg(long)]
    pub samples: Option<u64>,

    /// Samples per task
    #[arg(long, default_value = "100")]
    pub samples_per_task: u32,

    /// Stop when relative error below this (e.g., 0.01 for 1%)
    #[arg(long)]
    pub converge: Option<f64>,

    // ─────────────────────────────────────────────────────────────────────────
    // HARD CONSTRAINTS
    // ─────────────────────────────────────────────────────────────────────────
    /// Minimum memory per task (e.g., "2GB", "512MB")
    #[arg(long)]
    pub memory_min: Option<String>,

    /// Minimum disk space per task
    #[arg(long)]
    pub disk_min: Option<String>,

    /// Required architectures (comma-separated: "x86_64,arm64")
    #[arg(long)]
    pub arch: Option<String>,

    /// Task timeout (e.g., "5m", "1h30m")
    #[arg(long, default_value = "5m")]
    pub timeout: String,

    // ─────────────────────────────────────────────────────────────────────────
    // SOFT PREFERENCES
    // ─────────────────────────────────────────────────────────────────────────
    /// Preferred memory per task
    #[arg(long)]
    pub memory_prefer: Option<String>,

    /// Preferred cores per task
    #[arg(long)]
    pub cores_prefer: Option<u32>,

    /// Prefer nodes in same region
    #[arg(long)]
    pub prefer_local: bool,

    // ─────────────────────────────────────────────────────────────────────────
    // PLACEMENT STRATEGY
    // ─────────────────────────────────────────────────────────────────────────
    /// Target total cores
    #[arg(long)]
    pub target_cores: Option<u32>,

    /// Target total memory
    #[arg(long)]
    pub target_memory: Option<String>,

    /// Node quality preference (fewer_better, more_smaller, balanced)
    #[arg(long, value_enum)]
    pub node_quality: Option<NodeQuality>,

    /// Placement strategy (throughput, latency, cost, balanced)
    #[arg(long, value_enum, default_value = "throughput")]
    pub strategy: PlacementStrategy,

    // ─────────────────────────────────────────────────────────────────────────
    // EXCLUSIONS
    // ─────────────────────────────────────────────────────────────────────────
    /// Exclude nodes with memory below this
    #[arg(long)]
    pub exclude_memory_below: Option<String>,

    /// Exclude nodes with reliability below this (0.0-1.0)
    #[arg(long)]
    pub exclude_reliability_below: Option<f32>,

    /// Exclude battery-powered nodes
    #[arg(long)]
    pub exclude_battery: bool,

    // ─────────────────────────────────────────────────────────────────────────
    // BEHAVIOR
    // ─────────────────────────────────────────────────────────────────────────
    /// Wait for job completion
    #[arg(long)]
    pub wait: bool,

    /// Save results to file
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Priority level (low, normal, high, critical)
    #[arg(long, value_enum, default_value = "normal")]
    pub priority: Priority,

    /// Use preset configuration (beefy, swarm, balanced, cheap)
    #[arg(long, value_enum)]
    pub preset: Option<Preset>,

    /// Dry run - show what would be submitted without actually submitting
    #[arg(long)]
    pub dry_run: bool,

    /// Don't show the placement plan
    #[arg(long)]
    pub no_plan: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE SUBMIT
// ─────────────────────────────────────────────────────────────────────────────

/// Execute the submit command
pub async fn execute_submit(args: SubmitArgs, config: &Config) -> Result<(), CliError> {
    use console::style;

    // 1. Read and analyze the job file
    if !args.file.exists() {
        return Err(CliError::NotFound(format!(
            "File not found: {}",
            args.file.display()
        )));
    }

    let job_file = std::fs::read(&args.file)?;
    let runtime = args
        .runtime
        .clone()
        .or_else(|| config.default_runtime.clone())
        .unwrap_or_else(|| detect_runtime(&args.file));

    let file_name = args
        .file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    // 2. Apply preset if specified
    let args = apply_preset(args);

    // 3. Build job specification
    let spinner = Spinner::new("Building job specification...");
    let job_spec = build_job_spec(&args, &job_file, &runtime, &file_name, config)?;
    spinner.finish();

    // Show dry run info
    if args.dry_run {
        println!("\n{} Dry Run - Job Specification:", style("").yellow());
        println!("{}", style("─".repeat(60)).dim());
        println!("{}", serde_json::to_string_pretty(&job_spec)?);
        println!("{}", style("─".repeat(60)).dim());
        return Ok(());
    }

    // 4. Connect to coordinator
    let spinner = Spinner::new("Connecting to coordinator...");
    let client = CoordinatorClient::from_config(config).await.map_err(|e| {
        spinner.finish();
        CliError::Client(format!("Failed to connect: {}", e))
    })?;
    spinner.finish();

    // 5. Submit job
    let spinner = Spinner::new("Submitting job...");
    let job_id = client.submit_job(job_spec).await.map_err(|e| {
        spinner.finish();
        CliError::Client(format!("Failed to submit job: {}", e))
    })?;
    spinner.finish();

    println!();
    println!("{} Job submitted!", style("Marabunta").yellow().bold());
    println!("   Job ID: {}", style(&job_id).cyan());

    // 6. Get and display placement plan
    if !args.no_plan {
        if let Ok(plan) = client.get_placement_plan(&job_id).await {
            print_placement_plan(&plan);
        }
    }

    // 7. If --wait, monitor until completion
    if args.wait {
        println!();
        println!("Waiting for job completion...");
        monitor_job(&client, &job_id, args.output.as_ref()).await?;
    } else {
        println!();
        println!("Job running in background.");
        println!("Check status: {} {}", style("marabunta status").cyan(), &job_id);
        println!("Get results:  {} {}", style("marabunta results").cyan(), &job_id);
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// RUNTIME DETECTION
// ─────────────────────────────────────────────────────────────────────────────

/// Detect runtime from file extension
fn detect_runtime(path: &PathBuf) -> String {
    match path.extension().and_then(|e| e.to_str()) {
        Some("py") => "python3".to_string(),
        Some("wasm") => "wasm".to_string(),
        Some("lua") => "lua".to_string(),
        Some("pl") => "perl".to_string(),
        Some("rb") => "ruby".to_string(),
        Some("jl") => "julia".to_string(),
        Some("r") | Some("R") => "r".to_string(),
        Some("f90") | Some("f95") | Some("f03") => "fortran".to_string(),
        Some("js") | Some("mjs") => "nodejs".to_string(),
        Some("sh") | Some("bash") => "bash".to_string(),
        _ => "native".to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PRESET APPLICATION
// ─────────────────────────────────────────────────────────────────────────────

/// Apply preset configuration
fn apply_preset(mut args: SubmitArgs) -> SubmitArgs {
    if let Some(preset) = &args.preset {
        match preset {
            Preset::Beefy => {
                // High-end nodes, fewer but more powerful
                if args.node_quality.is_none() {
                    args.node_quality = Some(NodeQuality::FewerBetter);
                }
                if args.memory_min.is_none() {
                    args.memory_min = Some("4GB".to_string());
                }
                if args.exclude_reliability_below.is_none() {
                    args.exclude_reliability_below = Some(0.9);
                }
                args.exclude_battery = true;
            }
            Preset::Swarm => {
                // Many small nodes, maximum parallelism
                if args.node_quality.is_none() {
                    args.node_quality = Some(NodeQuality::MoreSmaller);
                }
                if args.strategy == PlacementStrategy::Throughput {
                    args.strategy = PlacementStrategy::Throughput;
                }
                args.samples_per_task = args.samples_per_task.min(50);
            }
            Preset::Balanced => {
                // Balanced approach
                if args.node_quality.is_none() {
                    args.node_quality = Some(NodeQuality::Balanced);
                }
                if args.strategy == PlacementStrategy::Throughput {
                    args.strategy = PlacementStrategy::Balanced;
                }
            }
            Preset::Cheap => {
                // Minimize token usage
                args.strategy = PlacementStrategy::Cost;
                if args.exclude_reliability_below.is_none() {
                    args.exclude_reliability_below = Some(0.5);
                }
            }
        }
    }
    args
}

// ─────────────────────────────────────────────────────────────────────────────
// JOB SPEC BUILDING
// ─────────────────────────────────────────────────────────────────────────────

/// Build job specification from arguments
fn build_job_spec(
    args: &SubmitArgs,
    file_content: &[u8],
    runtime: &str,
    file_name: &str,
    _config: &Config,
) -> Result<JobSpec, CliError> {
    // Parse timeout
    let timeout = parse_duration(&args.timeout)?;

    // Parse sizes
    let memory_min = parse_size(&args.memory_min)?;
    let disk_min = parse_size(&args.disk_min)?;
    let memory_prefer = parse_size(&args.memory_prefer)?;
    let target_memory = parse_size(&args.target_memory)?;
    let exclude_memory_below = parse_size(&args.exclude_memory_below)?;

    // Parse architectures
    let architectures: Vec<String> = args
        .arch
        .as_ref()
        .map(|a| a.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    // Build constraints
    let constraints = JobConstraints {
        memory_min,
        disk_min,
        architectures,
        excluded_nodes: Vec::new(),
        reliability_min: args.exclude_reliability_below,
        exclude_battery: args.exclude_battery,
        exclude_memory_below,
    };

    // Build preferences
    let preferences = JobPreferences {
        memory_prefer,
        cores_prefer: args.cores_prefer,
        prefer_local: args.prefer_local,
        preferred_runtimes: vec![runtime.to_string()],
    };

    // Build placement config
    let placement = PlacementConfig {
        target_cores: args.target_cores,
        target_memory,
        node_quality: args.node_quality.unwrap_or_default(),
        strategy: args.strategy,
    };

    // Encode file content
    let encoded_content = BASE64.encode(file_content);

    // Generate job name
    let name = args.name.clone().unwrap_or_else(|| file_name.to_string());

    Ok(JobSpec {
        name,
        runtime: runtime.to_string(),
        file_content: encoded_content,
        file_name: file_name.to_string(),
        samples: args.samples,
        samples_per_task: args.samples_per_task,
        converge: args.converge,
        constraints,
        preferences,
        placement,
        priority: args.priority.to_u32(),
        timeout_secs: timeout.as_secs(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// JOB MONITORING
// ─────────────────────────────────────────────────────────────────────────────

/// Monitor job until completion
async fn monitor_job(
    client: &CoordinatorClient,
    job_id: &str,
    output_path: Option<&PathBuf>,
) -> Result<(), CliError> {
    use console::style;

    let poll_interval = Duration::from_secs(2);
    let mut last_progress = -1.0f64;

    loop {
        let status = client.get_job_status(job_id).await?;

        // Update display if progress changed
        if (status.progress - last_progress).abs() > 0.001 || status.is_terminal() {
            clear_screen();
            print_job_status(&status);
            last_progress = status.progress;
        }

        // Check if job is complete
        if status.is_terminal() {
            println!();
            match status.state {
                JobState::Completed => {
                    print_success("Job completed successfully!");

                    // Save results if output path specified
                    if let Some(path) = output_path {
                        let results = client.get_results(job_id).await?;
                        let json = serde_json::to_string_pretty(&results)?;
                        std::fs::write(path, json)?;
                        println!("Results saved to: {}", path.display());
                    }
                }
                JobState::Failed => {
                    println!("{} Job failed!", style("ERROR").red().bold());
                    if let Ok(results) = client.get_results(job_id).await {
                        if let Some(result) = &results.result {
                            println!("Error: {}", result);
                        }
                    }
                }
                JobState::Cancelled => {
                    println!("{} Job was cancelled", style("INFO").yellow());
                }
                _ => {}
            }
            break;
        }

        sleep(poll_interval).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_runtime() {
        assert_eq!(detect_runtime(&PathBuf::from("test.py")), "python3");
        assert_eq!(detect_runtime(&PathBuf::from("test.wasm")), "wasm");
        assert_eq!(detect_runtime(&PathBuf::from("test.lua")), "lua");
        assert_eq!(detect_runtime(&PathBuf::from("test.js")), "nodejs");
        assert_eq!(detect_runtime(&PathBuf::from("test.bin")), "native");
    }

    #[test]
    fn test_preset_beefy() {
        let args = SubmitArgs {
            file: PathBuf::from("test.py"),
            name: None,
            runtime: None,
            samples: None,
            samples_per_task: 100,
            converge: None,
            memory_min: None,
            disk_min: None,
            arch: None,
            timeout: "5m".to_string(),
            memory_prefer: None,
            cores_prefer: None,
            prefer_local: false,
            target_cores: None,
            target_memory: None,
            node_quality: None,
            strategy: PlacementStrategy::Throughput,
            exclude_memory_below: None,
            exclude_reliability_below: None,
            exclude_battery: false,
            wait: false,
            output: None,
            priority: Priority::Normal,
            preset: Some(Preset::Beefy),
            dry_run: false,
            no_plan: false,
        };

        let args = apply_preset(args);
        assert_eq!(args.node_quality, Some(NodeQuality::FewerBetter));
        assert_eq!(args.memory_min, Some("4GB".to_string()));
        assert!(args.exclude_battery);
    }
}
