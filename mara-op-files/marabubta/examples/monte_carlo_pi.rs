// Marabunta - Licensed under the MIT License.
//! Monte Carlo Pi Estimation Example
//!
//! This example demonstrates how to use Marabunta Compute for distributed Monte Carlo
//! simulations. Each worker generates random points and counts how many fall
//! inside a unit circle, which can be used to estimate Pi.
//!
//! # How it works
//!
//! 1. The job is split across multiple workers, each handling a portion of samples
//! 2. Each worker generates random (x, y) points in [0, 1] x [0, 1]
//! 3. Points where x^2 + y^2 <= 1 are "inside" the quarter circle
//! 4. The ratio inside/total approximates Pi/4
//! 5. Results from all workers are aggregated to get the final estimate
//!
//! # Running this example
//!
//! ```bash
//! # Submit to the cluster
//! marabunta submit examples/monte_carlo_pi.rs --runtime native --workers 8
//!
//! # Or run locally for testing
//! cargo run --example monte_carlo_pi
//! ```

use marabunta_compute::common::{Job, JobId, Task, TaskId, TaskPayload};
use marabunta_compute::sdk::{
    marabunta_checkpoint_exists, marabunta_checkpoint_load, marabunta_checkpoint_save, marabunta_get_worker_count,
    marabunta_get_worker_rank, marabunta_hint_can_split, marabunta_hint_memory_needed, marabunta_hint_time_estimate,
    marabunta_log_info, marabunta_progress_set, marabunta_progress_set_stage, marabunta_result_set_json,
    marabunta_task_cleanup, marabunta_task_init_ex, marabunta_yield, MarabuntaContext, MARABUNTA_YIELD_ABORT,
    MARABUNTA_YIELD_CONTINUE, MARABUNTA_YIELD_PAUSE,
};
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::time::Instant;

// ============================================================================
// Configuration and State Types
// ============================================================================

/// Configuration for the Monte Carlo simulation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonteCarloConfig {
    /// Total number of samples across all workers
    pub total_samples: u64,
    /// Random seed (different workers use seed + rank)
    pub base_seed: u64,
    /// How often to checkpoint (every N samples)
    pub checkpoint_interval: u64,
    /// How often to report progress (every N samples)
    pub progress_interval: u64,
}

impl Default for MonteCarloConfig {
    fn default() -> Self {
        Self {
            total_samples: 100_000_000, // 100 million samples
            base_seed: 42,
            checkpoint_interval: 1_000_000, // Checkpoint every 1M samples
            progress_interval: 100_000,     // Report progress every 100K samples
        }
    }
}

/// State that can be checkpointed for fault tolerance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationState {
    /// Number of samples processed so far
    pub samples_done: u64,
    /// Number of points inside the circle
    pub inside_count: u64,
    /// Current RNG state (simplified - in practice, save actual RNG state)
    pub current_seed: u64,
}

/// Result from a single worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerResult {
    pub worker_rank: u32,
    pub samples_processed: u64,
    pub inside_count: u64,
    pub elapsed_ms: u64,
    pub pi_estimate: f64,
}

/// Aggregated result from all workers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalResult {
    pub total_samples: u64,
    pub total_inside: u64,
    pub pi_estimate: f64,
    pub relative_error: f64,
    pub worker_results: Vec<WorkerResult>,
    pub total_elapsed_ms: u64,
}

// ============================================================================
// Monte Carlo Simulation Logic
// ============================================================================

/// Run the Monte Carlo simulation
///
/// This is the main entry point that would be called by the Marabunta worker runtime.
pub fn run_monte_carlo(config: MonteCarloConfig) -> Result<WorkerResult, String> {
    let start_time = Instant::now();

    // Initialize the Marabunta task context
    // In a real cluster, these would be provided by the runtime
    let checkpoint_dir = CString::new("/tmp/marabunta_checkpoints").unwrap();
    let ctx = unsafe {
        marabunta_task_init_ex(
            1, // Task ID (would be assigned by scheduler)
            0, // Worker rank (would be assigned by scheduler)
            1, // Worker count (would be set by scheduler)
            checkpoint_dir.as_ptr(),
        )
    };

    if ctx.is_null() {
        return Err("Failed to initialize Marabunta context".to_string());
    }

    // Get worker information
    let worker_rank = unsafe { marabunta_get_worker_rank(ctx) };
    let worker_count = unsafe { marabunta_get_worker_count(ctx) };

    // Calculate this worker's share of samples
    let samples_per_worker = config.total_samples / worker_count as u64;
    let my_samples = if worker_rank == worker_count - 1 {
        // Last worker handles any remainder
        samples_per_worker + (config.total_samples % worker_count as u64)
    } else {
        samples_per_worker
    };

    // Set resource hints to help the scheduler
    unsafe {
        // We need about 1MB of memory for state
        marabunta_hint_memory_needed(ctx, 1024 * 1024);
        // Estimate time based on sample count (1M samples per second estimate)
        marabunta_hint_time_estimate(ctx, (my_samples / 1_000_000 + 1) as u32);
        // This task can be split across 2-16 workers
        marabunta_hint_can_split(ctx, 2, 16);
    }

    // Log start of computation
    let msg = CString::new(format!(
        "Worker {}/{} starting with {} samples",
        worker_rank, worker_count, my_samples
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, msg.as_ptr()) };

    // Try to restore from checkpoint if one exists
    let mut state = if unsafe { marabunta_checkpoint_exists(ctx) } == 1 {
        let stage_msg = CString::new("Restoring from checkpoint").unwrap();
        unsafe { marabunta_progress_set_stage(ctx, stage_msg.as_ptr()) };

        let mut buffer = vec![0u8; 1024];
        let len = unsafe { marabunta_checkpoint_load(ctx, buffer.as_mut_ptr(), buffer.len()) };
        if len > 0 {
            match serde_json::from_slice::<SimulationState>(&buffer[..len as usize]) {
                Ok(saved_state) => {
                    let msg = CString::new(format!(
                        "Restored from checkpoint at sample {}",
                        saved_state.samples_done
                    ))
                    .unwrap();
                    unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
                    saved_state
                }
                Err(_) => SimulationState {
                    samples_done: 0,
                    inside_count: 0,
                    current_seed: config.base_seed + worker_rank as u64,
                },
            }
        } else {
            SimulationState {
                samples_done: 0,
                inside_count: 0,
                current_seed: config.base_seed + worker_rank as u64,
            }
        }
    } else {
        SimulationState {
            samples_done: 0,
            inside_count: 0,
            current_seed: config.base_seed + worker_rank as u64,
        }
    };

    // Set stage to running
    let stage_msg = CString::new("Generating samples").unwrap();
    unsafe { marabunta_progress_set_stage(ctx, stage_msg.as_ptr()) };

    // Initialize RNG with worker-specific seed
    let mut rng = rand::rngs::StdRng::seed_from_u64(state.current_seed);

    // Main computation loop
    while state.samples_done < my_samples {
        // Check yield point for cooperative scheduling
        let yield_result = unsafe { marabunta_yield(ctx) };
        match yield_result {
            MARABUNTA_YIELD_ABORT => {
                let msg = CString::new("Received abort signal").unwrap();
                unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
                break;
            }
            MARABUNTA_YIELD_PAUSE => {
                // Save checkpoint and pause
                let checkpoint_data = serde_json::to_vec(&state).unwrap();
                unsafe {
                    marabunta_checkpoint_save(ctx, checkpoint_data.as_ptr(), checkpoint_data.len())
                };
                let msg = CString::new("Pausing at checkpoint").unwrap();
                unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
                break;
            }
            MARABUNTA_YIELD_CONTINUE => {}
            _ => {}
        }

        // Generate a random point in [0, 1] x [0, 1]
        let x: f64 = rng.gen();
        let y: f64 = rng.gen();

        // Check if point is inside the quarter circle (x^2 + y^2 <= 1)
        if x * x + y * y <= 1.0 {
            state.inside_count += 1;
        }

        state.samples_done += 1;

        // Periodic checkpoint
        if state.samples_done % config.checkpoint_interval == 0 {
            let checkpoint_data = serde_json::to_vec(&state).unwrap();
            unsafe { marabunta_checkpoint_save(ctx, checkpoint_data.as_ptr(), checkpoint_data.len()) };
        }

        // Periodic progress update
        if state.samples_done % config.progress_interval == 0 {
            let progress = ((state.samples_done as f64 / my_samples as f64) * 100.0) as u8;
            unsafe { marabunta_progress_set(ctx, progress) };
        }
    }

    // Calculate this worker's Pi estimate
    let pi_estimate = 4.0 * state.inside_count as f64 / state.samples_done as f64;
    let elapsed_ms = start_time.elapsed().as_millis() as u64;

    // Create result
    let result = WorkerResult {
        worker_rank,
        samples_processed: state.samples_done,
        inside_count: state.inside_count,
        elapsed_ms,
        pi_estimate,
    };

    // Set the result as JSON
    let result_json = serde_json::to_string(&result).unwrap();
    let result_cstr = CString::new(result_json).unwrap();
    unsafe { marabunta_result_set_json(ctx, result_cstr.as_ptr()) };

    // Log completion
    let msg = CString::new(format!(
        "Worker {} completed: {} samples, Pi estimate = {:.10}",
        worker_rank, state.samples_done, pi_estimate
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, msg.as_ptr()) };

    // Mark as 100% complete
    unsafe { marabunta_progress_set(ctx, 100) };

    // Cleanup
    unsafe { marabunta_task_cleanup(ctx) };

    Ok(result)
}

/// Aggregate results from all workers (run on coordinator)
pub fn aggregate_results(worker_results: Vec<WorkerResult>) -> FinalResult {
    let total_samples: u64 = worker_results.iter().map(|r| r.samples_processed).sum();
    let total_inside: u64 = worker_results.iter().map(|r| r.inside_count).sum();
    let total_elapsed_ms = worker_results
        .iter()
        .map(|r| r.elapsed_ms)
        .max()
        .unwrap_or(0);

    let pi_estimate = 4.0 * total_inside as f64 / total_samples as f64;
    let relative_error = ((pi_estimate - std::f64::consts::PI) / std::f64::consts::PI).abs();

    FinalResult {
        total_samples,
        total_inside,
        pi_estimate,
        relative_error,
        worker_results,
        total_elapsed_ms,
    }
}

// ============================================================================
// Job Creation Helpers
// ============================================================================

/// Create a Monte Carlo Pi estimation job for submission to the cluster
pub fn create_monte_carlo_job(config: MonteCarloConfig, num_workers: u32) -> Job {
    let mut job = Job::new("monte-carlo-pi");
    job.priority = 100;
    job.metadata = serde_json::json!({
        "total_samples": config.total_samples,
        "num_workers": num_workers,
        "description": "Monte Carlo Pi estimation using distributed random sampling"
    });

    // Create tasks for each worker
    for worker_id in 0..num_workers {
        let mut task = Task::new(
            job.id,
            TaskPayload::MonteCarlo {
                seed: config.base_seed + worker_id as u64,
                iterations: config.total_samples / num_workers as u64,
                params: serde_json::json!({
                    "checkpoint_interval": config.checkpoint_interval,
                    "progress_interval": config.progress_interval,
                }),
            },
        );
        task.name = Some(format!("monte-carlo-worker-{}", worker_id));
        job.tasks.push(task.id);
    }

    job
}

// ============================================================================
// Main Entry Point
// ============================================================================

fn main() {
    println!("=== Marabunta Compute: Monte Carlo Pi Estimation ===\n");

    // Configuration for the simulation
    let config = MonteCarloConfig {
        total_samples: 10_000_000, // 10 million samples for quick demo
        base_seed: 42,
        checkpoint_interval: 1_000_000,
        progress_interval: 500_000,
    };

    println!("Configuration:");
    println!("  Total samples: {}", config.total_samples);
    println!("  Checkpoint interval: {}", config.checkpoint_interval);
    println!();

    // Run the simulation (single worker for local demo)
    println!("Running simulation...\n");
    match run_monte_carlo(config.clone()) {
        Ok(result) => {
            // For demo, aggregate a single worker result
            let final_result = aggregate_results(vec![result]);

            println!("=== Results ===");
            println!("Total samples:    {}", final_result.total_samples);
            println!("Points inside:    {}", final_result.total_inside);
            println!("Pi estimate:      {:.10}", final_result.pi_estimate);
            println!("Actual Pi:        {:.10}", std::f64::consts::PI);
            println!(
                "Relative error:   {:.6}%",
                final_result.relative_error * 100.0
            );
            println!("Elapsed time:     {} ms", final_result.total_elapsed_ms);
            println!();

            // Show JSON result
            println!("JSON Result:");
            println!("{}", serde_json::to_string_pretty(&final_result).unwrap());
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_simulation() {
        let config = MonteCarloConfig {
            total_samples: 10_000,
            base_seed: 42,
            checkpoint_interval: 5_000,
            progress_interval: 1_000,
        };

        let result = run_monte_carlo(config).expect("Simulation failed");
        assert_eq!(result.samples_processed, 10_000);
        // Pi estimate should be roughly in the right ballpark
        assert!(result.pi_estimate > 2.0 && result.pi_estimate < 4.0);
    }

    #[test]
    fn test_aggregate_results() {
        let results = vec![
            WorkerResult {
                worker_rank: 0,
                samples_processed: 1000,
                inside_count: 785,
                elapsed_ms: 10,
                pi_estimate: 3.14,
            },
            WorkerResult {
                worker_rank: 1,
                samples_processed: 1000,
                inside_count: 780,
                elapsed_ms: 12,
                pi_estimate: 3.12,
            },
        ];

        let final_result = aggregate_results(results);
        assert_eq!(final_result.total_samples, 2000);
        assert_eq!(final_result.total_inside, 1565);
        // (785 + 780) / 2000 * 4 = 3.13
        assert!((final_result.pi_estimate - 3.13).abs() < 0.01);
    }
}
