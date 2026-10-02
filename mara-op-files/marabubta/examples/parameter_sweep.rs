// Marabunta - Licensed under the MIT License.
//! Parameter Sweep Example
//!
//! This example demonstrates how to perform a parameter sweep using Marabunta Compute.
//! Parameter sweeps are common in scientific computing, machine learning hyperparameter
//! tuning, and sensitivity analysis.
//!
//! # Use Cases
//!
//! - Machine learning hyperparameter optimization
//! - Scientific simulation with varying parameters
//! - Performance benchmarking across configurations
//! - Sensitivity analysis
//!
//! # How it works
//!
//! 1. Define a parameter space (ranges for each parameter)
//! 2. Generate all combinations (Cartesian product) or sample points
//! 3. Each worker evaluates the objective function for a subset of points
//! 4. Results are collected and analyzed to find optimal parameters
//!
//! # Running this example
//!
//! ```bash
//! # Submit to the cluster
//! marabunta submit examples/parameter_sweep.rs --runtime native --workers 64
//!
//! # Or run locally for testing
//! cargo run --example parameter_sweep
//! ```

use marabunta_compute::common::{Job, Task, TaskPayload};
use marabunta_compute::sdk::{
    marabunta_get_worker_count, marabunta_get_worker_rank, marabunta_log_info, marabunta_progress_set,
    marabunta_progress_set_stage, marabunta_result_set_json, marabunta_task_cleanup, marabunta_task_init_ex,
    marabunta_yield, MARABUNTA_YIELD_ABORT, MARABUNTA_YIELD_CONTINUE,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::CString;

// ============================================================================
// Parameter Space Definition
// ============================================================================

/// A single parameter dimension
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterRange {
    /// Continuous range with linear sampling
    Linear { min: f64, max: f64, steps: usize },
    /// Continuous range with logarithmic sampling (for learning rates, etc.)
    Log { min: f64, max: f64, steps: usize },
    /// Discrete set of values
    Discrete(Vec<f64>),
    /// Categorical values (mapped to indices)
    Categorical(Vec<String>),
}

impl ParameterRange {
    /// Get all values for this parameter range
    pub fn values(&self) -> Vec<f64> {
        match self {
            ParameterRange::Linear { min, max, steps } => {
                if *steps == 1 {
                    vec![*min]
                } else {
                    (0..*steps)
                        .map(|i| min + (max - min) * i as f64 / (*steps - 1) as f64)
                        .collect()
                }
            }
            ParameterRange::Log { min, max, steps } => {
                if *steps == 1 {
                    vec![*min]
                } else {
                    let log_min = min.ln();
                    let log_max = max.ln();
                    (0..*steps)
                        .map(|i| {
                            (log_min + (log_max - log_min) * i as f64 / (*steps - 1) as f64).exp()
                        })
                        .collect()
                }
            }
            ParameterRange::Discrete(values) => values.clone(),
            ParameterRange::Categorical(values) => (0..values.len()).map(|i| i as f64).collect(),
        }
    }

    /// Number of values in this range
    pub fn count(&self) -> usize {
        match self {
            ParameterRange::Linear { steps, .. } => *steps,
            ParameterRange::Log { steps, .. } => *steps,
            ParameterRange::Discrete(values) => values.len(),
            ParameterRange::Categorical(values) => values.len(),
        }
    }
}

/// Complete parameter space definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterSpace {
    /// Named parameters and their ranges
    pub parameters: HashMap<String, ParameterRange>,
    /// Order of parameters (for consistent iteration)
    pub order: Vec<String>,
}

impl ParameterSpace {
    pub fn new() -> Self {
        Self {
            parameters: HashMap::new(),
            order: Vec::new(),
        }
    }

    /// Add a parameter to the space
    pub fn add_parameter(mut self, name: impl Into<String>, range: ParameterRange) -> Self {
        let name = name.into();
        self.parameters.insert(name.clone(), range);
        self.order.push(name);
        self
    }

    /// Total number of combinations
    pub fn total_combinations(&self) -> usize {
        self.order
            .iter()
            .map(|name| self.parameters.get(name).unwrap().count())
            .product()
    }

    /// Generate all parameter combinations
    pub fn all_combinations(&self) -> Vec<HashMap<String, f64>> {
        let mut result = vec![HashMap::new()];

        for name in &self.order {
            let range = self.parameters.get(name).unwrap();
            let values = range.values();

            let mut new_result = Vec::new();
            for existing in result {
                for value in &values {
                    let mut combo = existing.clone();
                    combo.insert(name.clone(), *value);
                    new_result.push(combo);
                }
            }
            result = new_result;
        }

        result
    }

    /// Get combinations assigned to a specific worker
    pub fn combinations_for_worker(
        &self,
        worker_rank: u32,
        worker_count: u32,
    ) -> Vec<HashMap<String, f64>> {
        let all = self.all_combinations();
        let chunk_size = (all.len() + worker_count as usize - 1) / worker_count as usize;
        let start = worker_rank as usize * chunk_size;
        let end = (start + chunk_size).min(all.len());

        if start >= all.len() {
            Vec::new()
        } else {
            all[start..end].to_vec()
        }
    }
}

impl Default for ParameterSpace {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Evaluation Result Types
// ============================================================================

/// Result from evaluating a single parameter combination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationResult {
    /// The parameter values tested
    pub parameters: HashMap<String, f64>,
    /// Primary objective value (to minimize or maximize)
    pub objective: f64,
    /// Secondary metrics
    pub metrics: HashMap<String, f64>,
    /// Evaluation time in milliseconds
    pub elapsed_ms: u64,
    /// Worker that ran this evaluation
    pub worker_rank: u32,
}

/// Results from a worker (multiple evaluations)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSweepResult {
    pub worker_rank: u32,
    pub evaluations: Vec<EvaluationResult>,
    pub total_elapsed_ms: u64,
}

/// Final aggregated sweep results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepResult {
    /// All evaluation results
    pub all_results: Vec<EvaluationResult>,
    /// Best result (minimum objective)
    pub best_result: Option<EvaluationResult>,
    /// Worst result (maximum objective)
    pub worst_result: Option<EvaluationResult>,
    /// Summary statistics
    pub summary: SweepSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepSummary {
    pub total_evaluations: usize,
    pub mean_objective: f64,
    pub std_objective: f64,
    pub min_objective: f64,
    pub max_objective: f64,
    pub total_elapsed_ms: u64,
}

// ============================================================================
// Example Objective Functions
// ============================================================================

/// Rosenbrock function - a classic optimization test function
///
/// f(x, y) = (a - x)^2 + b * (y - x^2)^2
/// Global minimum at (a, a^2) with f = 0
pub fn rosenbrock(params: &HashMap<String, f64>) -> f64 {
    let x = params.get("x").copied().unwrap_or(0.0);
    let y = params.get("y").copied().unwrap_or(0.0);
    let a = 1.0;
    let b = 100.0;

    (a - x).powi(2) + b * (y - x.powi(2)).powi(2)
}

/// Rastrigin function - multimodal test function
///
/// f(x) = An + sum(x_i^2 - A*cos(2*pi*x_i))
/// Global minimum at origin with f = 0
pub fn rastrigin(params: &HashMap<String, f64>) -> f64 {
    let a = 10.0;
    let n = params.len() as f64;
    let sum: f64 = params
        .values()
        .map(|&x| x.powi(2) - a * (2.0 * std::f64::consts::PI * x).cos())
        .sum();

    a * n + sum
}

/// Simulated ML hyperparameter evaluation
///
/// This simulates finding optimal hyperparameters for a model
pub fn simulated_ml_objective(params: &HashMap<String, f64>) -> f64 {
    let learning_rate = params.get("learning_rate").copied().unwrap_or(0.01);
    let hidden_units = params.get("hidden_units").copied().unwrap_or(64.0);
    let dropout = params.get("dropout").copied().unwrap_or(0.1);
    let batch_size = params.get("batch_size").copied().unwrap_or(32.0);

    // Simulate validation loss based on hyperparameters
    // In reality, this would train a model and return validation loss
    let base_loss = 0.5;

    // Learning rate effect (too high or too low is bad)
    let lr_effect = if learning_rate < 0.0001 {
        0.3 // Too slow
    } else if learning_rate > 0.1 {
        0.4 // Too fast, unstable
    } else {
        -0.1 * (learning_rate.ln() + 3.0).abs() // Sweet spot around 0.001
    };

    // Hidden units effect (diminishing returns)
    let units_effect = -0.05 * (hidden_units / 64.0).ln().max(-1.0);

    // Dropout effect (some is good, too much is bad)
    let dropout_effect = if dropout < 0.1 {
        0.1 // Underfitting
    } else if dropout > 0.5 {
        0.15 // Too much dropout
    } else {
        -0.05 // Good range
    };

    // Batch size effect
    let batch_effect = 0.02 * (batch_size / 32.0 - 1.0).abs();

    // Add some noise to simulate stochastic training
    let noise = (learning_rate * 1000.0 + hidden_units + dropout * 100.0 + batch_size).sin() * 0.02;

    (base_loss + lr_effect + units_effect + dropout_effect + batch_effect + noise).max(0.01)
}

// ============================================================================
// Parameter Sweep Execution
// ============================================================================

/// Run parameter sweep for assigned combinations
pub fn run_parameter_sweep<F>(
    space: &ParameterSpace,
    objective_fn: F,
) -> Result<WorkerSweepResult, String>
where
    F: Fn(&HashMap<String, f64>) -> f64,
{
    let start = std::time::Instant::now();

    // Initialize Marabunta context
    let checkpoint_dir = CString::new("/tmp/marabunta_checkpoints").unwrap();
    let ctx = unsafe { marabunta_task_init_ex(1, 0, 1, checkpoint_dir.as_ptr()) };

    if ctx.is_null() {
        return Err("Failed to initialize Marabunta context".to_string());
    }

    let worker_rank = unsafe { marabunta_get_worker_rank(ctx) };
    let worker_count = unsafe { marabunta_get_worker_count(ctx) };

    // Set stage
    let stage = CString::new("Running parameter sweep").unwrap();
    unsafe { marabunta_progress_set_stage(ctx, stage.as_ptr()) };

    // Get combinations for this worker
    let combinations = space.combinations_for_worker(worker_rank, worker_count);
    let total = combinations.len();

    let log_msg = CString::new(format!(
        "Worker {}/{} evaluating {} parameter combinations",
        worker_rank, worker_count, total
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, log_msg.as_ptr()) };

    let mut evaluations = Vec::new();

    for (i, params) in combinations.iter().enumerate() {
        // Check for abort
        let yield_result = unsafe { marabunta_yield(ctx) };
        if yield_result == MARABUNTA_YIELD_ABORT {
            let msg = CString::new("Sweep aborted").unwrap();
            unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
            break;
        }

        let eval_start = std::time::Instant::now();

        // Evaluate objective function
        let objective = objective_fn(params);

        let eval = EvaluationResult {
            parameters: params.clone(),
            objective,
            metrics: HashMap::new(),
            elapsed_ms: eval_start.elapsed().as_millis() as u64,
            worker_rank,
        };
        evaluations.push(eval);

        // Update progress
        let progress = ((i + 1) as f64 / total as f64 * 100.0) as u8;
        unsafe { marabunta_progress_set(ctx, progress) };
    }

    let result = WorkerSweepResult {
        worker_rank,
        evaluations,
        total_elapsed_ms: start.elapsed().as_millis() as u64,
    };

    // Set result
    let result_json = serde_json::to_string(&result).unwrap();
    let result_cstr = CString::new(result_json).unwrap();
    unsafe { marabunta_result_set_json(ctx, result_cstr.as_ptr()) };

    let log_msg = CString::new(format!(
        "Worker {} completed {} evaluations in {} ms",
        worker_rank,
        result.evaluations.len(),
        result.total_elapsed_ms
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, log_msg.as_ptr()) };

    unsafe { marabunta_progress_set(ctx, 100) };
    unsafe { marabunta_task_cleanup(ctx) };

    Ok(result)
}

/// Aggregate results from all workers
pub fn aggregate_sweep_results(worker_results: Vec<WorkerSweepResult>) -> SweepResult {
    let all_results: Vec<EvaluationResult> = worker_results
        .iter()
        .flat_map(|w| w.evaluations.clone())
        .collect();

    let total_elapsed: u64 = worker_results
        .iter()
        .map(|w| w.total_elapsed_ms)
        .max()
        .unwrap_or(0);

    // Find best and worst
    let best_result = all_results
        .iter()
        .min_by(|a, b| a.objective.partial_cmp(&b.objective).unwrap())
        .cloned();

    let worst_result = all_results
        .iter()
        .max_by(|a, b| a.objective.partial_cmp(&b.objective).unwrap())
        .cloned();

    // Calculate summary statistics
    let objectives: Vec<f64> = all_results.iter().map(|r| r.objective).collect();
    let n = objectives.len() as f64;
    let mean = objectives.iter().sum::<f64>() / n;
    let variance = objectives.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    let std = variance.sqrt();
    let min = objectives.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = objectives.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    let summary = SweepSummary {
        total_evaluations: all_results.len(),
        mean_objective: mean,
        std_objective: std,
        min_objective: min,
        max_objective: max,
        total_elapsed_ms: total_elapsed,
    };

    SweepResult {
        all_results,
        best_result,
        worst_result,
        summary,
    }
}

// ============================================================================
// Job Creation Helpers
// ============================================================================

/// Create a parameter sweep job for cluster submission
pub fn create_sweep_job(space: ParameterSpace, num_workers: u32) -> Job {
    let mut job = Job::new("parameter-sweep");
    job.priority = 100;
    job.metadata = serde_json::json!({
        "parameter_space": space,
        "total_combinations": space.total_combinations(),
        "num_workers": num_workers,
    });

    // Create tasks for each worker
    for worker_id in 0..num_workers {
        let mut task = Task::new(
            job.id,
            TaskPayload::ParameterSweep {
                param_name: "worker_id".to_string(),
                param_value: serde_json::json!(worker_id),
                base_config: serde_json::json!(space),
            },
        );
        task.name = Some(format!("sweep-worker-{}", worker_id));
        job.tasks.push(task.id);
    }

    job
}

// ============================================================================
// Main Entry Point (Demo)
// ============================================================================

fn main() {
    println!("=== Marabunta Compute: Parameter Sweep ===\n");

    // Example 1: Rosenbrock function optimization
    println!("--- Example 1: Rosenbrock Function ---");
    let rosenbrock_space = ParameterSpace::new()
        .add_parameter(
            "x",
            ParameterRange::Linear {
                min: -2.0,
                max: 2.0,
                steps: 21,
            },
        )
        .add_parameter(
            "y",
            ParameterRange::Linear {
                min: -2.0,
                max: 2.0,
                steps: 21,
            },
        );

    println!(
        "Parameter space: {} combinations",
        rosenbrock_space.total_combinations()
    );

    match run_parameter_sweep(&rosenbrock_space, rosenbrock) {
        Ok(worker_result) => {
            let result = aggregate_sweep_results(vec![worker_result]);
            println!("\nResults:");
            println!("  Total evaluations: {}", result.summary.total_evaluations);
            println!("  Mean objective:    {:.6}", result.summary.mean_objective);
            println!("  Min objective:     {:.6}", result.summary.min_objective);
            println!("  Max objective:     {:.6}", result.summary.max_objective);

            if let Some(best) = &result.best_result {
                println!("\nBest parameters found:");
                println!("  x = {:.4}", best.parameters.get("x").unwrap());
                println!("  y = {:.4}", best.parameters.get("y").unwrap());
                println!("  f(x,y) = {:.6}", best.objective);
                println!("  (Global minimum is at (1, 1) with f = 0)");
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    println!("\n");

    // Example 2: ML Hyperparameter Tuning
    println!("--- Example 2: ML Hyperparameter Tuning ---");
    let ml_space = ParameterSpace::new()
        .add_parameter(
            "learning_rate",
            ParameterRange::Log {
                min: 0.0001,
                max: 0.1,
                steps: 10,
            },
        )
        .add_parameter(
            "hidden_units",
            ParameterRange::Discrete(vec![32.0, 64.0, 128.0, 256.0]),
        )
        .add_parameter(
            "dropout",
            ParameterRange::Linear {
                min: 0.0,
                max: 0.5,
                steps: 6,
            },
        )
        .add_parameter(
            "batch_size",
            ParameterRange::Discrete(vec![16.0, 32.0, 64.0, 128.0]),
        );

    println!(
        "Parameter space: {} combinations",
        ml_space.total_combinations()
    );

    match run_parameter_sweep(&ml_space, simulated_ml_objective) {
        Ok(worker_result) => {
            let result = aggregate_sweep_results(vec![worker_result]);
            println!("\nResults:");
            println!("  Total evaluations: {}", result.summary.total_evaluations);
            println!(
                "  Validation loss: {:.4} +/- {:.4}",
                result.summary.mean_objective, result.summary.std_objective
            );

            if let Some(best) = &result.best_result {
                println!("\nBest hyperparameters found:");
                println!(
                    "  learning_rate = {:.6}",
                    best.parameters.get("learning_rate").unwrap()
                );
                println!(
                    "  hidden_units  = {:.0}",
                    best.parameters.get("hidden_units").unwrap()
                );
                println!(
                    "  dropout       = {:.2}",
                    best.parameters.get("dropout").unwrap()
                );
                println!(
                    "  batch_size    = {:.0}",
                    best.parameters.get("batch_size").unwrap()
                );
                println!("  validation_loss = {:.4}", best.objective);
            }

            // Show top 5 configurations
            println!("\nTop 5 configurations:");
            let mut sorted = result.all_results.clone();
            sorted.sort_by(|a, b| a.objective.partial_cmp(&b.objective).unwrap());
            for (i, r) in sorted.iter().take(5).enumerate() {
                println!(
                    "  {}. loss={:.4}, lr={:.5}, units={:.0}, dropout={:.2}, batch={:.0}",
                    i + 1,
                    r.objective,
                    r.parameters.get("learning_rate").unwrap(),
                    r.parameters.get("hidden_units").unwrap(),
                    r.parameters.get("dropout").unwrap(),
                    r.parameters.get("batch_size").unwrap()
                );
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    println!("\n");

    // Show how to create a job for cluster submission
    println!("--- Job Creation Example ---");
    let job = create_sweep_job(ml_space, 8);
    println!("Created job: {}", job.id);
    println!("  Name: {}", job.name);
    println!("  Tasks: {}", job.tasks.len());
    println!(
        "  Metadata: {}",
        serde_json::to_string_pretty(&job.metadata).unwrap()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_range() {
        let range = ParameterRange::Linear {
            min: 0.0,
            max: 10.0,
            steps: 5,
        };
        let values = range.values();
        assert_eq!(values, vec![0.0, 2.5, 5.0, 7.5, 10.0]);
    }

    #[test]
    fn test_log_range() {
        let range = ParameterRange::Log {
            min: 0.001,
            max: 1.0,
            steps: 4,
        };
        let values = range.values();
        assert_eq!(values.len(), 4);
        assert!((values[0] - 0.001).abs() < 1e-10);
        assert!((values[3] - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_parameter_space_combinations() {
        let space = ParameterSpace::new()
            .add_parameter("a", ParameterRange::Discrete(vec![1.0, 2.0]))
            .add_parameter("b", ParameterRange::Discrete(vec![10.0, 20.0, 30.0]));

        assert_eq!(space.total_combinations(), 6);

        let combos = space.all_combinations();
        assert_eq!(combos.len(), 6);
    }

    #[test]
    fn test_rosenbrock_minimum() {
        let mut params = HashMap::new();
        params.insert("x".to_string(), 1.0);
        params.insert("y".to_string(), 1.0);

        let result = rosenbrock(&params);
        assert!((result - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_combinations_for_worker() {
        let space = ParameterSpace::new()
            .add_parameter("x", ParameterRange::Discrete(vec![1.0, 2.0, 3.0, 4.0, 5.0]));

        // 5 combinations, 2 workers
        let worker0 = space.combinations_for_worker(0, 2);
        let worker1 = space.combinations_for_worker(1, 2);

        assert_eq!(worker0.len() + worker1.len(), 5);
    }
}
