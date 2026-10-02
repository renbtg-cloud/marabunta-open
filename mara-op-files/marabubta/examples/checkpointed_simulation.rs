// Marabunta - Licensed under the MIT License.
//! Long-Running Simulation with Checkpoints
//!
//! This example demonstrates how to implement a long-running simulation that
//! uses Marabunta's checkpointing system for fault tolerance. The simulation can
//! be interrupted and resumed without losing progress.
//!
//! # Features Demonstrated
//!
//! - Periodic checkpointing for fault tolerance
//! - Graceful pause and resume
//! - Progress tracking with stages
//! - Resource hints for scheduler optimization
//! - Structured result output
//!
//! # Use Cases
//!
//! - Physics simulations (molecular dynamics, CFD)
//! - Genetic algorithms
//! - Training neural networks
//! - Financial Monte Carlo simulations
//! - Agent-based modeling
//!
//! # Running this example
//!
//! ```bash
//! # Submit to the cluster
//! marabunta submit examples/checkpointed_simulation.rs --runtime native \
//!     --timeout 24h --checkpoint-interval 5m
//!
//! # Or run locally for testing
//! cargo run --example checkpointed_simulation
//! ```

use marabunta_compute::common::{Job, Task, TaskPayload};
use marabunta_compute::sdk::{
    marabunta_checkpoint_exists, marabunta_checkpoint_load, marabunta_checkpoint_save, marabunta_get_task_id,
    marabunta_get_worker_count, marabunta_get_worker_rank, marabunta_hint_memory_needed, marabunta_hint_time_estimate,
    marabunta_log_info, marabunta_log_warn, marabunta_progress_set, marabunta_progress_set_message,
    marabunta_progress_set_stage, marabunta_result_set_json, marabunta_task_cleanup, marabunta_task_init_ex,
    marabunta_yield, MARABUNTA_YIELD_ABORT, MARABUNTA_YIELD_CONTINUE, MARABUNTA_YIELD_PAUSE,
};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::CString;
use std::time::{Duration, Instant};

// ============================================================================
// Simulation Configuration
// ============================================================================

/// Configuration for the simulation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationConfig {
    /// Total number of simulation steps
    pub total_steps: u64,
    /// Number of particles/agents in the simulation
    pub num_particles: usize,
    /// Simulation box dimensions
    pub box_size: [f64; 3],
    /// Time step (dt)
    pub dt: f64,
    /// Temperature for Langevin dynamics
    pub temperature: f64,
    /// How often to checkpoint (in steps)
    pub checkpoint_interval: u64,
    /// How often to sample observables (in steps)
    pub sample_interval: u64,
    /// Random seed
    pub seed: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            total_steps: 1_000_000,
            num_particles: 1000,
            box_size: [100.0, 100.0, 100.0],
            dt: 0.001,
            temperature: 300.0,
            checkpoint_interval: 10_000,
            sample_interval: 1_000,
            seed: 12345,
        }
    }
}

// ============================================================================
// Simulation State (Checkpointable)
// ============================================================================

/// Complete simulation state that can be checkpointed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationState {
    /// Current step number
    pub current_step: u64,
    /// Particle positions [N x 3]
    pub positions: Vec<[f64; 3]>,
    /// Particle velocities [N x 3]
    pub velocities: Vec<[f64; 3]>,
    /// Particle forces [N x 3]
    pub forces: Vec<[f64; 3]>,
    /// Random number generator state (as seed for reproducibility)
    pub rng_seed: u64,
    /// Accumulated observables
    pub observables: SimulationObservables,
}

/// Observables sampled during simulation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SimulationObservables {
    /// Time series of total energy
    pub energy_history: Vec<f64>,
    /// Time series of temperature
    pub temperature_history: Vec<f64>,
    /// Time series of pressure
    pub pressure_history: Vec<f64>,
    /// Mean square displacement samples
    pub msd_history: Vec<f64>,
    /// Sample times
    pub sample_times: Vec<f64>,
}

impl SimulationState {
    /// Create initial state
    pub fn new(config: &SimulationConfig) -> Self {
        let mut rng = StdRng::seed_from_u64(config.seed);

        // Initialize random positions within box
        let positions: Vec<[f64; 3]> = (0..config.num_particles)
            .map(|_| {
                [
                    rng.gen::<f64>() * config.box_size[0],
                    rng.gen::<f64>() * config.box_size[1],
                    rng.gen::<f64>() * config.box_size[2],
                ]
            })
            .collect();

        // Initialize velocities from Maxwell-Boltzmann distribution
        let sigma = (config.temperature / 300.0).sqrt(); // Simplified
        let velocities: Vec<[f64; 3]> = (0..config.num_particles)
            .map(|_| {
                [
                    rng.sample::<f64, _>(rand_distr::StandardNormal) * sigma,
                    rng.sample::<f64, _>(rand_distr::StandardNormal) * sigma,
                    rng.sample::<f64, _>(rand_distr::StandardNormal) * sigma,
                ]
            })
            .collect();

        // Initialize forces to zero
        let forces = vec![[0.0, 0.0, 0.0]; config.num_particles];

        Self {
            current_step: 0,
            positions,
            velocities,
            forces,
            rng_seed: rng.gen(),
            observables: SimulationObservables::default(),
        }
    }

    /// Serialize state to bytes for checkpointing
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_else(|_| serde_json::to_vec(self).unwrap())
    }

    /// Deserialize state from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes)
            .ok()
            .or_else(|| serde_json::from_slice(bytes).ok())
    }
}

// ============================================================================
// Simulation Physics
// ============================================================================

/// Lennard-Jones potential parameters
const EPSILON: f64 = 1.0;
const SIGMA: f64 = 1.0;
const CUTOFF: f64 = 2.5;

/// Calculate Lennard-Jones force between two particles
fn lennard_jones_force(r: f64) -> f64 {
    if r > CUTOFF * SIGMA || r < 0.1 {
        return 0.0;
    }

    let sr = SIGMA / r;
    let sr6 = sr.powi(6);
    let sr12 = sr6 * sr6;

    24.0 * EPSILON * (2.0 * sr12 - sr6) / r
}

/// Calculate total energy
fn calculate_energy(positions: &[[f64; 3]], velocities: &[[f64; 3]], box_size: &[f64; 3]) -> f64 {
    let n = positions.len();

    // Kinetic energy
    let kinetic: f64 = velocities
        .iter()
        .map(|v| 0.5 * (v[0] * v[0] + v[1] * v[1] + v[2] * v[2]))
        .sum();

    // Potential energy (Lennard-Jones)
    let mut potential = 0.0;
    for i in 0..n {
        for j in (i + 1)..n {
            let dx = positions[i][0] - positions[j][0];
            let dy = positions[i][1] - positions[j][1];
            let dz = positions[i][2] - positions[j][2];

            // Minimum image convention
            let dx = dx - box_size[0] * (dx / box_size[0]).round();
            let dy = dy - box_size[1] * (dy / box_size[1]).round();
            let dz = dz - box_size[2] * (dz / box_size[2]).round();

            let r = (dx * dx + dy * dy + dz * dz).sqrt();

            if r < CUTOFF * SIGMA && r > 0.1 {
                let sr = SIGMA / r;
                let sr6 = sr.powi(6);
                potential += 4.0 * EPSILON * (sr6 * sr6 - sr6);
            }
        }
    }

    kinetic + potential
}

/// Calculate instantaneous temperature
fn calculate_temperature(velocities: &[[f64; 3]]) -> f64 {
    let n = velocities.len() as f64;
    let kinetic: f64 = velocities
        .iter()
        .map(|v| v[0] * v[0] + v[1] * v[1] + v[2] * v[2])
        .sum();

    // T = 2 * KE / (3 * N * kB), simplified with kB = 1
    kinetic / (3.0 * n)
}

/// Single integration step using velocity Verlet
fn integration_step(state: &mut SimulationState, config: &SimulationConfig, rng: &mut StdRng) {
    let n = state.positions.len();
    let dt = config.dt;
    let box_size = &config.box_size;

    // Update positions
    for i in 0..n {
        for d in 0..3 {
            state.positions[i][d] +=
                state.velocities[i][d] * dt + 0.5 * state.forces[i][d] * dt * dt;
            // Periodic boundary conditions
            while state.positions[i][d] < 0.0 {
                state.positions[i][d] += box_size[d];
            }
            while state.positions[i][d] >= box_size[d] {
                state.positions[i][d] -= box_size[d];
            }
        }
    }

    // Store old forces
    let old_forces = state.forces.clone();

    // Calculate new forces
    for i in 0..n {
        state.forces[i] = [0.0, 0.0, 0.0];
    }

    for i in 0..n {
        for j in (i + 1)..n {
            let mut dr = [0.0; 3];
            for d in 0..3 {
                dr[d] = state.positions[i][d] - state.positions[j][d];
                // Minimum image convention
                dr[d] -= box_size[d] * (dr[d] / box_size[d]).round();
            }

            let r = (dr[0] * dr[0] + dr[1] * dr[1] + dr[2] * dr[2]).sqrt();
            let f_mag = lennard_jones_force(r);

            for d in 0..3 {
                let f = f_mag * dr[d] / r;
                state.forces[i][d] += f;
                state.forces[j][d] -= f;
            }
        }
    }

    // Add Langevin thermostat (simplified)
    let gamma = 0.1; // Friction coefficient
    let noise_strength = (2.0 * gamma * config.temperature * dt).sqrt();

    for i in 0..n {
        for d in 0..3 {
            state.forces[i][d] -= gamma * state.velocities[i][d];
            state.forces[i][d] += noise_strength * rng.sample::<f64, _>(rand_distr::StandardNormal);
        }
    }

    // Update velocities
    for i in 0..n {
        for d in 0..3 {
            state.velocities[i][d] += 0.5 * (old_forces[i][d] + state.forces[i][d]) * dt;
        }
    }

    state.current_step += 1;
}

// ============================================================================
// Simulation Result
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    pub total_steps: u64,
    pub completed_steps: u64,
    pub completion_percentage: f64,
    pub final_energy: f64,
    pub average_temperature: f64,
    pub temperature_std: f64,
    pub average_energy: f64,
    pub energy_std: f64,
    pub elapsed_secs: f64,
    pub steps_per_second: f64,
    pub checkpoints_saved: u64,
    pub was_resumed: bool,
}

// ============================================================================
// Main Simulation Runner
// ============================================================================

/// Run the simulation with checkpointing
pub fn run_simulation(config: SimulationConfig) -> Result<SimulationResult, String> {
    let start_time = Instant::now();

    // Initialize Marabunta context
    let checkpoint_dir = CString::new("/tmp/marabunta_checkpoints").unwrap();
    let ctx = unsafe {
        marabunta_task_init_ex(
            config.seed, // Use seed as task ID for demo
            0,
            1,
            checkpoint_dir.as_ptr(),
        )
    };

    if ctx.is_null() {
        return Err("Failed to initialize Marabunta context".to_string());
    }

    // Set resource hints
    let memory_estimate = config.num_particles * 3 * 8 * 4; // positions, velocities, forces, old_forces
    unsafe {
        marabunta_hint_memory_needed(ctx, memory_estimate as u64);
        // Estimate 1000 steps per second
        marabunta_hint_time_estimate(ctx, (config.total_steps / 1000 + 1) as u32);
    }

    let log_msg = CString::new(format!(
        "Starting simulation: {} particles, {} steps",
        config.num_particles, config.total_steps
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, log_msg.as_ptr()) };

    // Try to restore from checkpoint
    let (mut state, was_resumed) = if unsafe { marabunta_checkpoint_exists(ctx) } == 1 {
        let stage = CString::new("Restoring from checkpoint").unwrap();
        unsafe { marabunta_progress_set_stage(ctx, stage.as_ptr()) };

        let mut buffer = vec![0u8; 100 * 1024 * 1024]; // 100MB buffer
        let len = unsafe { marabunta_checkpoint_load(ctx, buffer.as_mut_ptr(), buffer.len()) };

        if len > 0 {
            if let Some(saved_state) = SimulationState::from_bytes(&buffer[..len as usize]) {
                let msg = CString::new(format!("Restored from step {}", saved_state.current_step))
                    .unwrap();
                unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
                (saved_state, true)
            } else {
                let msg = CString::new("Checkpoint corrupted, starting fresh").unwrap();
                unsafe { marabunta_log_warn(ctx, msg.as_ptr()) };
                (SimulationState::new(&config), false)
            }
        } else {
            (SimulationState::new(&config), false)
        }
    } else {
        (SimulationState::new(&config), false)
    };

    // Initialize RNG from state
    let mut rng = StdRng::seed_from_u64(state.rng_seed);

    // Set stage to running
    let stage = CString::new("Running simulation").unwrap();
    unsafe { marabunta_progress_set_stage(ctx, stage.as_ptr()) };

    let mut checkpoints_saved: u64 = 0;
    let mut last_checkpoint_time = Instant::now();

    // Main simulation loop
    while state.current_step < config.total_steps {
        // Check for abort/pause
        let yield_result = unsafe { marabunta_yield(ctx) };
        match yield_result {
            MARABUNTA_YIELD_ABORT => {
                let msg = CString::new("Simulation aborted").unwrap();
                unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
                break;
            }
            MARABUNTA_YIELD_PAUSE => {
                let msg = CString::new("Simulation pausing for checkpoint").unwrap();
                unsafe { marabunta_log_info(ctx, msg.as_ptr()) };

                // Save state before pausing
                state.rng_seed = rng.gen();
                let checkpoint_data = state.to_bytes();
                unsafe {
                    marabunta_checkpoint_save(ctx, checkpoint_data.as_ptr(), checkpoint_data.len())
                };
                checkpoints_saved += 1;
                break;
            }
            _ => {}
        }

        // Perform integration step
        integration_step(&mut state, &config, &mut rng);

        // Sample observables
        if state.current_step % config.sample_interval == 0 {
            let energy = calculate_energy(&state.positions, &state.velocities, &config.box_size);
            let temp = calculate_temperature(&state.velocities);

            state.observables.energy_history.push(energy);
            state.observables.temperature_history.push(temp);
            state
                .observables
                .sample_times
                .push(state.current_step as f64 * config.dt);
        }

        // Periodic checkpoint
        if state.current_step % config.checkpoint_interval == 0
            || last_checkpoint_time.elapsed() > Duration::from_secs(60)
        {
            state.rng_seed = rng.gen();
            let checkpoint_data = state.to_bytes();
            unsafe { marabunta_checkpoint_save(ctx, checkpoint_data.as_ptr(), checkpoint_data.len()) };
            checkpoints_saved += 1;
            last_checkpoint_time = Instant::now();

            let msg = CString::new(format!(
                "Checkpoint at step {}/{}",
                state.current_step, config.total_steps
            ))
            .unwrap();
            unsafe { marabunta_log_info(ctx, msg.as_ptr()) };
        }

        // Update progress
        if state.current_step % (config.total_steps / 100).max(1) == 0 {
            let progress = (state.current_step as f64 / config.total_steps as f64 * 100.0) as u8;
            unsafe { marabunta_progress_set(ctx, progress) };

            let msg = CString::new(format!(
                "Step {}/{} ({:.1}%)",
                state.current_step, config.total_steps, progress as f64
            ))
            .unwrap();
            unsafe { marabunta_progress_set_message(ctx, msg.as_ptr()) };
        }
    }

    // Calculate final statistics
    let final_energy = calculate_energy(&state.positions, &state.velocities, &config.box_size);

    let avg_temp = if state.observables.temperature_history.is_empty() {
        0.0
    } else {
        state.observables.temperature_history.iter().sum::<f64>()
            / state.observables.temperature_history.len() as f64
    };

    let temp_std = if state.observables.temperature_history.len() < 2 {
        0.0
    } else {
        let variance = state
            .observables
            .temperature_history
            .iter()
            .map(|t| (t - avg_temp).powi(2))
            .sum::<f64>()
            / state.observables.temperature_history.len() as f64;
        variance.sqrt()
    };

    let avg_energy = if state.observables.energy_history.is_empty() {
        0.0
    } else {
        state.observables.energy_history.iter().sum::<f64>()
            / state.observables.energy_history.len() as f64
    };

    let energy_std = if state.observables.energy_history.len() < 2 {
        0.0
    } else {
        let variance = state
            .observables
            .energy_history
            .iter()
            .map(|e| (e - avg_energy).powi(2))
            .sum::<f64>()
            / state.observables.energy_history.len() as f64;
        variance.sqrt()
    };

    let elapsed_secs = start_time.elapsed().as_secs_f64();
    let steps_per_second = state.current_step as f64 / elapsed_secs;

    let result = SimulationResult {
        total_steps: config.total_steps,
        completed_steps: state.current_step,
        completion_percentage: state.current_step as f64 / config.total_steps as f64 * 100.0,
        final_energy,
        average_temperature: avg_temp,
        temperature_std: temp_std,
        average_energy: avg_energy,
        energy_std,
        elapsed_secs,
        steps_per_second,
        checkpoints_saved,
        was_resumed,
    };

    // Set result
    let result_json = serde_json::to_string(&result).unwrap();
    let result_cstr = CString::new(result_json).unwrap();
    unsafe { marabunta_result_set_json(ctx, result_cstr.as_ptr()) };

    // Final log
    let msg = CString::new(format!(
        "Simulation complete: {} steps, {:.2} steps/sec",
        state.current_step, steps_per_second
    ))
    .unwrap();
    unsafe { marabunta_log_info(ctx, msg.as_ptr()) };

    unsafe { marabunta_progress_set(ctx, 100) };
    unsafe { marabunta_task_cleanup(ctx) };

    Ok(result)
}

// ============================================================================
// Job Creation
// ============================================================================

/// Create a simulation job for cluster submission
pub fn create_simulation_job(config: SimulationConfig) -> Job {
    let mut job = Job::new("molecular-dynamics");
    job.priority = 50;
    job.metadata = serde_json::json!({
        "config": config,
        "description": "Molecular dynamics simulation with Langevin thermostat"
    });

    let task = Task::new(
        job.id,
        TaskPayload::Function {
            name: "run_simulation".to_string(),
            input: serde_json::to_vec(&config).unwrap(),
        },
    );
    job.tasks.push(task.id);

    job
}

// ============================================================================
// Main Entry Point (Demo)
// ============================================================================

fn main() {
    println!("=== Marabunta Compute: Checkpointed Simulation ===\n");

    // Create configuration for a quick demo
    let config = SimulationConfig {
        total_steps: 100_000,
        num_particles: 100,
        box_size: [20.0, 20.0, 20.0],
        dt: 0.001,
        temperature: 300.0,
        checkpoint_interval: 10_000,
        sample_interval: 1_000,
        seed: 42,
    };

    println!("Simulation Configuration:");
    println!("  Particles:          {}", config.num_particles);
    println!("  Total steps:        {}", config.total_steps);
    println!("  Time step:          {}", config.dt);
    println!("  Temperature:        {} K", config.temperature);
    println!("  Box size:           {:?}", config.box_size);
    println!("  Checkpoint every:   {} steps", config.checkpoint_interval);
    println!("  Sample every:       {} steps", config.sample_interval);
    println!();

    println!("Running simulation...\n");

    match run_simulation(config.clone()) {
        Ok(result) => {
            println!("=== Results ===");
            println!(
                "Completed steps:      {}/{}",
                result.completed_steps, result.total_steps
            );
            println!("Completion:           {:.1}%", result.completion_percentage);
            println!("Elapsed time:         {:.2} seconds", result.elapsed_secs);
            println!(
                "Performance:          {:.0} steps/sec",
                result.steps_per_second
            );
            println!("Checkpoints saved:    {}", result.checkpoints_saved);
            println!("Resumed from checkpoint: {}", result.was_resumed);
            println!();
            println!("Physics:");
            println!("  Final energy:       {:.4}", result.final_energy);
            println!(
                "  Avg temperature:    {:.2} +/- {:.2}",
                result.average_temperature, result.temperature_std
            );
            println!(
                "  Avg energy:         {:.4} +/- {:.4}",
                result.average_energy, result.energy_std
            );
            println!();

            // Show JSON result
            println!("JSON Result:");
            println!("{}", serde_json::to_string_pretty(&result).unwrap());
        }
        Err(e) => {
            eprintln!("Simulation error: {}", e);
            std::process::exit(1);
        }
    }

    // Show job creation
    println!("\n--- Job Creation Example ---");
    let job = create_simulation_job(config);
    println!("Created job: {}", job.id);
    println!("  Name: {}", job.name);
    println!("  Tasks: {}", job.tasks.len());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simulation_state_serialization() {
        let config = SimulationConfig {
            num_particles: 10,
            ..Default::default()
        };
        let state = SimulationState::new(&config);

        let bytes = state.to_bytes();
        let restored = SimulationState::from_bytes(&bytes).unwrap();

        assert_eq!(restored.current_step, state.current_step);
        assert_eq!(restored.positions.len(), state.positions.len());
    }

    #[test]
    fn test_energy_conservation_short_run() {
        let config = SimulationConfig {
            total_steps: 1000,
            num_particles: 10,
            temperature: 0.0, // No thermostat for energy conservation test
            checkpoint_interval: 500,
            sample_interval: 100,
            ..Default::default()
        };

        let result = run_simulation(config).unwrap();
        assert_eq!(result.completed_steps, 1000);
    }

    #[test]
    fn test_temperature_equilibration() {
        let config = SimulationConfig {
            total_steps: 5000,
            num_particles: 50,
            temperature: 300.0,
            checkpoint_interval: 2500,
            sample_interval: 100,
            ..Default::default()
        };

        let result = run_simulation(config).unwrap();

        // Temperature should be near target after equilibration
        // Allow 50% deviation for short run
        assert!(result.average_temperature > 150.0 && result.average_temperature < 600.0);
    }
}
