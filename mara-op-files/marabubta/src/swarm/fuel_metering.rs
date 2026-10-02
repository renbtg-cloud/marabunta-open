// Marabunta - Licensed under the MIT License.
//! Pillar 11.1: Fuel Metering
//!
//! Provides deterministic instruction-counting and fuel consumption tracking
//! during WASM execution to ensure fair-market billing against the Energy Oracle.

use wasmtime::Store;
use tracing::{warn, info};

pub struct FuelMeter {
    total_fuel: u64,
}

impl FuelMeter {
    pub fn new(initial_fuel: u64) -> Self {
        Self { total_fuel: initial_fuel }
    }

    /// Configures the WASM store to track execution fuel and enforce strict bounds.
    pub fn inject_into_store<T>(&self, store: &mut Store<T>) {
        // Enforce the maximum fuel limit. If this limit is hit, wasmtime will TRAP.
        if let Err(e) = store.set_fuel(self.total_fuel) {
            warn!("Failed to inject fuel limit into execution store: {}", e);
        } else {
            info!("FuelMeter: injected {} limits into execution store.", self.total_fuel);
        }
    }

    /// Calculates the actual energy consumed (in Joules) based on instruction count.
    /// Assumes an average instruction cost (e.g., 0.5 nJ per operation on consumer CPUs).
    pub fn calculate_joules_consumed<T>(&self, store: &mut Store<T>) -> f64 {
        let fuel_remaining = store.get_fuel().unwrap_or(0);
        let actual_consumed = self.total_fuel.saturating_sub(fuel_remaining);
        let nj_per_instruction = 0.5; // Average nJ cost per WASM execution
        (actual_consumed as f64) * nj_per_instruction / 1_000_000_000.0 // Convert to Joules
    }
}
