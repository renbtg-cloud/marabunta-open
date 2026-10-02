// Marabunta - Licensed under the MIT License.
//! Aerodynamics simulation plugin -- a realistic computation example.
//!
//! Accepts a triangulated surface mesh and simulation parameters, runs a
//! simplified panel-method solver, and returns aerodynamic coefficients plus
//! a per-cell pressure field.  Every execution step emits audit events for
//! full traceability in classified environments.

use marabunta_plugin_sdk::prelude::*;
use marabunta_plugin_sdk::audit;

// ---------------------------------------------------------------------------
// I/O types (JSON-serialised over the wire)
// ---------------------------------------------------------------------------

/// Triangulated surface mesh.
#[derive(Debug, Serialize, Deserialize)]
struct Mesh {
    /// Vertex positions (x, y, z).
    vertices: Vec<[f64; 3]>,
    /// Tetrahedral cell connectivity (indices into `vertices`).
    cells: Vec<[u32; 4]>,
}

/// Simulation configuration.
#[derive(Debug, Serialize, Deserialize)]
struct SimConfig {
    /// Free-stream Mach number (0 < M < 5).
    mach_number: f64,
    /// Angle of attack in degrees.
    angle_of_attack: f64,
    /// Number of solver iterations (1..=10_000).
    iterations: u32,
}

/// Simulation results.
#[derive(Debug, Serialize, Deserialize)]
struct SimResult {
    lift_coefficient: f64,
    drag_coefficient: f64,
    /// One pressure value per cell.
    pressure_field: Vec<f64>,
}

// ---------------------------------------------------------------------------
// Plugin definition
// ---------------------------------------------------------------------------

/// Simplified aerodynamic panel-method solver.
#[marabunta_plugin]
#[derive(Default)]
struct AeroSimPlugin;

impl ComputePlugin for AeroSimPlugin {
    fn execute(&self, input: &[u8], params: &[u8]) -> Result<Vec<u8>, PluginError> {
        // ---- deserialise inputs ----
        let mesh: Mesh = serde_json::from_slice(input).map_err(|e| {
            audit::emit(
                "input_parse_error",
                serde_json::json!({ "error": e.to_string() }),
            );
            PluginError::InvalidInput
        })?;

        let config: SimConfig = serde_json::from_slice(params).map_err(|e| {
            audit::emit(
                "params_parse_error",
                serde_json::json!({ "error": e.to_string() }),
            );
            PluginError::InvalidParams
        })?;

        // ---- validate ----
        if mesh.vertices.is_empty() || mesh.cells.is_empty() {
            return Err(PluginError::InvalidInput);
        }
        if config.mach_number <= 0.0 || config.mach_number >= 5.0 {
            return Err(PluginError::InvalidParams);
        }
        if config.iterations == 0 || config.iterations > 10_000 {
            return Err(PluginError::InvalidParams);
        }

        audit::emit(
            "sim_started",
            serde_json::json!({
                "vertices": mesh.vertices.len(),
                "cells": mesh.cells.len(),
                "mach": config.mach_number,
                "aoa_deg": config.angle_of_attack,
                "iterations": config.iterations,
            }),
        );

        // ---- simplified panel-method solver ----
        let aoa_rad = config.angle_of_attack.to_radians();
        let num_cells = mesh.cells.len();

        // Compute per-cell pressure coefficient via a simplified Newtonian
        // impact model: Cp = 2 * sin^2(theta) where theta is the angle
        // between the free-stream direction and the panel normal.
        let freestream = [aoa_rad.cos(), aoa_rad.sin(), 0.0_f64];

        let mut pressure_field = Vec::with_capacity(num_cells);
        let mut total_lift = 0.0_f64;
        let mut total_drag = 0.0_f64;

        for cell in &mesh.cells {
            // Fetch the first three vertices of the tet to form a surface
            // triangle (sufficient for a panel-method approximation).
            let v0 = get_vertex(&mesh.vertices, cell[0]);
            let v1 = get_vertex(&mesh.vertices, cell[1]);
            let v2 = get_vertex(&mesh.vertices, cell[2]);

            let normal = triangle_normal(v0, v1, v2);
            let area = triangle_area(v0, v1, v2);

            // Angle between freestream and outward normal.
            let cos_theta = dot(&freestream, &normal);
            let sin2_theta = (1.0 - cos_theta * cos_theta).max(0.0);

            // Newtonian pressure coefficient.
            let cp = 2.0 * sin2_theta;

            // Iterative relaxation (simplified).
            let damping = 1.0 - (1.0 / (config.iterations as f64 + 1.0));
            let cp_relaxed = cp * damping;

            pressure_field.push(cp_relaxed);

            // Accumulate forces.
            total_lift += cp_relaxed * area * (-normal[1]); // lift ~ -y component
            total_drag += cp_relaxed * area * normal[0]; // drag ~ x component
        }

        // Normalise by dynamic pressure factor (0.5 * rho * V^2 cancels in
        // coefficient form; we normalise by the total reference area instead).
        let ref_area: f64 = mesh
            .cells
            .iter()
            .map(|c| {
                let a = get_vertex(&mesh.vertices, c[0]);
                let b = get_vertex(&mesh.vertices, c[1]);
                let d = get_vertex(&mesh.vertices, c[2]);
                triangle_area(a, b, d)
            })
            .sum();

        let cl = if ref_area > 1e-12 {
            total_lift / ref_area
        } else {
            0.0
        };
        let cd = if ref_area > 1e-12 {
            total_drag / ref_area
        } else {
            0.0
        };

        audit::emit(
            "sim_completed",
            serde_json::json!({
                "cl": cl,
                "cd": cd,
                "cells_computed": num_cells,
            }),
        );

        let result = SimResult {
            lift_coefficient: cl,
            drag_coefficient: cd,
            pressure_field,
        };

        serde_json::to_vec(&result)
            .map_err(|e| PluginError::Internal(format!("serialisation failed: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Geometry helpers
// ---------------------------------------------------------------------------

fn get_vertex(vertices: &[[f64; 3]], idx: u32) -> [f64; 3] {
    vertices
        .get(idx as usize)
        .copied()
        .unwrap_or([0.0, 0.0, 0.0])
}

fn triangle_normal(a: [f64; 3], b: [f64; 3], c: [f64; 3]) -> [f64; 3] {
    let u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]];
    let v = [c[0] - a[0], c[1] - a[1], c[2] - a[2]];
    let n = cross(&u, &v);
    let len = (n[0] * n[0] + n[1] * n[1] + n[2] * n[2]).sqrt();
    if len < 1e-12 {
        [0.0, 0.0, 1.0]
    } else {
        [n[0] / len, n[1] / len, n[2] / len]
    }
}

fn triangle_area(a: [f64; 3], b: [f64; 3], c: [f64; 3]) -> f64 {
    let u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]];
    let v = [c[0] - a[0], c[1] - a[1], c[2] - a[2]];
    let n = cross(&u, &v);
    0.5 * (n[0] * n[0] + n[1] * n[1] + n[2] * n[2]).sqrt()
}

fn cross(a: &[f64; 3], b: &[f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn dot(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use marabunta_plugin_sdk::testing::{TestHarness, HarnessError};

    /// Build a tiny 2-cell mesh (two tetrahedra sharing a face).
    fn sample_mesh() -> Mesh {
        Mesh {
            vertices: vec![
                [0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0],
                [0.5, 1.0, 0.0],
                [0.5, 0.5, 1.0],
                [0.5, 0.5, -1.0],
            ],
            cells: vec![[0, 1, 2, 3], [0, 1, 2, 4]],
        }
    }

    fn sample_config() -> SimConfig {
        SimConfig {
            mach_number: 0.8,
            angle_of_attack: 5.0,
            iterations: 100,
        }
    }

    #[test]
    fn basic_simulation_produces_results() {
        let harness = TestHarness::builder()
            .jurisdiction(CountryCode::FR)
            .zone_class(ZoneClass::MilRestricted)
            .classification(ClassificationLevel::Restricted)
            .fuel_limit(10_000_000)
            .memory_pages(512)
            .build();

        let input = serde_json::to_vec(&sample_mesh()).unwrap();
        let params = serde_json::to_vec(&sample_config()).unwrap();

        let output = harness
            .execute::<AeroSimPlugin>(&input, &params)
            .expect("simulation should succeed");

        let result: SimResult = serde_json::from_slice(&output).unwrap();

        // Two cells => two pressure values.
        assert_eq!(result.pressure_field.len(), 2);

        // Coefficients should be finite numbers.
        assert!(result.lift_coefficient.is_finite());
        assert!(result.drag_coefficient.is_finite());
    }

    #[test]
    fn rejects_empty_mesh() {
        let harness = TestHarness::builder().build();

        let empty_mesh = Mesh {
            vertices: vec![],
            cells: vec![],
        };
        let input = serde_json::to_vec(&empty_mesh).unwrap();
        let params = serde_json::to_vec(&sample_config()).unwrap();

        let err = harness
            .execute::<AeroSimPlugin>(&input, &params)
            .expect_err("empty mesh should fail");

        assert!(matches!(err, HarnessError::PluginError(PluginError::InvalidInput)));
    }

    #[test]
    fn rejects_invalid_mach_number() {
        let harness = TestHarness::builder().build();

        let input = serde_json::to_vec(&sample_mesh()).unwrap();
        let bad_config = SimConfig {
            mach_number: -1.0,
            angle_of_attack: 0.0,
            iterations: 10,
        };
        let params = serde_json::to_vec(&bad_config).unwrap();

        let err = harness
            .execute::<AeroSimPlugin>(&input, &params)
            .expect_err("negative mach should fail");

        assert!(matches!(err, HarnessError::PluginError(PluginError::InvalidParams)));
    }

    #[test]
    fn audit_events_are_emitted() {
        let harness = TestHarness::builder().build();

        let input = serde_json::to_vec(&sample_mesh()).unwrap();
        let params = serde_json::to_vec(&sample_config()).unwrap();

        let _ = harness
            .execute::<AeroSimPlugin>(&input, &params)
            .expect("simulation should succeed");

        let events = marabunta_plugin_sdk::audit::drain_events();
        let event_types: Vec<&str> = events.iter().map(|(t, _)| t.as_str()).collect();

        assert!(
            event_types.contains(&"sim_started"),
            "expected sim_started audit event"
        );
        assert!(
            event_types.contains(&"sim_completed"),
            "expected sim_completed audit event"
        );
    }
}

// ---------------------------------------------------------------------------
// Binary entry point (required for `cargo run --example aero_sim_plugin`)
// ---------------------------------------------------------------------------

fn main() {}
