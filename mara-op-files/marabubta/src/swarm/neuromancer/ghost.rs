// Marabunta - Licensed under the MIT License.
//! Ghost — Phantom node assembly system.
//!
//! Ghost decomposes large tasks into dependency-ordered work units,
//! assembles "phantom" virtual nodes from multiple physical contributors,
//! executes units respecting the DAG topology, and collects results.
//!
//! The [`MarabuntaTask`] trait is the SDK-facing interface for users who
//! want to submit decomposable tasks.  Internally, Ghost manages
//! [`PhantomState`] records that track per-unit assignments and statuses.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::GhostConfig;
use crate::swarm::neuromancer::plugin::PlacementHint;
use super::types::{MarabuntaEvent, NodeId, ResourceRequirements, TaskId};

// ============================================================================
// MarabuntaTask — SDK-facing trait
// ============================================================================

/// SDK-facing trait for tasks that can be decomposed across a phantom.
///
/// Implementors define how to split typed input into raw byte-level work
/// units and how to reassemble the typed output from raw byte-level results.
pub trait MarabuntaTask: Send + Sync {
    /// Typed input for this task.
    type Input: Send + Sync + Serialize + serde::de::DeserializeOwned;
    /// Typed output for this task.
    type Output: Send + Sync + Serialize + serde::de::DeserializeOwned;

    /// Human-readable task type identifier (e.g. `"sum"`, `"matrix_multiply"`).
    fn task_type(&self) -> &str;

    /// Decompose typed input into raw work units.
    fn decompose(&self, input: &Self::Input) -> Vec<RawWorkUnit>;

    /// Compose raw results back into the typed output.
    fn compose(&self, results: Vec<RawWorkUnitResult>) -> Self::Output;

    /// Optional placement affinity hint for a given work unit.
    fn affinity(&self, unit: &RawWorkUnit) -> PlacementHint {
        let _ = unit;
        PlacementHint::any()
    }
}

// ============================================================================
// RawWorkUnit / RawWorkUnitResult
// ============================================================================

/// A single byte-level unit of work produced by decomposition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawWorkUnit {
    /// Positional index of this unit within the phantom.
    pub index: u32,
    /// Serialised input payload.
    pub input: Vec<u8>,
    /// Indices of units that must complete before this one can start.
    pub dependencies: Vec<u32>,
}

/// Result of executing a single [`RawWorkUnit`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawWorkUnitResult {
    /// The index of the work unit that produced this result.
    pub index: u32,
    /// Serialised output payload.
    pub output: Vec<u8>,
}

// ============================================================================
// UnitAssignment / UnitStatus
// ============================================================================

/// Tracks the assignment and status of a single work unit within a phantom.
#[derive(Debug, Clone)]
pub struct UnitAssignment {
    /// Index of the work unit.
    pub unit_index: u32,
    /// Node currently assigned to execute this unit (if any).
    pub assigned_node: Option<NodeId>,
    /// Current execution status.
    pub status: UnitStatus,
    /// Output bytes once completed.
    pub result: Option<Vec<u8>>,
}

/// Execution status of a single work unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitStatus {
    /// Waiting for dependencies or assignment.
    Pending,
    /// Currently being executed on an assigned node.
    Running,
    /// Execution completed successfully.
    Completed,
    /// Execution failed with a reason.
    Failed(String),
    /// The assigned node died; unit is being re-assigned.
    Resurrecting,
}

// ============================================================================
// PhantomStatus / PhantomState
// ============================================================================

/// High-level status of a phantom assembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhantomStatus {
    /// Work units are being set up and assigned.
    Assembling,
    /// Units are actively executing.
    Executing,
    /// All units completed successfully.
    Completed,
    /// The phantom failed with a reason.
    Failed(String),
    /// The phantom has been dissolved (resources released).
    Dissolved,
}

/// Full state of a phantom node assembly.
#[derive(Debug)]
pub struct PhantomState {
    /// Unique identifier for this phantom.
    pub phantom_id: TaskId,
    /// The raw work units that make up this phantom's workload.
    pub units: Vec<RawWorkUnit>,
    /// Per-unit assignment tracking, keyed by unit index.
    pub assignments: HashMap<u32, UnitAssignment>,
    /// Physical nodes contributing resources to this phantom.
    pub contributing_nodes: Vec<NodeId>,
    /// Current high-level status.
    pub status: PhantomStatus,
    /// When this phantom was created.
    pub created_at: SystemTime,
}

// ============================================================================
// GhostError
// ============================================================================

/// Errors produced by the Ghost subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GhostError {
    /// The input to a task was invalid.
    InvalidInput(String),
    /// A dependency cycle was detected in the work unit DAG.
    CycleDetected,
    /// No contributing nodes are available for execution.
    NoNodesAvailable,
    /// A task or phantom failed with a reason.
    TaskFailed(String),
    /// The phantom has already been dissolved.
    AlreadyDissolved,
}

impl std::fmt::Display for GhostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput(msg) => write!(f, "invalid input: {}", msg),
            Self::CycleDetected => write!(f, "dependency cycle detected in work unit DAG"),
            Self::NoNodesAvailable => write!(f, "no contributing nodes available"),
            Self::TaskFailed(msg) => write!(f, "task failed: {}", msg),
            Self::AlreadyDissolved => write!(f, "phantom has already been dissolved"),
        }
    }
}

impl std::error::Error for GhostError {}

// ============================================================================
// GhostRequest / GhostResponse — wire types
// ============================================================================

/// Request messages sent to remote nodes for phantom execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GhostRequest {
    /// Ask a node to execute a specific work unit.
    ExecuteUnit {
        phantom_id: TaskId,
        unit: RawWorkUnit,
    },
    /// Query a node for a completed unit result.
    GetResult {
        phantom_id: TaskId,
        unit_index: u32,
    },
}

/// Response messages from remote nodes during phantom execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GhostResponse {
    /// A work unit completed successfully.
    UnitComplete {
        phantom_id: TaskId,
        index: u32,
        output: Vec<u8>,
    },
    /// A work unit failed.
    UnitFailed {
        phantom_id: TaskId,
        index: u32,
        error: String,
    },
    /// The requested result is not yet ready.
    ResultNotReady,
}

// ============================================================================
// Ghost — main struct
// ============================================================================

/// The Ghost phantom node assembly engine.
///
/// Manages phantom lifecycles: creation, execution (dependency-ordered),
/// node failure handling, and dissolution.
pub struct Ghost {
    config: GhostConfig,
    bus: Arc<NeuromancerBus>,
    phantoms: HashMap<TaskId, PhantomState>,
}

impl Ghost {
    /// Create a new Ghost engine.
    pub fn new(config: GhostConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self {
            config,
            bus,
            phantoms: HashMap::new(),
        }
    }

    /// Submit a pre-built set of raw work units as a new phantom.
    ///
    /// Creates a [`PhantomState`], initialises per-unit assignments as
    /// [`UnitStatus::Pending`], and emits a [`MarabuntaEvent::PhantomAssembled`]
    /// event on the bus.
    pub fn submit_raw_task(
        &mut self,
        task_id: TaskId,
        units: Vec<RawWorkUnit>,
        contributing_nodes: Vec<NodeId>,
    ) -> Result<TaskId, GhostError> {
        if units.is_empty() {
            return Err(GhostError::InvalidInput(
                "cannot submit a phantom with zero work units".into(),
            ));
        }

        let mut assignments = HashMap::new();
        for unit in &units {
            assignments.insert(
                unit.index,
                UnitAssignment {
                    unit_index: unit.index,
                    assigned_node: None,
                    status: UnitStatus::Pending,
                    result: None,
                },
            );
        }

        let state = PhantomState {
            phantom_id: task_id,
            units,
            assignments,
            contributing_nodes: contributing_nodes.clone(),
            status: PhantomStatus::Assembling,
            created_at: SystemTime::now(),
        };

        self.phantoms.insert(task_id, state);

        info!(
            phantom_id = ?hex::encode(task_id),
            num_units = self.phantoms[&task_id].units.len(),
            num_nodes = contributing_nodes.len(),
            "phantom assembled"
        );

        self.bus.emit(MarabuntaEvent::PhantomAssembled {
            phantom_id: task_id,
            contributing_nodes,
            total_resources: ResourceRequirements::default(),
            timestamp: SystemTime::now(),
        });

        Ok(task_id)
    }

    /// Convenience: auto-generate a random phantom ID and submit.
    pub fn assemble_phantom(
        &mut self,
        units: Vec<RawWorkUnit>,
        contributing_nodes: Vec<NodeId>,
    ) -> Result<TaskId, GhostError> {
        let phantom_id: TaskId = rand::random::<[u8; 32]>();
        self.submit_raw_task(phantom_id, units, contributing_nodes)
    }

    /// Execute all units of a phantom in dependency order.
    ///
    /// Uses Kahn's algorithm ([`topological_sort`]) to determine execution
    /// order, detects cycles, and collects results.  In V1 the execution
    /// is simulated locally: each unit's input bytes are stored as its
    /// output (real remote dispatch is handled externally).
    ///
    /// Returns a map from unit index to output bytes.
    pub fn execute_phantom_raw(
        &mut self,
        phantom_id: TaskId,
    ) -> Result<HashMap<u32, Vec<u8>>, GhostError> {
        let state = self
            .phantoms
            .get_mut(&phantom_id)
            .ok_or_else(|| GhostError::InvalidInput("phantom not found".into()))?;

        if state.status == PhantomStatus::Dissolved {
            return Err(GhostError::AlreadyDissolved);
        }

        state.status = PhantomStatus::Executing;

        // Topological sort to determine execution order.
        let order = topological_sort(&state.units)?;

        let mut results: HashMap<u32, Vec<u8>> = HashMap::new();

        for idx in order {
            // Find the unit with this index.
            let unit = state
                .units
                .iter()
                .find(|u| u.index == idx)
                .ok_or_else(|| {
                    GhostError::TaskFailed(format!("unit {} not found in phantom", idx))
                })?;

            // Mark as running.
            if let Some(assignment) = state.assignments.get_mut(&idx) {
                assignment.status = UnitStatus::Running;
            }

            // V1 simulation: use input as output (identity execution).
            let output = unit.input.clone();

            // Store result.
            results.insert(idx, output.clone());

            if let Some(assignment) = state.assignments.get_mut(&idx) {
                assignment.status = UnitStatus::Completed;
                assignment.result = Some(output);
            }

            debug!(phantom_id = ?hex::encode(phantom_id), unit_index = idx, "unit completed");
        }

        state.status = PhantomStatus::Completed;

        info!(
            phantom_id = ?hex::encode(phantom_id),
            num_results = results.len(),
            "phantom execution completed"
        );

        Ok(results)
    }

    /// Dissolve a phantom, releasing all resources and emitting
    /// [`MarabuntaEvent::PhantomDissolved`].
    pub fn dissolve_phantom(&mut self, phantom_id: TaskId) -> Result<(), GhostError> {
        let state = self
            .phantoms
            .get_mut(&phantom_id)
            .ok_or_else(|| GhostError::InvalidInput("phantom not found".into()))?;

        if state.status == PhantomStatus::Dissolved {
            return Err(GhostError::AlreadyDissolved);
        }

        state.status = PhantomStatus::Dissolved;

        info!(phantom_id = ?hex::encode(phantom_id), "phantom dissolved");

        self.bus.emit(MarabuntaEvent::PhantomDissolved {
            phantom_id,
            timestamp: SystemTime::now(),
        });

        Ok(())
    }

    /// Handle a node failure by marking all units assigned to the failed
    /// node as [`UnitStatus::Resurrecting`].
    pub fn handle_node_failure(&mut self, failed_node: NodeId) {
        let mut affected_count = 0usize;

        for state in self.phantoms.values_mut() {
            // Skip dissolved or completed phantoms.
            if state.status == PhantomStatus::Dissolved
                || state.status == PhantomStatus::Completed
            {
                continue;
            }

            for assignment in state.assignments.values_mut() {
                if assignment.assigned_node == Some(failed_node)
                    && assignment.status != UnitStatus::Completed
                {
                    assignment.status = UnitStatus::Resurrecting;
                    affected_count += 1;
                }
            }

            // Also remove from contributing nodes list.
            state
                .contributing_nodes
                .retain(|n| *n != failed_node);
        }

        if affected_count > 0 {
            warn!(
                node = ?failed_node,
                affected_units = affected_count,
                "node failure handled — units marked as resurrecting"
            );
        }
    }

    /// Handle a completed work unit result, updating the phantom's
    /// assignment record.
    pub fn handle_unit_complete(
        &mut self,
        phantom_id: TaskId,
        index: u32,
        output: Vec<u8>,
    ) -> Result<(), GhostError> {
        let state = self
            .phantoms
            .get_mut(&phantom_id)
            .ok_or_else(|| GhostError::InvalidInput("phantom not found".into()))?;

        if state.status == PhantomStatus::Dissolved {
            return Err(GhostError::AlreadyDissolved);
        }

        let assignment = state
            .assignments
            .get_mut(&index)
            .ok_or_else(|| GhostError::InvalidInput(format!("unit {} not found", index)))?;

        assignment.status = UnitStatus::Completed;
        assignment.result = Some(output);

        debug!(
            phantom_id = ?hex::encode(phantom_id),
            unit_index = index,
            "unit complete received"
        );

        // Check if all units are now completed.
        let all_complete = state
            .assignments
            .values()
            .all(|a| a.status == UnitStatus::Completed);

        if all_complete {
            state.status = PhantomStatus::Completed;
            info!(
                phantom_id = ?hex::encode(phantom_id),
                "all units complete — phantom completed"
            );
        }

        Ok(())
    }

    /// Return the current status of a phantom, if it exists.
    pub fn phantom_status(&self, phantom_id: &TaskId) -> Option<&PhantomStatus> {
        self.phantoms.get(phantom_id).map(|s| &s.status)
    }

    /// Return the number of active (non-dissolved) phantoms.
    pub fn active_phantom_count(&self) -> usize {
        self.phantoms
            .values()
            .filter(|s| s.status != PhantomStatus::Dissolved)
            .count()
    }

    /// Whether result caching is enabled in the configuration.
    pub fn cache_enabled(&self) -> bool {
        self.config.enable_cache
    }
}

// ============================================================================
// DAG topological sort (Kahn's algorithm)
// ============================================================================

/// Topologically sort work units by their dependency graph using Kahn's
/// algorithm.
///
/// Returns an ordered `Vec<u32>` of unit indices such that every unit
/// appears after all of its dependencies.
///
/// Returns [`GhostError::CycleDetected`] if the dependency graph contains
/// a cycle.
pub fn topological_sort(units: &[RawWorkUnit]) -> Result<Vec<u32>, GhostError> {
    // Build adjacency list and in-degree map.
    // Edge: dependency -> dependent  (dep must come before the unit)
    let indices: Vec<u32> = units.iter().map(|u| u.index).collect();
    let index_set: std::collections::HashSet<u32> = indices.iter().copied().collect();

    // in_degree[node] = number of dependencies it has (that are in the unit set)
    let mut in_degree: HashMap<u32, usize> = HashMap::new();
    // adjacency: from -> list of nodes that depend on `from`
    let mut adjacency: HashMap<u32, Vec<u32>> = HashMap::new();

    for &idx in &indices {
        in_degree.entry(idx).or_insert(0);
        adjacency.entry(idx).or_default();
    }

    for unit in units {
        for &dep in &unit.dependencies {
            // Only count dependencies that are within the unit set.
            if index_set.contains(&dep) {
                *in_degree.entry(unit.index).or_insert(0) += 1;
                adjacency.entry(dep).or_default().push(unit.index);
            }
        }
    }

    // BFS from zero in-degree nodes.
    let mut queue: VecDeque<u32> = VecDeque::new();
    for (&idx, &deg) in &in_degree {
        if deg == 0 {
            queue.push_back(idx);
        }
    }

    let mut result: Vec<u32> = Vec::with_capacity(indices.len());

    while let Some(node) = queue.pop_front() {
        result.push(node);

        if let Some(dependents) = adjacency.get(&node) {
            for &dependent in dependents {
                if let Some(deg) = in_degree.get_mut(&dependent) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dependent);
                    }
                }
            }
        }
    }

    if result.len() != indices.len() {
        return Err(GhostError::CycleDetected);
    }

    Ok(result)
}

// ============================================================================
// SumTask — test/example MarabuntaTask implementation
// ============================================================================

/// Example task that sums a vector of integers across phantom work units.
///
/// Each element becomes its own work unit with no dependencies; composition
/// simply sums the deserialized outputs.
pub struct SumTask;

impl MarabuntaTask for SumTask {
    type Input = Vec<i64>;
    type Output = i64;

    fn task_type(&self) -> &str {
        "sum"
    }

    fn decompose(&self, input: &Vec<i64>) -> Vec<RawWorkUnit> {
        input
            .iter()
            .enumerate()
            .map(|(i, &n)| RawWorkUnit {
                index: i as u32,
                input: serde_json::to_vec(&n).unwrap(),
                dependencies: vec![],
            })
            .collect()
    }

    fn compose(&self, results: Vec<RawWorkUnitResult>) -> i64 {
        results
            .iter()
            .map(|r| serde_json::from_slice::<i64>(&r.output).unwrap())
            .sum()
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::neuromancer::bus::NeuromancerBus;

    fn make_bus() -> Arc<NeuromancerBus> {
        Arc::new(NeuromancerBus::new(128))
    }

    fn make_ghost() -> Ghost {
        Ghost::new(GhostConfig::default(), make_bus())
    }

    // -- SumTask tests --

    #[test]
    fn test_sum_task_decompose() {
        let task = SumTask;
        let units = task.decompose(&vec![1, 2, 3]);
        assert_eq!(units.len(), 3);
        assert_eq!(units[0].index, 0);
        assert_eq!(units[1].index, 1);
        assert_eq!(units[2].index, 2);
        // Each unit should have no dependencies.
        for u in &units {
            assert!(u.dependencies.is_empty());
        }
        // Verify serialised values.
        let v0: i64 = serde_json::from_slice(&units[0].input).unwrap();
        assert_eq!(v0, 1);
    }

    #[test]
    fn test_sum_task_compose() {
        let task = SumTask;
        let results = vec![
            RawWorkUnitResult {
                index: 0,
                output: serde_json::to_vec(&1i64).unwrap(),
            },
            RawWorkUnitResult {
                index: 1,
                output: serde_json::to_vec(&2i64).unwrap(),
            },
            RawWorkUnitResult {
                index: 2,
                output: serde_json::to_vec(&3i64).unwrap(),
            },
        ];
        let sum = task.compose(results);
        assert_eq!(sum, 6);
    }

    // -- Topological sort tests --

    #[test]
    fn test_topological_sort_linear_chain() {
        // 0 -> 1 -> 2 (unit 1 depends on 0, unit 2 depends on 1)
        let units = vec![
            RawWorkUnit {
                index: 0,
                input: vec![],
                dependencies: vec![],
            },
            RawWorkUnit {
                index: 1,
                input: vec![],
                dependencies: vec![0],
            },
            RawWorkUnit {
                index: 2,
                input: vec![],
                dependencies: vec![1],
            },
        ];
        let order = topological_sort(&units).unwrap();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn test_topological_sort_no_deps() {
        let units = vec![
            RawWorkUnit {
                index: 0,
                input: vec![],
                dependencies: vec![],
            },
            RawWorkUnit {
                index: 1,
                input: vec![],
                dependencies: vec![],
            },
            RawWorkUnit {
                index: 2,
                input: vec![],
                dependencies: vec![],
            },
        ];
        let order = topological_sort(&units).unwrap();
        // All three should be present (order among independent nodes is unspecified).
        assert_eq!(order.len(), 3);
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2]);
    }

    #[test]
    fn test_topological_sort_cycle_detected() {
        // 0 -> 1 and 1 -> 0 — a cycle
        let units = vec![
            RawWorkUnit {
                index: 0,
                input: vec![],
                dependencies: vec![1],
            },
            RawWorkUnit {
                index: 1,
                input: vec![],
                dependencies: vec![0],
            },
        ];
        let result = topological_sort(&units);
        assert_eq!(result, Err(GhostError::CycleDetected));
    }

    // -- Phantom lifecycle tests --

    #[test]
    fn test_phantom_assembled_event_emitted() {
        let bus = make_bus();
        let mut rx = bus.subscribe();
        let mut ghost = Ghost::new(GhostConfig::default(), Arc::clone(&bus));

        let units = vec![RawWorkUnit {
            index: 0,
            input: vec![1, 2, 3],
            dependencies: vec![],
        }];
        let node = NodeId::new();
        let phantom_id = ghost
            .submit_raw_task(rand::random::<[u8; 32]>(), units, vec![node])
            .unwrap();

        // The bus should have received a PhantomAssembled event.
        let event = rx.try_recv().unwrap();
        match event {
            MarabuntaEvent::PhantomAssembled {
                phantom_id: pid,
                contributing_nodes,
                ..
            } => {
                assert_eq!(pid, phantom_id);
                assert_eq!(contributing_nodes, vec![node]);
            }
            other => panic!("expected PhantomAssembled, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_phantom_dissolved_event_emitted() {
        let bus = make_bus();
        let mut rx = bus.subscribe();
        let mut ghost = Ghost::new(GhostConfig::default(), Arc::clone(&bus));

        let units = vec![RawWorkUnit {
            index: 0,
            input: vec![],
            dependencies: vec![],
        }];
        let phantom_id = ghost
            .assemble_phantom(units, vec![NodeId::new()])
            .unwrap();

        // Drain the PhantomAssembled event.
        let _ = rx.try_recv().unwrap();

        ghost.dissolve_phantom(phantom_id).unwrap();

        let event = rx.try_recv().unwrap();
        match event {
            MarabuntaEvent::PhantomDissolved {
                phantom_id: pid, ..
            } => {
                assert_eq!(pid, phantom_id);
            }
            other => panic!("expected PhantomDissolved, got {:?}", other.type_name()),
        }
    }

    #[test]
    fn test_node_failure_marks_units_resurrecting() {
        let mut ghost = make_ghost();

        let failing_node = NodeId::new();
        let healthy_node = NodeId::new();

        let units = vec![
            RawWorkUnit {
                index: 0,
                input: vec![10],
                dependencies: vec![],
            },
            RawWorkUnit {
                index: 1,
                input: vec![20],
                dependencies: vec![],
            },
        ];

        let phantom_id = ghost
            .submit_raw_task(
                rand::random::<[u8; 32]>(),
                units,
                vec![failing_node, healthy_node],
            )
            .unwrap();

        // Manually assign units to specific nodes.
        {
            let state = ghost.phantoms.get_mut(&phantom_id).unwrap();
            state.assignments.get_mut(&0).unwrap().assigned_node = Some(failing_node);
            state.assignments.get_mut(&0).unwrap().status = UnitStatus::Running;
            state.assignments.get_mut(&1).unwrap().assigned_node = Some(healthy_node);
            state.assignments.get_mut(&1).unwrap().status = UnitStatus::Running;
        }

        ghost.handle_node_failure(failing_node);

        let state = ghost.phantoms.get(&phantom_id).unwrap();
        assert_eq!(state.assignments[&0].status, UnitStatus::Resurrecting);
        assert_eq!(state.assignments[&1].status, UnitStatus::Running);
    }

    // -- Additional coverage tests --

    #[test]
    fn test_execute_phantom_raw_collects_results() {
        let mut ghost = make_ghost();

        let units = vec![
            RawWorkUnit {
                index: 0,
                input: serde_json::to_vec(&10i64).unwrap(),
                dependencies: vec![],
            },
            RawWorkUnit {
                index: 1,
                input: serde_json::to_vec(&20i64).unwrap(),
                dependencies: vec![0],
            },
        ];

        let phantom_id = ghost
            .assemble_phantom(units, vec![NodeId::new()])
            .unwrap();

        let results = ghost.execute_phantom_raw(phantom_id).unwrap();
        assert_eq!(results.len(), 2);
        // V1 simulation: input == output
        let v0: i64 = serde_json::from_slice(&results[&0]).unwrap();
        let v1: i64 = serde_json::from_slice(&results[&1]).unwrap();
        assert_eq!(v0, 10);
        assert_eq!(v1, 20);

        // Status should be Completed.
        assert_eq!(
            ghost.phantom_status(&phantom_id),
            Some(&PhantomStatus::Completed)
        );
    }

    #[test]
    fn test_dissolve_already_dissolved_returns_error() {
        let mut ghost = make_ghost();

        let units = vec![RawWorkUnit {
            index: 0,
            input: vec![],
            dependencies: vec![],
        }];
        let phantom_id = ghost
            .assemble_phantom(units, vec![NodeId::new()])
            .unwrap();

        ghost.dissolve_phantom(phantom_id).unwrap();
        let err = ghost.dissolve_phantom(phantom_id).unwrap_err();
        assert_eq!(err, GhostError::AlreadyDissolved);
    }

    #[test]
    fn test_active_phantom_count() {
        let mut ghost = make_ghost();

        let mk_units = || {
            vec![RawWorkUnit {
                index: 0,
                input: vec![],
                dependencies: vec![],
            }]
        };

        let id1 = ghost
            .assemble_phantom(mk_units(), vec![NodeId::new()])
            .unwrap();
        let _id2 = ghost
            .assemble_phantom(mk_units(), vec![NodeId::new()])
            .unwrap();

        assert_eq!(ghost.active_phantom_count(), 2);

        ghost.dissolve_phantom(id1).unwrap();
        assert_eq!(ghost.active_phantom_count(), 1);
    }

    #[test]
    fn test_handle_unit_complete_marks_phantom_completed() {
        let mut ghost = make_ghost();

        let units = vec![
            RawWorkUnit {
                index: 0,
                input: vec![1],
                dependencies: vec![],
            },
            RawWorkUnit {
                index: 1,
                input: vec![2],
                dependencies: vec![],
            },
        ];

        let phantom_id = ghost
            .assemble_phantom(units, vec![NodeId::new()])
            .unwrap();

        // Set status to Executing so we can test completion path.
        ghost.phantoms.get_mut(&phantom_id).unwrap().status = PhantomStatus::Executing;

        ghost
            .handle_unit_complete(phantom_id, 0, vec![10])
            .unwrap();
        // Not yet complete — unit 1 still pending.
        assert_eq!(
            ghost.phantom_status(&phantom_id),
            Some(&PhantomStatus::Executing)
        );

        ghost
            .handle_unit_complete(phantom_id, 1, vec![20])
            .unwrap();
        // Now all units are done.
        assert_eq!(
            ghost.phantom_status(&phantom_id),
            Some(&PhantomStatus::Completed)
        );
    }

    #[test]
    fn test_submit_empty_units_returns_error() {
        let mut ghost = make_ghost();
        let result = ghost.submit_raw_task(rand::random::<[u8; 32]>(), vec![], vec![NodeId::new()]);
        assert!(matches!(result, Err(GhostError::InvalidInput(_))));
    }

    #[test]
    fn test_topological_sort_diamond_dag() {
        //     0
        //    / \
        //   1   2
        //    \ /
        //     3
        let units = vec![
            RawWorkUnit {
                index: 0,
                input: vec![],
                dependencies: vec![],
            },
            RawWorkUnit {
                index: 1,
                input: vec![],
                dependencies: vec![0],
            },
            RawWorkUnit {
                index: 2,
                input: vec![],
                dependencies: vec![0],
            },
            RawWorkUnit {
                index: 3,
                input: vec![],
                dependencies: vec![1, 2],
            },
        ];
        let order = topological_sort(&units).unwrap();
        assert_eq!(order.len(), 4);
        // 0 must come first, 3 must come last.
        assert_eq!(order[0], 0);
        assert_eq!(order[3], 3);
        // 1 and 2 must appear between 0 and 3.
        let pos1 = order.iter().position(|&x| x == 1).unwrap();
        let pos2 = order.iter().position(|&x| x == 2).unwrap();
        assert!(pos1 > 0 && pos1 < 3);
        assert!(pos2 > 0 && pos2 < 3);
    }

    #[test]
    fn test_cache_enabled_flag() {
        let ghost = make_ghost();
        assert!(ghost.cache_enabled());

        let ghost_no_cache = Ghost::new(
            GhostConfig {
                enable_cache: false,
                ..Default::default()
            },
            make_bus(),
        );
        assert!(!ghost_no_cache.cache_enabled());
    }
}
