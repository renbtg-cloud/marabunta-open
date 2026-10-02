// Marabunta - Licensed under the MIT License.
//! Core types for the distributed computing framework

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

// Re-export locality types for convenience when working with tasks
pub use crate::placement::locality::{DataId, DataPlacementHint, DataRef, LocalityPreference};

/// Unique identifier for a job
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "job-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TaskId(pub Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a worker
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(pub Uuid);

impl WorkerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "worker-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a master
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MasterId(pub Uuid);

impl MasterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for MasterId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MasterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "master-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a region
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionId(pub Uuid);

impl RegionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_name(name: &str) -> Self {
        Self(Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes()))
    }
}

impl Default for RegionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "region-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a holographic topology blueprint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopologyId(pub Uuid);

impl TopologyId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TopologyId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TopologyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "topology-{}", &self.0.to_string()[..8])
    }
}

/// Status of a job
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Status of a task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Checkpointing,
    Completed,
    Failed,
    Cancelled,
}

/// State of a task within a DAG workflow
/// This provides more fine-grained state tracking for task dependencies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum TaskState {
    /// Task is pending, dependencies not yet checked
    #[default]
    Pending,
    /// Task is waiting for dependencies to complete
    Waiting,
    /// All dependencies completed, task is ready to be scheduled
    Ready,
    /// Task is currently running on a worker
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed (may be retried or marked as terminal)
    Failed,
    /// Task was skipped due to failed dependencies
    Skipped,
    /// Task was cancelled
    Cancelled,
}


impl TaskState {
    /// Check if this state is terminal (no more state changes expected)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
        )
    }

    /// Check if task can be scheduled
    pub fn is_schedulable(&self) -> bool {
        matches!(self, TaskState::Ready)
    }
}

/// A job consisting of multiple tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub name: String,
    pub tasks: Vec<TaskId>,
    pub status: JobStatus,
    pub priority: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
    /// Tenant ID for multi-tenancy isolation
    /// Jobs are scoped to a tenant and can only access resources within that tenant
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<crate::tenancy::TenantId>,
    /// User ID who submitted the job
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_by: Option<String>,
}

impl Job {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            id: JobId::new(),
            name: name.into(),
            tasks: Vec::new(),
            status: JobStatus::Pending,
            priority: 0,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            metadata: serde_json::Value::Null,
            tenant_id: None,
            submitted_by: None,
        }
    }

    /// Creates a job with tenant context.
    pub fn with_tenant(name: impl Into<String>, tenant_id: crate::tenancy::TenantId) -> Self {
        let mut job = Self::new(name);
        job.tenant_id = Some(tenant_id);
        job
    }

    /// Sets the tenant for this job.
    pub fn set_tenant(&mut self, tenant_id: crate::tenancy::TenantId) {
        self.tenant_id = Some(tenant_id);
    }

    /// Sets who submitted this job.
    pub fn set_submitted_by(&mut self, user_id: impl Into<String>) {
        self.submitted_by = Some(user_id.into());
    }

    /// Checks if this job belongs to a specific tenant.
    pub fn belongs_to_tenant(&self, tenant_id: &crate::tenancy::TenantId) -> bool {
        self.tenant_id.as_ref() == Some(tenant_id)
    }
}

/// A task - the unit of work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub job_id: JobId,
    pub payload: TaskPayload,
    pub status: TaskStatus,
    /// Fine-grained state for DAG workflow tracking
    #[serde(default)]
    pub state: TaskState,
    pub assigned_worker: Option<WorkerId>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub checkpoints: Vec<CheckpointRef>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<TaskResult>,
    /// Task dependencies - IDs of tasks that must complete before this task can run
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
    /// Optional human-readable name for the task
    #[serde(default)]
    pub name: Option<String>,
    /// Input data references for locality-aware scheduling
    /// Tasks will preferentially be scheduled on workers that have this data locally
    #[serde(default)]
    pub input_data: Vec<DataRef>,
    /// Data placement hints for scheduling optimization
    #[serde(default)]
    pub placement_hint: Option<DataPlacementHint>,
}

impl Task {
    pub fn new(job_id: JobId, payload: TaskPayload) -> Self {
        Self {
            id: TaskId::new(),
            job_id,
            payload,
            status: TaskStatus::Pending,
            state: TaskState::Pending,
            assigned_worker: None,
            attempts: 0,
            max_attempts: 3,
            checkpoints: Vec::new(),
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            result: None,
            depends_on: Vec::new(),
            name: None,
            input_data: Vec::new(),
            placement_hint: None,
        }
    }

    /// Create a task with dependencies
    pub fn with_dependencies(job_id: JobId, payload: TaskPayload, depends_on: Vec<TaskId>) -> Self {
        let mut task = Self::new(job_id, payload);
        task.depends_on = depends_on;
        // Tasks with dependencies start in Waiting state
        if !task.depends_on.is_empty() {
            task.state = TaskState::Waiting;
        } else {
            task.state = TaskState::Ready;
        }
        task
    }

    /// Create a named task with dependencies
    pub fn with_name_and_dependencies(
        job_id: JobId,
        payload: TaskPayload,
        name: impl Into<String>,
        depends_on: Vec<TaskId>,
    ) -> Self {
        let mut task = Self::with_dependencies(job_id, payload, depends_on);
        task.name = Some(name.into());
        task
    }

    /// Create a task with input data for locality-aware scheduling
    pub fn with_input_data(job_id: JobId, payload: TaskPayload, input_data: Vec<DataRef>) -> Self {
        let mut task = Self::new(job_id, payload);
        task.input_data = input_data;
        task
    }

    /// Create a task with both dependencies and input data
    pub fn with_dependencies_and_data(
        job_id: JobId,
        payload: TaskPayload,
        depends_on: Vec<TaskId>,
        input_data: Vec<DataRef>,
    ) -> Self {
        let mut task = Self::with_dependencies(job_id, payload, depends_on);
        task.input_data = input_data;
        task
    }

    /// Add input data reference to this task
    pub fn add_input_data(&mut self, data_ref: DataRef) {
        self.input_data.push(data_ref);
    }

    /// Set placement hint for this task
    pub fn with_placement_hint(mut self, hint: DataPlacementHint) -> Self {
        self.placement_hint = Some(hint);
        self
    }

    /// Check if all dependencies are satisfied
    pub fn dependencies_satisfied(
        &self,
        completed_tasks: &std::collections::HashSet<TaskId>,
    ) -> bool {
        self.depends_on
            .iter()
            .all(|dep| completed_tasks.contains(dep))
    }

    /// Check if this task has any dependencies
    pub fn has_dependencies(&self) -> bool {
        !self.depends_on.is_empty()
    }

    /// Check if this task has data locality requirements
    pub fn has_data_requirements(&self) -> bool {
        !self.input_data.is_empty()
    }

    /// Get data references that are required (must be local)
    pub fn required_data(&self) -> Vec<&DataRef> {
        self.input_data
            .iter()
            .filter(|d| d.preference == LocalityPreference::Required)
            .collect()
    }

    /// Get data references that are preferred (should be local if possible)
    pub fn preferred_data(&self) -> Vec<&DataRef> {
        self.input_data
            .iter()
            .filter(|d| d.preference == LocalityPreference::Preferred)
            .collect()
    }
}

/// Task payload - what to execute
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskPayload {
    /// Shell command execution
    Shell { command: String, args: Vec<String> },
    /// Python script
    Python { script: String, args: Vec<String> },
    /// Generic function call with serialized input
    Function { name: String, input: std::sync::Arc<Vec<u8>> },
    /// Direct WASM Execution (Serverless)
    Wasm { 
        #[serde(skip)]
        wasm_bytes: std::sync::Arc<Vec<u8>>, 
        #[serde(default)]
        wasm_hash: Option<crate::swarm::types::BlobHash>,
        input: std::sync::Arc<Vec<u8>>,
        /// S3/Datalake URI for streaming massive datasets (e.g. 350PB) into the WASI sandbox
        /// Bypasses the 4GB linear memory limit of wasm32.
        #[serde(default)]
        dataset_shard_uri: Option<String>,
        /// Ahead-Of-Time (AOT) compiled binaries for specific architectures 
        /// (e.g. "x86_64-linux" -> serialized native machine code), populated 
        /// by the "Queen Ant" digestion node to completely eradicate JIT warm-up 
        /// latency on edge nodes.
        #[serde(skip)]
        aot_digested_blobs: Option<std::collections::HashMap<String, std::sync::Arc<Vec<u8>>>>,
    },
    /// Monte Carlo simulation
    MonteCarlo {
        seed: u64,
        iterations: u64,
        params: serde_json::Value,
        /// S3/Datalake URI for streaming the massive dataset configuration
        #[serde(default)]
        dataset_shard_uri: Option<String>,
    },
    /// Parameter sweep
    ParameterSweep {
        param_name: String,
        param_value: serde_json::Value,
        base_config: serde_json::Value,
    },
    /// 100% Software-Only Blind Computing (FHE / ZKP execution).
    /// Operates purely on FHE (Fully Homomorphic Encryption) ciphertexts via Host handles.
    BlindComputation {
        /// The WASM binary containing the blind logic (operations mapped to host handles).
        #[serde(skip)]
        wasm_bytes: std::sync::Arc<Vec<u8>>,
        /// Decoupled Data Plane: The hash of the multi-gigabyte FHE Evaluation Key (Server Key)
        /// The payload is streamed via StigmergicFetch, bypassing Bincode serialization.
        fhe_eval_key_hash: crate::swarm::types::BlobHash,
        /// Decoupled Data Plane: The hash of the encrypted inputs (Ciphertexts) 
        /// to be processed by the WASM payload.
        encrypted_inputs_hash: crate::swarm::types::BlobHash,
    },
    /// Long-running service (e.g. web server, database proxy)
    Service {
        #[serde(skip)]
        wasm_bytes: std::sync::Arc<Vec<u8>>,
        env: std::collections::HashMap<String, String>,
    },
    /// Executes a third-party plugin (e.g. DiLoCo PyTorch trainer, Genome Sequencer).
    /// The Core binary acts only as the network and settlement layer, communicating with the plugin via IPC.
    Plugin {
        /// The unique identifier of the plugin (e.g., "marabunta-diloco-python")
        plugin_id: String,
        /// The plugin execution payload (usually a script, or container configuration)
        executable_bytes: std::sync::Arc<Vec<u8>>,
        /// JSON-serialized configuration injected into the plugin environment
        config: serde_json::Value,
        /// List of required network data shards (BlobHashes) the core must download before booting the plugin
        required_blobs: Vec<crate::swarm::types::BlobHash>,
    },
}

impl Default for TaskPayload {
    fn default() -> Self {
        Self::Shell {
            command: "echo 'Marabunta unit of work'".to_string(),
            args: vec![],
        }
    }
}

/// Reference to a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub size_bytes: u64,
    pub location: CheckpointLocation,
}

/// Where a checkpoint is stored
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CheckpointLocation {
    Memory,
    LocalDisk { path: String },
    Remote { url: String },
}

/// Result of task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub success: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub output: Option<Vec<u8>>,
    pub duration_ms: u64,
}

/// Information about a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub address: String,
    pub region: RegionId,
    pub capabilities: WorkerCapabilities,
    pub status: WorkerStatus,
    pub current_load: f64,
    pub running_tasks: Vec<TaskId>,
    pub last_heartbeat: DateTime<Utc>,
}

/// Worker capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub has_gpu: bool,
    pub supported_tasks: Vec<String>,
}

/// Worker status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    Starting,
    Ready,
    Busy,
    Draining,
    Offline,
}

/// Information about a master
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterInfo {
    pub id: MasterId,
    pub address: String,
    pub region: RegionId,
    pub status: MasterStatus,
    pub worker_count: u32,
    pub pending_tasks: u32,
    pub last_heartbeat: DateTime<Utc>,
}

/// Master status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MasterStatus {
    Starting,
    Ready,
    Degraded,
    Offline,
}

/// Information about a region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionInfo {
    pub id: RegionId,
    pub name: String,
    pub masters: Vec<MasterId>,
    pub total_workers: u32,
    pub available_capacity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    pub task_id: TaskId,
    pub state: Vec<u8>,
    pub checksum: String,
    pub created_at: DateTime<Utc>,
}

// ============================================================================
// Holographic Topology (War Room Blueprint)
// ============================================================================

/// A synthetic swarm definition for large-scale simulation.
///
/// Allows users to "paint" swarms of 100M+ nodes without physically owning them.
/// This blueprint can be simulated in the War Room and eventually used to 
/// constrain real job deployments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HolographicTopology {
    pub id: TopologyId,
    pub name: String,
    pub regions: Vec<HolographicRegion>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HolographicRegion {
    pub region_name: String,
    pub node_groups: Vec<HolographicNodeGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HolographicNodeGroup {
    pub name: String,
    pub count: u64,
    pub hardware: HolographicHardware,
    pub behavior: HolographicBehavior,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HolographicHardware {
    pub cpu_cores: u32,
    pub ram_mb: u64,
    pub gpu_model: Option<String>,
    pub disk_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HolographicBehavior {
    /// Thermal profile (e.g. "aggressive_throttling", "datacenter_cooled").
    pub thermal_profile: String,
    /// Probability of the node being online (0.0 to 1.0).
    pub availability: f32,
    /// Probability of the node producing correct math (0.0 to 1.0).
    pub reliability: f32,
}
