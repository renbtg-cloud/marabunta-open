// Marabunta - Licensed under the MIT License.
//! Message types for all inter-component communication

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::common::types::*;

fn default_thermal_state() -> String {
    "normal".to_string()
}

/// Messages from Coordinator to Master
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoordinatorToMaster {
    /// Assign a job to this master's region
    AssignJob { job: Job, tasks: Vec<Task> },

    /// Heartbeat acknowledgment
    HeartbeatAck { timestamp: DateTime<Utc> },

    /// Request state sync
    RequestStateSync,

    /// Cluster configuration update
    ConfigUpdate { masters: Vec<MasterInfo> },

    /// Cancel a job
    CancelJob { job_id: JobId },

    /// Reassign tasks from failed worker
    ReassignTasks { tasks: Vec<Task> },
}

/// Messages from Master to Coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MasterToCoordinator {
    /// Register this master
    Register { info: MasterInfo },

    /// Heartbeat with status
    Heartbeat {
        master_id: MasterId,
        status: MasterStatus,
        worker_count: u32,
        pending_tasks: u32,
        running_tasks: u32,
    },

    /// Job completed successfully
    JobComplete {
        job_id: JobId,
        results: Vec<TaskResult>,
    },

    /// Job failed
    JobFailed { job_id: JobId, reason: String },

    /// Task status update
    TaskStatusUpdate {
        task_id: TaskId,
        status: TaskStatus,
        worker_id: Option<WorkerId>,
    },

    /// Worker failure report
    WorkerFailed {
        worker_id: WorkerId,
        affected_tasks: Vec<TaskId>,
    },
}

/// Messages from Master to Worker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MasterToWorker {
    /// Assign a task
    AssignTask { task: Task },

    /// Cancel a task
    CancelTask { task_id: TaskId },

    /// Request checkpoint
    RequestCheckpoint { task_id: TaskId },

    /// Heartbeat acknowledgment
    HeartbeatAck { timestamp: DateTime<Utc> },

    /// Shutdown request
    Shutdown { reason: String },
}

/// Messages from Worker to Master
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerToMaster {
    /// Register this worker
    Register { info: WorkerInfo },

    /// Heartbeat with status
    Heartbeat {
        worker_id: WorkerId,
        status: WorkerStatus,
        load: f64,
        running_tasks: Vec<TaskId>,
        available_memory_mb: u64,
        available_cpu: f64,
        /// Current thermal state: "normal", "elevated", "high", or "critical"
        #[serde(default = "default_thermal_state")]
        thermal_state: String,
    },

    /// Task started
    TaskStarted { task_id: TaskId },

    /// Task completed
    TaskComplete { task_id: TaskId, result: TaskResult },

    /// Task failed
    TaskFailed {
        task_id: TaskId,
        reason: String,
        retryable: bool,
    },

    /// Checkpoint created
    CheckpointReady {
        task_id: TaskId,
        checkpoint: CheckpointRef,
    },

    /// Task progress update
    TaskProgress {
        task_id: TaskId,
        progress: f64,
        message: Option<String>,
    },
}

/// Raft messages for coordinator consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RaftMessage {
    /// Request vote
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },

    /// Vote response
    VoteResponse { term: u64, vote_granted: bool },

    /// Append entries
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },

    /// Append entries response
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
}

/// Raft log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

/// Commands that can be replicated via Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RaftCommand {
    /// Register a master
    RegisterMaster { info: MasterInfo },
    /// Deregister a master
    DeregisterMaster { master_id: MasterId },
    /// Submit a job
    SubmitJob { job: Job, tasks: Vec<Task> },
    /// Update job status
    UpdateJobStatus { job_id: JobId, status: JobStatus },
    /// Update task status
    UpdateTaskStatus { task_id: TaskId, status: TaskStatus },
}

/// Envelope for all messages with routing info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub id: uuid::Uuid,
    pub timestamp: DateTime<Utc>,
    pub source: String,
    pub destination: String,
    pub payload: MessagePayload,
}

/// All possible message payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "category", rename_all = "snake_case")]
pub enum MessagePayload {
    CoordinatorToMaster(CoordinatorToMaster),
    MasterToCoordinator(MasterToCoordinator),
    MasterToWorker(MasterToWorker),
    WorkerToMaster(WorkerToMaster),
    Raft(RaftMessage),
}

impl MessageEnvelope {
    pub fn new(
        source: impl Into<String>,
        destination: impl Into<String>,
        payload: MessagePayload,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            timestamp: Utc::now(),
            source: source.into(),
            destination: destination.into(),
            payload,
        }
    }
}
