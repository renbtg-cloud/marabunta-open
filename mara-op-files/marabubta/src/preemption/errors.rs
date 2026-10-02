// Marabunta - Licensed under the MIT License.
//! Error types for the preemption engine

use thiserror::Error;

use super::types::{JobId, NodeId, TaskId};

/// Errors that can occur during preemption operations
#[derive(Error, Debug, Clone)]
pub enum PreemptionError {
    #[error("Task not found: {0}")]
    TaskNotFound(TaskId),

    #[error("Task not preemptible: {task_id} - {reason}")]
    TaskNotPreemptible { task_id: TaskId, reason: String },

    #[error("Policy not found: {0}")]
    PolicyNotFound(String),

    #[error("Policy already exists: {0}")]
    PolicyAlreadyExists(String),

    #[error("Invalid policy: {0}")]
    InvalidPolicy(String),

    #[error("Checkpoint failed for task {task_id}: {error}")]
    CheckpointFailed { task_id: TaskId, error: String },

    #[error("Kill failed for task {task_id}: {error}")]
    KillFailed { task_id: TaskId, error: String },

    #[error("Requeue failed for task {task_id}: {error}")]
    RequeueFailed { task_id: TaskId, error: String },

    #[error("Migration failed for task {task_id}: {error}")]
    MigrationFailed { task_id: TaskId, error: String },

    #[error("Preemption timeout for task {0}")]
    Timeout(TaskId),

    #[error("Preemption cancelled for task {0}")]
    Cancelled(TaskId),

    #[error("Insufficient resources: need {needed}, available {available}")]
    InsufficientResources { needed: String, available: String },

    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("Approval required from: {approvers:?}")]
    ApprovalRequired { approvers: Vec<String> },

    #[error("Approval timeout")]
    ApprovalTimeout,

    #[error("Approval denied by: {0}")]
    ApprovalDenied(String),

    #[error("Job not found: {0}")]
    JobNotFound(JobId),

    #[error("In cooldown period until {until}")]
    InCooldown { until: String },

    #[error("Max preemptions reached: {count}")]
    MaxPreemptionsReached { count: u32 },

    #[error("Protected by constraint: {0}")]
    ProtectedByConstraint(String),

    #[error("In blackout window until {until}")]
    InBlackoutWindow { until: String },

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type PreemptionResult<T> = Result<T, PreemptionError>;
