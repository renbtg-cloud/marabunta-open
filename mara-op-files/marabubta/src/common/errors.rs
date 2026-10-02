// Marabunta - Licensed under the MIT License.
//! Error types for the framework

use thiserror::Error;

use crate::common::types::{JobId, MasterId, TaskId, WorkerId};

/// Main error type for the marabunta compute framework
#[derive(Error, Debug)]
pub enum MarabuntaError {
    // Job errors
    #[error("Job not found: {0}")]
    JobNotFound(JobId),

    #[error("Job already exists: {0}")]
    JobAlreadyExists(JobId),

    #[error("Job in invalid state for operation: {0}")]
    InvalidJobState(JobId),

    // Task errors
    #[error("Task not found: {0}")]
    TaskNotFound(TaskId),

    #[error("Task execution failed: {0}")]
    TaskExecutionFailed(String),

    #[error("Task timeout")]
    TaskTimeout,

    #[error("Max retries exceeded for task: {0}")]
    MaxRetriesExceeded(TaskId),

    // Worker errors
    #[error("Worker not found: {0}")]
    WorkerNotFound(WorkerId),

    #[error("Worker unavailable: {0}")]
    WorkerUnavailable(WorkerId),

    #[error("No workers available")]
    NoWorkersAvailable,

    // Master errors
    #[error("Master not found: {0}")]
    MasterNotFound(MasterId),

    #[error("Master unavailable: {0}")]
    MasterUnavailable(MasterId),

    #[error("No masters available")]
    NoMastersAvailable,

    // Cluster errors
    #[error("Cluster not ready")]
    ClusterNotReady,

    #[error("Raft consensus failed: {0}")]
    RaftError(String),

    #[error("Leader not elected")]
    NoLeader,

    // Storage errors
    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Checkpoint not found: {0}")]
    CheckpointNotFound(String),

    #[error("Checkpoint corrupted: {0}")]
    CheckpointCorrupted(String),

    // Network errors
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    // Config errors
    #[error("Configuration error: {0}")]
    ConfigError(String),

    // Generic errors
    #[error("Internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

pub type MarabuntaResult<T> = Result<T, MarabuntaError>;
