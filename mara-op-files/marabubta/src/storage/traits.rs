// Marabunta - Licensed under the MIT License.
//! Storage trait definitions

use async_trait::async_trait;

use crate::common::types::*;
use crate::common::MarabuntaError;

/// State store for cluster state
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Store a value
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), MarabuntaError>;

    /// Get a value
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, MarabuntaError>;

    /// Delete a value
    async fn delete(&self, key: &str) -> Result<(), MarabuntaError>;

    /// Check if key exists
    async fn exists(&self, key: &str) -> Result<bool, MarabuntaError>;

    /// List keys with prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>, MarabuntaError>;

    /// Set with TTL
    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl_secs: u64) -> Result<(), MarabuntaError>;
}

/// Checkpoint store for task checkpoints
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    
    /// Save a checkpoint
    async fn save(&self, id: &str, data: &crate::common::types::CheckpointData) -> Result<(), MarabuntaError>;

    /// Load a checkpoint
    async fn load(&self, id: &str) -> Result<Option<crate::common::types::CheckpointData>, MarabuntaError>;

    /// Delete a checkpoint
    async fn delete(&self, id: &str) -> Result<(), MarabuntaError>;

    /// List checkpoints for a task
    async fn list_for_task(&self, task_id: TaskId) -> Result<Vec<String>, MarabuntaError>;

    /// Get checkpoint metadata without loading full state
    async fn get_metadata(&self, id: &str) -> Result<Option<CheckpointMetadata>, MarabuntaError>;
}

/// Checkpoint metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckpointMetadata {
    pub id: String,
    pub task_id: TaskId,
    pub size_bytes: u64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub checksum: String,
}

/// Job store for job metadata
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Save a job
    async fn save_job(&self, job: &Job) -> Result<(), MarabuntaError>;

    /// Get a job
    async fn get_job(&self, id: JobId) -> Result<Option<Job>, MarabuntaError>;

    /// Update job status
    async fn update_job_status(&self, id: JobId, status: JobStatus) -> Result<(), MarabuntaError>;

    /// Save a task
    async fn save_task(&self, task: &Task) -> Result<(), MarabuntaError>;

    /// Get a task
    async fn get_task(&self, id: TaskId) -> Result<Option<Task>, MarabuntaError>;

    /// Update task status
    async fn update_task_status(&self, id: TaskId, status: TaskStatus) -> Result<(), MarabuntaError>;

    /// Get tasks for job
    async fn get_tasks_for_job(&self, job_id: JobId) -> Result<Vec<Task>, MarabuntaError>;

    /// List jobs by status
    async fn list_jobs_by_status(&self, status: JobStatus) -> Result<Vec<Job>, MarabuntaError>;
}
