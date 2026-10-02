// Marabunta - Licensed under the MIT License.
//! Local filesystem storage implementations (for dev/testing)

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::fs;
use tracing::debug;

use crate::common::types::*;
use crate::common::MarabuntaError;
use crate::storage::traits::*;

/// In-memory state store for testing
pub struct MemoryStateStore {
    data: RwLock<HashMap<String, (Vec<u8>, Option<std::time::Instant>)>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), MarabuntaError> {
        self.data
            .write()
            .insert(key.to_string(), (value.to_vec(), None));
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, MarabuntaError> {
        let data = self.data.read();
        if let Some((value, ttl)) = data.get(key) {
            if let Some(expiry) = ttl {
                if expiry.elapsed() > std::time::Duration::ZERO {
                    return Ok(None); // Expired
                }
            }
            Ok(Some(value.clone()))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, key: &str) -> Result<(), MarabuntaError> {
        self.data.write().remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool, MarabuntaError> {
        Ok(self.data.read().contains_key(key))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, MarabuntaError> {
        Ok(self
            .data
            .read()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl_secs: u64) -> Result<(), MarabuntaError> {
        let expiry = std::time::Instant::now() + std::time::Duration::from_secs(ttl_secs);
        self.data
            .write()
            .insert(key.to_string(), (value.to_vec(), Some(expiry)));
        Ok(())
    }
}

/// Local filesystem checkpoint store
pub struct LocalCheckpointStore {
    base_dir: PathBuf,
}

impl LocalCheckpointStore {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    fn checkpoint_path(&self, id: &str) -> PathBuf {
        self.base_dir.join(format!("{}.ckpt", id))
    }
}

#[async_trait]
impl CheckpointStore for LocalCheckpointStore {
    async fn save(&self, id: &str, data: &crate::common::types::CheckpointData) -> Result<(), MarabuntaError> {
        fs::create_dir_all(&self.base_dir).await?;
        let path = self.checkpoint_path(id);
        let bytes = serde_json::to_vec(data)?;
        fs::write(path, bytes).await?;
        debug!("Saved checkpoint {} ({} bytes)", id, data.state.len());
        Ok(())
    }

    async fn load(&self, id: &str) -> Result<Option<crate::common::types::CheckpointData>, MarabuntaError> {
        let path = self.checkpoint_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).await?;
        let data: crate::common::types::CheckpointData = serde_json::from_slice(&bytes)?;
        Ok(Some(data))
    }

    async fn delete(&self, id: &str) -> Result<(), MarabuntaError> {
        let path = self.checkpoint_path(id);
        if path.exists() {
            fs::remove_file(path).await?;
        }
        Ok(())
    }

    async fn list_for_task(&self, task_id: TaskId) -> Result<Vec<String>, MarabuntaError> {
        let prefix = format!("{}_", task_id.0);
        let mut results = Vec::new();

        if let Ok(mut entries) = fs::read_dir(&self.base_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(&prefix) && name.ends_with(".ckpt") {
                    results.push(name.trim_end_matches(".ckpt").to_string());
                }
            }
        }

        Ok(results)
    }

    async fn get_metadata(&self, id: &str) -> Result<Option<CheckpointMetadata>, MarabuntaError> {
        if let Some(data) = self.load(id).await? {
            Ok(Some(CheckpointMetadata {
                id: id.to_string(),
                task_id: data.task_id,
                size_bytes: data.state.len() as u64,
                created_at: data.created_at,
                checksum: data.checksum,
            }))
        } else {
            Ok(None)
        }
    }
}

/// Local filesystem job store
pub struct LocalJobStore {
    base_dir: PathBuf,
}

impl LocalJobStore {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    fn job_path(&self, id: JobId) -> PathBuf {
        self.base_dir.join("jobs").join(format!("{}.json", id.0))
    }

    fn task_path(&self, id: TaskId) -> PathBuf {
        self.base_dir.join("tasks").join(format!("{}.json", id.0))
    }
}

#[async_trait]
impl JobStore for LocalJobStore {
    async fn save_job(&self, job: &Job) -> Result<(), MarabuntaError> {
        let path = self.job_path(job.id);
        fs::create_dir_all(path.parent().unwrap()).await?;
        let bytes = serde_json::to_vec_pretty(job)?;
        fs::write(path, bytes).await?;
        Ok(())
    }

    async fn get_job(&self, id: JobId) -> Result<Option<Job>, MarabuntaError> {
        let path = self.job_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).await?;
        let job: Job = serde_json::from_slice(&bytes)?;
        Ok(Some(job))
    }

    async fn update_job_status(&self, id: JobId, status: JobStatus) -> Result<(), MarabuntaError> {
        if let Some(mut job) = self.get_job(id).await? {
            job.status = status;
            self.save_job(&job).await?;
        }
        Ok(())
    }

    async fn save_task(&self, task: &Task) -> Result<(), MarabuntaError> {
        let path = self.task_path(task.id);
        fs::create_dir_all(path.parent().unwrap()).await?;
        let bytes = serde_json::to_vec_pretty(task)?;
        fs::write(path, bytes).await?;
        Ok(())
    }

    async fn get_task(&self, id: TaskId) -> Result<Option<Task>, MarabuntaError> {
        let path = self.task_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path).await?;
        let task: Task = serde_json::from_slice(&bytes)?;
        Ok(Some(task))
    }

    async fn update_task_status(&self, id: TaskId, status: TaskStatus) -> Result<(), MarabuntaError> {
        if let Some(mut task) = self.get_task(id).await? {
            task.status = status;
            self.save_task(&task).await?;
        }
        Ok(())
    }

    async fn get_tasks_for_job(&self, job_id: JobId) -> Result<Vec<Task>, MarabuntaError> {
        let tasks_dir = self.base_dir.join("tasks");
        let mut tasks = Vec::new();

        if let Ok(mut entries) = fs::read_dir(&tasks_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry
                    .path()
                    .extension()
                    .map(|e| e == "json")
                    .unwrap_or(false)
                {
                    let bytes = fs::read(entry.path()).await?;
                    if let Ok(task) = serde_json::from_slice::<Task>(&bytes) {
                        if task.job_id == job_id {
                            tasks.push(task);
                        }
                    }
                }
            }
        }

        Ok(tasks)
    }

    async fn list_jobs_by_status(&self, status: JobStatus) -> Result<Vec<Job>, MarabuntaError> {
        let jobs_dir = self.base_dir.join("jobs");
        let mut jobs = Vec::new();

        if let Ok(mut entries) = fs::read_dir(&jobs_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry
                    .path()
                    .extension()
                    .map(|e| e == "json")
                    .unwrap_or(false)
                {
                    let bytes = fs::read(entry.path()).await?;
                    if let Ok(job) = serde_json::from_slice::<Job>(&bytes) {
                        if job.status == status {
                            jobs.push(job);
                        }
                    }
                }
            }
        }

        Ok(jobs)
    }
}
