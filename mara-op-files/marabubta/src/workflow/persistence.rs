// Marabunta - Licensed under the MIT License.
//! Workflow Persistence
//!
//! Save and load workflow definitions and execution state.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::storage::{PersistenceBackend, PersistenceError, TypedStore};
use crate::workflow::types::*;

// ============================================================================
// Store Error
// ============================================================================

/// Errors that can occur during workflow storage operations
#[derive(Debug, thiserror::Error)]
pub enum WorkflowStoreError {
    #[error("Workflow not found: {0}")]
    WorkflowNotFound(WorkflowId),

    #[error("Run not found: {0}")]
    RunNotFound(WorkflowRunId),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Validation error: {0}")]
    Validation(#[from] WorkflowValidationError),
}

impl From<PersistenceError> for WorkflowStoreError {
    fn from(e: PersistenceError) -> Self {
        WorkflowStoreError::Storage(e.to_string())
    }
}

impl From<serde_json::Error> for WorkflowStoreError {
    fn from(e: serde_json::Error) -> Self {
        WorkflowStoreError::Serialization(e.to_string())
    }
}

// ============================================================================
// Workflow Store Trait
// ============================================================================

/// Trait for workflow storage operations
#[async_trait]
pub trait WorkflowStore: Send + Sync + 'static {
    // Workflow definition operations

    /// Save a workflow definition
    async fn save_workflow(&self, workflow: &Workflow) -> Result<(), WorkflowStoreError>;

    /// Get a workflow by ID
    async fn get_workflow(&self, id: WorkflowId) -> Result<Option<Workflow>, WorkflowStoreError>;

    /// List all workflows
    async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, WorkflowStoreError>;

    /// Delete a workflow
    async fn delete_workflow(&self, id: WorkflowId) -> Result<bool, WorkflowStoreError>;

    /// Find workflows by name (partial match)
    async fn find_workflows_by_name(
        &self,
        name_pattern: &str,
    ) -> Result<Vec<WorkflowSummary>, WorkflowStoreError>;

    /// Find workflows by tag
    async fn find_workflows_by_tag(
        &self,
        tag: &str,
    ) -> Result<Vec<WorkflowSummary>, WorkflowStoreError>;

    // Workflow run operations

    /// Save a workflow run
    async fn save_run(&self, run: &WorkflowRun) -> Result<(), WorkflowStoreError>;

    /// Get a run by ID
    async fn get_run(&self, id: WorkflowRunId) -> Result<Option<WorkflowRun>, WorkflowStoreError>;

    /// List runs for a workflow
    async fn list_runs_for_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<RunSummary>, WorkflowStoreError>;

    /// List all active runs
    async fn list_active_runs(&self) -> Result<Vec<RunSummary>, WorkflowStoreError>;

    /// Delete a run
    async fn delete_run(&self, id: WorkflowRunId) -> Result<bool, WorkflowStoreError>;

    /// Get runs by status
    async fn get_runs_by_status(
        &self,
        status: WorkflowStatus,
    ) -> Result<Vec<RunSummary>, WorkflowStoreError>;
}

// ============================================================================
// Summary Types
// ============================================================================

/// Summary information about a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSummary {
    pub id: WorkflowId,
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub step_count: usize,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<&Workflow> for WorkflowSummary {
    fn from(workflow: &Workflow) -> Self {
        Self {
            id: workflow.id,
            name: workflow.name.clone(),
            version: workflow.version.clone(),
            description: workflow.description.clone(),
            step_count: workflow.steps.len(),
            tags: workflow.tags.clone(),
            created_at: workflow.created_at,
            updated_at: workflow.updated_at,
        }
    }
}

/// Summary information about a workflow run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub id: WorkflowRunId,
    pub workflow_id: WorkflowId,
    pub workflow_version: String,
    pub status: WorkflowStatus,
    pub progress: f64,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

impl From<&WorkflowRun> for RunSummary {
    fn from(run: &WorkflowRun) -> Self {
        Self {
            id: run.id,
            workflow_id: run.workflow_id,
            workflow_version: run.workflow_version.clone(),
            status: run.status,
            progress: run.progress(),
            started_at: run.started_at,
            completed_at: run.completed_at,
            error: run.error.clone(),
        }
    }
}

// ============================================================================
// In-Memory Implementation
// ============================================================================

/// In-memory workflow store for testing and development
pub struct MemoryWorkflowStore {
    workflows: RwLock<HashMap<WorkflowId, Workflow>>,
    runs: RwLock<HashMap<WorkflowRunId, WorkflowRun>>,
}

impl MemoryWorkflowStore {
    pub fn new() -> Self {
        Self {
            workflows: RwLock::new(HashMap::new()),
            runs: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryWorkflowStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WorkflowStore for MemoryWorkflowStore {
    async fn save_workflow(&self, workflow: &Workflow) -> Result<(), WorkflowStoreError> {
        self.workflows.write().insert(workflow.id, workflow.clone());
        Ok(())
    }

    async fn get_workflow(&self, id: WorkflowId) -> Result<Option<Workflow>, WorkflowStoreError> {
        Ok(self.workflows.read().get(&id).cloned())
    }

    async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, WorkflowStoreError> {
        Ok(self
            .workflows
            .read()
            .values()
            .map(WorkflowSummary::from)
            .collect())
    }

    async fn delete_workflow(&self, id: WorkflowId) -> Result<bool, WorkflowStoreError> {
        Ok(self.workflows.write().remove(&id).is_some())
    }

    async fn find_workflows_by_name(
        &self,
        name_pattern: &str,
    ) -> Result<Vec<WorkflowSummary>, WorkflowStoreError> {
        let pattern = name_pattern.to_lowercase();
        Ok(self
            .workflows
            .read()
            .values()
            .filter(|w| w.name.to_lowercase().contains(&pattern))
            .map(WorkflowSummary::from)
            .collect())
    }

    async fn find_workflows_by_tag(
        &self,
        tag: &str,
    ) -> Result<Vec<WorkflowSummary>, WorkflowStoreError> {
        Ok(self
            .workflows
            .read()
            .values()
            .filter(|w| w.tags.contains(&tag.to_string()))
            .map(WorkflowSummary::from)
            .collect())
    }

    async fn save_run(&self, run: &WorkflowRun) -> Result<(), WorkflowStoreError> {
        self.runs.write().insert(run.id, run.clone());
        Ok(())
    }

    async fn get_run(&self, id: WorkflowRunId) -> Result<Option<WorkflowRun>, WorkflowStoreError> {
        Ok(self.runs.read().get(&id).cloned())
    }

    async fn list_runs_for_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<RunSummary>, WorkflowStoreError> {
        Ok(self
            .runs
            .read()
            .values()
            .filter(|r| r.workflow_id == workflow_id)
            .map(RunSummary::from)
            .collect())
    }

    async fn list_active_runs(&self) -> Result<Vec<RunSummary>, WorkflowStoreError> {
        Ok(self
            .runs
            .read()
            .values()
            .filter(|r| !r.is_terminal())
            .map(RunSummary::from)
            .collect())
    }

    async fn delete_run(&self, id: WorkflowRunId) -> Result<bool, WorkflowStoreError> {
        Ok(self.runs.write().remove(&id).is_some())
    }

    async fn get_runs_by_status(
        &self,
        status: WorkflowStatus,
    ) -> Result<Vec<RunSummary>, WorkflowStoreError> {
        Ok(self
            .runs
            .read()
            .values()
            .filter(|r| r.status == status)
            .map(RunSummary::from)
            .collect())
    }
}

// ============================================================================
// Persistent Implementation
// ============================================================================

/// Persistent workflow store using the storage backend
pub struct PersistentWorkflowStore<B: PersistenceBackend> {
    workflows: TypedStore<B>,
    runs: TypedStore<B>,
}

impl<B: PersistenceBackend + 'static> PersistentWorkflowStore<B> {
    /// Create a new persistent workflow store
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            workflows: TypedStore::from_arc(Arc::clone(&backend), "workflows"),
            runs: TypedStore::from_arc(backend, "workflow_runs"),
        }
    }

    /// Key for a workflow
    fn workflow_key(id: WorkflowId) -> String {
        format!("wf:{}", id.0)
    }

    /// Key for a run
    fn run_key(id: WorkflowRunId) -> String {
        format!("run:{}", id.0)
    }
}

#[async_trait]
impl<B: PersistenceBackend + 'static> WorkflowStore for PersistentWorkflowStore<B> {
    async fn save_workflow(&self, workflow: &Workflow) -> Result<(), WorkflowStoreError> {
        self.workflows
            .put(&Self::workflow_key(workflow.id), workflow)
            .await?;
        Ok(())
    }

    async fn get_workflow(&self, id: WorkflowId) -> Result<Option<Workflow>, WorkflowStoreError> {
        Ok(self.workflows.get(&Self::workflow_key(id)).await?)
    }

    async fn list_workflows(&self) -> Result<Vec<WorkflowSummary>, WorkflowStoreError> {
        let all: Vec<(String, Workflow)> = self.workflows.get_all().await?;
        Ok(all
            .into_iter()
            .map(|(_, w)| WorkflowSummary::from(&w))
            .collect())
    }

    async fn delete_workflow(&self, id: WorkflowId) -> Result<bool, WorkflowStoreError> {
        Ok(self.workflows.delete(&Self::workflow_key(id)).await?)
    }

    async fn find_workflows_by_name(
        &self,
        name_pattern: &str,
    ) -> Result<Vec<WorkflowSummary>, WorkflowStoreError> {
        let pattern = name_pattern.to_lowercase();
        let all: Vec<(String, Workflow)> = self.workflows.get_all().await?;
        Ok(all
            .into_iter()
            .filter(|(_, w)| w.name.to_lowercase().contains(&pattern))
            .map(|(_, w)| WorkflowSummary::from(&w))
            .collect())
    }

    async fn find_workflows_by_tag(
        &self,
        tag: &str,
    ) -> Result<Vec<WorkflowSummary>, WorkflowStoreError> {
        let all: Vec<(String, Workflow)> = self.workflows.get_all().await?;
        Ok(all
            .into_iter()
            .filter(|(_, w)| w.tags.contains(&tag.to_string()))
            .map(|(_, w)| WorkflowSummary::from(&w))
            .collect())
    }

    async fn save_run(&self, run: &WorkflowRun) -> Result<(), WorkflowStoreError> {
        self.runs.put(&Self::run_key(run.id), run).await?;
        Ok(())
    }

    async fn get_run(&self, id: WorkflowRunId) -> Result<Option<WorkflowRun>, WorkflowStoreError> {
        Ok(self.runs.get(&Self::run_key(id)).await?)
    }

    async fn list_runs_for_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<RunSummary>, WorkflowStoreError> {
        let all: Vec<(String, WorkflowRun)> = self.runs.get_all().await?;
        Ok(all
            .into_iter()
            .filter(|(_, r)| r.workflow_id == workflow_id)
            .map(|(_, r)| RunSummary::from(&r))
            .collect())
    }

    async fn list_active_runs(&self) -> Result<Vec<RunSummary>, WorkflowStoreError> {
        let all: Vec<(String, WorkflowRun)> = self.runs.get_all().await?;
        Ok(all
            .into_iter()
            .filter(|(_, r)| !r.is_terminal())
            .map(|(_, r)| RunSummary::from(&r))
            .collect())
    }

    async fn delete_run(&self, id: WorkflowRunId) -> Result<bool, WorkflowStoreError> {
        Ok(self.runs.delete(&Self::run_key(id)).await?)
    }

    async fn get_runs_by_status(
        &self,
        status: WorkflowStatus,
    ) -> Result<Vec<RunSummary>, WorkflowStoreError> {
        let all: Vec<(String, WorkflowRun)> = self.runs.get_all().await?;
        Ok(all
            .into_iter()
            .filter(|(_, r)| r.status == status)
            .map(|(_, r)| RunSummary::from(&r))
            .collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqliteBackend;

    #[tokio::test]
    async fn test_memory_store_workflow_crud() {
        let store = MemoryWorkflowStore::new();

        // Create
        let mut workflow = Workflow::new("test-workflow");
        workflow.add_step(WorkflowStep::job(
            "step1",
            crate::workflow::types::JobStepConfig::new("Test", "python3"),
        ));

        store.save_workflow(&workflow).await.unwrap();

        // Read
        let loaded = store.get_workflow(workflow.id).await.unwrap().unwrap();
        assert_eq!(loaded.name, "test-workflow");

        // List
        let list = store.list_workflows().await.unwrap();
        assert_eq!(list.len(), 1);

        // Delete
        assert!(store.delete_workflow(workflow.id).await.unwrap());
        assert!(store.get_workflow(workflow.id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_memory_store_run_crud() {
        let store = MemoryWorkflowStore::new();

        let workflow = Workflow::new("test");
        store.save_workflow(&workflow).await.unwrap();

        // Create run
        let run = WorkflowRun::new(&workflow);
        store.save_run(&run).await.unwrap();

        // Read
        let loaded = store.get_run(run.id).await.unwrap().unwrap();
        assert_eq!(loaded.workflow_id, workflow.id);

        // List for workflow
        let runs = store.list_runs_for_workflow(workflow.id).await.unwrap();
        assert_eq!(runs.len(), 1);

        // Active runs
        let active = store.list_active_runs().await.unwrap();
        assert_eq!(active.len(), 1);

        // Delete
        assert!(store.delete_run(run.id).await.unwrap());
    }

    #[tokio::test]
    async fn test_find_by_name() {
        let store = MemoryWorkflowStore::new();

        let mut w1 = Workflow::new("data-pipeline");
        w1.add_step(WorkflowStep::job(
            "s1",
            crate::workflow::types::JobStepConfig::new("T", "py"),
        ));
        store.save_workflow(&w1).await.unwrap();

        let mut w2 = Workflow::new("ml-training");
        w2.add_step(WorkflowStep::job(
            "s1",
            crate::workflow::types::JobStepConfig::new("T", "py"),
        ));
        store.save_workflow(&w2).await.unwrap();

        let results = store.find_workflows_by_name("pipe").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "data-pipeline");
    }

    #[tokio::test]
    async fn test_find_by_tag() {
        let store = MemoryWorkflowStore::new();

        let mut w1 = Workflow::new("workflow1");
        w1.tags = vec!["production".to_string(), "critical".to_string()];
        w1.add_step(WorkflowStep::job(
            "s1",
            crate::workflow::types::JobStepConfig::new("T", "py"),
        ));
        store.save_workflow(&w1).await.unwrap();

        let mut w2 = Workflow::new("workflow2");
        w2.tags = vec!["staging".to_string()];
        w2.add_step(WorkflowStep::job(
            "s1",
            crate::workflow::types::JobStepConfig::new("T", "py"),
        ));
        store.save_workflow(&w2).await.unwrap();

        let results = store.find_workflows_by_tag("production").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "workflow1");
    }

    #[tokio::test]
    async fn test_persistent_store() {
        let backend = Arc::new(SqliteBackend::in_memory().unwrap());
        let store = PersistentWorkflowStore::new(backend);

        let mut workflow = Workflow::new("persistent-test");
        workflow.add_step(WorkflowStep::job(
            "step1",
            crate::workflow::types::JobStepConfig::new("Test", "python3"),
        ));

        store.save_workflow(&workflow).await.unwrap();

        let loaded = store.get_workflow(workflow.id).await.unwrap().unwrap();
        assert_eq!(loaded.name, "persistent-test");

        let run = WorkflowRun::new(&workflow);
        store.save_run(&run).await.unwrap();

        let loaded_run = store.get_run(run.id).await.unwrap().unwrap();
        assert_eq!(loaded_run.workflow_id, workflow.id);
    }
}
