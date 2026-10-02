// Marabunta - Licensed under the MIT License.
//! Running task state tracking for the preemption engine

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::types::{JobId, NodeId, ResourceUsage, TaskId};

/// Information about a currently running task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningTask {
    pub task_id: TaskId,
    pub job_id: JobId,
    pub node_id: NodeId,

    /// When the task started
    pub started_at: DateTime<Utc>,
    /// Estimated completion time
    pub estimated_completion: Option<DateTime<Utc>>,
    /// Original time estimate for the task
    pub time_estimate: Option<Duration>,

    /// Task priority (higher = more important)
    pub priority: i32,
    /// Number of times this task has been preempted
    pub preemption_count: u32,
    /// Last time this task was preempted
    pub last_preempted_at: Option<DateTime<Utc>>,
    /// Whether this task can be preempted
    pub preemptible: bool,
    /// Optional cgroup ID for eBPF thermal preemption mapping
    pub cgroup_id: Option<u64>,

    /// Last checkpoint information
    pub last_checkpoint: Option<CheckpointInfo>,
    /// How often checkpoints are taken
    pub checkpoint_interval: Option<Duration>,

    /// Current resource usage
    pub resources: ResourceUsage,

    /// Who submitted this task
    pub submitter: String,
    /// Authority domain for governance
    pub authority_domain: String,

    /// Job type for type-based selection
    pub job_type: Option<String>,
}

impl RunningTask {
    /// Create a new running task
    pub fn new(
        task_id: impl Into<TaskId>,
        job_id: impl Into<JobId>,
        node_id: impl Into<NodeId>,
        priority: i32,
        resources: ResourceUsage,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            job_id: job_id.into(),
            node_id: node_id.into(),
            started_at: Utc::now(),
            estimated_completion: None,
            time_estimate: None,
            priority,
            preemption_count: 0,
            last_preempted_at: None,
            preemptible: true,
            cgroup_id: None,
            last_checkpoint: None,
            checkpoint_interval: None,
            resources,
            submitter: "unknown".to_string(),
            authority_domain: "default".to_string(),
            job_type: None,
        }
    }

    /// Get the runtime of this task so far
    pub fn runtime(&self) -> Duration {
        Utc::now() - self.started_at
    }

    /// Check if this task has exceeded its time estimate
    pub fn is_over_time_estimate(&self) -> bool {
        if let Some(estimate) = self.time_estimate {
            self.runtime() > estimate
        } else {
            false
        }
    }

    /// Get time since last checkpoint, or time since start if no checkpoint
    pub fn time_since_checkpoint(&self) -> Duration {
        if let Some(ref checkpoint) = self.last_checkpoint {
            Utc::now() - checkpoint.created_at
        } else {
            self.runtime()
        }
    }

    /// Estimate how much work would be lost if preempted now
    pub fn estimated_work_lost(&self) -> Duration {
        self.time_since_checkpoint()
    }

    /// Check if this task is in cooldown from a recent preemption
    pub fn is_in_cooldown(&self, cooldown: Duration) -> bool {
        if let Some(last_preempted) = self.last_preempted_at {
            Utc::now() - last_preempted < cooldown
        } else {
            false
        }
    }

    /// Get the cooldown end time if in cooldown
    pub fn cooldown_end(&self, cooldown: Duration) -> Option<DateTime<Utc>> {
        self.last_preempted_at.map(|t| t + cooldown)
    }

    /// Mark this task as preempted
    pub fn mark_preempted(&mut self) {
        self.preemption_count += 1;
        self.last_preempted_at = Some(Utc::now());
    }

    /// Set the checkpoint info
    pub fn set_checkpoint(&mut self, checkpoint: CheckpointInfo) {
        self.last_checkpoint = Some(checkpoint);
    }

    /// Check if this task has a recent valid checkpoint
    pub fn has_recent_checkpoint(&self, max_age: Duration) -> bool {
        if let Some(ref checkpoint) = self.last_checkpoint {
            checkpoint.verified && (Utc::now() - checkpoint.created_at) < max_age
        } else {
            false
        }
    }

    /// Builder methods
    pub fn with_submitter(mut self, submitter: impl Into<String>) -> Self {
        self.submitter = submitter.into();
        self
    }

    pub fn with_authority_domain(mut self, domain: impl Into<String>) -> Self {
        self.authority_domain = domain.into();
        self
    }

    pub fn with_time_estimate(mut self, estimate: Duration) -> Self {
        self.time_estimate = Some(estimate);
        self.estimated_completion = Some(self.started_at + estimate);
        self
    }

    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = Some(interval);
        self
    }

    pub fn with_job_type(mut self, job_type: impl Into<String>) -> Self {
        self.job_type = Some(job_type.into());
        self
    }

    pub fn non_preemptible(mut self) -> Self {
        self.preemptible = false;
        self
    }

    /// Set the start time (useful for testing)
    pub fn with_started_at(mut self, started_at: DateTime<Utc>) -> Self {
        self.started_at = started_at;
        self
    }
}

/// Information about a task checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInfo {
    /// Unique identifier for this checkpoint
    pub checkpoint_id: String,
    /// When the checkpoint was created
    pub created_at: DateTime<Utc>,
    /// Size of the checkpoint data in bytes
    pub size_bytes: u64,
    /// Where the checkpoint is stored
    pub storage_location: String,
    /// Whether the checkpoint has been verified
    pub verified: bool,
}

impl CheckpointInfo {
    /// Create a new checkpoint info
    pub fn new(
        checkpoint_id: impl Into<String>,
        size_bytes: u64,
        storage_location: impl Into<String>,
    ) -> Self {
        Self {
            checkpoint_id: checkpoint_id.into(),
            created_at: Utc::now(),
            size_bytes,
            storage_location: storage_location.into(),
            verified: false,
        }
    }

    /// Mark this checkpoint as verified
    pub fn verify(&mut self) {
        self.verified = true;
    }

    /// Get the age of this checkpoint
    pub fn age(&self) -> Duration {
        Utc::now() - self.created_at
    }
}

/// Summary statistics about running tasks
#[derive(Debug, Clone, Default)]
pub struct TaskStatistics {
    pub total_tasks: usize,
    pub preemptible_tasks: usize,
    pub tasks_with_checkpoints: usize,
    pub total_resources: ResourceUsage,
    pub average_runtime: Duration,
    pub average_priority: f64,
}

impl TaskStatistics {
    /// Calculate statistics from a collection of running tasks
    pub fn from_tasks<'a>(tasks: impl Iterator<Item = &'a RunningTask>) -> Self {
        let tasks: Vec<&RunningTask> = tasks.collect();
        let total_tasks = tasks.len();

        if total_tasks == 0 {
            return Self::default();
        }

        let preemptible_tasks = tasks.iter().filter(|t| t.preemptible).count();
        let tasks_with_checkpoints = tasks.iter().filter(|t| t.last_checkpoint.is_some()).count();

        let mut total_resources = ResourceUsage::zero();
        let mut total_runtime = Duration::zero();
        let mut total_priority: i64 = 0;

        for task in &tasks {
            total_resources.add(&task.resources);
            total_runtime += task.runtime();
            total_priority += task.priority as i64;
        }

        let average_runtime = total_runtime / total_tasks as i32;
        let average_priority = total_priority as f64 / total_tasks as f64;

        Self {
            total_tasks,
            preemptible_tasks,
            tasks_with_checkpoints,
            total_resources,
            average_runtime,
            average_priority,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_running_task_creation() {
        let task = RunningTask::new(
            "task-1",
            "job-1",
            "node-1",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        );

        assert_eq!(task.task_id, "task-1");
        assert_eq!(task.job_id, "job-1");
        assert_eq!(task.node_id, "node-1");
        assert_eq!(task.priority, 10);
        assert!(task.preemptible);
        assert_eq!(task.preemption_count, 0);
    }

    #[test]
    fn test_task_preemption_tracking() {
        let mut task = RunningTask::new(
            "task-1",
            "job-1",
            "node-1",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        );

        assert_eq!(task.preemption_count, 0);
        assert!(task.last_preempted_at.is_none());

        task.mark_preempted();

        assert_eq!(task.preemption_count, 1);
        assert!(task.last_preempted_at.is_some());
    }

    #[test]
    fn test_checkpoint_info() {
        let mut checkpoint = CheckpointInfo::new("ckpt-1", 1024, "/storage/ckpt-1");

        assert!(!checkpoint.verified);
        checkpoint.verify();
        assert!(checkpoint.verified);
    }

    #[test]
    fn test_task_statistics() {
        let tasks = vec![
            RunningTask::new(
                "task-1",
                "job-1",
                "node-1",
                10,
                ResourceUsage::new(2.0, 4.0, 0, 10.0),
            ),
            RunningTask::new(
                "task-2",
                "job-1",
                "node-1",
                20,
                ResourceUsage::new(4.0, 8.0, 1, 20.0),
            )
            .non_preemptible(),
        ];

        let stats = TaskStatistics::from_tasks(tasks.iter());

        assert_eq!(stats.total_tasks, 2);
        assert_eq!(stats.preemptible_tasks, 1);
        assert_eq!(stats.tasks_with_checkpoints, 0);
        assert_eq!(stats.total_resources.cpu_cores, 6.0);
        assert_eq!(stats.average_priority, 15.0);
    }
}
