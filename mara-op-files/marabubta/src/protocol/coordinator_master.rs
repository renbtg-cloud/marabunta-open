// Marabunta - Licensed under the MIT License.
//! Protocol messages for Coordinator-Master communication
//!
//! This module defines the message types used for communication between
//! coordinators and masters in the Marabunta Compute cluster.

use serde::{Deserialize, Serialize};

use crate::common::types::{Job, JobStatus, TaskResult, WorkerStatus};

/// Messages from Coordinator to Master
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoordinatorToMaster {
    /// Assign a job to this master for distribution
    AssignJob {
        job_id: String,
        job: Job,
        priority: i32,
    },

    /// Cancel a job
    CancelJob { job_id: String, reason: String },

    /// Update job priority
    UpdatePriority { job_id: String, new_priority: i32 },

    /// Heartbeat/health check
    Ping { timestamp: u64 },

    /// Request status of all jobs on this master
    GetStatus,

    /// Coordinator leadership changed
    LeadershipChange { new_leader: String },
}

/// Messages from Master to Coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MasterToCoordinator {
    /// Master registration
    Register {
        master_id: String,
        region: String,
        capacity: MasterCapacity,
    },

    /// Job status update
    JobStatus {
        job_id: String,
        status: JobStatus,
        progress: f64,
        tasks_completed: u32,
        tasks_total: u32,
    },

    /// Job completed
    JobCompleted { job_id: String, result: JobResult },

    /// Job failed
    JobFailed { job_id: String, error: String },

    /// Heartbeat response
    Pong {
        timestamp: u64,
        active_jobs: u32,
        active_workers: u32,
    },

    /// Full status response
    StatusReport {
        jobs: Vec<JobStatusSummary>,
        workers: Vec<WorkerStatusSummary>,
    },
}

/// Capacity information for a master
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterCapacity {
    /// Maximum number of concurrent jobs
    pub max_jobs: u32,
    /// Maximum number of workers this master can manage
    pub max_workers: u32,
    /// Current number of active jobs
    pub current_jobs: u32,
    /// Current number of connected workers
    pub current_workers: u32,
}

impl MasterCapacity {
    /// Create a new MasterCapacity with default values
    pub fn new(max_jobs: u32, max_workers: u32) -> Self {
        Self {
            max_jobs,
            max_workers,
            current_jobs: 0,
            current_workers: 0,
        }
    }

    /// Check if the master has capacity for more jobs
    pub fn has_job_capacity(&self) -> bool {
        self.current_jobs < self.max_jobs
    }

    /// Check if the master has capacity for more workers
    pub fn has_worker_capacity(&self) -> bool {
        self.current_workers < self.max_workers
    }

    /// Get the available job slots
    pub fn available_job_slots(&self) -> u32 {
        self.max_jobs.saturating_sub(self.current_jobs)
    }

    /// Get the load factor (0.0 - 1.0)
    pub fn load_factor(&self) -> f64 {
        if self.max_jobs == 0 {
            1.0
        } else {
            self.current_jobs as f64 / self.max_jobs as f64
        }
    }
}

impl Default for MasterCapacity {
    fn default() -> Self {
        Self::new(100, 1000)
    }
}

/// Result of a completed job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    /// Whether the job completed successfully
    pub success: bool,
    /// Number of tasks that completed successfully
    pub tasks_succeeded: u32,
    /// Number of tasks that failed
    pub tasks_failed: u32,
    /// Total execution time in milliseconds
    pub total_duration_ms: u64,
    /// Aggregated results from all tasks
    pub task_results: Vec<TaskResult>,
    /// Optional error message if the job failed
    pub error: Option<String>,
}

impl JobResult {
    /// Create a successful job result
    pub fn success(
        tasks_succeeded: u32,
        total_duration_ms: u64,
        task_results: Vec<TaskResult>,
    ) -> Self {
        Self {
            success: true,
            tasks_succeeded,
            tasks_failed: 0,
            total_duration_ms,
            task_results,
            error: None,
        }
    }

    /// Create a failed job result
    pub fn failure(
        tasks_succeeded: u32,
        tasks_failed: u32,
        total_duration_ms: u64,
        task_results: Vec<TaskResult>,
        error: String,
    ) -> Self {
        Self {
            success: false,
            tasks_succeeded,
            tasks_failed,
            total_duration_ms,
            task_results,
            error: Some(error),
        }
    }
}

/// Summary of a job's status for status reports
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusSummary {
    /// Job ID
    pub job_id: String,
    /// Current status
    pub status: JobStatus,
    /// Progress (0.0 - 1.0)
    pub progress: f64,
    /// Number of completed tasks
    pub tasks_completed: u32,
    /// Total number of tasks
    pub tasks_total: u32,
    /// Number of running tasks
    pub tasks_running: u32,
    /// Number of pending tasks
    pub tasks_pending: u32,
    /// Priority
    pub priority: i32,
}

impl JobStatusSummary {
    /// Create a new job status summary
    pub fn new(
        job_id: impl Into<String>,
        status: JobStatus,
        tasks_completed: u32,
        tasks_total: u32,
        tasks_running: u32,
        priority: i32,
    ) -> Self {
        let progress = if tasks_total > 0 {
            tasks_completed as f64 / tasks_total as f64
        } else {
            0.0
        };

        Self {
            job_id: job_id.into(),
            status,
            progress,
            tasks_completed,
            tasks_total,
            tasks_running,
            tasks_pending: tasks_total.saturating_sub(tasks_completed + tasks_running),
            priority,
        }
    }
}

/// Summary of a worker's status for status reports
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatusSummary {
    /// Worker ID
    pub worker_id: String,
    /// Current status
    pub status: WorkerStatus,
    /// Current load (0.0 - 1.0)
    pub load: f64,
    /// Number of running tasks
    pub running_tasks: u32,
    /// Available memory in MB
    pub available_memory_mb: u64,
    /// Available CPU cores
    pub available_cpu: f64,
}

impl WorkerStatusSummary {
    /// Create a new worker status summary
    pub fn new(
        worker_id: impl Into<String>,
        status: WorkerStatus,
        load: f64,
        running_tasks: u32,
        available_memory_mb: u64,
        available_cpu: f64,
    ) -> Self {
        Self {
            worker_id: worker_id.into(),
            status,
            load,
            running_tasks,
            available_memory_mb,
            available_cpu,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_to_master_serialization() {
        let job = Job::new("test-job");
        let msg = CoordinatorToMaster::AssignJob {
            job_id: "job-123".to_string(),
            job,
            priority: 10,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: CoordinatorToMaster = serde_json::from_str(&json).unwrap();

        match deserialized {
            CoordinatorToMaster::AssignJob {
                job_id, priority, ..
            } => {
                assert_eq!(job_id, "job-123");
                assert_eq!(priority, 10);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_master_to_coordinator_serialization() {
        let capacity = MasterCapacity::new(100, 1000);
        let msg = MasterToCoordinator::Register {
            master_id: "master-1".to_string(),
            region: "us-west-1".to_string(),
            capacity,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MasterToCoordinator = serde_json::from_str(&json).unwrap();

        match deserialized {
            MasterToCoordinator::Register {
                master_id,
                region,
                capacity,
            } => {
                assert_eq!(master_id, "master-1");
                assert_eq!(region, "us-west-1");
                assert_eq!(capacity.max_jobs, 100);
                assert_eq!(capacity.max_workers, 1000);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_ping_pong_serialization() {
        let ping = CoordinatorToMaster::Ping { timestamp: 12345 };
        let json = serde_json::to_string(&ping).unwrap();
        let deserialized: CoordinatorToMaster = serde_json::from_str(&json).unwrap();

        match deserialized {
            CoordinatorToMaster::Ping { timestamp } => {
                assert_eq!(timestamp, 12345);
            }
            _ => panic!("Wrong variant"),
        }

        let pong = MasterToCoordinator::Pong {
            timestamp: 12345,
            active_jobs: 5,
            active_workers: 10,
        };
        let json = serde_json::to_string(&pong).unwrap();
        let deserialized: MasterToCoordinator = serde_json::from_str(&json).unwrap();

        match deserialized {
            MasterToCoordinator::Pong {
                timestamp,
                active_jobs,
                active_workers,
            } => {
                assert_eq!(timestamp, 12345);
                assert_eq!(active_jobs, 5);
                assert_eq!(active_workers, 10);
            }
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_master_capacity() {
        let mut capacity = MasterCapacity::new(10, 100);
        assert!(capacity.has_job_capacity());
        assert!(capacity.has_worker_capacity());
        assert_eq!(capacity.available_job_slots(), 10);
        assert_eq!(capacity.load_factor(), 0.0);

        capacity.current_jobs = 5;
        assert_eq!(capacity.available_job_slots(), 5);
        assert_eq!(capacity.load_factor(), 0.5);

        capacity.current_jobs = 10;
        assert!(!capacity.has_job_capacity());
        assert_eq!(capacity.available_job_slots(), 0);
        assert_eq!(capacity.load_factor(), 1.0);
    }

    #[test]
    fn test_job_result() {
        let success = JobResult::success(10, 5000, vec![]);
        assert!(success.success);
        assert_eq!(success.tasks_succeeded, 10);
        assert_eq!(success.tasks_failed, 0);

        let failure = JobResult::failure(5, 5, 3000, vec![], "Task execution error".to_string());
        assert!(!failure.success);
        assert_eq!(failure.tasks_succeeded, 5);
        assert_eq!(failure.tasks_failed, 5);
        assert_eq!(failure.error, Some("Task execution error".to_string()));
    }

    #[test]
    fn test_job_status_summary() {
        let summary = JobStatusSummary::new(
            "job-1",
            JobStatus::Running,
            50,  // completed
            100, // total
            10,  // running
            5,   // priority
        );

        assert_eq!(summary.job_id, "job-1");
        assert_eq!(summary.progress, 0.5);
        assert_eq!(summary.tasks_pending, 40);
    }

    #[test]
    fn test_worker_status_summary() {
        let summary = WorkerStatusSummary::new("worker-1", WorkerStatus::Ready, 0.3, 2, 8192, 4.0);

        assert_eq!(summary.worker_id, "worker-1");
        assert_eq!(summary.status, WorkerStatus::Ready);
        assert_eq!(summary.load, 0.3);
    }

    #[test]
    fn test_status_report_serialization() {
        let msg = MasterToCoordinator::StatusReport {
            jobs: vec![JobStatusSummary::new(
                "job-1",
                JobStatus::Running,
                10,
                100,
                5,
                1,
            )],
            workers: vec![WorkerStatusSummary::new(
                "worker-1",
                WorkerStatus::Ready,
                0.5,
                3,
                4096,
                2.0,
            )],
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MasterToCoordinator = serde_json::from_str(&json).unwrap();

        match deserialized {
            MasterToCoordinator::StatusReport { jobs, workers } => {
                assert_eq!(jobs.len(), 1);
                assert_eq!(workers.len(), 1);
                assert_eq!(jobs[0].job_id, "job-1");
                assert_eq!(workers[0].worker_id, "worker-1");
            }
            _ => panic!("Wrong variant"),
        }
    }
}
