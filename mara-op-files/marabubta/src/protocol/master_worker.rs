// Marabunta - Licensed under the MIT License.
//! Master-Worker communication protocol
//!
//! This module defines the complete protocol for task execution between masters and workers.
//! It supports:
//! - Task assignment and cancellation
//! - Checkpointing and preemption
//! - Progress reporting and heartbeats
//! - Worker registration and capacity tracking

use serde::{Deserialize, Serialize};

use crate::common::types::TaskPayload;

/// Messages from Master to Worker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MasterToWorkerMsg {
    /// Assign a task to execute
    AssignTask {
        task_id: String,
        job_id: String,
        payload: TaskPayload,
        priority: i32,
        timeout_secs: u64,
        /// Resume from checkpoint if provided
        checkpoint_data: Option<Vec<u8>>,
    },

    /// Cancel a running task
    CancelTask {
        task_id: String,
        reason: String,
        /// Whether to save a checkpoint before cancelling
        save_checkpoint: bool,
    },

    /// Request immediate checkpoint
    RequestCheckpoint { task_id: String },

    /// Preempt task (checkpoint and yield for higher priority work)
    PreemptTask { task_id: String, reason: String },

    /// Ping for heartbeat
    Ping { timestamp: u64 },

    /// Request worker status
    GetStatus,

    /// Graceful shutdown request
    Shutdown { reason: String, timeout_secs: u64 },
}

/// Messages from Worker to Master
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerToMasterMsg {
    /// Worker registration
    Register {
        worker_id: String,
        capabilities: WorkerCapabilities,
    },

    /// Task started execution
    TaskStarted { task_id: String, started_at: u64 },

    /// Task progress update
    TaskProgress {
        task_id: String,
        progress: f64,
        stage: Option<String>,
        metrics: TaskMetrics,
    },

    /// Task completed successfully
    TaskCompleted {
        task_id: String,
        result: TaskResult,
        duration_ms: u64,
    },

    /// Task failed
    TaskFailed {
        task_id: String,
        error: String,
        retryable: bool,
        /// Checkpoint data if saved before failure
        checkpoint: Option<Vec<u8>>,
    },

    /// Checkpoint saved successfully
    CheckpointSaved {
        task_id: String,
        checkpoint_id: String,
        size_bytes: u64,
    },

    /// Heartbeat response with resource usage
    Pong {
        timestamp: u64,
        active_tasks: u32,
        cpu_usage: f64,
        memory_usage_mb: u64,
        available_capacity: WorkerCapacity,
    },

    /// Worker shutting down
    Shutdown {
        reason: String,
        /// Tasks that were still running
        active_tasks: Vec<String>,
    },

    /// Acknowledgment of task assignment
    TaskAccepted { task_id: String },

    /// Task assignment rejected (worker at capacity)
    TaskRejected { task_id: String, reason: String },
}

/// Worker capabilities describing hardware and supported task types
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerCapabilities {
    /// Number of CPU cores
    pub cpu_cores: u32,
    /// Total memory in megabytes
    pub memory_mb: u64,
    /// Number of GPUs
    pub gpu_count: u32,
    /// GPU memory in megabytes (if GPUs present)
    pub gpu_memory_mb: Option<u64>,
    /// Disk space in gigabytes
    pub disk_gb: u64,
    /// Tags for capability matching (e.g., "gpu", "high-memory", "python3")
    pub tags: Vec<String>,
    /// Maximum concurrent tasks this worker can handle
    pub max_concurrent_tasks: u32,
}

/// Current worker capacity (available resources)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerCapacity {
    /// Available CPU cores (can be fractional)
    pub available_cpu: f64,
    /// Available memory in megabytes
    pub available_memory_mb: u64,
    /// Available GPU count
    pub available_gpu: u32,
    /// Maximum tasks this worker can handle
    pub max_tasks: u32,
    /// Currently running tasks
    pub current_tasks: u32,
}

impl WorkerCapacity {
    /// Check if worker can accept more tasks
    pub fn can_accept_task(&self) -> bool {
        self.current_tasks < self.max_tasks
    }

    /// Calculate load as a percentage (0.0 to 1.0)
    pub fn load(&self) -> f64 {
        if self.max_tasks == 0 {
            1.0
        } else {
            self.current_tasks as f64 / self.max_tasks as f64
        }
    }
}

/// Metrics for a running task
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskMetrics {
    /// CPU usage (0.0 to 1.0 per core, can exceed 1.0 for multi-core)
    pub cpu_usage: f64,
    /// Memory usage in megabytes
    pub memory_mb: u64,
    /// GPU usage (0.0 to 1.0) if applicable
    pub gpu_usage: Option<f64>,
    /// Bytes read from disk
    pub disk_read_bytes: u64,
    /// Bytes written to disk
    pub disk_write_bytes: u64,
    /// Network bytes received
    pub network_rx_bytes: u64,
    /// Network bytes sent
    pub network_tx_bytes: u64,
}

/// Result of a completed task
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct TaskResult {
    /// Output data (serialized)
    pub output: Vec<u8>,
    /// Final metrics
    pub metrics: TaskMetrics,
    /// Exit code if applicable
    pub exit_code: Option<i32>,
    /// Standard output (truncated if too long)
    pub stdout: Option<String>,
    /// Standard error (truncated if too long)
    pub stderr: Option<String>,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_master_to_worker_serialization() {
        let msg = MasterToWorkerMsg::AssignTask {
            task_id: "task-123".to_string(),
            job_id: "job-456".to_string(),
            payload: TaskPayload::Shell {
                command: "echo".to_string(),
                args: vec!["hello".to_string()],
            },
            priority: 10,
            timeout_secs: 300,
            checkpoint_data: None,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: MasterToWorkerMsg = serde_json::from_str(&json).unwrap();

        if let MasterToWorkerMsg::AssignTask { task_id, .. } = deserialized {
            assert_eq!(task_id, "task-123");
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_worker_to_master_serialization() {
        let msg = WorkerToMasterMsg::TaskCompleted {
            task_id: "task-123".to_string(),
            result: TaskResult {
                output: vec![1, 2, 3],
                metrics: TaskMetrics {
                    cpu_usage: 0.75,
                    memory_mb: 512,
                    gpu_usage: None,
                    disk_read_bytes: 1024,
                    disk_write_bytes: 512,
                    network_rx_bytes: 100,
                    network_tx_bytes: 50,
                },
                exit_code: Some(0),
                stdout: Some("hello".to_string()),
                stderr: None,
            },
            duration_ms: 1500,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: WorkerToMasterMsg = serde_json::from_str(&json).unwrap();

        if let WorkerToMasterMsg::TaskCompleted {
            task_id,
            duration_ms,
            ..
        } = deserialized
        {
            assert_eq!(task_id, "task-123");
            assert_eq!(duration_ms, 1500);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_worker_capabilities_serialization() {
        let caps = WorkerCapabilities {
            cpu_cores: 8,
            memory_mb: 16384,
            gpu_count: 2,
            gpu_memory_mb: Some(8192),
            disk_gb: 500,
            tags: vec!["gpu".to_string(), "high-memory".to_string()],
            max_concurrent_tasks: 4,
        };

        let json = serde_json::to_string(&caps).unwrap();
        let deserialized: WorkerCapabilities = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.cpu_cores, 8);
        assert_eq!(deserialized.gpu_count, 2);
        assert_eq!(deserialized.tags.len(), 2);
    }

    #[test]
    fn test_worker_capacity_load() {
        let cap = WorkerCapacity {
            available_cpu: 4.0,
            available_memory_mb: 8192,
            available_gpu: 1,
            max_tasks: 4,
            current_tasks: 2,
        };

        assert_eq!(cap.load(), 0.5);
        assert!(cap.can_accept_task());

        let full = WorkerCapacity {
            max_tasks: 4,
            current_tasks: 4,
            ..Default::default()
        };

        assert_eq!(full.load(), 1.0);
        assert!(!full.can_accept_task());
    }

    #[test]
    fn test_cancel_task_message() {
        let msg = MasterToWorkerMsg::CancelTask {
            task_id: "task-789".to_string(),
            reason: "Job cancelled by user".to_string(),
            save_checkpoint: true,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("cancel_task"));
        assert!(json.contains("save_checkpoint"));
    }

    #[test]
    fn test_worker_registration() {
        let msg = WorkerToMasterMsg::Register {
            worker_id: "worker-abc".to_string(),
            capabilities: WorkerCapabilities {
                cpu_cores: 4,
                memory_mb: 8192,
                gpu_count: 0,
                gpu_memory_mb: None,
                disk_gb: 100,
                tags: vec!["python3".to_string()],
                max_concurrent_tasks: 2,
            },
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: WorkerToMasterMsg = serde_json::from_str(&json).unwrap();

        if let WorkerToMasterMsg::Register {
            worker_id,
            capabilities,
        } = deserialized
        {
            assert_eq!(worker_id, "worker-abc");
            assert_eq!(capabilities.cpu_cores, 4);
        } else {
            panic!("Wrong message type");
        }
    }
}
