// Marabunta - Licensed under the MIT License.
//! Failure types for the marabunta-compute job placement system
//!
//! Defines all possible failure types, contexts, and severities that can occur
//! during job execution and task placement.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Type alias for Job identifiers
pub type JobId = String;

/// Type alias for Task identifiers
pub type TaskId = String;

/// Type alias for Node identifiers
pub type NodeId = String;

/// Types of resources that can be exhausted or tracked
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    /// CPU cores/cycles
    Cpu,
    /// System memory (RAM)
    Memory,
    /// Graphics processing unit
    Gpu,
    /// Disk storage
    Disk,
    /// Network bandwidth
    Network,
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceType::Cpu => write!(f, "CPU"),
            ResourceType::Memory => write!(f, "Memory"),
            ResourceType::Gpu => write!(f, "GPU"),
            ResourceType::Disk => write!(f, "Disk"),
            ResourceType::Network => write!(f, "Network"),
        }
    }
}

/// Enumeration of all possible failure types in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailureType {
    // Node failures
    /// Node is unreachable (network issue or node down)
    NodeUnreachable { node_id: NodeId },
    /// Node crashed with optional error message
    NodeCrash {
        node_id: NodeId,
        error: Option<String>,
    },
    /// Node ran out of a specific resource
    NodeResourceExhausted {
        node_id: NodeId,
        resource: ResourceType,
    },

    // Task failures
    /// Task crashed during execution
    TaskCrash {
        exit_code: Option<i32>,
        stderr: Option<String>,
    },
    /// Task exceeded its time limit
    TaskTimeout { elapsed: Duration, limit: Duration },
    /// Task ran out of memory (OOM killed)
    TaskOom { memory_used: u64, memory_limit: u64 },
    /// Task was cancelled by user or system
    TaskCancelled { by: String, reason: String },

    // System failures
    /// Coordinator cluster is unreachable
    CoordinatorUnreachable,
    /// A specific master node is unreachable
    MasterUnreachable { master_id: String },
    /// Raft quorum lost - cluster cannot make decisions
    QuorumLost,

    // Placement failures
    /// No nodes available that match task requirements
    NoSuitableNodes { reason: String },
    /// Task placement violates a policy
    PolicyViolation {
        policy_id: String,
        violation: String,
    },
    /// Account or user has exceeded resource quota
    QuotaExceeded {
        account_id: String,
        resource: ResourceType,
    },

    // Network failures
    /// Network partition detected between nodes
    NetworkPartition { affected_nodes: Vec<NodeId> },
    /// Network operation timed out
    NetworkTimeout { operation: String },

    // Storage failures
    /// Failed to create a checkpoint
    CheckpointFailed { error: String },
    /// Checkpoint data is corrupted
    CheckpointCorrupted { checkpoint_id: String },
    /// Storage backend is unavailable
    StorageUnavailable { storage_id: String },

    // External failures
    /// External service (e.g., API, database) is unavailable
    ExternalServiceUnavailable { service: String },
    /// External service returned an error
    ExternalServiceError { service: String, error: String },

    // Unknown
    /// Catch-all for unclassified failures
    Unknown { description: String },
}

impl fmt::Display for FailureType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureType::NodeUnreachable { node_id } => {
                write!(f, "Node unreachable: {}", node_id)
            }
            FailureType::NodeCrash { node_id, error } => {
                write!(
                    f,
                    "Node crash: {} - {}",
                    node_id,
                    error.as_deref().unwrap_or("unknown error")
                )
            }
            FailureType::NodeResourceExhausted { node_id, resource } => {
                write!(f, "Node {} exhausted resource: {}", node_id, resource)
            }
            FailureType::TaskCrash { exit_code, stderr } => {
                write!(
                    f,
                    "Task crash (exit code: {:?}): {}",
                    exit_code,
                    stderr.as_deref().unwrap_or("no stderr")
                )
            }
            FailureType::TaskTimeout { elapsed, limit } => {
                write!(f, "Task timeout: elapsed {:?}, limit {:?}", elapsed, limit)
            }
            FailureType::TaskOom {
                memory_used,
                memory_limit,
            } => {
                write!(
                    f,
                    "Task OOM: used {} bytes, limit {} bytes",
                    memory_used, memory_limit
                )
            }
            FailureType::TaskCancelled { by, reason } => {
                write!(f, "Task cancelled by {}: {}", by, reason)
            }
            FailureType::CoordinatorUnreachable => write!(f, "Coordinator unreachable"),
            FailureType::MasterUnreachable { master_id } => {
                write!(f, "Master unreachable: {}", master_id)
            }
            FailureType::QuorumLost => write!(f, "Quorum lost"),
            FailureType::NoSuitableNodes { reason } => {
                write!(f, "No suitable nodes: {}", reason)
            }
            FailureType::PolicyViolation {
                policy_id,
                violation,
            } => {
                write!(f, "Policy violation ({}): {}", policy_id, violation)
            }
            FailureType::QuotaExceeded {
                account_id,
                resource,
            } => {
                write!(
                    f,
                    "Quota exceeded for account {} on {}",
                    account_id, resource
                )
            }
            FailureType::NetworkPartition { affected_nodes } => {
                write!(
                    f,
                    "Network partition affecting {} nodes",
                    affected_nodes.len()
                )
            }
            FailureType::NetworkTimeout { operation } => {
                write!(f, "Network timeout during: {}", operation)
            }
            FailureType::CheckpointFailed { error } => {
                write!(f, "Checkpoint failed: {}", error)
            }
            FailureType::CheckpointCorrupted { checkpoint_id } => {
                write!(f, "Checkpoint corrupted: {}", checkpoint_id)
            }
            FailureType::StorageUnavailable { storage_id } => {
                write!(f, "Storage unavailable: {}", storage_id)
            }
            FailureType::ExternalServiceUnavailable { service } => {
                write!(f, "External service unavailable: {}", service)
            }
            FailureType::ExternalServiceError { service, error } => {
                write!(f, "External service {} error: {}", service, error)
            }
            FailureType::Unknown { description } => {
                write!(f, "Unknown failure: {}", description)
            }
        }
    }
}

impl FailureType {
    /// Get a string key representing the failure type category (for grouping)
    pub fn type_key(&self) -> &'static str {
        match self {
            FailureType::NodeUnreachable { .. } => "node_unreachable",
            FailureType::NodeCrash { .. } => "node_crash",
            FailureType::NodeResourceExhausted { .. } => "node_resource_exhausted",
            FailureType::TaskCrash { .. } => "task_crash",
            FailureType::TaskTimeout { .. } => "task_timeout",
            FailureType::TaskOom { .. } => "task_oom",
            FailureType::TaskCancelled { .. } => "task_cancelled",
            FailureType::CoordinatorUnreachable => "coordinator_unreachable",
            FailureType::MasterUnreachable { .. } => "master_unreachable",
            FailureType::QuorumLost => "quorum_lost",
            FailureType::NoSuitableNodes { .. } => "no_suitable_nodes",
            FailureType::PolicyViolation { .. } => "policy_violation",
            FailureType::QuotaExceeded { .. } => "quota_exceeded",
            FailureType::NetworkPartition { .. } => "network_partition",
            FailureType::NetworkTimeout { .. } => "network_timeout",
            FailureType::CheckpointFailed { .. } => "checkpoint_failed",
            FailureType::CheckpointCorrupted { .. } => "checkpoint_corrupted",
            FailureType::StorageUnavailable { .. } => "storage_unavailable",
            FailureType::ExternalServiceUnavailable { .. } => "external_service_unavailable",
            FailureType::ExternalServiceError { .. } => "external_service_error",
            FailureType::Unknown { .. } => "unknown",
        }
    }

    /// Check if this failure type matches another (ignoring inner values)
    pub fn matches(&self, other: &FailureType) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// Severity levels for failures
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FailureSeverity {
    /// Low severity - typically recoverable with simple retry
    Low,
    /// Medium severity - task-level impact, may need different strategy
    Medium,
    /// High severity - job-level impact, significant intervention needed
    High,
    /// Critical severity - system-wide impact, immediate attention required
    Critical,
}

impl fmt::Display for FailureSeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureSeverity::Low => write!(f, "LOW"),
            FailureSeverity::Medium => write!(f, "MEDIUM"),
            FailureSeverity::High => write!(f, "HIGH"),
            FailureSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

impl FailureSeverity {
    /// Get the default severity for a failure type
    pub fn from_failure_type(failure_type: &FailureType) -> Self {
        match failure_type {
            // Critical failures
            FailureType::CoordinatorUnreachable
            | FailureType::QuorumLost
            | FailureType::NetworkPartition { .. } => FailureSeverity::Critical,

            // High severity failures
            FailureType::MasterUnreachable { .. }
            | FailureType::NodeCrash { .. }
            | FailureType::StorageUnavailable { .. } => FailureSeverity::High,

            // Medium severity failures
            FailureType::NodeUnreachable { .. }
            | FailureType::NodeResourceExhausted { .. }
            | FailureType::TaskOom { .. }
            | FailureType::CheckpointCorrupted { .. }
            | FailureType::NoSuitableNodes { .. }
            | FailureType::PolicyViolation { .. }
            | FailureType::QuotaExceeded { .. }
            | FailureType::ExternalServiceUnavailable { .. } => FailureSeverity::Medium,

            // Low severity failures
            FailureType::TaskCrash { .. }
            | FailureType::TaskTimeout { .. }
            | FailureType::TaskCancelled { .. }
            | FailureType::NetworkTimeout { .. }
            | FailureType::CheckpointFailed { .. }
            | FailureType::ExternalServiceError { .. }
            | FailureType::Unknown { .. } => FailureSeverity::Low,
        }
    }
}

/// Context information about where/when a failure occurred
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FailureContext {
    /// Associated job ID, if any
    pub job_id: Option<JobId>,
    /// Associated task ID, if any
    pub task_id: Option<TaskId>,
    /// Associated node ID, if any
    pub node_id: Option<NodeId>,
    /// Principal (user/service) that owns the job/task
    pub principal_id: Option<String>,
    /// Additional key-value metadata
    pub additional_data: HashMap<String, String>,
}

impl FailureContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self::default()
    }

    /// Create context for a job-level failure
    pub fn for_job(job_id: impl Into<JobId>) -> Self {
        Self {
            job_id: Some(job_id.into()),
            ..Default::default()
        }
    }

    /// Create context for a task-level failure
    pub fn for_task(job_id: impl Into<JobId>, task_id: impl Into<TaskId>) -> Self {
        Self {
            job_id: Some(job_id.into()),
            task_id: Some(task_id.into()),
            ..Default::default()
        }
    }

    /// Create context for a node-level failure
    pub fn for_node(node_id: impl Into<NodeId>) -> Self {
        Self {
            node_id: Some(node_id.into()),
            ..Default::default()
        }
    }

    /// Add job ID to context
    pub fn with_job(mut self, job_id: impl Into<JobId>) -> Self {
        self.job_id = Some(job_id.into());
        self
    }

    /// Add task ID to context
    pub fn with_task(mut self, task_id: impl Into<TaskId>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    /// Add node ID to context
    pub fn with_node(mut self, node_id: impl Into<NodeId>) -> Self {
        self.node_id = Some(node_id.into());
        self
    }

    /// Add principal ID to context
    pub fn with_principal(mut self, principal_id: impl Into<String>) -> Self {
        self.principal_id = Some(principal_id.into());
        self
    }

    /// Add additional data
    pub fn with_data(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.additional_data.insert(key.into(), value.into());
        self
    }
}

/// A complete failure record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    /// Unique identifier for this failure instance
    pub id: String,
    /// The type/category of failure
    pub failure_type: FailureType,
    /// When the failure actually occurred (if known)
    pub occurred_at: DateTime<Utc>,
    /// When the failure was detected by the system
    pub detected_at: DateTime<Utc>,
    /// Context information about the failure
    pub context: FailureContext,
    /// Severity level (can be overridden from default)
    pub severity: FailureSeverity,
}

impl Failure {
    /// Create a new failure with generated ID and current timestamp
    pub fn new(failure_type: FailureType, context: FailureContext) -> Self {
        let now = Utc::now();
        let severity = FailureSeverity::from_failure_type(&failure_type);

        Self {
            id: uuid::Uuid::new_v4().to_string(),
            failure_type,
            occurred_at: now,
            detected_at: now,
            context,
            severity,
        }
    }

    /// Create a failure with custom occurred_at time
    pub fn with_occurred_at(mut self, occurred_at: DateTime<Utc>) -> Self {
        self.occurred_at = occurred_at;
        self
    }

    /// Override the default severity
    pub fn with_severity(mut self, severity: FailureSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Get the detection latency (time between occurrence and detection)
    pub fn detection_latency(&self) -> Duration {
        self.detected_at.signed_duration_since(self.occurred_at)
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} - {} (detected at {})",
            self.severity, self.id, self.failure_type, self.detected_at
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failure_type_display() {
        let failure = FailureType::NodeUnreachable {
            node_id: "node-123".to_string(),
        };
        assert_eq!(failure.to_string(), "Node unreachable: node-123");

        let failure = FailureType::TaskTimeout {
            elapsed: Duration::seconds(120),
            limit: Duration::seconds(60),
        };
        assert!(failure.to_string().contains("timeout"));
    }

    #[test]
    fn test_failure_type_key() {
        let failure = FailureType::NodeCrash {
            node_id: "node-1".to_string(),
            error: None,
        };
        assert_eq!(failure.type_key(), "node_crash");
    }

    #[test]
    fn test_failure_type_matches() {
        let f1 = FailureType::NodeCrash {
            node_id: "node-1".to_string(),
            error: None,
        };
        let f2 = FailureType::NodeCrash {
            node_id: "node-2".to_string(),
            error: Some("error".to_string()),
        };
        let f3 = FailureType::NodeUnreachable {
            node_id: "node-1".to_string(),
        };

        assert!(f1.matches(&f2));
        assert!(!f1.matches(&f3));
    }

    #[test]
    fn test_severity_ordering() {
        assert!(FailureSeverity::Low < FailureSeverity::Medium);
        assert!(FailureSeverity::Medium < FailureSeverity::High);
        assert!(FailureSeverity::High < FailureSeverity::Critical);
    }

    #[test]
    fn test_severity_from_failure_type() {
        assert_eq!(
            FailureSeverity::from_failure_type(&FailureType::QuorumLost),
            FailureSeverity::Critical
        );
        assert_eq!(
            FailureSeverity::from_failure_type(&FailureType::TaskTimeout {
                elapsed: Duration::seconds(1),
                limit: Duration::seconds(1)
            }),
            FailureSeverity::Low
        );
    }

    #[test]
    fn test_failure_context_builder() {
        let ctx = FailureContext::new()
            .with_job("job-123")
            .with_task("task-456")
            .with_node("node-789")
            .with_principal("user@example.com")
            .with_data("retry_count", "3");

        assert_eq!(ctx.job_id, Some("job-123".to_string()));
        assert_eq!(ctx.task_id, Some("task-456".to_string()));
        assert_eq!(ctx.node_id, Some("node-789".to_string()));
        assert_eq!(ctx.principal_id, Some("user@example.com".to_string()));
        assert_eq!(
            ctx.additional_data.get("retry_count"),
            Some(&"3".to_string())
        );
    }

    #[test]
    fn test_failure_creation() {
        let failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: Some(1),
                stderr: Some("segfault".to_string()),
            },
            FailureContext::for_task("job-1", "task-1"),
        );

        assert!(!failure.id.is_empty());
        assert_eq!(failure.severity, FailureSeverity::Low);
        assert!(failure.context.job_id.is_some());
        assert!(failure.context.task_id.is_some());
    }

    #[test]
    fn test_failure_with_overrides() {
        let past = Utc::now() - Duration::seconds(60);
        let failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::new(),
        )
        .with_occurred_at(past)
        .with_severity(FailureSeverity::High);

        assert_eq!(failure.severity, FailureSeverity::High);
        assert!(failure.detection_latency() >= Duration::seconds(59));
    }
}
