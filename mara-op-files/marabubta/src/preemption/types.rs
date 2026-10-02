// Marabunta - Licensed under the MIT License.
//! Core types for the preemption engine

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type JobId = String;
pub type TaskId = String;
pub type NodeId = String;

/// Policy that governs when and how preemption can occur
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreemptionPolicy {
    pub id: String,
    pub name: String,

    /// When can preemption happen?
    pub trigger: PreemptionTrigger,

    /// What can be preempted?
    pub victim_selector: VictimSelector,

    /// How to handle preempted tasks?
    pub action: PreemptionAction,

    /// Constraints on preemption
    pub constraints: PreemptionConstraints,

    /// Governance domain for this policy
    pub authority_domain: String,

    /// Minimum priority level required to preempt under this policy
    pub min_preemptor_priority: u32,
}

impl PreemptionPolicy {
    /// Create a new preemption policy with default settings
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            trigger: PreemptionTrigger::PriorityBased {
                min_priority_difference: 10,
            },
            victim_selector: VictimSelector::LowestPriority,
            action: PreemptionAction::CheckpointAndRequeue {
                checkpoint_timeout: Duration::seconds(60),
                requeue_priority_penalty: 0,
                max_requeues: 3,
            },
            constraints: PreemptionConstraints::default(),
            authority_domain: "default".to_string(),
            min_preemptor_priority: 0,
        }
    }

    /// Create a priority-based policy
    pub fn priority_based(
        id: impl Into<String>,
        name: impl Into<String>,
        min_priority_difference: i32,
    ) -> Self {
        let mut policy = Self::new(id, name);
        policy.trigger = PreemptionTrigger::PriorityBased {
            min_priority_difference,
        };
        policy
    }

    /// Create a resource pressure policy
    pub fn resource_pressure(
        id: impl Into<String>,
        name: impl Into<String>,
        resource: ResourceType,
        threshold: f64,
    ) -> Self {
        let mut policy = Self::new(id, name);
        policy.trigger = PreemptionTrigger::ResourcePressure {
            resource,
            threshold,
        };
        policy.victim_selector = VictimSelector::ShortestRunning;
        policy
    }

    /// Create a maintenance policy
    pub fn maintenance(
        id: impl Into<String>,
        name: impl Into<String>,
        notification_period: Duration,
    ) -> Self {
        let mut policy = Self::new(id, name);
        policy.trigger = PreemptionTrigger::Maintenance {
            notification_period,
        };
        policy.action = PreemptionAction::CheckpointAndRequeue {
            checkpoint_timeout: Duration::seconds(300),
            requeue_priority_penalty: 0,
            max_requeues: 10,
        };
        policy
    }
}

/// Conditions that trigger preemption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreemptionTrigger {
    /// Higher priority job needs resources
    PriorityBased { min_priority_difference: i32 },

    /// Resource pressure on the system
    ResourcePressure {
        resource: ResourceType,
        threshold: f64,
    },

    /// Quota enforcement
    QuotaEnforcement { grace_period: Duration },

    /// Manual/administrative preemption
    Administrative,

    /// Scheduled maintenance
    Maintenance { notification_period: Duration },

    /// Any of the triggers
    Any(Vec<PreemptionTrigger>),

    /// All triggers must match
    All(Vec<PreemptionTrigger>),
}

impl PreemptionTrigger {
    /// Check if this trigger matches the given reason
    pub fn matches(&self, reason: &PreemptionReason, system_state: Option<&SystemState>) -> bool {
        match self {
            PreemptionTrigger::PriorityBased {
                min_priority_difference,
            } => {
                if let PreemptionReason::HighPriorityJob { priority, .. } = reason {
                    // The preemptor priority must be significantly higher
                    // This is checked against the victim in can_preempt
                    *priority >= *min_priority_difference
                } else {
                    false
                }
            }
            PreemptionTrigger::ResourcePressure {
                resource,
                threshold,
            } => {
                if let PreemptionReason::ResourcePressure {
                    resource: r,
                    current,
                    threshold: t,
                } = reason
                {
                    *r == *resource && *current >= *threshold && *current >= *t
                } else if let Some(state) = system_state {
                    // Check system state for resource pressure
                    let utilization = state.get_resource_utilization(resource);
                    utilization >= *threshold
                } else {
                    false
                }
            }
            PreemptionTrigger::QuotaEnforcement { .. } => {
                matches!(reason, PreemptionReason::QuotaExceeded { .. })
            }
            PreemptionTrigger::Administrative => {
                matches!(reason, PreemptionReason::Administrative { .. })
            }
            PreemptionTrigger::Maintenance { .. } => {
                matches!(reason, PreemptionReason::Maintenance { .. })
            }
            PreemptionTrigger::Any(triggers) => {
                triggers.iter().any(|t| t.matches(reason, system_state))
            }
            PreemptionTrigger::All(triggers) => {
                triggers.iter().all(|t| t.matches(reason, system_state))
            }
        }
    }
}

/// Types of resources that can trigger preemption
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    Cpu,
    Memory,
    Gpu,
    Disk,
    Network,
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceType::Cpu => write!(f, "CPU"),
            ResourceType::Memory => write!(f, "Memory"),
            ResourceType::Gpu => write!(f, "GPU"),
            ResourceType::Disk => write!(f, "Disk"),
            ResourceType::Network => write!(f, "Network"),
        }
    }
}

/// Strategy for selecting which tasks to preempt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VictimSelector {
    /// Lowest priority first
    LowestPriority,

    /// Shortest running time (least work lost)
    ShortestRunning,

    /// Longest running (might be stuck)
    LongestRunning,

    /// Most recent checkpoint (least recomputation)
    MostRecentCheckpoint,

    /// Specific job types
    JobType(Vec<String>),

    /// Jobs from specific domains
    FromDomain(String),

    /// Jobs exceeding their time estimate
    OverTimeEstimate,

    /// Custom scoring function
    Custom { score_function: String },

    /// Try selectors in order
    Cascade(Vec<VictimSelector>),

    /// Delegate preemption logic to the 50KB eBPF/WASM Lawyer
    BpfNegotiator {
        /// The bytecode of the Valuation Algorithm
        payload: Vec<u8>,
    },

    /// Score-based combination
    Weighted {
        selectors: Vec<(VictimSelector, f64)>,
    },
}

/// Action to take when preempting a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreemptionAction {
    /// Kill immediately
    Kill,

    /// Checkpoint then kill
    CheckpointAndKill {
        checkpoint_timeout: Duration,
        kill_on_timeout: bool,
    },

    /// Checkpoint and requeue
    CheckpointAndRequeue {
        checkpoint_timeout: Duration,
        requeue_priority_penalty: i32,
        max_requeues: u32,
    },

    /// Migrate to another node
    Migrate {
        target_selector: String,
        migration_timeout: Duration,
    },

    /// Suspend (if supported)
    Suspend { max_suspend_time: Duration },

    /// Graceful shutdown signal
    GracefulShutdown {
        signal: String,
        grace_period: Duration,
        escalate_to: Box<PreemptionAction>,
    },
}

impl PreemptionAction {
    /// Get the timeout for this action
    pub fn timeout(&self) -> Duration {
        match self {
            PreemptionAction::Kill => Duration::seconds(5),
            PreemptionAction::CheckpointAndKill {
                checkpoint_timeout, ..
            } => *checkpoint_timeout + Duration::seconds(5),
            PreemptionAction::CheckpointAndRequeue {
                checkpoint_timeout, ..
            } => *checkpoint_timeout + Duration::seconds(10),
            PreemptionAction::Migrate {
                migration_timeout, ..
            } => *migration_timeout,
            PreemptionAction::Suspend { max_suspend_time } => *max_suspend_time,
            PreemptionAction::GracefulShutdown {
                grace_period,
                escalate_to,
                ..
            } => *grace_period + escalate_to.timeout(),
        }
    }
}

/// Constraints that limit when preemption can occur
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PreemptionConstraints {
    /// Minimum runtime before preemption allowed
    pub min_runtime: Option<Duration>,

    /// Maximum times a job can be preempted
    pub max_preemptions: Option<u32>,

    /// Time between preemptions
    pub cooldown: Option<Duration>,

    /// Jobs that cannot be preempted (patterns)
    pub protected_jobs: Vec<String>,

    /// Nodes where preemption is restricted (patterns)
    pub protected_nodes: Vec<String>,

    /// Time windows when preemption is restricted
    pub blackout_windows: Vec<TimeWindow>,

    /// Require approval for preemption
    pub require_approval: Option<ApprovalRequirement>,
}

impl PreemptionConstraints {
    /// Create new constraints with no restrictions
    pub fn none() -> Self {
        Self::default()
    }

    /// Set minimum runtime before preemption
    pub fn with_min_runtime(mut self, duration: Duration) -> Self {
        self.min_runtime = Some(duration);
        self
    }

    /// Set maximum preemptions allowed
    pub fn with_max_preemptions(mut self, max: u32) -> Self {
        self.max_preemptions = Some(max);
        self
    }

    /// Set cooldown between preemptions
    pub fn with_cooldown(mut self, duration: Duration) -> Self {
        self.cooldown = Some(duration);
        self
    }

    /// Add protected job patterns
    pub fn with_protected_jobs(mut self, patterns: Vec<String>) -> Self {
        self.protected_jobs = patterns;
        self
    }

    /// Add protected node patterns
    pub fn with_protected_nodes(mut self, patterns: Vec<String>) -> Self {
        self.protected_nodes = patterns;
        self
    }
}

/// A time window during which preemption is blocked
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindow {
    /// Cron expression for when preemption is blocked
    pub cron: String,
    /// Duration of the blackout window
    pub duration: Duration,
}

impl TimeWindow {
    /// Check if a given time falls within this blackout window
    /// Note: This is a simplified implementation; a real implementation would parse cron
    pub fn is_active(&self, _time: DateTime<Utc>) -> bool {
        // Simplified: always returns false
        // A real implementation would parse the cron expression
        false
    }

    /// Get the end time of the current blackout window, if active
    pub fn end_time(&self, start: DateTime<Utc>) -> DateTime<Utc> {
        start + self.duration
    }
}

/// Requirement for approval before preemption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRequirement {
    /// List of approvers who can authorize preemption
    pub approvers: Vec<String>,
    /// Timeout for waiting for approval
    pub timeout: Duration,
    /// Default decision if approval times out (true = approve, false = deny)
    pub default_on_timeout: bool,
}

/// Reason why preemption is being requested
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PreemptionReason {
    HighPriorityJob {
        job_id: JobId,
        priority: i32,
        /// The 50KB eBPF/WASM Lawyer payload for Turing-Complete Thermodynamic Negotiation
        bpf_valuation_payload: Option<Vec<u8>>,
    },
    ResourcePressure {
        resource: ResourceType,
        current: f64,
        threshold: f64,
    },
    QuotaExceeded {
        account_id: String,
        overage: f64,
    },
    Maintenance {
        node_ids: Vec<NodeId>,
        scheduled_at: DateTime<Utc>,
    },
    Administrative {
        reason: String,
    },
}

impl std::fmt::Display for PreemptionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PreemptionReason::HighPriorityJob { job_id, priority, bpf_valuation_payload } => {
                let is_bpf = if bpf_valuation_payload.is_some() { " [BPF-Armed]" } else { "" };
                write!(f, "High priority job {} (priority {}){}", job_id, priority, is_bpf)
            }
            PreemptionReason::ResourcePressure {
                resource,
                current,
                threshold,
            } => {
                write!(
                    f,
                    "Resource pressure: {} at {:.1}% (threshold {:.1}%)",
                    resource,
                    current * 100.0,
                    threshold * 100.0
                )
            }
            PreemptionReason::QuotaExceeded {
                account_id,
                overage,
            } => {
                write!(
                    f,
                    "Quota exceeded for account {} by {:.1}%",
                    account_id,
                    overage * 100.0
                )
            }
            PreemptionReason::Maintenance {
                node_ids,
                scheduled_at,
            } => {
                write!(
                    f,
                    "Maintenance scheduled at {} for nodes {:?}",
                    scheduled_at, node_ids
                )
            }
            PreemptionReason::Administrative { reason } => {
                write!(f, "Administrative: {}", reason)
            }
        }
    }
}

/// Urgency level for preemption
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreemptionUrgency {
    /// Preempt immediately
    Immediate,
    /// Preempt within minutes
    Soon,
    /// Preempt at a specific scheduled time
    Scheduled,
    /// Preempt when convenient
    BestEffort,
}

impl PreemptionUrgency {
    /// Get the timeout multiplier for this urgency level
    pub fn timeout_multiplier(&self) -> f64 {
        match self {
            PreemptionUrgency::Immediate => 0.5,
            PreemptionUrgency::Soon => 1.0,
            PreemptionUrgency::Scheduled => 2.0,
            PreemptionUrgency::BestEffort => 5.0,
        }
    }
}

/// Resource usage information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu_cores: f64,
    pub memory_gb: f64,
    pub gpu_count: u32,
    pub disk_gb: f64,
}

impl ResourceUsage {
    /// Create a new resource usage with all zeros
    pub fn zero() -> Self {
        Self::default()
    }

    /// Create a new resource usage
    pub fn new(cpu_cores: f64, memory_gb: f64, gpu_count: u32, disk_gb: f64) -> Self {
        Self {
            cpu_cores,
            memory_gb,
            gpu_count,
            disk_gb,
        }
    }

    /// Check if this usage is empty (all zeros)
    pub fn is_empty(&self) -> bool {
        self.cpu_cores == 0.0 && self.memory_gb == 0.0 && self.gpu_count == 0 && self.disk_gb == 0.0
    }

    /// Add another resource usage to this one
    pub fn add(&mut self, other: &ResourceUsage) {
        self.cpu_cores += other.cpu_cores;
        self.memory_gb += other.memory_gb;
        self.gpu_count += other.gpu_count;
        self.disk_gb += other.disk_gb;
    }

    /// Subtract another resource usage from this one
    pub fn subtract(&mut self, other: &ResourceUsage) {
        self.cpu_cores = (self.cpu_cores - other.cpu_cores).max(0.0);
        self.memory_gb = (self.memory_gb - other.memory_gb).max(0.0);
        self.gpu_count = self.gpu_count.saturating_sub(other.gpu_count);
        self.disk_gb = (self.disk_gb - other.disk_gb).max(0.0);
    }

    /// Check if this resource can satisfy the given requirement
    pub fn can_satisfy(&self, required: &ResourceUsage) -> bool {
        self.cpu_cores >= required.cpu_cores
            && self.memory_gb >= required.memory_gb
            && self.gpu_count >= required.gpu_count
            && self.disk_gb >= required.disk_gb
    }

    /// Calculate what percentage of each resource is used
    pub fn utilization_ratio(&self, capacity: &ResourceUsage) -> f64 {
        let cpu_ratio = if capacity.cpu_cores > 0.0 {
            self.cpu_cores / capacity.cpu_cores
        } else {
            0.0
        };
        let mem_ratio = if capacity.memory_gb > 0.0 {
            self.memory_gb / capacity.memory_gb
        } else {
            0.0
        };
        let gpu_ratio = if capacity.gpu_count > 0 {
            self.gpu_count as f64 / capacity.gpu_count as f64
        } else {
            0.0
        };
        let disk_ratio = if capacity.disk_gb > 0.0 {
            self.disk_gb / capacity.disk_gb
        } else {
            0.0
        };

        // Return the maximum utilization ratio
        cpu_ratio.max(mem_ratio).max(gpu_ratio).max(disk_ratio)
    }

    /// Get utilization for a specific resource type
    pub fn get_utilization(&self, resource_type: &ResourceType) -> f64 {
        match resource_type {
            ResourceType::Cpu => self.cpu_cores,
            ResourceType::Memory => self.memory_gb,
            ResourceType::Gpu => self.gpu_count as f64,
            ResourceType::Disk => self.disk_gb,
            ResourceType::Network => 0.0, // Not tracked in ResourceUsage
        }
    }
}

impl std::fmt::Display for ResourceUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CPU: {:.2} cores, Memory: {:.2} GB, GPU: {}, Disk: {:.2} GB",
            self.cpu_cores, self.memory_gb, self.gpu_count, self.disk_gb
        )
    }
}

impl std::ops::Add for ResourceUsage {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            cpu_cores: self.cpu_cores + other.cpu_cores,
            memory_gb: self.memory_gb + other.memory_gb,
            gpu_count: self.gpu_count + other.gpu_count,
            disk_gb: self.disk_gb + other.disk_gb,
        }
    }
}

impl std::ops::Sub for ResourceUsage {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            cpu_cores: (self.cpu_cores - other.cpu_cores).max(0.0),
            memory_gb: (self.memory_gb - other.memory_gb).max(0.0),
            gpu_count: self.gpu_count.saturating_sub(other.gpu_count),
            disk_gb: (self.disk_gb - other.disk_gb).max(0.0),
        }
    }
}

/// System state for evaluating preemption triggers
#[derive(Debug, Clone, Default)]
pub struct SystemState {
    /// Resource utilization per node
    pub resource_utilization: HashMap<NodeId, ResourceUsage>,
    /// Resource capacity per node
    pub resource_capacity: HashMap<NodeId, ResourceUsage>,
    /// Queue pressure (jobs waiting / capacity)
    pub queue_pressure: f64,
    /// High priority jobs waiting
    pub pending_high_priority: Vec<JobId>,
}

impl SystemState {
    /// Create a new empty system state
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the overall resource utilization for a resource type
    pub fn get_resource_utilization(&self, resource_type: &ResourceType) -> f64 {
        let total_used: f64 = self
            .resource_utilization
            .values()
            .map(|r| r.get_utilization(resource_type))
            .sum();

        let total_capacity: f64 = self
            .resource_capacity
            .values()
            .map(|r| r.get_utilization(resource_type))
            .sum();

        if total_capacity > 0.0 {
            total_used / total_capacity
        } else {
            0.0
        }
    }

    /// Get total available resources across all nodes
    pub fn total_available(&self) -> ResourceUsage {
        let mut total = ResourceUsage::zero();
        for (node_id, capacity) in &self.resource_capacity {
            if let Some(used) = self.resource_utilization.get(node_id) {
                let available = capacity.clone() - used.clone();
                total.add(&available);
            } else {
                total.add(capacity);
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_usage_operations() {
        let r1 = ResourceUsage::new(4.0, 8.0, 1, 100.0);
        let r2 = ResourceUsage::new(2.0, 4.0, 0, 50.0);

        let sum = r1.clone() + r2.clone();
        assert_eq!(sum.cpu_cores, 6.0);
        assert_eq!(sum.memory_gb, 12.0);
        assert_eq!(sum.gpu_count, 1);
        assert_eq!(sum.disk_gb, 150.0);

        let diff = r1.clone() - r2.clone();
        assert_eq!(diff.cpu_cores, 2.0);
        assert_eq!(diff.memory_gb, 4.0);
        assert_eq!(diff.gpu_count, 1);
        assert_eq!(diff.disk_gb, 50.0);
    }

    #[test]
    fn test_resource_usage_can_satisfy() {
        let available = ResourceUsage::new(4.0, 8.0, 1, 100.0);
        let required_small = ResourceUsage::new(2.0, 4.0, 0, 50.0);
        let required_large = ResourceUsage::new(8.0, 16.0, 2, 200.0);

        assert!(available.can_satisfy(&required_small));
        assert!(!available.can_satisfy(&required_large));
    }

    #[test]
    fn test_preemption_policy_creation() {
        let policy = PreemptionPolicy::priority_based("test-policy", "Test Policy", 5);
        assert_eq!(policy.id, "test-policy");
        assert!(matches!(
            policy.trigger,
            PreemptionTrigger::PriorityBased {
                min_priority_difference: 5
            }
        ));
    }

    #[test]
    fn test_preemption_trigger_matches() {
        let trigger = PreemptionTrigger::PriorityBased {
            min_priority_difference: 10,
        };
        let reason = PreemptionReason::HighPriorityJob {
            job_id: "job-1".to_string(),
            priority: 15,
        };
        assert!(trigger.matches(&reason, None));

        let low_priority_reason = PreemptionReason::HighPriorityJob {
            job_id: "job-2".to_string(),
            priority: 5,
        };
        assert!(!trigger.matches(&low_priority_reason, None));
    }
}
