// Marabunta - Licensed under the MIT License.
//! Preemption-aware job queue integration

use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

use super::engine::{PreemptionEngine, PreemptionPlan, PreemptionRequest};
use super::types::{JobId, NodeId, PreemptionReason, ResourceUsage};

/// A job waiting in the queue
#[derive(Debug, Clone)]
pub struct QueuedJob {
    pub job_id: JobId,
    pub priority: i32,
    pub resources_needed: ResourceUsage,
    pub can_preempt: bool,
    pub queued_at: DateTime<Utc>,
    pub deadline: Option<DateTime<Utc>>,
}

impl QueuedJob {
    /// Create a new queued job
    pub fn new(job_id: impl Into<JobId>, priority: i32, resources_needed: ResourceUsage) -> Self {
        Self {
            job_id: job_id.into(),
            priority,
            resources_needed,
            can_preempt: true,
            queued_at: Utc::now(),
            deadline: None,
        }
    }

    /// Set whether this job can preempt others
    pub fn with_preemption(mut self, can_preempt: bool) -> Self {
        self.can_preempt = can_preempt;
        self
    }

    /// Set a deadline for this job
    pub fn with_deadline(mut self, deadline: DateTime<Utc>) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Check if this job has a deadline that's approaching
    pub fn is_urgent(&self) -> bool {
        if let Some(deadline) = self.deadline {
            let time_remaining = deadline - Utc::now();
            time_remaining < Duration::minutes(30)
        } else {
            false
        }
    }

    /// Get time spent waiting in queue
    pub fn wait_time(&self) -> Duration {
        Utc::now() - self.queued_at
    }
}

/// Result of attempting to schedule a job
#[derive(Debug)]
pub enum ScheduleResult {
    /// Job was scheduled successfully
    Scheduled { job_id: JobId, nodes: Vec<NodeId> },

    /// Preemption is needed to schedule the job
    NeedsPreemption { job_id: JobId, plan: PreemptionPlan },

    /// Not enough resources even with preemption
    InsufficientResources {
        job_id: JobId,
        shortage: ResourceUsage,
    },

    /// Queue is empty
    QueueEmpty,
}

/// Node resource information for scheduling
#[derive(Debug, Clone)]
pub struct NodeResources {
    pub node_id: NodeId,
    pub total: ResourceUsage,
    pub available: ResourceUsage,
}

impl NodeResources {
    /// Create a new node resource entry
    pub fn new(node_id: impl Into<NodeId>, total: ResourceUsage, available: ResourceUsage) -> Self {
        Self {
            node_id: node_id.into(),
            total,
            available,
        }
    }

    /// Check if this node can satisfy a resource request
    pub fn can_satisfy(&self, needed: &ResourceUsage) -> bool {
        self.available.can_satisfy(needed)
    }

    /// Calculate remaining resources after allocation
    pub fn remaining_after(&self, allocation: &ResourceUsage) -> ResourceUsage {
        self.available.clone() - allocation.clone()
    }
}

/// Preemption-aware job queue
pub struct PreemptionAwareQueue {
    engine: PreemptionEngine,
    pending_jobs: Vec<QueuedJob>,
    job_priorities: HashMap<i32, Vec<JobId>>,
}

impl PreemptionAwareQueue {
    /// Create a new preemption-aware queue
    pub fn new(engine: PreemptionEngine) -> Self {
        Self {
            engine,
            pending_jobs: Vec::new(),
            job_priorities: HashMap::new(),
        }
    }

    /// Get a reference to the preemption engine
    pub fn engine(&self) -> &PreemptionEngine {
        &self.engine
    }

    /// Get a mutable reference to the preemption engine
    pub fn engine_mut(&mut self) -> &mut PreemptionEngine {
        &mut self.engine
    }

    /// Add a job to the queue
    pub fn enqueue(&mut self, job: QueuedJob) {
        // Track by priority
        self.job_priorities
            .entry(job.priority)
            .or_default()
            .push(job.job_id.clone());

        // Insert maintaining priority order (highest priority first)
        let pos = self
            .pending_jobs
            .iter()
            .position(|j| j.priority < job.priority)
            .unwrap_or(self.pending_jobs.len());

        self.pending_jobs.insert(pos, job);
    }

    /// Remove a job from the queue
    pub fn dequeue(&mut self, job_id: &JobId) -> Option<QueuedJob> {
        if let Some(pos) = self.pending_jobs.iter().position(|j| &j.job_id == job_id) {
            let job = self.pending_jobs.remove(pos);

            // Remove from priority tracking
            if let Some(jobs) = self.job_priorities.get_mut(&job.priority) {
                jobs.retain(|id| id != job_id);
            }

            Some(job)
        } else {
            None
        }
    }

    /// Get the next job in the queue
    pub fn peek(&self) -> Option<&QueuedJob> {
        self.pending_jobs.first()
    }

    /// Get all pending jobs
    pub fn pending_jobs(&self) -> &[QueuedJob] {
        &self.pending_jobs
    }

    /// Get the number of pending jobs
    pub fn len(&self) -> usize {
        self.pending_jobs.len()
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        self.pending_jobs.is_empty()
    }

    /// Try to schedule the next job
    pub fn try_schedule(
        &mut self,
        available_resources: &HashMap<NodeId, ResourceUsage>,
    ) -> ScheduleResult {
        // Get the highest priority job
        let job = match self.pending_jobs.first() {
            Some(job) => job.clone(),
            None => return ScheduleResult::QueueEmpty,
        };

        // Try to find nodes with enough resources
        let allocation = self.find_allocation(&job, available_resources);

        match allocation {
            AllocationResult::Success { nodes } => {
                // Remove from queue
                self.dequeue(&job.job_id);
                ScheduleResult::Scheduled {
                    job_id: job.job_id,
                    nodes,
                }
            }

            AllocationResult::NeedsPreemption { shortage } => {
                if !job.can_preempt {
                    return ScheduleResult::InsufficientResources {
                        job_id: job.job_id,
                        shortage,
                    };
                }

                // Check if preemption can help
                if let Some(plan) = self.preemption_required(&job) {
                    ScheduleResult::NeedsPreemption {
                        job_id: job.job_id,
                        plan,
                    }
                } else {
                    ScheduleResult::InsufficientResources {
                        job_id: job.job_id,
                        shortage,
                    }
                }
            }

            AllocationResult::Insufficient { shortage } => ScheduleResult::InsufficientResources {
                job_id: job.job_id,
                shortage,
            },
        }
    }

    /// Find nodes to allocate for a job
    fn find_allocation(
        &self,
        job: &QueuedJob,
        available: &HashMap<NodeId, ResourceUsage>,
    ) -> AllocationResult {
        let needed = &job.resources_needed;

        // Try to find a single node with enough resources
        for (node_id, resources) in available {
            if resources.can_satisfy(needed) {
                return AllocationResult::Success {
                    nodes: vec![node_id.clone()],
                };
            }
        }

        // Calculate total available and potential from preemption
        let total_available = available
            .values()
            .fold(ResourceUsage::zero(), |mut acc, r| {
                acc.add(r);
                acc
            });

        // Calculate what running tasks have
        let total_running: ResourceUsage = self
            .engine
            .get_all_tasks()
            .filter(|t| t.priority < job.priority) // Only count lower priority tasks
            .fold(ResourceUsage::zero(), |mut acc, t| {
                acc.add(&t.resources);
                acc
            });

        let total_possible = total_available.clone() + total_running.clone();

        if total_possible.can_satisfy(needed) {
            // Can satisfy with preemption
            let shortage = needed.clone() - total_available;
            AllocationResult::NeedsPreemption { shortage }
        } else {
            // Not enough even with preemption
            let shortage = needed.clone() - total_possible;
            AllocationResult::Insufficient { shortage }
        }
    }

    /// Check what preemptions would be needed for a job
    pub fn preemption_required(&mut self, job: &QueuedJob) -> Option<PreemptionPlan> {
        if !job.can_preempt {
            return None;
        }

        // Calculate what we need to free
        let victims = self
            .engine
            .find_victims(&job.resources_needed, None, job.priority);

        if victims.is_empty() {
            return None;
        }

        // Create a preemption request
        let request = PreemptionRequest::new(
            format!("sched-{}", uuid::Uuid::new_v4()),
            "scheduler",
            PreemptionReason::HighPriorityJob {
            job_id: job.job_id.clone(),
            priority: job.priority,
            bpf_valuation_payload: None,
        },
            job.resources_needed.clone(),
        );

        Some(self.engine.request_preemption(request))
    }

    /// Get jobs by priority level
    pub fn jobs_by_priority(&self, priority: i32) -> Vec<&QueuedJob> {
        self.pending_jobs
            .iter()
            .filter(|j| j.priority == priority)
            .collect()
    }

    /// Get jobs that are urgent (approaching deadline)
    pub fn urgent_jobs(&self) -> Vec<&QueuedJob> {
        self.pending_jobs.iter().filter(|j| j.is_urgent()).collect()
    }

    /// Get total resources needed by all pending jobs
    pub fn total_pending_resources(&self) -> ResourceUsage {
        self.pending_jobs
            .iter()
            .fold(ResourceUsage::zero(), |mut acc, job| {
                acc.add(&job.resources_needed);
                acc
            })
    }

    /// Re-prioritize a job
    pub fn reprioritize(&mut self, job_id: &JobId, new_priority: i32) -> bool {
        if let Some(job) = self.dequeue(job_id) {
            let mut updated_job = job;
            updated_job.priority = new_priority;
            self.enqueue(updated_job);
            true
        } else {
            false
        }
    }

    /// Cancel a job and remove from queue
    pub fn cancel(&mut self, job_id: &JobId) -> Option<QueuedJob> {
        self.dequeue(job_id)
    }

    /// Get queue statistics
    pub fn stats(&self) -> QueueStats {
        let total_jobs = self.pending_jobs.len();
        let total_resources = self.total_pending_resources();

        let urgent_count = self.urgent_jobs().len();

        let wait_times: Vec<Duration> = self.pending_jobs.iter().map(|j| j.wait_time()).collect();
        let avg_wait_time = if !wait_times.is_empty() {
            Duration::milliseconds(
                wait_times.iter().map(|d| d.num_milliseconds()).sum::<i64>()
                    / wait_times.len() as i64,
            )
        } else {
            Duration::zero()
        };

        let max_wait_time = wait_times.into_iter().max().unwrap_or(Duration::zero());

        let priority_counts: HashMap<i32, usize> = self
            .job_priorities
            .iter()
            .map(|(p, jobs)| (*p, jobs.len()))
            .collect();

        QueueStats {
            total_jobs,
            urgent_jobs: urgent_count,
            total_resources,
            avg_wait_time,
            max_wait_time,
            priority_distribution: priority_counts,
        }
    }
}

/// Result of allocation attempt
enum AllocationResult {
    Success { nodes: Vec<NodeId> },
    NeedsPreemption { shortage: ResourceUsage },
    Insufficient { shortage: ResourceUsage },
}

/// Queue statistics
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub total_jobs: usize,
    pub urgent_jobs: usize,
    pub total_resources: ResourceUsage,
    pub avg_wait_time: Duration,
    pub max_wait_time: Duration,
    pub priority_distribution: HashMap<i32, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preemption::engine::PreemptionCallbacks;
    use crate::preemption::task_state::RunningTask;

    fn create_test_queue() -> PreemptionAwareQueue {
        let engine = PreemptionEngine::new(PreemptionCallbacks::noop());
        PreemptionAwareQueue::new(engine)
    }

    #[test]
    fn test_enqueue_dequeue() {
        let mut queue = create_test_queue();

        let job1 = QueuedJob::new("job-1", 10, ResourceUsage::new(2.0, 4.0, 0, 10.0));
        let job2 = QueuedJob::new("job-2", 20, ResourceUsage::new(2.0, 4.0, 0, 10.0));

        queue.enqueue(job1);
        queue.enqueue(job2);

        assert_eq!(queue.len(), 2);

        // Higher priority job should be first
        assert_eq!(queue.peek().unwrap().job_id, "job-2");

        let removed = queue.dequeue(&"job-2".to_string());
        assert!(removed.is_some());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_priority_ordering() {
        let mut queue = create_test_queue();

        // Add jobs in non-priority order
        queue.enqueue(QueuedJob::new(
            "job-low",
            10,
            ResourceUsage::new(1.0, 1.0, 0, 1.0),
        ));
        queue.enqueue(QueuedJob::new(
            "job-high",
            100,
            ResourceUsage::new(1.0, 1.0, 0, 1.0),
        ));
        queue.enqueue(QueuedJob::new(
            "job-mid",
            50,
            ResourceUsage::new(1.0, 1.0, 0, 1.0),
        ));

        // Should be ordered by priority (highest first)
        let jobs: Vec<&str> = queue
            .pending_jobs()
            .iter()
            .map(|j| j.job_id.as_str())
            .collect();
        assert_eq!(jobs, vec!["job-high", "job-mid", "job-low"]);
    }

    #[test]
    fn test_try_schedule_with_available_resources() {
        let mut queue = create_test_queue();

        queue.enqueue(QueuedJob::new(
            "job-1",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        ));

        let mut available = HashMap::new();
        available.insert("node-1".to_string(), ResourceUsage::new(4.0, 8.0, 1, 100.0));

        let result = queue.try_schedule(&available);

        match result {
            ScheduleResult::Scheduled { job_id, nodes } => {
                assert_eq!(job_id, "job-1");
                assert_eq!(nodes, vec!["node-1"]);
            }
            _ => panic!("Expected Scheduled result"),
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_try_schedule_needs_preemption() {
        let mut queue = create_test_queue();

        // Register a running low-priority task
        queue.engine_mut().register_task(RunningTask::new(
            "task-1",
            "job-low",
            "node-1",
            5,
            ResourceUsage::new(4.0, 8.0, 0, 50.0),
        ));

        // Enqueue a high-priority job
        queue.enqueue(QueuedJob::new(
            "job-high",
            100,
            ResourceUsage::new(4.0, 8.0, 0, 50.0),
        ));

        // No available resources
        let available = HashMap::new();

        let result = queue.try_schedule(&available);

        match result {
            ScheduleResult::NeedsPreemption { job_id, plan } => {
                assert_eq!(job_id, "job-high");
                assert!(plan.has_victims());
            }
            _ => panic!("Expected NeedsPreemption result, got {:?}", result),
        }
    }

    #[test]
    fn test_try_schedule_insufficient_resources() {
        let mut queue = create_test_queue();

        // Very large resource request
        queue.enqueue(
            QueuedJob::new(
                "job-big",
                10,
                ResourceUsage::new(1000.0, 1000.0, 100, 10000.0),
            )
            .with_preemption(false),
        );

        let mut available = HashMap::new();
        available.insert("node-1".to_string(), ResourceUsage::new(4.0, 8.0, 1, 100.0));

        let result = queue.try_schedule(&available);

        assert!(matches!(
            result,
            ScheduleResult::InsufficientResources { .. }
        ));
    }

    #[test]
    fn test_reprioritize() {
        let mut queue = create_test_queue();

        queue.enqueue(QueuedJob::new(
            "job-1",
            10,
            ResourceUsage::new(1.0, 1.0, 0, 1.0),
        ));
        queue.enqueue(QueuedJob::new(
            "job-2",
            20,
            ResourceUsage::new(1.0, 1.0, 0, 1.0),
        ));

        // job-2 should be first (priority 20)
        assert_eq!(queue.peek().unwrap().job_id, "job-2");

        // Boost job-1 priority
        queue.reprioritize(&"job-1".to_string(), 100);

        // Now job-1 should be first
        assert_eq!(queue.peek().unwrap().job_id, "job-1");
    }

    #[test]
    fn test_queue_stats() {
        let mut queue = create_test_queue();

        queue.enqueue(QueuedJob::new(
            "job-1",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        ));
        queue.enqueue(QueuedJob::new(
            "job-2",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        ));
        queue.enqueue(QueuedJob::new(
            "job-3",
            50,
            ResourceUsage::new(4.0, 8.0, 1, 20.0),
        ));

        let stats = queue.stats();

        assert_eq!(stats.total_jobs, 3);
        assert_eq!(stats.total_resources.cpu_cores, 8.0);
        assert_eq!(*stats.priority_distribution.get(&10).unwrap_or(&0), 2);
        assert_eq!(*stats.priority_distribution.get(&50).unwrap_or(&0), 1);
    }

    #[test]
    fn test_urgent_jobs() {
        let mut queue = create_test_queue();

        // Job without deadline
        queue.enqueue(QueuedJob::new(
            "job-1",
            10,
            ResourceUsage::new(1.0, 1.0, 0, 1.0),
        ));

        // Job with urgent deadline (10 minutes from now)
        queue.enqueue(
            QueuedJob::new("job-2", 10, ResourceUsage::new(1.0, 1.0, 0, 1.0))
                .with_deadline(Utc::now() + Duration::minutes(10)),
        );

        // Job with non-urgent deadline (2 hours from now)
        queue.enqueue(
            QueuedJob::new("job-3", 10, ResourceUsage::new(1.0, 1.0, 0, 1.0))
                .with_deadline(Utc::now() + Duration::hours(2)),
        );

        let urgent = queue.urgent_jobs();
        assert_eq!(urgent.len(), 1);
        assert_eq!(urgent[0].job_id, "job-2");
    }
}
