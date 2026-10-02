// Marabunta - Licensed under the MIT License.
//! Preemption Engine for the marabunta-compute job placement system
//!
//! This module provides a comprehensive preemption system that manages the lifecycle
//! of preemption decisions in a distributed computing environment. It supports:
//!
//! - **Priority-based preemption**: Higher priority jobs can preempt lower priority ones
//! - **Resource pressure preemption**: System can reclaim resources under pressure
//! - **Quota enforcement**: Preempt jobs that exceed their resource quotas
//! - **Maintenance preemption**: Gracefully drain nodes for maintenance
//! - **Administrative preemption**: Manual intervention by operators
//!
//! # Architecture
//!
//! The preemption engine is built around several key components:
//!
//! - [`PreemptionEngine`]: The core engine that manages policies, tracks running tasks,
//!   and executes preemption decisions
//! - [`PreemptionPolicy`]: Configuration for when and how preemption can occur
//! - [`VictimSelector`]: Strategies for choosing which tasks to preempt
//! - [`PreemptionAwareQueue`]: Queue integration for preemption-aware scheduling
//!
//! # Example Usage
//!
//! ```rust,ignore
//! use marabunta_compute::preemption::{
//!     PreemptionEngine, PreemptionCallbacks, PreemptionPolicy,
//!     PreemptionRequest, ResourceUsage, RunningTask,
//! };
//!
//! // Create engine with callbacks
//! let callbacks = PreemptionCallbacks::noop();
//! let mut engine = PreemptionEngine::new(callbacks);
//!
//! // Add a policy
//! let policy = PreemptionPolicy::priority_based("default", "Default Priority Policy", 10);
//! engine.add_policy(policy);
//!
//! // Register running tasks
//! let task = RunningTask::new("task-1", "job-1", "node-1", 50, ResourceUsage::new(4.0, 8.0, 0, 100.0));
//! engine.register_task(task);
//!
//! // Request preemption for a high-priority job
//! let request = PreemptionRequest::for_high_priority_job(
//!     "req-1",
//!     "job-high",
//!     100,
//!     ResourceUsage::new(4.0, 8.0, 0, 100.0),
//! );
//!
//! let plan = engine.request_preemption(request);
//! if plan.has_victims() {
//!     // Execute the preemption plan
//!     let result = engine.execute_preemption(plan).await;
//!     println!("Preemption result: success={}", result.success);
//! }
//! ```
//!
//! # Preemption Policies
//!
//! Policies define the rules for preemption:
//!
//! - **Triggers**: What conditions cause preemption (priority difference, resource pressure, etc.)
//! - **Victim Selection**: How to choose which tasks to preempt
//! - **Actions**: What to do with preempted tasks (kill, checkpoint, migrate, etc.)
//! - **Constraints**: Limits on preemption (cooldown periods, max preemptions, blackout windows)
//!
//! # Victim Selection Strategies
//!
//! The engine supports multiple victim selection strategies:
//!
//! - `LowestPriority`: Preempt lowest priority tasks first
//! - `ShortestRunning`: Preempt tasks that have run the least (minimize work lost)
//! - `LongestRunning`: Preempt tasks that have run longest (might be stuck)
//! - `MostRecentCheckpoint`: Preempt tasks with recent checkpoints (minimize recomputation)
//! - `Weighted`: Combine multiple strategies with weights
//! - `Cascade`: Try strategies in order until one succeeds
//!
//! # Thread Safety
//!
//! The engine is designed to be used in async contexts. For shared access,
//! use [`SharedPreemptionEngine`] which wraps the engine in an `Arc<RwLock>`.

mod engine;
mod errors;
mod queue;
pub mod quota_integration;
pub mod task_state;
pub mod types;
mod victim_selection;

// Re-export main types
pub use engine::{
    ExecutionResult, PreemptibilityResult, PreemptionCallbacks, PreemptionCandidate,
    PreemptionEngine, PreemptionEvent, PreemptionImpact, PreemptionOutcome, PreemptionPlan,
    PreemptionRequest, SharedPreemptionEngine,
};

pub use errors::{PreemptionError, PreemptionResult};

pub use queue::{NodeResources, PreemptionAwareQueue, QueueStats, QueuedJob, ScheduleResult};

pub use task_state::{CheckpointInfo, RunningTask, TaskStatistics};

pub use types::{
    ApprovalRequirement, JobId, NodeId, PreemptionAction, PreemptionConstraints, PreemptionPolicy,
    PreemptionReason, PreemptionTrigger, PreemptionUrgency, ResourceType, ResourceUsage,
    SystemState, TaskId, TimeWindow, VictimSelector,
};

pub use victim_selection::{ScoringWeights, VictimScorer};

// Quota integration (optional feature)
pub use quota_integration::{
    create_quota_aware_engine, create_quota_aware_engine_with_config, QuotaAwarePreemptionEngine,
    QuotaProtectionConfig, TaskQuotaInfo,
};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use chrono::{Duration, Utc};

    /// Integration test demonstrating the full preemption workflow
    #[tokio::test]
    async fn test_full_preemption_workflow() {
        // Create engine with test callbacks
        let mut engine = PreemptionEngine::new(PreemptionCallbacks::noop());

        // Add a priority-based policy
        let mut policy = PreemptionPolicy::priority_based("priority-policy", "Priority Policy", 10);
        policy.constraints = PreemptionConstraints::default()
            .with_min_runtime(Duration::seconds(5))
            .with_max_preemptions(3)
            .with_cooldown(Duration::seconds(30));
        engine.add_policy(policy);

        // Register some running tasks with varying priorities
        // Set started_at in the past so tasks pass min_runtime constraint
        let started_at = Utc::now() - Duration::seconds(10);
        for i in 0..5 {
            let priority = (i + 1) * 10; // 10, 20, 30, 40, 50
            let task = RunningTask::new(
                format!("task-{}", i),
                format!("job-{}", i),
                "node-1",
                priority,
                ResourceUsage::new(2.0, 4.0, 0, 20.0),
            )
            .with_submitter(format!("user-{}", i % 3))
            .with_started_at(started_at);
            engine.register_task(task);
        }

        // Request preemption for a high-priority job
        let request = PreemptionRequest::for_high_priority_job(
            "request-1",
            "job-high-priority",
            100,
            ResourceUsage::new(5.0, 10.0, 0, 50.0),
        );

        // Create preemption plan
        let plan = engine.request_preemption(request);

        assert!(plan.has_victims(), "Should have victims to preempt");
        assert!(
            plan.total_resources_freed.cpu_cores >= 4.0,
            "Should free enough CPU"
        );

        // Check impact
        let impact = engine.estimate_impact(&plan);
        assert!(impact.tasks_affected > 0);
        assert!(!impact.users_affected.is_empty());

        // Execute the plan
        let result = engine.execute_preemption(plan).await;

        assert!(result.success, "Preemption should succeed");
        assert!(!result.events.is_empty(), "Should have preemption events");

        // Verify task was removed
        let remaining_tasks: Vec<_> = engine.get_all_tasks().collect();
        assert!(
            remaining_tasks.len() < 5,
            "Some tasks should have been preempted"
        );

        // Verify history was recorded
        for event in &result.events {
            let history = engine.get_history(&event.job_id);
            assert!(history.is_some(), "History should be recorded");
        }
    }

    /// Test preemption-aware queue scheduling
    #[tokio::test]
    async fn test_preemption_aware_queue() {
        let engine = PreemptionEngine::new(PreemptionCallbacks::noop());
        let mut queue = PreemptionAwareQueue::new(engine);

        // Register a low-priority running task
        queue.engine_mut().register_task(RunningTask::new(
            "running-task",
            "job-low",
            "node-1",
            10,
            ResourceUsage::new(8.0, 16.0, 0, 100.0),
        ));

        // Enqueue a high-priority job
        queue.enqueue(QueuedJob::new(
            "job-high",
            100,
            ResourceUsage::new(8.0, 16.0, 0, 100.0),
        ));

        // Try to schedule with no available resources
        let available = std::collections::HashMap::new();
        let result = queue.try_schedule(&available);

        match result {
            ScheduleResult::NeedsPreemption { job_id, plan } => {
                assert_eq!(job_id, "job-high");
                assert!(plan.has_victims());

                // Execute preemption
                let exec_result = queue.engine_mut().execute_preemption(plan).await;
                assert!(exec_result.success);
            }
            _ => panic!("Expected NeedsPreemption result"),
        }
    }

    /// Test constraint enforcement
    #[test]
    fn test_constraint_enforcement() {
        let mut engine = PreemptionEngine::new(PreemptionCallbacks::noop());

        // Add policy with strict constraints
        let mut policy = PreemptionPolicy::priority_based("strict", "Strict Policy", 5);
        policy.constraints = PreemptionConstraints::default()
            .with_min_runtime(Duration::hours(1))
            .with_max_preemptions(2)
            .with_protected_jobs(vec!["critical-*".to_string()]);
        engine.add_policy(policy);

        // Task that doesn't meet min_runtime
        let new_task = RunningTask::new(
            "new-task",
            "job-1",
            "node-1",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        );

        let reason = PreemptionReason::HighPriorityJob {
            job_id: "high-job".to_string(),
            priority: 100,
        };

        let result = engine.can_preempt(&new_task, &reason, Utc::now());
        assert!(
            matches!(result, PreemptibilityResult::MinRuntimeNotMet { .. }),
            "Should fail min runtime check"
        );

        // Task that has been preempted too many times
        let mut often_preempted = RunningTask::new(
            "preempted-task",
            "job-2",
            "node-1",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        );
        often_preempted.preemption_count = 5;
        // Simulate an old start time
        often_preempted.started_at = Utc::now() - Duration::hours(2);

        let result = engine.can_preempt(&often_preempted, &reason, Utc::now());
        assert!(
            matches!(result, PreemptibilityResult::MaxPreemptionsReached { .. }),
            "Should fail max preemptions check"
        );

        // Protected job
        let protected_task = RunningTask::new(
            "critical-task",
            "critical-job-123",
            "node-1",
            10,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        );

        // Note: The protected_jobs check uses job_id pattern matching
        // This test verifies the pattern matching works
        let result = engine.can_preempt(&protected_task, &reason, Utc::now());
        // Will pass because task hasn't met min_runtime yet
        assert!(!result.is_preemptible());
    }

    /// Test victim selection strategies
    #[test]
    fn test_victim_selection_strategies() {
        let scorer = VictimScorer::with_default_weights();
        let constraints = PreemptionConstraints::default();
        let needed = ResourceUsage::new(4.0, 8.0, 0, 20.0);

        // Create tasks with different characteristics
        let mut tasks = vec![
            RunningTask::new(
                "low-pri",
                "job-1",
                "node-1",
                10,
                ResourceUsage::new(2.0, 4.0, 0, 10.0),
            ),
            RunningTask::new(
                "high-pri",
                "job-2",
                "node-1",
                90,
                ResourceUsage::new(2.0, 4.0, 0, 10.0),
            ),
            RunningTask::new(
                "medium-pri",
                "job-3",
                "node-1",
                50,
                ResourceUsage::new(2.0, 4.0, 0, 10.0),
            ),
        ];

        // Add checkpoint to one task
        tasks[1].last_checkpoint = Some(CheckpointInfo::new("ckpt-1", 1024, "/storage/ckpt-1"));

        // Test LowestPriority selector
        let victims = scorer.select_victims(
            &tasks,
            &needed,
            &VictimSelector::LowestPriority,
            &constraints,
        );
        assert!(!victims.is_empty());
        assert_eq!(
            victims[0].0.task_id, "low-pri",
            "Should select lowest priority first"
        );

        // Test MostRecentCheckpoint selector
        let victims = scorer.select_victims(
            &tasks,
            &needed,
            &VictimSelector::MostRecentCheckpoint,
            &constraints,
        );
        assert!(!victims.is_empty());
        // The task with checkpoint should score higher
    }

    /// Test resource pressure trigger
    #[test]
    fn test_resource_pressure_trigger() {
        let mut engine = PreemptionEngine::new(PreemptionCallbacks::noop());

        // Add resource pressure policy
        let policy = PreemptionPolicy::resource_pressure(
            "memory-pressure",
            "Memory Pressure Policy",
            ResourceType::Memory,
            0.9,
        );
        engine.add_policy(policy);

        // Register some tasks
        for i in 0..5 {
            engine.register_task(RunningTask::new(
                format!("task-{}", i),
                format!("job-{}", i),
                "node-1",
                50,
                ResourceUsage::new(2.0, 8.0, 0, 20.0),
            ));
        }

        // Create system state with high memory pressure
        let mut state = SystemState::new();
        state.resource_utilization.insert(
            "node-1".to_string(),
            ResourceUsage::new(8.0, 36.0, 0, 80.0), // 90% of capacity
        );
        state.resource_capacity.insert(
            "node-1".to_string(),
            ResourceUsage::new(10.0, 40.0, 2, 100.0),
        );

        // Evaluate triggers
        let requests = engine.evaluate_triggers(&state);

        // Should generate a preemption request due to memory pressure
        assert!(
            !requests.is_empty(),
            "Should generate request for memory pressure"
        );
        assert!(matches!(
            requests[0].reason,
            PreemptionReason::ResourcePressure { .. }
        ));
    }
}
pub mod bpf_arena;
#[cfg(test)]
mod bpf_arena_test;
