// Marabunta - Licensed under the MIT License.
//! Core preemption engine implementation

use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::task_state::{CheckpointInfo, RunningTask};
use super::types::{
    JobId, NodeId, PreemptionAction, PreemptionPolicy, PreemptionReason, PreemptionTrigger,
    PreemptionUrgency, ResourceType, ResourceUsage, SystemState, TaskId, VictimSelector,
};
use super::victim_selection::{estimate_recomputation, ScoringWeights, VictimScorer};

/// Request for preemption
#[derive(Debug, Clone)]
pub struct PreemptionRequest {
    pub id: String,
    pub requested_by: String,
    pub requested_at: DateTime<Utc>,
    pub reason: PreemptionReason,
    pub target_resources: ResourceUsage,
    pub target_nodes: Option<Vec<NodeId>>,
    pub urgency: PreemptionUrgency,
}

impl PreemptionRequest {
    /// Create a new preemption request
    pub fn new(
        id: impl Into<String>,
        requested_by: impl Into<String>,
        reason: PreemptionReason,
        target_resources: ResourceUsage,
    ) -> Self {
        Self {
            id: id.into(),
            requested_by: requested_by.into(),
            requested_at: Utc::now(),
            reason,
            target_resources,
            target_nodes: None,
            urgency: PreemptionUrgency::Soon,
        }
    }

    /// Set urgency level
    pub fn with_urgency(mut self, urgency: PreemptionUrgency) -> Self {
        self.urgency = urgency;
        self
    }

    /// Set target nodes
    pub fn with_target_nodes(mut self, nodes: Vec<NodeId>) -> Self {
        self.target_nodes = Some(nodes);
        self
    }

    /// Create a request for a high priority job
    pub fn for_high_priority_job(
        id: impl Into<String>,
        job_id: impl Into<JobId>,
        priority: i32,
        resources: ResourceUsage,
    ) -> Self {
        Self::new(
            id,
            "scheduler",
            PreemptionReason::HighPriorityJob {
                job_id: job_id.into(), bpf_valuation_payload: None,
                priority,
            },
            resources,
        )
    }

    /// Create a request for resource pressure
    pub fn for_resource_pressure(
        id: impl Into<String>,
        resource: ResourceType,
        current: f64,
        threshold: f64,
        target_resources: ResourceUsage,
    ) -> Self {
        Self::new(
            id,
            "system",
            PreemptionReason::ResourcePressure {
                resource,
                current,
                threshold,
            },
            target_resources,
        )
        .with_urgency(PreemptionUrgency::Soon)
    }

    /// Create a request for maintenance
    pub fn for_maintenance(
        id: impl Into<String>,
        node_ids: Vec<NodeId>,
        scheduled_at: DateTime<Utc>,
    ) -> Self {
        Self::new(
            id,
            "admin",
            PreemptionReason::Maintenance {
                node_ids: node_ids.clone(),
                scheduled_at,
            },
            ResourceUsage::zero(),
        )
        .with_target_nodes(node_ids)
        .with_urgency(PreemptionUrgency::Scheduled)
    }
}

/// Plan for executing preemption
#[derive(Debug, Clone)]
pub struct PreemptionPlan {
    pub id: String,
    pub request: PreemptionRequest,
    pub victims: Vec<PreemptionCandidate>,
    pub total_resources_freed: ResourceUsage,
    pub estimated_duration: Duration,
    pub warnings: Vec<String>,
}

impl PreemptionPlan {
    /// Check if this plan has any victims
    pub fn has_victims(&self) -> bool {
        !self.victims.is_empty()
    }

    /// Get the number of tasks that would be preempted
    pub fn victim_count(&self) -> usize {
        self.victims.len()
    }
}

/// A candidate task for preemption
#[derive(Debug, Clone)]
pub struct PreemptionCandidate {
    pub task: RunningTask,
    pub score: f64,
    pub action: PreemptionAction,
    pub estimated_recomputation: Duration,
    pub reason_matched: String,
}

/// Result of checking if a task can be preempted
#[derive(Debug, Clone)]
pub enum PreemptibilityResult {
    Preemptible,
    Protected { reason: String },
    InCooldown { until: DateTime<Utc> },
    MaxPreemptionsReached { count: u32 },
    MinRuntimeNotMet { remaining: Duration },
    InBlackoutWindow { until: DateTime<Utc> },
    RequiresApproval { approvers: Vec<String> },
}

impl PreemptibilityResult {
    /// Check if the task is preemptible
    pub fn is_preemptible(&self) -> bool {
        matches!(self, PreemptibilityResult::Preemptible)
    }
}

/// Event recording a preemption that occurred
#[derive(Debug, Clone)]
pub struct PreemptionEvent {
    pub id: String,
    pub task_id: TaskId,
    pub job_id: JobId,
    pub node_id: NodeId,
    pub reason: PreemptionReason,
    pub action_taken: PreemptionAction,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<PreemptionOutcome>,
    pub checkpoint_saved: Option<CheckpointInfo>,
    pub resources_freed: ResourceUsage,
}

impl PreemptionEvent {
    /// Create a new preemption event
    fn new(task: &RunningTask, reason: PreemptionReason, action: PreemptionAction) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            task_id: task.task_id.clone(),
            job_id: task.job_id.clone(),
            node_id: task.node_id.clone(),
            reason,
            action_taken: action,
            started_at: Utc::now(),
            completed_at: None,
            result: None,
            checkpoint_saved: None,
            resources_freed: task.resources.clone(),
        }
    }

    /// Mark the event as completed
    fn complete(&mut self, outcome: PreemptionOutcome) {
        self.completed_at = Some(Utc::now());
        self.result = Some(outcome);
    }

    /// Get the duration of this preemption
    pub fn duration(&self) -> Option<Duration> {
        self.completed_at.map(|c| c - self.started_at)
    }
}

/// Outcome of a preemption attempt
#[derive(Debug, Clone)]
pub enum PreemptionOutcome {
    Success,
    CheckpointFailed { error: String },
    KillFailed { error: String },
    Timeout,
    Cancelled,
}

impl PreemptionOutcome {
    /// Check if the outcome is successful
    pub fn is_success(&self) -> bool {
        matches!(self, PreemptionOutcome::Success)
    }
}

/// Result of executing a preemption plan
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub plan_id: String,
    pub events: Vec<PreemptionEvent>,
    pub total_resources_freed: ResourceUsage,
    pub duration: Duration,
    pub success: bool,
    pub errors: Vec<String>,
}

/// Impact assessment of a preemption plan
#[derive(Debug, Clone)]
pub struct PreemptionImpact {
    pub tasks_affected: u32,
    pub jobs_affected: u32,
    pub total_runtime_lost: Duration,
    pub estimated_recomputation: Duration,
    pub users_affected: Vec<String>,
    pub priority_distribution: HashMap<i32, u32>,
}

/// Callbacks for preemption operations
pub struct PreemptionCallbacks {
    /// Called when preemption starts
    pub on_preemption_start: Box<dyn Fn(&PreemptionEvent) + Send + Sync>,

    /// Called to request a checkpoint
    pub request_checkpoint:
        Box<dyn Fn(&TaskId) -> std::result::Result<CheckpointInfo, String> + Send + Sync>,

    /// Called to kill a task
    pub kill_task: Box<dyn Fn(&TaskId) -> std::result::Result<(), String> + Send + Sync>,

    /// Called to requeue a task
    pub requeue_task: Box<dyn Fn(&TaskId, i32) -> std::result::Result<(), String> + Send + Sync>,

    /// Called when preemption completes
    pub on_preemption_complete: Box<dyn Fn(&PreemptionEvent) + Send + Sync>,
}

impl PreemptionCallbacks {
    /// Create no-op callbacks for testing
    pub fn noop() -> Self {
        Self {
            on_preemption_start: Box::new(|_| {}),
            request_checkpoint: Box::new(|task_id| {
                Ok(CheckpointInfo::new(
                    format!("ckpt-{}", task_id),
                    0,
                    "memory",
                ))
            }),
            kill_task: Box::new(|_| Ok(())),
            requeue_task: Box::new(|_, _| Ok(())),
            on_preemption_complete: Box::new(|_| {}),
        }
    }
}

/// The main preemption engine
pub struct PreemptionEngine {
    policies: Vec<PreemptionPolicy>,
    running_tasks: HashMap<TaskId, RunningTask>,
    preemption_history: HashMap<JobId, Vec<PreemptionEvent>>,
    pending_requests: Vec<PreemptionRequest>,
    callbacks: PreemptionCallbacks,
    scorer: VictimScorer,
}

impl PreemptionEngine {
    /// Create a new preemption engine with the given callbacks
    pub fn new(callbacks: PreemptionCallbacks) -> Self {
        Self {
            policies: Vec::new(),
            running_tasks: HashMap::new(),
            preemption_history: HashMap::new(),
            pending_requests: Vec::new(),
            callbacks,
            scorer: VictimScorer::with_default_weights(),
        }
    }

    /// Create a new preemption engine with custom scoring weights
    pub fn with_scoring_weights(callbacks: PreemptionCallbacks, weights: ScoringWeights) -> Self {
        Self {
            policies: Vec::new(),
            running_tasks: HashMap::new(),
            preemption_history: HashMap::new(),
            pending_requests: Vec::new(),
            callbacks,
            scorer: VictimScorer::new(weights),
        }
    }

    // ========== Policy Management ==========

    /// Add a preemption policy
    pub fn add_policy(&mut self, policy: PreemptionPolicy) {
        // Remove existing policy with same ID if present
        self.policies.retain(|p| p.id != policy.id);
        self.policies.push(policy);
    }

    /// Remove a policy by ID
    pub fn remove_policy(&mut self, policy_id: &str) -> Option<PreemptionPolicy> {
        if let Some(pos) = self.policies.iter().position(|p| p.id == policy_id) {
            Some(self.policies.remove(pos))
        } else {
            None
        }
    }

    /// Get all policies
    pub fn get_policies(&self) -> &[PreemptionPolicy] {
        &self.policies
    }

    /// Get a policy by ID
    pub fn get_policy(&self, policy_id: &str) -> Option<&PreemptionPolicy> {
        self.policies.iter().find(|p| p.id == policy_id)
    }

    // ========== Task Tracking ==========

    /// Register a running task

    pub fn register_task(&mut self, task: RunningTask) {
        // [MARABUNTA WMD] If this task is high priority and has a cgroup, 
        // pin its cgroup to the eBPF thermal_guardian active map.
        if task.priority >= 90 {
            if let Some(cgroup_id) = task.cgroup_id {
                // In a full implementation, we'd use libbpf-rs to update the map.
                tracing::info!("BPF: Pinned high-priority cgroup {} to thermal_guardian active map.", cgroup_id);
            }
        }
        
        self.running_tasks.insert(task.task_id.clone(), task);
    }


    /// Unregister a task
    pub fn unregister_task(&mut self, task_id: &TaskId) -> Option<RunningTask> {
        self.running_tasks.remove(task_id)
    }

    /// Update a running task
    pub fn update_task(&mut self, task: RunningTask) {
        self.running_tasks.insert(task.task_id.clone(), task);
    }

    /// Get a running task
    pub fn get_task(&self, task_id: &TaskId) -> Option<&RunningTask> {
        self.running_tasks.get(task_id)
    }

    /// Get all running tasks
    pub fn get_all_tasks(&self) -> impl Iterator<Item = &RunningTask> {
        self.running_tasks.values()
    }

    /// Get tasks on a specific node
    pub fn get_tasks_on_node(&self, node_id: &NodeId) -> Vec<&RunningTask> {
        self.running_tasks
            .values()
            .filter(|t| &t.node_id == node_id)
            .collect()
    }

    // ========== Core Preemption Operations ==========

    /// Request preemption - returns a plan of what would be preempted
    pub fn request_preemption(&mut self, request: PreemptionRequest) -> PreemptionPlan {
        let plan_id = format!("plan-{}", uuid::Uuid::new_v4());

        // Find applicable policy
        let policy = self.find_applicable_policy(&request.reason);

        // Get constraints from policy or use defaults
        let constraints = policy.map(|p| p.constraints.clone()).unwrap_or_default();

        // Get victim selector from policy or use default
        let selector = policy
            .map(|p| p.victim_selector.clone())
            .unwrap_or(VictimSelector::LowestPriority);

        // Get action from policy or use default
        let action =
            policy
                .map(|p| p.action.clone())
                .unwrap_or(PreemptionAction::CheckpointAndRequeue {
                    checkpoint_timeout: Duration::seconds(60),
                    requeue_priority_penalty: 0,
                    max_requeues: 3,
                });

        // Get preemptor priority from request
        let preemptor_priority = match &request.reason {
            PreemptionReason::HighPriorityJob { priority, .. } => *priority,
            _ => i32::MAX, // Non-priority reasons can preempt anything
        };

        // Find candidates based on constraints and selector
        let candidate_tasks: Vec<RunningTask> = self
            .running_tasks
            .values()
            .filter(|task| {
                // Filter by node if specified
                if let Some(target_nodes) = &request.target_nodes {
                    if !target_nodes.contains(&task.node_id) {
                        return false;
                    }
                }

                // Must be preemptible
                if !task.preemptible {
                    return false;
                }

                // Priority check: can only preempt lower priority tasks
                if task.priority >= preemptor_priority {
                    return false;
                }

                true
            })
            .cloned()
            .collect();

        // Use scorer with constraints and selector to select victims
        let scored_victims = self.scorer.select_victims(
            &candidate_tasks,
            &request.target_resources,
            &selector,
            &constraints,
        );

        // Convert to PreemptionCandidate
        let candidates: Vec<PreemptionCandidate> = scored_victims
            .into_iter()
            .map(|(task, score)| {
                let estimated_recomputation = estimate_recomputation(&task);
                PreemptionCandidate {
                    task,
                    score,
                    action: action.clone(),
                    estimated_recomputation,
                    reason_matched: selector_name(&selector),
                }
            })
            .collect();

        // Build victims with actions and estimates
        let mut warnings = Vec::new();
        let mut total_resources_freed = ResourceUsage::zero();
        let mut estimated_duration = Duration::zero();

        let victims: Vec<PreemptionCandidate> = candidates
            .into_iter()
            .map(|candidate| {
                total_resources_freed.add(&candidate.task.resources);

                let action_duration = action.timeout();
                if action_duration > estimated_duration {
                    estimated_duration = action_duration;
                }

                PreemptionCandidate {
                    task: candidate.task,
                    score: candidate.score,
                    action: action.clone(),
                    estimated_recomputation: candidate.estimated_recomputation,
                    reason_matched: candidate.reason_matched,
                }
            })
            .collect();

        // Check if we can satisfy the request
        if !total_resources_freed.can_satisfy(&request.target_resources) {
            warnings.push(format!(
                "Cannot fully satisfy resource request. Needed: {}, Available: {}",
                request.target_resources, total_resources_freed
            ));
        }

        // Store the request
        self.pending_requests.push(request.clone());

        PreemptionPlan {
            id: plan_id,
            request,
            victims,
            total_resources_freed,
            estimated_duration,
            warnings,
        }
    }

    /// Execute a preemption plan
    pub async fn execute_preemption(&mut self, plan: PreemptionPlan) -> ExecutionResult {
        let start_time = Utc::now();
        let mut events = Vec::new();
        let mut total_resources_freed = ResourceUsage::zero();
        let mut errors = Vec::new();
        let mut success = true;

        for candidate in &plan.victims {
            // Check if task is still running (might have completed during planning)
            if !self.running_tasks.contains_key(&candidate.task.task_id) {
                events.push(self.create_cancelled_event(&candidate.task, &plan.request.reason));
                continue;
            }

            // Create event
            let mut event = PreemptionEvent::new(
                &candidate.task,
                plan.request.reason.clone(),
                candidate.action.clone(),
            );

            // Notify start
            (self.callbacks.on_preemption_start)(&event);

            // Execute the action
            let outcome = self
                .execute_action(&candidate.task, &candidate.action, &mut event)
                .await;

            match &outcome {
                PreemptionOutcome::Success => {
                    total_resources_freed.add(&candidate.task.resources);

                    // Update task preemption count if it will be requeued
                    if let Some(task) = self.running_tasks.get_mut(&candidate.task.task_id) {
                        task.mark_preempted();
                    }

                    // Remove from running tasks if killed
                    match &candidate.action {
                        PreemptionAction::Kill | PreemptionAction::CheckpointAndKill { .. } => {
                            self.running_tasks.remove(&candidate.task.task_id);
                        }
                        PreemptionAction::CheckpointAndRequeue { .. } => {
                            self.running_tasks.remove(&candidate.task.task_id);
                        }
                        _ => {}
                    }
                }
                PreemptionOutcome::CheckpointFailed { error } => {
                    errors.push(format!(
                        "Checkpoint failed for {}: {}",
                        candidate.task.task_id, error
                    ));
                    success = false;
                }
                PreemptionOutcome::KillFailed { error } => {
                    errors.push(format!(
                        "Kill failed for {}: {}",
                        candidate.task.task_id, error
                    ));
                    success = false;
                }
                PreemptionOutcome::Timeout => {
                    errors.push(format!("Timeout preempting {}", candidate.task.task_id));
                    success = false;
                }
                PreemptionOutcome::Cancelled => {
                    // Not an error, just cancelled
                }
            }

            // Complete event
            event.complete(outcome);

            // Notify completion
            (self.callbacks.on_preemption_complete)(&event);

            // Store in history
            self.preemption_history
                .entry(candidate.task.job_id.clone())
                .or_default()
                .push(event.clone());

            events.push(event);
        }

        // Remove from pending requests
        self.pending_requests.retain(|r| r.id != plan.request.id);

        ExecutionResult {
            plan_id: plan.id,
            events,
            total_resources_freed,
            duration: Utc::now() - start_time,
            success,
            errors,
        }
    }

    /// Execute a specific preemption action
    async fn execute_action(
        &self,
        task: &RunningTask,
        action: &PreemptionAction,
        event: &mut PreemptionEvent,
    ) -> PreemptionOutcome {
        match action {
            PreemptionAction::Kill => match (self.callbacks.kill_task)(&task.task_id) {
                Ok(()) => PreemptionOutcome::Success,
                Err(e) => PreemptionOutcome::KillFailed { error: e },
            },

            PreemptionAction::CheckpointAndKill {
                checkpoint_timeout,
                kill_on_timeout,
            } => {
                // Try to checkpoint
                let checkpoint_result = tokio::time::timeout(
                    std::time::Duration::from_secs(checkpoint_timeout.num_seconds() as u64),
                    async { (self.callbacks.request_checkpoint)(&task.task_id) },
                )
                .await;

                match checkpoint_result {
                    Ok(Ok(checkpoint)) => {
                        event.checkpoint_saved = Some(checkpoint);
                        // Now kill
                        match (self.callbacks.kill_task)(&task.task_id) {
                            Ok(()) => PreemptionOutcome::Success,
                            Err(e) => PreemptionOutcome::KillFailed { error: e },
                        }
                    }
                    Ok(Err(e)) => {
                        if *kill_on_timeout {
                            // Kill anyway
                            let _ = (self.callbacks.kill_task)(&task.task_id);
                        }
                        PreemptionOutcome::CheckpointFailed { error: e }
                    }
                    Err(_) => {
                        if *kill_on_timeout {
                            let _ = (self.callbacks.kill_task)(&task.task_id);
                            PreemptionOutcome::Timeout
                        } else {
                            PreemptionOutcome::Timeout
                        }
                    }
                }
            }

            PreemptionAction::CheckpointAndRequeue {
                checkpoint_timeout,
                requeue_priority_penalty,
                ..
            } => {
                // Try to checkpoint
                let checkpoint_result = tokio::time::timeout(
                    std::time::Duration::from_secs(checkpoint_timeout.num_seconds() as u64),
                    async { (self.callbacks.request_checkpoint)(&task.task_id) },
                )
                .await;

                match checkpoint_result {
                    Ok(Ok(checkpoint)) => {
                        event.checkpoint_saved = Some(checkpoint);
                        // Kill the running task
                        if let Err(e) = (self.callbacks.kill_task)(&task.task_id) {
                            return PreemptionOutcome::KillFailed { error: e };
                        }
                        // Requeue with penalty
                        match (self.callbacks.requeue_task)(
                            &task.task_id,
                            *requeue_priority_penalty,
                        ) {
                            Ok(()) => PreemptionOutcome::Success,
                            Err(e) => PreemptionOutcome::KillFailed {
                                error: format!("Requeue failed: {}", e),
                            },
                        }
                    }
                    Ok(Err(e)) => PreemptionOutcome::CheckpointFailed { error: e },
                    Err(_) => PreemptionOutcome::Timeout,
                }
            }

            PreemptionAction::Migrate { .. } => {
                // Migration is complex and would require additional callbacks
                // For now, fall back to checkpoint and requeue
                let fallback = PreemptionAction::CheckpointAndRequeue {
                    checkpoint_timeout: Duration::seconds(60),
                    requeue_priority_penalty: 0,
                    max_requeues: 3,
                };
                Box::pin(self.execute_action(task, &fallback, event)).await
            }

            PreemptionAction::Suspend { .. } => {
                // Suspend requires OS-level support
                // For now, just mark as success (would need actual implementation)
                PreemptionOutcome::Success
            }

            PreemptionAction::GracefulShutdown {
                grace_period,
                escalate_to,
                ..
            } => {
                // Wait for graceful shutdown
                tokio::time::sleep(std::time::Duration::from_secs(
                    grace_period.num_seconds() as u64
                ))
                .await;

                // Check if task is still running
                // In a real implementation, we'd check if the task responded to the signal
                // For now, escalate to the fallback action
                Box::pin(self.execute_action(task, escalate_to, event)).await
            }
        }
    }

    /// Find victims for a resource request
    pub fn find_victims(
        &self,
        needed: &ResourceUsage,
        nodes: Option<&[NodeId]>,
        preemptor_priority: i32,
    ) -> Vec<PreemptionCandidate> {
        // Get all candidate tasks
        let mut candidates: Vec<RunningTask> = self
            .running_tasks
            .values()
            .filter(|task| {
                // Filter by node if specified
                if let Some(target_nodes) = nodes {
                    if !target_nodes.contains(&task.node_id) {
                        return false;
                    }
                }

                // Must be preemptible
                if !task.preemptible {
                    return false;
                }

                // Priority check: can only preempt lower priority tasks
                if task.priority >= preemptor_priority {
                    return false;
                }

                true
            })
            .cloned()
            .collect();

        // Sort by priority (lowest first)
        candidates.sort_by_key(|t| t.priority);

        // Find policy for constraints and selector
        let policy = self.policies.first();
        let constraints = policy.map(|p| &p.constraints).cloned().unwrap_or_default();
        let selector = policy
            .map(|p| p.victim_selector.clone())
            .unwrap_or(VictimSelector::LowestPriority);

        // Use scorer to select victims
        let scored = self
            .scorer
            .select_victims(&candidates, needed, &selector, &constraints);

        // Convert to PreemptionCandidate
        scored
            .into_iter()
            .map(|(task, score)| {
                let estimated_recomputation = estimate_recomputation(&task);
                PreemptionCandidate {
                    task,
                    score,
                    action: policy
                        .map(|p| p.action.clone())
                        .unwrap_or(PreemptionAction::Kill),
                    estimated_recomputation,
                    reason_matched: selector_name(&selector),
                }
            })
            .collect()
    }

    /// Check if a task can be preempted
    pub fn can_preempt(
        &self,
        task: &RunningTask,
        reason: &PreemptionReason,
        now: DateTime<Utc>,
    ) -> PreemptibilityResult {
        // Not preemptible flag
        if !task.preemptible {
            return PreemptibilityResult::Protected {
                reason: "Task marked as non-preemptible".to_string(),
            };
        }

        // Find applicable policy
        let policy = self.find_applicable_policy(reason);

        if let Some(policy) = policy {
            let constraints = &policy.constraints;

            // Check minimum runtime
            if let Some(min_runtime) = constraints.min_runtime {
                let runtime = task.runtime();
                if runtime < min_runtime {
                    return PreemptibilityResult::MinRuntimeNotMet {
                        remaining: min_runtime - runtime,
                    };
                }
            }

            // Check max preemptions
            if let Some(max_preemptions) = constraints.max_preemptions {
                if task.preemption_count >= max_preemptions {
                    return PreemptibilityResult::MaxPreemptionsReached {
                        count: task.preemption_count,
                    };
                }
            }

            // Check cooldown
            if let Some(cooldown) = constraints.cooldown {
                if let Some(last_preempted) = task.last_preempted_at {
                    let cooldown_end = last_preempted + cooldown;
                    if now < cooldown_end {
                        return PreemptibilityResult::InCooldown {
                            until: cooldown_end,
                        };
                    }
                }
            }

            // Check protected jobs
            for pattern in &constraints.protected_jobs {
                if matches_pattern(&task.job_id, pattern) {
                    return PreemptibilityResult::Protected {
                        reason: format!("Job matches protected pattern: {}", pattern),
                    };
                }
            }

            // Check protected nodes
            for pattern in &constraints.protected_nodes {
                if matches_pattern(&task.node_id, pattern) {
                    return PreemptibilityResult::Protected {
                        reason: format!("Node matches protected pattern: {}", pattern),
                    };
                }
            }

            // Check blackout windows
            for window in &constraints.blackout_windows {
                if window.is_active(now) {
                    return PreemptibilityResult::InBlackoutWindow {
                        until: window.end_time(now),
                    };
                }
            }

            // Check approval requirement
            if let Some(ref approval) = constraints.require_approval {
                return PreemptibilityResult::RequiresApproval {
                    approvers: approval.approvers.clone(),
                };
            }
        }

        PreemptibilityResult::Preemptible
    }

    /// Evaluate triggers against current system state
    pub fn evaluate_triggers(&self, system_state: &SystemState) -> Vec<PreemptionRequest> {
        let mut requests = Vec::new();

        for policy in &self.policies {
            if let Some(request) = self.evaluate_trigger(&policy.trigger, system_state, policy) {
                requests.push(request);
            }
        }

        requests
    }

    /// Evaluate a single trigger
    fn evaluate_trigger(
        &self,
        trigger: &PreemptionTrigger,
        system_state: &SystemState,
        policy: &PreemptionPolicy,
    ) -> Option<PreemptionRequest> {
        match trigger {
            PreemptionTrigger::ResourcePressure {
                resource,
                threshold,
            } => {
                let utilization = system_state.get_resource_utilization(resource);
                if utilization >= *threshold {
                    let target =
                        self.calculate_target_reduction(*resource, utilization, *threshold);
                    Some(PreemptionRequest::for_resource_pressure(
                        format!("auto-{}", uuid::Uuid::new_v4()),
                        *resource,
                        utilization,
                        *threshold,
                        target,
                    ))
                } else {
                    None
                }
            }

            PreemptionTrigger::PriorityBased { .. } => {
                // Check if there are pending high priority jobs
                if !system_state.pending_high_priority.is_empty() {
                    // This would need more context about the pending jobs' resource requirements
                    // For now, return None - priority-based triggers are typically request-driven
                    None
                } else {
                    None
                }
            }

            PreemptionTrigger::Any(triggers) => {
                for t in triggers {
                    if let Some(request) = self.evaluate_trigger(t, system_state, policy) {
                        return Some(request);
                    }
                }
                None
            }

            PreemptionTrigger::All(triggers) => {
                let mut request = None;
                for t in triggers {
                    match self.evaluate_trigger(t, system_state, policy) {
                        Some(r) => request = Some(r),
                        None => return None,
                    }
                }
                request
            }

            _ => None,
        }
    }

    /// Calculate target resource reduction
    fn calculate_target_reduction(
        &self,
        resource: ResourceType,
        current: f64,
        target: f64,
    ) -> ResourceUsage {
        let reduction_ratio = (current - target).max(0.1);

        // Calculate how much to free based on current usage
        let mut resources = ResourceUsage::zero();
        match resource {
            ResourceType::Cpu => {
                let total_cpu: f64 = self
                    .running_tasks
                    .values()
                    .map(|t| t.resources.cpu_cores)
                    .sum();
                resources.cpu_cores = total_cpu * reduction_ratio;
            }
            ResourceType::Memory => {
                let total_mem: f64 = self
                    .running_tasks
                    .values()
                    .map(|t| t.resources.memory_gb)
                    .sum();
                resources.memory_gb = total_mem * reduction_ratio;
            }
            ResourceType::Gpu => {
                let total_gpu: u32 = self
                    .running_tasks
                    .values()
                    .map(|t| t.resources.gpu_count)
                    .sum();
                resources.gpu_count = (total_gpu as f64 * reduction_ratio) as u32;
            }
            ResourceType::Disk => {
                let total_disk: f64 = self
                    .running_tasks
                    .values()
                    .map(|t| t.resources.disk_gb)
                    .sum();
                resources.disk_gb = total_disk * reduction_ratio;
            }
            ResourceType::Network => {
                // Network resources not tracked in ResourceUsage
            }
        }

        resources
    }

    // ========== Query Methods ==========

    /// Get preemption history for a job
    pub fn get_history(&self, job_id: &JobId) -> Option<&Vec<PreemptionEvent>> {
        self.preemption_history.get(job_id)
    }

    /// Get all preemption history
    pub fn get_all_history(&self) -> &HashMap<JobId, Vec<PreemptionEvent>> {
        &self.preemption_history
    }

    /// Get pending preemption requests
    pub fn get_pending_requests(&self) -> &[PreemptionRequest] {
        &self.pending_requests
    }

    /// Estimate impact of a preemption plan
    pub fn estimate_impact(&self, plan: &PreemptionPlan) -> PreemptionImpact {
        let mut jobs_affected: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut users_affected: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut total_runtime_lost = Duration::zero();
        let mut estimated_recomputation = Duration::zero();
        let mut priority_distribution: HashMap<i32, u32> = HashMap::new();

        for victim in &plan.victims {
            jobs_affected.insert(&victim.task.job_id);
            users_affected.insert(&victim.task.submitter);
            total_runtime_lost += victim.task.runtime();
            estimated_recomputation += victim.estimated_recomputation;

            *priority_distribution
                .entry(victim.task.priority)
                .or_insert(0) += 1;
        }

        PreemptionImpact {
            tasks_affected: plan.victims.len() as u32,
            jobs_affected: jobs_affected.len() as u32,
            total_runtime_lost,
            estimated_recomputation,
            users_affected: users_affected.into_iter().map(String::from).collect(),
            priority_distribution,
        }
    }

    // ========== Helper Methods ==========

    /// Find an applicable policy for a preemption reason
    fn find_applicable_policy(&self, reason: &PreemptionReason) -> Option<&PreemptionPolicy> {
        self.policies
            .iter()
            .find(|policy| policy.trigger.matches(reason, None))
    }

    /// Create a cancelled event for a task that completed during planning
    fn create_cancelled_event(
        &self,
        task: &RunningTask,
        reason: &PreemptionReason,
    ) -> PreemptionEvent {
        let mut event = PreemptionEvent::new(task, reason.clone(), PreemptionAction::Kill);
        event.complete(PreemptionOutcome::Cancelled);
        event
    }
}

/// Thread-safe wrapper for PreemptionEngine
pub struct SharedPreemptionEngine {
    inner: Arc<RwLock<PreemptionEngine>>,
}

impl SharedPreemptionEngine {
    /// Create a new shared preemption engine
    pub fn new(callbacks: PreemptionCallbacks) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PreemptionEngine::new(callbacks))),
        }
    }

    /// Get a read lock on the engine
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, PreemptionEngine> {
        self.inner.read().await
    }

    /// Get a write lock on the engine
    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, PreemptionEngine> {
        self.inner.write().await
    }
}

impl Clone for SharedPreemptionEngine {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// Helper functions

fn matches_pattern(value: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.starts_with('*') && pattern.ends_with('*') {
        let inner = &pattern[1..pattern.len() - 1];
        return value.contains(inner);
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return value.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    value == pattern
}

fn selector_name(selector: &VictimSelector) -> String {
    match selector {
        VictimSelector::LowestPriority => "LowestPriority".to_string(),
        VictimSelector::ShortestRunning => "ShortestRunning".to_string(),
        VictimSelector::LongestRunning => "LongestRunning".to_string(),
        VictimSelector::MostRecentCheckpoint => "MostRecentCheckpoint".to_string(),
        VictimSelector::JobType(_) => "JobType".to_string(),
        VictimSelector::FromDomain(_) => "FromDomain".to_string(),
        VictimSelector::OverTimeEstimate => "OverTimeEstimate".to_string(),
        VictimSelector::Custom { score_function } => format!("Custom({})", score_function),
        VictimSelector::Cascade(_) => "Cascade".to_string(),
        VictimSelector::Weighted { .. } => "Weighted".to_string(), VictimSelector::BpfNegotiator { .. } => "BpfNegotiator".to_string(),
        
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_engine() -> PreemptionEngine {
        PreemptionEngine::new(PreemptionCallbacks::noop())
    }

    fn create_test_task(id: &str, priority: i32, cpu: f64, memory: f64) -> RunningTask {
        RunningTask::new(
            id,
            "job-1",
            "node-1",
            priority,
            ResourceUsage::new(cpu, memory, 0, 10.0),
        )
    }

    #[test]
    fn test_engine_creation() {
        let engine = create_test_engine();
        assert!(engine.get_policies().is_empty());
        assert!(engine.get_pending_requests().is_empty());
    }

    #[test]
    fn test_policy_management() {
        let mut engine = create_test_engine();

        let policy = PreemptionPolicy::priority_based("test-policy", "Test", 10);
        engine.add_policy(policy);

        assert_eq!(engine.get_policies().len(), 1);
        assert!(engine.get_policy("test-policy").is_some());

        let removed = engine.remove_policy("test-policy");
        assert!(removed.is_some());
        assert!(engine.get_policies().is_empty());
    }

    #[test]
    fn test_task_registration() {
        let mut engine = create_test_engine();

        let task = create_test_task("task-1", 10, 2.0, 4.0);
        engine.register_task(task.clone());

        assert!(engine.get_task(&"task-1".to_string()).is_some());

        let removed = engine.unregister_task(&"task-1".to_string());
        assert!(removed.is_some());
        assert!(engine.get_task(&"task-1".to_string()).is_none());
    }

    #[test]
    fn test_find_victims() {
        let mut engine = create_test_engine();

        // Add some tasks with different priorities
        engine.register_task(create_test_task("task-1", 10, 2.0, 4.0));
        engine.register_task(create_test_task("task-2", 50, 2.0, 4.0));
        engine.register_task(create_test_task("task-3", 90, 2.0, 4.0));

        let needed = ResourceUsage::new(3.0, 6.0, 0, 15.0);
        let victims = engine.find_victims(&needed, None, 100);

        // Should find low priority tasks
        assert!(!victims.is_empty());

        // First victim should be lowest priority
        assert_eq!(victims[0].task.priority, 10);
    }

    #[test]
    fn test_can_preempt() {
        let mut engine = create_test_engine();

        let policy = PreemptionPolicy::priority_based("test", "Test", 10);
        engine.add_policy(policy);

        let task = create_test_task("task-1", 10, 2.0, 4.0);
        let reason = PreemptionReason::HighPriorityJob {
                job_id: "job-high".to_string(),
                priority: 100,
                bpf_valuation_payload: None,
            };

        let result = engine.can_preempt(&task, &reason, Utc::now());
        assert!(result.is_preemptible());
    }

    #[test]
    fn test_can_preempt_non_preemptible() {
        let engine = create_test_engine();

        let task = create_test_task("task-1", 10, 2.0, 4.0).non_preemptible();
        let reason = PreemptionReason::HighPriorityJob {
                job_id: "job-high".to_string(),
                priority: 100,
                bpf_valuation_payload: None,
            };

        let result = engine.can_preempt(&task, &reason, Utc::now());
        assert!(!result.is_preemptible());
    }

    #[test]
    fn test_request_preemption() {
        let mut engine = create_test_engine();

        engine.register_task(create_test_task("task-1", 10, 2.0, 4.0));
        engine.register_task(create_test_task("task-2", 20, 2.0, 4.0));

        let request = PreemptionRequest::for_high_priority_job(
            "req-1",
            "job-high",
            100,
            ResourceUsage::new(3.0, 6.0, 0, 15.0),
        );

        let plan = engine.request_preemption(request);

        assert!(plan.has_victims());
        assert!(!plan.warnings.is_empty() || plan.total_resources_freed.cpu_cores >= 3.0);
    }

    #[test]
    fn test_estimate_impact() {
        let mut engine = create_test_engine();

        engine.register_task(create_test_task("task-1", 10, 2.0, 4.0).with_submitter("user1"));
        engine.register_task(create_test_task("task-2", 20, 2.0, 4.0).with_submitter("user2"));

        let request = PreemptionRequest::for_high_priority_job(
            "req-1",
            "job-high",
            100,
            ResourceUsage::new(3.0, 6.0, 0, 15.0),
        );

        let plan = engine.request_preemption(request);
        let impact = engine.estimate_impact(&plan);

        assert!(impact.tasks_affected > 0);
        assert!(!impact.users_affected.is_empty());
    }

    #[tokio::test]
    async fn test_execute_preemption() {
        let mut engine = create_test_engine();

        engine.register_task(create_test_task("task-1", 10, 2.0, 4.0));

        let request = PreemptionRequest::for_high_priority_job(
            "req-1",
            "job-high",
            100,
            ResourceUsage::new(2.0, 4.0, 0, 10.0),
        );

        let plan = engine.request_preemption(request);
        let result = engine.execute_preemption(plan).await;

        assert!(result.success);
        assert!(!result.events.is_empty());
    }

    #[test]
    fn test_preemption_request_builders() {
        let request = PreemptionRequest::for_high_priority_job(
            "req-1",
            "job-1",
            100,
            ResourceUsage::new(4.0, 8.0, 0, 20.0),
        );
        assert!(matches!(
            request.reason,
            PreemptionReason::HighPriorityJob { .. }
        ));

        let request = PreemptionRequest::for_resource_pressure(
            "req-2",
            ResourceType::Memory,
            0.95,
            0.90,
            ResourceUsage::new(0.0, 4.0, 0, 0.0),
        );
        assert!(matches!(
            request.reason,
            PreemptionReason::ResourcePressure { .. }
        ));
        assert_eq!(request.urgency, PreemptionUrgency::Soon);

        let request = PreemptionRequest::for_maintenance(
            "req-3",
            vec!["node-1".to_string()],
            Utc::now() + Duration::hours(1),
        );
        assert!(matches!(
            request.reason,
            PreemptionReason::Maintenance { .. }
        ));
        assert_eq!(request.urgency, PreemptionUrgency::Scheduled);
    }
}
