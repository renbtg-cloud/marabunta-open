// Marabunta - Licensed under the MIT License.
//! Failure handler - the core component for handling failures and executing recovery
//!
//! The FailureHandler is responsible for:
//! - Matching failures to recovery policies
//! - Executing recovery strategies
//! - Managing retry state and circuit breakers
//! - Recording failure history for analysis

use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::policy::{CircuitBreakerConfig, NotificationChannel, RecoveryPolicy};
use super::recovery::{
    calculate_backoff_delay, CheckpointSelection, FailureCondition, RecoveryOutcome,
    RecoveryStrategy, TaskStatus,
};
use super::types::{Failure, FailureSeverity, FailureType, JobId, NodeId, TaskId};

/// Information about a checkpoint
#[derive(Debug, Clone)]
pub struct CheckpointInfo {
    /// Unique identifier
    pub id: String,
    /// When the checkpoint was created
    pub created_at: DateTime<Utc>,
    /// Whether the checkpoint has been verified
    pub verified: bool,
    /// Size in bytes
    pub size_bytes: u64,
}

/// Callbacks for executing recovery actions
///
/// These are provided by the caller to integrate with the actual system.
pub struct FailureCallbacks {
    /// Retry a task on a specific node
    pub retry_task: Box<dyn Fn(&TaskId, &NodeId) -> Result<(), String> + Send + Sync>,
    /// Restore a checkpoint for a task
    pub restore_checkpoint: Box<dyn Fn(&TaskId, &str) -> Result<(), String> + Send + Sync>,
    /// Abort a job
    pub abort_job: Box<dyn Fn(&JobId, bool) -> Result<(), String> + Send + Sync>,
    /// Send a notification
    pub notify: Box<dyn Fn(&NotificationChannel, &str) -> Result<(), String> + Send + Sync>,
    /// Get available nodes for a task
    pub get_available_nodes:
        Box<dyn Fn(&super::types::FailureContext) -> Vec<NodeId> + Send + Sync>,
    /// Get checkpoints for a task
    pub get_checkpoints: Box<dyn Fn(&TaskId) -> Vec<CheckpointInfo> + Send + Sync>,
    /// Mark a node as failed
    pub mark_node_failed: Box<dyn Fn(&NodeId, &str) -> Result<(), String> + Send + Sync>,
}

impl Default for FailureCallbacks {
    fn default() -> Self {
        Self {
            retry_task: Box::new(|task_id, node_id| {
                debug!("Default retry_task callback: {} on {}", task_id, node_id);
                Ok(())
            }),
            restore_checkpoint: Box::new(|task_id, checkpoint_id| {
                debug!(
                    "Default restore_checkpoint callback: {} from {}",
                    task_id, checkpoint_id
                );
                Ok(())
            }),
            abort_job: Box::new(|job_id, save_partial| {
                debug!(
                    "Default abort_job callback: {} (save_partial: {})",
                    job_id, save_partial
                );
                Ok(())
            }),
            notify: Box::new(|_channel, message| {
                info!("Default notify callback: {}", message);
                Ok(())
            }),
            get_available_nodes: Box::new(|_context| {
                debug!("Default get_available_nodes callback");
                Vec::new()
            }),
            get_checkpoints: Box::new(|task_id| {
                debug!("Default get_checkpoints callback: {}", task_id);
                Vec::new()
            }),
            mark_node_failed: Box::new(|node_id, reason| {
                debug!(
                    "Default mark_node_failed callback: {} - {}",
                    node_id, reason
                );
                Ok(())
            }),
        }
    }
}

/// State tracking for retry attempts
#[derive(Debug, Clone)]
pub struct RetryState {
    /// Number of retry attempts made
    pub attempts: u32,
    /// Timestamp of last retry attempt
    pub last_attempt: DateTime<Utc>,
    /// Delay before next retry
    pub next_delay: Duration,
    /// Nodes that have failed for this task
    pub failed_nodes: Vec<NodeId>,
}

impl Default for RetryState {
    fn default() -> Self {
        Self {
            attempts: 0,
            last_attempt: Utc::now(),
            next_delay: Duration::zero(),
            failed_nodes: Vec::new(),
        }
    }
}

/// Internal recovery state tracking
struct RecoveryState {
    /// Retry state per task
    task_retries: HashMap<TaskId, RetryState>,
    /// Failures per job
    job_failures: HashMap<JobId, Vec<Failure>>,
    /// Failures per node
    node_failures: HashMap<NodeId, Vec<Failure>>,
}

impl RecoveryState {
    fn new() -> Self {
        Self {
            task_retries: HashMap::new(),
            job_failures: HashMap::new(),
            node_failures: HashMap::new(),
        }
    }

    fn get_retry_state(&self, task_id: &TaskId) -> Option<&RetryState> {
        self.task_retries.get(task_id)
    }

    fn get_or_create_retry_state(&mut self, task_id: &TaskId) -> &mut RetryState {
        self.task_retries
            .entry(task_id.clone())
            .or_default()
    }

    fn record_failure(&mut self, failure: &Failure) {
        if let Some(job_id) = &failure.context.job_id {
            self.job_failures
                .entry(job_id.clone())
                .or_default()
                .push(failure.clone());
        }
        if let Some(node_id) = &failure.context.node_id {
            self.node_failures
                .entry(node_id.clone())
                .or_default()
                .push(failure.clone());
        }
    }

    fn task_failure_count(&self, task_id: &TaskId) -> u32 {
        self.task_retries
            .get(task_id)
            .map(|s| s.attempts)
            .unwrap_or(0)
    }

    fn job_failure_count(&self, job_id: &JobId) -> usize {
        self.job_failures.get(job_id).map(|f| f.len()).unwrap_or(0)
    }

    fn cleanup_old_failures(&mut self, retention: Duration) {
        let cutoff = Utc::now() - retention;

        for failures in self.job_failures.values_mut() {
            failures.retain(|f| f.detected_at > cutoff);
        }
        for failures in self.node_failures.values_mut() {
            failures.retain(|f| f.detected_at > cutoff);
        }

        // Remove empty entries
        self.job_failures.retain(|_, v| !v.is_empty());
        self.node_failures.retain(|_, v| !v.is_empty());
    }
}

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    /// Normal operation - requests allowed
    Closed,
    /// Circuit is open - requests blocked
    Open,
    /// Testing if system has recovered
    HalfOpen,
}

/// Internal circuit breaker
struct CircuitBreaker {
    state: CircuitBreakerState,
    failure_count: u32,
    success_count: u32,
    last_failure: Option<DateTime<Utc>>,
    last_state_change: DateTime<Utc>,
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure: None,
            last_state_change: Utc::now(),
            config,
        }
    }

    fn allows(&self) -> bool {
        match self.state {
            CircuitBreakerState::Closed => true,
            CircuitBreakerState::Open => {
                // Check if timeout has elapsed
                let elapsed = Utc::now().signed_duration_since(self.last_state_change);
                elapsed >= self.config.timeout
            }
            CircuitBreakerState::HalfOpen => true, // Allow test request
        }
    }

    fn record_success(&mut self) {
        match self.state {
            CircuitBreakerState::Closed => {
                // Reset failure count on success
                self.failure_count = 0;
            }
            CircuitBreakerState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= self.config.success_threshold {
                    self.transition_to(CircuitBreakerState::Closed);
                }
            }
            CircuitBreakerState::Open => {
                // Shouldn't happen if allows() is checked first
            }
        }
    }

    fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure = Some(Utc::now());

        match self.state {
            CircuitBreakerState::Closed => {
                if self.failure_count >= self.config.failure_threshold {
                    self.transition_to(CircuitBreakerState::Open);
                }
            }
            CircuitBreakerState::HalfOpen => {
                // Any failure in half-open returns to open
                self.transition_to(CircuitBreakerState::Open);
            }
            CircuitBreakerState::Open => {
                // Already open
            }
        }
    }

    fn transition_to(&mut self, new_state: CircuitBreakerState) {
        if self.state != new_state {
            info!(
                "Circuit breaker transitioning from {:?} to {:?}",
                self.state, new_state
            );
            self.state = new_state;
            self.last_state_change = Utc::now();

            if new_state == CircuitBreakerState::Closed {
                self.failure_count = 0;
                self.success_count = 0;
            } else if new_state == CircuitBreakerState::HalfOpen {
                self.success_count = 0;
            }
        }
    }

    fn maybe_transition_to_half_open(&mut self) {
        if self.state == CircuitBreakerState::Open {
            let elapsed = Utc::now().signed_duration_since(self.last_state_change);
            if elapsed >= self.config.timeout {
                self.transition_to(CircuitBreakerState::HalfOpen);
            }
        }
    }
}

/// Record of a failure and its recovery
#[derive(Debug, Clone)]
pub struct FailureRecord {
    /// The original failure
    pub failure: Failure,
    /// All recovery attempts made
    pub recovery_attempts: Vec<RecoveryAttempt>,
    /// Final outcome
    pub final_outcome: RecoveryOutcome,
    /// Total duration of recovery process
    pub duration: Duration,
}

/// A single recovery attempt
#[derive(Debug, Clone)]
pub struct RecoveryAttempt {
    /// Strategy that was attempted
    pub strategy: RecoveryStrategy,
    /// When the attempt started
    pub started_at: DateTime<Utc>,
    /// When the attempt completed
    pub completed_at: DateTime<Utc>,
    /// Outcome of this attempt
    pub outcome: RecoveryOutcome,
    /// Additional details
    pub details: String,
}

/// Failure history storage
struct FailureHistory {
    failures: Vec<FailureRecord>,
    max_size: usize,
}

impl FailureHistory {
    fn new(max_size: usize) -> Self {
        Self {
            failures: Vec::new(),
            max_size,
        }
    }

    fn add(&mut self, record: FailureRecord) {
        self.failures.push(record);

        // Trim if over capacity
        while self.failures.len() > self.max_size {
            self.failures.remove(0);
        }
    }

    fn get_for_job(&self, job_id: &JobId) -> Vec<FailureRecord> {
        self.failures
            .iter()
            .filter(|r| r.failure.context.job_id.as_ref() == Some(job_id))
            .cloned()
            .collect()
    }

    fn get_for_node(&self, node_id: &NodeId) -> Vec<FailureRecord> {
        self.failures
            .iter()
            .filter(|r| r.failure.context.node_id.as_ref() == Some(node_id))
            .cloned()
            .collect()
    }

    fn cleanup_old(&mut self, retention: Duration) {
        let cutoff = Utc::now() - retention;
        self.failures.retain(|r| r.failure.detected_at > cutoff);
    }
}

/// Scope for calculating failure rates
#[derive(Debug, Clone)]
pub enum FailureRateScope {
    /// Global failure rate
    Global,
    /// Failure rate for a specific node
    Node(NodeId),
    /// Failure rate for a specific job
    Job(JobId),
    /// Failure rate for a specific failure type
    FailureType(FailureType),
}

/// Result of handling a failure
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// ID of the failure that was handled
    pub failure_id: String,
    /// Final outcome
    pub outcome: RecoveryOutcome,
    /// All recovery attempts made
    pub attempts: Vec<RecoveryAttempt>,
    /// Total duration
    pub duration: Duration,
    /// Notifications that were sent
    pub notifications_sent: Vec<String>,
}

/// Statistics about failures
#[derive(Debug, Clone)]
pub struct FailureStatistics {
    /// Total number of failures
    pub total_failures: u64,
    /// Failures grouped by type
    pub failures_by_type: HashMap<String, u64>,
    /// Failures grouped by severity
    pub failures_by_severity: HashMap<FailureSeverity, u64>,
    /// Percentage of successful recoveries
    pub recovery_success_rate: f64,
    /// Average time to recover
    pub mean_recovery_time: Duration,
    /// Top failing nodes
    pub top_failing_nodes: Vec<(NodeId, u64)>,
    /// Top failing jobs
    pub top_failing_jobs: Vec<(JobId, u64)>,
}

/// The main failure handler
pub struct FailureHandler {
    /// Recovery policies (sorted by priority)
    policies: Vec<RecoveryPolicy>,
    /// Recovery state tracking
    recovery_state: Arc<RwLock<RecoveryState>>,
    /// Circuit breakers by scope key
    circuit_breakers: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    /// Callbacks for executing actions
    callbacks: Arc<FailureCallbacks>,
    /// Historical failure records
    failure_history: Arc<RwLock<FailureHistory>>,
    /// Last notification timestamps for throttling
    last_notifications: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
}

impl FailureHandler {
    /// Create a new failure handler with the given callbacks
    pub fn new(callbacks: FailureCallbacks) -> Self {
        Self {
            policies: Vec::new(),
            recovery_state: Arc::new(RwLock::new(RecoveryState::new())),
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            callbacks: Arc::new(callbacks),
            failure_history: Arc::new(RwLock::new(FailureHistory::new(10000))),
            last_notifications: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new failure handler with default callbacks
    pub fn with_defaults() -> Self {
        Self::new(FailureCallbacks::default())
    }

    // Policy management

    /// Add a recovery policy
    pub fn add_policy(&mut self, policy: RecoveryPolicy) {
        self.policies.push(policy);
        // Sort by priority (highest first)
        self.policies.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Remove a recovery policy by ID
    pub fn remove_policy(&mut self, policy_id: &str) -> Option<RecoveryPolicy> {
        if let Some(pos) = self.policies.iter().position(|p| p.id == policy_id) {
            Some(self.policies.remove(pos))
        } else {
            None
        }
    }

    /// Get all policies
    pub fn get_policies(&self) -> &[RecoveryPolicy] {
        &self.policies
    }

    // Core failure handling

    /// Handle a failure - main entry point
    pub async fn handle_failure(&self, failure: Failure) -> RecoveryResult {
        let start_time = Utc::now();
        let failure_id = failure.id.clone();
        let mut attempts = Vec::new();
        let mut notifications_sent = Vec::new();

        info!("Handling failure: {}", failure);

        // Record the failure
        {
            let mut state = self.recovery_state.write().await;
            state.record_failure(&failure);
        }

        // Find matching policy
        let policy = self.find_policy(&failure);

        let (outcome, policy_attempts, policy_notifications) = if let Some(policy) = policy {
            info!("Found matching policy: {} ({})", policy.name, policy.id);

            // Log failure if policy says to
            if policy.logging.log_failures {
                info!(
                    "Failure logged: {} - {} (severity: {})",
                    failure.id, failure.failure_type, failure.severity
                );
            }

            // Send failure notification if configured
            if policy.notification.notify_on_failure {
                for channel in &policy.notification.channels {
                    if self
                        .should_send_notification(channel, policy.notification.throttle)
                        .await
                    {
                        let message = format!("Failure detected: {}", failure);
                        if let Err(e) = (self.callbacks.notify)(channel, &message) {
                            warn!("Failed to send notification: {}", e);
                        } else {
                            notifications_sent.push(format!("{:?}", channel));
                        }
                    }
                }
            }

            // Execute recovery strategy
            let result = self.execute_recovery(&failure, &policy.strategy).await;

            // Send recovery notification if configured
            if result.outcome.is_success() && policy.notification.notify_on_recovery {
                for channel in &policy.notification.channels {
                    if self
                        .should_send_notification(channel, policy.notification.throttle)
                        .await
                    {
                        let message = format!("Recovery successful: {}", failure.id);
                        if let Err(e) = (self.callbacks.notify)(channel, &message) {
                            warn!("Failed to send notification: {}", e);
                        } else {
                            notifications_sent.push(format!("{:?}", channel));
                        }
                    }
                }
            }

            (result.outcome, result.attempts, notifications_sent)
        } else {
            warn!("No matching policy found for failure: {}", failure.id);
            (
                RecoveryOutcome::Failed {
                    error: "No matching policy".to_string(),
                },
                Vec::new(),
                Vec::new(),
            )
        };

        attempts.extend(policy_attempts);
        notifications_sent = policy_notifications;

        let end_time = Utc::now();
        let duration = end_time.signed_duration_since(start_time);

        // Record to history
        {
            let mut history = self.failure_history.write().await;
            history.add(FailureRecord {
                failure,
                recovery_attempts: attempts.clone(),
                final_outcome: outcome.clone(),
                duration,
            });
        }

        RecoveryResult {
            failure_id,
            outcome,
            attempts,
            duration,
            notifications_sent,
        }
    }

    /// Find the first matching policy for a failure
    pub fn find_policy(&self, failure: &Failure) -> Option<&RecoveryPolicy> {
        self.policies.iter().find(|p| p.matches_failure(failure))
    }

    /// Execute a recovery strategy
    pub async fn execute_recovery(
        &self,
        failure: &Failure,
        strategy: &RecoveryStrategy,
    ) -> RecoveryResult {
        let start_time = Utc::now();
        let mut attempts = Vec::new();

        let outcome = self
            .execute_strategy_internal(failure, strategy, &mut attempts)
            .await;

        let end_time = Utc::now();

        RecoveryResult {
            failure_id: failure.id.clone(),
            outcome,
            attempts,
            duration: end_time.signed_duration_since(start_time),
            notifications_sent: Vec::new(),
        }
    }

    fn execute_strategy_internal<'a>(
        &'a self,
        failure: &'a Failure,
        strategy: &'a RecoveryStrategy,
        attempts: &'a mut Vec<RecoveryAttempt>,
    ) -> Pin<Box<dyn Future<Output = RecoveryOutcome> + Send + 'a>> {
        Box::pin(async move {
            let attempt_start = Utc::now();

            let outcome = match strategy {
                RecoveryStrategy::RetryImmediate {
                    max_attempts,
                    delay,
                } => {
                    self.execute_immediate_retry(failure, *max_attempts, *delay)
                        .await
                }

                RecoveryStrategy::RetryWithBackoff {
                    max_attempts,
                    initial_delay,
                    max_delay,
                    multiplier,
                    jitter,
                } => {
                    self.execute_backoff_retry(
                        failure,
                        *max_attempts,
                        *initial_delay,
                        *max_delay,
                        *multiplier,
                        *jitter,
                    )
                    .await
                }

                RecoveryStrategy::RetryOnDifferentNode {
                    max_attempts,
                    exclude_failed_nodes,
                    delay,
                } => {
                    self.execute_different_node_retry(
                        failure,
                        *max_attempts,
                        *exclude_failed_nodes,
                        *delay,
                    )
                    .await
                }

                RecoveryStrategy::RestoreCheckpointAndRetry {
                    checkpoint_selection,
                    max_attempts,
                    retry_strategy,
                } => {
                    self.execute_checkpoint_restore(
                        failure,
                        checkpoint_selection,
                        *max_attempts,
                        retry_strategy,
                        attempts,
                    )
                    .await
                }

                RecoveryStrategy::Failover {
                    replica_selector,
                    sync_state,
                } => {
                    self.execute_failover(failure, replica_selector, *sync_state)
                        .await
                }

                RecoveryStrategy::Recompute { max_attempts } => {
                    self.execute_recompute(failure, *max_attempts).await
                }

                RecoveryStrategy::SkipAndContinue { mark_as, notify } => {
                    self.execute_skip_and_continue(failure, *mark_as, *notify)
                        .await
                }

                RecoveryStrategy::EscalateToHuman {
                    notification_channels,
                    timeout,
                    default_action,
                } => {
                    self.execute_escalate(
                        failure,
                        notification_channels,
                        *timeout,
                        default_action,
                        attempts,
                    )
                    .await
                }

                RecoveryStrategy::Abort {
                    save_partial_results,
                    notify,
                } => {
                    self.execute_abort(failure, *save_partial_results, *notify)
                        .await
                }

                RecoveryStrategy::Custom { handler_id, params } => {
                    self.execute_custom(failure, handler_id, params).await
                }

                RecoveryStrategy::Cascade(strategies) => {
                    self.execute_cascade(failure, strategies, attempts).await
                }

                RecoveryStrategy::Conditional {
                    conditions,
                    default,
                } => {
                    self.execute_conditional(failure, conditions, default, attempts)
                        .await
                }
            };

            let attempt_end = Utc::now();
            attempts.push(RecoveryAttempt {
                strategy: strategy.clone(),
                started_at: attempt_start,
                completed_at: attempt_end,
                outcome: outcome.clone(),
                details: strategy.description(),
            });

            outcome
        })
    }

    async fn execute_immediate_retry(
        &self,
        failure: &Failure,
        max_attempts: u32,
        delay: Duration,
    ) -> RecoveryOutcome {
        let task_id = match &failure.context.task_id {
            Some(id) => id,
            None => {
                return RecoveryOutcome::Failed {
                    error: "No task ID in failure context".to_string(),
                }
            }
        };

        let node_id = failure
            .context
            .node_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        for attempt in 1..=max_attempts {
            debug!(
                "Immediate retry attempt {}/{} for task {}",
                attempt, max_attempts, task_id
            );

            // Wait for delay
            if delay.num_milliseconds() > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    delay.num_milliseconds() as u64
                ))
                .await;
            }

            // Update retry state
            {
                let mut state = self.recovery_state.write().await;
                let retry_state = state.get_or_create_retry_state(task_id);
                retry_state.attempts = attempt;
                retry_state.last_attempt = Utc::now();
            }

            // Attempt retry
            match (self.callbacks.retry_task)(task_id, &node_id) {
                Ok(()) => {
                    info!(
                        "Retry successful for task {} on attempt {}",
                        task_id, attempt
                    );
                    return RecoveryOutcome::Recovered;
                }
                Err(e) => {
                    warn!(
                        "Retry attempt {} failed for task {}: {}",
                        attempt, task_id, e
                    );
                }
            }
        }

        RecoveryOutcome::Failed {
            error: format!("Max retry attempts ({}) exceeded", max_attempts),
        }
    }

    async fn execute_backoff_retry(
        &self,
        failure: &Failure,
        max_attempts: u32,
        initial_delay: Duration,
        max_delay: Duration,
        multiplier: f64,
        jitter: f64,
    ) -> RecoveryOutcome {
        let task_id = match &failure.context.task_id {
            Some(id) => id,
            None => {
                return RecoveryOutcome::Failed {
                    error: "No task ID in failure context".to_string(),
                }
            }
        };

        let node_id = failure
            .context
            .node_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        for attempt in 1..=max_attempts {
            let delay =
                calculate_backoff_delay(attempt, initial_delay, max_delay, multiplier, jitter);
            debug!(
                "Backoff retry attempt {}/{} for task {} (delay: {:?})",
                attempt, max_attempts, task_id, delay
            );

            // Wait for calculated delay
            if delay.num_milliseconds() > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    delay.num_milliseconds() as u64
                ))
                .await;
            }

            // Update retry state
            {
                let mut state = self.recovery_state.write().await;
                let retry_state = state.get_or_create_retry_state(task_id);
                retry_state.attempts = attempt;
                retry_state.last_attempt = Utc::now();
                retry_state.next_delay = calculate_backoff_delay(
                    attempt + 1,
                    initial_delay,
                    max_delay,
                    multiplier,
                    jitter,
                );
            }

            // Attempt retry
            match (self.callbacks.retry_task)(task_id, &node_id) {
                Ok(()) => {
                    info!(
                        "Backoff retry successful for task {} on attempt {}",
                        task_id, attempt
                    );
                    return RecoveryOutcome::Recovered;
                }
                Err(e) => {
                    warn!(
                        "Backoff retry attempt {} failed for task {}: {}",
                        attempt, task_id, e
                    );
                }
            }
        }

        RecoveryOutcome::Failed {
            error: format!("Max backoff retry attempts ({}) exceeded", max_attempts),
        }
    }

    async fn execute_different_node_retry(
        &self,
        failure: &Failure,
        max_attempts: u32,
        exclude_failed_nodes: bool,
        delay: Duration,
    ) -> RecoveryOutcome {
        let task_id = match &failure.context.task_id {
            Some(id) => id,
            None => {
                return RecoveryOutcome::Failed {
                    error: "No task ID in failure context".to_string(),
                }
            }
        };

        // Get available nodes
        let available_nodes = (self.callbacks.get_available_nodes)(&failure.context);
        if available_nodes.is_empty() {
            return RecoveryOutcome::Failed {
                error: "No available nodes for retry".to_string(),
            };
        }

        // Get failed nodes if we need to exclude them
        let failed_nodes = if exclude_failed_nodes {
            let state = self.recovery_state.read().await;
            state
                .get_retry_state(task_id)
                .map(|s| s.failed_nodes.clone())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        for attempt in 1..=max_attempts {
            // Find a node that hasn't failed
            let candidate_nodes: Vec<_> = available_nodes
                .iter()
                .filter(|n| !failed_nodes.contains(n))
                .cloned()
                .collect();

            if candidate_nodes.is_empty() {
                return RecoveryOutcome::Failed {
                    error: "All available nodes have failed".to_string(),
                };
            }

            // Pick a node (could be more sophisticated - round robin, least loaded, etc.)
            let node_id = &candidate_nodes[attempt as usize % candidate_nodes.len()];

            debug!(
                "Different node retry attempt {}/{} for task {} on node {}",
                attempt, max_attempts, task_id, node_id
            );

            // Wait for delay
            if delay.num_milliseconds() > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(
                    delay.num_milliseconds() as u64
                ))
                .await;
            }

            // Attempt retry on different node
            match (self.callbacks.retry_task)(task_id, node_id) {
                Ok(()) => {
                    info!(
                        "Different node retry successful for task {} on node {}",
                        task_id, node_id
                    );
                    return RecoveryOutcome::Recovered;
                }
                Err(e) => {
                    warn!(
                        "Different node retry attempt {} failed for task {} on {}: {}",
                        attempt, task_id, node_id, e
                    );

                    // Record this node as failed
                    if exclude_failed_nodes {
                        let mut state = self.recovery_state.write().await;
                        let retry_state = state.get_or_create_retry_state(task_id);
                        if !retry_state.failed_nodes.contains(node_id) {
                            retry_state.failed_nodes.push(node_id.clone());
                        }
                    }
                }
            }
        }

        RecoveryOutcome::Failed {
            error: format!(
                "Max different node retry attempts ({}) exceeded",
                max_attempts
            ),
        }
    }

    async fn execute_checkpoint_restore(
        &self,
        failure: &Failure,
        checkpoint_selection: &CheckpointSelection,
        max_attempts: u32,
        retry_strategy: &RecoveryStrategy,
        attempts: &mut Vec<RecoveryAttempt>,
    ) -> RecoveryOutcome {
        let task_id = match &failure.context.task_id {
            Some(id) => id,
            None => {
                return RecoveryOutcome::Failed {
                    error: "No task ID in failure context".to_string(),
                }
            }
        };

        // Get checkpoints
        let checkpoints = (self.callbacks.get_checkpoints)(task_id);
        if checkpoints.is_empty() {
            return RecoveryOutcome::Failed {
                error: "No checkpoints available".to_string(),
            };
        }

        // Select checkpoint based on strategy
        let checkpoint = match checkpoint_selection {
            CheckpointSelection::MostRecent => checkpoints.into_iter().max_by_key(|c| c.created_at),
            CheckpointSelection::MostRecentVerified => checkpoints
                .into_iter()
                .filter(|c| c.verified)
                .max_by_key(|c| c.created_at),
            CheckpointSelection::Specific { checkpoint_id } => {
                checkpoints.into_iter().find(|c| c.id == *checkpoint_id)
            }
            CheckpointSelection::NthFromEnd { n } => {
                let mut sorted: Vec<_> = checkpoints;
                sorted.sort_by_key(|c| std::cmp::Reverse(c.created_at));
                sorted.into_iter().nth(*n as usize)
            }
        };

        let checkpoint = match checkpoint {
            Some(c) => c,
            None => {
                return RecoveryOutcome::Failed {
                    error: "No checkpoint matches selection criteria".to_string(),
                }
            }
        };

        for attempt in 1..=max_attempts {
            debug!(
                "Checkpoint restore attempt {}/{} for task {} from checkpoint {}",
                attempt, max_attempts, task_id, checkpoint.id
            );

            // Restore checkpoint
            match (self.callbacks.restore_checkpoint)(task_id, &checkpoint.id) {
                Ok(()) => {
                    info!(
                        "Checkpoint {} restored for task {}, proceeding with retry",
                        checkpoint.id, task_id
                    );

                    // Now execute the retry strategy
                    let retry_outcome = self
                        .execute_strategy_internal(failure, retry_strategy, attempts)
                        .await;

                    if retry_outcome.is_success() {
                        return retry_outcome;
                    }
                }
                Err(e) => {
                    warn!(
                        "Checkpoint restore attempt {} failed for task {}: {}",
                        attempt, task_id, e
                    );
                }
            }
        }

        RecoveryOutcome::Failed {
            error: format!(
                "Max checkpoint restore attempts ({}) exceeded",
                max_attempts
            ),
        }
    }

    async fn execute_failover(
        &self,
        _failure: &Failure,
        replica_selector: &str,
        sync_state: bool,
    ) -> RecoveryOutcome {
        info!(
            "Executing failover with selector '{}' (sync_state: {})",
            replica_selector, sync_state
        );

        // In a real implementation, this would:
        // 1. Find a replica based on the selector
        // 2. Optionally sync state to the replica
        // 3. Redirect traffic to the replica

        // For now, we return a partial recovery indicating the action was taken
        RecoveryOutcome::PartiallyRecovered {
            details: format!(
                "Failover initiated with selector '{}' (sync: {})",
                replica_selector, sync_state
            ),
        }
    }

    async fn execute_recompute(&self, failure: &Failure, max_attempts: u32) -> RecoveryOutcome {
        let task_id = match &failure.context.task_id {
            Some(id) => id,
            None => {
                return RecoveryOutcome::Failed {
                    error: "No task ID in failure context".to_string(),
                }
            }
        };

        // Get any available node
        let available_nodes = (self.callbacks.get_available_nodes)(&failure.context);
        if available_nodes.is_empty() {
            return RecoveryOutcome::Failed {
                error: "No available nodes for recompute".to_string(),
            };
        }

        for attempt in 1..=max_attempts {
            let node_id = &available_nodes[attempt as usize % available_nodes.len()];

            debug!(
                "Recompute attempt {}/{} for task {} on node {}",
                attempt, max_attempts, task_id, node_id
            );

            // Clear retry state (starting fresh)
            {
                let mut state = self.recovery_state.write().await;
                state.task_retries.remove(task_id);
            }

            match (self.callbacks.retry_task)(task_id, node_id) {
                Ok(()) => {
                    info!(
                        "Recompute successful for task {} on node {}",
                        task_id, node_id
                    );
                    return RecoveryOutcome::Recovered;
                }
                Err(e) => {
                    warn!(
                        "Recompute attempt {} failed for task {}: {}",
                        attempt, task_id, e
                    );
                }
            }
        }

        RecoveryOutcome::Failed {
            error: format!("Max recompute attempts ({}) exceeded", max_attempts),
        }
    }

    async fn execute_skip_and_continue(
        &self,
        failure: &Failure,
        mark_as: TaskStatus,
        notify: bool,
    ) -> RecoveryOutcome {
        let task_id = failure
            .context
            .task_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        info!("Skipping task {} and marking as {:?}", task_id, mark_as);

        if notify {
            let message = format!("Task {} skipped and marked as {:?}", task_id, mark_as);
            // Send notification through all available channels
            let _ = (self.callbacks.notify)(
                &NotificationChannel::Log {
                    level: "warn".to_string(),
                },
                &message,
            );
        }

        RecoveryOutcome::PartiallyRecovered {
            details: format!("Task skipped and marked as {:?}", mark_as),
        }
    }

    async fn execute_escalate(
        &self,
        failure: &Failure,
        notification_channels: &[String],
        timeout: Duration,
        default_action: &RecoveryStrategy,
        attempts: &mut Vec<RecoveryAttempt>,
    ) -> RecoveryOutcome {
        info!(
            "Escalating failure {} to human (timeout: {:?})",
            failure.id, timeout
        );

        // Send escalation notifications
        let message = format!(
            "ESCALATION: Failure requires human intervention: {}",
            failure
        );

        for channel_name in notification_channels {
            // Convert string channel name to NotificationChannel
            let channel = if channel_name.starts_with("email:") {
                NotificationChannel::Email {
                    recipients: vec![channel_name[6..].to_string()],
                }
            } else if channel_name.starts_with("slack:") {
                NotificationChannel::Slack {
                    channel: channel_name[6..].to_string(),
                }
            } else {
                NotificationChannel::Log {
                    level: "error".to_string(),
                }
            };

            if let Err(e) = (self.callbacks.notify)(&channel, &message) {
                warn!(
                    "Failed to send escalation notification to {}: {}",
                    channel_name, e
                );
            }
        }

        // Wait for timeout (in production this would wait for human response)
        info!(
            "Waiting {:?} for human response before executing default action",
            timeout
        );

        // For simulation, we'll just wait a short time or skip
        if timeout.num_seconds() > 0 {
            let wait_time = std::cmp::min(timeout.num_seconds() as u64, 5); // Cap at 5 seconds for testing
            tokio::time::sleep(std::time::Duration::from_secs(wait_time)).await;
        }

        // No human response, execute default action
        info!("No human response, executing default action");
        let default_outcome = self
            .execute_strategy_internal(failure, default_action, attempts)
            .await;

        if default_outcome.is_success() {
            default_outcome
        } else {
            RecoveryOutcome::Escalated {
                to: notification_channels.join(", "),
            }
        }
    }

    async fn execute_abort(
        &self,
        failure: &Failure,
        save_partial_results: bool,
        notify: bool,
    ) -> RecoveryOutcome {
        let job_id = failure
            .context
            .job_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        info!(
            "Aborting job {} (save_partial: {})",
            job_id, save_partial_results
        );

        if let Err(e) = (self.callbacks.abort_job)(&job_id, save_partial_results) {
            error!("Failed to abort job {}: {}", job_id, e);
            return RecoveryOutcome::Failed {
                error: format!("Failed to abort job: {}", e),
            };
        }

        if notify {
            let message = format!(
                "Job {} aborted due to failure: {}",
                job_id, failure.failure_type
            );
            let _ = (self.callbacks.notify)(
                &NotificationChannel::Log {
                    level: "error".to_string(),
                },
                &message,
            );
        }

        RecoveryOutcome::Aborted
    }

    async fn execute_custom(
        &self,
        _failure: &Failure,
        handler_id: &str,
        params: &HashMap<String, String>,
    ) -> RecoveryOutcome {
        info!(
            "Executing custom handler '{}' with params: {:?}",
            handler_id, params
        );

        // In a real implementation, this would look up and execute a custom handler
        // For now, we return a failure indicating the handler wasn't found

        RecoveryOutcome::Failed {
            error: format!("Custom handler '{}' not implemented", handler_id),
        }
    }

    async fn execute_cascade(
        &self,
        failure: &Failure,
        strategies: &[RecoveryStrategy],
        attempts: &mut Vec<RecoveryAttempt>,
    ) -> RecoveryOutcome {
        info!("Executing cascade of {} strategies", strategies.len());

        let mut last_outcome = RecoveryOutcome::Failed {
            error: "No strategies in cascade".to_string(),
        };

        for (i, strategy) in strategies.iter().enumerate() {
            debug!(
                "Cascade: trying strategy {}/{}: {}",
                i + 1,
                strategies.len(),
                strategy.description()
            );

            let outcome = self
                .execute_strategy_internal(failure, strategy, attempts)
                .await;

            if outcome.is_success() {
                info!(
                    "Cascade: strategy {} succeeded: {}",
                    i + 1,
                    strategy.description()
                );
                return outcome;
            }

            // Preserve outcome for terminal strategies like Abort
            last_outcome = outcome;

            // If this was a terminal strategy (Abort), return its outcome
            if matches!(strategy, RecoveryStrategy::Abort { .. }) {
                return last_outcome;
            }

            debug!("Cascade: strategy {} failed, trying next", i + 1);
        }

        last_outcome
    }

    async fn execute_conditional(
        &self,
        failure: &Failure,
        conditions: &[(FailureCondition, RecoveryStrategy)],
        default: &RecoveryStrategy,
        attempts: &mut Vec<RecoveryAttempt>,
    ) -> RecoveryOutcome {
        // Evaluate conditions to find matching strategy
        for (condition, strategy) in conditions {
            if self.evaluate_condition(condition, failure).await {
                debug!(
                    "Conditional: condition {:?} matched, executing strategy",
                    condition
                );
                return self
                    .execute_strategy_internal(failure, strategy, attempts)
                    .await;
            }
        }

        debug!("Conditional: no conditions matched, using default strategy");
        self.execute_strategy_internal(failure, default, attempts)
            .await
    }

    async fn evaluate_condition(&self, condition: &FailureCondition, failure: &Failure) -> bool {
        match condition {
            FailureCondition::FailureTypeIs(ft) => failure.failure_type.matches(ft),

            FailureCondition::SeverityAtLeast(severity) => failure.severity >= *severity,

            FailureCondition::RetryCountExceeds(count) => {
                if let Some(task_id) = &failure.context.task_id {
                    let state = self.recovery_state.read().await;
                    state.task_failure_count(task_id) > *count
                } else {
                    false
                }
            }

            FailureCondition::TotalFailuresExceed(count) => {
                if let Some(job_id) = &failure.context.job_id {
                    let state = self.recovery_state.read().await;
                    state.job_failure_count(job_id) > *count as usize
                } else {
                    false
                }
            }

            FailureCondition::FailureRateExceeds { rate, window } => {
                let actual_rate = self.failure_rate(FailureRateScope::Global, *window).await;
                actual_rate > *rate
            }

            FailureCondition::TimeOfDay { hours } => {
                let current_hour = Utc::now()
                    .format("%H")
                    .to_string()
                    .parse::<u32>()
                    .unwrap_or(0);
                hours.contains(&current_hour)
            }

            FailureCondition::Custom { evaluator_id } => {
                warn!(
                    "Custom evaluator '{}' not implemented, returning false",
                    evaluator_id
                );
                false
            }
        }
    }

    async fn should_send_notification(
        &self,
        channel: &NotificationChannel,
        throttle: Option<Duration>,
    ) -> bool {
        let Some(throttle_duration) = throttle else {
            return true;
        };

        let channel_key = format!("{:?}", channel);
        let mut last_notifications = self.last_notifications.write().await;

        if let Some(last_sent) = last_notifications.get(&channel_key) {
            let elapsed = Utc::now().signed_duration_since(*last_sent);
            if elapsed < throttle_duration {
                return false;
            }
        }

        last_notifications.insert(channel_key, Utc::now());
        true
    }

    // Circuit breaker operations

    /// Check if circuit breaker allows an operation
    pub async fn circuit_allows(&self, scope_key: &str) -> bool {
        let mut breakers = self.circuit_breakers.write().await;
        if let Some(breaker) = breakers.get_mut(scope_key) {
            breaker.maybe_transition_to_half_open();
            breaker.allows()
        } else {
            true // No circuit breaker means allowed
        }
    }

    /// Record a success for circuit breaker
    pub async fn record_success(&self, scope_key: &str) {
        let mut breakers = self.circuit_breakers.write().await;
        if let Some(breaker) = breakers.get_mut(scope_key) {
            breaker.record_success();
        }
    }

    /// Record a failure for circuit breaker
    pub async fn record_failure(&self, scope_key: &str) {
        let mut breakers = self.circuit_breakers.write().await;
        if let Some(breaker) = breakers.get_mut(scope_key) {
            breaker.record_failure();
        }
    }

    /// Get or create a circuit breaker for a scope
    pub async fn get_or_create_circuit_breaker(
        &self,
        scope_key: &str,
        config: CircuitBreakerConfig,
    ) {
        let mut breakers = self.circuit_breakers.write().await;
        breakers
            .entry(scope_key.to_string())
            .or_insert_with(|| CircuitBreaker::new(config));
    }

    // Queries

    /// Get failure history for a job
    pub async fn job_failures(&self, job_id: &JobId) -> Vec<FailureRecord> {
        let history = self.failure_history.read().await;
        history.get_for_job(job_id)
    }

    /// Get failure history for a node
    pub async fn node_failures(&self, node_id: &NodeId) -> Vec<FailureRecord> {
        let history = self.failure_history.read().await;
        history.get_for_node(node_id)
    }

    /// Get current retry state for a task
    pub async fn retry_state(&self, task_id: &TaskId) -> Option<RetryState> {
        let state = self.recovery_state.read().await;
        state.get_retry_state(task_id).cloned()
    }

    /// Get all circuit breaker states
    pub async fn circuit_breaker_states(&self) -> HashMap<String, CircuitBreakerState> {
        let breakers = self.circuit_breakers.read().await;
        breakers.iter().map(|(k, v)| (k.clone(), v.state)).collect()
    }

    // Analysis

    /// Calculate failure rate for a scope within a time window
    pub async fn failure_rate(&self, scope: FailureRateScope, window: Duration) -> f64 {
        let history = self.failure_history.read().await;
        let cutoff = Utc::now() - window;

        let failures: Vec<_> = history
            .failures
            .iter()
            .filter(|r| r.failure.detected_at >= cutoff)
            .filter(|r| match &scope {
                FailureRateScope::Global => true,
                FailureRateScope::Node(node_id) => {
                    r.failure.context.node_id.as_ref() == Some(node_id)
                }
                FailureRateScope::Job(job_id) => r.failure.context.job_id.as_ref() == Some(job_id),
                FailureRateScope::FailureType(ft) => r.failure.failure_type.matches(ft),
            })
            .collect();

        let window_secs = window.num_seconds() as f64;
        if window_secs > 0.0 {
            failures.len() as f64 / window_secs * 60.0 // failures per minute
        } else {
            0.0
        }
    }

    /// Get failure statistics for a time window
    pub async fn statistics(&self, window: Duration) -> FailureStatistics {
        let history = self.failure_history.read().await;
        let cutoff = Utc::now() - window;

        let recent: Vec<_> = history
            .failures
            .iter()
            .filter(|r| r.failure.detected_at >= cutoff)
            .collect();

        let total_failures = recent.len() as u64;

        // Count by type
        let mut failures_by_type: HashMap<String, u64> = HashMap::new();
        for record in &recent {
            *failures_by_type
                .entry(record.failure.failure_type.type_key().to_string())
                .or_insert(0) += 1;
        }

        // Count by severity
        let mut failures_by_severity: HashMap<FailureSeverity, u64> = HashMap::new();
        for record in &recent {
            *failures_by_severity
                .entry(record.failure.severity)
                .or_insert(0) += 1;
        }

        // Calculate recovery success rate
        let successful_recoveries = recent
            .iter()
            .filter(|r| r.final_outcome.is_success())
            .count();
        let recovery_success_rate = if total_failures > 0 {
            successful_recoveries as f64 / total_failures as f64
        } else {
            1.0
        };

        // Calculate mean recovery time
        let total_recovery_ms: i64 = recent.iter().map(|r| r.duration.num_milliseconds()).sum();
        let mean_recovery_time = if total_failures > 0 {
            Duration::milliseconds(total_recovery_ms / total_failures as i64)
        } else {
            Duration::zero()
        };

        // Top failing nodes
        let mut node_counts: HashMap<NodeId, u64> = HashMap::new();
        for record in &recent {
            if let Some(node_id) = &record.failure.context.node_id {
                *node_counts.entry(node_id.clone()).or_insert(0) += 1;
            }
        }
        let mut top_failing_nodes: Vec<_> = node_counts.into_iter().collect();
        top_failing_nodes.sort_by(|a, b| b.1.cmp(&a.1));
        top_failing_nodes.truncate(10);

        // Top failing jobs
        let mut job_counts: HashMap<JobId, u64> = HashMap::new();
        for record in &recent {
            if let Some(job_id) = &record.failure.context.job_id {
                *job_counts.entry(job_id.clone()).or_insert(0) += 1;
            }
        }
        let mut top_failing_jobs: Vec<_> = job_counts.into_iter().collect();
        top_failing_jobs.sort_by(|a, b| b.1.cmp(&a.1));
        top_failing_jobs.truncate(10);

        FailureStatistics {
            total_failures,
            failures_by_type,
            failures_by_severity,
            recovery_success_rate,
            mean_recovery_time,
            top_failing_nodes,
            top_failing_jobs,
        }
    }

    /// Cleanup old failure records
    pub async fn cleanup(&self, retention: Duration) {
        {
            let mut state = self.recovery_state.write().await;
            state.cleanup_old_failures(retention);
        }
        {
            let mut history = self.failure_history.write().await;
            history.cleanup_old(retention);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::policy::CircuitBreakerScope;
    use crate::failure::types::FailureContext;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn create_test_callbacks() -> FailureCallbacks {
        let retry_count = Arc::new(AtomicU32::new(0));
        let retry_count_clone = Arc::clone(&retry_count);

        FailureCallbacks {
            retry_task: Box::new(move |_task_id, _node_id| {
                let count = retry_count_clone.fetch_add(1, Ordering::SeqCst);
                // Succeed on 3rd attempt
                if count >= 2 {
                    Ok(())
                } else {
                    Err("Simulated failure".to_string())
                }
            }),
            restore_checkpoint: Box::new(|_task_id, _checkpoint_id| Ok(())),
            abort_job: Box::new(|_job_id, _save_partial| Ok(())),
            notify: Box::new(|_channel, _message| Ok(())),
            get_available_nodes: Box::new(|_context| {
                vec![
                    "node-1".to_string(),
                    "node-2".to_string(),
                    "node-3".to_string(),
                ]
            }),
            get_checkpoints: Box::new(|_task_id| {
                vec![
                    CheckpointInfo {
                        id: "cp-1".to_string(),
                        created_at: Utc::now() - Duration::hours(1),
                        verified: true,
                        size_bytes: 1024,
                    },
                    CheckpointInfo {
                        id: "cp-2".to_string(),
                        created_at: Utc::now() - Duration::minutes(30),
                        verified: true,
                        size_bytes: 2048,
                    },
                ]
            }),
            mark_node_failed: Box::new(|_node_id, _reason| Ok(())),
        }
    }

    #[tokio::test]
    async fn test_immediate_retry_success() {
        let handler = FailureHandler::new(create_test_callbacks());

        let failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: Some(1),
                stderr: None,
            },
            FailureContext::for_task("job-1", "task-1"),
        );

        let result = handler
            .execute_recovery(
                &failure,
                &RecoveryStrategy::RetryImmediate {
                    max_attempts: 5,
                    delay: Duration::milliseconds(10),
                },
            )
            .await;

        assert!(result.outcome.is_success());
    }

    #[tokio::test]
    async fn test_immediate_retry_failure() {
        // Create callbacks that always fail
        let callbacks = FailureCallbacks {
            retry_task: Box::new(|_task_id, _node_id| Err("Always fails".to_string())),
            ..FailureCallbacks::default()
        };

        let handler = FailureHandler::new(callbacks);

        let failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: Some(1),
                stderr: None,
            },
            FailureContext::for_task("job-1", "task-1"),
        );

        let result = handler
            .execute_recovery(
                &failure,
                &RecoveryStrategy::RetryImmediate {
                    max_attempts: 2,
                    delay: Duration::milliseconds(1),
                },
            )
            .await;

        assert!(!result.outcome.is_success());
    }

    #[tokio::test]
    async fn test_abort_strategy() {
        let handler = FailureHandler::new(FailureCallbacks::default());

        let failure = Failure::new(FailureType::QuorumLost, FailureContext::for_job("job-1"));

        let result = handler
            .execute_recovery(
                &failure,
                &RecoveryStrategy::Abort {
                    save_partial_results: true,
                    notify: true,
                },
            )
            .await;

        assert!(matches!(result.outcome, RecoveryOutcome::Aborted));
    }

    #[tokio::test]
    async fn test_cascade_strategy() {
        let handler = FailureHandler::new(create_test_callbacks());

        let failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: Some(1),
                stderr: None,
            },
            FailureContext::for_task("job-1", "task-1"),
        );

        let result = handler
            .execute_recovery(
                &failure,
                &RecoveryStrategy::Cascade(vec![
                    RecoveryStrategy::RetryImmediate {
                        max_attempts: 5,
                        delay: Duration::milliseconds(1),
                    },
                    RecoveryStrategy::Abort {
                        save_partial_results: true,
                        notify: false,
                    },
                ]),
            )
            .await;

        assert!(result.outcome.is_success());
    }

    #[tokio::test]
    async fn test_circuit_breaker_basic() {
        let handler = FailureHandler::with_defaults();

        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::seconds(60),
            scope: CircuitBreakerScope::Global,
        };

        handler.get_or_create_circuit_breaker("test", config).await;

        // Should be closed initially
        assert!(handler.circuit_allows("test").await);

        // Record failures to open circuit
        handler.record_failure("test").await;
        handler.record_failure("test").await;
        handler.record_failure("test").await;

        // Should be open now
        assert!(!handler.circuit_allows("test").await);
    }

    #[tokio::test]
    async fn test_policy_matching() {
        let mut handler = FailureHandler::with_defaults();

        handler.add_policy(
            RecoveryPolicy::new(
                "critical",
                "Critical Policy",
                super::super::policy::FailureMatcher::Severity(vec![FailureSeverity::Critical]),
                RecoveryStrategy::abort(true),
            )
            .with_priority(100),
        );

        handler.add_policy(
            RecoveryPolicy::new(
                "default",
                "Default Policy",
                super::super::policy::FailureMatcher::All,
                RecoveryStrategy::exponential_backoff(3),
            )
            .with_priority(0),
        );

        let critical_failure = Failure::new(FailureType::QuorumLost, FailureContext::new());

        let policy = handler.find_policy(&critical_failure);
        assert!(policy.is_some());
        assert_eq!(policy.unwrap().id, "critical");

        let low_failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: None,
                stderr: None,
            },
            FailureContext::new(),
        );

        let policy = handler.find_policy(&low_failure);
        assert!(policy.is_some());
        assert_eq!(policy.unwrap().id, "default");
    }

    #[tokio::test]
    async fn test_failure_statistics() {
        let handler = FailureHandler::with_defaults();

        // Handle some failures
        for i in 0..5 {
            let failure = Failure::new(
                FailureType::TaskCrash {
                    exit_code: Some(i),
                    stderr: None,
                },
                FailureContext::for_task(format!("job-{}", i), format!("task-{}", i))
                    .with_node(format!("node-{}", i % 3)),
            );
            let _ = handler.handle_failure(failure).await;
        }

        let stats = handler.statistics(Duration::hours(1)).await;

        assert_eq!(stats.total_failures, 5);
        assert!(stats.failures_by_type.contains_key("task_crash"));
        assert!(!stats.top_failing_nodes.is_empty());
    }
}
