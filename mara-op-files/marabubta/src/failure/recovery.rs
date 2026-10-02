// Marabunta - Licensed under the MIT License.
//! Recovery strategies for handling failures
//!
//! Defines the various recovery strategies that can be applied when a failure occurs,
//! including retry logic, failover, checkpoint restoration, and more.

use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::types::{FailureSeverity, FailureType};

/// Task status for skip-and-continue strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task was skipped entirely
    Skipped,
    /// Task completed with partial results
    PartialSuccess,
    /// Task failed but processing continued
    Failed,
}

/// How to select a checkpoint for restoration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub enum CheckpointSelection {
    /// Use the most recent checkpoint
    #[default]
    MostRecent,
    /// Use the most recent verified checkpoint
    MostRecentVerified,
    /// Use a specific checkpoint by ID
    Specific { checkpoint_id: String },
    /// Use the Nth checkpoint from the end (0 = most recent)
    NthFromEnd { n: u32 },
}


/// Recovery strategies that can be applied to handle failures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryStrategy {
    /// Immediate retry on the same node
    RetryImmediate {
        /// Maximum number of retry attempts
        max_attempts: u32,
        /// Fixed delay between retries
        delay: Duration,
    },

    /// Retry with exponential backoff
    RetryWithBackoff {
        /// Maximum number of retry attempts
        max_attempts: u32,
        /// Initial delay before first retry
        initial_delay: Duration,
        /// Maximum delay cap
        max_delay: Duration,
        /// Multiplier for each subsequent retry (e.g., 2.0 for doubling)
        multiplier: f64,
        /// Random jitter factor (0.0-1.0) to prevent thundering herd
        jitter: f64,
    },

    /// Retry on a different node
    RetryOnDifferentNode {
        /// Maximum number of retry attempts
        max_attempts: u32,
        /// Whether to exclude nodes that have previously failed this task
        exclude_failed_nodes: bool,
        /// Delay before retrying on new node
        delay: Duration,
    },

    /// Restore from checkpoint and retry
    RestoreCheckpointAndRetry {
        /// How to select which checkpoint to restore
        checkpoint_selection: CheckpointSelection,
        /// Maximum attempts for checkpoint restoration
        max_attempts: u32,
        /// Strategy to use for retry after checkpoint restore
        retry_strategy: Box<RecoveryStrategy>,
    },

    /// Failover to a replica or backup system
    Failover {
        /// Selector pattern for choosing replica (e.g., "nearest", "least-loaded")
        replica_selector: String,
        /// Whether to sync state before failover
        sync_state: bool,
    },

    /// Recompute the task from scratch (discard any partial progress)
    Recompute {
        /// Maximum recomputation attempts
        max_attempts: u32,
    },

    /// Skip the failed task and continue processing
    SkipAndContinue {
        /// How to mark the skipped task
        mark_as: TaskStatus,
        /// Whether to send notifications about skipping
        notify: bool,
    },

    /// Escalate to human operator for manual intervention
    EscalateToHuman {
        /// Notification channels to alert humans
        notification_channels: Vec<String>,
        /// How long to wait for human response
        timeout: Duration,
        /// What to do if no human responds in time
        default_action: Box<RecoveryStrategy>,
    },

    /// Abort the job entirely
    Abort {
        /// Whether to save any partial results before aborting
        save_partial_results: bool,
        /// Whether to send notifications about abortion
        notify: bool,
    },

    /// Custom recovery handler (plugin/extension point)
    Custom {
        /// Identifier for the custom handler
        handler_id: String,
        /// Parameters to pass to the handler
        params: HashMap<String, String>,
    },

    /// Try multiple strategies in order until one succeeds
    Cascade(Vec<RecoveryStrategy>),

    /// Choose strategy based on conditions
    Conditional {
        /// List of condition-strategy pairs to evaluate
        conditions: Vec<(FailureCondition, RecoveryStrategy)>,
        /// Default strategy if no conditions match
        default: Box<RecoveryStrategy>,
    },
}

impl RecoveryStrategy {
    /// Create a simple immediate retry strategy
    pub fn immediate_retry(max_attempts: u32, delay_secs: i64) -> Self {
        Self::RetryImmediate {
            max_attempts,
            delay: Duration::seconds(delay_secs),
        }
    }

    /// Create an exponential backoff retry strategy with sensible defaults
    pub fn exponential_backoff(max_attempts: u32) -> Self {
        Self::RetryWithBackoff {
            max_attempts,
            initial_delay: Duration::seconds(1),
            max_delay: Duration::seconds(300), // 5 minutes max
            multiplier: 2.0,
            jitter: 0.1,
        }
    }

    /// Create a different-node retry strategy
    pub fn retry_different_node(max_attempts: u32) -> Self {
        Self::RetryOnDifferentNode {
            max_attempts,
            exclude_failed_nodes: true,
            delay: Duration::seconds(5),
        }
    }

    /// Create an abort strategy
    pub fn abort(save_partial: bool) -> Self {
        Self::Abort {
            save_partial_results: save_partial,
            notify: true,
        }
    }

    /// Create a cascade of strategies
    pub fn cascade(strategies: Vec<RecoveryStrategy>) -> Self {
        Self::Cascade(strategies)
    }

    /// Get a description of this strategy
    pub fn description(&self) -> String {
        match self {
            Self::RetryImmediate {
                max_attempts,
                delay,
            } => {
                format!(
                    "Retry immediately up to {} times with {:?} delay",
                    max_attempts, delay
                )
            }
            Self::RetryWithBackoff {
                max_attempts,
                initial_delay,
                multiplier,
                ..
            } => {
                format!(
                    "Retry with backoff up to {} times (initial: {:?}, multiplier: {}x)",
                    max_attempts, initial_delay, multiplier
                )
            }
            Self::RetryOnDifferentNode {
                max_attempts,
                exclude_failed_nodes,
                ..
            } => {
                format!(
                    "Retry on different node up to {} times (exclude failed: {})",
                    max_attempts, exclude_failed_nodes
                )
            }
            Self::RestoreCheckpointAndRetry {
                checkpoint_selection,
                ..
            } => {
                format!("Restore checkpoint ({:?}) and retry", checkpoint_selection)
            }
            Self::Failover {
                replica_selector, ..
            } => {
                format!("Failover using selector: {}", replica_selector)
            }
            Self::Recompute { max_attempts } => {
                format!("Recompute from scratch up to {} times", max_attempts)
            }
            Self::SkipAndContinue { mark_as, .. } => {
                format!("Skip and continue (mark as {:?})", mark_as)
            }
            Self::EscalateToHuman { timeout, .. } => {
                format!("Escalate to human (timeout: {:?})", timeout)
            }
            Self::Abort {
                save_partial_results,
                ..
            } => {
                format!("Abort job (save partial: {})", save_partial_results)
            }
            Self::Custom { handler_id, .. } => {
                format!("Custom handler: {}", handler_id)
            }
            Self::Cascade(strategies) => {
                format!("Cascade of {} strategies", strategies.len())
            }
            Self::Conditional { conditions, .. } => {
                format!("Conditional with {} branches", conditions.len())
            }
        }
    }
}

/// Conditions for selecting recovery strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailureCondition {
    /// Match specific failure type
    FailureTypeIs(FailureType),

    /// Match failures at or above severity threshold
    SeverityAtLeast(FailureSeverity),

    /// Match when retry count exceeds threshold
    RetryCountExceeds(u32),

    /// Match when total failures for job/task exceed threshold
    TotalFailuresExceed(u32),

    /// Match when failure rate exceeds threshold in time window
    FailureRateExceeds { rate: f64, window: Duration },

    /// Match during specific hours (0-23)
    TimeOfDay { hours: Vec<u32> },

    /// Custom evaluator (plugin/extension point)
    Custom { evaluator_id: String },
}

impl FailureCondition {
    /// Create a condition for matching a severity level
    pub fn severity_at_least(severity: FailureSeverity) -> Self {
        Self::SeverityAtLeast(severity)
    }

    /// Create a condition for retry count
    pub fn retry_count_exceeds(count: u32) -> Self {
        Self::RetryCountExceeds(count)
    }

    /// Create a condition for failure rate
    pub fn failure_rate_exceeds(rate: f64, window_secs: i64) -> Self {
        Self::FailureRateExceeds {
            rate,
            window: Duration::seconds(window_secs),
        }
    }
}

/// Calculate the delay for a retry attempt with exponential backoff
pub fn calculate_backoff_delay(
    attempt: u32,
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
    jitter: f64,
) -> Duration {
    // Calculate base delay with exponential backoff
    let base_delay_ms =
        initial_delay.num_milliseconds() as f64 * multiplier.powi(attempt.saturating_sub(1) as i32);

    // Cap at max delay
    let capped_delay_ms = base_delay_ms.min(max_delay.num_milliseconds() as f64);

    // Apply jitter (random factor between 1-jitter and 1+jitter)
    let jitter_factor = if jitter > 0.0 {
        let random = rand::random::<f64>();
        1.0 - jitter + (2.0 * jitter * random)
    } else {
        1.0
    };

    let final_delay_ms = (capped_delay_ms * jitter_factor) as i64;
    Duration::milliseconds(final_delay_ms.max(0))
}

/// Outcome of a recovery attempt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecoveryOutcome {
    /// Failure was fully recovered
    Recovered,
    /// Failure was partially recovered with some degradation
    PartiallyRecovered { details: String },
    /// Recovery failed
    Failed { error: String },
    /// Failure was escalated to another party
    Escalated { to: String },
    /// Job/task was aborted
    Aborted,
}

impl RecoveryOutcome {
    /// Check if recovery was successful (full or partial)
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Recovered | Self::PartiallyRecovered { .. })
    }

    /// Check if recovery definitively failed
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed { .. } | Self::Aborted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_immediate_retry() {
        let strategy = RecoveryStrategy::immediate_retry(3, 5);
        match strategy {
            RecoveryStrategy::RetryImmediate {
                max_attempts,
                delay,
            } => {
                assert_eq!(max_attempts, 3);
                assert_eq!(delay, Duration::seconds(5));
            }
            _ => panic!("Wrong strategy type"),
        }
    }

    #[test]
    fn test_exponential_backoff() {
        let strategy = RecoveryStrategy::exponential_backoff(5);
        match strategy {
            RecoveryStrategy::RetryWithBackoff {
                max_attempts,
                multiplier,
                jitter,
                ..
            } => {
                assert_eq!(max_attempts, 5);
                assert_eq!(multiplier, 2.0);
                assert!(jitter > 0.0);
            }
            _ => panic!("Wrong strategy type"),
        }
    }

    #[test]
    fn test_calculate_backoff_delay_no_jitter() {
        let initial = Duration::seconds(1);
        let max = Duration::seconds(60);

        // First attempt: 1 second
        let delay1 = calculate_backoff_delay(1, initial, max, 2.0, 0.0);
        assert_eq!(delay1, Duration::seconds(1));

        // Second attempt: 2 seconds
        let delay2 = calculate_backoff_delay(2, initial, max, 2.0, 0.0);
        assert_eq!(delay2, Duration::seconds(2));

        // Third attempt: 4 seconds
        let delay3 = calculate_backoff_delay(3, initial, max, 2.0, 0.0);
        assert_eq!(delay3, Duration::seconds(4));

        // Fourth attempt: 8 seconds
        let delay4 = calculate_backoff_delay(4, initial, max, 2.0, 0.0);
        assert_eq!(delay4, Duration::seconds(8));
    }

    #[test]
    fn test_calculate_backoff_delay_with_cap() {
        let initial = Duration::seconds(10);
        let max = Duration::seconds(30);

        // First attempt: 10 seconds
        let delay1 = calculate_backoff_delay(1, initial, max, 2.0, 0.0);
        assert_eq!(delay1, Duration::seconds(10));

        // Second attempt: 20 seconds
        let delay2 = calculate_backoff_delay(2, initial, max, 2.0, 0.0);
        assert_eq!(delay2, Duration::seconds(20));

        // Third attempt: would be 40, capped at 30
        let delay3 = calculate_backoff_delay(3, initial, max, 2.0, 0.0);
        assert_eq!(delay3, Duration::seconds(30));
    }

    #[test]
    fn test_calculate_backoff_delay_with_jitter() {
        let initial = Duration::seconds(10);
        let max = Duration::seconds(60);

        // With 10% jitter, delay should be between 9 and 11 seconds
        let delay = calculate_backoff_delay(1, initial, max, 2.0, 0.1);
        assert!(delay >= Duration::milliseconds(9000));
        assert!(delay <= Duration::milliseconds(11000));
    }

    #[test]
    fn test_cascade_strategy() {
        let cascade = RecoveryStrategy::cascade(vec![
            RecoveryStrategy::immediate_retry(3, 1),
            RecoveryStrategy::retry_different_node(2),
            RecoveryStrategy::abort(true),
        ]);

        match cascade {
            RecoveryStrategy::Cascade(strategies) => {
                assert_eq!(strategies.len(), 3);
            }
            _ => panic!("Wrong strategy type"),
        }
    }

    #[test]
    fn test_recovery_outcome_is_success() {
        assert!(RecoveryOutcome::Recovered.is_success());
        assert!(RecoveryOutcome::PartiallyRecovered {
            details: "test".to_string()
        }
        .is_success());
        assert!(!RecoveryOutcome::Failed {
            error: "test".to_string()
        }
        .is_success());
        assert!(!RecoveryOutcome::Aborted.is_success());
        assert!(!RecoveryOutcome::Escalated {
            to: "human".to_string()
        }
        .is_success());
    }

    #[test]
    fn test_recovery_outcome_is_failure() {
        assert!(!RecoveryOutcome::Recovered.is_failure());
        assert!(!RecoveryOutcome::PartiallyRecovered {
            details: "test".to_string()
        }
        .is_failure());
        assert!(RecoveryOutcome::Failed {
            error: "test".to_string()
        }
        .is_failure());
        assert!(RecoveryOutcome::Aborted.is_failure());
    }

    #[test]
    fn test_strategy_description() {
        let strategy = RecoveryStrategy::exponential_backoff(5);
        let desc = strategy.description();
        assert!(desc.contains("backoff"));
        assert!(desc.contains("5"));
    }
}
