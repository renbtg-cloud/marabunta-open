// Marabunta - Licensed under the MIT License.
//! Failure Handling System for Marabunta Compute
//!
//! This module provides a comprehensive failure handling system for the marabunta-compute
//! job placement system. It includes:
//!
//! - **Types**: Failure types, contexts, and severities (`types.rs`)
//! - **Recovery Strategies**: Various strategies for recovering from failures (`recovery.rs`)
//! - **Policies**: Rules for matching failures to recovery strategies (`policy.rs`)
//! - **Handler**: The core failure handler that orchestrates recovery (`handler.rs`)
//! - **Degradation**: Graceful degradation management under stress (`degradation.rs`)
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use marabunta_compute::failure::{
//!     FailureHandler, FailureCallbacks, Failure, FailureType, FailureContext,
//!     RecoveryPolicy, FailureMatcher, RecoveryStrategy, FailureSeverity,
//! };
//!
//! // Create callbacks for your system
//! let callbacks = FailureCallbacks::default();
//!
//! // Create the failure handler
//! let mut handler = FailureHandler::new(callbacks);
//!
//! // Add a recovery policy
//! handler.add_policy(RecoveryPolicy::new(
//!     "task-crash-policy",
//!     "Handle Task Crashes",
//!     FailureMatcher::FailureType(vec![FailureType::TaskCrash {
//!         exit_code: None,
//!         stderr: None,
//!     }]),
//!     RecoveryStrategy::exponential_backoff(3),
//! ));
//!
//! // Handle a failure
//! // let failure = Failure::new(
//! //     FailureType::TaskCrash { exit_code: Some(1), stderr: None },
//! //     FailureContext::for_task("job-123", "task-456"),
//! // );
//! // let result = handler.handle_failure(failure).await;
//! ```
//!
//! # Graceful Degradation
//!
//! ```rust,no_run
//! use marabunta_compute::failure::{
//!     DegradationManager, DegradationLevel, SystemState,
//! };
//!
//! // Create a degradation manager
//! let mut manager = DegradationManager::with_defaults();
//!
//! // Evaluate system state
//! let state = SystemState::new()
//!     .with_failure_rate(15.0)
//!     .with_node_availability(70.0);
//!
//! let transition = manager.evaluate(&state);
//!
//! if transition.is_escalation() {
//!     println!("System degrading from {:?} to {:?}", transition.from, transition.to);
//! }
//! ```

pub mod degradation;
pub mod errors;
pub mod handler;
pub mod policy;
pub mod recovery;
pub mod types;

// Re-export main types for convenience
pub use degradation::{
    DegradationAction, DegradationCondition, DegradationLevel, DegradationManager,
    DegradationThresholds, DegradationTransition, SystemState,
};
pub use errors::{FailureHandlerError, FailureResult};
pub use handler::{
    CheckpointInfo, CircuitBreakerState, FailureCallbacks, FailureHandler, FailureRateScope,
    FailureRecord, FailureStatistics, RecoveryAttempt, RecoveryResult, RetryState,
};
pub use policy::{
    CircuitBreakerConfig, CircuitBreakerScope, FailureMatcher, LoggingPolicy, NotificationChannel,
    NotificationPolicy, RecoveryPolicy,
};
pub use recovery::{
    calculate_backoff_delay, CheckpointSelection, FailureCondition, RecoveryOutcome,
    RecoveryStrategy, TaskStatus,
};
pub use types::{
    Failure, FailureContext, FailureSeverity, FailureType, JobId, NodeId, ResourceType, TaskId,
};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use chrono::Duration;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// Test the full failure handling flow
    #[tokio::test]
    async fn test_full_failure_handling_flow() {
        // Create callbacks that track calls
        let retry_count = Arc::new(AtomicU32::new(0));
        let retry_count_clone = Arc::clone(&retry_count);

        let callbacks = FailureCallbacks {
            retry_task: Box::new(move |_task_id, _node_id| {
                let count = retry_count_clone.fetch_add(1, Ordering::SeqCst);
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
            get_checkpoints: Box::new(|_task_id| Vec::new()),
            mark_node_failed: Box::new(|_node_id, _reason| Ok(())),
        };

        let mut handler = FailureHandler::new(callbacks);

        // Add default policies
        for policy in policy::defaults::all() {
            handler.add_policy(policy);
        }

        // Create a task crash failure
        let failure = Failure::new(
            FailureType::TaskCrash {
                exit_code: Some(1),
                stderr: Some("Segmentation fault".to_string()),
            },
            FailureContext::for_task("job-123", "task-456").with_node("node-1"),
        );

        // Handle the failure
        let result = handler.handle_failure(failure).await;

        // Should have recovered after retries
        assert!(result.outcome.is_success());
        assert!(retry_count.load(Ordering::SeqCst) >= 3);
    }

    /// Test cascading recovery strategies
    #[tokio::test]
    async fn test_cascading_strategies() {
        let callbacks = FailureCallbacks {
            retry_task: Box::new(|_task_id, _node_id| Err("Always fails".to_string())),
            abort_job: Box::new(|_job_id, _save_partial| Ok(())),
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

        // Create a cascade that ends in abort
        let strategy = RecoveryStrategy::Cascade(vec![
            RecoveryStrategy::RetryImmediate {
                max_attempts: 2,
                delay: Duration::milliseconds(1),
            },
            RecoveryStrategy::Abort {
                save_partial_results: true,
                notify: false,
            },
        ]);

        let result = handler.execute_recovery(&failure, &strategy).await;

        // Should have aborted after retries failed
        assert!(matches!(result.outcome, RecoveryOutcome::Aborted));
        assert!(result.attempts.len() >= 2);
    }

    /// Test degradation manager integration
    #[test]
    fn test_degradation_flow() {
        let mut manager = DegradationManager::with_defaults();

        // Start at normal
        assert_eq!(manager.current_level(), DegradationLevel::Normal);

        // Simulate escalating issues
        let states = vec![
            SystemState::new()
                .with_failure_rate(6.0)
                .with_node_availability(85.0)
                .with_healthy_coordinators(5),
            SystemState::new()
                .with_failure_rate(12.0)
                .with_node_availability(65.0)
                .with_healthy_coordinators(3),
            SystemState::new()
                .with_failure_rate(25.0)
                .with_node_availability(40.0)
                .with_healthy_coordinators(2),
            SystemState::new()
                .with_failure_rate(30.0)
                .with_node_availability(20.0)
                .with_healthy_coordinators(1),
        ];

        let expected_levels = vec![
            DegradationLevel::Elevated,
            DegradationLevel::Degraded,
            DegradationLevel::Critical,
            DegradationLevel::Emergency,
        ];

        for (state, expected) in states.iter().zip(expected_levels.iter()) {
            let _transition = manager.evaluate(state);
            assert!(
                manager.current_level() >= *expected,
                "Expected {:?} or higher, got {:?}",
                expected,
                manager.current_level()
            );
        }

        // Recovery
        let state = SystemState::new()
            .with_failure_rate(1.0)
            .with_node_availability(99.0)
            .with_healthy_coordinators(5);

        let transition = manager.evaluate(&state);
        assert!(transition.is_deescalation());
        assert_eq!(manager.current_level(), DegradationLevel::Normal);
    }

    /// Test circuit breaker behavior
    #[tokio::test]
    async fn test_circuit_breaker_integration() {
        let handler = FailureHandler::with_defaults();

        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::seconds(1),
            scope: CircuitBreakerScope::PerNode,
        };

        handler
            .get_or_create_circuit_breaker("node-1", config)
            .await;

        // Should be closed initially
        assert!(handler.circuit_allows("node-1").await);

        // Record failures to open
        handler.record_failure("node-1").await;
        handler.record_failure("node-1").await;
        handler.record_failure("node-1").await;

        // Should be open now
        assert!(!handler.circuit_allows("node-1").await);

        // Wait for timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Should transition to half-open
        assert!(handler.circuit_allows("node-1").await);

        // Record success to close
        handler.record_success("node-1").await;
        handler.record_success("node-1").await;

        // Should be closed again
        let states = handler.circuit_breaker_states().await;
        assert_eq!(states.get("node-1"), Some(&CircuitBreakerState::Closed));
    }

    /// Test failure statistics
    #[tokio::test]
    async fn test_failure_statistics() {
        let handler = FailureHandler::with_defaults();

        // Generate some failures
        for i in 0..10 {
            let failure = Failure::new(
                if i % 2 == 0 {
                    FailureType::TaskCrash {
                        exit_code: Some(i),
                        stderr: None,
                    }
                } else {
                    FailureType::TaskTimeout {
                        elapsed: Duration::seconds(120),
                        limit: Duration::seconds(60),
                    }
                },
                FailureContext::for_task(format!("job-{}", i % 3), format!("task-{}", i))
                    .with_node(format!("node-{}", i % 5)),
            );
            let _ = handler.handle_failure(failure).await;
        }

        let stats = handler.statistics(Duration::hours(1)).await;

        assert_eq!(stats.total_failures, 10);
        assert!(stats.failures_by_type.contains_key("task_crash"));
        assert!(stats.failures_by_type.contains_key("task_timeout"));
        assert!(!stats.top_failing_nodes.is_empty());
        assert!(!stats.top_failing_jobs.is_empty());
    }
}
