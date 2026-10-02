// Marabunta - Licensed under the MIT License.
//! Error types for the failure handling system

use thiserror::Error;

/// Errors that can occur during failure handling
#[derive(Error, Debug)]
pub enum FailureHandlerError {
    /// No policy found that matches the failure
    #[error("No matching policy found for failure: {0}")]
    NoPolicyFound(String),

    /// Circuit breaker is open, blocking operations
    #[error("Circuit breaker is open for scope: {0}")]
    CircuitBreakerOpen(String),

    /// Recovery strategy execution failed
    #[error("Recovery strategy failed: {0}")]
    RecoveryFailed(String),

    /// Checkpoint restoration failed
    #[error("Checkpoint restoration failed: {0}")]
    CheckpointRestoreFailed(String),

    /// No checkpoint available for restoration
    #[error("No checkpoint available for task: {0}")]
    NoCheckpointAvailable(String),

    /// No suitable nodes available for retry
    #[error("No suitable nodes available: {0}")]
    NoSuitableNodes(String),

    /// Maximum retry attempts exceeded
    #[error("Maximum retry attempts exceeded: {attempts} attempts for {task_id}")]
    MaxRetriesExceeded { task_id: String, attempts: u32 },

    /// Notification delivery failed
    #[error("Notification delivery failed to {channel}: {error}")]
    NotificationFailed { channel: String, error: String },

    /// Custom handler not found
    #[error("Custom handler not found: {0}")]
    CustomHandlerNotFound(String),

    /// Custom evaluator not found
    #[error("Custom evaluator not found: {0}")]
    CustomEvaluatorNotFound(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Lock acquisition failed
    #[error("Failed to acquire lock: {0}")]
    LockFailed(String),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Callback execution failed
    #[error("Callback execution failed: {0}")]
    CallbackFailed(String),

    /// Timeout during recovery
    #[error("Recovery timeout after {0:?}")]
    Timeout(std::time::Duration),
}

/// Result type for failure handling operations
pub type FailureResult<T> = Result<T, FailureHandlerError>;

impl FailureHandlerError {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::LockFailed(_) | Self::NotificationFailed { .. } | Self::Timeout(_)
        )
    }

    /// Check if this error indicates a permanent failure
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::MaxRetriesExceeded { .. }
                | Self::NoCheckpointAvailable(_)
                | Self::NoSuitableNodes(_)
                | Self::CustomHandlerNotFound(_)
                | Self::CustomEvaluatorNotFound(_)
                | Self::InvalidConfiguration(_)
        )
    }
}
