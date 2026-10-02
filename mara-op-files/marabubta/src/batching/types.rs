// Marabunta - Licensed under the MIT License.
//! Core types for request batching
//!
//! This module defines the fundamental types used throughout the batching system.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

/// Unique identifier for a request within the batching system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(pub Uuid);

impl RequestId {
    /// Create a new random request ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Configuration for the batcher
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of requests in a single batch
    pub batch_size: usize,

    /// Maximum time to wait for a batch to fill before executing
    pub timeout: Duration,

    /// Maximum number of batches that can execute concurrently
    pub max_concurrent_batches: usize,

    /// Whether to enable request coalescing for identical requests
    pub coalescing_enabled: bool,

    /// Maximum time to wait for a batch execution to complete
    pub execution_timeout: Duration,

    /// Whether to retry failed batches
    pub retry_on_failure: bool,

    /// Maximum number of retry attempts for failed batches
    pub max_retries: usize,

    /// Base delay between retry attempts (exponential backoff)
    pub retry_base_delay: Duration,

    /// Maximum delay between retry attempts
    pub retry_max_delay: Duration,
}

impl BatchConfig {
    /// Create a new batch configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the batch timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum number of concurrent batches
    pub fn with_max_concurrent_batches(mut self, max: usize) -> Self {
        self.max_concurrent_batches = max;
        self
    }

    /// Enable or disable request coalescing
    pub fn with_coalescing(mut self, enabled: bool) -> Self {
        self.coalescing_enabled = enabled;
        self
    }

    /// Set the execution timeout for batches
    pub fn with_execution_timeout(mut self, timeout: Duration) -> Self {
        self.execution_timeout = timeout;
        self
    }

    /// Enable retry on failure with default settings
    pub fn with_retry(mut self, enabled: bool) -> Self {
        self.retry_on_failure = enabled;
        self
    }

    /// Set the maximum number of retry attempts
    pub fn with_max_retries(mut self, max: usize) -> Self {
        self.max_retries = max;
        self
    }

    /// Set retry delay configuration
    pub fn with_retry_delays(mut self, base: Duration, max: Duration) -> Self {
        self.retry_base_delay = base;
        self.retry_max_delay = max;
        self
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            timeout: Duration::from_millis(50),
            max_concurrent_batches: 4,
            coalescing_enabled: false,
            execution_timeout: Duration::from_secs(30),
            retry_on_failure: false,
            max_retries: 3,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

/// Errors that can occur during batch operations
#[derive(Error, Debug, Clone)]
pub enum BatchError {
    /// The batch execution failed
    #[error("Batch execution failed: {0}")]
    ExecutionFailed(String),

    /// The batch execution timed out
    #[error("Batch execution timed out after {0:?}")]
    ExecutionTimeout(Duration),

    /// The batcher has been shut down
    #[error("Batcher has been shut down")]
    ShutDown,

    /// The request was cancelled
    #[error("Request was cancelled")]
    Cancelled,

    /// Failed to send request to batcher
    #[error("Failed to send request: {0}")]
    SendFailed(String),

    /// Failed to receive result from batcher
    #[error("Failed to receive result: {0}")]
    ReceiveFailed(String),

    /// All retry attempts failed
    #[error("All {0} retry attempts failed")]
    RetriesExhausted(usize),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for batch operations
pub type BatchResult<T> = Result<T, BatchError>;

/// A pending request waiting to be batched
#[derive(Debug)]
pub struct PendingRequest<K, V> {
    /// Unique identifier for this request
    pub id: RequestId,

    /// The request key/payload
    pub request: K,

    /// Channel to send the result back to the caller
    pub response_tx: tokio::sync::oneshot::Sender<BatchResult<V>>,

    /// Timestamp when the request was submitted
    pub submitted_at: std::time::Instant,
}

impl<K, V> PendingRequest<K, V> {
    /// Create a new pending request
    pub fn new(request: K, response_tx: tokio::sync::oneshot::Sender<BatchResult<V>>) -> Self {
        Self {
            id: RequestId::new(),
            request,
            response_tx,
            submitted_at: std::time::Instant::now(),
        }
    }

    /// Complete this request with a result
    pub fn complete(self, result: BatchResult<V>) {
        // Ignore send errors - the receiver may have been dropped
        let _ = self.response_tx.send(result);
    }

    /// Get the age of this request
    pub fn age(&self) -> Duration {
        self.submitted_at.elapsed()
    }
}

/// A batch of requests ready for execution
#[derive(Debug)]
pub struct BatchRequest<K> {
    /// The requests in this batch
    pub requests: Vec<K>,

    /// Request IDs for correlation
    pub request_ids: Vec<RequestId>,

    /// Timestamp when the batch was created
    pub created_at: std::time::Instant,
}

impl<K> BatchRequest<K> {
    /// Create a new batch from pending requests
    pub fn from_pending<V>(pending: Vec<PendingRequest<K, V>>) -> (Self, Vec<tokio::sync::oneshot::Sender<BatchResult<V>>>) {
        let mut requests = Vec::with_capacity(pending.len());
        let mut request_ids = Vec::with_capacity(pending.len());
        let mut senders = Vec::with_capacity(pending.len());

        for p in pending {
            requests.push(p.request);
            request_ids.push(p.id);
            senders.push(p.response_tx);
        }

        let batch = Self {
            requests,
            request_ids,
            created_at: std::time::Instant::now(),
        };

        (batch, senders)
    }

    /// Get the size of this batch
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    /// Check if this batch is empty
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }
}

// =============================================================================
// Integration Types for Marabunta Operations
// =============================================================================

/// Request for submitting a task
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskSubmissionRequest {
    /// Job ID this task belongs to
    pub job_id: Uuid,

    /// Task payload data
    pub payload: Vec<u8>,

    /// Task priority (higher = more important)
    pub priority: u8,
}

/// Response from task submission
#[derive(Debug, Clone)]
pub struct TaskSubmissionResponse {
    /// Assigned task ID
    pub task_id: Uuid,

    /// Estimated queue position
    pub queue_position: usize,
}

/// Response from status query
#[derive(Debug, Clone)]
pub struct TaskStatusResponse {
    /// Task ID
    pub task_id: Uuid,

    /// Current status
    pub status: TaskStatus,

    /// Progress percentage (0-100)
    pub progress: Option<u8>,

    /// Worker ID if assigned
    pub worker_id: Option<String>,
}

/// Task status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task is queued and waiting
    Pending,
    /// Task is currently running
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed
    Failed,
    /// Task was cancelled
    Cancelled,
}

/// Response from result retrieval
#[derive(Debug, Clone)]
pub struct TaskResultResponse {
    /// Task ID
    pub task_id: Uuid,

    /// Result data (if completed)
    pub data: Option<Vec<u8>>,

    /// Error message (if failed)
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_generation() {
        let id1 = RequestId::new();
        let id2 = RequestId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_batch_config_builder() {
        let config = BatchConfig::new()
            .with_batch_size(50)
            .with_timeout(Duration::from_millis(100))
            .with_max_concurrent_batches(8)
            .with_coalescing(true);

        assert_eq!(config.batch_size, 50);
        assert_eq!(config.timeout, Duration::from_millis(100));
        assert_eq!(config.max_concurrent_batches, 8);
        assert!(config.coalescing_enabled);
    }

    #[test]
    fn test_batch_config_defaults() {
        let config = BatchConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.timeout, Duration::from_millis(50));
        assert_eq!(config.max_concurrent_batches, 4);
        assert!(!config.coalescing_enabled);
    }

    #[test]
    fn test_pending_request_age() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let pending: PendingRequest<String, String> = PendingRequest::new("test".to_string(), tx);

        std::thread::sleep(Duration::from_millis(10));

        assert!(pending.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_batch_request_from_pending() {
        let pending: Vec<PendingRequest<String, String>> = (0..5)
            .map(|i| {
                let (tx, _rx) = tokio::sync::oneshot::channel();
                PendingRequest::new(format!("request_{}", i), tx)
            })
            .collect();

        let (batch, senders) = BatchRequest::from_pending(pending);

        assert_eq!(batch.len(), 5);
        assert_eq!(senders.len(), 5);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_batch_error_display() {
        let err = BatchError::ExecutionFailed("test error".to_string());
        assert!(err.to_string().contains("test error"));

        let err = BatchError::ExecutionTimeout(Duration::from_secs(30));
        assert!(err.to_string().contains("30"));
    }
}
