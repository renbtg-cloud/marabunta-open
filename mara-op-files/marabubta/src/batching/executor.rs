// Marabunta - Licensed under the MIT License.
//! Batch executor trait and implementations
//!
//! This module defines the `BatchExecutor` trait that must be implemented
//! to provide batch processing logic, along with integration executors for
//! common Marabunta operations.

use crate::batching::types::{
    BatchError, TaskResultResponse, TaskStatus, TaskStatusResponse, TaskSubmissionRequest,
    TaskSubmissionResponse,
};
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

/// Trait for implementing batch operations
///
/// Implement this trait to define how a batch of requests should be processed.
/// The executor receives all requests in a batch and must return results in
/// the same order.
///
/// # Type Parameters
///
/// - `K`: The request/key type
/// - `V`: The response/value type
///
/// # Example
///
/// ```rust
/// use async_trait::async_trait;
/// use marabunta_compute::batching::{BatchExecutor, BatchError};
///
/// struct DatabaseBatchExecutor {
///     pool: /* database pool */
/// }
///
/// #[async_trait]
/// impl BatchExecutor<String, Vec<u8>> for DatabaseBatchExecutor {
///     async fn execute_batch(
///         &self,
///         keys: Vec<String>,
///     ) -> Vec<Result<Vec<u8>, BatchError>> {
///         // Execute batch query
///         // let results = self.pool.query("SELECT * FROM data WHERE key IN (...)", &keys).await;
///
///         // Return results in same order as keys
///         keys.into_iter()
///             .map(|k| Ok(k.into_bytes()))
///             .collect()
///     }
/// }
/// ```
#[async_trait]
pub trait BatchExecutor<K, V>: Send + Sync + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Execute a batch of requests
    ///
    /// # Arguments
    ///
    /// * `requests` - Vector of requests to process
    ///
    /// # Returns
    ///
    /// Vector of results in the same order as the input requests.
    /// Each request gets its own result, allowing partial failures.
    ///
    /// # Important
    ///
    /// The returned vector MUST have the same length as the input vector,
    /// with results in corresponding positions.
    async fn execute_batch(&self, requests: Vec<K>) -> Vec<Result<V, BatchError>>;

    /// Called before executing a batch (optional hook)
    ///
    /// Can be used for logging, metrics, or preparation.
    async fn before_batch(&self, _batch_size: usize) {}

    /// Called after executing a batch (optional hook)
    ///
    /// Can be used for logging, metrics, or cleanup.
    async fn after_batch(&self, _batch_size: usize, _success_count: usize) {}
}

/// Type-erased batch executor for dynamic dispatch
pub type BoxedBatchExecutor<K, V> = Arc<dyn BatchExecutor<K, V>>;

// =============================================================================
// Integration Executors for Marabunta Operations
// =============================================================================

/// Executor for batching task submissions
#[async_trait]
pub trait TaskSubmissionExecutorTrait: Send + Sync + 'static {
    /// Submit a batch of tasks
    async fn submit_tasks(
        &self,
        requests: Vec<TaskSubmissionRequest>,
    ) -> Vec<Result<TaskSubmissionResponse, BatchError>>;
}

/// Executor for batching status queries
#[async_trait]
pub trait StatusQueryExecutorTrait: Send + Sync + 'static {
    /// Query status for a batch of task IDs
    async fn query_status(
        &self,
        task_ids: Vec<Uuid>,
    ) -> Vec<Result<TaskStatusResponse, BatchError>>;
}

/// Executor for batching result retrieval
#[async_trait]
pub trait ResultRetrievalExecutorTrait: Send + Sync + 'static {
    /// Retrieve results for a batch of task IDs
    async fn retrieve_results(
        &self,
        task_ids: Vec<Uuid>,
    ) -> Vec<Result<TaskResultResponse, BatchError>>;
}

// =============================================================================
// Type aliases for integration batchers
// =============================================================================

/// Batcher type for task submission
pub type TaskSubmissionBatcher =
    crate::batching::Batcher<TaskSubmissionRequest, TaskSubmissionResponse>;

/// Batcher type for status queries
pub type StatusQueryBatcher = crate::batching::Batcher<Uuid, TaskStatusResponse>;

/// Batcher type for result retrieval
pub type ResultBatcher = crate::batching::Batcher<Uuid, TaskResultResponse>;

// =============================================================================
// Wrapper implementations to adapt trait implementations to BatchExecutor
// =============================================================================

/// Wrapper to adapt TaskSubmissionExecutorTrait to BatchExecutor
pub struct TaskSubmissionExecutor<T: TaskSubmissionExecutorTrait> {
    inner: T,
}

impl<T: TaskSubmissionExecutorTrait> TaskSubmissionExecutor<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T: TaskSubmissionExecutorTrait> BatchExecutor<TaskSubmissionRequest, TaskSubmissionResponse>
    for TaskSubmissionExecutor<T>
{
    async fn execute_batch(
        &self,
        requests: Vec<TaskSubmissionRequest>,
    ) -> Vec<Result<TaskSubmissionResponse, BatchError>> {
        self.inner.submit_tasks(requests).await
    }
}

/// Wrapper to adapt StatusQueryExecutorTrait to BatchExecutor
pub struct StatusQueryExecutor<T: StatusQueryExecutorTrait> {
    inner: T,
}

impl<T: StatusQueryExecutorTrait> StatusQueryExecutor<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T: StatusQueryExecutorTrait> BatchExecutor<Uuid, TaskStatusResponse>
    for StatusQueryExecutor<T>
{
    async fn execute_batch(
        &self,
        requests: Vec<Uuid>,
    ) -> Vec<Result<TaskStatusResponse, BatchError>> {
        self.inner.query_status(requests).await
    }
}

/// Wrapper to adapt ResultRetrievalExecutorTrait to BatchExecutor
pub struct ResultRetrievalExecutor<T: ResultRetrievalExecutorTrait> {
    inner: T,
}

impl<T: ResultRetrievalExecutorTrait> ResultRetrievalExecutor<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T: ResultRetrievalExecutorTrait> BatchExecutor<Uuid, TaskResultResponse>
    for ResultRetrievalExecutor<T>
{
    async fn execute_batch(
        &self,
        requests: Vec<Uuid>,
    ) -> Vec<Result<TaskResultResponse, BatchError>> {
        self.inner.retrieve_results(requests).await
    }
}

// =============================================================================
// Mock Implementations for Testing
// =============================================================================

/// Mock executor for task submission testing
pub struct MockTaskSubmissionExecutor;

impl MockTaskSubmissionExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockTaskSubmissionExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BatchExecutor<TaskSubmissionRequest, TaskSubmissionResponse> for MockTaskSubmissionExecutor {
    async fn execute_batch(
        &self,
        requests: Vec<TaskSubmissionRequest>,
    ) -> Vec<Result<TaskSubmissionResponse, BatchError>> {
        requests
            .into_iter()
            .enumerate()
            .map(|(i, _req)| {
                Ok(TaskSubmissionResponse {
                    task_id: Uuid::new_v4(),
                    queue_position: i,
                })
            })
            .collect()
    }
}

/// Mock executor for status query testing
pub struct MockStatusQueryExecutor;

impl MockStatusQueryExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockStatusQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BatchExecutor<Uuid, TaskStatusResponse> for MockStatusQueryExecutor {
    async fn execute_batch(
        &self,
        requests: Vec<Uuid>,
    ) -> Vec<Result<TaskStatusResponse, BatchError>> {
        requests
            .into_iter()
            .map(|task_id| {
                Ok(TaskStatusResponse {
                    task_id,
                    status: TaskStatus::Running,
                    progress: Some(50),
                    worker_id: Some("worker-1".to_string()),
                })
            })
            .collect()
    }
}

/// Mock executor for result retrieval testing
pub struct MockResultRetrievalExecutor;

impl MockResultRetrievalExecutor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockResultRetrievalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BatchExecutor<Uuid, TaskResultResponse> for MockResultRetrievalExecutor {
    async fn execute_batch(
        &self,
        requests: Vec<Uuid>,
    ) -> Vec<Result<TaskResultResponse, BatchError>> {
        requests
            .into_iter()
            .map(|task_id| {
                Ok(TaskResultResponse {
                    task_id,
                    data: Some(b"mock result data".to_vec()),
                    error: None,
                })
            })
            .collect()
    }
}

// =============================================================================
// Utility Executors
// =============================================================================

/// An executor that maps requests through a function
pub struct MapExecutor<K, V, F>
where
    F: Fn(K) -> Result<V, BatchError> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    map_fn: F,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V, F> MapExecutor<K, V, F>
where
    F: Fn(K) -> Result<V, BatchError> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(map_fn: F) -> Self {
        Self {
            map_fn,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<K, V, F> BatchExecutor<K, V> for MapExecutor<K, V, F>
where
    F: Fn(K) -> Result<V, BatchError> + Send + Sync + 'static,
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    async fn execute_batch(&self, requests: Vec<K>) -> Vec<Result<V, BatchError>> {
        requests.into_iter().map(&self.map_fn).collect()
    }
}

/// An executor that chains two executors together
pub struct ChainExecutor<K, M, V, E1, E2>
where
    E1: BatchExecutor<K, M>,
    E2: BatchExecutor<M, V>,
    K: Send + Sync + 'static,
    M: Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    first: E1,
    second: E2,
    _phantom: std::marker::PhantomData<(K, M, V)>,
}

impl<K, M, V, E1, E2> ChainExecutor<K, M, V, E1, E2>
where
    E1: BatchExecutor<K, M>,
    E2: BatchExecutor<M, V>,
    K: Send + Sync + 'static,
    M: Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(first: E1, second: E2) -> Self {
        Self {
            first,
            second,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<K, M, V, E1, E2> BatchExecutor<K, V> for ChainExecutor<K, M, V, E1, E2>
where
    E1: BatchExecutor<K, M>,
    E2: BatchExecutor<M, V>,
    K: Send + Sync + 'static,
    M: Send + Sync + Clone + 'static,
    V: Send + Sync + 'static,
{
    async fn execute_batch(&self, requests: Vec<K>) -> Vec<Result<V, BatchError>> {
        let first_results = self.first.execute_batch(requests).await;

        // Collect successful intermediate results for second batch
        let (successes, errors): (Vec<_>, Vec<_>) = first_results
            .into_iter()
            .enumerate()
            .partition(|(_, r)| r.is_ok());

        if successes.is_empty() {
            // All failed in first stage
            return errors.into_iter().map(|(_, r)| r.map(|_| unreachable!())).collect();
        }

        let intermediate: Vec<M> = successes
            .iter()
            .filter_map(|(_, r)| r.as_ref().ok().cloned())
            .collect();

        let second_results = self.second.execute_batch(intermediate).await;

        // Merge results back in order
        let total = successes.len() + errors.len();
        let mut final_results: Vec<Result<V, BatchError>> = (0..total)
            .map(|_| Err(BatchError::Internal("placeholder".to_string())))
            .collect();

        for (orig_idx, result) in errors {
            final_results[orig_idx] = result.map(|_| unreachable!());
        }

        let mut second_iter = second_results.into_iter();
        for (orig_idx, _) in successes {
            if let Some(result) = second_iter.next() {
                final_results[orig_idx] = result;
            }
        }

        final_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_map_executor() {
        let executor = MapExecutor::new(|s: String| Ok(s.len()));

        let results = executor
            .execute_batch(vec!["hello".to_string(), "world".to_string()])
            .await;

        assert_eq!(results[0].as_ref().unwrap(), &5);
        assert_eq!(results[1].as_ref().unwrap(), &5);
    }

    #[tokio::test]
    async fn test_mock_task_submission() {
        let executor = MockTaskSubmissionExecutor::new();

        let requests = vec![
            TaskSubmissionRequest {
                job_id: Uuid::new_v4(),
                payload: vec![1, 2, 3],
                priority: 5,
            },
            TaskSubmissionRequest {
                job_id: Uuid::new_v4(),
                payload: vec![4, 5, 6],
                priority: 10,
            },
        ];

        let results = executor.execute_batch(requests).await;
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
    }

    #[tokio::test]
    async fn test_mock_status_query() {
        let executor = MockStatusQueryExecutor::new();

        let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let results = executor.execute_batch(task_ids.clone()).await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().task_id, task_ids[0]);
        assert_eq!(results[1].as_ref().unwrap().task_id, task_ids[1]);
    }

    #[tokio::test]
    async fn test_mock_result_retrieval() {
        let executor = MockResultRetrievalExecutor::new();

        let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
        let results = executor.execute_batch(task_ids.clone()).await;

        assert_eq!(results.len(), 2);
        assert!(results[0].as_ref().unwrap().data.is_some());
    }
}
