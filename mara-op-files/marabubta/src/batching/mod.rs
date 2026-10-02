// Marabunta - Licensed under the MIT License.
//! Request Batching for Marabunta Compute
//!
//! This module provides efficient request batching functionality for the Marabunta Compute
//! framework. It collects individual requests and executes them in batches for improved
//! throughput and reduced overhead.
//!
//! # Architecture
//!
//! The batching system consists of four main components:
//!
//! - **Batcher** (`batcher.rs`): Core batcher that collects requests and executes batches
//! - **Executor** (`executor.rs`): Trait for implementing batch operations
//! - **Coalescing** (`coalescing.rs`): Automatic request coalescing for identical requests
//! - **Types** (`types.rs`): Core data structures for batch configuration and results
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use marabunta_compute::batching::{Batcher, BatchConfig, BatchExecutor};
//!
//! // Define your batch executor
//! struct MyExecutor;
//!
//! #[async_trait::async_trait]
//! impl BatchExecutor<String, String> for MyExecutor {
//!     async fn execute_batch(&self, requests: Vec<String>) -> Vec<Result<String, String>> {
//!         requests.into_iter()
//!             .map(|r| Ok(format!("processed: {}", r)))
//!             .collect()
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = BatchConfig::default();
//!     let batcher = Batcher::new(config, Arc::new(MyExecutor));
//!
//!     // Submit individual requests - they'll be batched automatically
//!     let result = batcher.submit("request1".to_string()).await;
//! }
//! ```
//!
//! # Features
//!
//! ## Request Batching
//!
//! The batcher collects individual requests and executes them in batches when:
//! - The batch size threshold is reached
//! - The batch timeout expires
//! - The batcher is explicitly flushed
//!
//! ## Request Coalescing
//!
//! Identical requests are automatically coalesced - only one actual request is made,
//! and all waiting callers receive the same result. This is particularly useful for:
//! - Status queries for the same task
//! - Repeated lookups for the same data
//! - Redundant API calls
//!
//! ## Per-Request Futures
//!
//! Each `submit()` call returns a future that resolves when its specific request
//! completes, even though the underlying batch operation processes multiple requests.
//!
//! ## Concurrency Control
//!
//! The batcher limits the number of concurrent batch operations to prevent
//! overwhelming downstream services.
//!
//! # Configuration
//!
//! ```rust
//! use std::time::Duration;
//! use marabunta_compute::batching::BatchConfig;
//!
//! let config = BatchConfig::new()
//!     .with_batch_size(100)
//!     .with_timeout(Duration::from_millis(50))
//!     .with_max_concurrent_batches(4)
//!     .with_coalescing(true);
//! ```
//!
//! # Integration Points
//!
//! The batching module provides integration points for common Marabunta operations:
//!
//! - **Task Submission**: Batch multiple task submissions together
//! - **Status Queries**: Coalesce repeated status checks for the same task
//! - **Result Retrieval**: Batch result fetches for multiple tasks
//!
//! ```rust,no_run
//! use marabunta_compute::batching::{TaskSubmissionBatcher, StatusQueryBatcher, ResultBatcher};
//!
//! // These integration types provide pre-configured batchers for common operations
//! ```
//!
//! # Error Handling
//!
//! Batch operations return per-request results, so individual failures don't
//! affect other requests in the same batch. If the entire batch fails, all
//! requests in that batch receive the error.
//!
//! # Metrics
//!
//! The batcher tracks useful metrics:
//! - Requests submitted
//! - Batches executed
//! - Average batch size
//! - Coalesced request count
//! - Batch execution time

pub mod batcher;
pub mod coalescing;
pub mod executor;
pub mod types;

// Re-export commonly used types
pub use batcher::{Batcher, BatcherHandle, BatcherStats};
pub use coalescing::{CoalescingBatcher, CoalescingKey};
pub use executor::{BatchExecutor, BoxedBatchExecutor};
pub use types::{
    BatchConfig, BatchError, BatchRequest, BatchResult, PendingRequest, RequestId,
};

// Integration types for common Marabunta operations
pub use executor::{
    ResultBatcher, ResultRetrievalExecutor, StatusQueryBatcher, StatusQueryExecutor,
    TaskSubmissionBatcher, TaskSubmissionExecutor,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Barrier;
    use tokio::time::{sleep, timeout};

    /// Simple test executor that echoes requests
    struct EchoExecutor {
        call_count: AtomicUsize,
        batch_sizes: parking_lot::Mutex<Vec<usize>>,
    }

    impl EchoExecutor {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                batch_sizes: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }

        fn batch_sizes(&self) -> Vec<usize> {
            self.batch_sizes.lock().clone()
        }
    }

    #[async_trait::async_trait]
    impl BatchExecutor<String, String> for EchoExecutor {
        async fn execute_batch(
            &self,
            requests: Vec<String>,
        ) -> Vec<Result<String, BatchError>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.batch_sizes.lock().push(requests.len());

            // Simulate some processing time
            sleep(Duration::from_millis(10)).await;

            requests
                .into_iter()
                .map(|r| Ok(format!("echo: {}", r)))
                .collect()
        }
    }

    /// Executor that fails for specific requests
    struct FailingExecutor {
        fail_pattern: String,
    }

    impl FailingExecutor {
        fn new(fail_pattern: &str) -> Self {
            Self {
                fail_pattern: fail_pattern.to_string(),
            }
        }
    }

    #[async_trait::async_trait]
    impl BatchExecutor<String, String> for FailingExecutor {
        async fn execute_batch(
            &self,
            requests: Vec<String>,
        ) -> Vec<Result<String, BatchError>> {
            requests
                .into_iter()
                .map(|r| {
                    if r.contains(&self.fail_pattern) {
                        Err(BatchError::ExecutionFailed(format!(
                            "Request '{}' contains fail pattern",
                            r
                        )))
                    } else {
                        Ok(format!("processed: {}", r))
                    }
                })
                .collect()
        }
    }

    /// Slow executor for timeout testing
    struct SlowExecutor {
        delay: Duration,
    }

    impl SlowExecutor {
        fn new(delay: Duration) -> Self {
            Self { delay }
        }
    }

    #[async_trait::async_trait]
    impl BatchExecutor<String, String> for SlowExecutor {
        async fn execute_batch(
            &self,
            requests: Vec<String>,
        ) -> Vec<Result<String, BatchError>> {
            sleep(self.delay).await;
            requests.into_iter().map(|r| Ok(r)).collect()
        }
    }

    // ==========================================================================
    // Basic Batching Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_single_request() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(10)
            .with_timeout(Duration::from_millis(100));

        let batcher = Batcher::new(config, executor.clone());
        let handle = batcher.start();

        let result = handle.submit("hello".to_string()).await;
        assert_eq!(result.unwrap(), "echo: hello");
        assert_eq!(executor.call_count(), 1);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_batch_by_size() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(5)
            .with_timeout(Duration::from_secs(10)); // Long timeout to ensure batching by size

        let batcher = Batcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit 5 requests concurrently - should trigger batch by size
        let mut futures = Vec::new();
        for i in 0..5 {
            futures.push(handle.submit(format!("request_{}", i)));
        }

        // Wait for all results concurrently so they can batch together
        let results: Vec<_> = futures::future::join_all(futures).await;
        for (i, result) in results.into_iter().enumerate() {
            assert_eq!(result.unwrap(), format!("echo: request_{}", i));
        }

        // Should have been exactly one batch of size 5
        assert_eq!(executor.call_count(), 1);
        assert_eq!(executor.batch_sizes(), vec![5]);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_batch_by_timeout() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100) // Large batch size
            .with_timeout(Duration::from_millis(50)); // Short timeout

        let batcher = Batcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit 3 requests (less than batch size)
        let mut futures = Vec::new();
        for i in 0..3 {
            futures.push(handle.submit(format!("request_{}", i)));
        }

        // Wait for all results - should complete due to timeout
        let results: Vec<_> = futures::future::join_all(futures).await;
        for (i, result) in results.into_iter().enumerate() {
            assert_eq!(result.unwrap(), format!("echo: request_{}", i));
        }

        // Should have been one batch (triggered by timeout)
        assert_eq!(executor.call_count(), 1);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_multiple_batches() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(3)
            .with_timeout(Duration::from_secs(10));

        let batcher = Batcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit 7 requests - should create 2 full batches + 1 partial
        let mut futures = Vec::new();
        for i in 0..7 {
            futures.push(handle.submit(format!("request_{}", i)));
        }

        // Wait a bit for timeout to trigger the partial batch
        sleep(Duration::from_millis(100)).await;

        let results: Vec<_> = futures::future::join_all(futures).await;
        for (i, result) in results.into_iter().enumerate() {
            assert_eq!(result.unwrap(), format!("echo: request_{}", i));
        }

        // Should have 3 batches: [3, 3, 1] or similar
        assert!(executor.call_count() >= 2);

        handle.shutdown().await;
    }

    // ==========================================================================
    // Error Handling Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_partial_failure() {
        let executor = Arc::new(FailingExecutor::new("fail"));
        let config = BatchConfig::new()
            .with_batch_size(5)
            .with_timeout(Duration::from_millis(100));

        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        // Submit mixed requests
        let futures = vec![
            handle.submit("good_request".to_string()),
            handle.submit("fail_request".to_string()),
            handle.submit("another_good".to_string()),
        ];

        let results: Vec<_> = futures::future::join_all(futures).await;

        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert!(results[2].is_ok());

        handle.shutdown().await;
    }

    // ==========================================================================
    // Timeout Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_batch_execution_timeout() {
        let executor = Arc::new(SlowExecutor::new(Duration::from_secs(5)));
        let config = BatchConfig::new()
            .with_batch_size(2)
            .with_timeout(Duration::from_millis(50))
            .with_execution_timeout(Duration::from_millis(100));

        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        let result = timeout(
            Duration::from_secs(1),
            handle.submit("slow_request".to_string()),
        )
        .await;

        // Should timeout
        assert!(result.is_ok()); // The outer timeout should not fire
        assert!(result.unwrap().is_err()); // The batch should have timed out

        handle.shutdown().await;
    }

    // ==========================================================================
    // Concurrent Access Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_concurrent_submissions() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(10)
            .with_timeout(Duration::from_millis(50))
            .with_max_concurrent_batches(2);

        let batcher = Batcher::new(config, executor.clone());
        let handle = Arc::new(batcher.start());

        // Spawn multiple concurrent tasks submitting requests
        let mut handles = Vec::new();
        for task_id in 0..10 {
            let batcher_handle = handle.clone();
            handles.push(tokio::spawn(async move {
                let mut results = Vec::new();
                for req_id in 0..5 {
                    let result = batcher_handle
                        .submit(format!("task_{}_req_{}", task_id, req_id))
                        .await;
                    results.push(result);
                }
                results
            }));
        }

        // Wait for all tasks
        let all_results: Vec<_> = futures::future::join_all(handles).await;

        // Verify all requests completed successfully
        for task_results in all_results {
            let results = task_results.unwrap();
            for result in results {
                assert!(result.is_ok());
            }
        }

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_max_concurrent_batches() {
        let executor = Arc::new(SlowExecutor::new(Duration::from_millis(100)));
        let config = BatchConfig::new()
            .with_batch_size(2)
            .with_timeout(Duration::from_millis(10))
            .with_max_concurrent_batches(2);

        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        // Submit many requests quickly
        let mut futures = Vec::new();
        for i in 0..20 {
            futures.push(handle.submit(format!("request_{}", i)));
        }

        // All should complete (concurrency limit shouldn't cause deadlock)
        let results = timeout(
            Duration::from_secs(5),
            futures::future::join_all(futures),
        )
        .await
        .expect("Should complete within timeout");

        for result in results {
            assert!(result.is_ok());
        }

        handle.shutdown().await;
    }

    // ==========================================================================
    // Coalescing Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_request_coalescing() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_millis(100))
            .with_coalescing(true);

        let batcher = CoalescingBatcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit identical requests concurrently
        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();

        for _ in 0..5 {
            let h = handle.clone();
            let b = barrier.clone();
            handles.push(tokio::spawn(async move {
                b.wait().await;
                h.submit("identical_request".to_string()).await
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // All should get the same result
        for result in &results {
            assert_eq!(result.as_ref().unwrap(), "echo: identical_request");
        }

        // Only one actual request should have been made (coalesced)
        let batch_sizes = executor.batch_sizes();
        let total_requests: usize = batch_sizes.iter().sum();
        assert!(total_requests < 5, "Requests should be coalesced");

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_coalescing_different_requests() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_millis(100))
            .with_coalescing(true);

        let batcher = CoalescingBatcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit different requests
        let futures = vec![
            handle.submit("request_a".to_string()),
            handle.submit("request_b".to_string()),
            handle.submit("request_c".to_string()),
        ];

        let results: Vec<_> = futures::future::join_all(futures).await;

        assert_eq!(results[0].as_ref().unwrap(), "echo: request_a");
        assert_eq!(results[1].as_ref().unwrap(), "echo: request_b");
        assert_eq!(results[2].as_ref().unwrap(), "echo: request_c");

        handle.shutdown().await;
    }

    // ==========================================================================
    // Stats Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_batcher_stats() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(5)
            .with_timeout(Duration::from_millis(50));

        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        // Submit requests
        let futures: Vec<_> = (0..12)
            .map(|i| handle.submit(format!("request_{}", i)))
            .collect();

        futures::future::join_all(futures).await;

        let stats = handle.stats();
        assert!(stats.requests_submitted >= 12);
        assert!(stats.batches_executed >= 2);

        handle.shutdown().await;
    }

    // ==========================================================================
    // Shutdown Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let executor = Arc::new(EchoExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_secs(10));

        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        // Submit some requests (collect futures without awaiting yet)
        let futures: Vec<_> = (0..5)
            .map(|i| handle.submit(format!("request_{}", i)))
            .collect();

        // Spawn the shutdown concurrently with awaiting the results,
        // so the submit futures can send their messages before shutdown completes.
        let handle_clone = handle.clone();
        let shutdown_fut = tokio::spawn(async move {
            // Small yield to let submit futures start sending
            tokio::task::yield_now().await;
            handle_clone.shutdown().await;
        });

        // All submitted requests should complete
        let results = futures::future::join_all(futures).await;
        for (i, r) in results.into_iter().enumerate() {
            assert!(r.is_ok(), "Request {} should complete: {:?}", i, r);
        }

        shutdown_fut.await.unwrap();
    }

    // ==========================================================================
    // Integration Type Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_task_submission_integration() {
        // Test that the integration types compile and work
        use crate::batching::executor::MockTaskSubmissionExecutor;

        let executor = Arc::new(MockTaskSubmissionExecutor::new());
        let config = BatchConfig::default();
        let batcher: TaskSubmissionBatcher = Batcher::new(config, executor);
        let handle = batcher.start();

        // Submit a task
        let task_spec = crate::batching::types::TaskSubmissionRequest {
            job_id: uuid::Uuid::new_v4(),
            payload: b"test payload".to_vec(),
            priority: 5,
        };

        let result = handle.submit(task_spec).await;
        assert!(result.is_ok());

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_status_query_integration() {
        use crate::batching::executor::MockStatusQueryExecutor;

        let executor = Arc::new(MockStatusQueryExecutor::new());
        let config = BatchConfig::default();
        let batcher: StatusQueryBatcher = Batcher::new(config, executor);
        let handle = batcher.start();

        let task_id = uuid::Uuid::new_v4();
        let result = handle.submit(task_id).await;
        assert!(result.is_ok());

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_result_retrieval_integration() {
        use crate::batching::executor::MockResultRetrievalExecutor;

        let executor = Arc::new(MockResultRetrievalExecutor::new());
        let config = BatchConfig::default();
        let batcher: ResultBatcher = Batcher::new(config, executor);
        let handle = batcher.start();

        let task_id = uuid::Uuid::new_v4();
        let result = handle.submit(task_id).await;
        assert!(result.is_ok());

        handle.shutdown().await;
    }
}
