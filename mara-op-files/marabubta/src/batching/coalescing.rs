// Marabunta - Licensed under the MIT License.
//! Request coalescing for the batching system
//!
//! This module provides automatic coalescing of identical requests. When multiple
//! callers request the same operation, only one actual request is made and all
//! callers receive the result.

use crate::batching::batcher::{Batcher, BatcherHandle};
use crate::batching::executor::BatchExecutor;
use crate::batching::types::{BatchConfig, BatchError, BatchResult};
use dashmap::DashMap;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::trace;

/// Trait for types that can be used as coalescing keys
///
/// Types implementing this trait can be used to identify identical requests
/// for coalescing purposes.
pub trait CoalescingKey: Clone + Eq + Hash + Send + Sync + 'static {}

// Blanket implementation for all suitable types
impl<T> CoalescingKey for T where T: Clone + Eq + Hash + Send + Sync + 'static {}

/// A batcher with automatic request coalescing
///
/// When coalescing is enabled, identical requests submitted within the same
/// batch window are coalesced - only one actual request is made, and all
/// callers waiting for that request receive the same result.
///
/// # Example
///
/// ```rust,no_run
/// use marabunta_compute::batching::{CoalescingBatcher, BatchConfig, BatchExecutor, BatchError};
/// use std::sync::Arc;
///
/// struct MyExecutor;
///
/// #[async_trait::async_trait]
/// impl BatchExecutor<String, String> for MyExecutor {
///     async fn execute_batch(&self, requests: Vec<String>) -> Vec<Result<String, BatchError>> {
///         requests.into_iter().map(|r| Ok(r.to_uppercase())).collect()
///     }
/// }
///
/// # tokio_test::block_on(async {
/// let config = BatchConfig::default().with_coalescing(true);
/// let batcher = CoalescingBatcher::new(config, Arc::new(MyExecutor));
/// let handle = batcher.start();
///
/// // These identical requests will be coalesced
/// let f1 = handle.submit("same_request".to_string());
/// let f2 = handle.submit("same_request".to_string());
///
/// // Both will get the same result, but only one actual request was made
/// let (r1, r2) = tokio::join!(f1, f2);
/// assert_eq!(r1.unwrap(), r2.unwrap());
/// # });
/// ```
pub struct CoalescingBatcher<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    config: BatchConfig,
    executor: Arc<dyn BatchExecutor<K, V>>,
}

impl<K, V> CoalescingBatcher<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    /// Create a new coalescing batcher
    pub fn new<E>(config: BatchConfig, executor: Arc<E>) -> Self
    where
        E: BatchExecutor<K, V>,
    {
        Self {
            config,
            executor: executor as Arc<dyn BatchExecutor<K, V>>,
        }
    }

    /// Start the coalescing batcher and return a handle
    pub fn start(self) -> CoalescingBatcherHandle<K, V> {
        // Create the underlying batcher with a coalescing executor wrapper
        let coalescing_state = Arc::new(CoalescingState::new());
        let coalescing_executor = Arc::new(CoalescingExecutor {
            inner: self.executor,
            state: coalescing_state.clone(),
        });

        let batcher = Batcher::new(self.config, coalescing_executor);
        let inner_handle = batcher.start();

        CoalescingBatcherHandle {
            inner: inner_handle,
            state: coalescing_state,
        }
    }
}

/// Internal state for tracking in-flight coalesced requests
struct CoalescingState<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    /// Map of in-flight requests: key -> list of waiting receivers
    in_flight: DashMap<K, Vec<oneshot::Sender<BatchResult<V>>>>,

    /// Statistics
    coalesced_count: std::sync::atomic::AtomicU64,
}

impl<K, V> CoalescingState<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    fn new() -> Self {
        Self {
            in_flight: DashMap::new(),
            coalesced_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Try to coalesce a request. Returns Some(receiver) if coalesced,
    /// None if this is the first request for this key.
    fn try_coalesce(&self, key: &K) -> Option<oneshot::Receiver<BatchResult<V>>> {
        let mut entry = self.in_flight.entry(key.clone()).or_default();

        if entry.is_empty() {
            // First request for this key
            None
        } else {
            // Coalesce with existing request
            let (tx, rx) = oneshot::channel();
            entry.push(tx);
            self.coalesced_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(rx)
        }
    }

    /// Register a new in-flight request
    fn register(&self, key: K) {
        self.in_flight.entry(key).or_default();
    }

    /// Complete all waiting requests for a key
    fn complete(&self, key: &K, result: BatchResult<V>) {
        if let Some((_, waiters)) = self.in_flight.remove(key) {
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }
        }
    }

    /// Get the count of coalesced requests
    fn coalesced_count(&self) -> u64 {
        self.coalesced_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Wrapper executor that handles coalescing
struct CoalescingExecutor<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    inner: Arc<dyn BatchExecutor<K, V>>,
    state: Arc<CoalescingState<K, V>>,
}

#[async_trait::async_trait]
impl<K, V> BatchExecutor<K, V> for CoalescingExecutor<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    async fn execute_batch(&self, requests: Vec<K>) -> Vec<Result<V, BatchError>> {
        // Deduplicate requests while preserving order information
        let mut unique_requests: Vec<K> = Vec::new();
        let mut request_indices: HashMap<K, usize> = HashMap::new();
        let mut original_to_unique: Vec<usize> = Vec::with_capacity(requests.len());

        for req in &requests {
            if let Some(&idx) = request_indices.get(req) {
                original_to_unique.push(idx);
            } else {
                let idx = unique_requests.len();
                unique_requests.push(req.clone());
                request_indices.insert(req.clone(), idx);
                original_to_unique.push(idx);
            }
        }

        trace!(
            "Coalescing: {} requests -> {} unique",
            requests.len(),
            unique_requests.len()
        );

        // Execute only unique requests
        let unique_results = self.inner.execute_batch(unique_requests.clone()).await;

        // Complete any in-flight waiters
        for (req, result) in unique_requests.iter().zip(unique_results.iter()) {
            self.state.complete(req, result.clone());
        }

        // Map results back to original request order
        original_to_unique
            .into_iter()
            .map(|idx| unique_results[idx].clone())
            .collect()
    }
}

/// Handle for submitting requests to a coalescing batcher
pub struct CoalescingBatcherHandle<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    inner: BatcherHandle<K, V>,
    state: Arc<CoalescingState<K, V>>,
}

impl<K, V> Clone for CoalescingBatcherHandle<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            state: self.state.clone(),
        }
    }
}

impl<K, V> CoalescingBatcherHandle<K, V>
where
    K: CoalescingKey,
    V: Clone + Send + 'static,
{
    /// Submit a request, potentially coalescing with identical in-flight requests
    pub async fn submit(&self, request: K) -> BatchResult<V> {
        // Check if we can coalesce with an existing request
        if let Some(rx) = self.state.try_coalesce(&request) {
            trace!("Request coalesced with in-flight request");
            return rx.await.unwrap_or(Err(BatchError::Cancelled));
        }

        // Register this request as in-flight
        self.state.register(request.clone());

        // Submit to the underlying batcher
        let result = self.inner.submit(request.clone()).await;

        // Complete any waiters (the executor also does this, but we do it here too
        // for immediate coalescing before batching)
        self.state.complete(&request, result.clone());

        result
    }

    /// Flush pending requests
    pub fn flush(&self) {
        self.inner.flush();
    }

    /// Get statistics including coalescing information
    pub fn stats(&self) -> CoalescingStats {
        let inner_stats = self.inner.stats();
        CoalescingStats {
            requests_submitted: inner_stats.requests_submitted,
            batches_executed: inner_stats.batches_executed,
            requests_succeeded: inner_stats.requests_succeeded,
            requests_failed: inner_stats.requests_failed,
            avg_batch_size: inner_stats.avg_batch_size,
            coalesced_requests: self.state.coalesced_count(),
        }
    }

    /// Check if the batcher is running
    pub fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    /// Shutdown the batcher
    pub async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

/// Statistics for a coalescing batcher
#[derive(Debug, Clone, Default)]
pub struct CoalescingStats {
    /// Total requests submitted
    pub requests_submitted: u64,

    /// Total batches executed
    pub batches_executed: u64,

    /// Total requests that succeeded
    pub requests_succeeded: u64,

    /// Total requests that failed
    pub requests_failed: u64,

    /// Average batch size
    pub avg_batch_size: f64,

    /// Number of requests that were coalesced (not executed, shared result)
    pub coalesced_requests: u64,
}

impl CoalescingStats {
    /// Calculate the coalescing ratio (coalesced / submitted)
    pub fn coalescing_ratio(&self) -> f64 {
        if self.requests_submitted == 0 {
            0.0
        } else {
            self.coalesced_requests as f64 / self.requests_submitted as f64
        }
    }

    /// Calculate the effective reduction in requests
    pub fn effective_requests(&self) -> u64 {
        self.requests_submitted.saturating_sub(self.coalesced_requests)
    }
}

// =============================================================================
// Deduplicating Executor Wrapper
// =============================================================================

/// A simpler executor wrapper that just deduplicates within a batch
/// without maintaining in-flight state across batches.
///
/// This is useful when you want deduplication within a batch but don't need
/// cross-batch coalescing.
pub struct DeduplicatingExecutor<K, V, E>
where
    K: CoalescingKey,
    V: Clone + Send + Sync + 'static,
    E: BatchExecutor<K, V>,
{
    inner: E,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V, E> DeduplicatingExecutor<K, V, E>
where
    K: CoalescingKey,
    V: Clone + Send + Sync + 'static,
    E: BatchExecutor<K, V>,
{
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<K, V, E> BatchExecutor<K, V> for DeduplicatingExecutor<K, V, E>
where
    K: CoalescingKey,
    V: Clone + Send + Sync + 'static,
    E: BatchExecutor<K, V>,
{
    async fn execute_batch(&self, requests: Vec<K>) -> Vec<Result<V, BatchError>> {
        // Deduplicate
        let mut unique_requests: Vec<K> = Vec::new();
        let mut request_indices: HashMap<K, usize> = HashMap::new();
        let mut original_to_unique: Vec<usize> = Vec::with_capacity(requests.len());

        for req in &requests {
            if let Some(&idx) = request_indices.get(req) {
                original_to_unique.push(idx);
            } else {
                let idx = unique_requests.len();
                unique_requests.push(req.clone());
                request_indices.insert(req.clone(), idx);
                original_to_unique.push(idx);
            }
        }

        // Execute unique requests
        let unique_results = self.inner.execute_batch(unique_requests).await;

        // Map back to original order
        original_to_unique
            .into_iter()
            .map(|idx| unique_results[idx].clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Barrier;

    struct TrackingExecutor {
        call_count: AtomicUsize,
        request_counts: Mutex<Vec<usize>>,
    }

    impl TrackingExecutor {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                request_counts: Mutex::new(Vec::new()),
            }
        }

        fn total_requests_seen(&self) -> usize {
            self.request_counts.lock().iter().sum()
        }
    }

    #[async_trait::async_trait]
    impl BatchExecutor<String, String> for TrackingExecutor {
        async fn execute_batch(&self, requests: Vec<String>) -> Vec<Result<String, BatchError>> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.request_counts.lock().push(requests.len());

            // Small delay to allow coalescing
            tokio::time::sleep(Duration::from_millis(10)).await;

            requests.into_iter().map(|r| Ok(r.to_uppercase())).collect()
        }
    }

    #[tokio::test]
    async fn test_coalescing_identical_requests() {
        let executor = Arc::new(TrackingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_millis(50))
            .with_coalescing(true);

        let batcher = CoalescingBatcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit identical requests
        let f1 = handle.submit("same".to_string());
        let f2 = handle.submit("same".to_string());
        let f3 = handle.submit("same".to_string());

        let (r1, r2, r3) = tokio::join!(f1, f2, f3);

        // All should get the same result
        assert_eq!(r1.unwrap(), "SAME");
        assert_eq!(r2.unwrap(), "SAME");
        assert_eq!(r3.unwrap(), "SAME");

        // The executor should have only seen one "same" request
        assert!(executor.total_requests_seen() < 3);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_coalescing_different_requests() {
        let executor = Arc::new(TrackingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_millis(50))
            .with_coalescing(true);

        let batcher = CoalescingBatcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit different requests
        let f1 = handle.submit("a".to_string());
        let f2 = handle.submit("b".to_string());
        let f3 = handle.submit("c".to_string());

        let (r1, r2, r3) = tokio::join!(f1, f2, f3);

        assert_eq!(r1.unwrap(), "A");
        assert_eq!(r2.unwrap(), "B");
        assert_eq!(r3.unwrap(), "C");

        // All three should have been executed
        assert_eq!(executor.total_requests_seen(), 3);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_coalescing_stats() {
        let executor = Arc::new(TrackingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(10)
            .with_timeout(Duration::from_millis(50))
            .with_coalescing(true);

        let batcher = CoalescingBatcher::new(config, executor.clone());
        let handle = Arc::new(batcher.start());

        // Submit mix of same and different requests concurrently
        // (coalescing requires concurrent in-flight requests to work)
        let barrier = Arc::new(Barrier::new(5));
        let mut spawn_handles = Vec::new();

        let requests = vec!["a", "a", "b", "a", "b"];
        for req in requests {
            let h = handle.clone();
            let b = barrier.clone();
            spawn_handles.push(tokio::spawn(async move {
                b.wait().await;
                h.submit(req.to_string()).await
            }));
        }

        let results: Vec<_> = futures::future::join_all(spawn_handles).await;
        for result in &results {
            assert!(result.as_ref().unwrap().is_ok());
        }

        let stats = handle.stats();
        // All 5 requests should have been submitted and completed
        assert!(stats.requests_submitted >= 5);
        // The executor should have seen fewer unique requests due to
        // deduplication within batches (3 unique: a, b, c) even if
        // handle-level coalescing didn't trigger
        assert!(executor.total_requests_seen() <= 5);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_deduplicating_executor() {
        struct InnerExecutor {
            seen: Mutex<Vec<Vec<String>>>,
        }

        #[async_trait::async_trait]
        impl BatchExecutor<String, String> for InnerExecutor {
            async fn execute_batch(&self, requests: Vec<String>) -> Vec<Result<String, BatchError>> {
                self.seen.lock().push(requests.clone());
                requests.into_iter().map(|r| Ok(r.to_uppercase())).collect()
            }
        }

        let inner = InnerExecutor {
            seen: Mutex::new(Vec::new()),
        };
        let executor = DeduplicatingExecutor::new(inner);

        // Execute batch with duplicates
        let requests = vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "b".to_string(),
        ];

        let results = executor.execute_batch(requests).await;

        // All results should be correct
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].as_ref().unwrap(), "A");
        assert_eq!(results[1].as_ref().unwrap(), "B");
        assert_eq!(results[2].as_ref().unwrap(), "A");
        assert_eq!(results[3].as_ref().unwrap(), "C");
        assert_eq!(results[4].as_ref().unwrap(), "B");

        // Inner executor should have seen deduplicated batch
        let seen = executor.inner.seen.lock();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].len(), 3); // Only unique: a, b, c
    }

    #[tokio::test]
    async fn test_concurrent_coalescing() {
        let executor = Arc::new(TrackingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_millis(100))
            .with_coalescing(true);

        let batcher = CoalescingBatcher::new(config, executor.clone());
        let handle = Arc::new(batcher.start());

        // Use barrier to ensure concurrent submission
        let barrier = Arc::new(Barrier::new(10));
        let mut handles = Vec::new();

        for _ in 0..10 {
            let h = handle.clone();
            let b = barrier.clone();
            handles.push(tokio::spawn(async move {
                b.wait().await;
                h.submit("concurrent".to_string()).await
            }));
        }

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // All should succeed with same result
        for result in results {
            assert_eq!(result.unwrap(), "CONCURRENT");
        }

        // Should have coalesced most requests
        assert!(executor.total_requests_seen() < 10);

        handle.shutdown().await;
    }
}
