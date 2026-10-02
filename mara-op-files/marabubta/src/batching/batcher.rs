// Marabunta - Licensed under the MIT License.
//! Core batcher implementation
//!
//! This module provides the `Batcher` struct that collects individual requests
//! and executes them in batches for improved efficiency.

use crate::batching::executor::BatchExecutor;
use crate::batching::types::{BatchConfig, BatchError, BatchRequest, BatchResult, PendingRequest};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::{interval, timeout, Instant};
use tracing::{error, info, trace, warn};

/// Statistics for the batcher
#[derive(Debug, Clone, Default)]
pub struct BatcherStats {
    /// Total requests submitted
    pub requests_submitted: u64,

    /// Total batches executed
    pub batches_executed: u64,

    /// Total requests that completed successfully
    pub requests_succeeded: u64,

    /// Total requests that failed
    pub requests_failed: u64,

    /// Average batch size
    pub avg_batch_size: f64,

    /// Average batch execution time in milliseconds
    pub avg_execution_time_ms: f64,

    /// Number of requests currently pending
    pub pending_requests: u64,

    /// Number of batches currently executing
    pub active_batches: u64,
}

/// Shared state for tracking statistics
struct BatcherState {
    requests_submitted: AtomicU64,
    batches_executed: AtomicU64,
    requests_succeeded: AtomicU64,
    requests_failed: AtomicU64,
    total_batch_size: AtomicU64,
    total_execution_time_ms: AtomicU64,
    pending_requests: AtomicU64,
    active_batches: AtomicU64,
    shutdown: AtomicBool,
}

impl BatcherState {
    fn new() -> Self {
        Self {
            requests_submitted: AtomicU64::new(0),
            batches_executed: AtomicU64::new(0),
            requests_succeeded: AtomicU64::new(0),
            requests_failed: AtomicU64::new(0),
            total_batch_size: AtomicU64::new(0),
            total_execution_time_ms: AtomicU64::new(0),
            pending_requests: AtomicU64::new(0),
            active_batches: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        }
    }

    fn stats(&self) -> BatcherStats {
        let batches = self.batches_executed.load(Ordering::Relaxed);
        let total_size = self.total_batch_size.load(Ordering::Relaxed);
        let total_time = self.total_execution_time_ms.load(Ordering::Relaxed);

        BatcherStats {
            requests_submitted: self.requests_submitted.load(Ordering::Relaxed),
            batches_executed: batches,
            requests_succeeded: self.requests_succeeded.load(Ordering::Relaxed),
            requests_failed: self.requests_failed.load(Ordering::Relaxed),
            avg_batch_size: if batches > 0 {
                total_size as f64 / batches as f64
            } else {
                0.0
            },
            avg_execution_time_ms: if batches > 0 {
                total_time as f64 / batches as f64
            } else {
                0.0
            },
            pending_requests: self.pending_requests.load(Ordering::Relaxed),
            active_batches: self.active_batches.load(Ordering::Relaxed),
        }
    }
}

/// Message types for the batcher's internal channel
enum BatcherMessage<K, V> {
    /// A new request to be batched
    Request(PendingRequest<K, V>),
    /// Flush pending requests immediately
    Flush,
    /// Shutdown the batcher
    Shutdown(oneshot::Sender<()>),
}

/// A batcher that collects requests and executes them in batches
///
/// The batcher provides an efficient way to batch multiple individual requests
/// together and execute them as a single batch operation. This is useful for:
///
/// - Reducing network round trips
/// - Improving throughput for batch-friendly backends
/// - Coalescing redundant requests
///
/// # Type Parameters
///
/// - `K`: The request/key type
/// - `V`: The response/value type
pub struct Batcher<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    config: BatchConfig,
    executor: Arc<dyn BatchExecutor<K, V>>,
}

impl<K, V> Batcher<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Create a new batcher with the given configuration and executor
    pub fn new<E>(config: BatchConfig, executor: Arc<E>) -> Self
    where
        E: BatchExecutor<K, V>,
    {
        Self {
            config,
            executor: executor as Arc<dyn BatchExecutor<K, V>>,
        }
    }

    /// Start the batcher and return a handle for submitting requests
    pub fn start(self) -> BatcherHandle<K, V> {
        let (tx, rx) = mpsc::unbounded_channel();
        let state = Arc::new(BatcherState::new());

        let worker = BatcherWorker {
            config: self.config.clone(),
            executor: self.executor,
            rx,
            state: state.clone(),
            pending: Vec::new(),
            last_batch_time: Instant::now(),
            concurrency_semaphore: Arc::new(Semaphore::new(self.config.max_concurrent_batches)),
        };

        // Spawn the worker task
        let worker_handle = tokio::spawn(worker.run());

        BatcherHandle {
            tx,
            state,
            worker_handle: Some(worker_handle),
        }
    }
}

/// Handle for submitting requests to the batcher
///
/// This handle is cloneable and can be shared across multiple tasks.
pub struct BatcherHandle<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    tx: mpsc::UnboundedSender<BatcherMessage<K, V>>,
    state: Arc<BatcherState>,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

impl<K, V> Clone for BatcherHandle<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            state: self.state.clone(),
            worker_handle: None, // Only the original handle owns the worker
        }
    }
}

impl<K, V> BatcherHandle<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    /// Submit a request to be batched
    ///
    /// Returns a future that resolves when the request's batch completes.
    pub async fn submit(&self, request: K) -> BatchResult<V> {
        if self.state.shutdown.load(Ordering::Relaxed) {
            return Err(BatchError::ShutDown);
        }

        let (response_tx, response_rx) = oneshot::channel();
        let pending = PendingRequest::new(request, response_tx);

        self.state.requests_submitted.fetch_add(1, Ordering::Relaxed);
        self.state.pending_requests.fetch_add(1, Ordering::Relaxed);

        if let Err(_) = self.tx.send(BatcherMessage::Request(pending)) {
            self.state.pending_requests.fetch_sub(1, Ordering::Relaxed);
            return Err(BatchError::SendFailed("Batcher channel closed".to_string()));
        }

        match response_rx.await {
            Ok(result) => result,
            Err(_) => Err(BatchError::ReceiveFailed(
                "Response channel closed".to_string(),
            )),
        }
    }

    /// Flush all pending requests immediately
    pub fn flush(&self) {
        let _ = self.tx.send(BatcherMessage::Flush);
    }

    /// Get current batcher statistics
    pub fn stats(&self) -> BatcherStats {
        self.state.stats()
    }

    /// Check if the batcher is still running
    pub fn is_running(&self) -> bool {
        !self.state.shutdown.load(Ordering::Relaxed)
    }

    /// Shutdown the batcher gracefully
    ///
    /// This will flush any pending requests and wait for them to complete.
    pub async fn shutdown(&self) {
        if self
            .state
            .shutdown
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            // Already shutting down
            return;
        }

        let (done_tx, done_rx) = oneshot::channel();
        if let Err(_) = self.tx.send(BatcherMessage::Shutdown(done_tx)) {
            // Worker already stopped
            return;
        }

        // Wait for worker to finish
        let _ = done_rx.await;
    }
}

/// Internal worker that processes batches
struct BatcherWorker<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    config: BatchConfig,
    executor: Arc<dyn BatchExecutor<K, V>>,
    rx: mpsc::UnboundedReceiver<BatcherMessage<K, V>>,
    state: Arc<BatcherState>,
    pending: Vec<PendingRequest<K, V>>,
    last_batch_time: Instant,
    concurrency_semaphore: Arc<Semaphore>,
}

impl<K, V> BatcherWorker<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    async fn run(mut self) {
        let mut check_interval = interval(self.config.timeout / 4);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;

                // Handle incoming messages
                msg = self.rx.recv() => {
                    match msg {
                        Some(BatcherMessage::Request(req)) => {
                            self.pending.push(req);

                            // Check if we should execute a batch
                            if self.pending.len() >= self.config.batch_size {
                                self.execute_batch().await;
                            }
                        }
                        Some(BatcherMessage::Flush) => {
                            if !self.pending.is_empty() {
                                self.execute_batch().await;
                            }
                        }
                        Some(BatcherMessage::Shutdown(done_tx)) => {
                            // Flush remaining requests
                            while !self.pending.is_empty() {
                                self.execute_batch().await;
                            }
                            let _ = done_tx.send(());
                            break;
                        }
                        None => {
                            // Channel closed
                            break;
                        }
                    }
                }

                // Check for timeout
                _ = check_interval.tick() => {
                    if !self.pending.is_empty() && self.last_batch_time.elapsed() >= self.config.timeout {
                        self.execute_batch().await;
                    }
                }
            }
        }

        info!("Batcher worker shutting down");
    }

    async fn execute_batch(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        let batch_size = std::cmp::min(self.pending.len(), self.config.batch_size);
        let pending: Vec<_> = self.pending.drain(..batch_size).collect();
        let pending_count = pending.len();

        // Update pending count
        self.state
            .pending_requests
            .fetch_sub(pending_count as u64, Ordering::Relaxed);

        // Create batch request
        let (batch, senders) = BatchRequest::from_pending(pending);

        trace!(
            "Executing batch of {} requests",
            batch.len()
        );

        // Get semaphore permit for concurrency control
        let permit = self.concurrency_semaphore.clone().acquire_owned().await;
        if permit.is_err() {
            // Semaphore closed, send errors to all senders
            for sender in senders {
                let _ = sender.send(Err(BatchError::ShutDown));
            }
            return;
        }
        let _permit = permit.unwrap();

        self.state.active_batches.fetch_add(1, Ordering::Relaxed);
        let start_time = Instant::now();

        // Execute the batch
        let executor = self.executor.clone();
        let execution_timeout = self.config.execution_timeout;
        let requests = batch.requests;

        // Call before_batch hook
        executor.before_batch(requests.len()).await;

        let results = timeout(execution_timeout, executor.execute_batch(requests)).await;

        let elapsed = start_time.elapsed();

        // Update statistics
        self.state.active_batches.fetch_sub(1, Ordering::Relaxed);
        self.state.batches_executed.fetch_add(1, Ordering::Relaxed);
        self.state
            .total_batch_size
            .fetch_add(pending_count as u64, Ordering::Relaxed);
        self.state
            .total_execution_time_ms
            .fetch_add(elapsed.as_millis() as u64, Ordering::Relaxed);

        match results {
            Ok(batch_results) => {
                if batch_results.len() != senders.len() {
                    error!(
                        "Batch executor returned {} results for {} requests",
                        batch_results.len(),
                        senders.len()
                    );

                    // Send errors for mismatched results
                    for sender in senders {
                        let _ = sender.send(Err(BatchError::Internal(
                            "Result count mismatch".to_string(),
                        )));
                        self.state.requests_failed.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    // Count successes and failures
                    let success_count = batch_results.iter().filter(|r| r.is_ok()).count();
                    let failure_count = batch_results.len() - success_count;

                    self.state
                        .requests_succeeded
                        .fetch_add(success_count as u64, Ordering::Relaxed);
                    self.state
                        .requests_failed
                        .fetch_add(failure_count as u64, Ordering::Relaxed);

                    // Call after_batch hook
                    executor.after_batch(batch_results.len(), success_count).await;

                    // Send results to waiting callers
                    for (sender, result) in senders.into_iter().zip(batch_results) {
                        let _ = sender.send(result);
                    }
                }
            }
            Err(_) => {
                warn!(
                    "Batch execution timed out after {:?}",
                    execution_timeout
                );

                // Send timeout errors to all senders
                for sender in senders {
                    let _ = sender.send(Err(BatchError::ExecutionTimeout(execution_timeout)));
                    self.state.requests_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        self.last_batch_time = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    struct CountingExecutor {
        count: AtomicUsize,
    }

    impl CountingExecutor {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl BatchExecutor<String, String> for CountingExecutor {
        async fn execute_batch(&self, requests: Vec<String>) -> Vec<Result<String, BatchError>> {
            self.count.fetch_add(1, Ordering::SeqCst);
            requests.into_iter().map(|r| Ok(r.to_uppercase())).collect()
        }
    }

    #[tokio::test]
    async fn test_batcher_creation() {
        let executor = Arc::new(CountingExecutor::new());
        let config = BatchConfig::default();
        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        assert!(handle.is_running());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_single_request() {
        let executor = Arc::new(CountingExecutor::new());
        let config = BatchConfig::new().with_timeout(Duration::from_millis(50));
        let batcher = Batcher::new(config, executor.clone());
        let handle = batcher.start();

        let result = handle.submit("hello".to_string()).await;
        assert_eq!(result.unwrap(), "HELLO");
        assert_eq!(executor.count(), 1);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_batch_size_trigger() {
        let executor = Arc::new(CountingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(3)
            .with_timeout(Duration::from_secs(10));
        let batcher = Batcher::new(config, executor.clone());
        let handle = batcher.start();

        // Submit 3 requests - should trigger batch by size
        let f1 = handle.submit("a".to_string());
        let f2 = handle.submit("b".to_string());
        let f3 = handle.submit("c".to_string());

        let (r1, r2, r3) = tokio::join!(f1, f2, f3);

        assert_eq!(r1.unwrap(), "A");
        assert_eq!(r2.unwrap(), "B");
        assert_eq!(r3.unwrap(), "C");
        assert_eq!(executor.count(), 1);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_stats() {
        let executor = Arc::new(CountingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(5)
            .with_timeout(Duration::from_millis(50));
        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        // Submit some requests
        let futures: Vec<_> = (0..10)
            .map(|i| handle.submit(format!("req{}", i)))
            .collect();

        futures::future::join_all(futures).await;

        let stats = handle.stats();
        assert_eq!(stats.requests_submitted, 10);
        assert!(stats.batches_executed >= 2);

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_flush() {
        let executor = Arc::new(CountingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_secs(10));
        let batcher = Batcher::new(config, executor.clone());
        let handle = batcher.start();

        let f1 = handle.submit("a".to_string());
        let f2 = handle.submit("b".to_string());

        // Flush before batch is full
        handle.flush();

        let (r1, r2) = tokio::join!(f1, f2);
        assert!(r1.is_ok());
        assert!(r2.is_ok());

        handle.shutdown().await;
    }

    #[tokio::test]
    async fn test_shutdown_flushes_pending() {
        let executor = Arc::new(CountingExecutor::new());
        let config = BatchConfig::new()
            .with_batch_size(100)
            .with_timeout(Duration::from_secs(10));
        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        // Submit requests
        let futures: Vec<_> = (0..5)
            .map(|i| handle.submit(format!("req{}", i)))
            .collect();

        // Shutdown concurrently with awaiting results so submit futures can
        // send their messages before the shutdown completes.
        let handle_clone = handle.clone();
        let shutdown_fut = tokio::spawn(async move {
            tokio::task::yield_now().await;
            handle_clone.shutdown().await;
        });

        // All should complete
        for result in futures::future::join_all(futures).await {
            assert!(result.is_ok());
        }

        shutdown_fut.await.unwrap();
    }

    #[tokio::test]
    async fn test_cloneable_handle() {
        let executor = Arc::new(CountingExecutor::new());
        let config = BatchConfig::new().with_timeout(Duration::from_millis(50));
        let batcher = Batcher::new(config, executor);
        let handle = batcher.start();

        let handle2 = handle.clone();
        let handle3 = handle.clone();

        let f1 = handle.submit("a".to_string());
        let f2 = handle2.submit("b".to_string());
        let f3 = handle3.submit("c".to_string());

        let (r1, r2, r3) = tokio::join!(f1, f2, f3);

        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert!(r3.is_ok());

        handle.shutdown().await;
    }
}
