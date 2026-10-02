// Marabunta - Licensed under the MIT License.
//! Batch Job Submission for the Marabunta SDK
//!
//! This module provides functionality for submitting multiple jobs at once
//! with parallel execution and result collection.
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::sdk::batch::{BatchSubmitter, BatchOptions};
//! use marabunta_compute::sdk::client::MarabuntaClient;
//! use marabunta_compute::sdk::builder::JobBuilder;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Arc::new(MarabuntaClient::new("http://localhost:8080")?);
//!
//!     let jobs = vec![
//!         JobBuilder::new("task-1").with_input(serde_json::json!({"id": 1})).build()?,
//!         JobBuilder::new("task-2").with_input(serde_json::json!({"id": 2})).build()?,
//!         JobBuilder::new("task-3").with_input(serde_json::json!({"id": 3})).build()?,
//!     ];
//!
//!     let batch = BatchSubmitter::new(client)
//!         .with_concurrency(10)
//!         .with_fail_fast(false);
//!
//!     let results = batch.submit_and_wait::<serde_json::Value>(jobs).await?;
//!
//!     for result in results {
//!         match result {
//!             Ok(value) => println!("Success: {:?}", value),
//!             Err(e) => println!("Error: {:?}", e),
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::sdk::client::{
    CancellationToken, ClientError, ClientResult, MarabuntaClient, JobRequest,
};

/// Options for batch submission
#[derive(Debug, Clone)]
pub struct BatchOptions {
    /// Maximum number of concurrent submissions
    pub max_concurrency: usize,
    /// Whether to stop on first error
    pub fail_fast: bool,
    /// Timeout for the entire batch operation
    pub batch_timeout: Option<Duration>,
    /// Delay between submissions (rate limiting)
    pub submission_delay: Option<Duration>,
    /// Whether to continue polling for results even if some jobs fail
    pub continue_on_error: bool,
    /// Callback interval for progress updates
    pub progress_interval: Duration,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_concurrency: 50,
            fail_fast: false,
            batch_timeout: Some(Duration::from_secs(3600)),
            submission_delay: None,
            continue_on_error: true,
            progress_interval: Duration::from_secs(1),
        }
    }
}

impl BatchOptions {
    /// Create new batch options with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum concurrency
    pub fn with_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max.max(1);
        self
    }

    /// Enable or disable fail-fast mode
    pub fn with_fail_fast(mut self, enabled: bool) -> Self {
        self.fail_fast = enabled;
        self
    }

    /// Set batch timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.batch_timeout = Some(timeout);
        self
    }

    /// Disable batch timeout
    pub fn without_timeout(mut self) -> Self {
        self.batch_timeout = None;
        self
    }

    /// Set submission delay for rate limiting
    pub fn with_submission_delay(mut self, delay: Duration) -> Self {
        self.submission_delay = Some(delay);
        self
    }

    /// Set continue on error behavior
    pub fn with_continue_on_error(mut self, enabled: bool) -> Self {
        self.continue_on_error = enabled;
        self
    }

    /// Set progress update interval
    pub fn with_progress_interval(mut self, interval: Duration) -> Self {
        self.progress_interval = interval;
        self
    }
}

/// Result of a single job in a batch
#[derive(Debug)]
pub struct BatchJobResult<T> {
    /// Index of the job in the original batch
    pub index: usize,
    /// Job ID (if submission was successful)
    pub job_id: Option<String>,
    /// Result of the job
    pub result: ClientResult<T>,
    /// Duration from submission to completion
    pub duration: Duration,
}

/// Progress information for batch operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProgress {
    /// Total number of jobs in the batch
    pub total: usize,
    /// Number of jobs submitted
    pub submitted: usize,
    /// Number of jobs completed (successfully or with error)
    pub completed: usize,
    /// Number of jobs that succeeded
    pub succeeded: usize,
    /// Number of jobs that failed
    pub failed: usize,
    /// Number of jobs currently running
    pub running: usize,
    /// Number of jobs pending
    pub pending: usize,
    /// Estimated time remaining (if calculable)
    pub eta_seconds: Option<f64>,
}

impl BatchProgress {
    /// Create new batch progress
    pub fn new(total: usize) -> Self {
        Self {
            total,
            submitted: 0,
            completed: 0,
            succeeded: 0,
            failed: 0,
            running: 0,
            pending: total,
        eta_seconds: None,
        }
    }

    /// Calculate completion percentage
    pub fn percentage(&self) -> f64 {
        if self.total == 0 {
            100.0
        } else {
            (self.completed as f64 / self.total as f64) * 100.0
        }
    }

    /// Check if all jobs are complete
    pub fn is_complete(&self) -> bool {
        self.completed >= self.total
    }
}

/// Batch submitter for parallel job submission and result collection
pub struct BatchSubmitter {
    client: Arc<MarabuntaClient>,
    options: BatchOptions,
    cancel: CancellationToken,
}

impl BatchSubmitter {
    /// Create a new batch submitter
    pub fn new(client: Arc<MarabuntaClient>) -> Self {
        Self {
            client,
            options: BatchOptions::default(),
            cancel: CancellationToken::new(),
        }
    }

    /// Set the batch options
    pub fn with_options(mut self, options: BatchOptions) -> Self {
        self.options = options;
        self
    }

    /// Set maximum concurrency
    pub fn with_concurrency(mut self, max: usize) -> Self {
        self.options.max_concurrency = max.max(1);
        self
    }

    /// Enable fail-fast mode
    pub fn with_fail_fast(mut self, enabled: bool) -> Self {
        self.options.fail_fast = enabled;
        self
    }

    /// Set batch timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.batch_timeout = Some(timeout);
        self
    }

    /// Set cancellation token
    pub fn with_cancellation(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// Submit all jobs and return their IDs immediately
    pub async fn submit_all(&self, jobs: Vec<JobRequest>) -> Vec<ClientResult<String>> {
        let semaphore = Arc::new(Semaphore::new(self.options.max_concurrency));
        let cancel = self.cancel.clone();
        let delay = self.options.submission_delay;

        let futures = jobs.into_iter().map(|job| {
            let client = self.client.clone();
            let semaphore = semaphore.clone();
            let cancel = cancel.clone();

            async move {
                // Check cancellation
                if cancel.is_cancelled() {
                    return Err(ClientError::Cancelled);
                }

                // Acquire semaphore permit
                let _permit = semaphore.acquire().await.map_err(|_| ClientError::Cancelled)?;

                // Apply submission delay if configured
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }

                // Submit the job
                client.submit_with_cancel(job, cancel.clone()).await
            }
        });

        stream::iter(futures)
            .buffer_unordered(self.options.max_concurrency)
            .collect()
            .await
    }

    /// Submit all jobs and wait for all results
    pub async fn submit_and_wait<T: DeserializeOwned>(
        &self,
        jobs: Vec<JobRequest>,
    ) -> Vec<BatchJobResult<T>> {
        let total = jobs.len();
        let start = Instant::now();

        // Submit all jobs
        let job_ids: Vec<_> = self.submit_all(jobs).await;

        // Track results
        let mut results = Vec::with_capacity(total);

        for (index, job_id_result) in job_ids.into_iter().enumerate() {
            let job_start = Instant::now();

            match job_id_result {
                Ok(job_id) => {
                    // Wait for this job's result
                    let result = self
                        .client
                        .wait_for_result_with_cancel::<T>(&job_id, self.cancel.clone())
                        .await;

                    results.push(BatchJobResult {
                        index,
                        job_id: Some(job_id),
                        result,
                        duration: job_start.elapsed(),
                    });
                }
                Err(e) => {
                    results.push(BatchJobResult {
                        index,
                        job_id: None,
                        result: Err(e),
                        duration: job_start.elapsed(),
                    });
                }
            }

            // Check for fail-fast
            if self.options.fail_fast {
                if let Some(last) = results.last() {
                    if last.result.is_err() {
                        break;
                    }
                }
            }

            // Check cancellation
            if self.cancel.is_cancelled() {
                break;
            }

            // Check batch timeout
            if let Some(timeout) = self.options.batch_timeout {
                if start.elapsed() > timeout {
                    break;
                }
            }
        }

        results
    }

    /// Submit jobs with progress callback
    pub async fn submit_and_wait_with_progress<T, F>(
        &self,
        jobs: Vec<JobRequest>,
        mut progress_callback: F,
    ) -> Vec<BatchJobResult<T>>
    where
        T: DeserializeOwned,
        F: FnMut(BatchProgress),
    {
        let total = jobs.len();
        let progress = Arc::new(AtomicProgress::new(total));
        let start = Instant::now();

        // Create progress reporter
        let _progress_clone = progress.clone();
        let interval = self.options.progress_interval;
        let cancel_clone = self.cancel.clone();

        let progress_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_clone.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {
                        // Progress is reported via the callback in the main loop
                    }
                }
            }
        });

        // Submit all jobs
        let job_ids: Vec<_> = self.submit_all(jobs).await;
        progress.set_submitted(job_ids.len());

        // Report initial progress
        progress_callback(progress.snapshot());

        // Track results
        let mut results = Vec::with_capacity(total);

        for (index, job_id_result) in job_ids.into_iter().enumerate() {
            let job_start = Instant::now();

            match job_id_result {
                Ok(job_id) => {
                    progress.increment_running();

                    let result = self
                        .client
                        .wait_for_result_with_cancel::<T>(&job_id, self.cancel.clone())
                        .await;

                    progress.decrement_running();

                    if result.is_ok() {
                        progress.increment_succeeded();
                    } else {
                        progress.increment_failed();
                    }

                    results.push(BatchJobResult {
                        index,
                        job_id: Some(job_id),
                        result,
                        duration: job_start.elapsed(),
                    });
                }
                Err(e) => {
                    progress.increment_failed();

                    results.push(BatchJobResult {
                        index,
                        job_id: None,
                        result: Err(e),
                        duration: job_start.elapsed(),
                    });
                }
            }

            progress.increment_completed();
            progress_callback(progress.snapshot());

            // Check for fail-fast
            if self.options.fail_fast {
                if let Some(last) = results.last() {
                    if last.result.is_err() {
                        break;
                    }
                }
            }

            // Check cancellation
            if self.cancel.is_cancelled() {
                break;
            }

            // Check batch timeout
            if let Some(timeout) = self.options.batch_timeout {
                if start.elapsed() > timeout {
                    break;
                }
            }
        }

        // Cancel progress reporter
        progress_handle.abort();

        results
    }

    /// Get current progress
    pub fn get_progress(&self) -> BatchProgress {
        BatchProgress::new(0) // Would need actual tracking
    }

    /// Cancel the batch operation
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

/// Atomic progress tracker
struct AtomicProgress {
    total: usize,
    submitted: AtomicU32,
    completed: AtomicU32,
    succeeded: AtomicU32,
    failed: AtomicU32,
    running: AtomicU32,
}

impl AtomicProgress {
    fn new(total: usize) -> Self {
        Self {
            total,
            submitted: AtomicU32::new(0),
            completed: AtomicU32::new(0),
            succeeded: AtomicU32::new(0),
            failed: AtomicU32::new(0),
            running: AtomicU32::new(0),
        }
    }

    fn set_submitted(&self, count: usize) {
        self.submitted.store(count as u32, Ordering::SeqCst);
    }

    fn increment_completed(&self) {
        self.completed.fetch_add(1, Ordering::SeqCst);
    }

    fn increment_succeeded(&self) {
        self.succeeded.fetch_add(1, Ordering::SeqCst);
    }

    fn increment_failed(&self) {
        self.failed.fetch_add(1, Ordering::SeqCst);
    }

    fn increment_running(&self) {
        self.running.fetch_add(1, Ordering::SeqCst);
    }

    fn decrement_running(&self) {
        self.running.fetch_sub(1, Ordering::SeqCst);
    }

    fn snapshot(&self) -> BatchProgress {
        let submitted = self.submitted.load(Ordering::SeqCst) as usize;
        let completed = self.completed.load(Ordering::SeqCst) as usize;
        let succeeded = self.succeeded.load(Ordering::SeqCst) as usize;
        let failed = self.failed.load(Ordering::SeqCst) as usize;
        let running = self.running.load(Ordering::SeqCst) as usize;

        BatchProgress {
            total: self.total,
            submitted,
            completed,
            succeeded,
            failed,
            running,
            pending: self.total.saturating_sub(completed),
            eta_seconds: None,
        }
    }
}

/// Convenience function to submit a batch of jobs
pub async fn submit_batch<T: DeserializeOwned>(
    client: Arc<MarabuntaClient>,
    jobs: Vec<JobRequest>,
) -> Vec<BatchJobResult<T>> {
    BatchSubmitter::new(client).submit_and_wait(jobs).await
}

/// Convenience function to submit a batch with options
pub async fn submit_batch_with_options<T: DeserializeOwned>(
    client: Arc<MarabuntaClient>,
    jobs: Vec<JobRequest>,
    options: BatchOptions,
) -> Vec<BatchJobResult<T>> {
    BatchSubmitter::new(client)
        .with_options(options)
        .submit_and_wait(jobs)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdk::builder::JobBuilder;
    use serde_json::json;

    #[tokio::test]
    async fn test_batch_options_default() {
        let options = BatchOptions::default();
        assert_eq!(options.max_concurrency, 50);
        assert!(!options.fail_fast);
        assert!(options.continue_on_error);
    }

    #[tokio::test]
    async fn test_batch_options_builder() {
        let options = BatchOptions::new()
            .with_concurrency(10)
            .with_fail_fast(true)
            .with_timeout(Duration::from_secs(60))
            .with_submission_delay(Duration::from_millis(100));

        assert_eq!(options.max_concurrency, 10);
        assert!(options.fail_fast);
        assert_eq!(options.batch_timeout, Some(Duration::from_secs(60)));
        assert_eq!(options.submission_delay, Some(Duration::from_millis(100)));
    }

    #[tokio::test]
    async fn test_batch_progress() {
        let mut progress = BatchProgress::new(10);
        assert_eq!(progress.total, 10);
        assert_eq!(progress.percentage(), 0.0);
        assert!(!progress.is_complete());

        progress.completed = 5;
        assert_eq!(progress.percentage(), 50.0);

        progress.completed = 10;
        assert!(progress.is_complete());
    }

    #[tokio::test]
    async fn test_batch_submitter_creation() {
        let client = Arc::new(MarabuntaClient::new("http://localhost:8080").unwrap());
        let batch = BatchSubmitter::new(client)
            .with_concurrency(5)
            .with_fail_fast(true)
            .with_timeout(Duration::from_secs(30));

        assert_eq!(batch.options.max_concurrency, 5);
        assert!(batch.options.fail_fast);
    }

    #[tokio::test]
    async fn test_submit_all() {
        let client = Arc::new(MarabuntaClient::new("http://localhost:8080").unwrap());

        let jobs = vec![
            JobBuilder::new("task-1").with_input(json!({})).build().unwrap(),
            JobBuilder::new("task-2").with_input(json!({})).build().unwrap(),
        ];

        let batch = BatchSubmitter::new(client);
        let results = batch.submit_all(jobs).await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn test_submit_and_wait() {
        let client = Arc::new(MarabuntaClient::new("http://localhost:8080").unwrap());

        let jobs = vec![
            JobBuilder::new("task-1").with_input(json!({})).build().unwrap(),
        ];

        // Pre-set the result
        let job_id = client.submit(jobs[0].clone()).await.unwrap();
        client.set_job_result(&job_id, json!({"value": 42}));

        // Now create a new batch and submit
        let jobs = vec![
            JobBuilder::new("task-1").with_input(json!({})).build().unwrap(),
        ];

        let batch = BatchSubmitter::new(client.clone());

        // We need to manually set the result for the new job
        tokio::spawn({
            let client = client.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                // Get the latest job id
                let id = {
                    let jobs = client.mock_jobs.read();
                    jobs.iter().last().map(|(id, _)| id.clone())
                };
                if let Some(id) = id {
                    client.set_job_result(&id, json!({"value": 42}));
                }
            }
        });

        let results: Vec<BatchJobResult<serde_json::Value>> =
            batch.submit_and_wait(jobs).await;

        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_batch_cancellation() {
        let client = Arc::new(MarabuntaClient::new("http://localhost:8080").unwrap());
        let cancel = CancellationToken::new();

        let batch = BatchSubmitter::new(client).with_cancellation(cancel.clone());

        // Cancel immediately
        cancel.cancel();

        let jobs = vec![
            JobBuilder::new("task-1").with_input(json!({})).build().unwrap(),
        ];

        let results = batch.submit_all(jobs).await;
        assert!(results.iter().all(|r| matches!(r, Err(ClientError::Cancelled))));
    }

    #[tokio::test]
    async fn test_atomic_progress() {
        let progress = AtomicProgress::new(10);

        progress.set_submitted(5);
        progress.increment_running();
        progress.increment_running();
        progress.increment_completed();
        progress.increment_succeeded();
        progress.decrement_running();

        let snapshot = progress.snapshot();
        assert_eq!(snapshot.total, 10);
        assert_eq!(snapshot.submitted, 5);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.succeeded, 1);
        assert_eq!(snapshot.running, 1);
    }
}
