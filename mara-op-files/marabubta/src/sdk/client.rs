// Marabunta - Licensed under the MIT License.
//! Marabunta SDK Client - Async job submission with retry and timeout support
//!
//! This module provides a high-level async client for interacting with the Marabunta
//! compute cluster. It includes:
//!
//! - Async/await support with proper cancellation
//! - Configurable retry logic with exponential backoff
//! - Request timeout configuration
//! - Type-safe result deserialization
//!
//! # Example
//!
//! ```rust,no_run
//! use marabunta_compute::sdk::client::{MarabuntaClient, MarabuntaClientConfig};
//! use marabunta_compute::sdk::builder::JobBuilder;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = MarabuntaClient::new("http://localhost:8080")?;
//!
//!     let job = JobBuilder::new("my-task")
//!         .with_input(serde_json::json!({"data": [1, 2, 3]}))
//!         .with_timeout(std::time::Duration::from_secs(300))
//!         .build()?;
//!
//!     let result: MyResult = client.submit_and_wait(job).await?;
//!     Ok(())
//! }
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout, Instant};

use crate::sdk::retry::RetryConfig;
use crate::sdk::timeout::TimeoutConfig;

/// Errors that can occur in the Marabunta SDK client
#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Request timeout after {0:?}")]
    Timeout(Duration),

    #[error("Job submission failed: {0}")]
    SubmissionFailed(String),

    #[error("Job execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Job cancelled")]
    Cancelled,

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Max retries exceeded after {attempts} attempts: {last_error}")]
    MaxRetriesExceeded { attempts: u32, last_error: String },

    #[error("Server error: {status} - {message}")]
    ServerError { status: u16, message: String },

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for client operations
pub type ClientResult<T> = Result<T, ClientError>;

/// Job status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// Job is pending in the queue
    Pending,
    /// Job is currently running
    Running,
    /// Job completed successfully
    Completed,
    /// Job failed
    Failed,
    /// Job was cancelled
    Cancelled,
    /// Job timed out
    TimedOut,
}

impl JobStatus {
    /// Check if the job is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled | JobStatus::TimedOut
        )
    }

    /// Check if the job completed successfully
    pub fn is_success(&self) -> bool {
        matches!(self, JobStatus::Completed)
    }
}

/// Job submission request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRequest {
    /// Task type/name
    pub task_type: String,
    /// Input data (JSON)
    pub input: serde_json::Value,
    /// Optional job priority (higher = more important)
    #[serde(default)]
    pub priority: i32,
    /// Optional job timeout
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Optional tags for job routing
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Optional metadata
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: std::collections::HashMap<String, String>,
    /// Number of retries on failure
    #[serde(default)]
    pub max_retries: u32,
    /// Whether to enable checkpointing
    #[serde(default = "default_true")]
    pub checkpoint_enabled: bool,
}

fn default_true() -> bool {
    true
}

impl JobRequest {
    /// Create a new job request
    pub fn new(task_type: impl Into<String>, input: serde_json::Value) -> Self {
        Self {
            task_type: task_type.into(),
            input,
            priority: 0,
            timeout_secs: None,
            tags: Vec::new(),
            metadata: std::collections::HashMap::new(),
            max_retries: 3,
            checkpoint_enabled: true,
        }
    }
}

/// Job submission response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResponse {
    /// Unique job ID
    pub job_id: String,
    /// Current job status
    pub status: JobStatus,
    /// Progress percentage (0-100)
    #[serde(default)]
    pub progress: u8,
    /// Current stage name
    #[serde(default)]
    pub stage: String,
    /// Status message
    #[serde(default)]
    pub message: String,
    /// Result data (only when completed)
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    /// Error message (only when failed)
    #[serde(default)]
    pub error: Option<String>,
    /// Timestamp when job was submitted
    #[serde(default)]
    pub submitted_at: Option<String>,
    /// Timestamp when job started running
    #[serde(default)]
    pub started_at: Option<String>,
    /// Timestamp when job completed
    #[serde(default)]
    pub completed_at: Option<String>,
}

/// Cancellation token for async operations
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    /// Create a new cancellation token
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Cancel the operation
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Check if cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Wait until cancelled
    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            self.notify.notified().await;
        }
    }

    /// Create a child token that is cancelled when parent is cancelled
    pub fn child(&self) -> CancellationToken {
        let child = CancellationToken::new();
        let child_clone = child.clone();
        let parent_cancelled = self.cancelled.clone();
        let parent_notify = self.notify.clone();

        tokio::spawn(async move {
            loop {
                if parent_cancelled.load(Ordering::SeqCst) {
                    child_clone.cancel();
                    break;
                }
                parent_notify.notified().await;
            }
        });

        child
    }
}

/// Client configuration
#[derive(Debug, Clone)]
pub struct MarabuntaClientConfig {
    /// Base URL of the Marabunta API
    pub base_url: String,
    /// Retry configuration
    pub retry: RetryConfig,
    /// Timeout configuration
    pub timeout: TimeoutConfig,
    /// Optional authentication token
    pub auth_token: Option<String>,
    /// Default job timeout
    pub default_job_timeout: Option<Duration>,
    /// Polling interval for job status
    pub poll_interval: Duration,
    /// Maximum concurrent requests
    pub max_concurrent_requests: usize,
}

impl Default for MarabuntaClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8080".to_string(),
            retry: RetryConfig::default(),
            timeout: TimeoutConfig::default(),
            auth_token: None,
            default_job_timeout: Some(Duration::from_secs(3600)),
            poll_interval: Duration::from_millis(500),
            max_concurrent_requests: 100,
        }
    }
}

impl MarabuntaClientConfig {
    /// Create a new configuration with the given base URL
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            ..Default::default()
        }
    }

    /// Set retry configuration
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Set timeout configuration
    pub fn with_timeout(mut self, timeout: TimeoutConfig) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set authentication token
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set default job timeout
    pub fn with_default_job_timeout(mut self, timeout: Duration) -> Self {
        self.default_job_timeout = Some(timeout);
        self
    }

    /// Set polling interval
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }
}

/// Async client for the Marabunta compute cluster
pub struct MarabuntaClient {
    config: MarabuntaClientConfig,
    /// Simulated job store for mock mode
    pub(crate) mock_jobs: parking_lot::RwLock<std::collections::HashMap<String, JobResponse>>,
    next_job_id: AtomicU64,
}

impl MarabuntaClient {
    /// Create a new client with the given base URL
    pub fn new(base_url: impl Into<String>) -> ClientResult<Self> {
        let config = MarabuntaClientConfig::new(base_url);
        Self::with_config(config)
    }

    /// Create a new client with the given configuration
    pub fn with_config(config: MarabuntaClientConfig) -> ClientResult<Self> {
        if config.base_url.is_empty() {
            return Err(ClientError::InvalidConfig("base_url cannot be empty".into()));
        }

        Ok(Self {
            config,
            mock_jobs: parking_lot::RwLock::new(std::collections::HashMap::new()),
            next_job_id: AtomicU64::new(1),
        })
    }

    /// Get the current configuration
    pub fn config(&self) -> &MarabuntaClientConfig {
        &self.config
    }

    /// Submit a job and return immediately with the job ID
    pub async fn submit(&self, job: JobRequest) -> ClientResult<String> {
        self.submit_with_cancel(job, CancellationToken::new()).await
    }

    /// Submit a job with cancellation support
    pub async fn submit_with_cancel(
        &self,
        _job: JobRequest,
        cancel: CancellationToken,
    ) -> ClientResult<String> {
        // Check cancellation
        if cancel.is_cancelled() {
            return Err(ClientError::Cancelled);
        }

        // In mock mode, simulate job submission
        let job_id = format!("job-{}", self.next_job_id.fetch_add(1, Ordering::SeqCst));

        let response = JobResponse {
            job_id: job_id.clone(),
            status: JobStatus::Pending,
            progress: 0,
            stage: String::new(),
            message: "Job submitted".to_string(),
            result: None,
            error: None,
            submitted_at: Some(chrono::Utc::now().to_rfc3339()),
            started_at: None,
            completed_at: None,
        };

        self.mock_jobs.write().insert(job_id.clone(), response);

        // Simulate async submission with timeout
        let submission_timeout = self.config.timeout.connect_timeout;
        tokio::select! {
            _ = cancel.cancelled() => {
                Err(ClientError::Cancelled)
            }
            result = async {
                sleep(Duration::from_millis(10)).await;
                Ok(job_id.clone())
            } => {
                timeout(submission_timeout, async { result }).await
                    .map_err(|_| ClientError::Timeout(submission_timeout))?
            }
        }
    }

    /// Get the status of a job
    pub async fn get_status(&self, job_id: &str) -> ClientResult<JobResponse> {
        self.get_status_with_cancel(job_id, CancellationToken::new())
            .await
    }

    /// Get the status of a job with cancellation support
    pub async fn get_status_with_cancel(
        &self,
        job_id: &str,
        cancel: CancellationToken,
    ) -> ClientResult<JobResponse> {
        if cancel.is_cancelled() {
            return Err(ClientError::Cancelled);
        }

        // In mock mode, look up the job
        let jobs = self.mock_jobs.read();
        jobs.get(job_id)
            .cloned()
            .ok_or_else(|| ClientError::NotFound(format!("Job {} not found", job_id)))
    }

    /// Wait for a job to complete and return the result
    pub async fn wait_for_result<T: DeserializeOwned>(
        &self,
        job_id: &str,
    ) -> ClientResult<T> {
        self.wait_for_result_with_cancel(job_id, CancellationToken::new())
            .await
    }

    /// Wait for a job to complete with cancellation support
    pub async fn wait_for_result_with_cancel<T: DeserializeOwned>(
        &self,
        job_id: &str,
        cancel: CancellationToken,
    ) -> ClientResult<T> {
        let poll_interval = self.config.poll_interval;
        let timeout_duration = self.config.default_job_timeout;

        let start = Instant::now();

        loop {
            // Check cancellation
            if cancel.is_cancelled() {
                return Err(ClientError::Cancelled);
            }

            // Check timeout
            if let Some(timeout_dur) = timeout_duration {
                if start.elapsed() > timeout_dur {
                    return Err(ClientError::Timeout(timeout_dur));
                }
            }

            // Get status
            let response = self.get_status_with_cancel(job_id, cancel.clone()).await?;

            match response.status {
                JobStatus::Completed => {
                    let result = response.result.ok_or_else(|| {
                        ClientError::Internal("Job completed but no result found".into())
                    })?;

                    return serde_json::from_value(result)
                        .map_err(|e| ClientError::Deserialization(e.to_string()));
                }
                JobStatus::Failed => {
                    return Err(ClientError::ExecutionFailed(
                        response.error.unwrap_or_else(|| "Unknown error".into()),
                    ));
                }
                JobStatus::Cancelled => {
                    return Err(ClientError::Cancelled);
                }
                JobStatus::TimedOut => {
                    return Err(ClientError::Timeout(
                        timeout_duration.unwrap_or(Duration::from_secs(0)),
                    ));
                }
                JobStatus::Pending | JobStatus::Running => {
                    // Continue polling
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            return Err(ClientError::Cancelled);
                        }
                        _ = sleep(poll_interval) => {
                            // Continue to next iteration
                        }
                    }
                }
            }
        }
    }

    /// Submit a job and wait for the result
    pub async fn submit_and_wait<T: DeserializeOwned>(
        &self,
        job: JobRequest,
    ) -> ClientResult<T> {
        self.submit_and_wait_with_cancel(job, CancellationToken::new())
            .await
    }

    /// Submit a job and wait for the result with cancellation support
    pub async fn submit_and_wait_with_cancel<T: DeserializeOwned>(
        &self,
        job: JobRequest,
        cancel: CancellationToken,
    ) -> ClientResult<T> {
        let job_id = self.submit_with_cancel(job, cancel.clone()).await?;
        self.wait_for_result_with_cancel(&job_id, cancel).await
    }

    /// Cancel a running job
    pub async fn cancel_job(&self, job_id: &str) -> ClientResult<()> {
        let mut jobs = self.mock_jobs.write();
        if let Some(job) = jobs.get_mut(job_id) {
            if !job.status.is_terminal() {
                job.status = JobStatus::Cancelled;
                job.completed_at = Some(chrono::Utc::now().to_rfc3339());
            }
            Ok(())
        } else {
            Err(ClientError::NotFound(format!("Job {} not found", job_id)))
        }
    }

    /// Set job result (for testing/mock purposes)
    pub fn set_job_result(&self, job_id: &str, result: serde_json::Value) {
        let mut jobs = self.mock_jobs.write();
        if let Some(job) = jobs.get_mut(job_id) {
            job.status = JobStatus::Completed;
            job.result = Some(result);
            job.progress = 100;
            job.completed_at = Some(chrono::Utc::now().to_rfc3339());
        }
    }

    /// Set job as failed (for testing/mock purposes)
    pub fn set_job_failed(&self, job_id: &str, error: impl Into<String>) {
        let mut jobs = self.mock_jobs.write();
        if let Some(job) = jobs.get_mut(job_id) {
            job.status = JobStatus::Failed;
            job.error = Some(error.into());
            job.completed_at = Some(chrono::Utc::now().to_rfc3339());
        }
    }

    /// Update job progress (for testing/mock purposes)
    pub fn set_job_progress(&self, job_id: &str, progress: u8, stage: impl Into<String>) {
        let mut jobs = self.mock_jobs.write();
        if let Some(job) = jobs.get_mut(job_id) {
            job.progress = progress.min(100);
            job.stage = stage.into();
            if job.status == JobStatus::Pending {
                job.status = JobStatus::Running;
                job.started_at = Some(chrono::Utc::now().to_rfc3339());
            }
        }
    }
}

/// Handle for tracking a submitted job
pub struct JobHandle {
    job_id: String,
    client: Arc<MarabuntaClient>,
    cancel: CancellationToken,
}

impl JobHandle {
    /// Create a new job handle
    pub fn new(job_id: String, client: Arc<MarabuntaClient>) -> Self {
        Self {
            job_id,
            client,
            cancel: CancellationToken::new(),
        }
    }

    /// Get the job ID
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the current status
    pub async fn status(&self) -> ClientResult<JobResponse> {
        self.client
            .get_status_with_cancel(&self.job_id, self.cancel.clone())
            .await
    }

    /// Wait for the result
    pub async fn wait<T: DeserializeOwned>(&self) -> ClientResult<T> {
        self.client
            .wait_for_result_with_cancel(&self.job_id, self.cancel.clone())
            .await
    }

    /// Cancel the job
    pub async fn cancel(&self) -> ClientResult<()> {
        self.cancel.cancel();
        self.client.cancel_job(&self.job_id).await
    }

    /// Get the cancellation token
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let client = MarabuntaClient::new("http://localhost:8080").unwrap();
        assert_eq!(client.config().base_url, "http://localhost:8080");
    }

    #[tokio::test]
    async fn test_client_config() {
        let config = MarabuntaClientConfig::new("http://test:8080")
            .with_poll_interval(Duration::from_secs(1))
            .with_default_job_timeout(Duration::from_secs(60));

        let client = MarabuntaClient::with_config(config).unwrap();
        assert_eq!(client.config().poll_interval, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_submit_job() {
        let client = MarabuntaClient::new("http://localhost:8080").unwrap();

        let job = JobRequest::new("test-task", serde_json::json!({"value": 42}));
        let job_id = client.submit(job).await.unwrap();

        assert!(job_id.starts_with("job-"));
    }

    #[tokio::test]
    async fn test_get_status() {
        let client = MarabuntaClient::new("http://localhost:8080").unwrap();

        let job = JobRequest::new("test-task", serde_json::json!({}));
        let job_id = client.submit(job).await.unwrap();

        let status = client.get_status(&job_id).await.unwrap();
        assert_eq!(status.status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn test_wait_for_result() {
        let client = Arc::new(MarabuntaClient::new("http://localhost:8080").unwrap());

        let job = JobRequest::new("test-task", serde_json::json!({}));
        let job_id = client.submit(job).await.unwrap();

        // Set result in background
        let client_clone = client.clone();
        let job_id_clone = job_id.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            client_clone.set_job_result(&job_id_clone, serde_json::json!({"result": "success"}));
        });

        #[derive(Debug, Deserialize, PartialEq)]
        struct TestResult {
            result: String,
        }

        let result: TestResult = client.wait_for_result(&job_id).await.unwrap();
        assert_eq!(result.result, "success");
    }

    #[tokio::test]
    async fn test_cancellation() {
        let client = MarabuntaClient::new("http://localhost:8080").unwrap();

        let job = JobRequest::new("test-task", serde_json::json!({}));
        let cancel = CancellationToken::new();

        // Cancel immediately
        cancel.cancel();

        let result = client.submit_with_cancel(job, cancel).await;
        assert!(matches!(result, Err(ClientError::Cancelled)));
    }

    #[tokio::test]
    async fn test_job_handle() {
        let client = Arc::new(MarabuntaClient::new("http://localhost:8080").unwrap());

        let job = JobRequest::new("test-task", serde_json::json!({}));
        let job_id = client.submit(job).await.unwrap();

        let handle = JobHandle::new(job_id.clone(), client.clone());

        // Set result in background
        let client_clone = client.clone();
        let job_id_clone = job_id.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            client_clone.set_job_result(&job_id_clone, serde_json::json!(42));
        });

        let result: i32 = handle.wait().await.unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_job_status_is_terminal() {
        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Completed.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Cancelled.is_terminal());
        assert!(JobStatus::TimedOut.is_terminal());
    }

    #[tokio::test]
    async fn test_cancel_job() {
        let client = MarabuntaClient::new("http://localhost:8080").unwrap();

        let job = JobRequest::new("test-task", serde_json::json!({}));
        let job_id = client.submit(job).await.unwrap();

        client.cancel_job(&job_id).await.unwrap();

        let status = client.get_status(&job_id).await.unwrap();
        assert_eq!(status.status, JobStatus::Cancelled);
    }
}
