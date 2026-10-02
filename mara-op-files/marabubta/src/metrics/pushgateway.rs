// Marabunta - Licensed under the MIT License.
//! Prometheus Pushgateway client for metric push
//!
//! Implements push support for Prometheus Pushgateway and compatible endpoints.
//! Supports both push (replace) and pushAdd (append) modes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::interval;

/// Configuration for the pushgateway client
#[derive(Debug, Clone)]
pub struct PushgatewayConfig {
    /// URL of the pushgateway (e.g., "http://localhost:9091")
    pub url: String,
    /// Job name for grouping metrics
    pub job: String,
    /// Instance name (optional)
    pub instance: Option<String>,
    /// Additional grouping labels
    pub grouping: HashMap<String, String>,
    /// Push interval
    pub push_interval: Duration,
    /// HTTP timeout
    pub timeout: Duration,
    /// Whether to use push (replace) or pushAdd (append)
    pub replace_metrics: bool,
    /// Retry configuration
    pub retry: RetryConfig,
    /// Basic auth credentials (optional)
    pub basic_auth: Option<(String, String)>,
}

impl Default for PushgatewayConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:9091".to_string(),
            job: "marabunta_compute".to_string(),
            instance: None,
            grouping: HashMap::new(),
            push_interval: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
            replace_metrics: true,
            retry: RetryConfig::default(),
            basic_auth: None,
        }
    }
}

impl PushgatewayConfig {
    /// Create with custom URL and job name
    pub fn new(url: impl Into<String>, job: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            job: job.into(),
            ..Default::default()
        }
    }

    /// Set the instance name
    pub fn instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    /// Add a grouping label
    pub fn grouping(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.grouping.insert(key.into(), value.into());
        self
    }

    /// Set push interval
    pub fn push_interval(mut self, interval: Duration) -> Self {
        self.push_interval = interval;
        self
    }

    /// Use pushAdd mode (append instead of replace)
    pub fn append_mode(mut self) -> Self {
        self.replace_metrics = false;
        self
    }

    /// Set basic auth
    pub fn basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.basic_auth = Some((username.into(), password.into()));
        self
    }

    /// Build the push URL with job and grouping labels
    pub fn push_url(&self) -> String {
        let mut url = format!("{}/metrics/job/{}", self.url.trim_end_matches('/'), url_encode(&self.job));

        if let Some(ref instance) = self.instance {
            url.push_str(&format!("/instance/{}", url_encode(instance)));
        }

        for (key, value) in &self.grouping {
            url.push_str(&format!("/{}/{}", url_encode(key), url_encode(value)));
        }

        url
    }
}

/// Retry configuration for push failures
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries
    pub max_retries: u32,
    /// Initial retry delay
    pub initial_delay: Duration,
    /// Maximum retry delay
    pub max_delay: Duration,
    /// Backoff multiplier
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    /// Calculate delay for a specific retry attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let delay_ms = self.initial_delay.as_millis() as f64 * self.multiplier.powi(attempt as i32);
        let delay = Duration::from_millis(delay_ms as u64);
        delay.min(self.max_delay)
    }
}

/// URL encode a string for use in pushgateway URLs
fn url_encode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}

/// Result of a push operation
#[derive(Debug, Clone)]
pub struct PushResult {
    /// Whether the push succeeded
    pub success: bool,
    /// HTTP status code (if available)
    pub status_code: Option<u16>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Number of retries attempted
    pub retries: u32,
    /// Duration of the push operation
    pub duration: Duration,
}

impl PushResult {
    fn success(status_code: u16, duration: Duration, retries: u32) -> Self {
        Self {
            success: true,
            status_code: Some(status_code),
            error: None,
            retries,
            duration,
        }
    }

    fn failure(error: String, status_code: Option<u16>, duration: Duration, retries: u32) -> Self {
        Self {
            success: false,
            status_code,
            error: Some(error),
            retries,
            duration,
        }
    }
}

/// Pushgateway client for pushing metrics
#[derive(Debug)]
pub struct PushgatewayClient {
    config: PushgatewayConfig,
    http_client: reqwest::Client,
}

impl PushgatewayClient {
    /// Create a new client with the given configuration
    pub fn new(config: PushgatewayConfig) -> Result<Self, PushgatewayError> {
        let http_client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| PushgatewayError::ClientInit(e.to_string()))?;

        Ok(Self {
            config,
            http_client,
        })
    }

    /// Push metrics to the pushgateway
    pub async fn push(&self, metrics: &str) -> PushResult {
        let start = std::time::Instant::now();
        let url = self.config.push_url();
        let mut retries = 0u32;

        loop {
            let result = self.do_push(&url, metrics).await;

            match result {
                Ok(status) if status.is_success() => {
                    return PushResult::success(status.as_u16(), start.elapsed(), retries);
                }
                Ok(status) => {
                    // Non-success status but request completed
                    if retries >= self.config.retry.max_retries {
                        return PushResult::failure(
                            format!("HTTP {}", status.as_u16()),
                            Some(status.as_u16()),
                            start.elapsed(),
                            retries,
                        );
                    }
                }
                Err(e) => {
                    if retries >= self.config.retry.max_retries {
                        return PushResult::failure(e.to_string(), None, start.elapsed(), retries);
                    }
                }
            }

            // Retry with backoff
            let delay = self.config.retry.delay_for_attempt(retries);
            tokio::time::sleep(delay).await;
            retries += 1;
        }
    }

    async fn do_push(&self, url: &str, metrics: &str) -> Result<reqwest::StatusCode, reqwest::Error> {
        let method = if self.config.replace_metrics {
            reqwest::Method::PUT
        } else {
            reqwest::Method::POST
        };

        let mut request = self
            .http_client
            .request(method, url)
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(metrics.to_string());

        if let Some((ref username, ref password)) = self.config.basic_auth {
            request = request.basic_auth(username, Some(password));
        }

        let response = request.send().await?;
        Ok(response.status())
    }

    /// Delete metrics from the pushgateway
    pub async fn delete(&self) -> PushResult {
        let start = std::time::Instant::now();
        let url = self.config.push_url();

        match self.http_client.delete(&url).send().await {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    PushResult::success(status.as_u16(), start.elapsed(), 0)
                } else {
                    PushResult::failure(
                        format!("HTTP {}", status.as_u16()),
                        Some(status.as_u16()),
                        start.elapsed(),
                        0,
                    )
                }
            }
            Err(e) => PushResult::failure(e.to_string(), None, start.elapsed(), 0),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &PushgatewayConfig {
        &self.config
    }
}

/// Error types for pushgateway operations
#[derive(Debug, Clone)]
pub enum PushgatewayError {
    /// Failed to initialize HTTP client
    ClientInit(String),
    /// Push failed after retries
    PushFailed(String),
    /// Invalid configuration
    InvalidConfig(String),
}

impl std::fmt::Display for PushgatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClientInit(e) => write!(f, "Failed to initialize HTTP client: {}", e),
            Self::PushFailed(e) => write!(f, "Push failed: {}", e),
            Self::InvalidConfig(e) => write!(f, "Invalid configuration: {}", e),
        }
    }
}

impl std::error::Error for PushgatewayError {}

/// Metric source trait for providing metrics to push
pub trait MetricSource: Send + Sync {
    /// Encode metrics in Prometheus text format
    fn encode(&self) -> Result<String, String>;
}

/// Background push service that periodically pushes metrics
pub struct PushService {
    client: Arc<PushgatewayClient>,
    source: Arc<dyn MetricSource>,
    config: PushgatewayConfig,
    stats: Arc<RwLock<PushStats>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl PushService {
    /// Create a new push service
    pub fn new(
        config: PushgatewayConfig,
        source: Arc<dyn MetricSource>,
    ) -> Result<Self, PushgatewayError> {
        let client = Arc::new(PushgatewayClient::new(config.clone())?);

        Ok(Self {
            client,
            source,
            config,
            stats: Arc::new(RwLock::new(PushStats::default())),
            shutdown_tx: None,
        })
    }

    /// Start the background push loop
    pub fn start(&mut self) -> tokio::task::JoinHandle<()> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let client = self.client.clone();
        let source = self.source.clone();
        let stats = self.stats.clone();
        let push_interval = self.config.push_interval;

        tokio::spawn(async move {
            let mut interval = interval(push_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::do_push_cycle(&client, &source, &stats).await;
                    }
                    _ = shutdown_rx.recv() => {
                        // Final push before shutdown
                        Self::do_push_cycle(&client, &source, &stats).await;
                        break;
                    }
                }
            }
        })
    }

    async fn do_push_cycle(
        client: &PushgatewayClient,
        source: &Arc<dyn MetricSource>,
        stats: &Arc<RwLock<PushStats>>,
    ) {
        let encode_result = source.encode();

        match encode_result {
            Ok(metrics) => {
                let result = client.push(&metrics).await;
                let mut stats = stats.write();

                if result.success {
                    stats.successful_pushes += 1;
                    stats.last_success = Some(std::time::Instant::now());
                    stats.consecutive_failures = 0;
                } else {
                    stats.failed_pushes += 1;
                    stats.last_failure = Some(std::time::Instant::now());
                    stats.last_error = result.error;
                    stats.consecutive_failures += 1;
                }

                stats.total_retries += result.retries as u64;
                stats.last_push_duration = Some(result.duration);
            }
            Err(e) => {
                let mut stats = stats.write();
                stats.failed_pushes += 1;
                stats.last_failure = Some(std::time::Instant::now());
                stats.last_error = Some(format!("Encode error: {}", e));
                stats.consecutive_failures += 1;
            }
        }
    }

    /// Stop the background push loop
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Get push statistics
    pub fn stats(&self) -> PushStats {
        self.stats.read().clone()
    }

    /// Trigger an immediate push
    pub async fn push_now(&self) -> PushResult {
        match self.source.encode() {
            Ok(metrics) => {
                let result = self.client.push(&metrics).await;
                let mut stats = self.stats.write();

                if result.success {
                    stats.successful_pushes += 1;
                    stats.last_success = Some(std::time::Instant::now());
                    stats.consecutive_failures = 0;
                } else {
                    stats.failed_pushes += 1;
                    stats.last_failure = Some(std::time::Instant::now());
                    stats.last_error = result.error.clone();
                    stats.consecutive_failures += 1;
                }

                result
            }
            Err(e) => PushResult::failure(
                format!("Encode error: {}", e),
                None,
                Duration::ZERO,
                0,
            ),
        }
    }
}

/// Statistics about push operations
#[derive(Debug, Clone, Default)]
pub struct PushStats {
    /// Number of successful pushes
    pub successful_pushes: u64,
    /// Number of failed pushes
    pub failed_pushes: u64,
    /// Total retries across all pushes
    pub total_retries: u64,
    /// Last successful push time
    pub last_success: Option<std::time::Instant>,
    /// Last failed push time
    pub last_failure: Option<std::time::Instant>,
    /// Last error message
    pub last_error: Option<String>,
    /// Duration of last push
    pub last_push_duration: Option<Duration>,
    /// Consecutive failures
    pub consecutive_failures: u32,
}

impl PushStats {
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.successful_pushes + self.failed_pushes;
        if total == 0 {
            1.0
        } else {
            self.successful_pushes as f64 / total as f64
        }
    }

    /// Check if push service is healthy
    pub fn is_healthy(&self) -> bool {
        self.consecutive_failures < 3
    }
}

/// Simple metric source that wraps a closure
pub struct ClosureMetricSource<F>
where
    F: Fn() -> Result<String, String> + Send + Sync,
{
    encode_fn: F,
}

impl<F> ClosureMetricSource<F>
where
    F: Fn() -> Result<String, String> + Send + Sync,
{
    /// Create a new closure-based metric source
    pub fn new(encode_fn: F) -> Self {
        Self { encode_fn }
    }
}

impl<F> MetricSource for ClosureMetricSource<F>
where
    F: Fn() -> Result<String, String> + Send + Sync,
{
    fn encode(&self) -> Result<String, String> {
        (self.encode_fn)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_push_url() {
        let config = PushgatewayConfig::new("http://localhost:9091", "my_job")
            .instance("server1")
            .grouping("env", "production")
            .grouping("region", "us-west");

        let url = config.push_url();
        assert!(url.starts_with("http://localhost:9091/metrics/job/my_job"));
        assert!(url.contains("/instance/server1"));
        assert!(url.contains("/env/production"));
        assert!(url.contains("/region/us-west"));
    }

    #[test]
    fn test_url_encoding() {
        assert_eq!(url_encode("simple"), "simple");
        assert_eq!(url_encode("with space"), "with%20space");
        assert_eq!(url_encode("with/slash"), "with%2Fslash");
    }

    #[test]
    fn test_retry_config_delay() {
        let config = RetryConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            multiplier: 2.0,
            max_retries: 5,
        };

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(800));
        // Should be capped at max_delay
        assert_eq!(config.delay_for_attempt(4), Duration::from_secs(1));
    }

    #[test]
    fn test_push_stats() {
        let mut stats = PushStats::default();
        assert!((stats.success_rate() - 1.0).abs() < 0.001);
        assert!(stats.is_healthy());

        stats.successful_pushes = 90;
        stats.failed_pushes = 10;
        assert!((stats.success_rate() - 0.9).abs() < 0.001);

        stats.consecutive_failures = 5;
        assert!(!stats.is_healthy());
    }

    #[test]
    fn test_push_result() {
        let success = PushResult::success(200, Duration::from_millis(50), 0);
        assert!(success.success);
        assert_eq!(success.status_code, Some(200));
        assert!(success.error.is_none());

        let failure = PushResult::failure(
            "Connection refused".to_string(),
            None,
            Duration::from_millis(100),
            3,
        );
        assert!(!failure.success);
        assert!(failure.status_code.is_none());
        assert_eq!(failure.error, Some("Connection refused".to_string()));
        assert_eq!(failure.retries, 3);
    }

    #[test]
    fn test_closure_metric_source() {
        let source = ClosureMetricSource::new(|| {
            Ok("# HELP test_metric A test\n# TYPE test_metric gauge\ntest_metric 42\n".to_string())
        });

        let result = source.encode();
        assert!(result.is_ok());
        assert!(result.unwrap().contains("test_metric 42"));
    }

    #[tokio::test]
    async fn test_pushgateway_client_creation() {
        let config = PushgatewayConfig::new("http://localhost:9091", "test_job");
        let client = PushgatewayClient::new(config);
        assert!(client.is_ok());
    }
}
