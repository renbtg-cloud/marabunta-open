// Marabunta - Licensed under the MIT License.
//! HTTP client connection pool
//!
//! This module provides a connection pool specifically for HTTP clients,
//! enabling efficient reuse of HTTP connections and connection keep-alive.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::{Client, ClientBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use super::config::PoolConfig;
use super::connection::ConnectionFactory;
use super::errors::{PoolError, PoolResult};
use super::health::HealthCheck;
use super::pool::Pool;

/// Configuration for HTTP client connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpClientConfig {
    /// Base URL for the HTTP client (optional)
    pub base_url: Option<String>,
    /// Request timeout
    pub timeout: Duration,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Whether to follow redirects
    pub follow_redirects: bool,
    /// Maximum number of redirects to follow
    pub max_redirects: usize,
    /// User-Agent header
    pub user_agent: String,
    /// Whether to enable cookies
    pub enable_cookies: bool,
    /// Whether to trust system certificates
    pub use_system_certs: bool,
    /// Health check endpoint (relative to base_url)
    pub health_check_path: Option<String>,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            base_url: None,
            timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            follow_redirects: true,
            max_redirects: 10,
            user_agent: format!("marabunta-compute/{}", env!("CARGO_PKG_VERSION")),
            enable_cookies: false,
            use_system_certs: true,
            health_check_path: Some("/health".to_string()),
        }
    }
}

impl HttpClientConfig {
    /// Create a new HTTP client configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the base URL
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    /// Set the request timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Enable or disable redirect following
    pub fn with_follow_redirects(mut self, follow: bool) -> Self {
        self.follow_redirects = follow;
        self
    }

    /// Set the user agent
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    /// Set the health check path
    pub fn with_health_check_path(mut self, path: impl Into<String>) -> Self {
        self.health_check_path = Some(path.into());
        self
    }

    /// Disable health check
    pub fn without_health_check(mut self) -> Self {
        self.health_check_path = None;
        self
    }

    /// Build a reqwest Client from this configuration
    pub fn build_client(&self) -> PoolResult<Client> {
        let mut builder = ClientBuilder::new()
            .timeout(self.timeout)
            .connect_timeout(self.connect_timeout)
            .user_agent(&self.user_agent)
            .pool_max_idle_per_host(1); // We manage pooling ourselves

        if self.follow_redirects {
            builder = builder.redirect(reqwest::redirect::Policy::limited(self.max_redirects));
        } else {
            builder = builder.redirect(reqwest::redirect::Policy::none());
        }

        // Note: cookie_store requires the 'cookies' feature in reqwest
        // Skipping cookie configuration when feature is not enabled
        let _ = self.enable_cookies;

        builder
            .build()
            .map_err(|e| PoolError::HttpClient(format!("Failed to build HTTP client: {}", e)))
    }
}

/// A pooled HTTP client connection
#[derive(Debug)]
pub struct HttpClientConnection {
    /// The underlying HTTP client
    client: Client,
    /// Base URL for requests
    base_url: Option<String>,
    /// Configuration used to create this connection
    config: HttpClientConfig,
    /// Connection ID for tracking
    id: u64,
    /// Whether the connection is healthy
    healthy: AtomicBool,
    /// When the connection was created
    created_at: Instant,
    /// Number of requests made with this connection
    request_count: AtomicU64,
}

impl HttpClientConnection {
    /// Create a new HTTP client connection
    fn new(client: Client, config: HttpClientConfig, id: u64) -> Self {
        Self {
            client,
            base_url: config.base_url.clone(),
            config,
            id,
            healthy: AtomicBool::new(true),
            created_at: Instant::now(),
            request_count: AtomicU64::new(0),
        }
    }

    /// Get the underlying HTTP client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get the base URL
    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    /// Build a full URL from a path
    pub fn url(&self, path: &str) -> String {
        match &self.base_url {
            Some(base) => {
                if path.starts_with('/') {
                    format!("{}{}", base.trim_end_matches('/'), path)
                } else {
                    format!("{}/{}", base.trim_end_matches('/'), path)
                }
            }
            None => path.to_string(),
        }
    }

    /// Get the connection ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the connection age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get the number of requests made
    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::Relaxed)
    }

    /// Increment the request count
    pub fn increment_request_count(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if the connection is healthy
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    /// Mark the connection as unhealthy
    pub fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::SeqCst);
    }

    /// Make a GET request
    pub async fn get(&self, path: &str) -> Result<reqwest::Response, reqwest::Error> {
        self.increment_request_count();
        self.client.get(self.url(path)).send().await
    }

    /// Make a POST request with JSON body
    pub async fn post_json<T: Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.increment_request_count();
        self.client.post(self.url(path)).json(body).send().await
    }

    /// Make a PUT request with JSON body
    pub async fn put_json<T: Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.increment_request_count();
        self.client.put(self.url(path)).json(body).send().await
    }

    /// Make a DELETE request
    pub async fn delete(&self, path: &str) -> Result<reqwest::Response, reqwest::Error> {
        self.increment_request_count();
        self.client.delete(self.url(path)).send().await
    }
}

/// Factory for creating HTTP client connections
pub struct HttpClientFactory {
    config: HttpClientConfig,
    counter: AtomicU64,
}

impl HttpClientFactory {
    /// Create a new factory with the given configuration
    pub fn new(config: HttpClientConfig) -> Self {
        Self {
            config,
            counter: AtomicU64::new(0),
        }
    }

    /// Create a factory with default configuration
    pub fn default_config() -> Self {
        Self::new(HttpClientConfig::default())
    }

    /// Create a factory with a base URL
    pub fn with_base_url(url: impl Into<String>) -> Self {
        Self::new(HttpClientConfig::default().with_base_url(url))
    }
}

#[async_trait]
impl ConnectionFactory<HttpClientConnection> for HttpClientFactory {
    type Error = PoolError;

    async fn create(&self) -> Result<HttpClientConnection, Self::Error> {
        let client = self.config.build_client()?;
        let id = self.counter.fetch_add(1, Ordering::SeqCst);

        debug!(conn_id = id, "Created new HTTP client connection");

        Ok(HttpClientConnection::new(client, self.config.clone(), id))
    }
}

#[async_trait]
impl HealthCheck<HttpClientConnection> for HttpClientFactory {
    async fn check(&self, conn: &HttpClientConnection) -> bool {
        // Basic check: is the connection marked healthy?
        if !conn.is_healthy() {
            return false;
        }

        // If there's a health check path, try to hit it
        if let Some(health_path) = &self.config.health_check_path {
            if conn.base_url.is_some() {
                match tokio::time::timeout(Duration::from_secs(5), conn.get(health_path)).await {
                    Ok(Ok(response)) => {
                        let healthy = response.status().is_success()
                            || response.status() == StatusCode::NOT_FOUND; // 404 means server is responding
                        if !healthy {
                            trace!(
                                conn_id = conn.id,
                                status = %response.status(),
                                "Health check returned non-success status"
                            );
                        }
                        return healthy;
                    }
                    Ok(Err(e)) => {
                        trace!(conn_id = conn.id, error = %e, "Health check request failed");
                        return false;
                    }
                    Err(_) => {
                        trace!(conn_id = conn.id, "Health check timed out");
                        return false;
                    }
                }
            }
        }

        // No health check configured or no base URL, assume healthy
        true
    }

    fn description(&self) -> &str {
        "HTTP client health check"
    }
}

/// Type alias for an HTTP client pool
pub type HttpClientPool = Pool<HttpClientConnection>;

/// Create a new HTTP client pool with default configuration
pub fn create_http_pool(base_url: Option<&str>) -> PoolResult<Arc<HttpClientPool>> {
    let config = match base_url {
        Some(url) => HttpClientConfig::default().with_base_url(url),
        None => HttpClientConfig::default().without_health_check(),
    };

    let pool_config = PoolConfig::default()
        .with_min_connections(1)
        .with_max_connections(10)
        .with_pool_name("http_client");

    let factory = Arc::new(HttpClientFactory::new(config));
    HttpClientPool::new(pool_config, factory)
}

/// Create a new HTTP client pool with custom configuration
pub fn create_http_pool_with_config(
    http_config: HttpClientConfig,
    pool_config: PoolConfig,
) -> PoolResult<Arc<HttpClientPool>> {
    let factory = Arc::new(HttpClientFactory::new(http_config));
    HttpClientPool::new(pool_config, factory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_config_default() {
        let config = HttpClientConfig::default();
        assert!(config.follow_redirects);
        assert!(!config.enable_cookies);
        assert!(config.health_check_path.is_some());
    }

    #[test]
    fn test_http_client_config_builder() {
        let config = HttpClientConfig::new()
            .with_base_url("http://localhost:8080")
            .with_timeout(Duration::from_secs(60))
            .with_user_agent("test-agent/1.0");

        assert_eq!(config.base_url, Some("http://localhost:8080".to_string()));
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.user_agent, "test-agent/1.0");
    }

    #[test]
    fn test_http_client_config_build_client() {
        let config = HttpClientConfig::default();
        let result = config.build_client();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_http_connection_url_building() {
        let config = HttpClientConfig::new().with_base_url("http://localhost:8080");
        let client = config.build_client().unwrap();
        let conn = HttpClientConnection::new(client, config, 1);

        assert_eq!(conn.url("/api/test"), "http://localhost:8080/api/test");
        assert_eq!(conn.url("api/test"), "http://localhost:8080/api/test");
    }

    #[tokio::test]
    async fn test_http_connection_no_base_url() {
        let config = HttpClientConfig::new();
        let client = config.build_client().unwrap();
        let conn = HttpClientConnection::new(client, config, 1);

        assert_eq!(
            conn.url("http://example.com/api/test"),
            "http://example.com/api/test"
        );
    }

    #[tokio::test]
    async fn test_http_factory_create() {
        let factory = HttpClientFactory::default_config();
        let conn = factory.create().await.unwrap();

        assert_eq!(conn.id(), 0);
        assert!(conn.is_healthy());
        assert_eq!(conn.request_count(), 0);
    }

    #[tokio::test]
    async fn test_http_factory_health_check_no_base_url() {
        let factory = HttpClientFactory::default_config();
        let conn = factory.create().await.unwrap();

        // Should return true when no base URL (can't perform health check)
        let is_healthy = factory.check(&conn).await;
        assert!(is_healthy);
    }

    #[tokio::test]
    async fn test_http_factory_health_check_unhealthy_marker() {
        let factory = HttpClientFactory::default_config();
        let conn = factory.create().await.unwrap();

        // Mark as unhealthy
        conn.mark_unhealthy();

        let is_healthy = factory.check(&conn).await;
        assert!(!is_healthy);
    }

    #[tokio::test]
    async fn test_http_connection_request_count() {
        let config = HttpClientConfig::new();
        let client = config.build_client().unwrap();
        let conn = HttpClientConnection::new(client, config, 1);

        assert_eq!(conn.request_count(), 0);
        conn.increment_request_count();
        assert_eq!(conn.request_count(), 1);
        conn.increment_request_count();
        assert_eq!(conn.request_count(), 2);
    }

    #[tokio::test]
    async fn test_create_http_pool_no_base_url() {
        let pool = create_http_pool(None);
        assert!(pool.is_ok());
    }

    #[test]
    fn test_http_config_serialization() {
        let config = HttpClientConfig::new()
            .with_base_url("http://localhost")
            .with_timeout(Duration::from_secs(30));

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HttpClientConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.base_url, config.base_url);
        assert_eq!(deserialized.timeout, config.timeout);
    }
}
