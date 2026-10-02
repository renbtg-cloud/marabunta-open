// Marabunta - Licensed under the MIT License.
//! Configuration types for connection pools
//!
//! This module provides configuration options for tuning pool behavior including
//! connection limits, timeouts, health check intervals, and naming.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for a connection pool
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use marabunta_compute::pool::PoolConfig;
///
/// let config = PoolConfig::default()
///     .with_min_connections(5)
///     .with_max_connections(20)
///     .with_idle_timeout(Duration::from_secs(300))
///     .with_max_lifetime(Duration::from_secs(3600))
///     .with_acquire_timeout(Duration::from_secs(30));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain in the pool
    ///
    /// The pool will try to maintain at least this many idle connections.
    /// Default: 1
    pub min_connections: usize,

    /// Maximum number of connections allowed in the pool
    ///
    /// The pool will not create more than this many connections total.
    /// Default: 10
    pub max_connections: usize,

    /// Maximum time a connection can be idle before being closed
    ///
    /// Connections that have been idle for longer than this duration
    /// will be closed and removed from the pool.
    /// Default: 10 minutes
    pub idle_timeout: Duration,

    /// Maximum lifetime of a connection
    ///
    /// Connections older than this will be closed even if healthy.
    /// This helps prevent issues from long-lived connections.
    /// Default: 30 minutes
    pub max_lifetime: Duration,

    /// Maximum time to wait for a connection when the pool is exhausted
    ///
    /// If no connection becomes available within this time, an error is returned.
    /// Default: 30 seconds
    pub acquire_timeout: Duration,

    /// Interval between health checks for idle connections
    ///
    /// The pool will periodically check the health of idle connections
    /// and remove any that fail the health check.
    /// Default: 30 seconds
    pub health_check_interval: Duration,

    /// Whether to validate connections on acquire
    ///
    /// If true, connections will be validated before being handed out.
    /// This adds latency but ensures connections are healthy.
    /// Default: true
    pub validate_on_acquire: bool,

    /// Name of the pool for metrics and logging
    ///
    /// Default: "default"
    pub pool_name: String,

    /// Whether to enable metrics collection
    ///
    /// Default: true
    pub enable_metrics: bool,

    /// Number of connection creation retries before giving up
    ///
    /// Default: 3
    pub connection_retries: usize,

    /// Delay between connection creation retries
    ///
    /// Default: 100ms
    pub retry_delay: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            idle_timeout: Duration::from_secs(600),        // 10 minutes
            max_lifetime: Duration::from_secs(1800),       // 30 minutes
            acquire_timeout: Duration::from_secs(30),      // 30 seconds
            health_check_interval: Duration::from_secs(30), // 30 seconds
            validate_on_acquire: true,
            pool_name: "default".to_string(),
            enable_metrics: true,
            connection_retries: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

impl PoolConfig {
    /// Create a new pool configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the minimum number of connections
    pub fn with_min_connections(mut self, min: usize) -> Self {
        self.min_connections = min;
        self
    }

    /// Set the maximum number of connections
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set the idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set the maximum connection lifetime
    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Set the acquire timeout
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set the health check interval
    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Enable or disable validation on acquire
    pub fn with_validate_on_acquire(mut self, validate: bool) -> Self {
        self.validate_on_acquire = validate;
        self
    }

    /// Set the pool name
    pub fn with_pool_name(mut self, name: impl Into<String>) -> Self {
        self.pool_name = name.into();
        self
    }

    /// Enable or disable metrics
    pub fn with_metrics(mut self, enable: bool) -> Self {
        self.enable_metrics = enable;
        self
    }

    /// Set the number of connection retries
    pub fn with_connection_retries(mut self, retries: usize) -> Self {
        self.connection_retries = retries;
        self
    }

    /// Set the retry delay
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.min_connections > self.max_connections {
            return Err(format!(
                "min_connections ({}) cannot be greater than max_connections ({})",
                self.min_connections, self.max_connections
            ));
        }

        if self.max_connections == 0 {
            return Err("max_connections must be at least 1".to_string());
        }

        if self.acquire_timeout.is_zero() {
            return Err("acquire_timeout must be greater than 0".to_string());
        }

        if self.idle_timeout > self.max_lifetime {
            return Err(format!(
                "idle_timeout ({:?}) should not be greater than max_lifetime ({:?})",
                self.idle_timeout, self.max_lifetime
            ));
        }

        Ok(())
    }

    /// Create a configuration optimized for high throughput
    ///
    /// Uses larger pool sizes and shorter timeouts.
    pub fn high_throughput() -> Self {
        Self {
            min_connections: 10,
            max_connections: 50,
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(1800),
            acquire_timeout: Duration::from_secs(5),
            health_check_interval: Duration::from_secs(10),
            validate_on_acquire: false,
            pool_name: "high_throughput".to_string(),
            enable_metrics: true,
            connection_retries: 2,
            retry_delay: Duration::from_millis(50),
        }
    }

    /// Create a configuration optimized for reliability
    ///
    /// Uses aggressive health checking and validation.
    pub fn reliable() -> Self {
        Self {
            min_connections: 2,
            max_connections: 10,
            idle_timeout: Duration::from_secs(60),
            max_lifetime: Duration::from_secs(300),
            acquire_timeout: Duration::from_secs(60),
            health_check_interval: Duration::from_secs(5),
            validate_on_acquire: true,
            pool_name: "reliable".to_string(),
            enable_metrics: true,
            connection_retries: 5,
            retry_delay: Duration::from_millis(200),
        }
    }

    /// Create a configuration for testing
    ///
    /// Uses small pool sizes and short timeouts for fast test execution.
    pub fn for_testing() -> Self {
        Self {
            min_connections: 1,
            max_connections: 3,
            idle_timeout: Duration::from_secs(5),
            max_lifetime: Duration::from_secs(10),
            acquire_timeout: Duration::from_millis(500),
            health_check_interval: Duration::from_millis(100),
            validate_on_acquire: true,
            pool_name: "test".to_string(),
            enable_metrics: false,
            connection_retries: 1,
            retry_delay: Duration::from_millis(10),
        }
    }
}

/// Builder for PoolConfig with validation
#[derive(Debug, Default)]
pub struct PoolConfigBuilder {
    config: PoolConfig,
    errors: Vec<String>,
}

impl PoolConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            config: PoolConfig::default(),
            errors: Vec::new(),
        }
    }

    /// Set the minimum number of connections
    pub fn min_connections(mut self, min: usize) -> Self {
        self.config.min_connections = min;
        self
    }

    /// Set the maximum number of connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.config.max_connections = max;
        self
    }

    /// Set the idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.config.idle_timeout = timeout;
        self
    }

    /// Set the maximum connection lifetime
    pub fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.config.max_lifetime = lifetime;
        self
    }

    /// Set the acquire timeout
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.config.acquire_timeout = timeout;
        self
    }

    /// Set the health check interval
    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.config.health_check_interval = interval;
        self
    }

    /// Enable or disable validation on acquire
    pub fn validate_on_acquire(mut self, validate: bool) -> Self {
        self.config.validate_on_acquire = validate;
        self
    }

    /// Set the pool name
    pub fn pool_name(mut self, name: impl Into<String>) -> Self {
        self.config.pool_name = name.into();
        self
    }

    /// Enable or disable metrics
    pub fn enable_metrics(mut self, enable: bool) -> Self {
        self.config.enable_metrics = enable;
        self
    }

    /// Build the configuration, validating all settings
    pub fn build(self) -> Result<PoolConfig, String> {
        self.config.validate()?;
        Ok(self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PoolConfig::default();
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.max_connections, 10);
        assert!(config.validate_on_acquire);
        assert!(config.enable_metrics);
    }

    #[test]
    fn test_builder_chain() {
        let config = PoolConfig::new()
            .with_min_connections(5)
            .with_max_connections(20)
            .with_pool_name("test");

        assert_eq!(config.min_connections, 5);
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.pool_name, "test");
    }

    #[test]
    fn test_validation_min_max() {
        let config = PoolConfig::default()
            .with_min_connections(10)
            .with_max_connections(5);

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("min_connections"));
    }

    #[test]
    fn test_validation_zero_max() {
        let config = PoolConfig::default().with_max_connections(0);
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_zero_timeout() {
        let config = PoolConfig::default().with_acquire_timeout(Duration::ZERO);
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_presets() {
        let high = PoolConfig::high_throughput();
        assert!(high.max_connections > 10);
        assert!(!high.validate_on_acquire);

        let reliable = PoolConfig::reliable();
        assert!(reliable.validate_on_acquire);
        assert!(reliable.health_check_interval < Duration::from_secs(10));

        let test = PoolConfig::for_testing();
        assert!(test.max_connections <= 5);
        assert!(!test.enable_metrics);
    }

    #[test]
    fn test_builder_build() {
        let result = PoolConfigBuilder::new()
            .min_connections(2)
            .max_connections(10)
            .pool_name("my_pool")
            .build();

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.min_connections, 2);
        assert_eq!(config.pool_name, "my_pool");
    }

    #[test]
    fn test_builder_validation_error() {
        let result = PoolConfigBuilder::new()
            .min_connections(20)
            .max_connections(5)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = PoolConfig::default().with_pool_name("serialized");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PoolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.pool_name, "serialized");
    }
}
