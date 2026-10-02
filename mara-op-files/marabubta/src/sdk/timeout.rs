// Marabunta - Licensed under the MIT License.
//! Request timeout configuration for the Marabunta SDK
//!
//! This module provides configurable timeouts for various operations:
//!
//! - Connect timeout: Time to establish connection
//! - Read timeout: Time to receive response
//! - Write timeout: Time to send request
//! - Total timeout: Overall operation timeout
//!
//! # Example
//!
//! ```rust
//! use marabunta_compute::sdk::timeout::TimeoutConfig;
//! use std::time::Duration;
//!
//! let config = TimeoutConfig::new()
//!     .with_connect_timeout(Duration::from_secs(5))
//!     .with_read_timeout(Duration::from_secs(30))
//!     .with_write_timeout(Duration::from_secs(10))
//!     .with_total_timeout(Duration::from_secs(60));
//! ```

use std::future::Future;
use std::time::Duration;

use thiserror::Error;
use tokio::time::{timeout, Instant};

/// Errors related to timeout operations
#[derive(Error, Debug, Clone)]
pub enum TimeoutError {
    #[error("Connect timeout after {0:?}")]
    Connect(Duration),

    #[error("Read timeout after {0:?}")]
    Read(Duration),

    #[error("Write timeout after {0:?}")]
    Write(Duration),

    #[error("Total operation timeout after {0:?}")]
    Total(Duration),

    #[error("Idle timeout after {0:?}")]
    Idle(Duration),
}

/// Configuration for various timeout values
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Timeout for establishing connection
    pub connect_timeout: Duration,
    /// Timeout for reading response
    pub read_timeout: Duration,
    /// Timeout for writing request
    pub write_timeout: Duration,
    /// Total timeout for the entire operation
    pub total_timeout: Option<Duration>,
    /// Timeout for idle connections in pool
    pub idle_timeout: Duration,
    /// Keep-alive interval
    pub keep_alive_interval: Option<Duration>,
    /// Maximum time to wait for a response after request is sent
    pub response_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            total_timeout: Some(Duration::from_secs(300)),
            idle_timeout: Duration::from_secs(90),
            keep_alive_interval: Some(Duration::from_secs(30)),
            response_timeout: Duration::from_secs(60),
        }
    }
}

impl TimeoutConfig {
    /// Create a new timeout configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a configuration with no timeouts (infinite waits)
    pub fn no_timeout() -> Self {
        Self {
            connect_timeout: Duration::MAX,
            read_timeout: Duration::MAX,
            write_timeout: Duration::MAX,
            total_timeout: None,
            idle_timeout: Duration::MAX,
            keep_alive_interval: None,
            response_timeout: Duration::MAX,
        }
    }

    /// Create a configuration optimized for quick operations
    pub fn quick() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            total_timeout: Some(Duration::from_secs(30)),
            idle_timeout: Duration::from_secs(30),
            keep_alive_interval: Some(Duration::from_secs(10)),
            response_timeout: Duration::from_secs(15),
        }
    }

    /// Create a configuration optimized for long-running operations
    pub fn long_running() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(300),
            write_timeout: Duration::from_secs(60),
            total_timeout: Some(Duration::from_secs(3600)),
            idle_timeout: Duration::from_secs(300),
            keep_alive_interval: Some(Duration::from_secs(60)),
            response_timeout: Duration::from_secs(300),
        }
    }

    /// Set connect timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set read timeout
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    /// Set write timeout
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Set total timeout
    pub fn with_total_timeout(mut self, timeout: Duration) -> Self {
        self.total_timeout = Some(timeout);
        self
    }

    /// Disable total timeout
    pub fn without_total_timeout(mut self) -> Self {
        self.total_timeout = None;
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set keep-alive interval
    pub fn with_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = Some(interval);
        self
    }

    /// Disable keep-alive
    pub fn without_keep_alive(mut self) -> Self {
        self.keep_alive_interval = None;
        self
    }

    /// Set response timeout
    pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// Apply all timeouts uniformly
    pub fn with_uniform_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self.read_timeout = timeout;
        self.write_timeout = timeout;
        self.response_timeout = timeout;
        self.total_timeout = Some(timeout);
        self
    }
}

/// A guard that tracks elapsed time and remaining timeout
#[derive(Debug, Clone)]
pub struct TimeoutGuard {
    start: Instant,
    config: TimeoutConfig,
}

impl TimeoutGuard {
    /// Create a new timeout guard
    pub fn new(config: TimeoutConfig) -> Self {
        Self {
            start: Instant::now(),
            config,
        }
    }

    /// Get elapsed time since creation
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Get remaining time for total timeout
    pub fn remaining(&self) -> Option<Duration> {
        self.config.total_timeout.map(|total| {
            let elapsed = self.elapsed();
            if elapsed >= total {
                Duration::ZERO
            } else {
                total - elapsed
            }
        })
    }

    /// Check if the total timeout has been exceeded
    pub fn is_expired(&self) -> bool {
        self.remaining().map(|r| r.is_zero()).unwrap_or(false)
    }

    /// Get the effective timeout for an operation
    ///
    /// Returns the minimum of the operation-specific timeout and remaining total timeout
    pub fn effective_timeout(&self, operation_timeout: Duration) -> Duration {
        match self.remaining() {
            Some(remaining) => operation_timeout.min(remaining),
            None => operation_timeout,
        }
    }

    /// Get effective connect timeout
    pub fn connect_timeout(&self) -> Duration {
        self.effective_timeout(self.config.connect_timeout)
    }

    /// Get effective read timeout
    pub fn read_timeout(&self) -> Duration {
        self.effective_timeout(self.config.read_timeout)
    }

    /// Get effective write timeout
    pub fn write_timeout(&self) -> Duration {
        self.effective_timeout(self.config.write_timeout)
    }

    /// Get effective response timeout
    pub fn response_timeout(&self) -> Duration {
        self.effective_timeout(self.config.response_timeout)
    }

    /// Check if there's enough time for an operation
    pub fn has_time_for(&self, operation: Duration) -> bool {
        match self.remaining() {
            Some(remaining) => remaining >= operation,
            None => true,
        }
    }
}

/// Execute a future with a timeout
pub async fn with_timeout<F, T>(
    duration: Duration,
    future: F,
) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    timeout(duration, future)
        .await
        .map_err(|_| TimeoutError::Total(duration))
}

/// Execute a future with the connect timeout
pub async fn with_connect_timeout<F, T>(
    config: &TimeoutConfig,
    future: F,
) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    timeout(config.connect_timeout, future)
        .await
        .map_err(|_| TimeoutError::Connect(config.connect_timeout))
}

/// Execute a future with the read timeout
pub async fn with_read_timeout<F, T>(
    config: &TimeoutConfig,
    future: F,
) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    timeout(config.read_timeout, future)
        .await
        .map_err(|_| TimeoutError::Read(config.read_timeout))
}

/// Execute a future with the write timeout
pub async fn with_write_timeout<F, T>(
    config: &TimeoutConfig,
    future: F,
) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    timeout(config.write_timeout, future)
        .await
        .map_err(|_| TimeoutError::Write(config.write_timeout))
}

/// Execute a future with the total timeout
pub async fn with_total_timeout<F, T>(
    config: &TimeoutConfig,
    future: F,
) -> Result<T, TimeoutError>
where
    F: Future<Output = T>,
{
    match config.total_timeout {
        Some(total) => timeout(total, future)
            .await
            .map_err(|_| TimeoutError::Total(total)),
        None => Ok(future.await),
    }
}

/// Builder for creating operations with multiple timeout stages
pub struct TimeoutBuilder {
    config: TimeoutConfig,
    guard: TimeoutGuard,
}

impl TimeoutBuilder {
    /// Create a new timeout builder
    pub fn new(config: TimeoutConfig) -> Self {
        let guard = TimeoutGuard::new(config.clone());
        Self { config, guard }
    }

    /// Execute an operation with connect timeout
    pub async fn connect<F, T, E>(&self, future: F) -> Result<T, TimeoutError>
    where
        F: Future<Output = Result<T, E>>,
    {
        if self.guard.is_expired() {
            return Err(TimeoutError::Total(
                self.config.total_timeout.unwrap_or(Duration::ZERO),
            ));
        }

        let effective_timeout = self.guard.connect_timeout();
        timeout(effective_timeout, future)
            .await
            .map_err(|_| TimeoutError::Connect(effective_timeout))?
            .map_err(|_| TimeoutError::Connect(effective_timeout))
    }

    /// Execute an operation with read timeout
    pub async fn read<F, T>(&self, future: F) -> Result<T, TimeoutError>
    where
        F: Future<Output = T>,
    {
        if self.guard.is_expired() {
            return Err(TimeoutError::Total(
                self.config.total_timeout.unwrap_or(Duration::ZERO),
            ));
        }

        let effective_timeout = self.guard.read_timeout();
        timeout(effective_timeout, future)
            .await
            .map_err(|_| TimeoutError::Read(effective_timeout))
    }

    /// Execute an operation with write timeout
    pub async fn write<F, T>(&self, future: F) -> Result<T, TimeoutError>
    where
        F: Future<Output = T>,
    {
        if self.guard.is_expired() {
            return Err(TimeoutError::Total(
                self.config.total_timeout.unwrap_or(Duration::ZERO),
            ));
        }

        let effective_timeout = self.guard.write_timeout();
        timeout(effective_timeout, future)
            .await
            .map_err(|_| TimeoutError::Write(effective_timeout))
    }

    /// Get the timeout guard
    pub fn guard(&self) -> &TimeoutGuard {
        &self.guard
    }

    /// Check if there's time remaining
    pub fn has_time_remaining(&self) -> bool {
        !self.guard.is_expired()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_timeout_config_defaults() {
        let config = TimeoutConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.read_timeout, Duration::from_secs(30));
        assert_eq!(config.total_timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_timeout_config_builder() {
        let config = TimeoutConfig::new()
            .with_connect_timeout(Duration::from_secs(5))
            .with_read_timeout(Duration::from_secs(15))
            .with_write_timeout(Duration::from_secs(20))
            .with_total_timeout(Duration::from_secs(120));

        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.read_timeout, Duration::from_secs(15));
        assert_eq!(config.write_timeout, Duration::from_secs(20));
        assert_eq!(config.total_timeout, Some(Duration::from_secs(120)));
    }

    #[test]
    fn test_timeout_config_quick() {
        let config = TimeoutConfig::quick();
        assert!(config.connect_timeout < TimeoutConfig::default().connect_timeout);
    }

    #[test]
    fn test_timeout_config_long_running() {
        let config = TimeoutConfig::long_running();
        assert!(config.total_timeout.unwrap() > TimeoutConfig::default().total_timeout.unwrap());
    }

    #[test]
    fn test_timeout_config_no_timeout() {
        let config = TimeoutConfig::no_timeout();
        assert!(config.total_timeout.is_none());
    }

    #[test]
    fn test_timeout_guard_remaining() {
        let config = TimeoutConfig::new().with_total_timeout(Duration::from_secs(60));
        let guard = TimeoutGuard::new(config);

        assert!(guard.remaining().unwrap() > Duration::from_secs(59));
        assert!(guard.remaining().unwrap() <= Duration::from_secs(60));
    }

    #[test]
    fn test_timeout_guard_effective_timeout() {
        let config = TimeoutConfig::new().with_total_timeout(Duration::from_millis(100));
        let guard = TimeoutGuard::new(config);

        // Operation timeout is shorter than remaining
        let effective = guard.effective_timeout(Duration::from_millis(50));
        assert_eq!(effective, Duration::from_millis(50));

        // When remaining is shorter, it should be used
        std::thread::sleep(Duration::from_millis(60));
        let effective = guard.effective_timeout(Duration::from_millis(50));
        assert!(effective < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_with_timeout_success() {
        let result = with_timeout(Duration::from_secs(1), async { 42 }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_with_timeout_expired() {
        let result = with_timeout(Duration::from_millis(10), async {
            sleep(Duration::from_millis(100)).await;
            42
        })
        .await;

        assert!(matches!(result, Err(TimeoutError::Total(_))));
    }

    #[tokio::test]
    async fn test_with_connect_timeout() {
        let config = TimeoutConfig::new().with_connect_timeout(Duration::from_secs(1));

        let result = with_connect_timeout(&config, async { "connected" }).await;
        assert_eq!(result.unwrap(), "connected");
    }

    #[tokio::test]
    async fn test_with_read_timeout() {
        let config = TimeoutConfig::new().with_read_timeout(Duration::from_secs(1));

        let result = with_read_timeout(&config, async { "read" }).await;
        assert_eq!(result.unwrap(), "read");
    }

    #[tokio::test]
    async fn test_timeout_builder() {
        let config = TimeoutConfig::new()
            .with_connect_timeout(Duration::from_secs(1))
            .with_total_timeout(Duration::from_secs(5));

        let builder = TimeoutBuilder::new(config);
        assert!(builder.has_time_remaining());

        let result = builder.read(async { "data" }).await;
        assert_eq!(result.unwrap(), "data");
    }

    #[test]
    fn test_timeout_guard_has_time_for() {
        let config = TimeoutConfig::new().with_total_timeout(Duration::from_secs(10));
        let guard = TimeoutGuard::new(config);

        assert!(guard.has_time_for(Duration::from_secs(5)));
        assert!(!guard.has_time_for(Duration::from_secs(15)));
    }

    #[test]
    fn test_uniform_timeout() {
        let config = TimeoutConfig::new().with_uniform_timeout(Duration::from_secs(30));

        assert_eq!(config.connect_timeout, Duration::from_secs(30));
        assert_eq!(config.read_timeout, Duration::from_secs(30));
        assert_eq!(config.write_timeout, Duration::from_secs(30));
        assert_eq!(config.response_timeout, Duration::from_secs(30));
        assert_eq!(config.total_timeout, Some(Duration::from_secs(30)));
    }
}
