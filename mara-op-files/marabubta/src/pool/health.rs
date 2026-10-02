// Marabunta - Licensed under the MIT License.
//! Health checking for pooled connections
//!
//! This module provides traits and types for validating connection health
//! and managing health check behavior.

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Trait for checking connection health
///
/// Implement this trait to define how the pool validates that a connection
/// is still healthy and usable.
///
/// # Example
///
/// ```rust,no_run
/// use async_trait::async_trait;
/// use marabunta_compute::pool::HealthCheck;
///
/// struct DatabaseConnection {
///     // ...
/// }
///
/// struct DatabaseHealthChecker;
///
/// #[async_trait]
/// impl HealthCheck<DatabaseConnection> for DatabaseHealthChecker {
///     async fn check(&self, conn: &DatabaseConnection) -> bool {
///         // Execute a simple query to verify connection
///         // conn.execute("SELECT 1").is_ok()
///         true
///     }
/// }
/// ```
#[async_trait]
pub trait HealthCheck<T>: Send + Sync {
    /// Check if the connection is healthy
    ///
    /// Returns `true` if the connection is healthy and can be used,
    /// `false` otherwise. This method should be quick to execute.
    async fn check(&self, conn: &T) -> bool;

    /// Optional: Get a human-readable description of the health check
    fn description(&self) -> &str {
        "default health check"
    }
}

/// Configuration for health checking behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Interval between health checks for idle connections
    pub interval: Duration,

    /// Timeout for individual health checks
    pub timeout: Duration,

    /// Whether to check health on acquire
    pub check_on_acquire: bool,

    /// Whether to check health on return
    pub check_on_return: bool,

    /// Number of consecutive failures before marking connection unhealthy
    pub failure_threshold: u32,

    /// Whether health checks are enabled
    pub enabled: bool,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            check_on_acquire: true,
            check_on_return: false,
            failure_threshold: 1,
            enabled: true,
        }
    }
}

impl HealthCheckConfig {
    /// Create a new health check configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the health check interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the health check timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enable or disable check on acquire
    pub fn with_check_on_acquire(mut self, check: bool) -> Self {
        self.check_on_acquire = check;
        self
    }

    /// Enable or disable check on return
    pub fn with_check_on_return(mut self, check: bool) -> Self {
        self.check_on_return = check;
        self
    }

    /// Set the failure threshold
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Enable or disable health checks entirely
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Create a permissive configuration (infrequent checks)
    pub fn permissive() -> Self {
        Self {
            interval: Duration::from_secs(300),
            timeout: Duration::from_secs(30),
            check_on_acquire: false,
            check_on_return: false,
            failure_threshold: 3,
            enabled: true,
        }
    }

    /// Create a strict configuration (frequent checks)
    pub fn strict() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            check_on_acquire: true,
            check_on_return: true,
            failure_threshold: 1,
            enabled: true,
        }
    }

    /// Create a configuration for testing
    pub fn for_testing() -> Self {
        Self {
            interval: Duration::from_millis(100),
            timeout: Duration::from_millis(50),
            check_on_acquire: true,
            check_on_return: false,
            failure_threshold: 1,
            enabled: true,
        }
    }
}

/// Result of a health check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthCheckResult {
    /// Connection is healthy
    Healthy,
    /// Connection is unhealthy
    Unhealthy,
    /// Health check timed out
    Timeout,
    /// Health check was skipped
    Skipped,
}

impl HealthCheckResult {
    /// Check if the result indicates a healthy connection
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy | Self::Skipped)
    }

    /// Check if the result indicates an unhealthy connection
    pub fn is_unhealthy(&self) -> bool {
        matches!(self, Self::Unhealthy | Self::Timeout)
    }
}

/// A no-op health checker that always returns healthy
///
/// Use this when you don't need health checking.
pub struct NoOpHealthCheck;

#[async_trait]
impl<T: Send + Sync> HealthCheck<T> for NoOpHealthCheck {
    async fn check(&self, _conn: &T) -> bool {
        true
    }

    fn description(&self) -> &str {
        "no-op health check (always healthy)"
    }
}

/// A health checker that runs multiple checks
pub struct CompositeHealthCheck<T> {
    checks: Vec<Box<dyn HealthCheck<T>>>,
}

impl<T> CompositeHealthCheck<T> {
    /// Create a new composite health checker
    pub fn new() -> Self {
        Self { checks: Vec::new() }
    }

    /// Add a health check
    pub fn add<H: HealthCheck<T> + 'static>(mut self, check: H) -> Self {
        self.checks.push(Box::new(check));
        self
    }
}

impl<T> Default for CompositeHealthCheck<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Send + Sync> HealthCheck<T> for CompositeHealthCheck<T> {
    async fn check(&self, conn: &T) -> bool {
        for check in &self.checks {
            if !check.check(conn).await {
                return false;
            }
        }
        true
    }

    fn description(&self) -> &str {
        "composite health check"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert!(config.enabled);
        assert!(config.check_on_acquire);
        assert!(!config.check_on_return);
        assert_eq!(config.failure_threshold, 1);
    }

    #[test]
    fn test_health_check_config_builder() {
        let config = HealthCheckConfig::new()
            .with_interval(Duration::from_secs(60))
            .with_timeout(Duration::from_secs(10))
            .with_check_on_acquire(false)
            .with_failure_threshold(3);

        assert_eq!(config.interval, Duration::from_secs(60));
        assert_eq!(config.timeout, Duration::from_secs(10));
        assert!(!config.check_on_acquire);
        assert_eq!(config.failure_threshold, 3);
    }

    #[test]
    fn test_health_check_config_presets() {
        let permissive = HealthCheckConfig::permissive();
        assert!(!permissive.check_on_acquire);
        assert!(permissive.interval > Duration::from_secs(60));

        let strict = HealthCheckConfig::strict();
        assert!(strict.check_on_acquire);
        assert!(strict.check_on_return);
        assert!(strict.interval < Duration::from_secs(10));
    }

    #[test]
    fn test_health_check_result() {
        assert!(HealthCheckResult::Healthy.is_healthy());
        assert!(HealthCheckResult::Skipped.is_healthy());
        assert!(!HealthCheckResult::Unhealthy.is_healthy());
        assert!(!HealthCheckResult::Timeout.is_healthy());

        assert!(HealthCheckResult::Unhealthy.is_unhealthy());
        assert!(HealthCheckResult::Timeout.is_unhealthy());
        assert!(!HealthCheckResult::Healthy.is_unhealthy());
    }

    #[tokio::test]
    async fn test_no_op_health_check() {
        let checker = NoOpHealthCheck;
        assert!(checker.check(&()).await);
        assert!(checker.check(&42).await);
        assert!(checker.check(&"test".to_string()).await);
    }

    struct AlwaysUnhealthy;

    #[async_trait]
    impl<T: Send + Sync> HealthCheck<T> for AlwaysUnhealthy {
        async fn check(&self, _conn: &T) -> bool {
            false
        }
    }

    struct AlwaysHealthy;

    #[async_trait]
    impl<T: Send + Sync> HealthCheck<T> for AlwaysHealthy {
        async fn check(&self, _conn: &T) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_composite_health_check_all_pass() {
        let checker = CompositeHealthCheck::new()
            .add(AlwaysHealthy)
            .add(AlwaysHealthy);

        assert!(checker.check(&()).await);
    }

    #[tokio::test]
    async fn test_composite_health_check_one_fails() {
        let checker = CompositeHealthCheck::new()
            .add(AlwaysHealthy)
            .add(AlwaysUnhealthy);

        assert!(!checker.check(&()).await);
    }

    #[tokio::test]
    async fn test_composite_health_check_empty() {
        let checker: CompositeHealthCheck<()> = CompositeHealthCheck::new();
        assert!(checker.check(&()).await);
    }
}
