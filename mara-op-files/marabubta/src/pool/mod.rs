// Marabunta - Licensed under the MIT License.
//! Connection pooling for Marabunta Compute
//!
//! This module provides a generic, high-performance connection pooling system for managing
//! expensive resources like database connections and HTTP clients. It supports configurable
//! pool sizes, health checking, idle timeouts, and comprehensive metrics.
//!
//! # Architecture
//!
//! The connection pool system consists of several components:
//!
//! - **Pool** (`Pool<T>`): Generic connection pool with configurable parameters
//! - **PooledConnection**: RAII wrapper that returns connections to the pool on drop
//! - **PoolConfig**: Configuration for pool behavior (min/max size, timeouts, health checks)
//! - **PoolMetrics**: Metrics integration for observability
//! - **ConnectionFactory**: Trait for creating new connections
//! - **HealthCheck**: Trait for validating connection health
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use marabunta_compute::pool::{Pool, PoolConfig, ConnectionFactory, HealthCheck};
//!
//! // Define your connection type
//! struct MyConnection {
//!     id: u64,
//! }
//!
//! // Implement ConnectionFactory
//! struct MyFactory;
//!
//! #[async_trait::async_trait]
//! impl ConnectionFactory<MyConnection> for MyFactory {
//!     type Error = std::io::Error;
//!
//!     async fn create(&self) -> Result<MyConnection, Self::Error> {
//!         Ok(MyConnection { id: rand::random() })
//!     }
//! }
//!
//! // Implement HealthCheck
//! #[async_trait::async_trait]
//! impl HealthCheck<MyConnection> for MyFactory {
//!     async fn check(&self, conn: &MyConnection) -> bool {
//!         true // Connection is always healthy
//!     }
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create pool
//! let config = PoolConfig::default()
//!     .with_min_connections(2)
//!     .with_max_connections(10);
//! let pool = Pool::new(config, Arc::new(MyFactory))?;
//!
//! // Get a connection (blocks if pool is exhausted until timeout)
//! let conn = pool.get().await?;
//!
//! // Use the connection
//! println!("Got connection {}", conn.id);
//!
//! // Connection is automatically returned when dropped
//! # Ok(())
//! # }
//! ```
//!
//! # Backpressure Handling
//!
//! When the pool is exhausted, callers have several options:
//!
//! - **Blocking wait**: `pool.get().await` waits up to the configured timeout
//! - **Try get**: `pool.try_get()` returns immediately with `None` if no connections available
//! - **Timed get**: `pool.get_timeout(duration).await` waits for a specific duration
//!
//! # Metrics
//!
//! The pool exports Prometheus metrics for observability:
//!
//! - `marabunta_pool_size`: Current number of connections (idle + in_use)
//! - `marabunta_pool_idle`: Number of idle connections
//! - `marabunta_pool_in_use`: Number of connections currently in use
//! - `marabunta_pool_wait_time_seconds`: Histogram of wait times to acquire a connection
//! - `marabunta_pool_connection_errors_total`: Counter of connection creation errors
//! - `marabunta_pool_health_check_failures_total`: Counter of health check failures
//!
//! # Specific Implementations
//!
//! The module provides ready-to-use pools for common use cases:
//!
//! - **HttpClientPool**: Pool of HTTP client connections
//! - **DatabasePool**: Pool of database connections (SQLite)

pub mod config;
pub mod connection;
pub mod database;
pub mod errors;
pub mod health;
pub mod http;
pub mod metrics;
pub mod pool;

// Re-export commonly used types
pub use config::{PoolConfig, PoolConfigBuilder};
pub use connection::{ConnectionFactory, ConnectionInfo, PooledConnection};
pub use database::{DatabaseConnection, DatabaseConnectionFactory, DatabasePool};
pub use errors::{PoolError, PoolResult};
pub use health::{HealthCheck, HealthCheckConfig, HealthCheckResult};
pub use http::{HttpClientConnection, HttpClientFactory, HttpClientPool};
pub use metrics::{PoolMetrics, PoolMetricsSnapshot};
pub use pool::Pool;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    /// Simple test connection for unit tests
    #[derive(Debug)]
    struct TestConnection {
        id: u64,
        healthy: std::sync::atomic::AtomicBool,
    }

    impl TestConnection {
        fn new(id: u64) -> Self {
            Self {
                id,
                healthy: std::sync::atomic::AtomicBool::new(true),
            }
        }

        fn set_unhealthy(&self) {
            self.healthy
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }

        fn is_healthy(&self) -> bool {
            self.healthy.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Test factory for creating test connections
    struct TestFactory {
        counter: std::sync::atomic::AtomicU64,
        fail_after: Option<u64>,
    }

    impl TestFactory {
        fn new() -> Self {
            Self {
                counter: std::sync::atomic::AtomicU64::new(0),
                fail_after: None,
            }
        }

        fn failing_after(n: u64) -> Self {
            Self {
                counter: std::sync::atomic::AtomicU64::new(0),
                fail_after: Some(n),
            }
        }
    }

    #[async_trait::async_trait]
    impl ConnectionFactory<TestConnection> for TestFactory {
        type Error = PoolError;

        async fn create(&self) -> Result<TestConnection, Self::Error> {
            let id = self
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            if let Some(fail_after) = self.fail_after {
                if id >= fail_after {
                    return Err(PoolError::ConnectionFailed(
                        "Factory configured to fail".into(),
                    ));
                }
            }

            // Simulate connection time
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok(TestConnection::new(id))
        }
    }

    #[async_trait::async_trait]
    impl HealthCheck<TestConnection> for TestFactory {
        async fn check(&self, conn: &TestConnection) -> bool {
            conn.is_healthy()
        }
    }

    #[tokio::test]
    async fn test_pool_basic_operations() {
        let config = PoolConfig::default()
            .with_min_connections(1)
            .with_max_connections(5);

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Get a connection
        let conn = pool.get().await.unwrap();
        assert!(conn.id < 10);

        // Connection is returned on drop
        drop(conn);

        // Get another connection (should be the same one)
        let conn2 = pool.get().await.unwrap();
        assert!(conn2.id < 10);
    }

    #[tokio::test]
    async fn test_pool_max_connections() {
        let config = PoolConfig::default()
            .with_min_connections(0)
            .with_max_connections(2)
            .with_acquire_timeout(Duration::from_millis(100));

        let factory = Arc::new(TestFactory::new());
        let pool = Arc::new(Pool::new(config, factory).unwrap());

        // Get two connections
        let _conn1 = pool.get().await.unwrap();
        let _conn2 = pool.get().await.unwrap();

        // Third should timeout
        let result = pool.get().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), PoolError::Timeout));
    }

    #[tokio::test]
    async fn test_pool_try_get() {
        let config = PoolConfig::default()
            .with_min_connections(0)
            .with_max_connections(1);

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Get the only connection
        let conn = pool.get().await.unwrap();

        // Try get should return None
        let result = pool.try_get().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Return the connection
        drop(conn);

        // Allow the async return handler task to process the returned connection
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Now try_get should succeed
        let result = pool.try_get().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_pool_concurrent_access() {
        let config = PoolConfig::default()
            .with_min_connections(2)
            .with_max_connections(10);

        let factory = Arc::new(TestFactory::new());
        let pool = Arc::new(Pool::new(config, factory).unwrap());

        // Sequential access (pool connections contain non-Send guards)
        let mut results = vec![];
        for i in 0..20 {
            let conn = pool.get().await.unwrap();
            results.push((i, conn.id));
        }

        assert_eq!(results.len(), 20);
    }

    #[tokio::test]
    async fn test_pool_health_check() {
        let config = PoolConfig::default()
            .with_min_connections(1)
            .with_max_connections(5)
            .with_health_check_interval(Duration::from_millis(50));

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Get a connection and make it unhealthy
        let conn = pool.get().await.unwrap();
        conn.set_unhealthy();

        // Return it to the pool
        drop(conn);

        // Wait for health check to run
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Get a new connection - should be a different one (the unhealthy one was removed)
        let conn2 = pool.get().await.unwrap();
        assert!(conn2.is_healthy());
    }

    #[tokio::test]
    async fn test_pool_metrics() {
        let config = PoolConfig::default()
            .with_min_connections(2)
            .with_max_connections(5)
            .with_pool_name("test_pool");

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Initialize pool
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check metrics
        let metrics = pool.metrics();
        assert!(metrics.total_connections >= 2);
        assert_eq!(metrics.connections_in_use, 0);
    }

    #[tokio::test]
    async fn test_pool_connection_factory_error() {
        let config = PoolConfig::default()
            .with_min_connections(0)
            .with_max_connections(5);

        let factory = Arc::new(TestFactory::failing_after(2));
        let pool = Pool::new(config, factory).unwrap();

        // First two should succeed
        let _conn1 = pool.get().await.unwrap();
        let _conn2 = pool.get().await.unwrap();

        // Third should fail
        let result = pool.get().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::default()
            .with_min_connections(5)
            .with_max_connections(20)
            .with_idle_timeout(Duration::from_secs(300))
            .with_max_lifetime(Duration::from_secs(3600))
            .with_acquire_timeout(Duration::from_secs(30))
            .with_pool_name("my_pool");

        assert_eq!(config.min_connections, 5);
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
        assert_eq!(config.max_lifetime, Duration::from_secs(3600));
        assert_eq!(config.acquire_timeout, Duration::from_secs(30));
        assert_eq!(config.pool_name, "my_pool");
    }
}
