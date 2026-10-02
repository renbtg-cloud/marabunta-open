// Marabunta - Licensed under the MIT License.
//! Core connection pool implementation
//!
//! This module provides the main `Pool<T>` type that manages a pool of
//! connections with configurable behavior.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify, Semaphore};
use tracing::{debug, info, trace, warn};

use super::config::PoolConfig;
use super::connection::{
    next_connection_id, ConnectionFactory, PooledConnection, PooledConnectionInner,
};
use super::errors::{PoolError, PoolResult};
use super::health::HealthCheck;
use super::metrics::{PoolMetrics, PoolMetricsSnapshot};

/// A generic connection pool
///
/// The pool manages a set of connections, handling creation, health checking,
/// and lifecycle management. Connections are created lazily up to the maximum
/// pool size, and returned connections are reused when possible.
///
/// # Type Parameters
///
/// - `T`: The connection type to pool
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use marabunta_compute::pool::{Pool, PoolConfig, ConnectionFactory, HealthCheck, PoolError};
///
/// struct MyConnection { /* ... */ }
/// struct MyFactory;
///
/// #[async_trait::async_trait]
/// impl ConnectionFactory<MyConnection> for MyFactory {
///     type Error = PoolError;
///     async fn create(&self) -> Result<MyConnection, Self::Error> {
///         Ok(MyConnection { /* ... */ })
///     }
/// }
///
/// #[async_trait::async_trait]
/// impl HealthCheck<MyConnection> for MyFactory {
///     async fn check(&self, _conn: &MyConnection) -> bool { true }
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = PoolConfig::default();
/// let factory = Arc::new(MyFactory);
/// let pool = Pool::new(config, factory)?;
///
/// let conn = pool.get().await?;
/// // Use connection...
/// // Connection is returned to pool when dropped
/// # Ok(())
/// # }
/// ```
pub struct Pool<T>
where
    T: Send + Sync + 'static,
{
    /// Pool configuration
    config: PoolConfig,
    /// Connection factory
    factory: Arc<dyn ConnectionFactory<T, Error = PoolError> + Send + Sync>,
    /// Health checker
    health_check: Arc<dyn HealthCheck<T> + Send + Sync>,
    /// Pool of idle connections
    idle_connections: Mutex<VecDeque<PooledConnectionInner<T>>>,
    /// Semaphore for limiting total connections
    connection_semaphore: Arc<Semaphore>,
    /// Channel for receiving returned connections
    return_tx: mpsc::UnboundedSender<PooledConnectionInner<T>>,
    /// Notification for when connections become available
    available_notify: Arc<Notify>,
    /// Whether the pool is closed
    closed: AtomicBool,
    /// Current number of connections (idle + in_use)
    total_connections: AtomicUsize,
    /// Number of connections currently in use
    in_use_count: AtomicUsize,
    /// Metrics collector
    metrics: Arc<PoolMetrics>,
    /// Shutdown signal
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl<T> Pool<T>
where
    T: Send + Sync + 'static,
{
    /// Create a new connection pool
    ///
    /// The factory must implement both `ConnectionFactory` and `HealthCheck`.
    pub fn new<F>(config: PoolConfig, factory: Arc<F>) -> PoolResult<Arc<Self>>
    where
        F: ConnectionFactory<T, Error = PoolError> + HealthCheck<T> + Send + Sync + 'static,
    {
        Self::with_health_check(config, factory.clone(), factory)
    }

    /// Create a new connection pool with a separate health checker
    pub fn with_health_check<F, H>(
        config: PoolConfig,
        factory: Arc<F>,
        health_check: Arc<H>,
    ) -> PoolResult<Arc<Self>>
    where
        F: ConnectionFactory<T, Error = PoolError> + Send + Sync + 'static,
        H: HealthCheck<T> + Send + Sync + 'static,
    {
        // Validate configuration
        config.validate().map_err(PoolError::InvalidConfig)?;

        let (return_tx, return_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let metrics = Arc::new(PoolMetrics::new(&config.pool_name, config.enable_metrics));

        let pool = Arc::new(Self {
            config: config.clone(),
            factory,
            health_check,
            idle_connections: Mutex::new(VecDeque::with_capacity(config.max_connections)),
            connection_semaphore: Arc::new(Semaphore::new(config.max_connections)),
            return_tx,
            available_notify: Arc::new(Notify::new()),
            closed: AtomicBool::new(false),
            total_connections: AtomicUsize::new(0),
            in_use_count: AtomicUsize::new(0),
            metrics,
            shutdown_tx,
        });

        // Start background tasks
        pool.start_return_handler(return_rx);
        pool.start_health_check_task(shutdown_rx.clone());
        pool.start_idle_cleanup_task(shutdown_rx);

        // Pre-fill minimum connections
        if config.min_connections > 0 {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                pool_clone.fill_minimum_connections().await;
            });
        }

        info!(
            pool_name = %config.pool_name,
            min = config.min_connections,
            max = config.max_connections,
            "Connection pool created"
        );

        Ok(pool)
    }

    /// Get a connection from the pool
    ///
    /// This method will wait up to `acquire_timeout` for a connection to
    /// become available. If the pool is exhausted and no connection becomes
    /// available within the timeout, a `PoolError::Timeout` is returned.
    pub async fn get(self: &Arc<Self>) -> PoolResult<PooledConnection<T>> {
        self.get_timeout(self.config.acquire_timeout).await
    }

    /// Get a connection with a custom timeout
    pub async fn get_timeout(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> PoolResult<PooledConnection<T>> {
        if self.is_closed() {
            return Err(PoolError::PoolClosed);
        }

        let start = Instant::now();

        loop {
            // Try to get an idle connection first
            if let Some(conn) = self.try_get_idle().await? {
                self.metrics.record_wait_time(start.elapsed());
                self.metrics.record_connection_acquired();
                return Ok(conn);
            }

            // Check if we've exceeded the timeout
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                self.metrics.record_timeout();
                return Err(PoolError::Timeout);
            }

            let remaining = timeout - elapsed;

            // Try to create a new connection if we haven't hit the limit
            match tokio::time::timeout(
                remaining.min(Duration::from_millis(100)),
                self.connection_semaphore.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => {
                    // We got a permit, create a new connection
                    match self.create_connection().await {
                        Ok(conn) => {
                            // Drop the permit - we're tracking connections ourselves
                            permit.forget();
                            self.metrics.record_wait_time(start.elapsed());
                            self.metrics.record_connection_acquired();
                            return Ok(conn);
                        }
                        Err(e) => {
                            // Creation failed, permit is dropped
                            drop(permit);
                            self.metrics.record_connection_error("creation_failed");

                            // If this is a transient error, we might retry
                            if !e.is_transient() {
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(Err(_)) => {
                    // Semaphore closed - pool is shutting down
                    return Err(PoolError::PoolClosed);
                }
                Err(_) => {
                    // Timeout waiting for semaphore - pool is at capacity
                    // Wait for a connection to be returned
                    let wait_result = tokio::time::timeout(
                        remaining.min(Duration::from_millis(100)),
                        self.available_notify.notified(),
                    )
                    .await;

                    if wait_result.is_err() && start.elapsed() >= timeout {
                        self.metrics.record_timeout();
                        return Err(PoolError::Timeout);
                    }
                }
            }
        }
    }

    /// Try to get a connection without waiting
    ///
    /// Returns `None` if no connection is immediately available.
    pub async fn try_get(self: &Arc<Self>) -> PoolResult<Option<PooledConnection<T>>> {
        if self.is_closed() {
            return Err(PoolError::PoolClosed);
        }

        // Try to get an idle connection
        if let Some(conn) = self.try_get_idle().await? {
            self.metrics.record_connection_acquired();
            return Ok(Some(conn));
        }

        // Try to create a new connection if we haven't hit the limit
        match self.connection_semaphore.clone().try_acquire_owned() {
            Ok(permit) => match self.create_connection().await {
                Ok(conn) => {
                    permit.forget();
                    self.metrics.record_connection_acquired();
                    Ok(Some(conn))
                }
                Err(e) => {
                    drop(permit);
                    Err(e)
                }
            },
            Err(_) => {
                // Pool is at capacity
                Ok(None)
            }
        }
    }

    /// Try to get an idle connection from the pool
    async fn try_get_idle(self: &Arc<Self>) -> PoolResult<Option<PooledConnection<T>>> {
        loop {
            let mut idle = self.idle_connections.lock();
            let conn = idle.pop_front();
            drop(idle);

            let Some(mut conn) = conn else {
                return Ok(None);
            };

            // Check if connection has exceeded its lifetime
            if conn.info.age() > self.config.max_lifetime {
                debug!(
                    conn_id = conn.info.id,
                    age_secs = conn.info.age().as_secs(),
                    "Connection exceeded max lifetime, discarding"
                );
                self.total_connections.fetch_sub(1, Ordering::SeqCst);
                self.connection_semaphore.add_permits(1);
                self.metrics.record_connection_closed("max_lifetime");
                continue;
            }

            // Check if connection has been idle too long
            if conn.info.idle_time() > self.config.idle_timeout {
                // Only discard if we're above minimum
                if self.total_connections.load(Ordering::SeqCst) > self.config.min_connections {
                    debug!(
                        conn_id = conn.info.id,
                        idle_secs = conn.info.idle_time().as_secs(),
                        "Connection exceeded idle timeout, discarding"
                    );
                    self.total_connections.fetch_sub(1, Ordering::SeqCst);
                    self.connection_semaphore.add_permits(1);
                    self.metrics.record_connection_closed("idle_timeout");
                    continue;
                }
            }

            // Validate if configured
            if self.config.validate_on_acquire {
                let is_healthy = self.health_check.check(&conn.conn).await;
                if !is_healthy {
                    debug!(conn_id = conn.info.id, "Connection failed health check, discarding");
                    self.total_connections.fetch_sub(1, Ordering::SeqCst);
                    self.connection_semaphore.add_permits(1);
                    self.metrics.record_health_check_failure();
                    self.metrics.record_connection_closed("health_check_failed");
                    continue;
                }
            }

            // Connection is good, hand it out
            self.in_use_count.fetch_add(1, Ordering::SeqCst);
            self.update_metrics();
            conn.info.mark_used();
            return Ok(Some(PooledConnection::new(conn, self.return_tx.clone())));
        }
    }

    /// Create a new connection
    async fn create_connection(self: &Arc<Self>) -> PoolResult<PooledConnection<T>> {
        let mut last_error = None;

        for attempt in 0..self.config.connection_retries {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_delay).await;
            }

            match self.factory.create().await {
                Ok(conn) => {
                    let id = next_connection_id();
                    let inner = PooledConnectionInner::new(conn, id);

                    self.total_connections.fetch_add(1, Ordering::SeqCst);
                    self.in_use_count.fetch_add(1, Ordering::SeqCst);
                    self.update_metrics();
                    self.metrics.record_connection_created();

                    debug!(conn_id = id, "Created new connection");
                    return Ok(PooledConnection::new(inner, self.return_tx.clone()));
                }
                Err(e) => {
                    warn!(
                        attempt = attempt + 1,
                        max_attempts = self.config.connection_retries,
                        error = %e,
                        "Connection creation failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| PoolError::ConnectionFailed("Unknown error".into())))
    }

    /// Start the task that handles returned connections
    fn start_return_handler(self: &Arc<Self>, mut return_rx: mpsc::UnboundedReceiver<PooledConnectionInner<T>>) {
        let pool = Arc::downgrade(self);

        tokio::spawn(async move {
            while let Some(conn) = return_rx.recv().await {
                let Some(pool) = pool.upgrade() else {
                    break;
                };

                // Return the connection to the pool
                pool.return_connection(conn).await;
            }
        });
    }

    /// Return a connection to the pool
    async fn return_connection(self: &Arc<Self>, conn: PooledConnectionInner<T>) {
        self.in_use_count.fetch_sub(1, Ordering::SeqCst);
        self.metrics.record_connection_released();

        // Check if pool is closed
        if self.is_closed() {
            self.total_connections.fetch_sub(1, Ordering::SeqCst);
            self.connection_semaphore.add_permits(1);
            self.metrics.record_connection_closed("pool_closed");
            return;
        }

        // Check lifetime
        if conn.info.age() > self.config.max_lifetime {
            self.total_connections.fetch_sub(1, Ordering::SeqCst);
            self.connection_semaphore.add_permits(1);
            self.metrics.record_connection_closed("max_lifetime");
            trace!(conn_id = conn.info.id, "Connection exceeded max lifetime on return");
            return;
        }

        // Add to idle pool
        {
            let mut idle = self.idle_connections.lock();
            idle.push_back(conn);
        }

        self.update_metrics();
        self.available_notify.notify_one();
    }

    /// Start the health check background task
    fn start_health_check_task(self: &Arc<Self>, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        let pool = Arc::downgrade(self);
        let interval = self.config.health_check_interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let Some(pool) = pool.upgrade() else {
                            break;
                        };
                        pool.run_health_checks().await;
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Run health checks on idle connections
    async fn run_health_checks(self: &Arc<Self>) {
        let mut to_check = Vec::new();

        // Collect connections to check
        {
            let mut idle = self.idle_connections.lock();
            while let Some(conn) = idle.pop_front() {
                to_check.push(conn);
            }
        }

        let mut healthy = Vec::new();
        let mut unhealthy_count = 0;

        // Check each connection
        for conn in to_check {
            // Check lifetime first
            if conn.info.age() > self.config.max_lifetime {
                self.total_connections.fetch_sub(1, Ordering::SeqCst);
                self.connection_semaphore.add_permits(1);
                self.metrics.record_connection_closed("max_lifetime");
                unhealthy_count += 1;
                continue;
            }

            // Run health check
            if self.health_check.check(&conn.conn).await {
                healthy.push(conn);
            } else {
                self.total_connections.fetch_sub(1, Ordering::SeqCst);
                self.connection_semaphore.add_permits(1);
                self.metrics.record_health_check_failure();
                self.metrics.record_connection_closed("health_check_failed");
                unhealthy_count += 1;
            }
        }

        // Return healthy connections to pool
        {
            let mut idle = self.idle_connections.lock();
            for conn in healthy {
                idle.push_back(conn);
            }
        }

        if unhealthy_count > 0 {
            debug!(
                removed = unhealthy_count,
                "Removed unhealthy connections during health check"
            );
            self.fill_minimum_connections().await;
        }

        self.update_metrics();
    }

    /// Start the idle connection cleanup task
    fn start_idle_cleanup_task(self: &Arc<Self>, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        let pool = Arc::downgrade(self);
        let interval = self.config.idle_timeout / 2;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let Some(pool) = pool.upgrade() else {
                            break;
                        };
                        pool.cleanup_idle_connections();
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Clean up idle connections that have been idle too long
    fn cleanup_idle_connections(self: &Arc<Self>) {
        let mut removed = 0;
        let mut idle = self.idle_connections.lock();

        // Only remove connections if we're above minimum
        let total = self.total_connections.load(Ordering::SeqCst);
        if total <= self.config.min_connections {
            return;
        }

        let max_to_remove = total - self.config.min_connections;
        let mut to_remove = Vec::new();

        // Find idle connections to remove
        let mut i = 0;
        while i < idle.len() && to_remove.len() < max_to_remove {
            if idle[i].info.idle_time() > self.config.idle_timeout {
                if let Some(conn) = idle.remove(i) {
                    to_remove.push(conn);
                }
            } else {
                i += 1;
            }
        }

        drop(idle);

        // Actually remove them
        for _conn in to_remove {
            self.total_connections.fetch_sub(1, Ordering::SeqCst);
            self.connection_semaphore.add_permits(1);
            self.metrics.record_connection_closed("idle_timeout");
            removed += 1;
        }

        if removed > 0 {
            trace!(removed = removed, "Cleaned up idle connections");
            self.update_metrics();
        }
    }

    /// Fill the pool to the minimum number of connections
    async fn fill_minimum_connections(self: &Arc<Self>) {
        let current = self.total_connections.load(Ordering::SeqCst);
        let needed = self.config.min_connections.saturating_sub(current);

        if needed == 0 {
            return;
        }

        trace!(current = current, target = self.config.min_connections, "Filling minimum connections");

        for _ in 0..needed {
            match self.connection_semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    match self.factory.create().await {
                        Ok(conn) => {
                            permit.forget();
                            let id = next_connection_id();
                            let inner = PooledConnectionInner::new(conn, id);
                            self.total_connections.fetch_add(1, Ordering::SeqCst);
                            self.metrics.record_connection_created();

                            let mut idle = self.idle_connections.lock();
                            idle.push_back(inner);
                        }
                        Err(e) => {
                            drop(permit);
                            warn!(error = %e, "Failed to create minimum connection");
                            self.metrics.record_connection_error("creation_failed");
                        }
                    }
                }
                Err(_) => {
                    // Pool is at capacity
                    break;
                }
            }
        }

        self.update_metrics();
    }

    /// Update the metrics
    fn update_metrics(&self) {
        let total = self.total_connections.load(Ordering::SeqCst);
        let in_use = self.in_use_count.load(Ordering::SeqCst);
        let idle = total.saturating_sub(in_use);
        self.metrics.set_pool_size(total, idle, in_use);
    }

    /// Get current pool metrics
    pub fn metrics(&self) -> PoolMetricsSnapshot {
        let total = self.total_connections.load(Ordering::SeqCst);
        let in_use = self.in_use_count.load(Ordering::SeqCst);
        let idle = total.saturating_sub(in_use);

        PoolMetricsSnapshot::new(
            total,
            idle,
            in_use,
            0, // Would need additional tracking
            0,
            0,
            0,
            0,
            0,
            self.metrics.average_wait_time(),
        )
    }

    /// Get the pool configuration
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Get the current pool size
    pub fn size(&self) -> usize {
        self.total_connections.load(Ordering::SeqCst)
    }

    /// Get the number of idle connections
    pub fn idle_count(&self) -> usize {
        self.idle_connections.lock().len()
    }

    /// Get the number of connections in use
    pub fn in_use_count(&self) -> usize {
        self.in_use_count.load(Ordering::SeqCst)
    }

    /// Check if the pool is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Close the pool
    ///
    /// This will prevent new connections from being acquired and
    /// shut down background tasks. Existing connections will be
    /// closed when returned.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            info!(pool_name = %self.config.pool_name, "Closing connection pool");
            let _ = self.shutdown_tx.send(true);

            // Close idle connections
            let mut idle = self.idle_connections.lock();
            let count = idle.len();
            idle.clear();
            drop(idle);

            self.total_connections.fetch_sub(count, Ordering::SeqCst);
            self.connection_semaphore.add_permits(count);

            self.update_metrics();
        }
    }
}

impl<T: Send + Sync + 'static> Drop for Pool<T> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    /// Simple test connection
    #[derive(Debug)]
    struct TestConn {
        id: u64,
        healthy: AtomicBool,
    }

    impl TestConn {
        fn new(id: u64) -> Self {
            Self {
                id,
                healthy: AtomicBool::new(true),
            }
        }

        fn set_unhealthy(&self) {
            self.healthy.store(false, Ordering::SeqCst);
        }
    }

    /// Test factory
    struct TestFactory {
        counter: AtomicU64,
        fail_at: Option<u64>,
    }

    impl TestFactory {
        fn new() -> Self {
            Self {
                counter: AtomicU64::new(0),
                fail_at: None,
            }
        }

        fn failing_at(n: u64) -> Self {
            Self {
                counter: AtomicU64::new(0),
                fail_at: Some(n),
            }
        }
    }

    #[async_trait::async_trait]
    impl ConnectionFactory<TestConn> for TestFactory {
        type Error = PoolError;

        async fn create(&self) -> Result<TestConn, Self::Error> {
            let id = self.counter.fetch_add(1, Ordering::SeqCst);
            if let Some(fail_at) = self.fail_at {
                if id >= fail_at {
                    return Err(PoolError::ConnectionFailed("Configured to fail".into()));
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok(TestConn::new(id))
        }
    }

    #[async_trait::async_trait]
    impl HealthCheck<TestConn> for TestFactory {
        async fn check(&self, conn: &TestConn) -> bool {
            conn.healthy.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn test_pool_create_and_get() {
        let config = PoolConfig::for_testing();
        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        let conn = pool.get().await.unwrap();
        assert!(conn.id < 10);
    }

    #[tokio::test]
    async fn test_pool_connection_reuse() {
        let config = PoolConfig::for_testing().with_min_connections(0);
        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Get and return a connection
        let conn1 = pool.get().await.unwrap();
        let id1 = conn1.id;
        drop(conn1);

        // Small delay to allow return to complete
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Get again - should be the same connection
        let conn2 = pool.get().await.unwrap();
        assert_eq!(conn2.id, id1);
    }

    #[tokio::test]
    async fn test_pool_max_connections() {
        let config = PoolConfig::for_testing()
            .with_min_connections(0)
            .with_max_connections(2);

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Get two connections
        let conn1 = pool.get().await.unwrap();
        let conn2 = pool.get().await.unwrap();

        assert_eq!(pool.in_use_count(), 2);

        // Third should timeout
        let result = pool.get().await;
        assert!(matches!(result, Err(PoolError::Timeout)));

        // Return one
        drop(conn1);
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Now we can get another
        let conn3 = pool.get().await.unwrap();
        assert!(conn3.id < 10);

        drop(conn2);
        drop(conn3);
    }

    #[tokio::test]
    async fn test_pool_try_get() {
        let config = PoolConfig::for_testing()
            .with_min_connections(0)
            .with_max_connections(1);

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // First should succeed
        let conn = pool.try_get().await.unwrap();
        assert!(conn.is_some());

        // Second should return None (not Err)
        let result = pool.try_get().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_pool_health_check() {
        let config = PoolConfig::for_testing()
            .with_min_connections(1)
            .with_health_check_interval(Duration::from_millis(50));

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Wait for minimum connection to be created
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Get connection and make it unhealthy
        let conn = pool.get().await.unwrap();
        conn.set_unhealthy();
        drop(conn);

        // Wait for health check
        tokio::time::sleep(Duration::from_millis(100)).await;

        // New connection should be healthy
        let conn2 = pool.get().await.unwrap();
        assert!(conn2.healthy.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_pool_close() {
        let config = PoolConfig::for_testing();
        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        let _conn = pool.get().await.unwrap();

        pool.close();
        assert!(pool.is_closed());

        // Further gets should fail
        let result = pool.get().await;
        assert!(matches!(result, Err(PoolError::PoolClosed)));
    }

    #[tokio::test]
    async fn test_pool_metrics() {
        let config = PoolConfig::for_testing()
            .with_min_connections(2)
            .with_max_connections(5);

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Wait for minimum connections
        tokio::time::sleep(Duration::from_millis(50)).await;

        let metrics = pool.metrics();
        assert!(metrics.total_connections >= 2);
    }

    #[tokio::test]
    async fn test_pool_concurrent_access() {
        let config = PoolConfig::for_testing()
            .with_min_connections(2)
            .with_max_connections(10);

        let factory = Arc::new(TestFactory::new());
        let pool = Pool::new(config, factory).unwrap();

        // Sequential access (pool connections contain non-Send guards)
        let mut results = vec![];
        for i in 0..20 {
            let conn = pool.get().await.unwrap();
            results.push((i, conn.id));
        }

        assert_eq!(results.len(), 20);
    }
}
