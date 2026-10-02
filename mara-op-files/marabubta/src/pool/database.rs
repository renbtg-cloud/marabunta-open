// Marabunta - Licensed under the MIT License.
//! Database connection pool
//!
//! This module provides a connection pool specifically for database connections,
//! with support for SQLite and other database backends.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use super::config::PoolConfig;
use super::connection::ConnectionFactory;
use super::errors::{PoolError, PoolResult};
use super::health::HealthCheck;
use super::pool::Pool;

/// Configuration for database connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database path (or ":memory:" for in-memory database)
    pub path: String,
    /// Whether to create the database if it doesn't exist
    pub create_if_missing: bool,
    /// Whether to open the database in read-only mode
    pub read_only: bool,
    /// Busy timeout in milliseconds
    pub busy_timeout_ms: u32,
    /// Whether to enable WAL mode
    pub wal_mode: bool,
    /// Page size for the database
    pub page_size: Option<u32>,
    /// Cache size in pages (negative for KB)
    pub cache_size: Option<i32>,
    /// Whether to enable foreign key constraints
    pub foreign_keys: bool,
    /// SQL to run on connection initialization
    pub init_sql: Option<String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
            create_if_missing: true,
            read_only: false,
            busy_timeout_ms: 5000,
            wal_mode: true,
            page_size: None,
            cache_size: Some(-64000), // 64MB cache
            foreign_keys: true,
            init_sql: None,
        }
    }
}

impl DatabaseConfig {
    /// Create a new database configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Create configuration for an in-memory database
    pub fn in_memory() -> Self {
        Self {
            path: ":memory:".to_string(),
            wal_mode: false, // WAL not supported for in-memory
            ..Default::default()
        }
    }

    /// Create configuration for a file-based database
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    /// Set the database path
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Enable or disable creation if missing
    pub fn with_create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
    }

    /// Enable or disable read-only mode
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set the busy timeout
    pub fn with_busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout_ms = timeout.as_millis() as u32;
        self
    }

    /// Enable or disable WAL mode
    pub fn with_wal_mode(mut self, wal: bool) -> Self {
        self.wal_mode = wal;
        self
    }

    /// Set the cache size
    pub fn with_cache_size_kb(mut self, kb: u32) -> Self {
        self.cache_size = Some(-(kb as i32));
        self
    }

    /// Enable or disable foreign keys
    pub fn with_foreign_keys(mut self, enabled: bool) -> Self {
        self.foreign_keys = enabled;
        self
    }

    /// Set initialization SQL
    pub fn with_init_sql(mut self, sql: impl Into<String>) -> Self {
        self.init_sql = Some(sql.into());
        self
    }

    /// Build open flags for rusqlite
    fn open_flags(&self) -> OpenFlags {
        let mut flags = OpenFlags::SQLITE_OPEN_URI | OpenFlags::SQLITE_OPEN_NO_MUTEX;

        if self.read_only {
            flags |= OpenFlags::SQLITE_OPEN_READ_ONLY;
        } else {
            flags |= OpenFlags::SQLITE_OPEN_READ_WRITE;
        }

        if self.create_if_missing {
            flags |= OpenFlags::SQLITE_OPEN_CREATE;
        }

        flags
    }

    /// Open a new connection with this configuration
    fn open_connection(&self) -> PoolResult<Connection> {
        let conn = Connection::open_with_flags(&self.path, self.open_flags())
            .map_err(|e| PoolError::Database(format!("Failed to open database: {}", e)))?;

        // Apply pragmas
        conn.busy_timeout(Duration::from_millis(self.busy_timeout_ms as u64))
            .map_err(|e| PoolError::Database(format!("Failed to set busy timeout: {}", e)))?;

        if self.wal_mode && self.path != ":memory:" {
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(|e| PoolError::Database(format!("Failed to enable WAL mode: {}", e)))?;
        }

        if let Some(page_size) = self.page_size {
            conn.pragma_update(None, "page_size", page_size)
                .map_err(|e| PoolError::Database(format!("Failed to set page size: {}", e)))?;
        }

        if let Some(cache_size) = self.cache_size {
            conn.pragma_update(None, "cache_size", cache_size)
                .map_err(|e| PoolError::Database(format!("Failed to set cache size: {}", e)))?;
        }

        if self.foreign_keys {
            conn.pragma_update(None, "foreign_keys", "ON")
                .map_err(|e| {
                    PoolError::Database(format!("Failed to enable foreign keys: {}", e))
                })?;
        }

        // Run init SQL if provided
        if let Some(ref init_sql) = self.init_sql {
            conn.execute_batch(init_sql)
                .map_err(|e| PoolError::Database(format!("Failed to run init SQL: {}", e)))?;
        }

        Ok(conn)
    }
}

/// A pooled database connection
pub struct DatabaseConnection {
    /// The underlying SQLite connection (mutex-protected for async safety)
    conn: Mutex<Connection>,
    /// Configuration used to create this connection
    config: DatabaseConfig,
    /// Connection ID for tracking
    id: u64,
    /// Whether the connection is healthy
    healthy: AtomicBool,
    /// When the connection was created
    created_at: Instant,
    /// Number of queries executed with this connection
    query_count: AtomicU64,
    /// Number of transactions executed
    transaction_count: AtomicU64,
}

impl std::fmt::Debug for DatabaseConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseConnection")
            .field("id", &self.id)
            .field("path", &self.config.path)
            .field("healthy", &self.is_healthy())
            .field("age", &self.age())
            .field("query_count", &self.query_count())
            .finish()
    }
}

impl DatabaseConnection {
    /// Create a new database connection
    fn new(conn: Connection, config: DatabaseConfig, id: u64) -> Self {
        Self {
            conn: Mutex::new(conn),
            config,
            id,
            healthy: AtomicBool::new(true),
            created_at: Instant::now(),
            query_count: AtomicU64::new(0),
            transaction_count: AtomicU64::new(0),
        }
    }

    /// Get the connection ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the database path
    pub fn path(&self) -> &str {
        &self.config.path
    }

    /// Get the connection age
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get the number of queries executed
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    /// Get the number of transactions executed
    pub fn transaction_count(&self) -> u64 {
        self.transaction_count.load(Ordering::Relaxed)
    }

    /// Check if the connection is healthy
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    /// Mark the connection as unhealthy
    pub fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::SeqCst);
    }

    /// Execute a query that returns no data
    ///
    /// Note: This is a blocking operation. Use with care in async contexts.
    pub fn execute(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> PoolResult<usize> {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        let conn = self.conn.lock();
        conn.execute(sql, params)
            .map_err(|e| PoolError::Database(format!("Execute failed: {}", e)))
    }

    /// Execute a batch of SQL statements
    ///
    /// Note: This is a blocking operation. Use with care in async contexts.
    pub fn execute_batch(&self, sql: &str) -> PoolResult<()> {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        let conn = self.conn.lock();
        conn.execute_batch(sql)
            .map_err(|e| PoolError::Database(format!("Execute batch failed: {}", e)))
    }

    /// Execute a query and get a single value
    ///
    /// Note: This is a blocking operation. Use with care in async contexts.
    pub fn query_row<T, F>(&self, sql: &str, params: &[&dyn rusqlite::ToSql], f: F) -> PoolResult<T>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.query_count.fetch_add(1, Ordering::Relaxed);
        let conn = self.conn.lock();
        conn.query_row(sql, params, f)
            .map_err(|e| PoolError::Database(format!("Query failed: {}", e)))
    }

    /// Execute a callback with direct access to the connection
    ///
    /// This allows for more complex operations like transactions.
    /// Note: This is a blocking operation. Use with care in async contexts.
    pub fn with_connection<F, R>(&self, f: F) -> PoolResult<R>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<R>,
    {
        let conn = self.conn.lock();
        f(&conn).map_err(|e| PoolError::Database(format!("Connection operation failed: {}", e)))
    }

    /// Execute a callback within a transaction
    ///
    /// Note: This is a blocking operation. Use with care in async contexts.
    pub fn transaction<F, R>(&self, f: F) -> PoolResult<R>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<R>,
    {
        self.transaction_count.fetch_add(1, Ordering::Relaxed);
        let mut conn = self.conn.lock();

        let tx = conn
            .transaction()
            .map_err(|e| PoolError::Database(format!("Failed to start transaction: {}", e)))?;

        let result = f(&tx);

        match result {
            Ok(value) => {
                tx.commit()
                    .map_err(|e| PoolError::Database(format!("Failed to commit: {}", e)))?;
                Ok(value)
            }
            Err(e) => {
                // Transaction will be rolled back on drop
                Err(PoolError::Database(format!("Transaction failed: {}", e)))
            }
        }
    }

    /// Check if the connection is valid by executing a simple query
    pub fn ping(&self) -> bool {
        let conn = self.conn.lock();
        conn.query_row("SELECT 1", [], |_row| Ok(())).is_ok()
    }
}

/// Factory for creating database connections
pub struct DatabaseConnectionFactory {
    config: DatabaseConfig,
    counter: AtomicU64,
}

impl DatabaseConnectionFactory {
    /// Create a new factory with the given configuration
    pub fn new(config: DatabaseConfig) -> Self {
        Self {
            config,
            counter: AtomicU64::new(0),
        }
    }

    /// Create a factory for an in-memory database
    pub fn in_memory() -> Self {
        Self::new(DatabaseConfig::in_memory())
    }

    /// Create a factory for a file-based database
    pub fn file(path: impl Into<String>) -> Self {
        Self::new(DatabaseConfig::file(path))
    }
}

#[async_trait]
impl ConnectionFactory<DatabaseConnection> for DatabaseConnectionFactory {
    type Error = PoolError;

    async fn create(&self) -> Result<DatabaseConnection, Self::Error> {
        // Note: SQLite connection creation is synchronous, but we wrap it
        // in a spawn_blocking to avoid blocking the async runtime
        let config = self.config.clone();
        let id = self.counter.fetch_add(1, Ordering::SeqCst);

        let conn = tokio::task::spawn_blocking(move || config.open_connection())
            .await
            .map_err(|e| PoolError::ConnectionFailed(format!("Task join error: {}", e)))??;

        debug!(conn_id = id, path = %self.config.path, "Created new database connection");

        Ok(DatabaseConnection::new(conn, self.config.clone(), id))
    }
}

#[async_trait]
impl HealthCheck<DatabaseConnection> for DatabaseConnectionFactory {
    async fn check(&self, conn: &DatabaseConnection) -> bool {
        // Basic check: is the connection marked healthy?
        if !conn.is_healthy() {
            return false;
        }

        // Try a simple query synchronously since DatabaseConnection
        // uses a Mutex internally and ping() is a quick operation
        let healthy = conn.ping();

        if !healthy {
            trace!(conn_id = conn.id, "Database connection health check failed");
        }

        healthy
    }

    fn description(&self) -> &str {
        "SQLite database health check"
    }
}

/// Type alias for a database connection pool
pub type DatabasePool = Pool<DatabaseConnection>;

/// Create a new in-memory database pool
pub fn create_memory_pool() -> PoolResult<Arc<DatabasePool>> {
    let pool_config = PoolConfig::default()
        .with_min_connections(1)
        .with_max_connections(5)
        .with_pool_name("memory_db");

    let factory = Arc::new(DatabaseConnectionFactory::in_memory());
    DatabasePool::new(pool_config, factory)
}

/// Create a new file-based database pool
pub fn create_file_pool(path: impl Into<String>) -> PoolResult<Arc<DatabasePool>> {
    let pool_config = PoolConfig::default()
        .with_min_connections(1)
        .with_max_connections(10)
        .with_pool_name("file_db");

    let factory = Arc::new(DatabaseConnectionFactory::file(path));
    DatabasePool::new(pool_config, factory)
}

/// Create a database pool with custom configuration
pub fn create_pool_with_config(
    db_config: DatabaseConfig,
    pool_config: PoolConfig,
) -> PoolResult<Arc<DatabasePool>> {
    let factory = Arc::new(DatabaseConnectionFactory::new(db_config));
    DatabasePool::new(pool_config, factory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        assert_eq!(config.path, ":memory:");
        assert!(config.create_if_missing);
        assert!(!config.read_only);
        assert!(config.wal_mode);
        assert!(config.foreign_keys);
    }

    #[test]
    fn test_database_config_builder() {
        let config = DatabaseConfig::new()
            .with_path("/tmp/test.db")
            .with_busy_timeout(Duration::from_secs(10))
            .with_cache_size_kb(128000)
            .with_wal_mode(true);

        assert_eq!(config.path, "/tmp/test.db");
        assert_eq!(config.busy_timeout_ms, 10000);
        assert_eq!(config.cache_size, Some(-128000));
    }

    #[test]
    fn test_database_config_in_memory() {
        let config = DatabaseConfig::in_memory();
        assert_eq!(config.path, ":memory:");
        assert!(!config.wal_mode); // WAL not supported for in-memory
    }

    #[test]
    fn test_database_config_file() {
        let config = DatabaseConfig::file("/tmp/test.db");
        assert_eq!(config.path, "/tmp/test.db");
        assert!(config.wal_mode);
    }

    #[tokio::test]
    async fn test_database_factory_create_memory() {
        let factory = DatabaseConnectionFactory::in_memory();
        let conn = factory.create().await.unwrap();

        assert_eq!(conn.id(), 0);
        assert!(conn.is_healthy());
        assert_eq!(conn.query_count(), 0);
    }

    #[tokio::test]
    async fn test_database_connection_execute() {
        let factory = DatabaseConnectionFactory::in_memory();
        let conn = factory.create().await.unwrap();

        // Create a table
        conn.execute_batch(
            "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);",
        )
        .unwrap();

        // Insert data
        let rows = conn
            .execute("INSERT INTO test (name) VALUES (?1)", &[&"test"])
            .unwrap();
        assert_eq!(rows, 1);

        // Query data
        let name: String = conn
            .query_row("SELECT name FROM test WHERE id = 1", &[], |row| row.get(0))
            .unwrap();
        assert_eq!(name, "test");

        assert_eq!(conn.query_count(), 3);
    }

    #[tokio::test]
    async fn test_database_connection_transaction() {
        let factory = DatabaseConnectionFactory::in_memory();
        let conn = factory.create().await.unwrap();

        conn.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);")
            .unwrap();

        // Successful transaction
        let result = conn.transaction(|tx| {
            tx.execute("INSERT INTO test (value) VALUES (1)", [])?;
            tx.execute("INSERT INTO test (value) VALUES (2)", [])?;
            Ok(())
        });
        assert!(result.is_ok());

        // Verify data was committed
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM test", &[], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);

        assert_eq!(conn.transaction_count(), 1);
    }

    #[tokio::test]
    async fn test_database_connection_ping() {
        let factory = DatabaseConnectionFactory::in_memory();
        let conn = factory.create().await.unwrap();

        assert!(conn.ping());
    }

    #[tokio::test]
    async fn test_database_factory_health_check() {
        let factory = DatabaseConnectionFactory::in_memory();
        let conn = factory.create().await.unwrap();

        assert!(factory.check(&conn).await);

        // Mark as unhealthy
        conn.mark_unhealthy();
        assert!(!factory.check(&conn).await);
    }

    #[tokio::test]
    async fn test_create_memory_pool() {
        let pool = create_memory_pool().unwrap();

        // Get a connection
        let conn = pool.get().await.unwrap();
        assert!(conn.ping());
    }

    #[tokio::test]
    async fn test_database_pool_concurrent_access() {
        // Use a shared in-memory database so all pooled connections see the same data
        let db_config = DatabaseConfig::new()
            .with_path("file:concurrent_test?mode=memory&cache=shared")
            .with_wal_mode(false);

        let pool_config = PoolConfig::default()
            .with_min_connections(1)
            .with_max_connections(5)
            .with_pool_name("concurrent_test");

        let pool = create_pool_with_config(db_config, pool_config).unwrap();

        // First connection creates the table
        {
            let conn = pool.get().await.unwrap();
            conn.execute_batch("CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, value INTEGER);")
                .unwrap();
            conn.execute("INSERT INTO counter (id, value) VALUES (1, 0)", &[])
                .unwrap();
        }

        // Sequential increments (SQLite connections contain non-Send guards)
        for _ in 0..10 {
            let conn = pool.get().await.unwrap();
            conn.transaction(|tx| {
                tx.execute(
                    "UPDATE counter SET value = value + 1 WHERE id = 1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        }

        // Verify final value
        let conn = pool.get().await.unwrap();
        let value: i32 = conn
            .query_row("SELECT value FROM counter WHERE id = 1", &[], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, 10);
    }

    #[test]
    fn test_database_config_serialization() {
        let config = DatabaseConfig::new()
            .with_path("/tmp/test.db")
            .with_wal_mode(true);

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DatabaseConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.path, config.path);
        assert_eq!(deserialized.wal_mode, config.wal_mode);
    }
}
