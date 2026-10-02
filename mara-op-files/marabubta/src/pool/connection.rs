// Marabunta - Licensed under the MIT License.
//! Connection types and traits for the connection pool
//!
//! This module defines the core traits for creating connections and the
//! RAII wrapper that handles automatic return to the pool.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::mpsc;

use super::errors::PoolError;

/// Factory trait for creating new connections
///
/// Implement this trait to define how connections are created for your
/// specific connection type.
///
/// # Example
///
/// ```rust,no_run
/// use async_trait::async_trait;
/// use marabunta_compute::pool::{ConnectionFactory, PoolError};
///
/// struct DatabaseConnection {
///     url: String,
/// }
///
/// struct DatabaseFactory {
///     url: String,
/// }
///
/// #[async_trait]
/// impl ConnectionFactory<DatabaseConnection> for DatabaseFactory {
///     type Error = PoolError;
///
///     async fn create(&self) -> Result<DatabaseConnection, Self::Error> {
///         // Connect to database
///         Ok(DatabaseConnection {
///             url: self.url.clone(),
///         })
///     }
/// }
/// ```
#[async_trait]
pub trait ConnectionFactory<T>: Send + Sync {
    /// Error type returned when connection creation fails
    type Error: Into<PoolError> + Send;

    /// Create a new connection
    ///
    /// This method is called when the pool needs to create a new connection,
    /// either to fill the minimum pool size or to handle increased demand.
    async fn create(&self) -> Result<T, Self::Error>;
}

/// Metadata about a pooled connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Unique identifier for this connection
    pub id: u64,
    /// When this connection was created
    pub created_at: Instant,
    /// When this connection was last used
    pub last_used_at: Instant,
    /// Number of times this connection has been used
    pub use_count: u64,
}

impl ConnectionInfo {
    /// Create new connection info
    pub fn new(id: u64) -> Self {
        let now = Instant::now();
        Self {
            id,
            created_at: now,
            last_used_at: now,
            use_count: 0,
        }
    }

    /// Get the age of this connection
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Get the time since this connection was last used
    pub fn idle_time(&self) -> std::time::Duration {
        self.last_used_at.elapsed()
    }

    /// Record that the connection was used
    pub fn mark_used(&mut self) {
        self.last_used_at = Instant::now();
        self.use_count += 1;
    }
}

/// Internal wrapper for a connection with its metadata
pub(crate) struct PooledConnectionInner<T> {
    /// The actual connection
    pub conn: T,
    /// Metadata about the connection
    pub info: ConnectionInfo,
}

impl<T> PooledConnectionInner<T> {
    pub fn new(conn: T, id: u64) -> Self {
        Self {
            conn,
            info: ConnectionInfo::new(id),
        }
    }
}

/// RAII wrapper for a pooled connection
///
/// When this struct is dropped, the connection is automatically returned
/// to the pool for reuse. The wrapper provides transparent access to the
/// underlying connection through `Deref` and `DerefMut`.
///
/// # Example
///
/// ```rust,no_run
/// # use marabunta_compute::pool::{Pool, PoolConfig, PooledConnection};
/// # async fn example(pool: &Pool<String>) -> Result<(), Box<dyn std::error::Error>> {
/// // Get a connection from the pool
/// let conn = pool.get().await?;
///
/// // Use the connection (accesses underlying type via Deref)
/// println!("Connection: {}", *conn);
///
/// // Connection is automatically returned when `conn` goes out of scope
/// # Ok(())
/// # }
/// ```
pub struct PooledConnection<T> {
    /// The connection (Option for taking on drop)
    inner: Option<PooledConnectionInner<T>>,
    /// Channel to return the connection to the pool
    return_tx: mpsc::UnboundedSender<PooledConnectionInner<T>>,
    /// Whether this connection should be discarded instead of returned
    discard: bool,
}

impl<T> PooledConnection<T> {
    /// Create a new pooled connection wrapper
    pub(crate) fn new(
        inner: PooledConnectionInner<T>,
        return_tx: mpsc::UnboundedSender<PooledConnectionInner<T>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            return_tx,
            discard: false,
        }
    }

    /// Get the connection ID
    pub fn id(&self) -> u64 {
        self.inner.as_ref().map(|i| i.info.id).unwrap_or(0)
    }

    /// Get connection info
    pub fn info(&self) -> Option<&ConnectionInfo> {
        self.inner.as_ref().map(|i| &i.info)
    }

    /// Get the age of this connection
    pub fn age(&self) -> std::time::Duration {
        self.inner
            .as_ref()
            .map(|i| i.info.age())
            .unwrap_or_default()
    }

    /// Get the time since this connection was last used
    pub fn idle_time(&self) -> std::time::Duration {
        self.inner
            .as_ref()
            .map(|i| i.info.idle_time())
            .unwrap_or_default()
    }

    /// Get the number of times this connection has been used
    pub fn use_count(&self) -> u64 {
        self.inner.as_ref().map(|i| i.info.use_count).unwrap_or(0)
    }

    /// Mark this connection to be discarded instead of returned to the pool
    ///
    /// Use this when you know the connection is in a bad state and should
    /// not be reused.
    pub fn discard(&mut self) {
        self.discard = true;
    }

    /// Take ownership of the inner connection, preventing it from being
    /// returned to the pool
    ///
    /// This is useful when you need to own the connection and manage its
    /// lifecycle yourself.
    pub fn take(mut self) -> Option<T> {
        self.discard = true;
        self.inner.take().map(|i| i.conn)
    }
}

impl<T> Deref for PooledConnection<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner.as_ref().expect("connection already taken").conn
    }
}

impl<T> DerefMut for PooledConnection<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .inner
            .as_mut()
            .expect("connection already taken")
            .conn
    }
}

impl<T> Drop for PooledConnection<T> {
    fn drop(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            if !self.discard {
                // Update usage info
                inner.info.mark_used();
                // Return to pool (ignore errors if pool was dropped)
                let _ = self.return_tx.send(inner);
            }
            // If discard is true, the connection is simply dropped
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for PooledConnection<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledConnection")
            .field("id", &self.id())
            .field("discard", &self.discard)
            .field("connection", &self.inner.as_ref().map(|i| &i.conn))
            .finish()
    }
}

/// Global connection ID generator
static CONNECTION_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique connection ID
pub fn next_connection_id() -> u64 {
    CONNECTION_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn test_connection_info_new() {
        let info = ConnectionInfo::new(42);
        assert_eq!(info.id, 42);
        assert_eq!(info.use_count, 0);
        assert!(info.age() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_connection_info_mark_used() {
        let mut info = ConnectionInfo::new(1);
        std::thread::sleep(std::time::Duration::from_millis(10));
        info.mark_used();
        assert_eq!(info.use_count, 1);
        assert!(info.idle_time() < info.age());
    }

    #[tokio::test]
    async fn test_pooled_connection_deref() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let inner = PooledConnectionInner::new("test_value".to_string(), 1);
        let conn = PooledConnection::new(inner, tx);

        // Test Deref
        assert_eq!(*conn, "test_value");

        // Drop to return to pool
        drop(conn);

        // Should receive the connection back
        let returned = rx.recv().await;
        assert!(returned.is_some());
        assert_eq!(returned.unwrap().conn, "test_value");
    }

    #[tokio::test]
    async fn test_pooled_connection_deref_mut() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let inner = PooledConnectionInner::new(vec![1, 2, 3], 1);
        let mut conn = PooledConnection::new(inner, tx);

        // Test DerefMut
        conn.push(4);
        assert_eq!(*conn, vec![1, 2, 3, 4]);

        drop(conn);
        let returned = rx.recv().await.unwrap();
        assert_eq!(returned.conn, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_pooled_connection_discard() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Keep a clone of the sender alive so the channel doesn't close
        // when the PooledConnection is dropped, which would cause recv()
        // to return None and match the wrong select arm.
        let _tx_keepalive = tx.clone();
        let inner = PooledConnectionInner::new("discard_me".to_string(), 1);
        let mut conn = PooledConnection::new(inner, tx);

        // Mark for discard
        conn.discard();
        drop(conn);

        // Should not receive anything (connection was discarded)
        tokio::select! {
            _ = rx.recv() => panic!("Should not receive discarded connection"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
    }

    #[tokio::test]
    async fn test_pooled_connection_take() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Keep a clone of the sender alive so the channel doesn't close
        // when the PooledConnection is dropped after take(), which would
        // cause recv() to return None and match the wrong select arm.
        let _tx_keepalive = tx.clone();
        let inner = PooledConnectionInner::new("take_me".to_string(), 1);
        let conn = PooledConnection::new(inner, tx);

        // Take the connection
        let taken = conn.take();
        assert_eq!(taken, Some("take_me".to_string()));

        // Should not receive anything (connection was taken)
        tokio::select! {
            _ = rx.recv() => panic!("Should not receive taken connection"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
    }

    #[test]
    fn test_next_connection_id() {
        let id1 = next_connection_id();
        let id2 = next_connection_id();
        assert!(id2 > id1);
    }

    #[tokio::test]
    async fn test_pooled_connection_info() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let inner = PooledConnectionInner::new("test".to_string(), 42);
        let conn = PooledConnection::new(inner, tx);

        assert_eq!(conn.id(), 42);
        assert!(conn.info().is_some());
        assert_eq!(conn.use_count(), 0);
        assert!(conn.age() < std::time::Duration::from_secs(1));
    }
}
