// Marabunta - Licensed under the MIT License.
//! Error types for the connection pool module
//!
//! This module defines error types used throughout the connection pooling system.

use std::fmt;
use thiserror::Error;

/// Errors that can occur in the connection pool
#[derive(Error, Debug)]
pub enum PoolError {
    /// Pool is exhausted and timeout expired waiting for a connection
    #[error("Connection acquisition timeout: pool exhausted")]
    Timeout,

    /// Pool has been closed/shutdown
    #[error("Pool has been closed")]
    PoolClosed,

    /// Failed to create a new connection
    #[error("Failed to create connection: {0}")]
    ConnectionFailed(String),

    /// Connection health check failed
    #[error("Connection health check failed: {0}")]
    HealthCheckFailed(String),

    /// Connection exceeded its maximum lifetime
    #[error("Connection exceeded maximum lifetime")]
    MaxLifetimeExceeded,

    /// Connection was idle for too long
    #[error("Connection exceeded idle timeout")]
    IdleTimeoutExceeded,

    /// Invalid pool configuration
    #[error("Invalid pool configuration: {0}")]
    InvalidConfig(String),

    /// Internal pool error
    #[error("Internal pool error: {0}")]
    Internal(String),

    /// Database-specific error
    #[error("Database error: {0}")]
    Database(String),

    /// HTTP client error
    #[error("HTTP client error: {0}")]
    HttpClient(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl PoolError {
    /// Check if the error is a timeout error
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout)
    }

    /// Check if the error is a pool closed error
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::PoolClosed)
    }

    /// Check if the error is a connection error
    pub fn is_connection_error(&self) -> bool {
        matches!(
            self,
            Self::ConnectionFailed(_) | Self::HealthCheckFailed(_) | Self::Database(_)
        )
    }

    /// Check if the error is transient and can be retried
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::ConnectionFailed(_) | Self::HealthCheckFailed(_)
        )
    }

    /// Create a connection failed error from any error type
    pub fn connection_failed<E: fmt::Display>(err: E) -> Self {
        Self::ConnectionFailed(err.to_string())
    }

    /// Create a database error from any error type
    pub fn database<E: fmt::Display>(err: E) -> Self {
        Self::Database(err.to_string())
    }

    /// Create an HTTP client error from any error type
    pub fn http_client<E: fmt::Display>(err: E) -> Self {
        Self::HttpClient(err.to_string())
    }
}

/// Result type alias for pool operations
pub type PoolResult<T> = Result<T, PoolError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_variants() {
        let timeout = PoolError::Timeout;
        assert!(timeout.is_timeout());
        assert!(!timeout.is_closed());
        assert!(timeout.is_transient());

        let closed = PoolError::PoolClosed;
        assert!(closed.is_closed());
        assert!(!closed.is_timeout());

        let conn_failed = PoolError::ConnectionFailed("test".into());
        assert!(conn_failed.is_connection_error());
        assert!(conn_failed.is_transient());

        let db_err = PoolError::Database("db error".into());
        assert!(db_err.is_connection_error());
        assert!(!db_err.is_transient());
    }

    #[test]
    fn test_error_display() {
        let err = PoolError::Timeout;
        assert!(err.to_string().contains("timeout"));

        let err = PoolError::ConnectionFailed("connection refused".into());
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn test_error_constructors() {
        let err = PoolError::connection_failed(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        assert!(matches!(err, PoolError::ConnectionFailed(_)));

        let err = PoolError::database("constraint violation");
        assert!(matches!(err, PoolError::Database(_)));

        let err = PoolError::http_client("request timeout");
        assert!(matches!(err, PoolError::HttpClient(_)));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let pool_err: PoolError = io_err.into();
        assert!(matches!(pool_err, PoolError::Io(_)));
    }
}
