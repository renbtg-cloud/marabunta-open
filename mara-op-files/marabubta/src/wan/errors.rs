// Marabunta - Licensed under the MIT License.
//! Error types for WAN bootstrap and NAT traversal
//!
//! Provides comprehensive error handling for all WAN-related operations.

use std::net::SocketAddr;
use thiserror::Error;

use super::protocol::{CoordinatorId, NodeId, RelaySessionId, SessionId};

/// Main error type for bootstrap operations
#[derive(Error, Debug)]
pub enum BootstrapError {
    // Registration errors
    #[error("Coordinator already registered: {0}")]
    CoordinatorAlreadyRegistered(CoordinatorId),

    #[error("Coordinator not found: {0}")]
    CoordinatorNotFound(CoordinatorId),

    #[error("Maximum coordinators reached: {0}")]
    MaxCoordinatorsReached(usize),

    #[error("Invalid region: {0}")]
    InvalidRegion(String),

    #[error("Region not allowed: {0}")]
    RegionNotAllowed(String),

    // Authentication errors
    #[error("Authentication required")]
    AuthenticationRequired,

    #[error("Invalid authentication token")]
    InvalidAuthToken,

    #[error("Authentication token expired")]
    AuthTokenExpired,

    #[error("Permission denied for coordinator: {0}")]
    PermissionDenied(CoordinatorId),

    // Connection errors
    #[error("Connection failed to {0}: {1}")]
    ConnectionFailed(SocketAddr, String),

    #[error("Connection closed unexpectedly")]
    ConnectionClosed,

    #[error("Connection timeout")]
    ConnectionTimeout,

    #[error("All bootstrap servers unavailable")]
    NoServersAvailable,

    // Protocol errors
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Invalid message format: {0}")]
    InvalidMessage(String),

    #[error("Message too large: {0} bytes (max: {1})")]
    MessageTooLarge(usize, usize),

    #[error("Unexpected response type")]
    UnexpectedResponse,

    // Server errors
    #[error("Server not running")]
    ServerNotRunning,

    #[error("Server already running")]
    ServerAlreadyRunning,

    #[error("Failed to bind to address {0}: {1}")]
    BindFailed(SocketAddr, String),

    // Generic errors
    #[error("Internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<serde_json::Error> for BootstrapError {
    fn from(err: serde_json::Error) -> Self {
        BootstrapError::Serialization(err.to_string())
    }
}

/// Error type for NAT traversal operations
#[derive(Error, Debug)]
pub enum NatError {
    #[error("STUN request failed: {0}")]
    StunFailed(String),

    #[error("NAT type detection failed: {0}")]
    NatDetectionFailed(String),

    #[error("Hole punch session not found: {0}")]
    SessionNotFound(SessionId),

    #[error("Hole punch session expired: {0}")]
    SessionExpired(SessionId),

    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),

    #[error("Peer not reachable: {0}")]
    PeerNotReachable(NodeId),

    #[error("Hole punch failed after {0} attempts")]
    HolePunchFailed(u32),

    #[error("Symmetric NAT detected - direct connection not possible")]
    SymmetricNatBlocked,

    #[error("Timeout waiting for peer")]
    PeerTimeout,

    #[error("Invalid punch instructions")]
    InvalidInstructions,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Error type for relay operations
#[derive(Error, Debug)]
pub enum RelayError {
    #[error("Maximum relay sessions reached: {0}")]
    MaxSessionsReached(usize),

    #[error("Relay session not found: {0}")]
    SessionNotFound(RelaySessionId),

    #[error("Relay session expired: {0}")]
    SessionExpired(RelaySessionId),

    #[error("Node not in session: node={0}, session={1}")]
    NodeNotInSession(NodeId, RelaySessionId),

    #[error("Bandwidth limit exceeded for session: {0}")]
    BandwidthLimitExceeded(RelaySessionId),

    #[error("Session timeout: {0}")]
    SessionTimeout(RelaySessionId),

    #[error("Invalid relay data")]
    InvalidData,

    #[error("Peer disconnected: {0}")]
    PeerDisconnected(NodeId),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for bootstrap operations
pub type BootstrapResult<T> = Result<T, BootstrapError>;

/// Result type for NAT traversal operations
pub type NatResult<T> = Result<T, NatError>;

/// Result type for relay operations
pub type RelayResult<T> = Result<T, RelayError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_error_display() {
        let err = BootstrapError::MaxCoordinatorsReached(100);
        assert!(err.to_string().contains("100"));

        let err = BootstrapError::ConnectionFailed(
            "127.0.0.1:9000".parse().unwrap(),
            "refused".to_string(),
        );
        assert!(err.to_string().contains("127.0.0.1:9000"));
        assert!(err.to_string().contains("refused"));
    }

    #[test]
    fn test_nat_error_display() {
        let session_id = SessionId::new();
        let err = NatError::SessionExpired(session_id);
        assert!(err.to_string().contains(&session_id.to_string()));
    }

    #[test]
    fn test_relay_error_display() {
        let session_id = RelaySessionId::new();
        let err = RelayError::BandwidthLimitExceeded(session_id);
        assert!(err.to_string().contains(&session_id.to_string()));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let bootstrap_err: BootstrapError = io_err.into();
        assert!(matches!(bootstrap_err, BootstrapError::Io(_)));
    }

    #[test]
    fn test_error_from_serde() {
        let json_err = serde_json::from_str::<String>("invalid").unwrap_err();
        let bootstrap_err: BootstrapError = json_err.into();
        assert!(matches!(bootstrap_err, BootstrapError::Serialization(_)));
    }
}
