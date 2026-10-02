// Marabunta - Licensed under the MIT License.
//! Network layer error types for Phantom Protocol.
//!
//! This module defines all error types used throughout the anonymous network layer.
//! Errors are designed to be informative for debugging while avoiding information
//! leakage about network topology or routing decisions.
//!
//! # Security Notes
//!
//! - Error messages avoid revealing relay identities or circuit paths
//! - Timing-safe error handling prevents side-channel attacks
//! - Circuit and relay IDs are opaque and do not leak routing information

use std::fmt;
use thiserror::Error;

/// Unique identifier for an onion routing circuit.
///
/// Circuit IDs are cryptographically random and do not reveal any information
/// about the circuit's path or endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CircuitId(pub [u8; 16]);

impl CircuitId {
    /// Create a new random circuit ID.
    pub fn random() -> Self {
        use rand::RngCore;
        let mut id = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut id);
        CircuitId(id)
    }

    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        CircuitId(bytes)
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for CircuitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only show first 4 bytes to avoid leaking full ID
        write!(f, "circuit-{:02x}{:02x}...", self.0[0], self.0[1])
    }
}

impl Default for CircuitId {
    fn default() -> Self {
        Self::random()
    }
}

/// Unique identifier for a relay node.
///
/// Relay IDs are derived from the relay's public key and do not reveal
/// network location or other identifying information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelayId(pub [u8; 32]);

impl RelayId {
    /// Create a new relay ID from public key hash.
    pub fn from_public_key(public_key: &[u8; 32]) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"relay-id-v1");
        hasher.update(public_key);
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        RelayId(id)
    }

    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        RelayId(bytes)
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for RelayId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only show first 4 bytes to avoid leaking full ID
        write!(f, "relay-{:02x}{:02x}...", self.0[0], self.0[1])
    }
}

/// Unique identifier for a peer in the mesh network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId(pub [u8; 32]);

impl PeerId {
    /// Create a new random peer ID.
    pub fn random() -> Self {
        use rand::RngCore;
        let mut id = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut id);
        PeerId(id)
    }

    /// Create from public key.
    pub fn from_public_key(public_key: &[u8; 32]) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"peer-id-v1");
        hasher.update(public_key);
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        PeerId(id)
    }

    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        PeerId(bytes)
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peer-{:02x}{:02x}...", self.0[0], self.0[1])
    }
}

/// Errors that can occur during onion routing operations.
///
/// These errors are designed to provide useful debugging information while
/// avoiding leakage of sensitive routing information.
#[derive(Debug, Error)]
pub enum OnionError {
    /// Failed to build a circuit through relay nodes.
    ///
    /// This can occur when not enough relays are available or when
    /// relay negotiation fails.
    #[error("Failed to build circuit: {0}")]
    CircuitBuildFailed(String),

    /// The specified circuit was not found.
    ///
    /// This can happen if the circuit ID is invalid, the circuit has expired,
    /// or the circuit was destroyed.
    #[error("Circuit not found: {0}")]
    CircuitNotFound(CircuitId),

    /// The circuit has expired due to timeout.
    ///
    /// Circuits have a limited lifetime to prevent long-term correlation attacks.
    #[error("Circuit expired")]
    CircuitExpired,

    /// A relay node in the circuit path is unreachable.
    ///
    /// The relay ID is intentionally vague to avoid revealing circuit topology.
    #[error("Relay unreachable: {0}")]
    RelayUnreachable(RelayId),

    /// Decryption failed at a hop in the circuit.
    ///
    /// The hop number is relative and does not reveal absolute position.
    #[error("Decryption failed at hop {0}")]
    DecryptionFailed(usize),

    /// The cell payload exceeds the maximum allowed size.
    ///
    /// All cells must be fixed-size for traffic analysis resistance.
    #[error("Cell too large: {size} > {max}")]
    CellTooLarge {
        /// Actual size of the payload
        size: usize,
        /// Maximum allowed size
        max: usize,
    },

    /// Not enough relays available to build a circuit.
    #[error("Insufficient relays: need {needed}, have {available}")]
    InsufficientRelays {
        /// Number of relays needed
        needed: usize,
        /// Number of relays available
        available: usize,
    },

    /// Key exchange with a relay failed.
    #[error("Key exchange failed")]
    KeyExchangeFailed,

    /// Cell routing failed.
    #[error("Cell routing failed: {0}")]
    RoutingFailed(String),

    /// Network I/O error.
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Circuit is busy and cannot accept more operations.
    #[error("Circuit busy")]
    CircuitBusy,

    /// Invalid cell format received.
    #[error("Invalid cell format")]
    InvalidCellFormat,

    /// The relay registry is not available.
    #[error("Relay registry unavailable")]
    RegistryUnavailable,

    /// Timeout waiting for operation to complete.
    #[error("Operation timed out")]
    Timeout,

    /// Internal error in the onion router.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for OnionError {
    fn from(err: std::io::Error) -> Self {
        OnionError::NetworkError(err.to_string())
    }
}

/// Errors that can occur in the peer mesh network.
#[derive(Debug, Error)]
pub enum MeshError {
    /// Bootstrap into the network failed.
    ///
    /// This typically means no bootstrap nodes were reachable.
    #[error("Bootstrap failed: no peers reachable")]
    BootstrapFailed,

    /// The maximum number of peer connections has been reached.
    #[error("Peer limit reached")]
    PeerLimitReached,

    /// The specified peer was not found.
    #[error("Peer not found: {0}")]
    PeerNotFound(PeerId),

    /// Connection to a peer failed.
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Peer protocol violation detected.
    #[error("Protocol violation from peer: {0}")]
    ProtocolViolation(String),

    /// Peer authentication failed.
    #[error("Peer authentication failed")]
    AuthenticationFailed,

    /// Gossip protocol error.
    #[error("Gossip error: {0}")]
    GossipError(String),

    /// Network I/O error.
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Peer has invalid or expired certificate.
    #[error("Invalid peer certificate")]
    InvalidCertificate,

    /// Peer was banned due to misbehavior.
    #[error("Peer is banned")]
    PeerBanned,

    /// Internal mesh error.
    #[error("Internal mesh error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for MeshError {
    fn from(err: std::io::Error) -> Self {
        MeshError::NetworkError(err.to_string())
    }
}

/// Errors that can occur during cover traffic generation.
#[derive(Debug, Error)]
pub enum CoverError {
    /// Cover traffic generator is not running.
    #[error("Cover traffic generator not running")]
    NotRunning,

    /// Failed to send cover cell.
    #[error("Failed to send cover cell: {0}")]
    SendFailed(String),

    /// Traffic shaping queue is full.
    #[error("Traffic shaping queue full")]
    QueueFull,

    /// Circuit for cover traffic is unavailable.
    #[error("No circuit available for cover traffic")]
    NoCircuit,

    /// Underlying onion routing error.
    #[error("Onion routing error: {0}")]
    OnionError(#[from] OnionError),
}

/// Errors that can occur during gossip protocol operations.
#[derive(Debug, Error)]
pub enum GossipError {
    /// No peers available for gossip exchange.
    #[error("No peers available for gossip")]
    NoPeersAvailable,

    /// Gossip message was invalid or malformed.
    #[error("Invalid gossip message: {0}")]
    InvalidMessage(String),

    /// Gossip round timed out.
    #[error("Gossip round timed out")]
    Timeout,

    /// Rate limit exceeded for gossip messages.
    #[error("Gossip rate limit exceeded")]
    RateLimitExceeded,

    /// Underlying mesh error.
    #[error("Mesh error: {0}")]
    MeshError(#[from] MeshError),
}

/// Errors that can occur during fingerprint security_domain operations.
#[derive(Debug, Error)]
pub enum SecurityDomainError {
    /// Identity rotation failed.
    #[error("Identity rotation failed: {0}")]
    RotationFailed(String),

    /// Profile generation failed.
    #[error("Profile generation failed: {0}")]
    ProfileGenerationFailed(String),

    /// Timing normalization failed.
    #[error("Timing normalization failed")]
    TimingFailed,

    /// Underlying onion routing error.
    #[error("Onion routing error: {0}")]
    OnionError(#[from] OnionError),
}

/// Reasons for removing a peer from the mesh.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoveReason {
    /// Peer timed out (no response).
    Timeout,
    /// Peer violated protocol rules.
    ProtocolViolation,
    /// Peer was explicitly banned.
    Banned,
    /// Peer disconnected gracefully.
    Disconnected,
    /// Peer replaced due to mesh rebalancing.
    Replaced,
    /// Connection error.
    ConnectionError,
}

impl fmt::Display for RemoveReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoveReason::Timeout => write!(f, "timeout"),
            RemoveReason::ProtocolViolation => write!(f, "protocol violation"),
            RemoveReason::Banned => write!(f, "banned"),
            RemoveReason::Disconnected => write!(f, "disconnected"),
            RemoveReason::Replaced => write!(f, "replaced"),
            RemoveReason::ConnectionError => write!(f, "connection error"),
        }
    }
}

/// Priority levels for traffic shaping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[derive(Default)]
pub enum Priority {
    /// Low priority - can be delayed significantly.
    Low = 0,
    /// Normal priority - standard traffic.
    #[default]
    Normal = 1,
    /// High priority - minimize delay.
    High = 2,
    /// Critical priority - send immediately.
    Critical = 3,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_id_random() {
        let id1 = CircuitId::random();
        let id2 = CircuitId::random();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_circuit_id_display_truncated() {
        let mut bytes = [0u8; 16];
        bytes[0] = 0xab;
        bytes[1] = 0xcd;
        let id = CircuitId::from_bytes(bytes);
        let display = format!("{}", id);
        // Should not contain full ID
        assert!(display.len() < 40);
        assert!(display.starts_with("circuit-"));
    }

    #[test]
    fn test_relay_id_from_public_key() {
        let pk1 = [1u8; 32];
        let pk2 = [2u8; 32];
        let id1 = RelayId::from_public_key(&pk1);
        let id2 = RelayId::from_public_key(&pk2);
        assert_ne!(id1, id2);

        // Same key should produce same ID
        let id1_again = RelayId::from_public_key(&pk1);
        assert_eq!(id1, id1_again);
    }

    #[test]
    fn test_peer_id_from_public_key() {
        let pk = [42u8; 32];
        let id1 = PeerId::from_public_key(&pk);
        let id2 = PeerId::from_public_key(&pk);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_onion_error_display() {
        let err = OnionError::CircuitExpired;
        assert_eq!(err.to_string(), "Circuit expired");

        let err = OnionError::CellTooLarge {
            size: 1024,
            max: 512,
        };
        assert_eq!(err.to_string(), "Cell too large: 1024 > 512");

        let err = OnionError::DecryptionFailed(2);
        assert_eq!(err.to_string(), "Decryption failed at hop 2");
    }

    #[test]
    fn test_mesh_error_display() {
        let err = MeshError::BootstrapFailed;
        assert_eq!(err.to_string(), "Bootstrap failed: no peers reachable");

        let err = MeshError::PeerLimitReached;
        assert_eq!(err.to_string(), "Peer limit reached");
    }

    #[test]
    fn test_remove_reason_display() {
        assert_eq!(RemoveReason::Timeout.to_string(), "timeout");
        assert_eq!(
            RemoveReason::ProtocolViolation.to_string(),
            "protocol violation"
        );
        assert_eq!(RemoveReason::Banned.to_string(), "banned");
    }

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::Low < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "test");
        let onion_err: OnionError = io_err.into();
        assert!(matches!(onion_err, OnionError::NetworkError(_)));
    }
}
