// Marabunta - Licensed under the MIT License.
//! Bootstrap protocol messages and types
//!
//! Defines the wire protocol for communication between:
//! - Coordinators and the bootstrap server
//! - Nodes and the bootstrap server
//! - Bootstrap server responses
//!
//! Messages are length-prefixed JSON for simplicity and debuggability.

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::codec::{Decoder, Encoder};
use uuid::Uuid;

// ============================================================================
// Identifiers
// ============================================================================

/// Unique identifier for a coordinator in the bootstrap system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CoordinatorId(pub Uuid);

impl CoordinatorId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        Self(Uuid::from_bytes(*bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for CoordinatorId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CoordinatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "coord-{}", &self.0.to_string()[..8])
    }
}

/// Unique identifier for a node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        Self(Uuid::from_bytes(*bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", &self.0.to_string()[..8])
    }
}

/// Session identifier for hole punching
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "session-{}", &self.0.to_string()[..8])
    }
}

/// Session identifier for relay connections
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelaySessionId(pub Uuid);

impl RelaySessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RelaySessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RelaySessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "relay-{}", &self.0.to_string()[..8])
    }
}

// ============================================================================
// Authentication
// ============================================================================

/// Authentication token for coordinator operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthToken {
    /// The token value (typically a JWT or API key)
    pub token: String,
    /// When the token expires (Unix timestamp)
    pub expires_at: u64,
}

impl AuthToken {
    pub fn new(token: String, ttl: Duration) -> Self {
        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + ttl.as_secs();
        Self { token, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now >= self.expires_at
    }

    /// Hash the token for secure storage (don't store raw tokens)
    pub fn hash(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(self.token.as_bytes());
        hasher.finalize().into()
    }
}

// ============================================================================
// Registration and Heartbeat
// ============================================================================

/// Registration request from a coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorRegistration {
    /// Human-readable name for the coordinator
    pub name: String,
    /// Public address where nodes can connect
    pub public_addr: SocketAddr,
    /// Internal address (for same-network nodes)
    pub internal_addr: Option<SocketAddr>,
    /// Organization running this coordinator
    pub organization: String,
    /// Geographic region (e.g., "us-west", "eu-central")
    pub region: String,
    /// Whether this coordinator accepts phantom (anonymous BYOD) nodes
    pub accepts_phantom: bool,
    /// Current capacity information
    pub capacity: CapacityInfo,
    /// Authentication token
    pub auth_token: AuthToken,
    /// Optional metadata
    pub metadata: Option<serde_json::Value>,
}

/// Capacity information for a coordinator
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapacityInfo {
    /// Total CPU cores available in the cluster
    pub total_cores: u32,
    /// Currently available CPU cores
    pub available_cores: u32,
    /// Total memory in GB
    pub total_memory_gb: u32,
    /// Currently available memory in GB
    pub available_memory_gb: u32,
    /// Number of pending jobs in queue
    pub pending_jobs: u32,
}

/// Heartbeat from a coordinator with status update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorHeartbeat {
    /// Coordinator ID (assigned during registration)
    pub coordinator_id: CoordinatorId,
    /// Current status
    pub status: CoordinatorStatus,
    /// Auth token for verification
    pub auth_token: AuthToken,
}

/// Status update in heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorStatus {
    /// Number of active nodes
    pub node_count: u32,
    /// Updated capacity information
    pub capacity: CapacityInfo,
    /// Current health status
    pub health: HealthStatus,
    /// Optional status message
    pub message: Option<String>,
}

/// Health status of a coordinator
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// Fully operational
    Healthy,
    /// Operational but with some issues
    Degraded,
    /// Not accepting new work
    Overloaded,
    /// Shutting down gracefully
    Draining,
    /// Not operational
    Unhealthy,
}

/// Request to unregister a coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterRequest {
    pub coordinator_id: CoordinatorId,
    pub auth_token: AuthToken,
    /// Optional reason for unregistering
    pub reason: Option<String>,
}

// ============================================================================
// Discovery
// ============================================================================

/// Query for finding coordinators
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoordinatorQuery {
    /// Filter by region
    pub region: Option<String>,
    /// Filter by organization
    pub organization: Option<String>,
    /// Minimum available CPU cores
    pub min_capacity_cores: Option<u32>,
    /// Minimum available memory in GB
    pub min_capacity_memory_gb: Option<u32>,
    /// Only coordinators accepting phantom nodes
    pub accepts_phantom: Option<bool>,
    /// Maximum results to return
    pub limit: usize,
    /// Offset for pagination
    pub offset: usize,
}

impl CoordinatorQuery {
    pub fn new() -> Self {
        Self {
            limit: 10,
            ..Default::default()
        }
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn with_organization(mut self, org: impl Into<String>) -> Self {
        self.organization = Some(org.into());
        self
    }

    pub fn accepting_phantom(mut self) -> Self {
        self.accepts_phantom = Some(true);
        self
    }

    pub fn with_min_cores(mut self, cores: u32) -> Self {
        self.min_capacity_cores = Some(cores);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// Public coordinator information returned to nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorInfo {
    /// Coordinator ID
    pub id: CoordinatorId,
    /// Human-readable name
    pub name: String,
    /// Public address for connection
    pub addr: SocketAddr,
    /// Geographic region
    pub region: String,
    /// Organization
    pub organization: String,
    /// Whether phantom nodes are accepted
    pub accepts_phantom: bool,
    /// Available CPU cores
    pub available_cores: u32,
    /// Available memory in GB
    pub available_memory_gb: u32,
    /// Pending jobs
    pub pending_jobs: u32,
    /// Health status
    pub health: HealthStatus,
}

// ============================================================================
// NAT Traversal Types
// ============================================================================

/// STUN binding response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindingResponse {
    /// Client's public IP:port as seen by server
    pub public_addr: SocketAddr,
    /// Detected NAT type
    pub nat_type: NatType,
    /// Server timestamp for clock sync
    pub server_time: u64,
}

/// Type of NAT detected
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NatType {
    /// No NAT - public IP address
    NoNat,
    /// Full cone NAT - easiest traversal
    FullCone,
    /// Restricted cone NAT - medium difficulty
    RestrictedCone,
    /// Port restricted cone NAT - harder
    PortRestricted,
    /// Symmetric NAT - very hard, may need relay
    Symmetric,
    /// Could not determine NAT type
    Unknown,
}

impl NatType {
    /// Whether direct P2P connection is likely possible
    pub fn can_hole_punch(&self) -> bool {
        matches!(
            self,
            NatType::NoNat | NatType::FullCone | NatType::RestrictedCone | NatType::PortRestricted
        )
    }

    /// Whether relay is recommended
    pub fn needs_relay(&self) -> bool {
        matches!(self, NatType::Symmetric)
    }
}

/// Request to initiate hole punching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolePunchRequest {
    /// Node initiating the request
    pub node_id: NodeId,
    /// Node's public address
    pub node_addr: SocketAddr,
    /// Target node to connect to
    pub target_node: NodeId,
    /// NAT type of requesting node
    pub nat_type: NatType,
}

/// Instructions for hole punching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PunchInstructions {
    /// Session ID for this hole punch attempt
    pub session_id: SessionId,
    /// Peer's public address to punch toward
    pub peer_public_addr: SocketAddr,
    /// Peer's reported NAT type
    pub peer_nat_type: NatType,
    /// Synchronized time to start punching (Unix timestamp millis)
    pub punch_at_ms: u64,
    /// Number of packets to send
    pub packet_count: u32,
    /// Interval between packets (millis)
    pub interval_ms: u32,
    /// Fallback relay address if hole punch fails
    pub fallback_relay: Option<SocketAddr>,
}

// ============================================================================
// Relay Types
// ============================================================================

/// Request to create a relay session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayRequest {
    /// Requesting node
    pub node_id: NodeId,
    /// Target peer node
    pub peer_node: NodeId,
    /// Reason for needing relay
    pub reason: RelayReason,
}

/// Why relay is needed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayReason {
    /// Symmetric NAT blocking direct connection
    SymmetricNat,
    /// Hole punch failed
    HolePunchFailed,
    /// Corporate firewall blocking UDP
    FirewallBlocked,
    /// Fallback for reliability
    Fallback,
}

// ============================================================================
// Protocol Messages
// ============================================================================

/// All possible messages in the bootstrap protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BootstrapMessage {
    // ========== Coordinator -> Bootstrap ==========
    /// Register a new coordinator
    RegisterCoordinator(CoordinatorRegistration),
    /// Heartbeat with status update
    Heartbeat(CoordinatorHeartbeat),
    /// Unregister coordinator
    Unregister(UnregisterRequest),

    // ========== Node -> Bootstrap ==========
    /// Find available coordinators
    FindCoordinators(CoordinatorQuery),
    /// Get specific coordinator by ID
    GetCoordinator {
        id: CoordinatorId,
    },
    /// STUN binding request to discover public IP
    StunRequest {
        node_id: NodeId,
    },
    /// Request hole punch coordination
    HolePunchRequest(HolePunchRequest),
    /// Request relay session
    RelayRequest(RelayRequest),
    /// Send data through relay
    RelayData {
        session_id: RelaySessionId,
        from_node: NodeId,
        #[serde(with = "base64_serde")]
        data: Vec<u8>,
    },
    /// Close relay session
    RelayClose {
        session_id: RelaySessionId,
    },

    // ========== Bootstrap -> Client ==========
    /// Registration successful
    RegisterResponse {
        coordinator_id: CoordinatorId,
        /// Suggested heartbeat interval
        heartbeat_interval_secs: u32,
    },
    /// Heartbeat acknowledged
    HeartbeatAck {
        /// Time until next heartbeat required (secs)
        next_heartbeat_secs: u32,
    },
    /// Unregister acknowledged
    UnregisterAck,
    /// List of coordinators matching query
    CoordinatorList {
        coordinators: Vec<CoordinatorInfo>,
        /// Total count (for pagination)
        total_count: usize,
    },
    /// Single coordinator details
    CoordinatorDetails(Option<CoordinatorInfo>),
    /// STUN response with public address
    StunResponse(BindingResponse),
    /// Hole punch instructions
    HolePunchInstructions(PunchInstructions),
    /// Relay session created
    RelaySessionCreated {
        session_id: RelaySessionId,
        relay_addr: SocketAddr,
    },
    /// Relay data from peer
    RelayDataReceived {
        session_id: RelaySessionId,
        from_node: NodeId,
        #[serde(with = "base64_serde")]
        data: Vec<u8>,
    },
    /// Error response
    Error {
        code: ErrorCode,
        message: String,
    },
    /// Ping/Pong for keepalive
    Ping {
        timestamp: u64,
    },
    Pong {
        timestamp: u64,
        server_time: u64,
    },
}

/// Error codes for protocol errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    // General errors
    InvalidRequest,
    Unauthorized,
    RateLimited,
    InternalError,

    // Registration errors
    AlreadyRegistered,
    MaxCoordinatorsReached,
    InvalidRegion,
    RegionNotAllowed,

    // Query errors
    CoordinatorNotFound,
    NodeNotFound,
    InvalidQuery,

    // NAT errors
    HolePunchFailed,
    PeerUnavailable,

    // Relay errors
    MaxRelaySessionsReached,
    RelaySessionNotFound,
    BandwidthExceeded,
}

// ============================================================================
// Message Codec
// ============================================================================

/// Codec for encoding/decoding bootstrap messages
/// Format: [4 bytes length (big-endian)][JSON payload]
pub struct BootstrapCodec {
    max_message_size: usize,
}

impl BootstrapCodec {
    pub fn new() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1MB max
        }
    }

    pub fn with_max_size(max_message_size: usize) -> Self {
        Self { max_message_size }
    }

    /// Encode a message to bytes (for UDP)
    pub fn encode_bytes(msg: &BootstrapMessage) -> Result<Vec<u8>, std::io::Error> {
        serde_json::to_vec(msg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }

    /// Decode a message from bytes (for UDP)
    pub fn decode_bytes(data: &[u8]) -> Result<BootstrapMessage, std::io::Error> {
        serde_json::from_slice(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

impl Default for BootstrapCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for BootstrapCodec {
    type Item = BootstrapMessage;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for length
        if src.len() < 4 {
            return Ok(None);
        }

        // Peek at length
        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Validate length
        if length > self.max_message_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Message too large: {} bytes (max: {})",
                    length, self.max_message_size
                ),
            ));
        }

        // Check if we have the full message
        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Consume length prefix
        src.advance(4);

        // Extract and deserialize
        let data = src.split_to(length);
        let msg = serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(Some(msg))
    }
}

impl Encoder<BootstrapMessage> for BootstrapCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: BootstrapMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data = serde_json::to_vec(&item)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        if data.len() > self.max_message_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Message too large: {} bytes", data.len()),
            ));
        }

        dst.reserve(4 + data.len());
        dst.put_u32(data.len() as u32);
        dst.extend_from_slice(&data);

        Ok(())
    }
}

// ============================================================================
// Helper for base64 encoding binary data in JSON
// ============================================================================

mod base64_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use base64::Engine;
        let s = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_id() {
        let id = CoordinatorId::new();
        let display = id.to_string();
        assert!(display.starts_with("coord-"));
        assert_eq!(display.len(), 14); // "coord-" + 8 chars
    }

    #[test]
    fn test_auth_token_expiry() {
        let token = AuthToken::new("secret".to_string(), Duration::from_secs(3600));
        assert!(!token.is_expired());

        let expired = AuthToken {
            token: "secret".to_string(),
            expires_at: 0, // Already expired
        };
        assert!(expired.is_expired());
    }

    #[test]
    fn test_auth_token_hash() {
        let token = AuthToken::new("secret".to_string(), Duration::from_secs(3600));
        let hash = token.hash();
        assert_eq!(hash.len(), 32);

        // Same token should produce same hash
        let token2 = AuthToken::new("secret".to_string(), Duration::from_secs(7200));
        assert_eq!(token.hash(), token2.hash());
    }

    #[test]
    fn test_coordinator_query_builder() {
        let query = CoordinatorQuery::new()
            .with_region("us-west")
            .with_organization("acme")
            .accepting_phantom()
            .with_min_cores(4)
            .with_limit(20);

        assert_eq!(query.region, Some("us-west".to_string()));
        assert_eq!(query.organization, Some("acme".to_string()));
        assert_eq!(query.accepts_phantom, Some(true));
        assert_eq!(query.min_capacity_cores, Some(4));
        assert_eq!(query.limit, 20);
    }

    #[test]
    fn test_nat_type_properties() {
        assert!(NatType::NoNat.can_hole_punch());
        assert!(NatType::FullCone.can_hole_punch());
        assert!(NatType::RestrictedCone.can_hole_punch());
        assert!(NatType::PortRestricted.can_hole_punch());
        assert!(!NatType::Symmetric.can_hole_punch());

        assert!(!NatType::NoNat.needs_relay());
        assert!(NatType::Symmetric.needs_relay());
    }

    #[test]
    fn test_message_serialize_roundtrip() {
        let msg = BootstrapMessage::RegisterResponse {
            coordinator_id: CoordinatorId::new(),
            heartbeat_interval_secs: 30,
        };

        let encoded = serde_json::to_string(&msg).unwrap();
        let decoded: BootstrapMessage = serde_json::from_str(&encoded).unwrap();

        if let BootstrapMessage::RegisterResponse {
            heartbeat_interval_secs,
            ..
        } = decoded
        {
            assert_eq!(heartbeat_interval_secs, 30);
        } else {
            panic!("Wrong message type after roundtrip");
        }
    }

    #[test]
    fn test_codec_roundtrip() {
        let mut codec = BootstrapCodec::new();
        let mut buf = BytesMut::new();

        let msg = BootstrapMessage::Ping { timestamp: 12345 };

        codec.encode(msg.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        if let BootstrapMessage::Ping { timestamp } = decoded {
            assert_eq!(timestamp, 12345);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_codec_partial_read() {
        let mut codec = BootstrapCodec::new();
        let mut buf = BytesMut::new();

        let msg = BootstrapMessage::Pong {
            timestamp: 12345,
            server_time: 67890,
        };

        codec.encode(msg, &mut buf).unwrap();

        // Split buffer to simulate partial read
        let full_len = buf.len();
        let partial = buf.split_to(full_len / 2);
        let rest = buf;

        let mut partial_buf = BytesMut::from(&partial[..]);

        // Should return None (incomplete)
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());

        // Add rest of data
        partial_buf.extend_from_slice(&rest);

        // Now should decode
        let decoded = codec.decode(&mut partial_buf).unwrap().unwrap();
        assert!(matches!(decoded, BootstrapMessage::Pong { .. }));
    }

    #[test]
    fn test_codec_reject_oversized() {
        let mut codec = BootstrapCodec::with_max_size(100);
        let mut buf = BytesMut::new();

        // Create a message larger than max size
        let large_msg = BootstrapMessage::Error {
            code: ErrorCode::InternalError,
            message: "x".repeat(200),
        };

        let result = codec.encode(large_msg, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_udp_encode_decode() {
        let msg = BootstrapMessage::StunRequest {
            node_id: NodeId::new(),
        };

        let encoded = BootstrapCodec::encode_bytes(&msg).unwrap();
        let decoded = BootstrapCodec::decode_bytes(&encoded).unwrap();

        assert!(matches!(decoded, BootstrapMessage::StunRequest { .. }));
    }

    #[test]
    fn test_capacity_info_default() {
        let capacity = CapacityInfo::default();
        assert_eq!(capacity.total_cores, 0);
        assert_eq!(capacity.available_cores, 0);
    }

    #[test]
    fn test_health_status_serialize() {
        let status = HealthStatus::Healthy;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"healthy\"");

        let degraded: HealthStatus = serde_json::from_str("\"degraded\"").unwrap();
        assert_eq!(degraded, HealthStatus::Degraded);
    }
}
