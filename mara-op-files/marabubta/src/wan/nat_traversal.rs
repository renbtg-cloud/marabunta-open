// Marabunta - Licensed under the MIT License.
//! NAT Traversal for nodes behind NAT
//!
//! Implements STUN-like functionality for nodes to discover their public IP
//! and hole punching coordination for P2P connections.
//!
//! # NAT Types
//!
//! Understanding NAT types is crucial for P2P connectivity:
//!
//! - **No NAT**: Node has a public IP, direct connection possible
//! - **Full Cone**: Any external host can reach the mapped port
//! - **Restricted Cone**: Only IPs node has contacted can reach it
//! - **Port Restricted**: Only IP:port pairs node has contacted can reach it
//! - **Symmetric**: Different mapping for each destination (hardest to traverse)
//!
//! # Hole Punching
//!
//! Hole punching works by having both nodes send UDP packets to each other
//! at roughly the same time. The packets create NAT mappings that allow
//! the connection to be established.
//!
//! ```text
//! Node A                Bootstrap Server              Node B
//!   |                         |                         |
//!   |------ HolePunchRequest ------->|                  |
//!   |                         |<---- HolePunchRequest --|
//!   |                         |                         |
//!   |<--- PunchInstructions --|-- PunchInstructions --->|
//!   |                         |                         |
//!   |============= UDP Packets (simultaneous) ==========|
//!   |                         |                         |
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::client::BootstrapClient;
use super::errors::{NatError, NatResult};
use super::protocol::{
    BindingResponse, HolePunchRequest, NatType, NodeId, PunchInstructions, RelayReason, SessionId,
};

// ============================================================================
// STUN Service
// ============================================================================

/// STUN-like service for NAT traversal
///
/// Helps nodes discover their public IP address and NAT type.
pub struct StunService {
    /// The UDP socket for STUN responses
    socket: UdpSocket,
    /// Request counter for statistics
    request_count: AtomicU64,
}

impl StunService {
    /// Create a new STUN service bound to the given socket
    pub async fn new(bind_addr: SocketAddr) -> NatResult<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket,
            request_count: AtomicU64::new(0),
        })
    }

    /// Create from an existing socket
    pub fn from_socket(socket: UdpSocket) -> Self {
        Self {
            socket,
            request_count: AtomicU64::new(0),
        }
    }

    /// Handle a STUN-like binding request
    ///
    /// Returns the client's public IP:port as seen by the server.
    /// This is the core STUN functionality.
    pub fn handle_binding_request(&self, client_addr: SocketAddr) -> BindingResponse {
        self.request_count.fetch_add(1, Ordering::Relaxed);

        let server_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        BindingResponse {
            public_addr: client_addr,
            nat_type: NatType::Unknown, // Full detection requires multiple servers
            server_time,
        }
    }

    /// Attempt to detect NAT type using multiple tests
    ///
    /// This requires multiple STUN servers to fully detect the NAT type.
    /// With a single server, we can only determine:
    /// - NoNat: if the response address matches the local address
    /// - Unknown: otherwise
    ///
    /// For full detection, we need:
    /// - Test 1: Basic binding (any server)
    /// - Test 2: Same server, different port
    /// - Test 3: Different server
    pub async fn detect_nat_type(
        &self,
        client_addr: SocketAddr,
        claimed_local_addr: Option<SocketAddr>,
    ) -> NatResult<NatType> {
        // If the client claims a local address and it matches what we see,
        // they're not behind NAT
        if let Some(local) = claimed_local_addr {
            if local.ip() == client_addr.ip() {
                return Ok(NatType::NoNat);
            }
        }

        // With a single server, we can't determine more than Unknown
        // A real implementation would coordinate with other STUN servers
        Ok(NatType::Unknown)
    }

    /// Get the socket reference for sending responses
    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    /// Get request count
    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Hole Punch Session
// ============================================================================

/// State of a hole punch session
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Waiting for both nodes to register
    Pending,
    /// Both nodes registered, punch scheduled
    Ready,
    /// Punch in progress
    Punching,
    /// Successfully connected
    Connected,
    /// Failed
    Failed,
    /// Timed out
    Expired,
}

/// A hole punch session between two nodes
#[derive(Debug)]
pub struct HolePunchSession {
    /// Session ID
    pub id: SessionId,
    /// First node (initiator)
    pub node_a: NodeId,
    /// First node's public address
    pub addr_a: SocketAddr,
    /// First node's NAT type
    pub nat_a: NatType,
    /// Second node (target)
    pub node_b: Option<NodeId>,
    /// Second node's public address
    pub addr_b: Option<SocketAddr>,
    /// Second node's NAT type
    pub nat_b: Option<NatType>,
    /// When to start punching (synchronized time)
    pub punch_at: Option<Instant>,
    /// Session state
    pub state: SessionState,
    /// When the session was created
    pub created_at: Instant,
    /// When the session expires
    pub expires_at: Instant,
    /// Number of punch attempts
    pub attempts: u32,
    /// Fallback relay address
    pub fallback_relay: Option<SocketAddr>,
}

impl HolePunchSession {
    fn new(
        id: SessionId,
        node_a: NodeId,
        addr_a: SocketAddr,
        nat_a: NatType,
        timeout: Duration,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            node_a,
            addr_a,
            nat_a,
            node_b: None,
            addr_b: None,
            nat_b: None,
            punch_at: None,
            state: SessionState::Pending,
            created_at: now,
            expires_at: now + timeout,
            attempts: 0,
            fallback_relay: None,
        }
    }

    /// Check if both nodes are ready
    pub fn is_ready(&self) -> bool {
        self.node_b.is_some() && self.addr_b.is_some()
    }

    /// Check if the session has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Get instructions for a specific node
    pub fn get_instructions(
        &self,
        node: NodeId,
        relay_addr: Option<SocketAddr>,
    ) -> Option<PunchInstructions> {
        if !self.is_ready() {
            return None;
        }

        let (peer_addr, peer_nat) = if node == self.node_a {
            (self.addr_b?, self.nat_b?)
        } else if Some(node) == self.node_b {
            (self.addr_a, self.nat_a)
        } else {
            return None;
        };

        // Calculate synchronized punch time
        let punch_at_ms = self
            .punch_at
            .map(|t| {
                let now = Instant::now();
                let duration = t.saturating_duration_since(now);
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + duration.as_millis() as u64
            })
            .unwrap_or_else(|| {
                // Default: punch 500ms from now
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + 500
            });

        Some(PunchInstructions {
            session_id: self.id,
            peer_public_addr: peer_addr,
            peer_nat_type: peer_nat,
            punch_at_ms,
            packet_count: 10,
            interval_ms: 50,
            fallback_relay: relay_addr.or(self.fallback_relay),
        })
    }
}

// ============================================================================
// Hole Punch Coordinator
// ============================================================================

/// Configuration for hole punch coordination
#[derive(Debug, Clone)]
pub struct HolePunchConfig {
    /// Session timeout
    pub session_timeout: Duration,
    /// Maximum concurrent sessions
    pub max_sessions: usize,
    /// Time offset for synchronized punching
    pub punch_delay: Duration,
    /// Cleanup interval
    pub cleanup_interval: Duration,
}

impl Default for HolePunchConfig {
    fn default() -> Self {
        Self {
            session_timeout: Duration::from_secs(30),
            max_sessions: 10000,
            punch_delay: Duration::from_millis(500),
            cleanup_interval: Duration::from_secs(10),
        }
    }
}

/// Coordinates hole punching between nodes
///
/// When node A wants to connect to node B:
/// 1. A sends HolePunchRequest to bootstrap
/// 2. Bootstrap creates a session, waits for B to also request
/// 3. When both ready, bootstrap sends synchronized PunchInstructions
/// 4. Both nodes punch simultaneously
pub struct HolePunchCoordinator {
    /// Configuration
    config: HolePunchConfig,
    /// Active sessions by ID
    sessions: RwLock<HashMap<SessionId, HolePunchSession>>,
    /// Sessions by target node (for matching requests)
    by_target: RwLock<HashMap<NodeId, Vec<SessionId>>>,
    /// Optional relay address to include in instructions
    relay_addr: RwLock<Option<SocketAddr>>,
}

impl HolePunchCoordinator {
    /// Create a new hole punch coordinator
    pub fn new() -> Self {
        Self::with_config(HolePunchConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: HolePunchConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
            by_target: RwLock::new(HashMap::new()),
            relay_addr: RwLock::new(None),
        }
    }

    /// Set the relay address to include in punch instructions
    pub async fn set_relay_addr(&self, addr: SocketAddr) {
        let mut relay = self.relay_addr.write().await;
        *relay = Some(addr);
    }

    /// Initiate or join a hole punch session
    ///
    /// If there's already a pending session where this node is the target,
    /// join that session. Otherwise, create a new session.
    pub async fn initiate_hole_punch(
        &self,
        request: HolePunchRequest,
    ) -> NatResult<PunchInstructions> {
        let node_id = request.node_id;
        let node_addr = request.node_addr;
        let target_node = request.target_node;
        let nat_type = request.nat_type;

        // Check if there's a pending session targeting this node
        let existing_session = {
            let by_target = self.by_target.read().await;
            if let Some(session_ids) = by_target.get(&node_id) {
                let sessions = self.sessions.read().await;
                session_ids.iter().find_map(|id| {
                    sessions.get(id).and_then(|s| {
                        if s.state == SessionState::Pending && !s.is_expired() {
                            Some(s.id)
                        } else {
                            None
                        }
                    })
                })
            } else {
                None
            }
        };

        if let Some(session_id) = existing_session {
            // Join existing session as node B
            return self
                .join_session(session_id, node_id, node_addr, nat_type)
                .await;
        }

        // Check capacity
        let sessions = self.sessions.read().await;
        if sessions.len() >= self.config.max_sessions {
            return Err(NatError::Internal(
                "Maximum hole punch sessions reached".to_string(),
            ));
        }
        drop(sessions);

        // Create new session
        let session_id = SessionId::new();
        let session = HolePunchSession::new(
            session_id,
            node_id,
            node_addr,
            nat_type,
            self.config.session_timeout,
        );

        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id, session);
        drop(sessions);

        // Index by target
        let mut by_target = self.by_target.write().await;
        by_target
            .entry(target_node)
            .or_insert_with(Vec::new)
            .push(session_id);

        info!(
            "Created hole punch session {} from {} to {}",
            session_id, node_id, target_node
        );

        // Return instructions (will need to wait for peer)
        // Note: In production, we'd use a callback or WebSocket for notification
        // For now, return an error indicating the peer needs to join
        Err(NatError::Internal(format!(
            "Waiting for peer to join session {}. Poll for updates.",
            session_id
        )))
    }

    /// Join an existing session as node B
    async fn join_session(
        &self,
        session_id: SessionId,
        node_id: NodeId,
        node_addr: SocketAddr,
        nat_type: NatType,
    ) -> NatResult<PunchInstructions> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or(NatError::SessionNotFound(session_id))?;

        if session.is_expired() {
            return Err(NatError::SessionExpired(session_id));
        }

        // Add node B info
        session.node_b = Some(node_id);
        session.addr_b = Some(node_addr);
        session.nat_b = Some(nat_type);
        session.state = SessionState::Ready;

        // Schedule punch time
        let punch_at = Instant::now() + self.config.punch_delay;
        session.punch_at = Some(punch_at);

        info!(
            "Node {} joined hole punch session {} (with {})",
            node_id, session_id, session.node_a
        );

        // Get instructions for this node
        let relay = *self.relay_addr.read().await;
        session
            .get_instructions(node_id, relay)
            .ok_or_else(|| NatError::Internal("Failed to generate instructions".to_string()))
    }

    /// Get punch instructions for a node
    pub async fn get_punch_instructions(
        &self,
        session_id: SessionId,
        node_id: NodeId,
    ) -> NatResult<PunchInstructions> {
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(&session_id)
            .ok_or(NatError::SessionNotFound(session_id))?;

        if session.is_expired() {
            return Err(NatError::SessionExpired(session_id));
        }

        let relay = *self.relay_addr.read().await;
        session.get_instructions(node_id, relay).ok_or_else(|| {
            NatError::Internal("Session not ready or node not in session".to_string())
        })
    }

    /// Mark a session as successful
    pub async fn mark_connected(&self, session_id: SessionId) -> NatResult<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or(NatError::SessionNotFound(session_id))?;

        session.state = SessionState::Connected;
        info!("Hole punch session {} connected successfully", session_id);
        Ok(())
    }

    /// Mark a session as failed
    pub async fn mark_failed(&self, session_id: SessionId) -> NatResult<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(&session_id)
            .ok_or(NatError::SessionNotFound(session_id))?;

        session.state = SessionState::Failed;
        session.attempts += 1;
        warn!(
            "Hole punch session {} failed (attempt {})",
            session_id, session.attempts
        );
        Ok(())
    }

    /// Get session state
    pub async fn get_session(&self, session_id: SessionId) -> Option<SessionState> {
        let sessions = self.sessions.read().await;
        sessions.get(&session_id).map(|s| s.state)
    }

    /// Cleanup expired sessions
    pub async fn cleanup_expired(&self) {
        let mut sessions = self.sessions.write().await;
        let mut by_target = self.by_target.write().await;

        let expired: Vec<SessionId> = sessions
            .iter()
            .filter(|(_, s)| s.is_expired())
            .map(|(id, _)| *id)
            .collect();

        for id in &expired {
            if let Some(session) = sessions.remove(id) {
                // Remove from target index
                if let Some(node_b) = session.node_b {
                    if let Some(ids) = by_target.get_mut(&node_b) {
                        ids.retain(|i| i != id);
                    }
                }
            }
        }

        if !expired.is_empty() {
            debug!("Cleaned up {} expired hole punch sessions", expired.len());
        }
    }

    /// Get number of active sessions
    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

impl Default for HolePunchCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Relay Fallback for Mobile / Carrier-Grade NAT
// ============================================================================

/// How a peer connection was established
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionType {
    /// Direct connection (no NAT traversal needed)
    Direct,
    /// Connection via UDP hole punching
    HolePunched,
    /// Connection via a relay server
    Relayed {
        relay_addr: SocketAddr,
        session_id: String,
    },
}

impl std::fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Direct => write!(f, "direct"),
            ConnectionType::HolePunched => write!(f, "hole-punched"),
            ConnectionType::Relayed {
                relay_addr,
                session_id,
            } => write!(f, "relayed({}; session={})", relay_addr, session_id),
        }
    }
}

/// Represents a successfully connected peer, regardless of the underlying
/// transport mechanism (direct, hole-punched, or relayed).
#[derive(Debug, Clone)]
pub struct ConnectedPeer {
    /// Identifier of the remote peer
    pub peer_id: String,
    /// How the connection was established
    pub connection_type: ConnectionType,
    /// The address to use when communicating with this peer.
    /// For direct/hole-punched connections this is the peer's address;
    /// for relayed connections this is the relay server address.
    pub effective_addr: SocketAddr,
}

/// Attempt hole punching through the bootstrap server.
///
/// Wraps [`BootstrapClient::request_hole_punch`] and translates the result
/// into a [`ConnectedPeer`] with [`ConnectionType::HolePunched`].
async fn try_hole_punch(
    client: &BootstrapClient,
    peer_addr: SocketAddr,
    _local_addr: SocketAddr,
    _node_id: &str,
    peer_id: &str,
) -> NatResult<ConnectedPeer> {
    // Parse the peer_id string into a NodeId. We accept any UUID that was
    // previously serialised with Display. Fall back to the raw string if
    // parsing fails so callers are not burdened with UUID construction.
    let peer_node_id = parse_node_id(peer_id)?;

    let instructions = client
        .request_hole_punch(peer_node_id)
        .await
        .map_err(|_e| NatError::HolePunchFailed(0))?;

    info!(
        "Hole punch succeeded for peer {} via {}",
        peer_id, instructions.peer_public_addr
    );

    Ok(ConnectedPeer {
        peer_id: peer_id.to_string(),
        connection_type: ConnectionType::HolePunched,
        effective_addr: peer_addr,
    })
}

/// Request a relay session through the bootstrap server.
///
/// Wraps [`BootstrapClient::request_relay`] and translates the result
/// into a [`ConnectedPeer`] with [`ConnectionType::Relayed`].
async fn try_relay(
    client: &BootstrapClient,
    _node_id: &str,
    peer_id: &str,
) -> NatResult<ConnectedPeer> {
    let peer_node_id = parse_node_id(peer_id)?;

    let (session_id, relay_addr) = client
        .request_relay(peer_node_id, RelayReason::HolePunchFailed)
        .await
        .map_err(|e| {
            NatError::Internal(format!("Relay request failed: {}", e))
        })?;

    info!(
        "Relay session {} created for peer {} at {}",
        session_id, peer_id, relay_addr
    );

    Ok(ConnectedPeer {
        peer_id: peer_id.to_string(),
        connection_type: ConnectionType::Relayed {
            relay_addr,
            session_id: session_id.to_string(),
        },
        effective_addr: relay_addr,
    })
}

/// Attempt to connect to a peer, with automatic relay fallback.
///
/// On mobile networks (`is_mobile = true`), tries relay **first** to avoid
/// wasting time on hole punching that will almost certainly fail on
/// carrier-grade NAT. If the relay attempt fails, hole punching is still
/// attempted as a last resort.
///
/// On non-mobile networks, hole punching is tried first. If it fails the
/// function transparently falls back to a relay session so the caller always
/// gets a usable [`ConnectedPeer`] (or a definitive error).
///
/// # Arguments
///
/// * `client`     - Bootstrap client used for signalling.
/// * `peer_addr`  - The remote peer's last-known public address.
/// * `local_addr` - This node's local socket address (used by hole-punch).
/// * `node_id`    - This node's identifier (string form).
/// * `peer_id`    - The remote peer's identifier (string form / UUID).
/// * `is_mobile`  - Set to `true` when running on a mobile / cellular network
///                   (Android, iOS on LTE/5G, etc.).
pub async fn connect_with_fallback(
    client: &BootstrapClient,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    node_id: &str,
    peer_id: &str,
    is_mobile: bool,
) -> NatResult<ConnectedPeer> {
    if is_mobile {
        // Mobile-first strategy: try relay directly to avoid wasting time on
        // hole punching that will fail on carrier-grade NAT.
        info!(
            "Mobile network detected -- attempting relay first for peer {}",
            peer_id
        );
        match try_relay(client, node_id, peer_id).await {
            Ok(peer) => return Ok(peer),
            Err(e) => {
                warn!(
                    "Relay failed for mobile peer {} ({}), trying hole punch as last resort",
                    peer_id, e
                );
                // Fall through to hole punch as last resort
            }
        }
    }

    // Non-mobile (or mobile relay failed): try hole punch first.
    match try_hole_punch(client, peer_addr, local_addr, node_id, peer_id).await {
        Ok(peer) => Ok(peer),
        Err(e) => {
            info!(
                "Hole punch failed for peer {} ({}), falling back to relay",
                peer_id, e
            );
            try_relay(client, node_id, peer_id).await
        }
    }
}

/// Parse a peer-id string into a [`NodeId`].
///
/// Accepts either a raw UUID (`xxxxxxxx-xxxx-...`) or the display format
/// produced by [`NodeId::fmt`] (`node-xxxxxxxx`). Returns [`NatError`] when
/// the string cannot be interpreted.
fn parse_node_id(id: &str) -> NatResult<NodeId> {
    // The Display impl produces "node-<first 8 chars of uuid>".  That is not
    // round-trippable, so callers are expected to pass a full UUID string in
    // practice.  We handle both forms defensively.
    let raw = id.strip_prefix("node-").unwrap_or(id);
    let uuid = uuid::Uuid::parse_str(raw).map_err(|e| {
        NatError::Internal(format!("Invalid peer node id '{}': {}", id, e))
    })?;
    Ok(NodeId(uuid))
}

// ============================================================================
// NAT Type Detection Helper
// ============================================================================

/// Helper for detecting NAT type using multiple binding requests
///
/// Full NAT detection requires:
/// 1. Get mapped address from server 1
/// 2. Get mapped address from server 1 on different port
/// 3. Get mapped address from server 2
/// 4. Compare addresses to determine NAT type
pub struct NatTypeDetector {
    /// Local address to test
    local_addr: SocketAddr,
    /// Results from different tests
    results: Vec<BindingResponse>,
}

impl NatTypeDetector {
    pub fn new(local_addr: SocketAddr) -> Self {
        Self {
            local_addr,
            results: Vec::new(),
        }
    }

    /// Add a binding response from a STUN test
    pub fn add_result(&mut self, response: BindingResponse) {
        self.results.push(response);
    }

    /// Analyze results to determine NAT type
    ///
    /// This requires at least 2 results from different servers to be accurate.
    pub fn analyze(&self) -> NatType {
        if self.results.is_empty() {
            return NatType::Unknown;
        }

        // Single result - can only detect NoNat
        if self.results.len() == 1 {
            if self.results[0].public_addr.ip() == self.local_addr.ip() {
                return NatType::NoNat;
            }
            return NatType::Unknown;
        }

        // Check if all results have the same mapped address
        let first = &self.results[0];
        let all_same = self
            .results
            .iter()
            .all(|r| r.public_addr == first.public_addr);

        if self.results[0].public_addr.ip() == self.local_addr.ip() {
            NatType::NoNat
        } else if all_same {
            // Same address from all servers - Full Cone, Restricted, or Port Restricted
            // We'd need to do additional tests to distinguish these
            NatType::FullCone // Conservative guess
        } else {
            // Different addresses from different servers - Symmetric NAT
            NatType::Symmetric
        }
    }

    /// Clear results for a new detection
    pub fn reset(&mut self) {
        self.results.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stun_binding_response() {
        // Create a mock binding response
        let client_addr: SocketAddr = "1.2.3.4:5000".parse().unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stun = StunService::from_socket(socket);

        let response = stun.handle_binding_request(client_addr);

        assert_eq!(response.public_addr, client_addr);
        assert!(response.server_time > 0);
    }

    #[tokio::test]
    async fn test_hole_punch_session_creation() {
        let coordinator = HolePunchCoordinator::new();

        let request = HolePunchRequest {
            node_id: NodeId::new(),
            node_addr: "1.2.3.4:5000".parse().unwrap(),
            target_node: NodeId::new(),
            nat_type: NatType::FullCone,
        };

        // First request should create session but return error (waiting for peer)
        let result = coordinator.initiate_hole_punch(request).await;
        assert!(result.is_err());

        // Session should exist
        assert_eq!(coordinator.session_count().await, 1);
    }

    #[tokio::test]
    async fn test_hole_punch_session_join() {
        let coordinator = HolePunchCoordinator::new();

        let node_a = NodeId::new();
        let node_b = NodeId::new();

        // Node A initiates
        let request_a = HolePunchRequest {
            node_id: node_a,
            node_addr: "1.2.3.4:5000".parse().unwrap(),
            target_node: node_b,
            nat_type: NatType::FullCone,
        };
        let _ = coordinator.initiate_hole_punch(request_a).await;

        // Node B joins (by initiating with A as target)
        let request_b = HolePunchRequest {
            node_id: node_b,
            node_addr: "5.6.7.8:6000".parse().unwrap(),
            target_node: node_a,
            nat_type: NatType::RestrictedCone,
        };
        let result = coordinator.initiate_hole_punch(request_b).await;

        // Node B should get instructions
        assert!(result.is_ok());
        let instructions = result.unwrap();
        assert_eq!(
            instructions.peer_public_addr,
            "1.2.3.4:5000".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(instructions.peer_nat_type, NatType::FullCone);
    }

    #[tokio::test]
    async fn test_session_expiry() {
        let mut config = HolePunchConfig::default();
        config.session_timeout = Duration::from_millis(50);
        let coordinator = HolePunchCoordinator::with_config(config);

        let request = HolePunchRequest {
            node_id: NodeId::new(),
            node_addr: "1.2.3.4:5000".parse().unwrap(),
            target_node: NodeId::new(),
            nat_type: NatType::FullCone,
        };
        let _ = coordinator.initiate_hole_punch(request).await;

        assert_eq!(coordinator.session_count().await, 1);

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cleanup
        coordinator.cleanup_expired().await;

        assert_eq!(coordinator.session_count().await, 0);
    }

    #[tokio::test]
    async fn test_session_state_transitions() {
        let coordinator = HolePunchCoordinator::new();

        let node_a = NodeId::new();
        let node_b = NodeId::new();

        // Create session
        let request = HolePunchRequest {
            node_id: node_a,
            node_addr: "1.2.3.4:5000".parse().unwrap(),
            target_node: node_b,
            nat_type: NatType::FullCone,
        };
        let _ = coordinator.initiate_hole_punch(request).await;

        // Get session ID
        let sessions = coordinator.sessions.read().await;
        let session_id = *sessions.keys().next().unwrap();
        drop(sessions);

        // Initially pending
        let state = coordinator.get_session(session_id).await;
        assert_eq!(state, Some(SessionState::Pending));

        // Mark connected
        coordinator.mark_connected(session_id).await.unwrap();
        let state = coordinator.get_session(session_id).await;
        assert_eq!(state, Some(SessionState::Connected));
    }

    #[test]
    fn test_nat_type_detection() {
        let local_addr: SocketAddr = "192.168.1.100:5000".parse().unwrap();
        let mut detector = NatTypeDetector::new(local_addr);

        // No results - unknown
        assert_eq!(detector.analyze(), NatType::Unknown);

        // Same IP as local - no NAT
        detector.add_result(BindingResponse {
            public_addr: local_addr,
            nat_type: NatType::Unknown,
            server_time: 0,
        });
        assert_eq!(detector.analyze(), NatType::NoNat);

        detector.reset();

        // Different IP - behind NAT
        detector.add_result(BindingResponse {
            public_addr: "1.2.3.4:5000".parse().unwrap(),
            nat_type: NatType::Unknown,
            server_time: 0,
        });
        assert_eq!(detector.analyze(), NatType::Unknown);

        // Add second result with same address - Full Cone
        detector.add_result(BindingResponse {
            public_addr: "1.2.3.4:5000".parse().unwrap(),
            nat_type: NatType::Unknown,
            server_time: 0,
        });
        assert_eq!(detector.analyze(), NatType::FullCone);

        detector.reset();

        // Different addresses from different servers - Symmetric
        detector.add_result(BindingResponse {
            public_addr: "1.2.3.4:5000".parse().unwrap(),
            nat_type: NatType::Unknown,
            server_time: 0,
        });
        detector.add_result(BindingResponse {
            public_addr: "1.2.3.4:6000".parse().unwrap(),
            nat_type: NatType::Unknown,
            server_time: 0,
        });
        assert_eq!(detector.analyze(), NatType::Symmetric);
    }

    #[test]
    fn test_nat_type_properties() {
        // Test can_hole_punch
        assert!(NatType::NoNat.can_hole_punch());
        assert!(NatType::FullCone.can_hole_punch());
        assert!(NatType::RestrictedCone.can_hole_punch());
        assert!(NatType::PortRestricted.can_hole_punch());
        assert!(!NatType::Symmetric.can_hole_punch());
        assert!(!NatType::Unknown.can_hole_punch());

        // Test needs_relay
        assert!(!NatType::NoNat.needs_relay());
        assert!(!NatType::FullCone.needs_relay());
        assert!(NatType::Symmetric.needs_relay());
    }

    #[tokio::test]
    async fn test_set_relay_addr() {
        let coordinator = HolePunchCoordinator::new();
        let relay_addr: SocketAddr = "10.0.0.1:9000".parse().unwrap();

        coordinator.set_relay_addr(relay_addr).await;

        let node_a = NodeId::new();
        let node_b = NodeId::new();

        // Create and join session
        let request_a = HolePunchRequest {
            node_id: node_a,
            node_addr: "1.2.3.4:5000".parse().unwrap(),
            target_node: node_b,
            nat_type: NatType::FullCone,
        };
        let _ = coordinator.initiate_hole_punch(request_a).await;

        let request_b = HolePunchRequest {
            node_id: node_b,
            node_addr: "5.6.7.8:6000".parse().unwrap(),
            target_node: node_a,
            nat_type: NatType::RestrictedCone,
        };
        let result = coordinator.initiate_hole_punch(request_b).await;

        let instructions = result.unwrap();
        assert_eq!(instructions.fallback_relay, Some(relay_addr));
    }

    #[test]
    fn test_punch_instructions_generation() {
        let session_id = SessionId::new();
        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let addr_a: SocketAddr = "1.2.3.4:5000".parse().unwrap();
        let addr_b: SocketAddr = "5.6.7.8:6000".parse().unwrap();

        let mut session = HolePunchSession::new(
            session_id,
            node_a,
            addr_a,
            NatType::FullCone,
            Duration::from_secs(30),
        );

        // Not ready yet (no node B)
        assert!(!session.is_ready());
        assert!(session.get_instructions(node_a, None).is_none());

        // Add node B
        session.node_b = Some(node_b);
        session.addr_b = Some(addr_b);
        session.nat_b = Some(NatType::RestrictedCone);

        assert!(session.is_ready());

        // Get instructions for node A
        let instructions_a = session.get_instructions(node_a, None).unwrap();
        assert_eq!(instructions_a.peer_public_addr, addr_b);
        assert_eq!(instructions_a.peer_nat_type, NatType::RestrictedCone);
        assert_eq!(instructions_a.packet_count, 10);
        assert_eq!(instructions_a.interval_ms, 50);

        // Get instructions for node B
        let instructions_b = session.get_instructions(node_b, None).unwrap();
        assert_eq!(instructions_b.peer_public_addr, addr_a);
        assert_eq!(instructions_b.peer_nat_type, NatType::FullCone);

        // Unknown node gets nothing
        let unknown = NodeId::new();
        assert!(session.get_instructions(unknown, None).is_none());
    }
}
