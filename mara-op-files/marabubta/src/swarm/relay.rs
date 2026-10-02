// Marabunta - Licensed under the MIT License.
//! NAT traversal for the Marabunta Swarm (Phase 8).
//!
//! Provides three-tier connectivity:
//!
//! 1. **Direct** -- both nodes have public IPs or are on the same LAN.
//! 2. **Hole-punch** -- at least one node is behind a Cone NAT; we
//!    coordinate a simultaneous-open TCP connection via a signaling relay.
//! 3. **TURN-like relay** -- both nodes are behind Symmetric NATs (or
//!    hole-punching failed); traffic is forwarded through a relay node
//!    that has a public IP.
//!
//! The [`NatProber`] determines what kind of NAT the local node is behind
//! by issuing STUN-like UDP binding requests to two independent servers
//! and comparing the mapped addresses.
//!
//! The [`RelayServer`] manages bidirectional relay sessions between two
//! NAT'd peers. Each session is bandwidth-capped and automatically
//! cleaned up after inactivity.
//!
//! The [`HolePuncher`] implements TCP simultaneous-open. Both sides begin
//! connecting to each other's external address at approximately the same
//! time (coordinated by a signaling message through the relay or gossip).
//!
//! [`choose_strategy`] is the entry point: given our NAT type and the
//! peer's NAT type it returns the cheapest viable [`ConnectionStrategy`].

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::config::{
    HOLE_PUNCH_TIMEOUT, RELAY_MAX_BANDWIDTH, RELAY_MAX_SESSIONS, STUN_PROBE_TIMEOUT,
};
use super::types::{NatType, NodeId, SwarmError};

// ============================================================================
// STUN probe result
// ============================================================================

/// The result of a NAT type probe.
#[derive(Debug, Clone)]
pub struct NatProbeResult {
    /// Detected NAT type.
    pub nat_type: NatType,
    /// External (mapped) address as seen by the first STUN server, if any.
    pub external_addr: Option<SocketAddr>,
    /// Which STUN server was actually used.
    pub stun_server_used: Option<SocketAddr>,
}

// ============================================================================
// NatProber
// ============================================================================

/// Determines the local node's NAT type using STUN-like binding probes.
///
/// The algorithm:
///
/// 1. Bind a local UDP socket on an ephemeral port.
/// 2. Send a small "binding request" packet to server A and read back the
///    mapped address.
/// 3. Send the same packet from the *same* local port to server B.
/// 4. If both servers report the same mapped address the NAT is Cone
///    (full-cone, address-restricted, or port-restricted -- all
///    hole-punchable). If they differ, the NAT is Symmetric.
/// 5. If the mapped address equals the local socket address, we are Public.
pub struct NatProber;

/// Magic bytes prefixed to our pseudo-STUN binding request so that the
/// receiving STUN helper can distinguish it from random traffic.
const STUN_MAGIC: &[u8; 4] = b"CAMB";

/// Maximum size of a STUN-like response we will read.
const STUN_MAX_RESPONSE: usize = 128;

impl NatProber {
    /// Probe NAT type by contacting the given STUN servers.
    ///
    /// `stun_servers` must contain at least 2 entries for a reliable
    /// determination. If fewer are supplied, the result degrades:
    /// - 1 server: we can distinguish Public vs NAT but cannot tell Cone
    ///   from Symmetric.
    /// - 0 servers: returns `NatType::Unknown`.
    pub async fn probe(&self, stun_servers: &[SocketAddr]) -> NatProbeResult {
        if stun_servers.is_empty() {
            return NatProbeResult {
                nat_type: NatType::Unknown,
                external_addr: None,
                stun_server_used: None,
            };
        }

        // Bind an ephemeral UDP socket.
        let socket = match UdpSocket::bind("0.0.0.0:0").await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                warn!(error = %e, "failed to bind UDP socket for STUN probe");
                return NatProbeResult {
                    nat_type: NatType::Unknown,
                    external_addr: None,
                    stun_server_used: None,
                };
            }
        };

        let local_addr = socket.local_addr().ok();

        // Probe server A.
        let mapped_a = Self::probe_one(&socket, stun_servers[0]).await;

        if stun_servers.len() < 2 {
            // Only one server -- best-effort classification.
            let nat_type = match (&mapped_a, &local_addr) {
                (Some(mapped), Some(local)) if mapped.ip() == local.ip() => NatType::Public,
                (Some(_), _) => NatType::ConeNat, // Can't distinguish without a second server.
                (None, _) => NatType::Unknown,
            };
            return NatProbeResult {
                nat_type,
                external_addr: mapped_a,
                stun_server_used: Some(stun_servers[0]),
            };
        }

        // Probe server B from the same socket.
        let mapped_b = Self::probe_one(&socket, stun_servers[1]).await;

        let nat_type = match (&mapped_a, &mapped_b, &local_addr) {
            // Both servers report the same mapped address.
            (Some(a), Some(b), Some(local)) if a == b => {
                if a.ip() == local.ip() {
                    NatType::Public
                } else {
                    NatType::ConeNat
                }
            }
            (Some(a), Some(b), None) if a == b => NatType::ConeNat,
            // Different mapped addresses -- Symmetric NAT.
            (Some(_), Some(_), _) => NatType::SymmetricNat,
            // One or both probes failed.
            _ => NatType::Unknown,
        };

        NatProbeResult {
            nat_type,
            external_addr: mapped_a,
            stun_server_used: Some(stun_servers[0]),
        }
    }

    /// Send a pseudo-STUN binding request to `server` and parse the mapped
    /// address from the response.
    ///
    /// Response format: `CAMB` + 1 byte address family (4=IPv4, 6=IPv6) +
    /// 2 bytes port (big-endian) + 4 or 16 bytes address.
    async fn probe_one(socket: &UdpSocket, server: SocketAddr) -> Option<SocketAddr> {
        // Build a minimal binding request: magic + 16-byte transaction ID.
        let txn_id = Uuid::new_v4();
        let mut request = Vec::with_capacity(20);
        request.extend_from_slice(STUN_MAGIC);
        request.extend_from_slice(txn_id.as_bytes());

        if let Err(e) = socket.send_to(&request, server).await {
            debug!(server = %server, error = %e, "STUN probe send failed");
            return None;
        }

        // Wait for response with timeout.
        let mut buf = [0u8; STUN_MAX_RESPONSE];
        let recv_fut = socket.recv_from(&mut buf);

        match tokio::time::timeout(STUN_PROBE_TIMEOUT, recv_fut).await {
            Ok(Ok((n, _from))) => Self::parse_stun_response(&buf[..n]),
            Ok(Err(e)) => {
                debug!(server = %server, error = %e, "STUN probe recv failed");
                None
            }
            Err(_) => {
                debug!(server = %server, "STUN probe timed out");
                None
            }
        }
    }

    /// Parse a pseudo-STUN response into a SocketAddr.
    fn parse_stun_response(data: &[u8]) -> Option<SocketAddr> {
        // Minimum: 4 magic + 1 family + 2 port + 4 addr = 11 bytes (IPv4).
        if data.len() < 11 {
            return None;
        }
        if &data[0..4] != STUN_MAGIC {
            return None;
        }

        let family = data[4];
        let port = u16::from_be_bytes([data[5], data[6]]);

        match family {
            4 if data.len() >= 11 => {
                let ip = std::net::Ipv4Addr::new(data[7], data[8], data[9], data[10]);
                Some(SocketAddr::new(std::net::IpAddr::V4(ip), port))
            }
            6 if data.len() >= 23 => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&data[7..23]);
                let ip = std::net::Ipv6Addr::from(octets);
                Some(SocketAddr::new(std::net::IpAddr::V6(ip), port))
            }
            _ => None,
        }
    }
}

// ============================================================================
// RelaySession
// ============================================================================

/// A single relay session bridging two NAT'd nodes.
///
/// Both `initiator` and `target` send their data to the relay; the relay
/// forwards traffic from each side to the other. Bandwidth is metered
/// via `bytes_relayed`.
#[derive(Debug)]
pub struct RelaySession {
    /// Unique session identifier.
    pub session_id: Uuid,
    /// The node that requested the relay.
    pub initiator: NodeId,
    /// The node being connected to.
    pub target: NodeId,
    /// When this session was created.
    pub created_at: DateTime<Utc>,
    /// Total bytes forwarded through this session so far.
    pub bytes_relayed: AtomicU64,
    /// Whether the target has accepted the session.
    accepted: std::sync::atomic::AtomicBool,
}

impl RelaySession {
    fn new(session_id: Uuid, initiator: NodeId, target: NodeId) -> Self {
        Self {
            session_id,
            initiator,
            target,
            created_at: Utc::now(),
            bytes_relayed: AtomicU64::new(0),
            accepted: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Mark the session as accepted by the target node.
    pub fn accept(&self) {
        self.accepted.store(true, Ordering::Release);
    }

    /// Whether the target has accepted.
    pub fn is_accepted(&self) -> bool {
        self.accepted.load(Ordering::Acquire)
    }

    /// Total bytes relayed so far.
    pub fn total_bytes(&self) -> u64 {
        self.bytes_relayed.load(Ordering::Relaxed)
    }
}

// ============================================================================
// RelayServer
// ============================================================================

/// TURN-like relay server that bridges traffic between NAT'd nodes.
///
/// A relay node with `CanRelay` runs a `RelayServer` that accepts session
/// requests from NAT'd peers. Each session is bandwidth-capped at
/// [`RELAY_MAX_BANDWIDTH`] bytes and the total number of concurrent
/// sessions is bounded by [`RELAY_MAX_SESSIONS`].
pub struct RelayServer {
    /// The address this relay listens on.
    listen_addr: SocketAddr,
    /// Active sessions keyed by session ID.
    sessions: DashMap<Uuid, Arc<RelaySession>>,
    /// Maximum concurrent sessions.
    max_sessions: usize,
    /// Maximum bytes per session before throttling/closing.
    max_bandwidth_per_session: u64,
}

impl RelayServer {
    /// Create a new relay server bound to `listen_addr`.
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            sessions: DashMap::new(),
            max_sessions: RELAY_MAX_SESSIONS,
            max_bandwidth_per_session: RELAY_MAX_BANDWIDTH,
        }
    }

    /// Return the configured listen address.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Create a new relay session. Returns the session ID on success.
    ///
    /// Fails with [`SwarmError::Relay`] if the session limit is reached.
    pub fn create_session(
        &self,
        initiator: NodeId,
        target: NodeId,
    ) -> Result<Uuid, SwarmError> {
        if self.sessions.len() >= self.max_sessions {
            return Err(SwarmError::Relay(format!(
                "relay at capacity ({} sessions)",
                self.max_sessions
            )));
        }

        let session_id = Uuid::new_v4();
        let session = Arc::new(RelaySession::new(session_id, initiator, target));
        self.sessions.insert(session_id, session);

        info!(
            session_id = %session_id,
            initiator = %initiator,
            target = %target,
            "relay session created"
        );

        Ok(session_id)
    }

    /// Mark a session as accepted by the target node.
    ///
    /// Fails if the session does not exist or if `from` is not the target.
    pub fn accept_session(
        &self,
        session_id: Uuid,
        from: NodeId,
    ) -> Result<(), SwarmError> {
        let session = self
            .sessions
            .get(&session_id)
            .ok_or_else(|| SwarmError::Relay(format!("session {} not found", session_id)))?;

        if session.target != from {
            return Err(SwarmError::Relay(format!(
                "node {} is not the target of session {}",
                from, session_id
            )));
        }

        session.accept();

        info!(
            session_id = %session_id,
            target = %from,
            "relay session accepted"
        );

        Ok(())
    }

    /// Relay data through a session.
    ///
    /// Increments the bandwidth counter and returns an error if the
    /// session's bandwidth cap has been exceeded, if the session does
    /// not exist, or if it hasn't been accepted yet.
    pub fn relay_data(
        &self,
        session_id: Uuid,
        data: Vec<u8>,
    ) -> Result<(), SwarmError> {
        let session = self
            .sessions
            .get(&session_id)
            .ok_or_else(|| SwarmError::Relay(format!("session {} not found", session_id)))?;

        if !session.is_accepted() {
            return Err(SwarmError::Relay(format!(
                "session {} not yet accepted by target",
                session_id
            )));
        }

        let new_total = session
            .bytes_relayed
            .fetch_add(data.len() as u64, Ordering::Relaxed)
            + data.len() as u64;

        if new_total > self.max_bandwidth_per_session {
            return Err(SwarmError::Relay(format!(
                "session {} exceeded bandwidth cap ({} > {} bytes)",
                session_id, new_total, self.max_bandwidth_per_session
            )));
        }

        debug!(
            session_id = %session_id,
            data_len = data.len(),
            total_bytes = new_total,
            "data relayed"
        );

        Ok(())
    }

    /// Close and remove a relay session.
    pub fn close_session(&self, session_id: &Uuid) {
        if let Some((_, session)) = self.sessions.remove(session_id) {
            info!(
                session_id = %session_id,
                bytes_relayed = session.total_bytes(),
                "relay session closed"
            );
        }
    }

    /// Current number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Get a session by ID (for inspection/testing).
    pub fn get_session(&self, session_id: &Uuid) -> Option<Arc<RelaySession>> {
        self.sessions.get(session_id).map(|r| r.value().clone())
    }

    /// List all active session IDs.
    pub fn list_sessions(&self) -> Vec<Uuid> {
        self.sessions.iter().map(|r| *r.key()).collect()
    }

    /// Prune sessions that have been idle (not accepted) for longer than
    /// `max_age`. Returns the number of sessions removed.
    pub fn prune_stale(&self, max_age: Duration) -> usize {
        let cutoff = Utc::now() - chrono::Duration::from_std(max_age).unwrap_or_default();
        let stale: Vec<Uuid> = self
            .sessions
            .iter()
            .filter(|r| !r.is_accepted() && r.created_at < cutoff)
            .map(|r| r.session_id)
            .collect();

        let count = stale.len();
        for id in stale {
            self.sessions.remove(&id);
        }

        if count > 0 {
            debug!(pruned = count, "pruned stale relay sessions");
        }
        count
    }
}

// ============================================================================
// HolePuncher
// ============================================================================

/// Coordinates TCP hole-punching for Cone NAT peers.
///
/// TCP simultaneous-open works when both sides attempt to connect to each
/// other's external address at the same time. The NAT devices see the
/// outgoing SYN and create a mapping; when the peer's SYN arrives it
/// matches the existing mapping and the connection completes.
///
/// In practice this requires:
/// 1. Both peers know each other's external address (from STUN or gossip).
/// 2. A signaling channel (e.g. via a relay node) to synchronize the
///    connection attempt.
/// 3. Retries, because timing is critical.
pub struct HolePuncher;

/// Number of rapid-fire connection attempts during hole-punching.
const PUNCH_ATTEMPTS: u32 = 5;

/// Delay between successive punch attempts.
const PUNCH_RETRY_DELAY: Duration = Duration::from_millis(200);

impl HolePuncher {
    /// Attempt a TCP hole-punch to `peer_external` from `local_port`.
    ///
    /// Tries up to [`PUNCH_ATTEMPTS`] times with [`PUNCH_RETRY_DELAY`]
    /// between attempts, giving up after `timeout` total elapsed time.
    /// On success returns the connected [`TcpStream`].
    pub async fn punch(
        &self,
        local_port: u16,
        peer_external: SocketAddr,
        timeout: Duration,
    ) -> Result<TcpStream, SwarmError> {
        let local_addr: SocketAddr = format!("0.0.0.0:{}", local_port)
            .parse()
            .map_err(|e| SwarmError::Relay(format!("invalid local port: {}", e)))?;

        let deadline = tokio::time::Instant::now() + timeout;

        for attempt in 0..PUNCH_ATTEMPTS {
            if tokio::time::Instant::now() >= deadline {
                break;
            }

            debug!(
                attempt = attempt + 1,
                peer = %peer_external,
                local_port = local_port,
                "hole-punch attempt"
            );

            // Bind a socket on the specific local port so the NAT creates a
            // consistent mapping.
            let socket = match tokio::net::TcpSocket::new_v4() {
                Ok(s) => s,
                Err(e) => {
                    debug!(error = %e, "failed to create TCP socket for hole-punch");
                    continue;
                }
            };

            // Allow address reuse so multiple attempts can bind the same port.
            if let Err(e) = socket.set_reuseaddr(true) {
                debug!(error = %e, "set_reuseaddr failed");
            }

            #[cfg(unix)]
            {
                if let Err(e) = socket.set_reuseport(true) {
                    debug!(error = %e, "set_reuseport failed");
                }
            }

            if let Err(e) = socket.bind(local_addr) {
                debug!(error = %e, local_addr = %local_addr, "bind failed for hole-punch");
                continue;
            }

            // Calculate remaining time until deadline.
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let connect_timeout = remaining.min(Duration::from_secs(2));

            match tokio::time::timeout(connect_timeout, socket.connect(peer_external)).await {
                Ok(Ok(stream)) => {
                    info!(
                        peer = %peer_external,
                        attempt = attempt + 1,
                        "hole-punch succeeded"
                    );
                    return Ok(stream);
                }
                Ok(Err(e)) => {
                    debug!(
                        attempt = attempt + 1,
                        error = %e,
                        "hole-punch connect attempt failed"
                    );
                }
                Err(_) => {
                    debug!(
                        attempt = attempt + 1,
                        "hole-punch connect attempt timed out"
                    );
                }
            }

            // Brief pause before retrying.
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining > PUNCH_RETRY_DELAY {
                tokio::time::sleep(PUNCH_RETRY_DELAY).await;
            }
        }

        Err(SwarmError::Relay(format!(
            "hole-punch to {} failed after {} attempts",
            peer_external, PUNCH_ATTEMPTS
        )))
    }
}

// ============================================================================
// ConnectionStrategy
// ============================================================================

/// The best connection strategy for reaching a particular peer.
///
/// Returned by [`choose_strategy`] after evaluating both sides' NAT types.
#[derive(Debug, Clone)]
pub enum ConnectionStrategy {
    /// Peer is directly reachable (public IP or same LAN).
    Direct(SocketAddr),
    /// Attempt TCP hole-punch via a signaling relay.
    HolePunch {
        /// The peer's externally-mapped address.
        peer_external: SocketAddr,
        /// The signaling relay that coordinates timing.
        signaler: SocketAddr,
    },
    /// Full relay: all traffic goes through the relay node.
    Relay {
        /// Address of the relay node.
        relay_addr: SocketAddr,
        /// Session ID allocated by the relay.
        session_id: Uuid,
    },
}

// ============================================================================
// Strategy selection
// ============================================================================

/// Determine the best [`ConnectionStrategy`] for connecting to a peer.
///
/// Decision matrix:
///
/// | Our NAT     | Peer NAT     | Strategy     |
/// |-------------|-------------|--------------|
/// | Public      | Public      | Direct       |
/// | Public      | Cone        | Direct (peer behind NAT but we're public) |
/// | Public      | Symmetric   | Direct (we're public, peer connects to us) |
/// | Cone        | Public      | Direct       |
/// | Cone        | Cone        | HolePunch    |
/// | Cone        | Symmetric   | Relay        |
/// | Symmetric   | Public      | Direct       |
/// | Symmetric   | Cone        | Relay        |
/// | Symmetric   | Symmetric   | Relay        |
/// | Unknown     | *           | Relay (conservative) |
///
/// If a direct or hole-punch strategy requires a `peer_addr` but none is
/// available, we fall back to Relay.
pub fn choose_strategy(
    our_nat: &NatType,
    peer_nat: &NatType,
    peer_addr: Option<SocketAddr>,
    relay_nodes: &[SocketAddr],
) -> Result<ConnectionStrategy, SwarmError> {
    let default_relay = relay_nodes.first().copied();

    // Helper: get a relay address or return an error if none is available.
    let require_relay = || -> Result<SocketAddr, SwarmError> {
        default_relay.ok_or(SwarmError::NoRelayAvailable)
    };

    match (our_nat, peer_nat) {
        // Both public -- always direct.
        (NatType::Public, NatType::Public) => {
            if let Some(addr) = peer_addr {
                Ok(ConnectionStrategy::Direct(addr))
            } else {
                Ok(ConnectionStrategy::Relay {
                    relay_addr: require_relay()?,
                    session_id: Uuid::new_v4(),
                })
            }
        }

        // One side is public -- direct connection should work.
        (NatType::Public, _) | (_, NatType::Public) => {
            if let Some(addr) = peer_addr {
                Ok(ConnectionStrategy::Direct(addr))
            } else {
                Ok(ConnectionStrategy::Relay {
                    relay_addr: require_relay()?,
                    session_id: Uuid::new_v4(),
                })
            }
        }

        // Both Cone -- hole-punchable.
        (NatType::ConeNat, NatType::ConeNat) => {
            if let Some(addr) = peer_addr {
                Ok(ConnectionStrategy::HolePunch {
                    peer_external: addr,
                    signaler: require_relay()?,
                })
            } else {
                Ok(ConnectionStrategy::Relay {
                    relay_addr: require_relay()?,
                    session_id: Uuid::new_v4(),
                })
            }
        }

        // One side Symmetric -- relay required.
        (NatType::ConeNat, NatType::SymmetricNat)
        | (NatType::SymmetricNat, NatType::ConeNat)
        | (NatType::SymmetricNat, NatType::SymmetricNat) => Ok(ConnectionStrategy::Relay {
            relay_addr: require_relay()?,
            session_id: Uuid::new_v4(),
        }),

        // Unknown NAT -- play it safe with relay.
        _ => Ok(ConnectionStrategy::Relay {
            relay_addr: require_relay()?,
            session_id: Uuid::new_v4(),
        }),
    }
}

// ============================================================================
// Connection helper
// ============================================================================

/// High-level connection helper: given a strategy, establish a TCP stream.
///
/// - **Direct**: plain `TcpStream::connect`.
/// - **HolePunch**: uses [`HolePuncher::punch`]; falls back to relay on
///   failure.
/// - **Relay**: opens a TCP connection to the relay and sends a handshake
///   frame with the session ID so the relay knows which session this
///   connection belongs to.
pub async fn connect_with_strategy(
    strategy: &ConnectionStrategy,
    fallback_relay: Option<SocketAddr>,
) -> Result<TcpStream, SwarmError> {
    match strategy {
        ConnectionStrategy::Direct(addr) => {
            TcpStream::connect(addr)
                .await
                .map_err(|e| SwarmError::Relay(format!("direct connect to {} failed: {}", addr, e)))
        }

        ConnectionStrategy::HolePunch {
            peer_external,
            signaler: _,
        } => {
            let puncher = HolePuncher;
            match puncher
                .punch(0, *peer_external, HOLE_PUNCH_TIMEOUT)
                .await
            {
                Ok(stream) => Ok(stream),
                Err(e) => {
                    warn!(error = %e, "hole-punch failed, falling back to relay");
                    if let Some(relay_addr) = fallback_relay {
                        TcpStream::connect(relay_addr).await.map_err(|e2| {
                            SwarmError::Relay(format!(
                                "relay fallback to {} also failed: {}",
                                relay_addr, e2
                            ))
                        })
                    } else {
                        Err(e)
                    }
                }
            }
        }

        ConnectionStrategy::Relay {
            relay_addr,
            session_id,
        } => {
            let mut stream = TcpStream::connect(relay_addr).await.map_err(|e| {
                SwarmError::Relay(format!("connect to relay {} failed: {}", relay_addr, e))
            })?;

            // Send session handshake: 16-byte session UUID so the relay knows
            // which session to associate this connection with.
            stream
                .write_all(session_id.as_bytes())
                .await
                .map_err(|e| {
                    SwarmError::Relay(format!("relay handshake write failed: {}", e))
                })?;

            Ok(stream)
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    // -----------------------------------------------------------------------
    // NatProber::parse_stun_response
    // -----------------------------------------------------------------------

    #[test]
    fn parse_stun_response_ipv4() {
        let mut data = Vec::new();
        data.extend_from_slice(STUN_MAGIC); // 4 bytes magic
        data.push(4); // family IPv4
        data.extend_from_slice(&8080u16.to_be_bytes()); // port
        data.extend_from_slice(&[192, 168, 1, 100]); // address

        let result = NatProber::parse_stun_response(&data);
        assert!(result.is_some());
        let addr = result.unwrap();
        assert_eq!(addr.port(), 8080);
        assert_eq!(
            addr.ip(),
            std::net::IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100))
        );
    }

    #[test]
    fn parse_stun_response_ipv6() {
        let mut data = Vec::new();
        data.extend_from_slice(STUN_MAGIC);
        data.push(6); // family IPv6
        data.extend_from_slice(&9090u16.to_be_bytes());
        // 16 bytes of IPv6 address (::1)
        let mut ipv6_bytes = [0u8; 16];
        ipv6_bytes[15] = 1;
        data.extend_from_slice(&ipv6_bytes);

        let result = NatProber::parse_stun_response(&data);
        assert!(result.is_some());
        let addr = result.unwrap();
        assert_eq!(addr.port(), 9090);
        assert_eq!(
            addr.ip(),
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
        );
    }

    #[test]
    fn parse_stun_response_too_short() {
        let data = b"CAMB4";
        assert!(NatProber::parse_stun_response(data).is_none());
    }

    #[test]
    fn parse_stun_response_bad_magic() {
        let mut data = vec![0, 0, 0, 0, 4, 0x1F, 0x90, 127, 0, 0, 1];
        assert!(NatProber::parse_stun_response(&data).is_none());
    }

    #[test]
    fn parse_stun_response_unknown_family() {
        let mut data = Vec::new();
        data.extend_from_slice(STUN_MAGIC);
        data.push(99); // unknown family
        data.extend_from_slice(&8080u16.to_be_bytes());
        data.extend_from_slice(&[127, 0, 0, 1]);
        assert!(NatProber::parse_stun_response(&data).is_none());
    }

    // -----------------------------------------------------------------------
    // NatProber::probe with no servers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn probe_no_servers_returns_unknown() {
        let prober = NatProber;
        let result = prober.probe(&[]).await;
        assert_eq!(result.nat_type, NatType::Unknown);
        assert!(result.external_addr.is_none());
        assert!(result.stun_server_used.is_none());
    }

    // -----------------------------------------------------------------------
    // RelayServer
    // -----------------------------------------------------------------------

    #[test]
    fn relay_server_create_and_accept_session() {
        let server = RelayServer::new(addr(5000));
        let initiator = NodeId::new();
        let target = NodeId::new();

        let session_id = server.create_session(initiator, target).unwrap();
        assert_eq!(server.session_count(), 1);

        // Accept from wrong node should fail.
        let wrong_node = NodeId::new();
        assert!(server.accept_session(session_id, wrong_node).is_err());

        // Accept from correct target should succeed.
        server.accept_session(session_id, target).unwrap();

        let session = server.get_session(&session_id).unwrap();
        assert!(session.is_accepted());
    }

    #[test]
    fn relay_server_relay_data() {
        let server = RelayServer::new(addr(5001));
        let initiator = NodeId::new();
        let target = NodeId::new();

        let session_id = server.create_session(initiator, target).unwrap();

        // Relay before accept should fail.
        let result = server.relay_data(session_id, vec![1, 2, 3]);
        assert!(result.is_err());

        // Accept, then relay.
        server.accept_session(session_id, target).unwrap();
        let result = server.relay_data(session_id, vec![1, 2, 3]);
        assert!(result.is_ok());

        let session = server.get_session(&session_id).unwrap();
        assert_eq!(session.total_bytes(), 3);
    }

    #[test]
    fn relay_server_bandwidth_cap() {
        let mut server = RelayServer::new(addr(5002));
        server.max_bandwidth_per_session = 10; // Very low cap for testing.

        let initiator = NodeId::new();
        let target = NodeId::new();

        let session_id = server.create_session(initiator, target).unwrap();
        server.accept_session(session_id, target).unwrap();

        // First relay within cap.
        assert!(server.relay_data(session_id, vec![0; 5]).is_ok());
        // Second relay still within cap.
        assert!(server.relay_data(session_id, vec![0; 5]).is_ok());
        // Third relay exceeds cap.
        assert!(server.relay_data(session_id, vec![0; 5]).is_err());
    }

    #[test]
    fn relay_server_session_limit() {
        let mut server = RelayServer::new(addr(5003));
        server.max_sessions = 2;

        let n1 = NodeId::new();
        let n2 = NodeId::new();
        let n3 = NodeId::new();
        let n4 = NodeId::new();

        assert!(server.create_session(n1, n2).is_ok());
        assert!(server.create_session(n3, n4).is_ok());
        // Third session should fail.
        assert!(server.create_session(n1, n3).is_err());
        assert_eq!(server.session_count(), 2);
    }

    #[test]
    fn relay_server_close_session() {
        let server = RelayServer::new(addr(5004));
        let session_id = server
            .create_session(NodeId::new(), NodeId::new())
            .unwrap();
        assert_eq!(server.session_count(), 1);

        server.close_session(&session_id);
        assert_eq!(server.session_count(), 0);
    }

    #[test]
    fn relay_server_close_nonexistent_session() {
        let server = RelayServer::new(addr(5005));
        // Should not panic.
        server.close_session(&Uuid::new_v4());
        assert_eq!(server.session_count(), 0);
    }

    #[test]
    fn relay_server_relay_nonexistent_session() {
        let server = RelayServer::new(addr(5006));
        let result = server.relay_data(Uuid::new_v4(), vec![1]);
        assert!(result.is_err());
    }

    #[test]
    fn relay_server_list_sessions() {
        let server = RelayServer::new(addr(5007));
        let id1 = server
            .create_session(NodeId::new(), NodeId::new())
            .unwrap();
        let id2 = server
            .create_session(NodeId::new(), NodeId::new())
            .unwrap();

        let mut ids = server.list_sessions();
        ids.sort();
        let mut expected = vec![id1, id2];
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn relay_server_prune_stale() {
        let server = RelayServer::new(addr(5008));
        let target = NodeId::new();
        let id1 = server
            .create_session(NodeId::new(), target)
            .unwrap();
        let id2 = server
            .create_session(NodeId::new(), NodeId::new())
            .unwrap();

        // Accept one session so it won't be pruned.
        server.accept_session(id1, target).unwrap();

        // Prune with a zero-duration max_age won't prune anything just created.
        let pruned = server.prune_stale(Duration::from_secs(0));
        // Sessions just created -- their created_at is "now", so with a 0s
        // max_age the cutoff is also "now". Depending on clock granularity
        // they might or might not be older. The accepted one is never pruned.
        // The important invariant is that accepted sessions survive.
        assert!(server.get_session(&id1).is_some());
    }

    // -----------------------------------------------------------------------
    // RelaySession
    // -----------------------------------------------------------------------

    #[test]
    fn relay_session_accept_and_bytes() {
        let session = RelaySession::new(Uuid::new_v4(), NodeId::new(), NodeId::new());
        assert!(!session.is_accepted());
        assert_eq!(session.total_bytes(), 0);

        session.accept();
        assert!(session.is_accepted());

        session.bytes_relayed.fetch_add(100, Ordering::Relaxed);
        assert_eq!(session.total_bytes(), 100);
    }

    // -----------------------------------------------------------------------
    // choose_strategy
    // -----------------------------------------------------------------------

    #[test]
    fn strategy_public_public_direct() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::Public,
            &NatType::Public,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Direct(_)));
    }

    #[test]
    fn strategy_public_cone_direct() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::Public,
            &NatType::ConeNat,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Direct(_)));
    }

    #[test]
    fn strategy_cone_public_direct() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::ConeNat,
            &NatType::Public,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Direct(_)));
    }

    #[test]
    fn strategy_cone_cone_hole_punch() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::ConeNat,
            &NatType::ConeNat,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::HolePunch { .. }));
    }

    #[test]
    fn strategy_cone_symmetric_relay() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::ConeNat,
            &NatType::SymmetricNat,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Relay { .. }));
    }

    #[test]
    fn strategy_symmetric_symmetric_relay() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::SymmetricNat,
            &NatType::SymmetricNat,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Relay { .. }));
    }

    #[test]
    fn strategy_unknown_relay() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::Unknown,
            &NatType::ConeNat,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Relay { .. }));
    }

    #[test]
    fn strategy_no_peer_addr_fallback_to_relay() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::Public,
            &NatType::Public,
            None, // No address known.
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Relay { .. }));
    }

    #[test]
    fn strategy_no_relays_available() {
        let result = choose_strategy(
            &NatType::SymmetricNat,
            &NatType::SymmetricNat,
            Some(addr(4200)),
            &[], // No relay nodes.
        );
        // Should return an error, not a dummy address.
        assert!(matches!(result, Err(SwarmError::NoRelayAvailable)));
    }

    #[test]
    fn strategy_symmetric_cone_relay() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::SymmetricNat,
            &NatType::ConeNat,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Relay { .. }));
    }

    #[test]
    fn strategy_public_symmetric_direct() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::Public,
            &NatType::SymmetricNat,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        // Public node is directly reachable from any NAT type.
        assert!(matches!(strategy, ConnectionStrategy::Direct(_)));
    }

    #[test]
    fn strategy_symmetric_public_direct() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::SymmetricNat,
            &NatType::Public,
            Some(addr(4200)),
            &relays,
        )
        .unwrap();
        assert!(matches!(strategy, ConnectionStrategy::Direct(_)));
    }

    #[test]
    fn strategy_cone_cone_no_addr_relay() {
        let relays = vec![addr(9000)];
        let strategy = choose_strategy(
            &NatType::ConeNat,
            &NatType::ConeNat,
            None,
            &relays,
        )
        .unwrap();
        // Can't hole-punch without peer address.
        assert!(matches!(strategy, ConnectionStrategy::Relay { .. }));
    }

    // -----------------------------------------------------------------------
    // NatProbeResult default creation
    // -----------------------------------------------------------------------

    #[test]
    fn nat_probe_result_fields() {
        let result = NatProbeResult {
            nat_type: NatType::ConeNat,
            external_addr: Some(addr(12345)),
            stun_server_used: Some(addr(3478)),
        };
        assert_eq!(result.nat_type, NatType::ConeNat);
        assert_eq!(result.external_addr.unwrap().port(), 12345);
        assert_eq!(result.stun_server_used.unwrap().port(), 3478);
    }
}
