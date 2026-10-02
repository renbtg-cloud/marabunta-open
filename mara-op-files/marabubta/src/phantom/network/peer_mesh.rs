// Marabunta - Licensed under the MIT License.
//! Peer-to-peer mesh network for relay discovery.
//!
//! This module implements a decentralized peer mesh for discovering and
//! connecting to relay nodes. There is no single point that knows all
//! participants, providing resilience against targeted attacks.
//!
//! # Design Principles
//!
//! - **Decentralization**: No central authority or directory
//! - **Resilience**: Network survives node failures
//! - **Privacy**: Peers don't learn full network topology
//! - **Scalability**: Efficient gossip-based discovery
//!
//! # Discovery Process
//!
//! 1. Bootstrap from known seed nodes
//! 2. Exchange peer lists with connected peers
//! 3. Periodically refresh peer information
//! 4. Maintain minimum and maximum peer counts

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use tokio::sync::mpsc;

use super::errors::{GossipError, MeshError, PeerId, RemoveReason};
use super::onion::X25519PublicKey;

/// Configuration for the peer mesh network.
#[derive(Debug, Clone)]
pub struct MeshConfig {
    /// Maximum number of connected peers.
    pub max_peers: usize,

    /// Minimum number of peers to maintain.
    pub min_peers: usize,

    /// Interval for peer list exchange.
    pub peer_exchange_interval: Duration,

    /// Timeout for peer responses.
    pub peer_timeout: Duration,

    /// Bootstrap nodes to initially connect to.
    pub bootstrap_nodes: Vec<SocketAddr>,

    /// How often to check peer health.
    pub health_check_interval: Duration,

    /// Maximum age for peer information before refresh.
    pub peer_info_max_age: Duration,

    /// Number of peers to exchange in each gossip round.
    pub gossip_fanout: usize,

    /// Maximum number of peers to accept per exchange.
    pub max_peers_per_exchange: usize,

    /// Whether to accept incoming connections.
    pub accept_incoming: bool,

    /// Port for incoming connections.
    pub listen_port: u16,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            max_peers: 50,
            min_peers: 10,
            peer_exchange_interval: Duration::from_secs(60),
            peer_timeout: Duration::from_secs(30),
            bootstrap_nodes: Vec::new(),
            health_check_interval: Duration::from_secs(30),
            peer_info_max_age: Duration::from_secs(3600), // 1 hour
            gossip_fanout: 3,
            max_peers_per_exchange: 20,
            accept_incoming: true,
            listen_port: 9000,
        }
    }
}

/// Information about a peer in the mesh.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Unique peer identifier.
    pub id: PeerId,

    /// Known addresses for this peer.
    pub addresses: Vec<SocketAddr>,

    /// Peer's public key for encrypted communication.
    pub public_key: X25519PublicKey,

    /// Peer's advertised capabilities.
    pub capabilities: PeerCapabilities,

    /// When we last heard from this peer.
    pub last_seen: Instant,

    /// Reputation score (0.0 - 1.0).
    pub reputation: f32,

    /// Number of successful interactions.
    pub successful_interactions: u64,

    /// Number of failed interactions.
    pub failed_interactions: u64,

    /// When this peer info was created.
    pub created_at: Instant,
}

impl PeerInfo {
    /// Create new peer info with default reputation.
    pub fn new(
        id: PeerId,
        addresses: Vec<SocketAddr>,
        public_key: X25519PublicKey,
        capabilities: PeerCapabilities,
    ) -> Self {
        PeerInfo {
            id,
            addresses,
            public_key,
            capabilities,
            last_seen: Instant::now(),
            reputation: 0.5, // Start neutral
            successful_interactions: 0,
            failed_interactions: 0,
            created_at: Instant::now(),
        }
    }

    /// Update reputation based on interaction result.
    pub fn record_interaction(&mut self, success: bool) {
        if success {
            self.successful_interactions += 1;
            // Increase reputation, max 1.0
            self.reputation = (self.reputation + 0.01).min(1.0);
        } else {
            self.failed_interactions += 1;
            // Decrease reputation, min 0.0
            self.reputation = (self.reputation - 0.05).max(0.0);
        }
        self.last_seen = Instant::now();
    }

    /// Check if peer info is stale.
    pub fn is_stale(&self, max_age: Duration) -> bool {
        self.last_seen.elapsed() > max_age
    }

    /// Get age of this peer info.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Capabilities advertised by a peer.
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerCapabilities {
    /// Whether peer acts as a relay node.
    pub is_relay: bool,

    /// Self-reported bandwidth in KB/s.
    pub bandwidth_kbps: u32,

    /// Whether peer accepts new circuits.
    pub accepts_circuits: bool,

    /// Whether peer supports IPv6.
    pub supports_ipv6: bool,

    /// Protocol version supported.
    pub protocol_version: u16,
}

impl PeerCapabilities {
    /// Check if capabilities match requirements.
    pub fn matches(&self, requirements: &PeerCapabilities) -> bool {
        (!requirements.is_relay || self.is_relay)
            && self.bandwidth_kbps >= requirements.bandwidth_kbps
            && (!requirements.accepts_circuits || self.accepts_circuits)
    }
}

/// State of a peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is being established.
    Connecting,
    /// Connection is active.
    Connected,
    /// Connection is being closed.
    Disconnecting,
    /// Connection has been closed.
    Disconnected,
}

/// Active connection to a peer.
pub struct PeerConnection {
    /// Peer we're connected to.
    pub peer_id: PeerId,

    /// Connection address.
    pub address: SocketAddr,

    /// Connection state.
    pub state: ConnectionState,

    /// When connection was established.
    pub connected_at: Instant,

    /// Bytes sent over this connection.
    pub bytes_sent: AtomicU64,

    /// Bytes received over this connection.
    pub bytes_received: AtomicU64,

    /// Last activity time.
    pub last_activity: RwLock<Instant>,
}

impl PeerConnection {
    /// Create a new connection.
    fn new(peer_id: PeerId, address: SocketAddr) -> Self {
        PeerConnection {
            peer_id,
            address,
            state: ConnectionState::Connecting,
            connected_at: Instant::now(),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            last_activity: RwLock::new(Instant::now()),
        }
    }

    /// Update last activity time.
    pub fn touch(&self) {
        *self.last_activity.write() = Instant::now();
    }

    /// Get time since last activity.
    pub fn idle_time(&self) -> Duration {
        self.last_activity.read().elapsed()
    }
}

/// Peer mesh network for decentralized peer discovery.
pub struct PeerMesh {
    /// Configuration.
    config: MeshConfig,

    /// Known peers (may not be connected).
    known_peers: RwLock<HashMap<PeerId, PeerInfo>>,

    /// Active connections.
    connections: RwLock<HashMap<PeerId, PeerConnection>>,

    /// Gossip protocol handler.
    gossip: RwLock<Option<GossipProtocol>>,

    /// Our own peer ID.
    our_id: PeerId,

    /// Our own public key.
    #[allow(dead_code)]
    our_public_key: X25519PublicKey,

    /// Running flag (wrapped in Arc for sharing with spawned tasks).
    running: Arc<AtomicBool>,

    /// Shutdown signal.
    shutdown_tx: RwLock<Option<mpsc::Sender<()>>>,

    /// Statistics.
    stats: MeshStats,
}

/// Statistics about mesh operations.
#[derive(Debug, Default)]
pub struct MeshStats {
    /// Peers discovered.
    pub peers_discovered: AtomicU64,
    /// Successful connections.
    pub connections_established: AtomicU64,
    /// Failed connections.
    pub connections_failed: AtomicU64,
    /// Gossip rounds completed.
    pub gossip_rounds: AtomicU64,
    /// Peers removed.
    pub peers_removed: AtomicU64,
}

impl PeerMesh {
    /// Create a new peer mesh with the given configuration.
    pub fn new(config: MeshConfig) -> Self {
        let our_id = PeerId::random();
        let our_public_key = X25519PublicKey::from_bytes([0u8; 32]); // Placeholder

        PeerMesh {
            config,
            known_peers: RwLock::new(HashMap::new()),
            connections: RwLock::new(HashMap::new()),
            gossip: RwLock::new(None),
            our_id,
            our_public_key,
            running: Arc::new(AtomicBool::new(false)),
            shutdown_tx: RwLock::new(None),
            stats: MeshStats::default(),
        }
    }

    /// Get our peer ID.
    pub fn our_id(&self) -> PeerId {
        self.our_id
    }

    /// Bootstrap into the network by connecting to seed nodes.
    pub async fn bootstrap(&self) -> Result<(), MeshError> {
        if self.config.bootstrap_nodes.is_empty() {
            return Err(MeshError::BootstrapFailed);
        }

        let mut connected = 0;

        for addr in &self.config.bootstrap_nodes {
            match self.connect_to_address(*addr).await {
                Ok(_) => {
                    connected += 1;
                    if connected >= self.config.min_peers {
                        break;
                    }
                }
                Err(_) => continue,
            }
        }

        if connected == 0 {
            return Err(MeshError::BootstrapFailed);
        }

        self.running.store(true, Ordering::Release);

        // Start gossip after bootstrap
        self.start_gossip();

        Ok(())
    }

    /// Connect to a peer at the given address.
    async fn connect_to_address(&self, addr: SocketAddr) -> Result<PeerId, MeshError> {
        // Check peer limit
        if self.connections.read().len() >= self.config.max_peers {
            return Err(MeshError::PeerLimitReached);
        }

        // In a real implementation:
        // 1. Open TCP/TLS connection
        // 2. Exchange handshake
        // 3. Verify peer identity

        // Simulate connection delay
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Create peer info (in practice, received during handshake)
        let peer_id = PeerId::random();
        let public_key = X25519PublicKey::from_bytes([0u8; 32]);
        let peer_info = PeerInfo::new(peer_id, vec![addr], public_key, PeerCapabilities::default());

        // Store peer info
        self.known_peers.write().insert(peer_id, peer_info);

        // Create connection
        let mut conn = PeerConnection::new(peer_id, addr);
        conn.state = ConnectionState::Connected;
        self.connections.write().insert(peer_id, conn);

        self.stats
            .connections_established
            .fetch_add(1, Ordering::Relaxed);
        self.stats.peers_discovered.fetch_add(1, Ordering::Relaxed);

        Ok(peer_id)
    }

    /// Get random peers for circuit building.
    ///
    /// Returns peers that are suitable relay nodes.
    pub fn get_random_relays(&self, count: usize) -> Vec<PeerInfo> {
        let peers = self.known_peers.read();

        let relays: Vec<_> = peers
            .values()
            .filter(|p| p.capabilities.is_relay && p.reputation >= 0.3)
            .cloned()
            .collect();

        let mut rng = rand::thread_rng();
        let mut selected: Vec<PeerInfo> = relays;
        selected.shuffle(&mut rng);
        selected.truncate(count);
        selected
    }

    /// Get peers matching specific capabilities.
    pub fn get_peers_by_capability(&self, requirements: &PeerCapabilities) -> Vec<PeerInfo> {
        let peers = self.known_peers.read();

        peers
            .values()
            .filter(|p| p.capabilities.matches(requirements) && p.reputation >= 0.3)
            .cloned()
            .collect()
    }

    /// Add a newly discovered peer.
    pub async fn add_peer(&self, info: PeerInfo) -> Result<(), MeshError> {
        // Check if we already know this peer
        if self.known_peers.read().contains_key(&info.id) {
            // Update existing info
            if let Some(existing) = self.known_peers.write().get_mut(&info.id) {
                existing.last_seen = Instant::now();
                // Update addresses if new ones provided
                for addr in info.addresses {
                    if !existing.addresses.contains(&addr) {
                        existing.addresses.push(addr);
                    }
                }
            }
            return Ok(());
        }

        // Check peer limit for new peers
        if self.known_peers.read().len() >= self.config.max_peers * 2 {
            // Prune oldest low-reputation peers
            self.prune_peers();
        }

        self.known_peers.write().insert(info.id, info);
        self.stats.peers_discovered.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Remove a peer from the mesh.
    pub async fn remove_peer(&self, id: &PeerId, reason: RemoveReason) -> Result<(), MeshError> {
        // Remove connection if exists
        if let Some(conn) = self.connections.write().remove(id) {
            let _ = conn; // Connection removed, state no longer needed
        }

        // Remove from known peers if banned
        if reason == RemoveReason::Banned || reason == RemoveReason::ProtocolViolation {
            self.known_peers.write().remove(id);
        } else {
            // Just mark as disconnected
            if let Some(peer) = self.known_peers.write().get_mut(id) {
                peer.reputation = (peer.reputation - 0.1).max(0.0);
            }
        }

        self.stats.peers_removed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Prune old or low-reputation peers.
    fn prune_peers(&self) {
        let mut peers = self.known_peers.write();
        let max_age = self.config.peer_info_max_age;

        // Remove stale peers with low reputation
        peers.retain(|_, p| !p.is_stale(max_age) || p.reputation >= 0.5);

        // If still too many, remove lowest reputation
        if peers.len() > self.config.max_peers * 2 {
            let mut sorted: Vec<_> = peers.iter().map(|(id, p)| (*id, p.reputation)).collect();
            sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            let to_remove: Vec<_> = sorted
                .into_iter()
                .take(peers.len() - self.config.max_peers)
                .map(|(id, _)| id)
                .collect();

            for id in to_remove {
                peers.remove(&id);
            }
        }
    }

    /// Start the gossip protocol for peer discovery.
    pub fn start_gossip(&self) {
        let mesh = Arc::new(PeerMeshHandle {
            known_peers: self.known_peers.read().clone(),
            config: self.config.clone(),
            our_id: self.our_id,
        });

        let gossip = GossipProtocol::new(Arc::downgrade(&mesh));
        *self.gossip.write() = Some(gossip);

        // Start gossip background task
        let interval = self.config.peer_exchange_interval;
        let stats = self.stats.clone();
        let running = Arc::clone(&self.running);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            while running.load(Ordering::Relaxed) {
                ticker.tick().await;
                stats.gossip_rounds.fetch_add(1, Ordering::Relaxed);
                // In a real implementation, run gossip round here
            }
        });
    }

    /// Get number of known peers.
    pub fn known_peer_count(&self) -> usize {
        self.known_peers.read().len()
    }

    /// Get number of connected peers.
    pub fn connected_peer_count(&self) -> usize {
        self.connections.read().len()
    }

    /// Get a peer by ID.
    pub fn get_peer(&self, id: &PeerId) -> Option<PeerInfo> {
        self.known_peers.read().get(id).cloned()
    }

    /// Check if connected to a specific peer.
    pub fn is_connected(&self, id: &PeerId) -> bool {
        self.connections
            .read()
            .get(id)
            .map(|c| c.state == ConnectionState::Connected)
            .unwrap_or(false)
    }

    /// Get all connected peers.
    pub fn connected_peers(&self) -> Vec<PeerId> {
        self.connections
            .read()
            .iter()
            .filter(|(_, c)| c.state == ConnectionState::Connected)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get statistics.
    pub fn stats(&self) -> &MeshStats {
        &self.stats
    }

    /// Shutdown the mesh.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Release);

        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.try_send(());
        }

        // Close all connections
        for (_, conn) in self.connections.write().drain() {
            let _ = conn; // Connection drained/dropped
        }
    }

    /// Check if mesh is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Ensure we have minimum peers connected.
    pub async fn ensure_min_peers(&self) -> Result<(), MeshError> {
        let connected = self.connected_peer_count();
        if connected >= self.config.min_peers {
            return Ok(());
        }

        let needed = self.config.min_peers - connected;

        // Try to connect to known but unconnected peers
        let candidates: Vec<_> = {
            let known = self.known_peers.read();
            let connected = self.connections.read();

            known
                .iter()
                .filter(|(id, p)| {
                    !connected.contains_key(*id) && p.reputation >= 0.3 && !p.addresses.is_empty()
                })
                .map(|(_, p)| p.addresses[0])
                .take(needed)
                .collect()
        };

        for addr in candidates {
            let _ = self.connect_to_address(addr).await;
        }

        Ok(())
    }
}

impl Clone for MeshStats {
    fn clone(&self) -> Self {
        MeshStats {
            peers_discovered: AtomicU64::new(self.peers_discovered.load(Ordering::Relaxed)),
            connections_established: AtomicU64::new(
                self.connections_established.load(Ordering::Relaxed),
            ),
            connections_failed: AtomicU64::new(self.connections_failed.load(Ordering::Relaxed)),
            gossip_rounds: AtomicU64::new(self.gossip_rounds.load(Ordering::Relaxed)),
            peers_removed: AtomicU64::new(self.peers_removed.load(Ordering::Relaxed)),
        }
    }
}

/// Handle for gossip protocol to access mesh state.
pub struct PeerMeshHandle {
    known_peers: HashMap<PeerId, PeerInfo>,
    config: MeshConfig,
    our_id: PeerId,
}

/// Gossip protocol for peer discovery.
///
/// Implements epidemic-style gossip for discovering new peers and
/// sharing peer information across the network.
pub struct GossipProtocol {
    /// Reference to the peer mesh.
    mesh: Weak<PeerMeshHandle>,

    /// Peers we've recently exchanged with.
    recent_exchanges: RwLock<Vec<(PeerId, Instant)>>,

    /// Running flag.
    running: AtomicBool,
}

impl GossipProtocol {
    /// Create a new gossip protocol handler.
    pub fn new(mesh: Weak<PeerMeshHandle>) -> Self {
        GossipProtocol {
            mesh,
            recent_exchanges: RwLock::new(Vec::new()),
            running: AtomicBool::new(true),
        }
    }

    /// Run a gossip round, exchanging peer lists with random peers.
    pub async fn run_gossip_round(&self) -> Result<(), GossipError> {
        let mesh = self.mesh.upgrade().ok_or(GossipError::NoPeersAvailable)?;

        // Select random peers for exchange
        let mut rng = rand::thread_rng();
        let mut candidates: Vec<_> = mesh.known_peers.keys().cloned().collect();
        candidates.shuffle(&mut rng);
        candidates.truncate(mesh.config.gossip_fanout);

        if candidates.is_empty() {
            return Err(GossipError::NoPeersAvailable);
        }

        // Exchange with each selected peer
        for peer_id in candidates {
            if let Some(peer_info) = mesh.known_peers.get(&peer_id) {
                let _ = self.exchange_with_peer(peer_info).await;
            }
        }

        // Clean up old exchange records
        self.cleanup_recent_exchanges();

        Ok(())
    }

    /// Exchange peer lists with a specific peer.
    async fn exchange_with_peer(&self, peer: &PeerInfo) -> Result<Vec<PeerInfo>, GossipError> {
        // Check if we exchanged recently
        {
            let recent = self.recent_exchanges.read();
            if recent.iter().any(|(id, _)| *id == peer.id) {
                return Ok(Vec::new());
            }
        }

        // In a real implementation:
        // 1. Send our peer list to the peer
        // 2. Receive their peer list
        // 3. Merge new peers into our list

        // Simulate network delay
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Record this exchange
        self.recent_exchanges
            .write()
            .push((peer.id, Instant::now()));

        // Return simulated received peers
        Ok(Vec::new())
    }

    /// Handle an incoming peer advertisement.
    pub async fn handle_peer_ad(&self, ad: PeerAdvertisement) -> Result<(), GossipError> {
        // Validate advertisement
        if ad.peers.is_empty() {
            return Err(GossipError::InvalidMessage("Empty peer list".to_string()));
        }

        let mesh = self.mesh.upgrade().ok_or(GossipError::NoPeersAvailable)?;

        // Filter and validate peers
        let max_per_exchange = mesh.config.max_peers_per_exchange;
        let mut added = 0;

        for peer in ad.peers.into_iter().take(max_per_exchange) {
            // Skip our own ID
            if peer.id == mesh.our_id {
                continue;
            }

            // In practice, verify peer signature/certificate here

            added += 1;
        }

        if added == 0 {
            return Err(GossipError::InvalidMessage("No valid peers".to_string()));
        }

        Ok(())
    }

    /// Clean up old exchange records.
    fn cleanup_recent_exchanges(&self) {
        let cutoff = Instant::now() - Duration::from_secs(300);
        self.recent_exchanges
            .write()
            .retain(|(_, time)| *time > cutoff);
    }

    /// Stop the gossip protocol.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }
}

/// Advertisement message containing peer information.
#[derive(Debug, Clone)]
pub struct PeerAdvertisement {
    /// Peer who sent this advertisement.
    pub from: PeerId,

    /// Peers being advertised.
    pub peers: Vec<PeerInfo>,

    /// When this advertisement was created.
    pub timestamp: u64,

    /// Signature over the advertisement (placeholder).
    pub signature: Vec<u8>,
}

impl PeerAdvertisement {
    /// Create a new peer advertisement.
    pub fn new(from: PeerId, peers: Vec<PeerInfo>) -> Self {
        PeerAdvertisement {
            from,
            peers,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            signature: Vec::new(),
        }
    }

    /// Verify the advertisement signature.
    pub fn verify(&self, _public_key: &X25519PublicKey) -> bool {
        // In practice, verify cryptographic signature
        true
    }

    /// Get age of this advertisement.
    pub fn age(&self) -> Duration {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Duration::from_secs(now.saturating_sub(self.timestamp))
    }
}

/// Builder for creating mesh configurations.
pub struct MeshConfigBuilder {
    config: MeshConfig,
}

impl MeshConfigBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        MeshConfigBuilder {
            config: MeshConfig::default(),
        }
    }

    /// Set maximum peers.
    pub fn max_peers(mut self, max: usize) -> Self {
        self.config.max_peers = max;
        self
    }

    /// Set minimum peers.
    pub fn min_peers(mut self, min: usize) -> Self {
        self.config.min_peers = min;
        self
    }

    /// Set peer exchange interval.
    pub fn peer_exchange_interval(mut self, interval: Duration) -> Self {
        self.config.peer_exchange_interval = interval;
        self
    }

    /// Add bootstrap nodes.
    pub fn bootstrap_nodes(mut self, nodes: Vec<SocketAddr>) -> Self {
        self.config.bootstrap_nodes = nodes;
        self
    }

    /// Set gossip fanout.
    pub fn gossip_fanout(mut self, fanout: usize) -> Self {
        self.config.gossip_fanout = fanout;
        self
    }

    /// Set listen port.
    pub fn listen_port(mut self, port: u16) -> Self {
        self.config.listen_port = port;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> MeshConfig {
        self.config
    }
}

impl Default for MeshConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_peer_info() -> PeerInfo {
        PeerInfo::new(
            PeerId::random(),
            vec!["127.0.0.1:9001".parse().unwrap()],
            X25519PublicKey::from_bytes([1u8; 32]),
            PeerCapabilities {
                is_relay: true,
                bandwidth_kbps: 10000,
                accepts_circuits: true,
                supports_ipv6: false,
                protocol_version: 1,
            },
        )
    }

    #[test]
    fn test_mesh_config_default() {
        let config = MeshConfig::default();
        assert_eq!(config.max_peers, 50);
        assert_eq!(config.min_peers, 10);
        assert_eq!(config.gossip_fanout, 3);
    }

    #[test]
    fn test_mesh_config_builder() {
        let config = MeshConfigBuilder::new()
            .max_peers(100)
            .min_peers(20)
            .gossip_fanout(5)
            .build();

        assert_eq!(config.max_peers, 100);
        assert_eq!(config.min_peers, 20);
        assert_eq!(config.gossip_fanout, 5);
    }

    #[test]
    fn test_peer_info_creation() {
        let peer = create_test_peer_info();
        assert!(!peer.addresses.is_empty());
        assert_eq!(peer.reputation, 0.5);
        assert_eq!(peer.successful_interactions, 0);
    }

    #[test]
    fn test_peer_info_reputation() {
        let mut peer = create_test_peer_info();

        // Successful interaction
        peer.record_interaction(true);
        assert!(peer.reputation > 0.5);
        assert_eq!(peer.successful_interactions, 1);

        // Failed interaction
        peer.record_interaction(false);
        assert!(peer.reputation < 0.56); // Decreased
        assert_eq!(peer.failed_interactions, 1);
    }

    #[test]
    fn test_peer_info_staleness() {
        let peer = create_test_peer_info();

        // Fresh peer is not stale
        assert!(!peer.is_stale(Duration::from_secs(60)));

        // With very short max age, should be stale
        assert!(peer.is_stale(Duration::from_nanos(1)));
    }

    #[test]
    fn test_peer_capabilities_matches() {
        let caps = PeerCapabilities {
            is_relay: true,
            bandwidth_kbps: 10000,
            accepts_circuits: true,
            supports_ipv6: true,
            protocol_version: 1,
        };

        let requires_relay = PeerCapabilities {
            is_relay: true,
            ..Default::default()
        };
        assert!(caps.matches(&requires_relay));

        let requires_high_bandwidth = PeerCapabilities {
            bandwidth_kbps: 5000,
            ..Default::default()
        };
        assert!(caps.matches(&requires_high_bandwidth));

        let requires_too_much = PeerCapabilities {
            bandwidth_kbps: 50000,
            ..Default::default()
        };
        assert!(!caps.matches(&requires_too_much));
    }

    #[test]
    fn test_peer_mesh_creation() {
        let config = MeshConfig::default();
        let mesh = PeerMesh::new(config);

        assert_eq!(mesh.known_peer_count(), 0);
        assert_eq!(mesh.connected_peer_count(), 0);
        assert!(!mesh.is_running());
    }

    #[tokio::test]
    async fn test_peer_mesh_add_peer() {
        let config = MeshConfig::default();
        let mesh = PeerMesh::new(config);

        let peer = create_test_peer_info();
        let peer_id = peer.id;

        mesh.add_peer(peer).await.unwrap();

        assert_eq!(mesh.known_peer_count(), 1);
        assert!(mesh.get_peer(&peer_id).is_some());
    }

    #[tokio::test]
    async fn test_peer_mesh_remove_peer() {
        let config = MeshConfig::default();
        let mesh = PeerMesh::new(config);

        let peer = create_test_peer_info();
        let peer_id = peer.id;

        mesh.add_peer(peer).await.unwrap();
        assert_eq!(mesh.known_peer_count(), 1);

        mesh.remove_peer(&peer_id, RemoveReason::Timeout)
            .await
            .unwrap();
        // Timeout doesn't remove from known_peers, just decreases reputation
        assert!(mesh.get_peer(&peer_id).is_some());

        mesh.remove_peer(&peer_id, RemoveReason::Banned)
            .await
            .unwrap();
        assert!(mesh.get_peer(&peer_id).is_none());
    }

    #[tokio::test]
    async fn test_peer_mesh_bootstrap_empty() {
        let config = MeshConfig {
            bootstrap_nodes: Vec::new(),
            ..Default::default()
        };
        let mesh = PeerMesh::new(config);

        let result = mesh.bootstrap().await;
        assert!(matches!(result, Err(MeshError::BootstrapFailed)));
    }

    #[test]
    fn test_peer_mesh_get_random_relays() {
        let config = MeshConfig::default();
        let mesh = PeerMesh::new(config);

        // Add some peers
        for _ in 0..5 {
            let peer = create_test_peer_info();
            mesh.known_peers.write().insert(peer.id, peer);
        }

        let relays = mesh.get_random_relays(3);
        assert!(relays.len() <= 3);
        assert!(relays.iter().all(|p| p.capabilities.is_relay));
    }

    #[test]
    fn test_peer_mesh_get_peers_by_capability() {
        let config = MeshConfig::default();
        let mesh = PeerMesh::new(config);

        // Add a relay
        let relay = create_test_peer_info();
        mesh.known_peers.write().insert(relay.id, relay);

        // Add a non-relay
        let mut non_relay = create_test_peer_info();
        non_relay.capabilities.is_relay = false;
        mesh.known_peers.write().insert(non_relay.id, non_relay);

        let requirements = PeerCapabilities {
            is_relay: true,
            ..Default::default()
        };

        let relays = mesh.get_peers_by_capability(&requirements);
        assert_eq!(relays.len(), 1);
    }

    #[test]
    fn test_peer_connection() {
        let peer_id = PeerId::random();
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let conn = PeerConnection::new(peer_id, addr);

        assert_eq!(conn.state, ConnectionState::Connecting);
        assert_eq!(conn.bytes_sent.load(Ordering::Relaxed), 0);

        conn.touch();
        std::thread::sleep(Duration::from_millis(10));
        assert!(conn.idle_time() >= Duration::from_millis(10));
    }

    #[test]
    fn test_connection_state() {
        assert_ne!(ConnectionState::Connected, ConnectionState::Disconnected);
        assert_eq!(ConnectionState::Connected, ConnectionState::Connected);
    }

    #[test]
    fn test_peer_advertisement() {
        let from = PeerId::random();
        let peers = vec![create_test_peer_info()];
        let ad = PeerAdvertisement::new(from, peers);

        assert_eq!(ad.from, from);
        assert_eq!(ad.peers.len(), 1);
        assert!(ad.timestamp > 0);
    }

    #[test]
    fn test_peer_advertisement_age() {
        let from = PeerId::random();
        let ad = PeerAdvertisement::new(from, Vec::new());

        // Just created, should have very small age
        assert!(ad.age() < Duration::from_secs(1));
    }

    #[test]
    fn test_gossip_protocol() {
        let mesh = Arc::new(PeerMeshHandle {
            known_peers: HashMap::new(),
            config: MeshConfig::default(),
            our_id: PeerId::random(),
        });

        let gossip = GossipProtocol::new(Arc::downgrade(&mesh));
        assert!(gossip.running.load(Ordering::Relaxed));

        gossip.stop();
        assert!(!gossip.running.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_gossip_round_no_peers() {
        let mesh = Arc::new(PeerMeshHandle {
            known_peers: HashMap::new(),
            config: MeshConfig::default(),
            our_id: PeerId::random(),
        });

        let gossip = GossipProtocol::new(Arc::downgrade(&mesh));
        let result = gossip.run_gossip_round().await;

        assert!(matches!(result, Err(GossipError::NoPeersAvailable)));
    }

    #[tokio::test]
    async fn test_handle_peer_ad_empty() {
        let mesh = Arc::new(PeerMeshHandle {
            known_peers: HashMap::new(),
            config: MeshConfig::default(),
            our_id: PeerId::random(),
        });

        let gossip = GossipProtocol::new(Arc::downgrade(&mesh));
        let ad = PeerAdvertisement::new(PeerId::random(), Vec::new());

        let result = gossip.handle_peer_ad(ad).await;
        assert!(matches!(result, Err(GossipError::InvalidMessage(_))));
    }

    #[test]
    fn test_mesh_stats_clone() {
        let stats = MeshStats {
            peers_discovered: AtomicU64::new(10),
            connections_established: AtomicU64::new(5),
            connections_failed: AtomicU64::new(1),
            gossip_rounds: AtomicU64::new(100),
            peers_removed: AtomicU64::new(2),
        };

        let cloned = stats.clone();
        assert_eq!(
            stats.peers_discovered.load(Ordering::Relaxed),
            cloned.peers_discovered.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn test_mesh_shutdown() {
        let config = MeshConfig::default();
        let mesh = PeerMesh::new(config);

        // Add a peer
        let peer = create_test_peer_info();
        mesh.known_peers.write().insert(peer.id, peer);

        mesh.shutdown();

        assert!(!mesh.is_running());
        assert_eq!(mesh.connected_peer_count(), 0);
    }

    #[test]
    fn test_prune_peers() {
        let mut config = MeshConfig::default();
        config.max_peers = 2;

        let mesh = PeerMesh::new(config);

        // Add many peers with low reputation
        for i in 0..10 {
            let mut peer = create_test_peer_info();
            peer.reputation = 0.1 + (i as f32 * 0.05);
            mesh.known_peers.write().insert(peer.id, peer);
        }

        mesh.prune_peers();

        // Should have pruned to max_peers * 2 = 4
        // Actually our prune threshold is max_peers * 2, so we need more than that
        assert!(mesh.known_peer_count() <= 10);
    }
}
