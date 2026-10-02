// Marabunta - Licensed under the MIT License.
//! Phantom Network Layer for Anonymous Communication.
//!
//! This module provides a complete anonymous communication system for Marabunta Compute,
//! enabling BYOD (Bring Your Own Device) participants to contribute compute resources
//! without revealing their identity or being tracked.
//!
//! # Architecture Overview
//!
//! ```text
//!                          ┌─────────────────────────────────────────────────────┐
//!                          │              Phantom Network Layer                   │
//!                          │                                                     │
//! ┌──────────┐            │  ┌────────────┐    ┌────────────┐    ┌──────────┐  │
//! │  Worker  │───────────▶│  │   Onion    │───▶│   Cover    │───▶│  Peer    │  │
//! │  Node    │            │  │   Router   │    │  Traffic   │    │  Mesh    │  │
//! └──────────┘            │  └────────────┘    └────────────┘    └──────────┘  │
//!       │                 │        │                 │                │        │
//!       │                 │        ▼                 ▼                ▼        │
//!       │                 │  ┌─────────────────────────────────────────────┐   │
//!       │                 │  │           Fingerprint SecurityDomain               │   │
//!       └─────────────────│  └─────────────────────────────────────────────┘   │
//!                          │                                                     │
//!                          └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Components
//!
//! ## Onion Routing (`onion`)
//!
//! Implements Tor-style onion routing where data passes through multiple relay
//! nodes, with each node only knowing the previous and next hop. This provides:
//!
//! - **Sender anonymity**: Destination doesn't know the origin
//! - **Recipient anonymity**: Origin doesn't know the final destination
//! - **Unlinkability**: Different sessions cannot be correlated
//!
//! ```rust,ignore
//! use phantom::network::onion::{OnionRouter, OnionConfig};
//!
//! let config = OnionConfig {
//!     min_hops: 3,
//!     max_hops: 5,
//!     ..Default::default()
//! };
//!
//! let router = OnionRouter::new(config, relay_registry);
//! let circuit_id = router.build_circuit().await?;
//! router.send(circuit_id, &data).await?;
//! ```
//!
//! ## Cover Traffic (`cover_traffic`)
//!
//! Generates indistinguishable cover traffic to prevent traffic analysis attacks.
//! All participants send traffic at similar rates, making it impossible to
//! identify when real work is being done.
//!
//! ```rust,ignore
//! use phantom::network::cover_traffic::{CoverTrafficGenerator, CoverConfig};
//!
//! let generator = CoverTrafficGenerator::new(CoverConfig::default());
//! generator.start(router);
//!
//! // Real data is mixed with cover traffic
//! generator.send_real_with_cover(&data, &router).await?;
//! ```
//!
//! ## Peer Mesh (`peer_mesh`)
//!
//! Decentralized peer discovery and connection management. No single point
//! knows all participants, providing resilience against network mapping attacks.
//!
//! ```rust,ignore
//! use phantom::network::peer_mesh::{PeerMesh, MeshConfig};
//!
//! let mesh = PeerMesh::new(MeshConfig::default());
//! mesh.bootstrap().await?;
//!
//! let relays = mesh.get_random_relays(3);
//! ```
//!
//! ## Fingerprint SecurityDomain (`fingerprint_security_domain`)
//!
//! Prevents device identification through hardware/software characteristics,
//! timing patterns, or behavioral analysis.
//!
//! ```rust,ignore
//! use phantom::network::fingerprint_security_domain::{FingerprintSecurityDomain, SecurityDomainConfig};
//!
//! let security_domain = FingerprintSecurityDomain::new(SecurityDomainConfig::default());
//!
//! // Report fake device characteristics
//! let chars = security_domain.reported_characteristics();
//!
//! // Add timing jitter to operations
//! security_domain.jittered_delay(Duration::from_millis(100)).await;
//! ```
//!
//! # Security Properties
//!
//! The Phantom Network Layer provides the following security guarantees:
//!
//! | Property | Mechanism | Protection Against |
//! |----------|-----------|-------------------|
//! | Anonymity | Onion routing | Origin/destination discovery |
//! | Unlinkability | Circuit rotation | Session correlation |
//! | Traffic analysis resistance | Cover traffic | Activity monitoring |
//! | Timing analysis resistance | Latency normalization | Timing correlation |
//! | Fingerprinting resistance | Fake profiles | Device identification |
//!
//! # Threat Model
//!
//! We protect against:
//!
//! - **Passive global adversary**: Can observe all network traffic
//! - **Malicious coordinators**: May try to identify participants
//! - **Compromised relays**: Some relays may be adversarial
//! - **Traffic analysis**: Statistical correlation of traffic patterns
//!
//! We do NOT protect against:
//!
//! - **Global active adversary**: Can modify all traffic
//! - **Majority relay compromise**: If >50% of relays are malicious
//! - **Timing attacks with unlimited observation**: Very long-term patterns
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use phantom::network::{
//!     onion::{OnionRouter, OnionConfig, RelayRegistry},
//!     cover_traffic::{CoverTrafficGenerator, CoverConfig},
//!     peer_mesh::{PeerMesh, MeshConfig},
//!     fingerprint_security_domain::{FingerprintSecurityDomain, SecurityDomainConfig},
//! };
//! use std::sync::Arc;
//!
//! // Initialize components
//! let relay_registry = Arc::new(RelayRegistry::new());
//! let router = Arc::new(OnionRouter::new(OnionConfig::default(), relay_registry));
//! let mesh = PeerMesh::new(MeshConfig::default());
//! let cover = CoverTrafficGenerator::new(CoverConfig::default());
//! let security_domain = FingerprintSecurityDomain::new(SecurityDomainConfig::default());
//!
//! // Bootstrap into network
//! mesh.bootstrap().await?;
//!
//! // Build anonymous circuit
//! let circuit_id = router.build_circuit().await?;
//!
//! // Start cover traffic
//! cover.set_circuit(circuit_id);
//! cover.start(Arc::clone(&router));
//!
//! // Send data anonymously
//! cover.send_real_with_cover(&task_result, &router).await?;
//!
//! // Periodically rotate identity
//! if security_domain.rotation_due() {
//!     security_domain.rotate_identity(&router).await?;
//! }
//! ```
//!
//! # Performance Considerations
//!
//! Anonymous communication inherently adds latency and bandwidth overhead:
//!
//! - **Latency**: ~50-100ms per hop (3-5 hops typical)
//! - **Bandwidth**: ~2x due to cover traffic
//! - **CPU**: Encryption at each hop
//!
//! For latency-sensitive applications, consider:
//!
//! - Using fewer hops (reduced anonymity)
//! - Longer circuit lifetimes
//! - Prebuilding circuits
//!
//! # Configuration
//!
//! Each component has sensible defaults suitable for most use cases.
//! See individual module documentation for configuration options.

pub mod cover_traffic;
pub mod errors;
pub mod fingerprint_security_domain;
pub mod onion;
pub mod peer_mesh;

// Re-export commonly used types
pub use cover_traffic::{
    CombinedStats, CoverConfig, CoverStats, CoverStrategy, CoverTrafficGenerator,
    CoverTrafficSystem, PendingCell, TokenBucket, TrafficShaper,
};
pub use errors::{
    CircuitId, CoverError, SecurityDomainError, GossipError, MeshError, OnionError, PeerId, Priority,
    RelayId, RemoveReason,
};
pub use fingerprint_security_domain::{
    SecurityDomainConfig, SecurityDomainConfigBuilder, DeviceCharacteristics, FakeProfile, FingerprintSecurityDomain,
    IntegratedSecurityDomain, LatencyNormalizer, OperationRandomizer,
};
pub use onion::{
    Circuit, CircuitInfo, CircuitState, OnionCell, OnionConfig, OnionRouter, OnionStats,
    RelayConfig, RelayFlags, RelayNode, RelayRegistry, RelayService, SymmetricKey,
    X25519PrivateKey, X25519PublicKey, CELL_SIZE, MAX_PAYLOAD_SIZE,
};
pub use peer_mesh::{
    ConnectionState, GossipProtocol, MeshConfig, MeshConfigBuilder, MeshStats, PeerAdvertisement,
    PeerCapabilities, PeerConnection, PeerInfo, PeerMesh,
};

use std::sync::Arc;
use std::time::Duration;

/// Integrated anonymous communication system.
///
/// Combines all phantom network components into a single, easy-to-use system.
pub struct PhantomNetwork {
    /// Onion router for anonymous circuits.
    pub router: Arc<OnionRouter>,

    /// Peer mesh for relay discovery.
    pub mesh: Arc<PeerMesh>,

    /// Cover traffic generator.
    pub cover: Arc<CoverTrafficGenerator>,

    /// Fingerprint security_domain.
    pub security_domain: Arc<FingerprintSecurityDomain>,

    /// Current circuit ID.
    circuit_id: parking_lot::RwLock<Option<CircuitId>>,
}

impl PhantomNetwork {
    /// Create a new phantom network with default configuration.
    pub fn new() -> Self {
        Self::with_config(
            OnionConfig::default(),
            MeshConfig::default(),
            CoverConfig::default(),
            SecurityDomainConfig::default(),
        )
    }

    /// Create a new phantom network with custom configuration.
    pub fn with_config(
        onion_config: OnionConfig,
        mesh_config: MeshConfig,
        cover_config: CoverConfig,
        security_domain_config: SecurityDomainConfig,
    ) -> Self {
        let relay_registry = Arc::new(RelayRegistry::new());
        let router = Arc::new(OnionRouter::new(onion_config, relay_registry));
        let mesh = Arc::new(PeerMesh::new(mesh_config));
        let cover = Arc::new(CoverTrafficGenerator::new(cover_config));
        let security_domain = Arc::new(FingerprintSecurityDomain::new(security_domain_config));

        PhantomNetwork {
            router,
            mesh,
            cover,
            security_domain,
            circuit_id: parking_lot::RwLock::new(None),
        }
    }

    /// Bootstrap the network and build initial circuit.
    pub async fn start(&self) -> Result<(), MeshError> {
        // Bootstrap peer mesh
        self.mesh.bootstrap().await?;

        // Build initial circuit
        let circuit_id = self
            .router
            .build_circuit()
            .await
            .map_err(|e| MeshError::Internal(e.to_string()))?;

        *self.circuit_id.write() = Some(circuit_id);

        // Start cover traffic
        self.cover.set_circuit(circuit_id);
        self.cover.start(Arc::clone(&self.router));

        Ok(())
    }

    /// Send data anonymously.
    pub async fn send(&self, data: &[u8]) -> Result<(), OnionError> {
        let circuit_id = self
            .circuit_id
            .read()
            .ok_or_else(|| OnionError::Internal("No circuit available".to_string()))?;

        // Apply timing jitter
        self.security_domain.jittered_delay(Duration::from_millis(10)).await;

        self.router.send(circuit_id, data).await
    }

    /// Receive data from the network.
    pub async fn receive(&self) -> Result<Vec<u8>, OnionError> {
        let circuit_id = self
            .circuit_id
            .read()
            .ok_or_else(|| OnionError::Internal("No circuit available".to_string()))?;

        self.router.receive(circuit_id).await
    }

    /// Rotate identity for enhanced privacy.
    pub async fn rotate_identity(&self) -> Result<(), OnionError> {
        // Destroy old circuit
        if let Some(old_id) = *self.circuit_id.read() {
            let _ = self.router.destroy_circuit(old_id).await;
        }

        // Build new circuit
        let new_id = self.router.build_circuit().await?;
        *self.circuit_id.write() = Some(new_id);

        // Update cover traffic
        self.cover.set_circuit(new_id);

        // Generate new fake profile
        self.security_domain.generate_fake_profile();

        Ok(())
    }

    /// Get current circuit ID.
    pub fn current_circuit(&self) -> Option<CircuitId> {
        *self.circuit_id.read()
    }

    /// Get device characteristics (fake if security_domain is enabled).
    pub fn characteristics(&self) -> DeviceCharacteristics {
        self.security_domain.reported_characteristics()
    }

    /// Check if identity rotation is due.
    pub fn rotation_due(&self) -> bool {
        self.security_domain.rotation_due()
    }

    /// Shutdown the network.
    pub async fn shutdown(&self) {
        self.cover.stop();
        self.mesh.shutdown();

        if let Some(circuit_id) = *self.circuit_id.read() {
            let _ = self.router.destroy_circuit(circuit_id).await;
        }
    }

    /// Get network statistics.
    pub fn stats(&self) -> NetworkStats {
        NetworkStats {
            onion: OnionStatsSnapshot {
                circuits_built: self
                    .router
                    .stats()
                    .circuits_built
                    .load(std::sync::atomic::Ordering::Relaxed),
                circuits_failed: self
                    .router
                    .stats()
                    .circuits_failed
                    .load(std::sync::atomic::Ordering::Relaxed),
                cells_sent: self
                    .router
                    .stats()
                    .cells_sent
                    .load(std::sync::atomic::Ordering::Relaxed),
                cells_received: self
                    .router
                    .stats()
                    .cells_received
                    .load(std::sync::atomic::Ordering::Relaxed),
            },
            mesh: MeshStatsSnapshot {
                peers_discovered: self
                    .mesh
                    .stats()
                    .peers_discovered
                    .load(std::sync::atomic::Ordering::Relaxed),
                connections_established: self
                    .mesh
                    .stats()
                    .connections_established
                    .load(std::sync::atomic::Ordering::Relaxed),
                gossip_rounds: self
                    .mesh
                    .stats()
                    .gossip_rounds
                    .load(std::sync::atomic::Ordering::Relaxed),
            },
            cover: CoverStatsSnapshot {
                cover_cells_sent: self
                    .cover
                    .stats()
                    .cover_cells_sent
                    .load(std::sync::atomic::Ordering::Relaxed),
                real_cells_sent: self
                    .cover
                    .stats()
                    .real_cells_sent
                    .load(std::sync::atomic::Ordering::Relaxed),
            },
            security_domain: SecurityDomainStatsSnapshot {
                jitters_applied: self
                    .security_domain
                    .stats()
                    .jitters_applied
                    .load(std::sync::atomic::Ordering::Relaxed),
                identity_rotations: self
                    .security_domain
                    .stats()
                    .identity_rotations
                    .load(std::sync::atomic::Ordering::Relaxed),
            },
        }
    }
}

impl Default for PhantomNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of network statistics.
#[derive(Debug, Clone)]
pub struct NetworkStats {
    /// Onion routing statistics.
    pub onion: OnionStatsSnapshot,
    /// Mesh statistics.
    pub mesh: MeshStatsSnapshot,
    /// Cover traffic statistics.
    pub cover: CoverStatsSnapshot,
    /// SecurityDomain statistics.
    pub security_domain: SecurityDomainStatsSnapshot,
}

/// Snapshot of onion routing statistics.
#[derive(Debug, Clone)]
pub struct OnionStatsSnapshot {
    /// Circuits built.
    pub circuits_built: u64,
    /// Circuits that failed to build.
    pub circuits_failed: u64,
    /// Cells sent.
    pub cells_sent: u64,
    /// Cells received.
    pub cells_received: u64,
}

/// Snapshot of mesh statistics.
#[derive(Debug, Clone)]
pub struct MeshStatsSnapshot {
    /// Peers discovered.
    pub peers_discovered: u64,
    /// Connections established.
    pub connections_established: u64,
    /// Gossip rounds completed.
    pub gossip_rounds: u64,
}

/// Snapshot of cover traffic statistics.
#[derive(Debug, Clone)]
pub struct CoverStatsSnapshot {
    /// Cover cells sent.
    pub cover_cells_sent: u64,
    /// Real cells sent.
    pub real_cells_sent: u64,
}

/// Snapshot of security_domain statistics.
#[derive(Debug, Clone)]
pub struct SecurityDomainStatsSnapshot {
    /// Timing jitters applied.
    pub jitters_applied: u64,
    /// Identity rotations performed.
    pub identity_rotations: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phantom_network_creation() {
        let network = PhantomNetwork::new();
        assert!(network.current_circuit().is_none());
    }

    #[test]
    fn test_phantom_network_with_config() {
        let onion_config = OnionConfig {
            min_hops: 4,
            max_hops: 6,
            ..Default::default()
        };

        let network = PhantomNetwork::with_config(
            onion_config,
            MeshConfig::default(),
            CoverConfig::default(),
            SecurityDomainConfig::default(),
        );

        assert_eq!(network.router.config().min_hops, 4);
    }

    #[test]
    fn test_characteristics() {
        let network = PhantomNetwork::new();
        let chars = network.characteristics();

        assert!(chars.cpu_cores > 0);
        assert!(chars.ram_gb > 0);
        assert!(!chars.os.is_empty());
    }

    #[test]
    fn test_rotation_due() {
        let config = SecurityDomainConfig {
            rotate_identity_interval: Duration::from_millis(1),
            ..Default::default()
        };

        let network = PhantomNetwork::with_config(
            OnionConfig::default(),
            MeshConfig::default(),
            CoverConfig::default(),
            config,
        );

        std::thread::sleep(Duration::from_millis(10));
        assert!(network.rotation_due());
    }

    #[test]
    fn test_network_stats() {
        let network = PhantomNetwork::new();
        let stats = network.stats();

        assert_eq!(stats.onion.circuits_built, 0);
        assert_eq!(stats.mesh.peers_discovered, 0);
        assert_eq!(stats.cover.cover_cells_sent, 0);
    }

    #[test]
    fn test_default_impl() {
        let network: PhantomNetwork = Default::default();
        assert!(network.current_circuit().is_none());
    }

    #[test]
    fn test_cell_size_constant() {
        assert_eq!(CELL_SIZE, 512);
        assert!(MAX_PAYLOAD_SIZE < CELL_SIZE);
    }
}
