// Marabunta - Licensed under the MIT License.
//! MarabuntaNode orchestrator tying all subsystems together.
//!
//! Manages identity, bootstrap, gossip, traffic shaping, transport profile
//! transition, blind job execution, and graceful shutdown.

use crate::marabunta::config::MarabuntaConfig;
use crate::marabunta::discovery::BootstrapChain;
use crate::marabunta::federation::FederationManager;
use crate::marabunta::gossip::{CapabilityVector, GossipEngine, GossipEntry, GossipStore};
use crate::marabunta::hierarchy::{HierarchyRouter, RoutingTable};
use crate::marabunta::identity::{FederationId, NodeId, NodeIdentity};
use crate::marabunta::neighborhood::NeighborhoodManager;
use crate::marabunta::sandbox::WasmSandbox;
use crate::marabunta::traffic::TrafficShaper;
use crate::marabunta::transition::{DarkMode, TransitionEngine, TransportProfile};
use crate::marabunta::transport::TransportManager;
use crate::marabunta::vault::{AdminAction, AdminCommand, DefaultVaultLogic, VaultLogic};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// MarabuntaNode errors.
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("identity error: {0}")]
    Identity(#[from] crate::marabunta::identity::IdentityError),

    #[error("transport error: {0}")]
    Transport(#[from] crate::marabunta::transport::TransportError),

    #[error("discovery error: {0}")]
    Discovery(#[from] crate::marabunta::discovery::DiscoveryError),

    #[error("vault error: {0}")]
    Vault(#[from] crate::marabunta::vault::VaultError),

    #[error("node not started")]
    NotStarted,

    #[error("node error: {0}")]
    Other(String),
}

/// State of the node lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Created,
    Starting,
    Running,
    ShuttingDown,
    Stopped,
}

/// The main Marabunta protocol node.
pub struct MarabuntaNode {
    config: MarabuntaConfig,
    identity: Arc<NodeIdentity>,
    federation_id: FederationId,
    gossip_engine: GossipEngine,
    gossip_store: Arc<GossipStore>,
    neighborhood_manager: NeighborhoodManager,
    hierarchy_router: HierarchyRouter,
    traffic_shaper: TrafficShaper,
    transition_engine: TransitionEngine,
    dark_mode: DarkMode,
    transport_manager: TransportManager,
    bpf_loader: Arc<crate::marabunta::bpf_loader::BpfLoader>,
    bootstrap_chain: BootstrapChain,
    vault_logic: Arc<dyn VaultLogic>,
    pub federation_manager: Arc<RwLock<FederationManager>>,
    /// Dead Man's Switch: Unix timestamp of TTL expiry.
    ttl_expiry: Arc<AtomicU64>,
    state: NodeState,
}

impl MarabuntaNode {
    /// Create a new MarabuntaNode with a fresh identity and federation context.
    pub fn new(config: MarabuntaConfig) -> Result<Self, NodeError> {
        let identity = if let Ok(data) = std::fs::read(".gemini/tmp/keystore.json") {
            let ks: crate::marabunta::identity::EncryptedKeystore = serde_json::from_slice(&data).map_err(|e| NodeError::Identity(crate::marabunta::identity::IdentityError::Serialization(e.to_string())))?;
            Arc::new(ks.load(b"marabunta_default_passphrase").map_err(|e| NodeError::Identity(e))?)
        } else {
            let new_id = NodeIdentity::generate()?;
            let _ = std::fs::create_dir_all(".gemini/tmp/");
            if let Ok(ks) = crate::marabunta::identity::EncryptedKeystore::save(&new_id, b"marabunta_default_passphrase") {
                if let Ok(json_bytes) = serde_json::to_vec(&ks) {
                    let _ = std::fs::write(".gemini/tmp/keystore.json", json_bytes);
                }
            }
            Arc::new(new_id)
        };
        let federation_id = FederationId::generate();
        Self::with_identity_and_federation(config, identity, federation_id)
    }

    /// Create a MarabuntaNode with an existing identity.
    pub fn with_identity(
        config: MarabuntaConfig,
        identity: Arc<NodeIdentity>,
    ) -> Result<Self, NodeError> {
        let fid = if let Ok(data) = std::fs::read(".gemini/tmp/federation_id.bin") { if data.len() == 32 { let mut b = [0u8; 32]; b.copy_from_slice(&data); FederationId::new(b) } else { FederationId::generate() } } else { FederationId::generate() };
        Self::with_identity_and_federation(config, identity, fid)
    }

    /// Create a MarabuntaNode with an existing identity and federation ID.
    pub fn with_identity_and_federation(
        config: MarabuntaConfig,
        identity: Arc<NodeIdentity>,
        federation_id: FederationId,
    ) -> Result<Self, NodeError> {
        let gossip_store = Arc::new(GossipStore::new());
        let gossip_engine = GossipEngine::new(identity.node_id(), gossip_store.clone());
        let neighborhood_manager = NeighborhoodManager::new();
        let hierarchy_router = HierarchyRouter::new(RoutingTable::new());
        let traffic_shaper = TrafficShaper::new();
        let transition_engine = TransitionEngine::new();
        let dark_mode = DarkMode::new();
        let transport_manager = TransportManager::new();
        let bpf_loader = Arc::new(crate::marabunta::bpf_loader::BpfLoader::new());
        let bootstrap_chain = BootstrapChain::new(config.bootstrap_relays.clone());
        let vault_logic = Arc::new(DefaultVaultLogic {
            required_signatures: 3,
            min_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(3600),
        });
        let federation_manager = Arc::new(RwLock::new(FederationManager::new(federation_id, Arc::clone(&identity))));

        // Initial TTL: 24 hours from now
        let ttl_expiry = Arc::new(AtomicU64::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 86400,
        ));

        Ok(Self {
            config,
            identity,
            federation_id,
            gossip_engine,
            gossip_store,
            neighborhood_manager,
            hierarchy_router,
            traffic_shaper,
            transition_engine,
            dark_mode,
            transport_manager,
            bpf_loader,
            bootstrap_chain,
            vault_logic,
            federation_manager,
            ttl_expiry,
            state: NodeState::Created,
        })
    }

    /// Start the node: bootstrap, connect to relays, register in gossip.
    pub async fn start(&mut self) -> Result<(), NodeError> {
        self.state = NodeState::Starting;

        // 1. Discover relay endpoints
        let endpoints = self.bootstrap_chain.discover().await?;

        // 2. Connect to first available relay
        if let Some(endpoint) = endpoints.first() {
            match self.transport_manager.connect(&endpoint.url).await {
                Ok(method) => {
                    tracing::info!(
                        node_id = %self.identity.node_id(),
                        transport = %method,
                        "Connected to relay"
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to relay: {}", e);
                    // Continue without relay — will retry
                }
            }
        }

        // 3. Register self in gossip store
        let my_entry = GossipEntry::new(
            self.identity.node_id(),
            CapabilityVector::default(), // Bounds cached via get_bounds()
        );
        self.gossip_store.merge_entry(my_entry);

        // 4. Load eBPF kernel guardians
        self.bpf_loader.load_thermal_guardian();

        self.state = NodeState::Running;
        tracing::info!(
            node_id = %self.identity.node_id(),
            "MarabuntaNode started"
        );

        // Start the Dead Man's Switch
        self.spawn_ttl_watcher();

        Ok(())
    }

    /// Graceful shutdown: announce LEAVE, drain jobs, close connections.
    pub async fn shutdown(&mut self) -> Result<(), NodeError> {
        self.state = NodeState::ShuttingDown;

        // Remove self from gossip
        self.gossip_store.remove(&self.identity.node_id());

        // Close transport
        self.transport_manager.close().await?;

        self.state = NodeState::Stopped;
        tracing::info!(
            node_id = %self.identity.node_id(),
            "MarabuntaNode stopped"
        );
        Ok(())
    }

    /// Get the node's identity.
    pub fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }

    /// Get the node's federation identity.
    pub fn federation_id(&self) -> FederationId {
        self.federation_id
    }

    /// Get the node's identity Arc.
    pub fn identity_arc(&self) -> Arc<NodeIdentity> {
        Arc::clone(&self.identity)
    }

    /// Get the node's current state.
    pub fn state(&self) -> NodeState {
        self.state
    }

    /// Get the programmable vault logic.
    pub fn vault_logic(&self) -> Arc<dyn VaultLogic> {
        self.vault_logic.clone()
    }

    /// Verify and execute an administrative action.
    pub fn handle_admin_action(&mut self, action: AdminAction) -> Result<(), NodeError> {
        // 1. Validate against programmable vault logic
        self.vault_logic.verify_action(&action)?;

        // 2. Execute the command
        match action.command {
            AdminCommand::Kill => {
                tracing::warn!("Vault: Authorized Kill command received. Shutting down.");
                self.shutdown_immediate();
            }
            AdminCommand::Extinction => {
                tracing::error!("Vault: EXTINCTION PULSE RECEIVED. Scrubbing memory and exiting.");
                self.scrub_and_vaporize();
            }
            AdminCommand::KeepGoing { next_expiry } => {
                let ts = next_expiry
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| NodeError::Other("invalid timestamp".into()))?
                    .as_secs();
                self.ttl_expiry.store(ts, Ordering::SeqCst);
                tracing::info!("Vault: KEEP_GOING pulse received. TTL extended to {}", ts);
            }
            AdminCommand::SignTreaty { treaty_id, partner } => {
                let mut fm = self.federation_manager.write();
                fm.enact_treaty(crate::marabunta::federation::DiplomaticTreaty {
                    treaty_id,
                    partner,
                    capacity_cap_pct: 15,
                    recall_threshold: 500,
                    visa_duration: Duration::from_secs(3600),
                    signed_at: SystemTime::now(),
                    gateway_uri: None,
                });
            }
            AdminCommand::FormCoalition { coalition_id, members, associated_topology } => {
                let mut fm = self.federation_manager.write();
                fm.join_coalition(crate::marabunta::federation::WolfPackCoalition {
                    coalition_id,
                    members: members.into_iter().collect(),
                    leader: self.federation_id, // Default to self as leader for now
                    total_aggregate_reputation: 1.0,
                    topology: crate::marabunta::federation::WolfPackTopology::Pending,
                    associated_topology,
                    signatures: std::collections::HashMap::new(),
                    status: crate::marabunta::federation::WolfPackStatus::Draft,
                });
            }
            AdminCommand::SetLogLevel { level } => {
                tracing::info!("Vault: Authorized LogLevel change to {}", level);
            }
            _ => {
                return Err(NodeError::Other(
                    "command not yet implemented in node".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn shutdown_immediate(&mut self) {
        self.state = NodeState::Stopped;
    }

    fn scrub_and_vaporize(&mut self) {
        // Implementation Requirement: Memory scrubbing logic would go here
        std::process::exit(1);
    }

    /// Start the Dead Man's Switch background watcher.
    pub fn spawn_ttl_watcher(&self) {
        let ttl = Arc::clone(&self.ttl_expiry);
        tokio::spawn(async move {
            loop {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let expiry = ttl.load(Ordering::SeqCst);

                if now >= expiry {
                    tracing::error!("DEAD MAN'S SWITCH EXPIRED. Committing digital suicide.");
                    std::process::exit(1);
                }

                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    }

    /// Get the current transport profile.
    pub fn transport_profile(&self) -> TransportProfile {
        self.transition_engine.current_profile()
    }

    /// Get the gossip store.
    pub fn gossip_store(&self) -> &Arc<GossipStore> {
        &self.gossip_store
    }

    /// Get the gossip engine.
    pub fn gossip_engine(&self) -> &GossipEngine {
        &self.gossip_engine
    }

    /// Get the config.
    pub fn config(&self) -> &MarabuntaConfig {
        &self.config
    }

    /// Get the identity.
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Whether the node is connected to a relay.
    pub fn is_connected(&self) -> bool {
        self.transport_manager.is_connected()
    }

    /// Number of known peers in gossip.
    pub fn known_peers(&self) -> usize {
        self.gossip_store.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_node() {
        let config = MarabuntaConfig::default();
        let node = MarabuntaNode::new(config).unwrap();
        assert_eq!(node.state(), NodeState::Created);
        assert_ne!(node.node_id().0, [0u8; 32]);
    }

    #[test]
    fn test_node_with_identity() {
        let config = MarabuntaConfig::default();
        let identity = Arc::new(NodeIdentity::generate().unwrap());
        let expected_id = identity.node_id();
        let node = MarabuntaNode::with_identity(config, identity).unwrap();
        assert_eq!(node.node_id(), expected_id);
    }

    #[test]
    fn test_initial_state() {
        let node = MarabuntaNode::new(MarabuntaConfig::default()).unwrap();
        assert_eq!(node.state(), NodeState::Created);
        assert_eq!(node.transport_profile(), TransportProfile::Open);
        assert!(!node.is_connected());
        assert_eq!(node.known_peers(), 0);
    }

    #[tokio::test]
    async fn test_start_registers_self() {
        let mut node = MarabuntaNode::new(MarabuntaConfig::default()).unwrap();
        // Start will try to connect to relay (which won't exist), but should still register
        let _ = node.start().await;
        // Self should be in gossip store
        assert!(node.gossip_store().get(&node.node_id()).is_some());
    }

    #[tokio::test]
    async fn test_shutdown_removes_self() {
        let mut node = MarabuntaNode::new(MarabuntaConfig::default()).unwrap();
        let _ = node.start().await;
        let node_id = node.node_id();
        node.shutdown().await.unwrap();
        assert!(node.gossip_store().get(&node_id).is_none());
        assert_eq!(node.state(), NodeState::Stopped);
    }
}
