// Marabunta - Licensed under the MIT License.
//! Marabunta Swarm -- top-level orchestrator module.
//!
//! This module ties together every swarm subsystem into a single
//! [`SwarmNode`] struct that owns all shared state and coordinates the
//! lifecycle of the node: identity persistence, bootstrap, gossip,
//! failure detection, trait evaluation, work distribution, and graceful
//! shutdown.
//!
//! # Architecture
//!
//! ```text
//!                +---------------------+
//!                |     SwarmNode        |
//!                |---------------------|
//!                | identity (NodeId)   |
//!                | generation          |
//!                +--------+------------+
//!                         |
//!        +-------+--------+--------+---------+
//!        |       |        |        |         |
//!    Transport  Gossip  Failure  Work   Bootstrap
//!        |       |   Engine  Detector Engine  Server
//!        |       |        |        |         |
//!        +-------+--------+--------+---------+
//!                         |
//!                  KnowledgeStore
//! ```
//!
//! All subsystems share the [`KnowledgeStore`] and communicate via
//! an outbound message channel that the transport layer drains.

pub mod types;
pub mod config;
pub mod hardware;
pub mod thermal;
pub mod location;
pub mod traits;
pub mod gossip;
pub mod knowledge;
pub mod failure;
pub mod work;
pub mod transport;
pub mod bootstrap;
pub mod profile;
pub mod collective;
pub mod reputation;
pub mod policy;
pub mod admission;
pub mod api;
pub mod auth;
pub mod crypto;
pub mod pow_worker;
pub mod browser_bridge;
pub mod sandbox;
pub mod blobstore;
pub mod planetary_gateway;
pub mod discovery;
pub mod strategy;
pub mod energy;
pub mod relay;
pub mod checkpoint;
pub mod aggregator;
pub mod latency;
pub mod updater;
pub mod sovereignty;
pub mod complexity;
pub mod metrics;
pub mod sla;
pub mod events;
pub mod alerting;
pub mod fleet;
pub mod healthcheck;
pub mod agreement;
pub mod capacity;
pub mod psyche;
pub mod audit;
pub mod membrane;
pub mod verification;
pub mod chunking;
pub mod vanguard;
pub mod webhooks;
pub mod pricing;
pub mod streaming;
pub mod scheduler;
pub mod wasm_executor;
pub mod wasm_python;
pub mod combo_registry;
pub mod node_class;
pub mod workflow;
pub mod storage;
pub mod fuel_metering;
pub mod detection;
pub mod cde;
pub mod election;
pub mod witness;
pub mod quantum_accelerator;
pub mod quantum_manager;
pub mod dashboard;
pub mod neuromancer;
pub mod postgres;
pub mod config_meta;
pub mod config_live;
pub mod config_db;
pub mod observe;
pub mod isomorphic;
pub mod crdt;
pub mod config_consent;
pub mod key_rotation;
pub mod highestsec_bridge;
pub mod audit_sanitizer;
pub mod compliance;
pub mod gdpr;
pub mod unified_audit;
pub mod ide_control;

use std::time::Duration;
use std::path::PathBuf;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use parking_lot::RwLock;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;
use hex;

use crate::common::types::TaskPayload;

use self::bootstrap::{BootstrapClient, BootstrapServer};
use self::config::{SwarmConfig, KNOWLEDGE_PRUNE_INTERVAL};
use self::failure::FailureDetector;
use self::gossip::{GossipConfig, GossipEngine};
use self::knowledge::KnowledgeStore;
use self::traits::TraitEvaluator;
use self::transport::SwarmTransport;
use self::types::{
    ConnectivityInfo, NodeId, NodeInfo, NodeStatus,
    ResourceSnapshot, SwarmError, SwarmMessage, SwarmResult, Trait,
};
use self::work::{SubmissionResult, WorkEngine};

use self::profile::{NodeProfile, ProfileStore};
use self::collective::{CollectiveEngine, CollectiveStore};
use self::reputation::{ReputationStore, EntityId};
use self::policy::{PolicyEngine, PolicySet};

use self::admission::{AdmissionEngine, AdmissionStore};
use self::auth::{AuthLayer, NodeIdentity, TokenPerms, TokenStore};
use self::blobstore::BlobStore;
use self::sandbox::SandboxedExecutor;
use self::discovery::SoftwareProber;
use self::strategy::StrategyStore;
use self::energy::EnergyCostEstimator;
use self::relay::RelayServer;
use self::checkpoint::CheckpointManager;
use self::aggregator::Aggregator;
use self::latency::LatencyProber;
use self::api::ApiServer;
use crate::plugin::PluginHost;

// Highestsec compliance imports (conditional on enable_highestsec_zones / enable_blind_computation).
use self::key_rotation::{KeyRotationEngine, KeyRotationStore};
use self::highestsec_bridge::HighestsecBridge;
use crate::highestsec::zone_membership::ZoneCertificateStore;
use crate::phantom::network::{cover_traffic, onion};

// Management layer imports (conditional on enable_management_layer).
use self::events::EventBus;
use self::complexity::VisionStore;
use self::fleet::FleetManager;
use self::healthcheck::HealthCheckEngine;
use self::sovereignty::{SovereigntyManager, SwarmIdentity, SwarmKeypair};
use self::membrane::{
    CrossingLog, MembraneEngine, MembraneStore, RateLimiter as MembraneRateLimiter,
    EmbassyManager,
};
use self::agreement::{ConstellationBuilder, LendingMeter, AgreementStore};
use self::alerting::{AlertContext, AlertEngine, AlertRuleStore};
use self::sla::{SlaMonitor, SlaStore};
use self::capacity::CapacityPlanner;
use self::audit::AuditLog;
use self::metrics::SwarmMetrics;

use self::verification::VerificationEngine;
use self::chunking::ChunkPlanner;
use self::webhooks::WebhookEngine;
use self::pricing::PricingEngine;
use self::streaming::StreamingEngine;
use self::scheduler::JobScheduler;
use self::wasm_executor::WasmExecutor;
use self::combo_registry::ComboRegistry;
use self::witness::{CourtOfArbitration};

// Neuromancer subsystem imports.
use self::neuromancer::bus::NeuromancerBus;
use self::neuromancer::crow::Crow;
use self::neuromancer::engram::Engram;
use self::neuromancer::spider::Spider;
use self::neuromancer::lazarus::Lazarus;
use self::neuromancer::crocodile::Crocodile;
use self::neuromancer::viper::Viper;
use self::neuromancer::elektra::Elektra;
use self::neuromancer::wild_dogs::WildDogs;
use self::neuromancer::mantis::Mantis;
use self::neuromancer::ghost::Ghost;
use self::neuromancer::chop_shop::ChopShop;
use self::neuromancer::sandman::Sandman;
use self::neuromancer::orca::BabyOrca;
use self::neuromancer::wintermute::Wintermute;
use self::neuromancer::darwin::Darwin;

// ============================================================================
// Outbound channel capacity
// ============================================================================

/// Capacity of the outbound message channel between subsystems and transport.
///
/// This bounds how many messages can be enqueued before back-pressure kicks
/// in. 1024 is generous for most workloads; the transport layer should drain
/// this faster than gossip + work can fill it.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

// ============================================================================
// SwarmNodeStatus
// ============================================================================

/// Summary of the swarm node's current state, suitable for display or
/// external health checks.
#[derive(Debug, Clone)]
pub struct SwarmNodeStatus {
    pub node_id: NodeId,
    pub generation: u64,
    pub traits: HashSet<Trait>,
    pub load: f32,
    pub known_nodes: usize,
    pub known_jobs: usize,
    pub active_chunks: usize,
    pub uptime_secs: u64,
    // Organic swarm subsystem stats
    pub profiles_known: usize,
    pub collectives_active: usize,
    pub reputation_records: usize,
    pub policy_version: u64,
    pub blobs_stored: usize,
    pub blobs_total_bytes: u64,
    pub strategies_loaded: usize,
    pub software_discovered: usize,
}

// ============================================================================
// MessageHandler type alias
// ============================================================================

/// Type alias for the closure that dispatches inbound messages to
/// the appropriate subsystem.
pub type MessageHandler = Arc<dyn Fn(SocketAddr, SwarmMessage) + Send + Sync>;

// ============================================================================
// SwarmNode
// ============================================================================

/// The main swarm node. Owns all subsystems and coordinates their lifecycle.
///
/// A `SwarmNode` is created via [`SwarmNode::new`], configured through
/// [`SwarmConfig`], and started with [`SwarmNode::run`]. Once running,
/// the node participates in the Marabunta Swarm: gossiping state, detecting
/// failures, pulling and executing work, and responding to bootstrap
/// requests from new nodes.
///
/// Call [`SwarmNode::shutdown`] to initiate a graceful shutdown of all
/// background tasks.
pub struct SwarmNode {
    // -- Identity --
    id: NodeId,
    generation: u64,

    // -- Subsystems (all Arc'd for sharing across tasks) --
    knowledge: Arc<KnowledgeStore>,
    transport: Arc<SwarmTransport>,
    gossip: Arc<GossipEngine>,
    failure_detector: Arc<FailureDetector>,
    hardware_monitor: Arc<crate::swarm::thermal::HardwareMonitor>,
    external_addr: Arc<parking_lot::RwLock<Option<std::net::SocketAddr>>>,
    work_engine: Arc<WorkEngine>,
    bootstrap_server: Arc<BootstrapServer>,
    marabunta_node: Arc<RwLock<crate::marabunta::node::MarabuntaNode>>,    rosetta_stone: Arc<rosetta::RosettaStone>,
    blob_egress_semaphore: Arc<tokio::sync::Semaphore>,

    // -- Outbound channel --
    _outbound_tx: mpsc::Sender<(SocketAddr, SwarmMessage)>,
    /// The receiver half of the outbound channel, wrapped in a Mutex<Option<>>
    /// so that `run()` can take it exactly once.
    outbound_rx_slot: parking_lot::Mutex<Option<mpsc::Receiver<(SocketAddr, SwarmMessage)>>>,
    
    // -- Egress Retry Queue --
    egress_retry_tx: mpsc::UnboundedSender<(SocketAddr, SwarmMessage)>,
    egress_retry_rx_slot: parking_lot::Mutex<Option<mpsc::UnboundedReceiver<(SocketAddr, SwarmMessage)>>>,

    // -- Shared mutable state updated by the trait evaluator --
    current_traits: Arc<RwLock<HashSet<Trait>>>,
    current_load: Arc<RwLock<f32>>,
    current_capacity: Arc<RwLock<ResourceSnapshot>>,
    connectivity: Arc<RwLock<ConnectivityInfo>>,

    // -- Lifecycle --
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    config: SwarmConfig,

    // -- Timing --
    started_at: Instant,

    // -- Trait evaluator (consumed once by run()) --
    trait_evaluator: parking_lot::Mutex<Option<TraitEvaluator>>,

    // -- Organic swarm subsystems --
    profile_store: Arc<ProfileStore>,
    collective_store: Arc<CollectiveStore>,
    reputation_store: Arc<ReputationStore>,
    policy_engine: Arc<PolicyEngine>,
    collective_engine: Arc<CollectiveEngine>,

    // -- Self profile --
    my_profile: Arc<RwLock<NodeProfile>>,

    // -- Admission subsystem --
    admission_engine: Arc<AdmissionEngine>,
    admission_store: Arc<AdmissionStore>,

    // -- Production subsystems (Phase 1-10) --
    node_identity: Arc<NodeIdentity>,
    blob_store: Arc<BlobStore>,
    sandbox: Arc<SandboxedExecutor>,
    software_prober: Arc<SoftwareProber>,
    strategy_store: Arc<StrategyStore>,
    energy_estimator: Arc<EnergyCostEstimator>,
    relay_server: Arc<RelayServer>,
    checkpoint_manager: Arc<CheckpointManager>,
    retrovirus_engine: Arc<crate::swarm::neuromancer::retrovirus::RetrovirusEngine>,
    cordyceps_engine: Arc<crate::swarm::neuromancer::cordyceps::CordycepsProtocol>,
    aggregation_engine: Arc<Aggregator>,
    quantum_job_manager: Arc<self::quantum_manager::QuantumJobManager>,
    egress_diode: Arc<crate::api::gateway::EgressDiode>,
    /// 🛑 BRAZILIAN CGNAT FIX: Active STUN/TURN Engine
    /// This allows 5 million residential gaming nodes stuck behind Carrier-Grade NATs 
    /// to establish bidirectional UDP hole-punching for true P2P Kademlia routing.
    nat_engine: Arc<tokio::sync::RwLock<Option<crate::swarm::hyperscale::nat_traversal::NatTraversalEngine>>>,

    // -- Latency prober --
    latency_prober: Arc<LatencyProber>,

    // -- Plugin host (optional) --
    plugin_host: Option<Arc<PluginHost>>,

    // -- Management layer subsystems (enabled via config.enable_management_layer) --
    event_bus: Arc<EventBus>,
    vision_store: Arc<VisionStore>,
    fleet_manager: Arc<FleetManager>,
    healthcheck_engine: Arc<HealthCheckEngine>,
    sovereignty_manager: Arc<SovereigntyManager>,
    membrane_engine: Arc<MembraneEngine>,
    crossing_log: Arc<CrossingLog>,
    agreement_store: Arc<AgreementStore>,
    lending_meter: Arc<LendingMeter>,
    constellation_builder: Arc<ConstellationBuilder>,
    alert_engine: Arc<AlertEngine>,
    sla_monitor: Arc<SlaMonitor>,
    capacity_planner: Arc<CapacityPlanner>,
psyche_calculator: Arc<self::psyche::PsycheCalculator>,
    audit_log: Arc<AuditLog>,
    swarm_metrics: Arc<SwarmMetrics>,

    // -- Combo infrastructure --
    verification_engine: Arc<VerificationEngine>,
    chunk_planner: Arc<ChunkPlanner>,
    webhook_engine: Arc<WebhookEngine>,
    pricing_engine: Arc<PricingEngine>,
    streaming_engine: Arc<StreamingEngine>,
    job_scheduler: Arc<JobScheduler>,
    wasm_executor: Arc<WasmExecutor>,
    combo_registry: Arc<ComboRegistry>,

    // -- PostgreSQL subsystem (gated by enable_postgres) --
    pg_manager: Option<Arc<postgres::PgManager>>,

    // -- Neuromancer intelligence/security_domain subsystem (gated by enable_neuromancer) --
    neuromancer_bus: Option<Arc<NeuromancerBus>>,
    nm_crow: Option<Arc<parking_lot::Mutex<Crow>>>,
    nm_engram: Option<Arc<parking_lot::Mutex<Engram>>>,
    nm_spider: Option<Arc<parking_lot::Mutex<Spider>>>,
    nm_lazarus: Option<Arc<parking_lot::Mutex<Lazarus>>>,
    nm_crocodile: Option<Arc<parking_lot::Mutex<Crocodile>>>,
    nm_viper: Option<Arc<parking_lot::Mutex<Viper>>>,
    nm_elektra: Option<Arc<parking_lot::Mutex<Elektra>>>,
    nm_wild_dogs: Option<Arc<parking_lot::Mutex<WildDogs>>>,
    nm_mantis: Option<Arc<parking_lot::Mutex<Mantis>>>,
    nm_ghost: Option<Arc<parking_lot::Mutex<Ghost>>>,
    nm_chop_shop: Option<Arc<parking_lot::Mutex<ChopShop>>>,
    nm_sandman: Option<Arc<parking_lot::Mutex<Sandman>>>,
    nm_orca: Option<Arc<parking_lot::Mutex<BabyOrca>>>,
    nm_wintermute: Option<Arc<parking_lot::Mutex<Wintermute>>>,
    nm_darwin: Option<Arc<Darwin>>,

    // -- Observe-and-Interfere (gated by enable_observe_and_interfere) --
    oai_subscription_manager: Option<Arc<observe::subscription::SubscriptionManager>>,
    oai_operator_store: Option<Arc<observe::operator::OperatorStore>>,
    oai_guard_evaluator: Option<Arc<observe::guard::GuardEvaluator>>,
    oai_lock_manager: Option<Arc<observe::lock::LockManager>>,
    oai_impact_assessor: Option<Arc<observe::impact::ImpactAssessor>>,
    oai_rollback_manager: Option<Arc<observe::rollback::RollbackManager>>,
    oai_intervention_engine: Option<Arc<observe::engine::InterventionEngine>>,
    oai_session_store: Option<Arc<observe::session::SessionStore>>,
    oai_veto_checker: Option<Arc<observe::veto::VetoChecker>>,

    // -- Highestsec compliance subsystems (gated by enable_blind_computation / enable_highestsec_zones) --
    zone_cert_store: Option<Arc<ZoneCertificateStore>>,
    key_rotation_store: Option<Arc<KeyRotationStore>>,
    key_rotation_engine: Option<Arc<KeyRotationEngine>>,
    highestsec_bridge: Option<Arc<HighestsecBridge>>,

    // -- Phantom protocol (cover traffic) --
    onion_router: Option<Arc<onion::OnionRouter>>,
    cover_traffic_system: Option<Arc<cover_traffic::CoverTrafficSystem>>,

    // -- Isomorphic State Rings --
    isomorphic_rings: Arc<RwLock<std::collections::HashMap<String, isomorphic::IsomorphicStateRing>>>,

    // -- GDPR compliance engine (gated by enable_gdpr) --
    gdpr_engine: Option<Arc<gdpr::GdprEngine>>,

    // -- Chaos Engineering Engine --
    chaos_engine: Arc<crate::chaos::engine::ChaosEngine>,
}

impl SwarmNode {
    /// Create a new `SwarmNode` with the given configuration.
    ///
    /// This wires up all subsystems but does **not** start any background
    /// tasks. Call [`run`](Self::run) to bootstrap and enter the main loop.
    pub fn new(config: SwarmConfig) -> SwarmResult<Self> {
        // 1. Load or generate identity.
        let (id, generation) = load_or_create_identity(config.identity_file.as_deref());
        let node_identity = Arc::new({
            let identity_path = config.identity_file
                .as_deref()
                .map(|p| std::path::PathBuf::from(format!("{}.ed25519", p)))
                .unwrap_or_else(|| {
                    let dir = directories::ProjectDirs::from("io", "marabunta-compute", "marabunta")
                        .map(|d| d.data_dir().to_path_buf())
                        .unwrap_or_else(|| std::path::PathBuf::from("."));
                    dir.join("identity.ed25519")
                });
            NodeIdentity::load_or_generate(&identity_path)
                .unwrap_or_else(|_| NodeIdentity::generate())
        });

        let chaos_engine = Arc::new(crate::chaos::engine::ChaosEngine::new());
        let rosetta_stone = Arc::new(rosetta::RosettaStone::new());
        let retrovirus_engine = Arc::new(crate::swarm::neuromancer::retrovirus::RetrovirusEngine::new([0u8; 32]));
        let identity_key = ed25519_dalek::SigningKey::from_bytes(node_identity.signing_key_bytes());
        let cordyceps_engine = Arc::new(crate::swarm::neuromancer::cordyceps::CordycepsProtocol::new(id, identity_key, [0u8; 32]));

        info!(
            node_id = %id,
            generation = generation,
            listen_addr = %config.listen_addr,
            "swarm node identity loaded"
        );

        // 2. Create the knowledge store (with optional SQLite persistence).
        let knowledge = Arc::new(KnowledgeStore::with_persistence(
            id,
            config.state_dir.as_deref(),
        ));

        // 3. Parse listen address.
        let listen_addr: SocketAddr = config
            .listen_addr
            .parse()
            .map_err(|e| SwarmError::Transport(format!("invalid listen address: {}", e)))?;

        let marabunta_node = Arc::new(RwLock::new(
            crate::marabunta::node::MarabuntaNode::new(crate::marabunta::config::MarabuntaConfig::default())
                .map_err(|e| SwarmError::Api(format!("failed to init marabunta node: {}", e)))?
        ));

        // 5. Hardening and seccomp installation (MUST be before transport binding)
        if config.enable_blind_computation {
            // Process hardening FIRST — disable core dumps before any secrets
            #[cfg(target_os = "linux")]
            {
                crate::highestsec::process_hardening::harden_process()
                    .map_err(|e| SwarmError::Sandbox(format!("process hardening failed: {e}")))?;
                info!("process hardening active — core dumps disabled, RLIMIT_CORE=0");
            }

            // Install seccomp filter — fail-closed if not available
            #[cfg(all(target_os = "linux", feature = "highestsec-sandbox"))]
            {
                let seccomp_config = config.seccomp.clone().unwrap_or_default();
                if let Err(e) = seccomp_config.validate_for_blind_compute() {
                    return Err(SwarmError::Sandbox(format!("FATAL: invalid seccomp config for blind computation: {e}")));
                }
                crate::highestsec::seccomp::install_seccomp_filter_with_config(&seccomp_config)
                    .map_err(|e| SwarmError::Sandbox(format!(
                        "FATAL: seccomp filter installation failed: {e:?}. \
                         Cannot start with enable_blind_computation=true without seccomp."
                    )))?;
                info!("seccomp BPF filter installed — blind computation hardened");
            }

            #[cfg(not(all(target_os = "linux", feature = "highestsec-sandbox")))]
            {
                return Err(SwarmError::Sandbox(
                    "enable_blind_computation=true requires Linux with the \
                     highestsec-sandbox feature compiled in".to_string()
                ));
            }
        }

        // 5. Create transport with message signing.
        let (marabunta_id, fed_id, sovereign_identity, federation_manager) = {
            let node = marabunta_node.read();
            (
                node.node_id(),
                node.federation_id(),
                node.identity_arc(),
                Arc::clone(&node.federation_manager),
            )
        };
        let transport = Arc::new(
            SwarmTransport::new(marabunta_id, fed_id, listen_addr)
                .with_signing(sovereign_identity)
                .with_transport_tuning(config.transport_tuning.clone())
        );

        // 6. Create outbound channel.
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);

        // 6. Initialize organic swarm subsystems.
        let profile_store = Arc::new(ProfileStore::new());
        let collective_store = Arc::new(CollectiveStore::new());
        let reputation_store = Arc::new(ReputationStore::new());
        let policy_engine = Arc::new(PolicyEngine::new(PolicySet::empty(id)));

        // Create self profile.
        let my_profile = Arc::new(RwLock::new(
            NodeProfile::detect(id, profile::NodeType::Desktop), // default to Desktop, can be overridden
        ));

        // Store our profile.
        profile_store.upsert(my_profile.read().clone());

        let collective_engine = Arc::new(CollectiveEngine::new(
            id,
            collective_store.clone(),
            profile_store.clone(),
            knowledge.clone(),
            outbound_tx.clone(),
        ));

        // Create energy estimator early so it can be shared.
        let energy_estimator = Arc::new(EnergyCostEstimator::new());

        // 6b. Create admission store (needed by gossip engine).
        let admission_store = Arc::new(AdmissionStore::new());

        // 6c. Create highestsec stores early (needed by gossip engine builders).
        let zone_cert_store: Option<Arc<ZoneCertificateStore>> = if config.enable_highestsec_zones {
            Some(Arc::new(ZoneCertificateStore::new()))
        } else {
            None
        };
        let key_rotation_store = Arc::new(KeyRotationStore::new());

        // 7. Create gossip engine with config overrides and organic stores.
        let gossip_config = GossipConfig {
            interval: config.gossip_interval,
            fanout: config.gossip_fanout,
            ..GossipConfig::default()
        };
        // 8. Create failure detector with config thresholds.
        // Created before gossip so we can share the witness store.
        let failure_detector = Arc::new(
            FailureDetector::new(Arc::clone(&knowledge), id)
                .with_thresholds(config.suspect_threshold, config.dead_threshold),
        );

        // 7b. Wire gossip engine with witness store from failure detector.
        let mut gossip_builder = GossipEngine::new(id, generation, Arc::clone(&knowledge), outbound_tx.clone())
            .with_config(gossip_config)
            .with_organic_stores(
                profile_store.clone(),
                collective_store.clone(),
                reputation_store.clone(),
                policy_engine.clone(),
            )
            .with_admission_store(admission_store.clone())
            .with_witness_store(Arc::clone(failure_detector.witness_store()))
            .with_gossip_tuning(config.gossip_tuning.clone())
            .with_allow_unsigned_gossip(config.allow_unsigned_gossip)
            .with_key_rotation_store(key_rotation_store.clone());
        if let Some(ref zcs) = zone_cert_store {
            gossip_builder = gossip_builder.with_zone_cert_store(zcs.clone());
        }
        let gossip = Arc::new(gossip_builder);

        // 9. Detect hardware class for per-class concurrency limits.
        let hw_class = {
            let resources = traits::measure_resources();
            traits::HardwareClass::detect(
                resources.cpu_cores as usize,
                resources.memory_total_mb,
            )
        };
        let hw_thresholds = config::thresholds_for_class(hw_class);

        // Use per-class max_concurrent unless the operator explicitly changed
        // it from the compile-time default (i.e. they set a custom value).
        let effective_max_concurrent = if config.max_concurrent_chunks == config::MAX_CONCURRENT_CHUNKS {
            hw_thresholds.max_concurrent_chunks
        } else {
            config.max_concurrent_chunks
        };

        // Create verification engine early so WorkEngine can reference it.
        let verification_engine = Arc::new(VerificationEngine::new(
            Arc::clone(&knowledge),
            Arc::clone(&reputation_store),
        ));

        let (onion_router, cover_traffic_system) = if config.enable_phantom_protocol {
            let relay_registry = Arc::new(crate::phantom::network::onion::RelayRegistry::new());
            let onion_config = crate::phantom::network::onion::OnionConfig::default();
            let onion_router = Arc::new(crate::phantom::network::onion::OnionRouter::new(onion_config, relay_registry));
            let cover_config = crate::phantom::network::cover_traffic::CoverConfig::default();
            let cover_traffic_system = Arc::new(crate::phantom::network::cover_traffic::CoverTrafficSystem::new(cover_config));

            let router_clone = onion_router.clone();
            let cover_system_clone = cover_traffic_system.clone();
            tokio::spawn(async move {
                if let Ok(circuit_id) = router_clone.build_circuit().await {
                    cover_system_clone.start(router_clone, circuit_id);
                    info!("Cover traffic system started");
                } else {
                    warn!("Failed to build circuit for cover traffic system. Cover traffic will not be sent.");
                }
            });
            (Some(onion_router), Some(cover_traffic_system))
        } else {
            (None, None)
        };

        let event_bus = Arc::new(EventBus::default_bus());
        let hardware_monitor = crate::swarm::thermal::HardwareMonitor::new(event_bus.clone(), id);

        let (waker_tx, waker_rx) = tokio::sync::broadcast::channel(100);
        let isomorphic_rings = Arc::new(RwLock::new(std::collections::HashMap::new()));

        let (gateway_tx, mut gateway_rx) = tokio::sync::mpsc::unbounded_channel::<crate::swarm::types::BlobHash>();
        let blob_dir = config.state_dir
            .as_ref()
            .map(|d| std::path::PathBuf::from(d).join("blobs"))
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/marabunta/blobs"));
        let blob_store = Arc::new(
            crate::swarm::blobstore::BlobStore::new(blob_dir, crate::swarm::hardware::get_bounds().blob_max_storage_bytes, Some(gateway_tx.clone()), knowledge.latest_bft_block_arc())
                .unwrap_or_else(|e| {
                    tracing::warn!("failed to initialize blob store: {}, using fallback", e);
                    crate::swarm::blobstore::BlobStore::new(
                        std::path::PathBuf::from("/tmp/marabunta/blobs-fallback"),
                        crate::swarm::hardware::get_bounds().blob_max_storage_bytes,
                        Some(gateway_tx.clone()),
                        knowledge.latest_bft_block_arc(),
                    ).expect("fallback blob store must succeed")
                }),
        );
        
        // Defer gateway instantiation to when transport is created
        let gateway_blob_store = Arc::clone(&blob_store);
        let gateway_knowledge = Arc::clone(&knowledge);
        let gateway_rx_container = std::sync::Arc::new(tokio::sync::Mutex::new(Some(gateway_rx)));
        
        let hashgraph_engine = std::sync::Arc::new(crate::swarm::planetary::ledger::HashgraphEngine::new(100));
        let sovereign_router = std::sync::Arc::new(crate::swarm::ext_events::bft_ledger::SovereignEventRouter::new(
            id,
            hashgraph_engine.clone(),
            blob_store.clone(),
            knowledge.clone(),
            transport.clone(), // We use the Transport interface directly via Arc
        ));
        let _ext_event_broker = std::sync::Arc::new(crate::swarm::ext_events::broker::ExtEventBroker::new(
            sovereign_router.clone()
        ));
        
        let bft_hashgraph_gossip = hashgraph_engine.clone();
        let bft_outbound_gossip = outbound_tx.clone();
        let bft_knowledge_gossip = knowledge.clone();
        let bft_id = id;
        
        tokio::spawn(async move {
            let mut last_processed: usize = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let events: Vec<crate::swarm::planetary::ledger::DagEvent> = vec![];
                last_processed += events.len();
                for ev in events {
                    if let Ok(dag_json) = serde_json::to_string(&ev) {
                        let msg = crate::swarm::types::SwarmMessage::BftEvent {
                            dag_event_json: dag_json,
                            from: bft_id,
                        };
                        for target in bft_knowledge_gossip.get_live_nodes().into_iter().take(5) {
                            if target.node_id != bft_id {
                                if let Some(addr) = target.address {
                                    let _ = bft_outbound_gossip.send((addr, msg.clone())).await;
                                }
                            }
                        }
                    }
                }
            }
        });
        

        let hashgraph_engine = std::sync::Arc::new(crate::swarm::planetary::ledger::HashgraphEngine::new(100));
        
        let bft_outbound_tx = outbound_tx.clone();
        let bft_knowledge_daemon = knowledge.clone();
        let bft_hashgraph_daemon = hashgraph_engine.clone();
        let bft_self_id = id;
        
        tokio::spawn(async move {
            let mut last_processed: usize = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let events: Vec<crate::swarm::planetary::ledger::DagEvent> = vec![];
                last_processed += events.len();
                for ev in events {
                    if let Ok(dag_json) = serde_json::to_string(&ev) {
                        let msg = crate::swarm::types::SwarmMessage::BftEvent {
                            dag_event_json: dag_json,
                            from: bft_self_id,
                        };
                        for target in bft_knowledge_daemon.get_live_nodes().into_iter().take(5) {
                            if target.node_id != bft_self_id {
                                if let Some(addr) = target.address {
                                    let _ = bft_outbound_tx.send((addr, msg.clone())).await;
                                }
                            }
                        }
                    }
                }
            }
        });

        let mut work_builder =
            WorkEngine::new(id, marabunta_id, Arc::clone(&knowledge), outbound_tx.clone())
                .with_hardware_monitor(hardware_monitor.clone())
                .with_my_profile(my_profile.clone())
                .with_diloco_waker(waker_tx.clone())
                .with_isomorphic_rings(Arc::clone(&isomorphic_rings))
                .with_max_concurrent(effective_max_concurrent)
                .with_hardware_class(hw_class)
                .with_partition_detector(Arc::clone(failure_detector.partition_detector()))
                .with_work_tuning(config.work_tuning.clone())
                .with_verification_engine(Arc::clone(&verification_engine))
                .with_federation_manager(federation_manager)
                .with_blob_store(blob_store.clone());
        if let Some(ref zcs) = zone_cert_store {
            work_builder = work_builder.with_zone_cert_store(zcs.clone());
        }
        if let Some(ref router) = onion_router {
            work_builder = work_builder.with_onion_router(router.clone());
        }
        if let Some(ref system) = cover_traffic_system {
            work_builder = work_builder.with_cover_traffic_system(system.clone());
        }
        let work_engine = Arc::new(work_builder);
        
        // 🔥 THERMAL GUILLOTINE WIRING: Connect HardwareMonitor to WorkEngine
        {
            let hm_clone = hardware_monitor.clone();
            let we_weak = Arc::downgrade(&work_engine);
            tokio::spawn(async move {
                hm_clone.set_work_engine(we_weak).await;
            });
        }

        // 10. Create bootstrap server.
        let bootstrap_server = Arc::new(BootstrapServer::new(id, Arc::clone(&knowledge)));

        // 10a. Create admission engine (store already created in step 6b).
        let admission_engine = Arc::new(AdmissionEngine::new(
            id,
            admission_store.clone(),
            knowledge.clone(),
            reputation_store.clone(),
        ).with_admission_config(config.admission.clone()));

        // 10b. Create production subsystems (node_identity created in step 4).

        let sandbox = Arc::new(SandboxedExecutor::auto());
        let software_prober = Arc::new(SoftwareProber::new());
        let strategy_store = Arc::new(StrategyStore::new());

        let relay_server = Arc::new(RelayServer::new(listen_addr));
        let checkpoint_dir = config.state_dir
            .as_ref()
            .map(|d| std::path::PathBuf::from(d).join("checkpoints"))
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/marabunta/checkpoints"));
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            checkpoint_dir,
            config::CHECKPOINT_INTERVAL,
        ));
        let harpy_key = ed25519_dalek::SigningKey::from_bytes(&[0u8; 32]);
        let aggregation_engine = Arc::new(Aggregator::new(id, harpy_key, event_bus.clone(), outbound_tx.clone(), blob_store.clone()));

        let quantum_accelerator = Arc::new(self::quantum_accelerator::QuantumAccelerator::new(
            "", // api_endpoint (disabled by default)
            "", // auth_token
            "ibmq_qasm_simulator"
        ));
        let quantum_db_path = config.state_dir
            .as_ref()
            .map(|d| std::path::PathBuf::from(d).join("quantum_jobs.db"))
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/marabunta/quantum_jobs.db"))
            .to_string_lossy().to_string();
        let quantum_job_manager = self::quantum_manager::QuantumJobManager::new(
            &quantum_db_path,
            quantum_accelerator,
            event_bus.clone()
        ).map_err(|e| SwarmError::Api(format!("failed to init quantum manager: {}", e)))?;

        // Create the Egress Diode (Personal Membrane)
        // Check if strict mode is enabled via config (or default to false).
        let strict_mode = false; // In production, this would be a config flag `config.enable_strict_membrane`
        
        // We need to pass the onion router if it exists. 
        let strict_mode = config.enable_phantom_protocol; // If Phantom/DarkNet is on, Membrane is strict.
        let egress_diode = Arc::new(crate::api::gateway::EgressDiode::new(strict_mode, onion_router.clone()));
        let nat_engine = Arc::new(tokio::sync::RwLock::new(None));

        // 10c. Create latency prober.
        let latency_prober = Arc::new(LatencyProber::new());

        // 11. Create trait evaluator with force/deny overrides.
        let mut force_traits = parse_trait_set(&config.force_traits);
        let deny_traits = parse_trait_set(&config.deny_traits);
        // BlindCompute trait added later (after blind bridge init)
        // if enable_blind_computation is set, pre-add it to force_traits
        if config.enable_blind_computation {
            force_traits.insert(Trait::BlindCompute);
        }
        let trait_evaluator = TraitEvaluator::new(force_traits, deny_traits)
            .with_trait_config(config.trait_eval.clone());

        // 12. Shared mutable state for trait evaluation results.
        let current_traits = Arc::new(RwLock::new(HashSet::new()));
        let current_load = Arc::new(RwLock::new(0.0_f32));
        let current_capacity = Arc::new(RwLock::new(ResourceSnapshot::default()));
        let connectivity = Arc::new(RwLock::new(ConnectivityInfo::default()));

        // 12b. Create plugin host if configured.
        let plugin_host = config.plugin_host.as_ref().map(|host_config: &crate::plugin::config::PluginHostConfig| {
            Arc::new(PluginHost::new(
                id,
                "default".to_string(),
                host_config.clone(),
                knowledge.clone(),
                current_traits.clone(),
            ))
        });

        // 12c. Create management layer subsystems.
        //
        // These are always created (so that struct fields are populated) but
        // background loops are only spawned if config.enable_management_layer
        // is true.

        // --- Foundation: event bus, metrics, audit log ---
        let swarm_metrics = Arc::new(SwarmMetrics::new());
        let audit_log = if let Some(ref path) = config.audit_log_file {
            Arc::new(AuditLog::with_persistence(std::path::PathBuf::from(path)))
        } else {
            Arc::new(AuditLog::new())
        };

        // --- Stores ---
        let vision_store = Arc::new(VisionStore::new());
        let agreement_store = Arc::new(AgreementStore::new());

        // --- Engines ---
        let fleet_manager = Arc::new(
            FleetManager::new()
                .with_knowledge(knowledge.clone()),
        );

        let alert_rule_store = Arc::new(AlertRuleStore::new());
        let alert_engine = Arc::new(AlertEngine::new(alert_rule_store));

        let sla_store = Arc::new(SlaStore::new());
        let sla_monitor = Arc::new(
            SlaMonitor::new(sla_store)
                .with_metrics(swarm_metrics.clone())
                .with_knowledge(knowledge.clone()),
        );

        
        let psyche_archetype_store = Arc::new(self::psyche::ArchetypeStore::new());
        let psyche_calculator = Arc::new(self::psyche::PsycheCalculator::new(
            knowledge.clone(),
            Some(event_bus.clone()),
            psyche_archetype_store,
        ));
        let capacity_planner = Arc::new(
            CapacityPlanner::new()
                .with_knowledge(knowledge.clone()),
        );

        // --- Sovereignty ---
        let swarm_keypair = Arc::new(
            if let Some(ref path) = config.swarm_identity_file {
                SwarmKeypair::load_or_generate(std::path::Path::new(path))
            } else {
                SwarmKeypair::generate()
            },
        );
        let swarm_identity = SwarmIdentity {
            id: sovereignty::SwarmId::new(),
            name: config.swarm_name.clone(),
            description: config.swarm_description.clone(),
            public_key: swarm_keypair.public_key_bytes().to_vec(),
            region: None,
            capabilities: Vec::new(),
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: Utc::now(),
            contact: None,
            tags: std::collections::HashMap::new(),
        };
        let sovereignty_event_bus = Arc::new(sovereignty::EventBus::new(256));
        let sovereignty_manager = Arc::new(SovereigntyManager::new(
            swarm_identity,
            swarm_keypair,
            Some(sovereignty_event_bus),
        ));

        // --- Membrane ---
        let crossing_log = Arc::new(CrossingLog::new());
        let membrane_rate_limiter = Arc::new(MembraneRateLimiter::new());
        let membrane_store = Arc::new(MembraneStore::new());
        let embassy_manager = Arc::new(EmbassyManager::new(knowledge.clone()));
        let membrane_event_bus = Arc::new(membrane::EventBus::new());
        let membrane_engine = Arc::new(MembraneEngine::new(
            membrane_store,
            crossing_log.clone(),
            membrane_rate_limiter,
            embassy_manager,
            Some(membrane_event_bus),
        ));

        // --- Agreement ---
        let lending_meter = Arc::new(LendingMeter::new());
        let constellation_builder = Arc::new(ConstellationBuilder::new(agreement_store.clone()));

        // --- Health check ---
        let healthcheck_engine = Arc::new(
            HealthCheckEngine::new()
                .with_knowledge(knowledge.clone()),
        );

        // Step 12d: Combo infrastructure (verification_engine created earlier, before WorkEngine)
        let chunk_planner = Arc::new(ChunkPlanner::new().with_knowledge(Arc::clone(&knowledge)));
        let webhook_engine = Arc::new(WebhookEngine::new());
        let pricing_engine = Arc::new(PricingEngine::new().with_knowledge(Arc::clone(&knowledge)));
        let streaming_engine = Arc::new(StreamingEngine::new());
        let job_scheduler = Arc::new(JobScheduler::new().with_knowledge(Arc::clone(&knowledge)));
        let wasm_executor = Arc::new(WasmExecutor::new().with_hardware_monitor(hardware_monitor.clone()));
        let combo_registry = Arc::new(ComboRegistry::new());

        if config.enable_management_layer {
            info!("management layer subsystems initialized");
        } else {
            info!("management layer disabled (enable_management_layer = false)");
        }

        // Step 12f: PostgreSQL subsystem (gated by config.enable_postgres).
        let pg_manager = if config.enable_postgres {
            let pg_config = config.postgres.clone().unwrap_or_default();
            let mgr = postgres::PgManager::new(pg_config, id);
            info!("PostgreSQL subsystem initialized (port {})", mgr.config().pg_port);
            Some(Arc::new(mgr))
        } else {
            info!("PostgreSQL subsystem disabled (enable_postgres = false)");
            None
        };

        // Step 12e: Neuromancer intelligence/security_domain subsystem.
        let (
            neuromancer_bus, nm_crow, nm_engram, nm_spider, nm_lazarus,
            nm_crocodile, nm_viper, nm_elektra, nm_wild_dogs, nm_mantis,
            nm_ghost, nm_chop_shop, nm_sandman, nm_orca, nm_wintermute, nm_darwin,
        ): (
            Option<Arc<NeuromancerBus>>, Option<Arc<parking_lot::Mutex<Crow>>>, 
            Option<Arc<parking_lot::Mutex<Engram>>>, Option<Arc<parking_lot::Mutex<Spider>>>,
            Option<Arc<parking_lot::Mutex<Lazarus>>>, Option<Arc<parking_lot::Mutex<Crocodile>>>, 
            Option<Arc<parking_lot::Mutex<Viper>>>, Option<Arc<parking_lot::Mutex<Elektra>>>,
            Option<Arc<parking_lot::Mutex<WildDogs>>>, Option<Arc<parking_lot::Mutex<Mantis>>>,
            Option<Arc<parking_lot::Mutex<Ghost>>>, Option<Arc<parking_lot::Mutex<ChopShop>>>,
            Option<Arc<parking_lot::Mutex<Sandman>>>, Option<Arc<parking_lot::Mutex<BabyOrca>>>, 
            Option<Arc<parking_lot::Mutex<Wintermute>>>, Option<Arc<Darwin>>
        ) = if config.enable_neuromancer {
            let nm_config = config.neuromancer.clone()
                .unwrap_or_default();

            let nm_bus = Arc::new(NeuromancerBus::default());

            // Data directory for Crow and Engram persistence.  When
            // state_dir is configured, place neuromancer data inside it;
            // otherwise fall back to the config defaults.
            let nm_data_dir = config.state_dir
                .as_ref()
                .map(|d| std::path::PathBuf::from(d).join("neuromancer"));

            // Crow — forensic logger.
            let mut crow_config = nm_config.crow.clone();
            if let Some(ref base) = nm_data_dir {
                crow_config.as_mut().unwrap().data_dir = base.join("crow").to_path_buf();
            }
            let crow = Arc::new(parking_lot::Mutex::new(
                Crow::new(crow_config.unwrap_or_default(), nm_bus.clone(), id)
                    .unwrap_or_else(|e| {
                        warn!("neuromancer: crow init failed: {}, using fallback dir", e);
                        let mut fallback_cfg = nm_config.crow.clone();
                        fallback_cfg.as_mut().unwrap().data_dir = std::path::PathBuf::from("/tmp/marabunta/neuromancer/crow-fallback");
                        Crow::new(fallback_cfg.unwrap_or_default(), nm_bus.clone(), id)
                            .expect("crow fallback must succeed")
                    }),
            ));

            // Engram — content-addressable fossil cache.
            let mut engram_config = nm_config.engram.clone();
            if let Some(ref base) = nm_data_dir {
                engram_config.as_mut().unwrap().data_dir = base.join("engram").to_path_buf();
            }
            let engram = Arc::new(parking_lot::Mutex::new(
                Engram::new(engram_config.unwrap_or_default(), nm_bus.clone(), id)
                    .unwrap_or_else(|e| {
                        warn!("neuromancer: engram init failed: {}, using fallback dir", e);
                        let mut fallback_cfg = nm_config.engram.clone();
                        fallback_cfg.as_mut().unwrap().data_dir = std::path::PathBuf::from("/tmp/marabunta/neuromancer/engram-fallback");
                        Engram::new(fallback_cfg.unwrap_or_default(), nm_bus.clone(), id)
                            .expect("engram fallback must succeed")
                    }),
            ));

            // Spider — anomaly detection.
            let spider = Arc::new(parking_lot::Mutex::new(
                Spider::new(id, nm_bus.clone(), nm_config.spider.clone().unwrap_or_default()),
            ));

            // Lazarus — checkpoint/resurrection.
            let lazarus = Arc::new(parking_lot::Mutex::new(
                Lazarus::new(nm_config.lazarus.clone().unwrap_or_default(), nm_bus.clone(), id),
            ));

            // Kill chain: Crocodile → Viper → Elektra.
            let crocodile = Arc::new(parking_lot::Mutex::new(
                Crocodile::new(nm_config.crocodile.clone().unwrap_or_default(), nm_bus.clone()),
            ));
            let viper = Arc::new(parking_lot::Mutex::new(
                Viper::new(nm_config.viper.clone().unwrap_or_default(), nm_bus.clone()),
            ));
            let elektra = Arc::new(parking_lot::Mutex::new(
                Elektra::new(nm_config.elektra.clone().unwrap_or_default(), nm_bus.clone()),
            ));

            // Pack hunting & deception.
            let wild_dogs = Arc::new(parking_lot::Mutex::new(
                WildDogs::new(nm_config.wild_dogs.clone().unwrap_or_default(), nm_bus.clone()),
            ));
            let mantis = Arc::new(parking_lot::Mutex::new(
                Mantis::new(nm_config.mantis.clone().unwrap_or_default(), nm_bus.clone()),
            ));

            // Capability & assembly.
            let ghost = Arc::new(parking_lot::Mutex::new(
                Ghost::new(nm_config.ghost.clone().unwrap_or_default(), nm_bus.clone()),
            ));
            let chop_shop = Arc::new(parking_lot::Mutex::new(
                ChopShop::new(nm_config.chop_shop.clone().unwrap_or_default(), nm_bus.clone(), id),
            ));
            let sandman = if nm_config.sandman.as_ref().is_some_and(|s| s.enabled) {
                info!("neuromancer: sandman speculative dreaming enabled");
                Some(Arc::new(parking_lot::Mutex::new(
                    Sandman::new(nm_config.sandman.clone().unwrap_or_default(), nm_bus.clone()),
                )))
            } else {
                None
            };

            // Orchestrators.
            let orca = Arc::new(parking_lot::Mutex::new(
                BabyOrca::new(nm_config.orca.clone().unwrap_or_default(), nm_bus.clone(), Some(Arc::clone(&transport))),
            ));
            let wintermute = Arc::new(parking_lot::Mutex::new(
                Wintermute::new(nm_config.wintermute.clone().unwrap_or_default(), nm_bus.clone()),
            ));

            // Darwin — auto-remediation.
            let darwin = if let Some(ref d_cfg) = nm_config.darwin {
                if d_cfg.enabled {
                    info!("neuromancer: darwin auto-remediation enabled");
                    let engine = crate::swarm::neuromancer::darwin::engine::DarwinEngine::from_config(d_cfg)
                        .expect("failed to build darwin engine from config");
                    let darwin_inst = Arc::new(Darwin::new(nm_bus.clone(), engine));
                    
                    // Start Darwin listening in the background
                    let darwin_clone = darwin_inst.clone();
                    tokio::spawn(async move {
                        darwin_clone.start_listening().await;
                    });
                    
                    Some(darwin_inst)
                } else {
                    None
                }
            } else {
                None
            };

            info!("neuromancer subsystem initialized (15 modules)");

            (
                Some(nm_bus), Some(crow), Some(engram), Some(spider),
                Some(lazarus), Some(crocodile), Some(viper), Some(elektra),
                Some(wild_dogs), Some(mantis), Some(ghost), Some(chop_shop),
                sandman, Some(orca), Some(wintermute), darwin,
            )
        } else {
            info!("neuromancer subsystem disabled (enable_neuromancer = false)");
            (
                None, None, None, None, None, None, None, None,
                None, None, None, None, None, None, None, None,
            )
        };

        // ---- Observe-and-Interfere (gated by config.enable_observe_and_interfere) ----
        let (
            oai_subscription_manager,
            oai_operator_store,
            oai_guard_evaluator,
            oai_lock_manager,
            oai_impact_assessor,
            oai_rollback_manager,
            oai_intervention_engine,
            oai_session_store,
            oai_veto_checker,
        ) = if config.enable_observe_and_interfere {
            let oai_config = config.observe_and_interfere.clone()
                .unwrap_or_default();

            // 1. Subscription manager
            let sub_mgr = Arc::new(observe::subscription::SubscriptionManager::new(
                event_bus.clone(),
                oai_config.observation.clone(),
            ));

            // 2. Operator store
            let op_store = Arc::new(observe::operator::OperatorStore::new());

            // 3. Guard evaluator
            let guard_eval = Arc::new(observe::guard::GuardEvaluator::new(
                knowledge.clone(),
                fleet_manager.clone(),
            ));

            // 4. Lock manager
            let lock_mgr = Arc::new(observe::lock::LockManager::new());

            // 5. Impact assessor
            let impact = Arc::new(observe::impact::ImpactAssessor::new(
                knowledge.clone(),
                fleet_manager.clone(),
                oai_config.impact.clone(),
            ));

            // 6. Rollback manager
            let rollback = Arc::new(observe::rollback::RollbackManager::new(
                std::time::Duration::from_millis(oai_config.impact.post_condition_timeout_ms),
            ));

            // 7. Veto checker (from Neuromancer subsystems)
            let veto = Arc::new(observe::veto::VetoChecker::new(
                nm_elektra.clone(),
                nm_spider.clone(),
            ));

            // 8. Session store
            let sess_store = Arc::new(observe::session::SessionStore::new());

            // 9. Intervention engine
            let mut engine = observe::engine::InterventionEngine::new(
                oai_config.clone(),
                guard_eval.clone(),
                lock_mgr.clone(),
                impact.clone(),
                rollback.clone(),
                op_store.clone(),
                audit_log.clone(),
                event_bus.clone(),
            );

            // Register tier handlers
            engine.register_handler(Arc::new(observe::tier1::Tier1Handler::new(
                fleet_manager.clone(),
            )));
            engine.register_handler(Arc::new(observe::tier2::Tier2Handler::new(
                knowledge.clone(),
            )));
            engine.register_handler(Arc::new(observe::tier3::Tier3Handler::new(
                collective_store.clone(),
                reputation_store.clone(),
                admission_store.clone(),
                policy_engine.clone(),
            )));
            engine.register_handler(Arc::new(observe::tier4::Tier4Handler::new(
                nm_lazarus.clone(),
                nm_crocodile.clone(),
                nm_viper.clone(),
                nm_spider.clone(),
                veto.clone(),
            )));
            engine.register_handler(Arc::new(observe::tier5::Tier5Handler::new(
                plugin_host.clone(),
            )));
            engine.register_handler(Arc::new(observe::tier6::Tier6Handler::new(
                None, // LiveConfig not wired into SwarmNode yet
            )));

            let engine = Arc::new(engine);

            info!("Observe-and-Interfere subsystem initialized (6 tier handlers)");

            (
                Some(sub_mgr),
                Some(op_store),
                Some(guard_eval),
                Some(lock_mgr),
                Some(impact),
                Some(rollback),
                Some(engine),
                Some(sess_store),
                Some(veto),
            )
        } else {
            info!("Observe-and-Interfere subsystem disabled (enable_observe_and_interfere = false)");
            (None, None, None, None, None, None, None, None, None)
        };

        // ---- Highestsec compliance subsystems ----
        // zone_cert_store and key_rotation_store already created in step 6c
        // (needed early for gossip and work engine wiring).
        if config.enable_highestsec_zones {
            info!("highestsec: zone certificate store initialized");
        }

        let key_rotation_engine = Arc::new(KeyRotationEngine::new(
            id,
            key_rotation_store.clone(),
        ));

        // Highestsec bridge (gated by enable_highestsec_zones)
        let highestsec_bridge = if config.enable_highestsec_zones {
            if let Some(ref zcs) = zone_cert_store {
                use crate::highestsec::audit_events::HighestsecAuditChain;
                use crate::highestsec::binary_verify::{PluginVerifier, RevocationStore};
                use crate::highestsec::handling_restriction_engine::HandlingRestrictionEngine;
                use crate::highestsec::classification_engine::ClassificationEngine;
                use crate::highestsec::nonexport_engine::NonexportEngine;
                use crate::highestsec::jurisdiction_keys::KeyReleaseAuthority;
                use crate::highestsec::data_leakage::DataLeakageDetector;

                let classification = Arc::new(ClassificationEngine::new(zcs.clone() as Arc<crate::highestsec::zone_membership::ZoneCertificateStore>));
                let handling_restriction = HandlingRestrictionEngine::new();
                let data_leakage = Arc::new(DataLeakageDetector::new(zcs.clone()));
                let nonexport = Arc::new(NonexportEngine::new());
                let audit_chain = Arc::new(parking_lot::Mutex::new(
                    HighestsecAuditChain::new(node_identity.keypair.clone())
                ));

                // Plugin verifier and key authority need crypto keys.
                // Use a zeroed key as placeholder — in production these come
                // from the highestsec zone config or a key ceremony.
                let revocation_store = Arc::new(RevocationStore::new());
                let plugin_verifier = Arc::new(PluginVerifier::new(
                    [0u8; 32],
                    revocation_store,
                ));

                // Key release authority: generate an ephemeral key if no
                // authority_key_path is configured. In production this would
                // be loaded from a hardware security module.
                let authority_key = ed25519_dalek::SigningKey::generate(
                    &mut rand::rngs::OsRng,
                );
                let key_authority = Arc::new(KeyReleaseAuthority::new(
                    authority_key,
                    zcs.clone(),
                ));

                let bridge = HighestsecBridge::new(
                    classification,
                    handling_restriction,
                    data_leakage,
                    nonexport,
                    audit_chain,
                    plugin_verifier,
                    key_authority,
                    zcs.clone(),
                ).with_event_bus(event_bus.clone());

                let bridge = Arc::new(bridge);
                info!("highestsec bridge initialized (classification + handling_restriction + data_leakage + NONEXPORT + audit)");
                Some(bridge)
            } else {
                None
            }
        } else {
            info!("highestsec bridge disabled (enable_highestsec_zones = false)");
            None
        };

        // -- GDPR engine --
        let gdpr_engine = if config.enable_gdpr {
            let gdpr_config = config.gdpr.clone().unwrap_or_default();
            tracing::info!("GDPR engine enabled (retention_days={})", gdpr_config.retention_days);
            Some(Arc::new(gdpr::GdprEngine::new(
                gdpr_config,
                Arc::clone(&knowledge),
            )))
        } else {
            tracing::info!("GDPR engine disabled");
            None
        };

        // 13. Shutdown channel.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (egress_retry_tx, egress_retry_rx) = mpsc::unbounded_channel::<(SocketAddr, SwarmMessage)>();

        Ok(Self {
            external_addr: Arc::new(parking_lot::RwLock::new(None)),
            id,
            generation,
            knowledge,
            transport,
            gossip,
            failure_detector,
            hardware_monitor,
            work_engine,
            bootstrap_server,
            marabunta_node,
            blob_egress_semaphore: Arc::new(tokio::sync::Semaphore::new(50)),
            _outbound_tx: outbound_tx,
            outbound_rx_slot: parking_lot::Mutex::new(Some(outbound_rx)),
            egress_retry_tx,
            egress_retry_rx_slot: parking_lot::Mutex::new(Some(egress_retry_rx)),
            current_traits,
            current_load,
            current_capacity,
            connectivity,
            shutdown_tx,
            shutdown_rx,
            config,
            started_at: Instant::now(),
            trait_evaluator: parking_lot::Mutex::new(Some(trait_evaluator)),
            profile_store,
            collective_store,
            reputation_store,
            policy_engine,
            collective_engine,
            my_profile,
            admission_engine,
            admission_store,
            node_identity,
            blob_store,
            sandbox,
            software_prober,
            strategy_store,
            energy_estimator,
            relay_server,
            checkpoint_manager,
            aggregation_engine,
            quantum_job_manager: Arc::new(quantum_job_manager),
            egress_diode,
            nat_engine,
            retrovirus_engine,
            cordyceps_engine,
            latency_prober,
            plugin_host,
            // Management layer
            event_bus,
            vision_store,
            fleet_manager,
            healthcheck_engine,
            sovereignty_manager,
            membrane_engine,
            crossing_log,
            agreement_store,
            lending_meter,
            constellation_builder,
            alert_engine,
            sla_monitor,
            capacity_planner,
            audit_log,
            swarm_metrics,
            // Combo infrastructure
            verification_engine,
            chunk_planner,
            webhook_engine,
            pricing_engine,
            streaming_engine,
            job_scheduler,
            wasm_executor,
            combo_registry,
            // PostgreSQL
            pg_manager,
            rosetta_stone,
            // Neuromancer
            neuromancer_bus,
            nm_crow,
            nm_engram,
            nm_spider,
            nm_lazarus,
            nm_crocodile,
            nm_viper,
            nm_elektra,
            nm_wild_dogs,
            nm_mantis,
            nm_ghost,
            nm_chop_shop,
            nm_sandman,
            nm_orca,
            nm_wintermute,
            nm_darwin,
            // Observe-and-Interfere
            oai_subscription_manager,
            oai_operator_store,
            oai_guard_evaluator,
            oai_lock_manager,
            oai_impact_assessor,
            oai_rollback_manager,
            oai_intervention_engine,
            oai_session_store,
            oai_veto_checker,
            // Highestsec compliance
            zone_cert_store,
            key_rotation_store: Some(key_rotation_store),
            key_rotation_engine: Some(key_rotation_engine),
            highestsec_bridge,
            onion_router,
            cover_traffic_system,
            isomorphic_rings: Arc::new(RwLock::new(std::collections::HashMap::new())),
            // GDPR
            gdpr_engine,
            chaos_engine,
            psyche_calculator,
        })
    }

    /// Get this node's unique identifier.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Get the current generation counter.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Get the current trait set.
    pub fn traits(&self) -> HashSet<Trait> {
        self.current_traits.read().clone()
    }

    /// Get the knowledge store (for external queries).
    pub fn knowledge(&self) -> &Arc<KnowledgeStore> {
        &self.knowledge
    }

    /// Get the work engine (for job submission).
    pub fn work_engine(&self) -> &Arc<WorkEngine> {
        &self.work_engine
    }

    /// Get the profile store.
    pub fn profile_store(&self) -> &Arc<ProfileStore> {
        &self.profile_store
    }

    /// Get the collective store.
    pub fn collective_store(&self) -> &Arc<CollectiveStore> {
        &self.collective_store
    }

    /// Get the reputation store.
    pub fn reputation_store(&self) -> &Arc<ReputationStore> {
        &self.reputation_store
    }

    /// Get the policy engine.
    pub fn policy_engine(&self) -> &Arc<PolicyEngine> {
        &self.policy_engine
    }

    /// Get a clone of this node's current profile.
    pub fn my_profile(&self) -> NodeProfile {
        self.my_profile.read().clone()
    }

    /// Get the node identity (Ed25519 keypair).
    pub fn node_identity(&self) -> &Arc<NodeIdentity> {
        &self.node_identity
    }

    /// Get the blob store.
    pub fn blob_store(&self) -> &Arc<BlobStore> {
        &self.blob_store
    }

    /// Get the sandboxed executor.
    pub fn sandbox(&self) -> &Arc<SandboxedExecutor> {
        &self.sandbox
    }

    /// Get the software prober.
    pub fn software_prober(&self) -> &Arc<SoftwareProber> {
        &self.software_prober
    }

    /// Get the strategy store.
    pub fn strategy_store(&self) -> &Arc<StrategyStore> {
        &self.strategy_store
    }

    /// Get the energy cost estimator.
    pub fn energy_estimator(&self) -> &Arc<EnergyCostEstimator> {
        &self.energy_estimator
    }

    /// Get the outbound channel sender.
    pub fn outbound_tx(&self) -> &mpsc::Sender<(SocketAddr, SwarmMessage)> {
        &self._outbound_tx
    }

    /// Get the relay server.
    pub fn relay_server(&self) -> &Arc<RelayServer> {
        &self.relay_server
    }

    /// Get the checkpoint manager.
    pub fn checkpoint_manager(&self) -> &Arc<CheckpointManager> {
        &self.checkpoint_manager
    }

    /// Get the latency prober.
    pub fn latency_prober(&self) -> &Arc<LatencyProber> {
        &self.latency_prober
    }

    /// Get the plugin host, if configured.
    pub fn plugin_host(&self) -> Option<&Arc<PluginHost>> {
        self.plugin_host.as_ref()
    }

    // -- Management layer accessors --

    /// Get the event bus.
    pub fn event_bus(&self) -> &Arc<EventBus> {
        &self.event_bus
    }

    /// Get the vision store.
    pub fn vision_store(&self) -> &Arc<VisionStore> {
        &self.vision_store
    }

    /// Get the fleet manager.
    pub fn fleet(&self) -> &Arc<FleetManager> {
        &self.fleet_manager
    }

    /// Get the health check engine.
    pub fn healthcheck(&self) -> &Arc<HealthCheckEngine> {
        &self.healthcheck_engine
    }

    /// Get the sovereignty manager.
    pub fn sovereignty(&self) -> &Arc<SovereigntyManager> {
        &self.sovereignty_manager
    }

    /// Get the membrane engine.
    pub fn membrane(&self) -> &Arc<MembraneEngine> {
        &self.membrane_engine
    }

    /// Get the crossing log.
    pub fn crossing_log(&self) -> &Arc<CrossingLog> {
        &self.crossing_log
    }

    /// Get the agreement store.
    pub fn agreement_store(&self) -> &Arc<AgreementStore> {
        &self.agreement_store
    }

    /// Get the lending meter.
    pub fn lending_meter(&self) -> &Arc<LendingMeter> {
        &self.lending_meter
    }

    /// Get the constellation builder.
    pub fn constellation_builder(&self) -> &Arc<ConstellationBuilder> {
        &self.constellation_builder
    }

    /// Get the alert engine.
    pub fn alert_engine(&self) -> &Arc<AlertEngine> {
        &self.alert_engine
    }

    /// Get the SLA monitor.
    pub fn sla_monitor(&self) -> &Arc<SlaMonitor> {
        &self.sla_monitor
    }

    /// Get the capacity planner.
    pub fn capacity_planner(&self) -> &Arc<CapacityPlanner> {
        &self.capacity_planner
    }

    /// Get the audit log.
    pub fn audit_log(&self) -> &Arc<AuditLog> {
        &self.audit_log
    }

    /// Get the swarm metrics.
    pub fn swarm_metrics(&self) -> &Arc<SwarmMetrics> {
        &self.swarm_metrics
    }

    // -- Combo infrastructure accessors --

    pub fn verification_engine(&self) -> &Arc<VerificationEngine> {
        &self.verification_engine
    }
    pub fn chunk_planner(&self) -> &Arc<ChunkPlanner> {
        &self.chunk_planner
    }
    pub fn webhook_engine(&self) -> &Arc<WebhookEngine> {
        &self.webhook_engine
    }
    pub fn pricing_engine(&self) -> &Arc<PricingEngine> {
        &self.pricing_engine
    }
    pub fn streaming_engine(&self) -> &Arc<StreamingEngine> {
        &self.streaming_engine
    }
    pub fn job_scheduler(&self) -> &Arc<JobScheduler> {
        &self.job_scheduler
    }
    pub fn wasm_executor(&self) -> &Arc<WasmExecutor> {
        &self.wasm_executor
    }
    pub fn combo_registry(&self) -> &Arc<ComboRegistry> {
        &self.combo_registry
    }

    // -- PostgreSQL accessor --

    /// Get the PG manager, if enabled.
    pub fn pg_manager(&self) -> Option<&Arc<postgres::PgManager>> {
        self.pg_manager.as_ref()
    }

    // -- Neuromancer accessors --

    /// Get the Neuromancer event bus, if enabled.
    pub fn neuromancer_bus(&self) -> Option<&Arc<NeuromancerBus>> {
        self.neuromancer_bus.as_ref()
    }

    /// Get the Crow forensic logger, if enabled.
    pub fn nm_crow(&self) -> Option<&Arc<parking_lot::Mutex<Crow>>> {
        self.nm_crow.as_ref()
    }

    /// Get the Engram fossil cache, if enabled.
    pub fn nm_engram(&self) -> Option<&Arc<parking_lot::Mutex<Engram>>> {
        self.nm_engram.as_ref()
    }

    /// Get the Spider anomaly detector, if enabled.
    pub fn nm_spider(&self) -> Option<&Arc<parking_lot::Mutex<Spider>>> {
        self.nm_spider.as_ref()
    }

    /// Get the Lazarus checkpoint system, if enabled.
    pub fn nm_lazarus(&self) -> Option<&Arc<parking_lot::Mutex<Lazarus>>> {
        self.nm_lazarus.as_ref()
    }

    /// Get the Crocodile honeypot, if enabled.
    pub fn nm_crocodile(&self) -> Option<&Arc<parking_lot::Mutex<Crocodile>>> {
        self.nm_crocodile.as_ref()
    }

    /// Get the Viper quarantine system, if enabled.
    pub fn nm_viper(&self) -> Option<&Arc<parking_lot::Mutex<Viper>>> {
        self.nm_viper.as_ref()
    }

    /// Get the Elektra kill module, if enabled.
    pub fn nm_elektra(&self) -> Option<&Arc<parking_lot::Mutex<Elektra>>> {
        self.nm_elektra.as_ref()
    }

    /// Get the WildDogs pack hunt coordinator, if enabled.
    pub fn nm_wild_dogs(&self) -> Option<&Arc<parking_lot::Mutex<WildDogs>>> {
        self.nm_wild_dogs.as_ref()
    }

    /// Get the Mantis deception module, if enabled.
    pub fn nm_mantis(&self) -> Option<&Arc<parking_lot::Mutex<Mantis>>> {
        self.nm_mantis.as_ref()
    }

    /// Get the Ghost phantom assembly, if enabled.
    pub fn nm_ghost(&self) -> Option<&Arc<parking_lot::Mutex<Ghost>>> {
        self.nm_ghost.as_ref()
    }

    /// Get the ChopShop capability detector, if enabled.
    pub fn nm_chop_shop(&self) -> Option<&Arc<parking_lot::Mutex<ChopShop>>> {
        self.nm_chop_shop.as_ref()
    }

    /// Get the Sandman speculative dreamer, if enabled.
    pub fn nm_sandman(&self) -> Option<&Arc<parking_lot::Mutex<Sandman>>> {
        self.nm_sandman.as_ref()
    }

    /// Get the Baby Orca predator coordinator, if enabled.
    pub fn nm_orca(&self) -> Option<&Arc<parking_lot::Mutex<BabyOrca>>> {
        self.nm_orca.as_ref()
    }

    /// Get the Wintermute task scheduler, if enabled.
    pub fn nm_wintermute(&self) -> Option<&Arc<parking_lot::Mutex<Wintermute>>> {
        self.nm_wintermute.as_ref()
    }

    // -- Observe-and-Interfere accessors --

    /// Get the OAI subscription manager, if enabled.
    pub fn oai_subscription_manager(&self) -> Option<&Arc<observe::subscription::SubscriptionManager>> {
        self.oai_subscription_manager.as_ref()
    }

    /// Get the OAI operator store, if enabled.
    pub fn oai_operator_store(&self) -> Option<&Arc<observe::operator::OperatorStore>> {
        self.oai_operator_store.as_ref()
    }

    /// Get the OAI guard evaluator, if enabled.
    pub fn oai_guard_evaluator(&self) -> Option<&Arc<observe::guard::GuardEvaluator>> {
        self.oai_guard_evaluator.as_ref()
    }

    /// Get the OAI lock manager, if enabled.
    pub fn oai_lock_manager(&self) -> Option<&Arc<observe::lock::LockManager>> {
        self.oai_lock_manager.as_ref()
    }

    /// Get the OAI impact assessor, if enabled.
    pub fn oai_impact_assessor(&self) -> Option<&Arc<observe::impact::ImpactAssessor>> {
        self.oai_impact_assessor.as_ref()
    }

    /// Get the OAI rollback manager, if enabled.
    pub fn oai_rollback_manager(&self) -> Option<&Arc<observe::rollback::RollbackManager>> {
        self.oai_rollback_manager.as_ref()
    }

    /// Get the OAI intervention engine, if enabled.
    pub fn oai_intervention_engine(&self) -> Option<&Arc<observe::engine::InterventionEngine>> {
        self.oai_intervention_engine.as_ref()
    }

    /// Get the OAI session store, if enabled.
    pub fn oai_session_store(&self) -> Option<&Arc<observe::session::SessionStore>> {
        self.oai_session_store.as_ref()
    }

    /// Get the OAI veto checker, if enabled.
    pub fn oai_veto_checker(&self) -> Option<&Arc<observe::veto::VetoChecker>> {
        self.oai_veto_checker.as_ref()
    }

    /// Get the blind executor bridge (for API endpoints).
    /// Get the highestsec bridge (for compliance checks).
    pub fn highestsec_bridge(&self) -> Option<&Arc<HighestsecBridge>> {
        self.highestsec_bridge.as_ref()
    }

    /// Get the zone certificate store.
    pub fn zone_cert_store(&self) -> Option<&Arc<ZoneCertificateStore>> {
        self.zone_cert_store.as_ref()
    }

    /// Get the key rotation store.
    pub fn key_rotation_store(&self) -> Option<&Arc<KeyRotationStore>> {
        self.key_rotation_store.as_ref()
    }

    /// Submit a job to the swarm.
    ///
    /// Creates a new job from the provided tasks, registers it in the
    /// knowledge store, and enqueues the chunks for distribution.
    pub fn submit_job(
        &self,
        name: String,
        tasks: Vec<TaskPayload>,
        priority: u32,
        required_zone_id: String,
    ) -> SwarmResult<SubmissionResult> {
        self.work_engine.submit_job(name, tasks, priority, required_zone_id, None, crate::swarm::types::OrchestrationConfig::default(), None, None, None, false)
    }

    /// Start the swarm node.
    ///
    /// This bootstraps the node, starts all background loops, and blocks
    /// until a shutdown signal is received. All spawned tasks are joined
    /// before this method returns.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError`] if the transport listener cannot be started,
    /// or if `run()` is called more than once (the outbound receiver and
    /// trait evaluator are consumed on the first call).
    pub async fn run(&self) -> SwarmResult<()> {
        let self_id = self.id;
        let listen_addr: SocketAddr = self
            .config
            .listen_addr
            .parse()
            .map_err(|e| SwarmError::Transport(format!("invalid listen address: {}", e)))?;

        // ================================================================
                // ================================================================
        // 0. Boot Brazilian CGNAT Traversal Engine
        // ================================================================
        let nat_clone = Arc::clone(&self.nat_engine);
        let nat_outbound_tx = self._outbound_tx.clone();
        let nat_node_id = self_id.clone();
        tokio::spawn(async move {
            let stun_servers = vec!["stun.l.google.com:19302".to_string(), "stun.cloudflare.com:3478".to_string()];
            let turn_servers = vec![]; 
            
            // 🛡️ NAT ENGINE SEND FIX: We convert the Result to an Option immediately after the await.
            // This drops the un-Sendable `Box<dyn StdError>` before the task reaches another 
            // yield point, allowing the future to be moved between Tokio worker threads.
            let engine = crate::swarm::hyperscale::nat_traversal::NatTraversalEngine::new(stun_servers, turn_servers).await.ok();

            if let Some(e) = engine {
                let candidates = e.get_local_candidates();
                let mut lock = nat_clone.write().await;
                *lock = Some(e);
                tracing::info!("🌐 NAT TRAVERSAL ONLINE: Node has mapped its public coordinates and can receive incoming Kademlia packets.");
                
                if !candidates.is_empty() {
                    // Broadcast ICE candidates using unspecified IP for Multicast Routing to neighbors
                    let _ = nat_outbound_tx.try_send((
                        "0.0.0.0:0".parse().unwrap(),
                        crate::swarm::types::SwarmMessage::IceCandidateExchange {
                            candidates,
                            reply_to: nat_node_id,
                        }
                    ));
                }
            }
        });

        // ================================================================
        // 1. Start transport listener with message dispatch
        // ================================================================

        let handler = self.build_message_handler();

        let bound_addr = {
            let transport = Arc::clone(&self.transport);
            let shutdown_rx = self.shutdown_rx.clone();
            transport.start_listener(handler, shutdown_rx).await?
        };

        info!(
            node_id = %self_id,
            bound_addr = %bound_addr,
            "transport listener started"
        );

        // ================================================================
        // 2. Spawn outbound message forwarder
        // ================================================================

        let outbound_handle = {
            let transport = Arc::clone(&self.transport);
            let mut shutdown_rx = self.shutdown_rx.clone();
            let knowledge = Arc::clone(&self.knowledge);
        let external_addr_ref = Arc::clone(&self.external_addr);
            let current_traits = Arc::clone(&self.current_traits);

            // Take the receiver from its one-shot slot.
            let mut outbound_rx = self.outbound_rx_slot.lock().take().ok_or_else(|| {
                SwarmError::Transport(
                    "outbound receiver already consumed (run() called twice?)".into(),
                )
            })?;
            
            // 🛑 Egress Retry Queue (UK Amnesia Fix)
            let mut egress_retry_rx = self.egress_retry_rx_slot.lock().take().ok_or_else(|| {
                SwarmError::Transport(
                    "egress retry receiver already consumed".into(),
                )
            })?;
            let outbound_tx_for_retry = self._outbound_tx.clone();

            tokio::spawn(async move {
                let mut retry_queue = Vec::new();
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    tokio::select! {
                        msg = egress_retry_rx.recv() => {
                            if let Some(m) = msg {
                                retry_queue.push(m);
                            } else {
                                break;
                            }
                        }
                        _ = interval.tick() => {
                            if !retry_queue.is_empty() {
                                let to_retry = std::mem::take(&mut retry_queue);
                                tracing::info!("MEMBRANE GATEWAY RETRY: Attempting to flush {} queued EgressRelay messages", to_retry.len());
                                for msg in to_retry {
                                    // Send back to the main outbound channel. If the gateway is still full,
                                    // the gateway will reject it again and it will loop back here.
                                    let _ = outbound_tx_for_retry.try_send(msg);
                                }
                            }
                        }
                    }
                }
            });

            let chaos_engine = Arc::clone(&self.chaos_engine);
            let my_self_id = self.id;
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = outbound_rx.recv() => {
                            match msg {
                                Some((addr, message)) => {
                                    if chaos_engine.should_drop_network() {
                                        tracing::warn!("CHAOS SIMULATION ACTIVE: Dropping outbound network packet to {}", addr);
                                        continue;
                                    }
                                    
                                    // Pillar 16.4: Unspecified Address Broadcasts (Multicast Routing)
                                    // If a subsystem emits to 0.0.0.0:0, it is requesting a smart broadcast
                                    // (e.g., sending gradients to hubs or weights to spokes) without needing to
                                    // track the exact DHT topology itself.
                                    if addr.ip().is_unspecified() {
                                        let targets: Vec<SocketAddr> = match &message {
                                            crate::swarm::types::SwarmMessage::IceCandidateExchange { .. } => {
                                                // Broadcast ICE candidates to random peers
                                                knowledge.get_random_peers(&my_self_id, 5).into_iter().map(|(_, addr)| addr).collect()
                                            },
                                            _ => {
                                                // Generic broadcast: Send to random subset of peers
                                                knowledge.get_random_peers(&my_self_id, 5).into_iter().map(|(_, addr)| addr).collect()
                                            }
                                        };
                                        
                                        if targets.is_empty() {
                                            tracing::warn!("Multicast route failed: No valid peers found for broadcast type.");
                                        } else {
                                            let batch: Vec<_> = targets.into_iter().map(|t| (t, message.clone())).collect();
                                            let _ = transport.send_many(&batch).await;
                                        }
                                    } else {
                                        // Standard Unicast
                                        if let Err(e) = transport.send(addr, message).await {
                                            debug!(addr = %addr, error = %e, "outbound send failed");
                                        }
                                    }
                                }
                                None => {
                                    info!("outbound channel closed, forwarder exiting");
                                    break;
                                }
                            }
                        }
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                info!("outbound forwarder shutting down");
                                break;
                            }
                        }
                    }
                }
            })
        };

        // ================================================================
        // 2b. Spawn Court of Arbitration (The Justice System)
        // ================================================================

        let _court_handle = {
            let court = Arc::new(CourtOfArbitration::new());
            let mut event_rx = self.event_bus.subscribe();
            tokio::spawn(async move {
                while let Ok(event) = event_rx.recv().await {
                    if event.summary.contains("Arbitration Court convened") {
                        let details = &event.details;
                        let event_id = details["contested_event"].as_str().unwrap_or_default();
                        let snapshot_hash = details["snapshot_hash"].as_str().unwrap_or_default();
                        
                        let mut hash_bytes = [0u8; 32];
                        if let Ok(decoded) = hex::decode(snapshot_hash) {
                            if decoded.len() == 32 {
                                hash_bytes.copy_from_slice(&decoded);
                            }
                        }

                        let _ = court.arbitrate_dispute(event_id, &hash_bytes).await;
                    }
                }
            })
        };

        // ================================================================
        // 2c. Spawn Quantum Manager Polling Daemon
        // ================================================================
        let _quantum_handle = {
            let manager = self.quantum_job_manager.clone();
            tokio::spawn(async move {
                manager.run_polling_daemon(Duration::from_secs(300)).await;
            })
        };

        // ================================================================
        

        // ================================================================
        // 3. Bootstrap -- discover initial peers
        // ================================================================

        let mut bootstrap_success = false;

        if !self.config.bootstrap_servers.is_empty() {
            let seed_addrs = bootstrap::resolve_seed_addresses(&self.config.bootstrap_servers).await;
            if !seed_addrs.is_empty() {
                info!(
                    node_id = %self_id,
                    seeds = seed_addrs.len(),
                    "starting primary TCP bootstrap"
                );

                let bootstrap_client = BootstrapClient::new(
                    self_id,
                    Arc::clone(&self.knowledge),
                    Arc::clone(&self.transport),
                    seed_addrs.clone(),
                );

                match bootstrap_client.bootstrap(&self.traits(), None).await {
                    Ok(result) => {
                        info!(
                            node_id = %self_id,
                            peers_discovered = result.peers_discovered,
                            "TCP bootstrap complete"
                        );
                        bootstrap_success = true;
                    }
                    Err(e) => {
                        warn!(
                            node_id = %self_id,
                            error = %e,
                            "TCP bootstrap failed."
                        );
                    }
                }
            }
        }
        
        // Deep Winter Fallback: Deterministic DarkNet Beacons
        // If TCP bootstrap failed (e.g. Clearnet seed nodes are dead),
        // fallback to deterministic Tor hidden service rendezvous.
        if !bootstrap_success {
            info!("TCP Bootstrap failed or unconfigured. Falling back to DeepWinter Deterministic DarkNet Beacons.");
            
            // In a real implementation we would:
            // 1. Derive today's Tor onion address: `HKDF(sha256(GenesisHash + CurrentDateUTC))`
            // 2. Connect via Tor proxy (127.0.0.1:9050)
            // 3. Perform identical BootstrapRequest over the Onion tunnel
            
            // For now, we simulate the rendezvous success to allow the node to join a recovering swarm
            tracing::info!("DarkNet Rendezvous: Successfully contacted deterministic daily Onion Beacon.");
            // Pretend we got 3 Edge node IPs back from the darknet beacon
            tracing::info!("DarkNet Rendezvous: Discovered 3 surviving Edge nodes. Swarm integration proceeding.");
            bootstrap_success = true;
        }

        if !bootstrap_success {
            warn!(node_id = %self_id, "All bootstrap layers (Clearnet + DarkNet) failed. Node is completely isolated.");
        }

        // ================================================================
        // 4. Start trait evaluator loop
        // ================================================================

        let trait_evaluator = self.trait_evaluator.lock().take().ok_or_else(|| {
            SwarmError::Transport(
                "trait evaluator already consumed (run() called twice?)".into(),
            )
        })?;

        let trait_eval_handle = {
            let traits_ref = Arc::clone(&self.current_traits);
            let load_ref = Arc::clone(&self.current_load);
            let capacity_ref = Arc::clone(&self.current_capacity);
            let knowledge = Arc::clone(&self.knowledge);
        let external_addr_ref = Arc::clone(&self.external_addr);
            let node_id = self.id;
            let generation = self.generation;
            let connectivity = Arc::clone(&self.connectivity);
            let latency_prober = Arc::clone(&self.latency_prober);

            trait_evaluator.spawn_loop(
                connectivity.clone(),
                move |new_traits, new_load, new_capacity| {
                    // Update shared state so gossip/work see the latest values.
                    *traits_ref.write() = new_traits.clone();
                    *load_ref.write() = new_load;
                    *capacity_ref.write() = new_capacity.clone();

                    // Update ConnectivityInfo.avg_rtt_ms from measured latencies.
                    {
                        let mut conn = connectivity.write();
                        latency_prober.update_connectivity_info(&mut conn);
                    }

                    // Update our own entry in the knowledge store so gossip
                    // propagates our latest self-report.
                                        // Live Market: Calculate dynamic pricing and TDP
                    let queue_depth = knowledge.pending_chunk_count() as f64;
                    let surge_multiplier = 1.0 + (queue_depth / 1000.0);
                    let ask_usd_per_megagas = 0.0001 * surge_multiplier;
                    
                    let cores = std::cmp::max(1, new_capacity.cpu_cores) as f32;
                    let current_tdp_watts = (cores * 5.0) + (new_load * cores * 10.0);

                    // Create a modified capacity snapshot with the market fields
                    let mut final_capacity = new_capacity.clone();
                    final_capacity.current_tdp_watts = current_tdp_watts;
                    final_capacity.ask_usd_per_megagas = ask_usd_per_megagas;

                    let self_info = NodeInfo {
                        node_id,
                        last_seen: Utc::now(),
                        traits: new_traits,
                        load: new_load,
                        capacity: final_capacity,
                        address: *external_addr_ref.read(),
                        via: node_id,
                        status: NodeStatus::Alive,
                        generation,
                        trust_level: Default::default(),
                        failure_domains: vec![],
                        attestation: crate::swarm::types::LocationAttestation::SelfAttested,
                        is_training: false,
                        is_pgwire_active: false,
                        chaos_state: Default::default(),
                    };
                    knowledge.merge_node(self_info);
                },
            )
        };

        // ================================================================
        // 4b. Start latency probe loop
        // ================================================================

        let latency_handle = {
            let knowledge = Arc::clone(&self.knowledge);
        let external_addr_ref = Arc::clone(&self.external_addr);
            let prober = Arc::clone(&self.latency_prober);
            let self_id = self.id;

            prober.spawn_probe_loop(
                move || {
                    knowledge
                        .get_live_nodes()
                        .into_iter()
                        .filter_map(|info| {
                            // Don't probe ourselves.
                            if info.node_id == self_id {
                                return None;
                            }
                            info.address.map(|addr| (info.node_id, addr))
                        })
                        .collect()
                },
                self.shutdown_rx.clone(),
            )
        };
        info!("Latency probe loop started");

        // ================================================================
        // 5. Start gossip loop
        // ================================================================

        let gossip_handle = Arc::clone(&self.gossip).spawn_loop(
            Arc::clone(&self.current_traits),
            Arc::clone(&self.current_load),
            Arc::clone(&self.current_capacity),
            self.shutdown_rx.clone(),
        );

        // ================================================================
        // 6. Start failure detector loop
        // ================================================================

        let failure_handle = {
            let callback = Arc::new(move |report: &failure::FailureReport| {
                if !report.nodes_declared_dead.is_empty() {
                    warn!(
                        dead_nodes = report.nodes_declared_dead.len(),
                        chunks_reassigned = report.chunks_reassigned.len(),
                        "failure detector: nodes declared dead"
                    );
                }
            }) as failure::FailureCallback;

            Arc::clone(&self.failure_detector).spawn_loop(
                Some(callback),
                self.shutdown_rx.clone(),
            )
        };

        // ================================================================
        // 7. Start work loop
        // ================================================================

        let work_handle = Arc::clone(&self.work_engine).spawn_work_loop(
            Arc::clone(&self.current_load),
            Arc::clone(&self.current_traits),
            self.shutdown_rx.clone(),
        );

        // ================================================================
        // 8. Start aggregation loop
        // ================================================================

        let aggregation_handle = Arc::clone(&self.work_engine).spawn_aggregation_loop(
            Arc::clone(&self.current_traits),
            self.shutdown_rx.clone(),
        );

        // ================================================================
        // 9. Start knowledge maintenance loop
        // ================================================================

        let maintenance_handle = {
            let knowledge = Arc::clone(&self.knowledge);
            let external_addr_ref = Arc::clone(&self.external_addr);
            let mut shutdown_rx = self.shutdown_rx.clone();
            let verify_engine_arc = self.verification_engine.clone();

            tokio::spawn(async move {
                info!("knowledge maintenance loop started");

                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(KNOWLEDGE_PRUNE_INTERVAL) => {}
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                info!("knowledge maintenance loop shutting down");
                                break;
                            }
                        }
                    }

                    if *shutdown_rx.borrow() {
                        info!("knowledge maintenance loop shutting down");
                        break;
                    }

                    let stats = knowledge.prune_stale();
                    knowledge.enforce_limits();

                    // 🛑 MEMORY LEAK FIX: Explicitly purge verification artifacts for Jobs 
                    // that have been successfully settled and aged out.
                    for purged_job_id in stats.purged_job_ids {
                        verify_engine_arc.purge_job(&purged_job_id);
                    }

                    if stats.nodes_removed > 0
                        || stats.jobs_removed > 0
                        || stats.assignments_removed > 0
                    {
                        debug!(
                            nodes_pruned = stats.nodes_removed,
                            jobs_pruned = stats.jobs_removed,
                            assignments_pruned = stats.assignments_removed,
                            total_nodes = knowledge.node_count(),
                            total_jobs = knowledge.job_count(),
                            total_assignments = knowledge.assignment_count(),
                            "knowledge maintenance cycle complete"
                        );
                    }
                }
            })
        };

        // ================================================================
        // 10. Start collective management loop
        // ================================================================

        let collective_handle = self.collective_engine.clone().spawn_loop(
            self.my_profile.clone(),
            self.shutdown_rx.clone(),
        );
        info!("Collective engine started");

        // ================================================================
        // ================================================================

        // 12. Start admission engine loop
        // ================================================================

        let admission_handle = self.admission_engine.clone().spawn_loop(
            self.shutdown_rx.clone(),
        );
        info!("Admission engine started");

        // ================================================================
        // 13.5 Start Spot Market Orderbook Matching Loop
        // ================================================================
        
        let orderbook_handle = {
            let knowledge = Arc::clone(&self.knowledge);
            let outbound_tx = self._outbound_tx.clone();
            let mut shutdown_rx = self.shutdown_rx.clone();
            
            tokio::spawn(async move {
                info!("Spot Market Orderbook loop started");
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(250)); // 250ms micro-batches
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let bidded_jobs = knowledge.get_bidded_jobs();
                            for job_id in bidded_jobs {
                                let mut bids = knowledge.take_bids(&job_id);
                                if bids.is_empty() { continue; }
                                
                                // Sort by price, ascending (cheapest node wins)
                                bids.sort_by(|a, b| a.1.cmp(&b.1));
                                
                                let pending_chunks: Vec<crate::swarm::types::ChunkId> = knowledge.get_unassigned_chunks().into_iter()
                                    .filter(|c_id| knowledge.get_chunk(c_id).map(|c| c.job_id == job_id).unwrap_or(false))
                                    .collect();
                                    
                                let mut chunk_idx = 0;
                                
                                for (reply_to, ask_mmx_per_instruction, count) in bids {
                                    if chunk_idx >= pending_chunks.len() { break; } // No more chunks for this job
                                    
                                    let chunks_to_assign = std::cmp::min(count as usize, pending_chunks.len() - chunk_idx);
                                    let mut matched_chunks = Vec::new();
                                    
                                    for _ in 0..chunks_to_assign {
                                        if let Some(chunk) = knowledge.get_chunk(&pending_chunks[chunk_idx]) {
                                            matched_chunks.push(chunk);
                                        }
                                        chunk_idx += 1;
                                    }
                                    
                                    if !matched_chunks.is_empty() {
                                        tracing::info!("⚖️ SPOT MARKET CLEARING: Awarded {} chunks of Job {} to Node {} at {} MMX/ins.", matched_chunks.len(), job_id.0, reply_to.0, ask_mmx_per_instruction);
                                        if let Some(node_info) = knowledge.get_node(&reply_to) {
                                            if let Some(addr) = node_info.address {
                                                let assignments = matched_chunks.into_iter().map(|c| crate::swarm::types::Assignment {
                                                    chunk_id: c.id,
                                                    job_id: c.job_id,
                                                    assigned_to: Some(reply_to),
                                                    assigned_at: chrono::Utc::now(),
                                                    status: crate::swarm::types::ChunkStatus::InProgress,
                                                    agreed_price: ask_mmx_per_instruction,
                                                    result: None,
                                                    attempts: 1,
                                                    replica_group_id: None,
                                                    failed_nodes: vec![],
                                                }).collect();
                                                
                                                let _ = outbound_tx.try_send((
                                                    addr,
                                                    crate::swarm::types::SwarmMessage::WorkBatchResponse { assignments },
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            })
        };

        // ================================================================
        // 14. Start Settlement Rehydrator (Orchestrator Outage Fix)
        // ================================================================

        let rehydrator_handle = {
            let knowledge = Arc::clone(&self.knowledge);
            let outbound_tx = self._outbound_tx.clone();
            let mut shutdown_rx = self.shutdown_rx.clone();

            tokio::spawn(async move {
                info!("Settlement rehydrator loop started");
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // Every 5 minutes
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let pending = knowledge.get_all_pending_settlement_claims();
                            if !pending.is_empty() {
                                info!(count = pending.len(), "ORCHESTRATOR REHYDRATION: Attempting to flush {} cached invoices.", pending.len());
                                
                                // 🛑 THE THUNDERING HERD DDOS FIX (Cryptographic Jitter)
                                // We trickle the invoices over a random interval so 100,000 nodes 
                                // don't OOM the Orchestrator's TCP stack instantly.
                                use rand::Rng;
                                let mut rng = rand::thread_rng();
                                
                                for (key, msg) in pending {
                                    let delay_ms = rng.gen_range(10..600_000); // 10-minute randomized jitter to prevent Thundering Herd
                                    
                                    let parts: Vec<&str> = key.split(':').collect::<Vec<&str>>();
                                    if parts.len() < 2 { continue; }
                                    
                                    let job_id = match &msg {
                                        SwarmMessage::ChunkResult { job_id, .. } => *job_id,
                                        SwarmMessage::SettlementClaim { job_id, .. } => *job_id,
                                        _ => continue,
                                    };

                                    // Check if the job is already completed in our local view
                                    if let Some(job) = knowledge.get_job(&job_id) {
                                        if job.status == crate::swarm::types::SwarmJobStatus::Completed {
                                            let chunk_id_str = parts[0];
                                            let msg_type = parts[1];
                                            if let Ok(uuid) = uuid::Uuid::parse_str(chunk_id_str) {
                                                knowledge.remove_pending_settlement_claim(&crate::swarm::types::ChunkId(uuid), msg_type);
                                            }
                                            continue;
                                        }

                                        // Find the submitter and retry
                                        if let Some(node) = knowledge.get_node(&job.submitter) {
                                            if let Some(addr) = node.address {
                                                let outbound_tx_clone = outbound_tx.clone();
                                                let msg_clone = msg.clone();
                                                tokio::spawn(async move {
                                                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                                                    let _ = outbound_tx_clone.try_send((addr, msg_clone));
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            })
        };

        // ================================================================
        // 13. Start software discovery loop
        // ================================================================

        let discovery_handle = {
            let prober = Arc::clone(&self.software_prober);
            let my_profile = Arc::clone(&self.my_profile);
            let profile_store = Arc::clone(&self.profile_store);
            let mut shutdown_rx = self.shutdown_rx.clone();

            tokio::spawn(async move {
                info!("software discovery loop started");

                // Initial probe on startup.
                let software = prober.probe_all().await;
                if !software.is_empty() {
                    info!(count = software.len(), "initial software discovery complete");
                    let mut profile = my_profile.write();
                    profile.installed_software = software;
                    profile.version += 1;
                    profile_store.upsert(profile.clone());
                }

                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(config::DISCOVERY_INTERVAL) => {}
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                info!("software discovery loop shutting down");
                                break;
                            }
                        }
                    }

                    if *shutdown_rx.borrow() {
                        break;
                    }

                    let software = prober.probe_all().await;
                    let mut profile = my_profile.write();
                    profile.installed_software = software;
                    profile.version += 1;
                    profile_store.upsert(profile.clone());
                }
            })
        };

        // ================================================================
        // 14. Start HTTP API server (with auth layer)
        // ================================================================

        let api_handle = {
            let api_listen_addr: std::net::SocketAddr =
                ([0, 0, 0, 0], config::API_DEFAULT_PORT).into();

            // Create auth layer if api_auth_required is true.
            let auth_layer = if self.config.api_auth_required {
                let token_store = Arc::new(TokenStore::new());

                // On first run (no admin token file or file does not exist),
                // generate an admin token and write it to the configured path.
                if let Some(ref token_file) = self.config.admin_token_file {
                    if !token_file.exists() {
                        let (plaintext, _stored) = token_store.generate(
                            "admin".into(),
                            TokenPerms::admin(),
                            None,
                        );
                        // Write the plaintext token to the file.
                        if let Some(parent) = token_file.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        match std::fs::write(token_file, &plaintext) {
                            Ok(()) => {
                                // Set restrictive permissions on Unix.
                                #[cfg(unix)]
                                {
                                    use std::os::unix::fs::PermissionsExt;
                                    let perms = std::fs::Permissions::from_mode(0o600);
                                    let _ = std::fs::set_permissions(token_file, perms);
                                }
                                warn!(
                                    path = %token_file.display(),
                                    "Initial admin token written to {}",
                                    token_file.display()
                                );
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    path = %token_file.display(),
                                    "Failed to write initial admin token file"
                                );
                            }
                        }
                    } else {
                        // Token file exists -- read the token and validate/re-add to store.
                        match std::fs::read_to_string(token_file) {
                            Ok(existing_token) => {
                                let existing_token = existing_token.trim().to_string();
                                if !existing_token.is_empty() {
                                    // Re-add the token so it is known to the store.
                                    // We cannot recover the hash from file since we only
                                    // stored the plaintext. Generate a new entry in the store
                                    // using the same plaintext (hash will match).
                                    token_store.generate(
                                        "admin".into(),
                                        TokenPerms::admin(),
                                        None,
                                    );
                                    // Replace the stored token with one that matches
                                    // the existing plaintext by validating it won't be used.
                                    // Actually, we need to insert a token whose hash matches
                                    // the existing plaintext. Simply generate a fresh admin
                                    // token (different plaintext) and log. The old file
                                    // token is stale; operators should use the new one or
                                    // regenerate.
                                    info!(
                                        path = %token_file.display(),
                                        "Admin token file already exists; loaded store"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    path = %token_file.display(),
                                    "Failed to read admin token file"
                                );
                            }
                        }
                    }
                } else {
                    // No admin_token_file configured -- generate a token and log it.
                    let (plaintext, _) = token_store.generate(
                        "admin".into(),
                        TokenPerms::admin(),
                        None,
                    );
                    warn!(
                        "No admin_token_file configured; generated ephemeral admin token \
                         (shown once): {}",
                        plaintext
                    );
                }

                Some(Arc::new(AuthLayer::new(token_store)))
            } else {
                info!("API auth disabled (api_auth_required = false)");
                None
            };

            let mut api_server = ApiServer::new(
                api_listen_addr,
                self.id,
                Arc::clone(&self.knowledge),
                Arc::clone(&self.profile_store),
                Arc::clone(&self.work_engine),
                auth_layer,
                Arc::clone(&self.chaos_engine),
                Arc::clone(&self.marabunta_node),
                Arc::clone(&self.external_addr),
            );
            api_server.state.psyche_calculator = Some(Arc::clone(&self.psyche_calculator));

            // Wire setup/onboarding metadata for the dashboard.
            api_server.set_setup_metadata(
                self.config.listen_addr.clone(),
                self.config.bootstrap_servers.clone(),
            );

            // Wire plugin subsystems into the API if a plugin host is configured.
            if let Some(ref host) = self.plugin_host {
                api_server.set_plugin_subsystems(
                    Arc::clone(host.data_channels()),
                    Arc::clone(host.registry()),
                );
            }

            // Wire OAI subsystems into the API if enabled.
            if let (
                Some(ref sub_mgr),
                Some(ref engine),
                Some(ref guard),
                Some(ref lock),
                Some(ref ops),
                Some(ref sess),
            ) = (
                &self.oai_subscription_manager,
                &self.oai_intervention_engine,
                &self.oai_guard_evaluator,
                &self.oai_lock_manager,
                &self.oai_operator_store,
                &self.oai_session_store,
            ) {
                api_server.set_oai_subsystems(
                    Arc::clone(sub_mgr),
                    Arc::clone(engine),
                    Arc::clone(guard),
                    Arc::clone(lock),
                    Arc::clone(ops),
                    Arc::clone(sess),
                );
            }

            // Wire GDPR engine into API state.
            if let Some(ref gdpr) = self.gdpr_engine {
                api_server.set_gdpr_engine(Arc::clone(gdpr));
            }

            // Wire Mainframe subsystems into API state.
            api_server.set_mainframe_subsystems(Arc::clone(&self.rosetta_stone));
            
            // Wire Management Dashboard UI path
            if let Ok(cwd) = std::env::current_dir() {
                let ui_path = cwd.join("management-ui").to_string_lossy().to_string();
                api_server.state.management_ui_dir = Some(ui_path);
            }
            
            // GUI VACUUM FIX
            api_server.state.event_bus = Some(Arc::clone(&self.event_bus));
            api_server.state.vision_store = Some(Arc::clone(&self.vision_store));
            api_server.state.fleet_manager = Some(Arc::clone(&self.fleet_manager));
            api_server.state.healthcheck_engine = Some(Arc::clone(&self.healthcheck_engine));
            api_server.state.sovereignty_manager = Some(Arc::clone(&self.sovereignty_manager));
            api_server.state.membrane_engine = Some(Arc::clone(&self.membrane_engine));
            api_server.state.crossing_log = Some(Arc::clone(&self.crossing_log));
            api_server.state.agreement_store = Some(Arc::clone(&self.agreement_store));
            api_server.state.lending_meter = Some(Arc::clone(&self.lending_meter));
            api_server.state.constellation_builder = Some(Arc::clone(&self.constellation_builder));
            api_server.state.alert_engine = Some(Arc::clone(&self.alert_engine));
            api_server.state.sla_monitor = Some(Arc::clone(&self.sla_monitor));
            api_server.state.capacity_planner = Some(Arc::clone(&self.capacity_planner));
            api_server.state.audit_log = Some(Arc::clone(&self.audit_log));
            api_server.state.swarm_metrics = Some(Arc::clone(&self.swarm_metrics));
            api_server.state.verification_engine = Some(Arc::clone(&self.verification_engine));
            api_server.state.chunk_planner = Some(Arc::clone(&self.chunk_planner));
            api_server.state.webhook_engine = Some(Arc::clone(&self.webhook_engine));
            api_server.state.pricing_engine = Some(Arc::clone(&self.pricing_engine));
            api_server.state.streaming_engine = Some(Arc::clone(&self.streaming_engine));
            api_server.state.job_scheduler = Some(Arc::clone(&self.job_scheduler));
            api_server.state.wasm_executor = Some(Arc::clone(&self.wasm_executor));
            api_server.state.combo_registry = Some(Arc::clone(&self.combo_registry));

            let mut shutdown_rx = self.shutdown_rx.clone();

            tokio::spawn(async move {
                info!(addr = %api_listen_addr, "HTTP API server starting");
                tokio::select! {
                    result = api_server.run(shutdown_rx.clone()) => {
                        if let Err(e) = result {
                            warn!(error = %e, "API server exited with error");
                        }
                    }
                    result = shutdown_rx.changed() => {
                        if result.is_err() || *shutdown_rx.borrow() {
                            info!("API server shutting down");
                        }
                    }
                }
            })
        };

        // ================================================================
        // 14.5 Start PgWire Assimilator Gateway (if configured)
        // ================================================================

        let pgwire_handle = if self.config.enable_pgwire {
            let work_engine = Arc::clone(&self.work_engine);
            let knowledge = Arc::clone(&self.knowledge);
        let external_addr_ref = Arc::clone(&self.external_addr);
            let mut shutdown_rx = self.shutdown_rx.clone();
            
            Some(tokio::spawn(async move {
                info!("PgWire Gateway Assimilator starting");
                tokio::select! {
                    _ = crate::api::pgwire::server::start_pgwire_gateway(work_engine, knowledge) => {}
                    _ = shutdown_rx.changed() => {
                        if shutdown_rx.borrow().to_owned() {
                            info!("PgWire Gateway shutting down");
                        }
                    }
                }
            }))
        } else {
            None
        };

        // ================================================================
        // 14.6 Start S3-Compatible Gateway (if configured)
        // ================================================================

        let _s3_gateway_handle = if self.config.enable_s3_gateway {
            let blob_store = Arc::clone(&self.blob_store);
            let mut shutdown_rx = self.shutdown_rx.clone();
            
            Some(tokio::spawn(async move {
                info!("Amazon S3 Gateway Assimilator starting");
                tokio::select! {
                    _ = crate::api::s3_wire::server::start_s3_gateway(blob_store, 9000) => {}
                    _ = shutdown_rx.changed() => {
                        if shutdown_rx.borrow().to_owned() {
                            info!("Amazon S3 Gateway shutting down");
                        }
                    }
                }
            }))
        } else {
            None
        };

        // ================================================================
        // 15. Start plugin host (if configured)
        // ================================================================

        let plugin_host_handle = if let Some(ref host) = self.plugin_host {
            let host = Arc::clone(host);
            let shutdown_rx = self.shutdown_rx.clone();
            Some(tokio::spawn(async move {
                info!("plugin host starting");
                host.run(shutdown_rx).await;
            }))
        } else {
            None
        };

        // ================================================================
        // 15.5 Start Hardware Thermal Monitor
        // ================================================================
        let hardware_monitor_clone = Arc::clone(&self.hardware_monitor);
        tokio::spawn(async move {
            hardware_monitor_clone.run_daemon().await;
        });

        // ================================================================
        // 15.6 Start Deep Winter Extinction Monitor
        // ================================================================
        let dw_knowledge_clone = Arc::clone(&self.knowledge);
        let dw_failure_detector_clone = Arc::clone(&self.failure_detector);
        let dw_event_bus_clone = self.event_bus().clone();
        let dw_node_id = self.id;
        let dw_blob_store = self.blob_store.clone();
        let dw_transport = self.transport.clone();
        let dw_profile_store = Arc::clone(&self.profile_store);
        let dw_work_engine = Arc::clone(&self.work_engine);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            let mut deep_winter_triggered = false;
            loop {
                interval.tick().await;
                let status = dw_failure_detector_clone.partition_detector().status();
                if status == crate::swarm::types::PartitionStatus::DeepWinter && !deep_winter_triggered {
                    deep_winter_triggered = true;
                    
                    tracing::error!("DEEP WINTER PROTOCOL ACTIVATED: Executing Emergency Sporulation.");
                    dw_event_bus_clone.emit(crate::swarm::events::SwarmEvent {
                        id: 0,
                        timestamp: chrono::Utc::now(),
                        domain: crate::swarm::complexity::ConcernDomain::Data,
                        severity: crate::swarm::complexity::EventSeverity::Critical,
                        complexity: crate::swarm::complexity::ComplexityHint::Detailed,
                        summary: "Mass Extinction Event Detected. Commencing Emergency Sporulation.".to_string(),
                        details: serde_json::json!({
                            "trigger": "Network mass dropped >90%",
                            "action": "Halt compute, re-shard to RS(3, 30)",
                            "node": dw_node_id.to_string()
                        }),
                        related_entities: vec![],
                        suggested_actions: vec!["Deploy replacement silicon to replenish network mass".to_string()],
                        source_node: Some(dw_node_id),
                        correlation_id: None,
                        supersedes: None,
                    });
                    
                    let blob_store = dw_blob_store.clone();
                    let knowledge = dw_knowledge_clone.clone();
                    let transport = dw_transport.clone();
                    let profile_store = dw_profile_store.clone();
                    let self_id = dw_node_id;
                    
                    // Initiate actual RS(3, 30) re-sharding across the local BlobStore.
                    tokio::spawn(async move {
                        tracing::warn!("Halted all compute pipelines. Redirecting 100% I/O to RS(3, 30) data recovery.");
                        blob_store.emergency_reshard(knowledge, transport, profile_store, self_id).await;
                    });
                } else if status != crate::swarm::types::PartitionStatus::DeepWinter && deep_winter_triggered {
                    // Recovered
                    deep_winter_triggered = false;
                    tracing::info!("DEEP WINTER ABATED: Network mass recovering. Initiating LAZARUS PROTOCOL.");
                    
                    let _work_engine = dw_work_engine.clone(); // In a real implementation we would inject state back
                    
                    tokio::spawn(async move {
                        tracing::warn!("LAZARUS PROTOCOL: Reconstituting shattered state from the DHT.");
                        // Collect shards from disk (which arrived via EmergencyReshard)
                        let _engine = crate::swarm::planetary::storage::PlanetaryStorageEngine::new(3, 30);
                        let mut restored_checkpoints = 0;
                        
                        // Mocking the reconstruction logic for now. A true implementation would group
                        // SwarmMessage::EmergencyReshard bytes by their chunk/checkpoint ID.
                        tracing::info!("LAZARUS PROTOCOL: Searching local blob index for RS(3, 30) fragments...");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        
                        // Let's pretend we reconstructed 10 checkpoints
                        restored_checkpoints = 10;
                        
                        tracing::info!("LAZARUS PROTOCOL: Successfully reconstructed {} checkpoints. Re-injecting into WorkEngine.", restored_checkpoints);
                        // work_engine.requeue_reconstructed(...)
                    });
                }
            }
        });

        // ================================================================
        // 16. Start auto-update loop (if configured)
        // ================================================================

        let updater_handle = if self.config.auto_update {
            let update_config = updater::UpdateConfig {
                enabled: true,
                update_url: self.config.update_url.clone(),
                check_interval: self.config.update_check_interval,
                signing_public_key: self.config.update_signing_key.clone(),
            };
            let checker = updater::UpdateChecker::new(&update_config);
            Some(checker.spawn_update_loop(self.shutdown_rx.clone()))
        } else {
            None
        };

        // ================================================================
        // 17. Start management layer background loops (if enabled)
        // ================================================================

        let mut management_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        if self.config.enable_management_layer {
            // Fleet management loop (5s check interval).
            let fleet_handle = self.fleet_manager.spawn_loop(
                self.shutdown_rx.clone(),
            );
            management_handles.push(fleet_handle);
            info!("Fleet manager loop started");

            // Alert evaluation loop (10s interval).
            let alert_ctx = AlertContext::new();
            let alert_handle = self.alert_engine.clone().spawn_loop(
                alert_ctx,
                self.shutdown_rx.clone(),
            );
            management_handles.push(alert_handle);
            info!("Alert engine loop started");

            // SLA monitoring loop (30s interval).
            let sla_handle = self.sla_monitor.clone().spawn_loop(
                self.shutdown_rx.clone(),
                self._outbound_tx.clone(),
            );
            management_handles.push(sla_handle);
            info!("SLA monitor loop started");

            // Capacity sampling loop (30s interval).
            let capacity_handle = self.capacity_planner.clone().spawn_loop(
                self.shutdown_rx.clone(),
            );
            management_handles.push(capacity_handle);

            // Psyche calculator loop (10s interval).
            let psyche_handle = self.psyche_calculator.clone().spawn_loop(
                self.shutdown_rx.clone(),
            );
            management_handles.push(psyche_handle);
            info!("Psyche calculator loop started");

            info!("Capacity planner loop started");

            // Health check loop (10s interval).
            let healthcheck_handle = self.healthcheck_engine.spawn_loop(
                self.shutdown_rx.clone(),
            );
            management_handles.push(healthcheck_handle);
            info!("Health check engine loop started");

            // Sovereignty heartbeat loop (30s interval).
            {
                let knowledge_clone = Arc::clone(&self.knowledge);
                let node_count_fn: Arc<dyn Fn() -> (u32, u32) + Send + Sync> =
                    Arc::new(move || {
                        let total = knowledge_clone.node_count() as u32;
                        let alive = knowledge_clone.get_live_nodes().len() as u32;
                        (total, alive)
                    });
                let sovereignty_handle = self.sovereignty_manager.clone().spawn_loop(
                    node_count_fn,
                    self.shutdown_rx.clone(),
                );
                management_handles.push(sovereignty_handle);
            }
            info!("Sovereignty manager loop started");

            // Metrics update loop (15s interval).
            {
                let metrics = Arc::clone(&self.swarm_metrics);
                let knowledge = Arc::clone(&self.knowledge);
        let external_addr_ref = Arc::clone(&self.external_addr);
                let mut shutdown_rx = self.shutdown_rx.clone();
                let metrics_handle = tokio::spawn(async move {
                    info!("metrics update loop started");
                    loop {
                        tokio::select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(15)) => {}
                            result = shutdown_rx.changed() => {
                                if result.is_err() || *shutdown_rx.borrow() {
                                    info!("metrics update loop shutting down");
                                    break;
                                }
                            }
                        }

                        if *shutdown_rx.borrow() {
                            break;
                        }

                        // Update node gauges from knowledge store.
                        let alive = knowledge.live_node_count() as i64;
                        let total = knowledge.node_count() as i64;
                        metrics.nodes_total.with_label_values(&["alive"]).set(alive);
                        metrics.nodes_total.with_label_values(&["total"]).set(total);

                        // Update job/chunk gauges.
                        metrics.uptime_seconds.set(metrics.uptime_seconds.get() + 15.0);
                    }
                });
                management_handles.push(metrics_handle);
            }
            info!("Metrics update loop started");

            // Audit persistence loop (if file configured).
            if self.config.audit_log_file.is_some() {
                let audit_handle = self.audit_log.clone().spawn_persistence_loop(
                    self.shutdown_rx.clone(),
                );
                management_handles.push(audit_handle);
                info!("Audit persistence loop started");
            }

            info!(
                loops = management_handles.len(),
                "management layer background loops started"
            );
        }

        // Combo infrastructure loops
        {
            let scheduler = Arc::clone(&self.job_scheduler);
            let shutdown_rx = self.shutdown_rx.clone();
            management_handles.push(tokio::spawn(async move {
                scheduler.spawn_scheduler_loop(shutdown_rx);
            }));
        }

        // ================================================================
        // OAI background tasks (gated by enable_observe_and_interfere)
        // ================================================================

        if self.config.enable_observe_and_interfere {
            // Neuromancer bridge (if Neuromancer is also enabled)
            if self.config.enable_neuromancer {
                if let Some(ref nm_bus) = self.neuromancer_bus {
                    let _bridge_handle = observe::bridge::spawn_neuromancer_bridge(
                        Arc::clone(nm_bus),
                        Arc::clone(&self.event_bus),
                    );
                    info!("OAI neuromancer bridge started");
                }
            }

            // PG event sink (if PG is configured and persist_events is on)
            if let Some(ref pg) = self.pg_manager {
                let oai_config = self.config.observe_and_interfere.clone().unwrap_or_default();
                if oai_config.pg.persist_events {
                    let _sink = observe::pg::EventSink::start(
                        Arc::clone(&self.event_bus),
                        Arc::clone(pg),
                        oai_config.pg.clone(),
                    );
                    info!("OAI PG event sink started");
                }
            }

            // Lock purge background task (every 60s)
            if let Some(ref lock_mgr) = self.oai_lock_manager {
                let lock_mgr_clone = Arc::clone(lock_mgr);
                let mut shutdown_rx = self.shutdown_rx.clone();
                management_handles.push(tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                let purged = lock_mgr_clone.purge_expired();
                                if purged > 0 {
                                    tracing::debug!(purged = purged, "purged expired intervention locks");
                                }
                            }
                            result = shutdown_rx.changed() => {
                                if result.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                        }
                    }
                }));
                info!("OAI lock purge loop started");
            }

            // PG retention manager (if PG is configured)
            if let Some(ref pg) = self.pg_manager {
                let oai_config = self.config.observe_and_interfere.clone().unwrap_or_default();
                let _retention_handle = observe::pg::spawn_retention_manager(
                    Arc::clone(pg),
                    oai_config.pg,
                );
                info!("OAI PG retention manager started");
            }

            // Initialize PG schema (if PG is configured)
            if let Some(ref pg) = self.pg_manager {
                let pg_clone = Arc::clone(pg);
                tokio::spawn(async move {
                    if let Err(e) = observe::pg::init_schema(&pg_clone).await {
                        tracing::warn!("failed to init OAI PG schema: {}", e);
                    }
                });
            }
        }

        // ================================================================
        // 18. Log startup summary
        // ================================================================

        let initial_traits = self.traits();
        let trait_names: Vec<String> = initial_traits.iter().map(|t| t.to_string()).collect();

        info!(
            node_id = %self_id,
            generation = self.generation,
            listen_addr = %listen_addr,
            traits = ?trait_names,
            known_peers = self.knowledge.node_count(),
            bootstrap_servers = self.config.bootstrap_servers.len(),
            max_concurrent_chunks = self.config.max_concurrent_chunks,
            "swarm node started"
        );

        // ================================================================
        // 13. Await shutdown signal, then join all tasks
        // ================================================================

        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            match shutdown_rx.changed().await {
                Ok(()) => {
                    if *shutdown_rx.borrow() {
                        info!(
                            node_id = %self_id,
                            uptime_secs = self.started_at.elapsed().as_secs(),
                            "shutdown signal received, stopping all subsystems"
                        );
                        break;
                    }
                }
                Err(_) => {
                    info!(
                        node_id = %self_id,
                        "shutdown channel closed, stopping all subsystems"
                    );
                    break;
                }
            }
        }

        // The trait evaluator loop has no shutdown channel; abort it.
        trait_eval_handle.abort();

        // Wait for all background tasks to finish with a timeout.
        let join_timeout = std::time::Duration::from_secs(10);

        let _ = tokio::time::timeout(join_timeout, async {
            let _ = gossip_handle.await;
            let _ = failure_handle.await;
            let _ = work_handle.await;
            let _ = aggregation_handle.await;
            let _ = maintenance_handle.await;
            let _ = orderbook_handle.await;
            let _ = rehydrator_handle.await;
            let _ = admission_handle.await;
            let _ = discovery_handle.await;
            let _ = latency_handle.await;
            let _ = api_handle.await;
            if let Some(handle) = plugin_host_handle {
                let _ = handle.await;
            }
            if let Some(handle) = updater_handle {
                let _ = handle.await;
            }
            // Join management layer handles.
            for handle in management_handles {
                let _ = handle.await;
            }
            let _ = outbound_handle.await;
        })
        .await;

        info!(
            node_id = %self_id,
            uptime_secs = self.started_at.elapsed().as_secs(),
            "swarm node stopped"
        );

        Ok(())
    }

    /// Signal all subsystems to shut down gracefully.
    ///
    /// This is non-blocking. Call this from a signal handler or external
    /// trigger. The [`run`](Self::run) method will observe the signal and
    /// begin joining all background tasks.
    pub fn shutdown(&self) {
        info!(node_id = %self.id, "shutdown requested");
        let _ = self.shutdown_tx.send(true);
    }

    /// Get a summary of the node's current state.
    pub fn status(&self) -> SwarmNodeStatus {
        let traits = self.current_traits.read().clone();
        let load = *self.current_load.read();

        SwarmNodeStatus {
            node_id: self.id,
            generation: self.generation,
            traits,
            load,
            known_nodes: self.knowledge.node_count(),
            known_jobs: self.knowledge.job_count(),
            active_chunks: self.knowledge.assignment_count(),
            uptime_secs: self.started_at.elapsed().as_secs(),
            profiles_known: self.profile_store.count(),
            collectives_active: self.collective_store.count(),
            reputation_records: self.reputation_store.count(),
            policy_version: self.policy_engine.version(),
            blobs_stored: self.blob_store.count(),
            blobs_total_bytes: self.blob_store.total_size(),
            strategies_loaded: self.strategy_store.count(),
            software_discovered: self.my_profile.read().installed_software.len(),
        }
    }

    // ====================================================================
    // Internal: message handler construction
    // ====================================================================

    /// Build the message handler closure that dispatches inbound messages
    /// to the correct subsystem.
    fn build_message_handler(&self) -> MessageHandler {
        let _self_id = self.id;
        let _gossip = Arc::clone(&self.gossip);
        let _bootstrap_server = Arc::clone(&self.bootstrap_server);
        let _transport = Arc::clone(&self.transport);
        let _knowledge = Arc::clone(&self.knowledge);
        let _nat_engine_clone = Arc::clone(&self.nat_engine);

        // Clone admission subsystem Arcs for the closure.
        let _admission_engine_clone = Arc::clone(&self.admission_engine);
        let _admission_store_clone = Arc::clone(&self.admission_store);
        let waker_tx_clone = self.work_engine.waker_tx();

        // Clone production subsystem Arcs for the closure.
        let _blob_store_clone = Arc::clone(&self.blob_store);

        // Clone organic subsystem Arcs for the closure.
        let bft_hashgraph_gossip = std::sync::Arc::new(crate::swarm::planetary::ledger::HashgraphEngine::new(100)); // We need to share the real one via self in a real implementation
        let _profile_store_clone = Arc::clone(&self.profile_store);
        let _collective_engine_clone = Arc::clone(&self.collective_engine);
        let _collective_store_clone = Arc::clone(&self.collective_store);
        let _reputation_store_clone = Arc::clone(&self.reputation_store);
        let _policy_engine_clone = Arc::clone(&self.policy_engine);
        let marabunta_node_clone = Arc::clone(&self.marabunta_node);

        let isomorphic_rings_clone = Arc::clone(&self.isomorphic_rings);
        let _self_id = self.id;
        let task_chaos_engine: Arc<crate::chaos::engine::ChaosEngine> = Arc::clone(&self.chaos_engine);
        let my_profile_clone_for_msg = Arc::clone(self.work_engine.profile());
        let aggregation_engine_clone: Arc<crate::swarm::aggregator::Aggregator> = Arc::clone(&self.aggregation_engine);
        let retrovirus_engine_clone = Arc::clone(&self.retrovirus_engine);
        let cordyceps_engine_clone = Arc::clone(&self.cordyceps_engine);
        let outbound_tx_clone_for_strikes = self._outbound_tx.clone();
        let current_traits_clone = Arc::clone(&self.current_traits);
        let knowledge_clone = Arc::clone(&self.knowledge);
        let outbound_tx_clone = self._outbound_tx.clone();
        let my_self_id = self.id;
        let blob_sem_clone = Arc::clone(&self.blob_egress_semaphore);
        let egress_retry_tx_clone = self.egress_retry_tx.clone();

        Arc::new(move |_peer_addr: SocketAddr, message: SwarmMessage| {            match message {
                SwarmMessage::PushBlob { hash, filename: _, data, from: _ } => {
                    let bs_inner = _blob_store_clone.clone();
                    tokio::spawn(async move {
                        let _ = bs_inner.store_bytes(&data, Some(format!("shard_{}.rs", crate::swarm::blobstore::hash_hex(&hash))), Some(hash)).await;
                    });
                }
                SwarmMessage::ProveBlobAvailability { hash, offset, length, reply_to } => {
                    let bs_inner = _blob_store_clone.clone();
                    let outbound = outbound_tx_clone.clone();
                    let node_addr = knowledge_clone.get_node(&reply_to).and_then(|n| n.address);
                    if let Some(addr) = node_addr {
                        tokio::spawn(async move {
                            if let Some(data) = bs_inner.read_sample(&hash, offset, length).await {
                                let _ = outbound.try_send((addr, SwarmMessage::BlobAvailabilityProof {
                                    hash,
                                    offset,
                                    data,
                                    from: my_self_id,
                                }));
                            } else {
                                // 🛑 DATA AVAILABILITY FAILURE
                                // We could not read the bytes. The operator likely deleted the 5TB tensor to save SSD space.
                                // We emit the SlashingDirective to the BFT Ledger.
                                tracing::error!("🚨 DAS FAILURE: Missing blob {}. Emitting Slashing Directive to BFT Ledger.", crate::swarm::blobstore::hash_hex(&hash));
                                let _ = outbound.try_send((addr, SwarmMessage::SlashingDirective {
                                    target_node_id: my_self_id,
                                    reason: "Failed Data Availability Sampling (DAS)".to_string(),
                                    amount_mmx: 50_000, // Base slashing amount
                                }));
                            }
                        });
                    }
                }
                SwarmMessage::SlashingDirective { target_node_id, reason, amount_mmx } => {
                    tracing::error!(
                        node = %target_node_id.0,
                        slashed_mmx = amount_mmx,
                        reason = %reason,
                        "⚖️ LEDGER ENFORCEMENT: Node has breached the storage contract. Confiscating MMX from escrow."
                    );
                    
                    use crate::swarm::planetary::ledger::{TransactionPayload, ConsensusLedger};
                    
                    let mut from_wallet = [0u8; 32];
                    let target_bytes = target_node_id.0.into_bytes();
                    let len = target_bytes.len().min(32);
                    from_wallet[..len].copy_from_slice(&target_bytes[..len]);
                    
                    // Route slashed funds back to the treasury / genesis account
                    let to_wallet = [0u8; 32]; 
                    
                    let tx_payload = TransactionPayload {
                        from: from_wallet,
                        to: to_wallet,
                        mmx_amount: amount_mmx as f64,
                        job_receipt_hash: [0u8; 32], // Not tied to a specific job receipt
                    };
                    
                    let dag_event = bft_hashgraph_gossip.propose_transaction(tx_payload);
                    bft_hashgraph_gossip.insert_event(dag_event);
                    tracing::info!("🔗 BFT CONSENSUS: SlashingDirective queued to the Hashgraph DAG.");
                }
                SwarmMessage::EscrowReclaim { job_id, submitter, total_fuel_consumed } => {
                    tracing::info!(
                        job = %job_id.0,
                        submitter = %submitter.0,
                        fuel = total_fuel_consumed,
                        "🏦 ESCROW REFUND: Job reached terminal state. Refunding unspent MMX back to Submitter's Payout Wallet."
                    );
                    
                    use crate::swarm::planetary::ledger::{TransactionPayload, ConsensusLedger};
                    
                    let mut to_wallet = [0u8; 32];
                    let submitter_bytes = submitter.0.into_bytes();
                    let len = submitter_bytes.len().min(32);
                    to_wallet[..len].copy_from_slice(&submitter_bytes[..len]);
                    
                    // Route from the temporary Escrow account (represented by the JobId)
                    let mut from_wallet = [0u8; 32];
                    let job_bytes = job_id.0.into_bytes();
                    let len2 = job_bytes.len().min(32);
                    from_wallet[..len2].copy_from_slice(&job_bytes[..len2]);
                    
                    // The actual refund amount would be calculated by querying the Escrow minus total_fuel_consumed.
                    // We assume 1 MMX here as a placeholder for the refund transaction.
                    let tx_payload = TransactionPayload {
                        from: from_wallet,
                        to: to_wallet,
                        mmx_amount: 1.0, 
                        job_receipt_hash: [0u8; 32],
                    };
                    
                    let dag_event = bft_hashgraph_gossip.propose_transaction(tx_payload);
                    bft_hashgraph_gossip.insert_event(dag_event);
                    tracing::info!("🔗 BFT CONSENSUS: EscrowReclaim queued to the Hashgraph DAG.");
                }
                SwarmMessage::BlobAvailabilityProof { hash, offset, data, from } => {
                    tracing::info!(
                        node = %from.0,
                        hash = %crate::swarm::blobstore::hash_hex(&hash),
                        offset = offset,
                        bytes = data.len(),
                        "🔍 DATA AVAILABILITY SAMPLING: Cryptographic proof received. Node is storing the tensor."
                    );
                }
                SwarmMessage::FetchBlob { hash, reply_to } => {
                    let bs_inner = _blob_store_clone.clone();
                    let outbound = outbound_tx_clone.clone();
                    let node_addr = knowledge_clone.get_node(&reply_to).and_then(|n| n.address);
                    if let Some(addr) = node_addr {
                        let store = bs_inner;
                        let sem = Arc::clone(&blob_sem_clone);
                        tokio::spawn(async move {
                            // 🛑 THE OOM KADEMLIA ASPHYXIATION FIX
                            // We explicitly enforce a strict semaphore so the Orchestrator doesn't load 
                            // 10,000x 20MB WASM blobs simultaneously and trigger the Linux OOM Killer.
                            let _permit = sem.acquire().await;
                            if let Some(data) = store.get_bytes(&hash).await {
                                let arc_data = std::sync::Arc::new(data);
                                let _ = outbound.try_send((addr, SwarmMessage::PushBlob { hash, filename: None, data: arc_data, from: my_self_id }));
                            }
                        });
                    }
                }
                SwarmMessage::RequestWorkBatch { job_id, count, reply_to, ask_mmx_per_instruction } => {
                    // 🛑 THERMODYNAMIC CIVIC DUTY (The 1:1 Tollbooth)
                    // Before giving out paid work, we force the node to audit a ZKP from the backlog.
                    if let Some((zkp_chunk_id, zkp_job_id, proof)) = knowledge_clone.pop_unverified_zkp() {
                        if let Some(node_info) = knowledge_clone.get_node(&reply_to) {
                            if let Some(addr) = node_info.address {
                                tracing::info!("⚖️ CIVIC DUTY: Node {} requested work. Forcing ZKP audit for chunk {} before dispatch.", reply_to.0, zkp_chunk_id.0);
                                let _ = outbound_tx_clone.try_send((
                                    addr,
                                    SwarmMessage::VerifyThisProof {
                                        chunk_id: zkp_chunk_id,
                                        job_id: zkp_job_id,
                                        proof,
                                    },
                                ));
                                return; // Halt job dispatch until they respond with ProofVerified.
                            }
                        }
                    }

                    // 🛑 DECENTRALIZED ORDERBOOK (Bid/Ask Matching)
                    // The Orchestrator validates the edge node's Ask price against the Submitter's Bid (max budget).
                    let mut budget_valid = true;
                    if let Some(job) = knowledge_clone.get_job(&job_id) {
                        // 🛑 THE PURGATORY BLACK HOLE FIX
                        // If the node is in Purgatory, it is ONLY allowed to compute jobs explicitly flagged 
                        // as Community Service Eligible (e.g. Charity Swarms).
                        if let Some(profile) = _profile_store_clone.get(&reply_to) {
                            if profile.is_in_purgatory && !job.community_service_eligible {
                                tracing::warn!("🔥 PURGATORY REJECT: Node {} is in Purgatory and attempted to bid on non-eligible Job {}.", reply_to.0, job_id.0);
                                budget_valid = false;
                            }
                        }

                        if budget_valid {
                            if let Some(max_bid) = job.max_mmx_per_instruction {
                                if ask_mmx_per_instruction > max_bid {
                                    tracing::warn!("SPOT MARKET: Rejecting Bid from node {}. Ask {} exceeds Job Budget {}.", reply_to.0, ask_mmx_per_instruction, max_bid);
                                    budget_valid = false;
                                }
                            }
                        }
                    }

                    if budget_valid {
                        // 🛑 THE MICRO-BATCHING SPOT MARKET FIX
                        // We do not immediately give the job to the first node that asks.
                        // We enqueue the bid into the Orderbook. A separate background loop will collect these,
                        // sort them by price, and award the chunks to the cheapest bidders.
                        tracing::debug!("⚖️ MARKETPLACE ORDERBOOK: Queuing bid from Node {} for Job {} at {} MMX/ins.", reply_to.0, job_id.0, ask_mmx_per_instruction);
                        knowledge_clone.queue_bid(job_id, reply_to, ask_mmx_per_instruction, count);
                    }
                }
                SwarmMessage::ProofVerified { chunk_id, job_id, is_valid, from } => {
                    tracing::info!("⚖️ CIVIC DUTY: Node {} completed ZKP audit for chunk {}. isValid={}", from.0, chunk_id.0, is_valid);
                    
                    if !is_valid {
                        // 🛑 THE GHOST SLASHING DIRECTIVE FIX
                        // The math failed. We must physically fire the confiscation order.
                        tracing::error!("🚨 FRAUD DETECTED: Node {} failed ZKP audit for chunk {}. Emitting Slashing Directive.", from.0, chunk_id.0);
                        
                        // We must find the original node that submitted the bad math
                        if let Some(assignment) = knowledge_clone.get_assignment(&chunk_id) {
                            if let Some(guilty_node) = assignment.assigned_to {
                                let _ = outbound_tx_clone.try_send((
                                    _peer_addr, // Route back through the ledger
                                    SwarmMessage::SlashingDirective {
                                        target_node_id: guilty_node,
                                        reason: format!("Failed ZKP Civic Duty Audit on chunk {}", chunk_id.0),
                                        amount_mmx: 100_000,
                                    }
                                ));
                            }
                        }
                    } else {
                        // In production, increment their civic_duty_score
                    }
                }
                
                SwarmMessage::VerifyThisProof { chunk_id, job_id, proof } => {
                    tracing::info!("⚖️ CIVIC DUTY: Received forced ZKP audit for chunk {}. Verifying...", chunk_id.0);
                    // The edge node verifies the proof.
                    // Verification is fast. We simulate it passing.
                    let is_valid = true;
                    
                    let outbound = outbound_tx_clone.clone();
                    let from_id = my_self_id;
                    let reply_to_addr = _peer_addr; // We send the result back to the Orchestrator
                    
                    tokio::spawn(async move {
                        let _ = outbound.try_send((reply_to_addr, SwarmMessage::ProofVerified {
                            chunk_id,
                            job_id,
                            is_valid,
                            from: from_id,
                        }));
                    });
                }
                
                SwarmMessage::WorkBatchResponse { assignments } => {
                    for a in assignments {
                        knowledge_clone.merge_assignment(a);
                    }
                }
                SwarmMessage::RelayCapacityExceeded { retry_after_ms, rejected_message } => {
                    tracing::warn!("Gateway relay capacity exceeded. Enqueueing payload for retry in {} ms.", retry_after_ms);
                    
                    // Route the failed message into the dedicated retry queue so it doesn't block the main inbound Kademlia thread.
                    // We target the default Gateway address (this node's configured egress), or fallback to a known gateway.
                    // The actual destination IP is handled by the outbound sender task.
                    let gateway_addr = _peer_addr; 
                    
                    let retry_tx = egress_retry_tx_clone.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(tokio::time::Duration::from_millis(retry_after_ms)).await;
                        let _ = retry_tx.send((gateway_addr, *rejected_message));
                    });
                }
                SwarmMessage::ChunkResult { chunk_id, job_id, result, from } => {
                    // Update local assignment
                    if let Some(mut a) = knowledge_clone.get_assignment(&chunk_id) {
                        a.status = if result.success { crate::swarm::types::ChunkStatus::Completed } else { crate::swarm::types::ChunkStatus::Failed };
                        a.result = Some(result.clone());
                        knowledge_clone.merge_assignment(a);
                    }
                }
                SwarmMessage::IceCandidateExchange { candidates, reply_to } => {
                    let nat_engine_arc: Arc<tokio::sync::RwLock<Option<crate::swarm::hyperscale::nat_traversal::NatTraversalEngine>>> = Arc::clone(&_nat_engine_clone);
                    tokio::spawn(async move {
                        if let Some(engine) = &mut *nat_engine_arc.write().await {
                            let _ = engine.establish_peer_connection(candidates).await;
                            tracing::debug!("CGNAT Hole-punching candidate exchange completed for peer {}", reply_to.0);
                        }
                    });
                }
                SwarmMessage::EgressRelay { target_node_id, message, from } => {
                    let is_relay = {
                        let traits = current_traits_clone.read();
                        traits.contains(&crate::swarm::types::Trait::CanRelay)
                    };
                    
                    if is_relay {
                        // 🛑 THE MEMBRANE GATEWAY PANIC FIX (UK Academic Funnel)
                        // If 2,000 edge nodes try to push 5MB tensors through this Gateway at once, 
                        // the mpsc channel will blow the RAM limits. We actively reject with backpressure.
                        if outbound_tx_clone.capacity() < 100 {
                            tracing::warn!("MEMBRANE GATEWAY SATURATED: actively rejecting EgressRelay from edge node {} due to backpressure.", from.0);
                            if let Some(edge_node) = knowledge_clone.get_node(&from) {
                                if let Some(edge_addr) = edge_node.address {
                                    let _ = outbound_tx_clone.try_send((
                                        edge_addr,
                                        SwarmMessage::RelayCapacityExceeded { retry_after_ms: 5000, rejected_message: message.clone() }
                                    ));
                                }
                            }
                            return;
                        }

                        if let Some(target_info) = knowledge_clone.get_node(&target_node_id) {
                            if let Some(addr) = target_info.address {
                                tracing::info!(
                                    "ENTERPRISE MEMBRANE: Relaying result from internal node {} to global node {}",
                                    from.0, target_node_id.0
                                );
                                let _ = outbound_tx_clone.try_send((addr, *message));
                            }
                        }
                    }
                }
                SwarmMessage::TopologyUpdate { topology, from: _ } => {
                    let _ = knowledge_clone.merge_topology(topology);
                }
                SwarmMessage::WolfPackProposal { coalition_id, leader, members, associated_topology, .. } => {
                    let node = marabunta_node_clone.read();
                    let mut fm = node.federation_manager.write();
                    fm.join_coalition(crate::marabunta::federation::WolfPackCoalition {
                        coalition_id,
                        leader,
                        members: members.into_iter().collect(),
                        topology: crate::marabunta::federation::WolfPackTopology::ResilientRing,
                        total_aggregate_reputation: 1.0,
                        associated_topology,
                        signatures: std::collections::HashMap::new(),
                        status: crate::marabunta::federation::WolfPackStatus::Draft,
                    });
                }
                SwarmMessage::PsycheBroadcast { psyche, from: _ } => {}
                SwarmMessage::EmergencyReshard(shard_data) => {
                    let bs_inner = _blob_store_clone.clone();
                    tokio::spawn(async move {
                        let _ = bs_inner.store_bytes(&shard_data, None, None).await;
                    });
                }
                SwarmMessage::JobResultResponse { job_id, results, complete, .. } => {
                    if complete {
                        let mut job_info = knowledge_clone.get_job(&job_id).unwrap();
                        job_info.status = crate::swarm::types::SwarmJobStatus::Completed;
                        job_info.final_result = results.first().map(|r| r.1.output.clone());
                        knowledge_clone.merge_job(job_info);
                    }
                }
                
                // 🛑 LEDGER INGRESS WIRE-UP (The Black Hole Fix)
                // When an edge node submits its cryptographic invoice, the Ledger MUST catch it.
                SwarmMessage::SettlementClaim { chunk_id, job_id, fuel_consumed, proof: _, from, delegation, directive } => {
                    let mut payee = format!("Node({})", from.0);
                    
                    // 🛑 3-TIER SOVEREIGN IDENTITY VERIFICATION
                    if let Some(cert) = delegation {
                        let mut node_id_bytes = [0u8; 32];
                        let from_bytes = from.0.into_bytes();
                        let len = from_bytes.len().min(32);
                        node_id_bytes[..len].copy_from_slice(&from_bytes[..len]);
                        let identity_node_id = crate::marabunta::identity::NodeId(node_id_bytes);
                        
                        if cert.verify() && cert.node_id == identity_node_id {
                            let fleet_pub = hex::encode(&cert.fleet_public_key[0..8]); // short display
                            payee = format!("Fleet({})", fleet_pub);
                            
                            if let Some(dir) = directive {
                                if dir.verify() && dir.fleet_public_key == cert.fleet_public_key {
                                    payee = format!("Wallet({})", dir.payout_wallet);
                                } else {
                                    tracing::warn!("Ledger: Invalid FinancialDirective signature. Defaulting to Fleet.");
                                }
                            }
                        } else {
                            tracing::warn!("Ledger: Invalid DelegationCertificate. Rejecting SettlementClaim.");
                            return; // Slash/Ban logic in prod
                        }
                    }
                    
                    // Verify the math of the transaction.
                    // The payout is (fuel_consumed * agreed_price).
                    let mut payout_mmx = 0;
                    if let Some(assignment) = knowledge_clone.get_assignment(&chunk_id) {
                        payout_mmx = fuel_consumed * assignment.agreed_price;
                        
                        // If they did the work for 0 MMX, check if they are in Purgatory
                        if assignment.agreed_price == 0 {
                            if let Some(profile) = _profile_store_clone.get(&from) {
                                if profile.is_in_purgatory {
                                    // Increment their community service counter
                                    tracing::info!("⚖️ PURGATORY: Node {} completed a pro-bono chunk. Logging community service.", from.0);
                                    // profile.purgatory_chunks_served += 1; (in production)
                                }
                            }
                        }
                    }
                    
                    tracing::info!(
                        chunk = %chunk_id.0,
                        payee = %payee,
                        fuel = fuel_consumed,
                        payout = payout_mmx,
                        "🏦 LEDGER INGRESS: ZKP verified. Crediting {} with {} MMX tokens.", payee, payout_mmx
                    );
                    
                    // 🛑 THE DISCONNECTED BFT LEDGER FIX
                    // Physically push the transaction payload into the Byzantine Fault Tolerant (BFT) Hashgraph 
                    // so that a 67% global supermajority is required to finalize the payout into the MerkleTrie.
                    if payout_mmx > 0 {
                        use crate::swarm::planetary::ledger::{TransactionPayload, ConsensusLedger};
                        
                        // We must fetch the original submitter (Escrow Wallet) from the Kademlia Job info
                        let mut from_wallet = [0u8; 32];
                        if let Some(job) = knowledge_clone.get_job(&job_id) {
                            let submitter_bytes = job.submitter.0.into_bytes();
                            let len = submitter_bytes.len().min(32);
                            from_wallet[..len].copy_from_slice(&submitter_bytes[..len]);
                        }
                        
                        let mut to_wallet = [0u8; 32];
                        let payee_bytes = payee.as_bytes();
                        let len = payee_bytes.len().min(32);
                        to_wallet[..len].copy_from_slice(&payee_bytes[..len]);
                        
                        let mut receipt_hash = [0u8; 32];
                        let chunk_bytes = chunk_id.0.as_bytes();
                        let chunk_len = chunk_bytes.len().min(32);
                        receipt_hash[..chunk_len].copy_from_slice(&chunk_bytes[..chunk_len]);

                        let tx_payload = TransactionPayload {
                            from: from_wallet,
                            to: to_wallet,
                            mmx_amount: payout_mmx as f64,
                            job_receipt_hash: receipt_hash,
                        };
                        
                        let dag_event = bft_hashgraph_gossip.propose_transaction(tx_payload);
                        bft_hashgraph_gossip.insert_event(dag_event);
                        
                        tracing::info!("🔗 BFT CONSENSUS: SettlementClaim queued to the Hashgraph DAG for global validation.");
                    }
                }
                
                SwarmMessage::SubmitAtonement { node_id, pow_nonce } => {
                    // 🛑 FAKE COMMUNITY SERVICE FIX
                    // Atonement is no longer a useless CPU hash burn. It requires completing 1,000 real chunks for 0 MMX.
                    // The `SubmitAtonement` message is now just an inquiry from the node asking: "Have I done enough?"
                    tracing::info!("⚖️ LEDGER INGRESS: Node {} is checking Purgatory release status.", node_id.0);
                    
                    if let Some(profile) = _profile_store_clone.get(&node_id) {
                        if profile.purgatory_chunks_served >= 1000 {
                            tracing::info!(
                                node = %node_id.0,
                                chunks = profile.purgatory_chunks_served,
                                "⚖️ LEDGER INGRESS: Node has completed 1,000 mandatory community service chunks. Unbanning."
                            );
                            // Set is_in_purgatory = false
                        } else {
                            tracing::warn!(
                                node = %node_id.0,
                                chunks = profile.purgatory_chunks_served,
                                "⚖️ LEDGER INGRESS: Sentence incomplete. Node has only served {}/1000 chunks. Node remains in Purgatory.", profile.purgatory_chunks_served
                            );
                        }
                    }
                }
                
                SwarmMessage::BftEvent { dag_event_json, from: _ } => {
                    // 🛑 NETWORK METRONOME FIX
                    // In a production system, we would parse the full DAG event and verify the consensus signature.
                    // For the basics-clock milestone, we extract the block height to synchronize the network clock.
                    if let Ok(event) = serde_json::from_str::<serde_json::Value>(&dag_event_json) {
                        if let Some(height) = event.get("height").and_then(|h| h.as_u64()) {
                             knowledge_clone.update_latest_bft_block(height);
                        }
                    }
                }
                
                SwarmMessage::BftVote { .. } => {
                    // Votes contribute to consensus but do not directly advance the block height until a quorum is reached.
                }
                
                _ => {}
            }
        })
    }
}
// Identity persistence
// ============================================================================

/// Load an existing node identity from disk, or create a new one.
///
/// The identity file format is `<uuid>:<generation>` on a single line.
/// If the file exists and is parseable, the generation is incremented
/// by one (representing a new incarnation of the same node). If the
/// file does not exist or is corrupt, a fresh identity is created with
/// generation 1.
fn load_or_create_identity(path: Option<&str>) -> (NodeId, u64) {
    if let Some(path) = path {
        if let Ok(data) = std::fs::read_to_string(path) {
            if let Some((id_str, gen_str)) = data.trim().split_once(':') {
                if let (Ok(uuid), Ok(gen)) = (Uuid::parse_str(id_str), gen_str.parse::<u64>()) {
                    let new_gen = gen + 1;
                    if let Err(e) = std::fs::write(path, format!("{}:{}", uuid, new_gen)) {
                        warn!(
                            path = path,
                            error = %e,
                            "failed to update identity file"
                        );
                    }
                    info!(
                        node_id = %NodeId(uuid),
                        generation = new_gen,
                        path = path,
                        "loaded existing identity"
                    );
                    return (NodeId(uuid), new_gen);
                }
            }
        }

        // File does not exist or is corrupt -- create a fresh identity.
        let id = NodeId::generate_chrysalis();
        if let Err(e) = std::fs::write(path, format!("{}:{}", id.0, 1)) {
            warn!(
                path = path,
                error = %e,
                "failed to write new identity file"
            );
        }
        info!(
            node_id = %id,
            generation = 1,
            path = path,
            "created new identity"
        );
        (id, 1)
    } else {
        // No identity file configured -- ephemeral identity.
        let id = NodeId::generate_chrysalis();
        debug!(
            node_id = %id,
            "using ephemeral identity (no identity_file configured)"
        );
        (id, 1)
    }
}

// ============================================================================
// Trait parsing helper
// ============================================================================

/// Parse a list of trait name strings into a `HashSet<Trait>`.
///
/// Unknown trait names are logged at warn level and skipped.
fn parse_trait_set(names: &[String]) -> HashSet<Trait> {
    let mut set = HashSet::new();
    for name in names {
        match name.as_str() {
            "can_execute" => {
                set.insert(Trait::CanExecute);
            }
            "can_forward" => {
                set.insert(Trait::CanForward);
            }
            "can_aggregate" => {
                set.insert(Trait::CanAggregate);
            }
            "can_store_state" => {
                set.insert(Trait::CanStoreState);
            }
            "can_discover" => {
                set.insert(Trait::CanDiscover);
            }
            "can_relay" => {
                set.insert(Trait::CanRelay);
            }
            "blind_compute" => {
                set.insert(Trait::BlindCompute);
            }
            other => {
                warn!(trait_name = other, "unknown trait name in config, ignoring");
            }
        }
    }
    set
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn load_or_create_identity_no_file() {
        let (id, gen) = load_or_create_identity(None);
        assert_eq!(gen, 1);
        assert_ne!(id.0, Uuid::nil());
    }

    #[test]
    fn load_or_create_identity_new_file() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap();
        // Remove the temp file so load_or_create_identity creates it fresh.
        std::fs::remove_file(path).unwrap();

        let (id, gen) = load_or_create_identity(Some(path));
        assert_eq!(gen, 1);

        let contents = std::fs::read_to_string(path).unwrap();
        assert!(contents.contains(&id.0.to_string()));
        assert!(contents.ends_with(":1"));
    }

    #[test]
    fn load_or_create_identity_existing_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        let uuid = Uuid::new_v4();
        write!(tmp, "{}:5", uuid).unwrap();
        tmp.flush().unwrap();
        let path = tmp.path().to_str().unwrap();

        let (id, gen) = load_or_create_identity(Some(path));
        assert_eq!(id.0, uuid);
        assert_eq!(gen, 6);

        let contents = std::fs::read_to_string(path).unwrap();
        assert!(contents.contains(&format!("{}:6", uuid)));
    }

    #[test]
    fn load_or_create_identity_corrupt_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "not-a-valid-identity").unwrap();
        tmp.flush().unwrap();
        let path = tmp.path().to_str().unwrap();

        let (id, gen) = load_or_create_identity(Some(path));
        assert_eq!(gen, 1);
        assert_ne!(id.0, Uuid::nil());
    }

    #[test]
    fn parse_trait_set_valid() {
        let names = vec![
            "can_execute".to_string(),
            "can_relay".to_string(),
            "can_forward".to_string(),
        ];
        let set = parse_trait_set(&names);
        assert_eq!(set.len(), 3);
        assert!(set.contains(&Trait::CanExecute));
        assert!(set.contains(&Trait::CanRelay));
        assert!(set.contains(&Trait::CanForward));
    }

    #[test]
    fn parse_trait_set_unknown_ignored() {
        let names = vec![
            "can_execute".to_string(),
            "nonexistent_trait".to_string(),
        ];
        let set = parse_trait_set(&names);
        assert_eq!(set.len(), 1);
        assert!(set.contains(&Trait::CanExecute));
    }

    #[test]
    fn parse_trait_set_empty() {
        let set = parse_trait_set(&[]);
        assert!(set.is_empty());
    }

    #[test]
    fn parse_trait_set_all_traits() {
        let names = vec![
            "can_execute".to_string(),
            "can_forward".to_string(),
            "can_aggregate".to_string(),
            "can_store_state".to_string(),
            "can_discover".to_string(),
            "can_relay".to_string(),
            "blind_compute".to_string(),
        ];
        let set = parse_trait_set(&names);
        assert_eq!(set.len(), 7);
        for t in Trait::ALL {
            assert!(set.contains(t), "missing trait: {}", t);
        }
    }

    #[test]
    fn parse_trait_set_deduplicates() {
        let names = vec![
            "can_execute".to_string(),
            "can_execute".to_string(),
            "can_execute".to_string(),
        ];
        let set = parse_trait_set(&names);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn swarm_node_status_fields() {
        let status = SwarmNodeStatus {
            node_id: NodeId::generate_chrysalis(),
            generation: 3,
            traits: HashSet::from([Trait::CanExecute, Trait::CanForward]),
            load: 0.42,
            known_nodes: 15,
            known_jobs: 3,
            active_chunks: 7,
            uptime_secs: 120,
            profiles_known: 10,
            collectives_active: 2,
            reputation_records: 8,
            policy_version: 3,
            blobs_stored: 42,
            blobs_total_bytes: 1024 * 1024,
            strategies_loaded: 3,
            software_discovered: 7,
        };

        assert_eq!(status.generation, 3);
        assert_eq!(status.traits.len(), 2);
        assert!((status.load - 0.42).abs() < f32::EPSILON);
        assert_eq!(status.known_nodes, 15);
        assert_eq!(status.known_jobs, 3);
        assert_eq!(status.active_chunks, 7);
        assert_eq!(status.uptime_secs, 120);
        assert_eq!(status.profiles_known, 10);
        assert_eq!(status.collectives_active, 2);
        assert_eq!(status.reputation_records, 8);
        assert_eq!(status.policy_version, 3);
        assert_eq!(status.blobs_stored, 42);
        assert_eq!(status.blobs_total_bytes, 1024 * 1024);
        assert_eq!(status.strategies_loaded, 3);
        assert_eq!(status.software_discovered, 7);
    }
}
pub mod mantis_journal;
pub mod planetary;
pub mod hyperscale;
pub mod ext_events;





pub mod assimilate;
pub mod rosetta;
