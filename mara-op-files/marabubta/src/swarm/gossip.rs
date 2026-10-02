// Marabunta - Licensed under the MIT License.
//! Gossip protocol engine for the Marabunta Swarm.

use std::collections::{HashSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use rand::seq::SliceRandom;
use rand::Rng;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::config::{
    GOSSIP_FANOUT, GOSSIP_INTERVAL, GOSSIP_JITTER_PERCENT, GOSSIP_MIN_FANOUT,
    MAX_ASSIGNMENTS_PER_MESSAGE, MAX_JOBS_PER_MESSAGE, MAX_NODES_PER_MESSAGE,
};
use super::auth::NodeIdentity;
use super::blobstore::BlobStore;
use super::failure::WitnessStore;
use super::knowledge::KnowledgeStore;
use super::types::{
    GossipMessage, NodeId, NodeInfo, NodeStatus, ResourceSnapshot, SwarmMessage, Trait,
};

use super::profile::ProfileStore;
use super::collective::CollectiveStore;
use super::reputation::ReputationStore;
use super::policy::PolicyEngine;

#[derive(Debug, Default, Clone)]
pub struct GossipStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub nodes_updated: u64,
    pub jobs_updated: u64,
    pub assignments_updated: u64,
    pub send_errors: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub anomalies_detected: u64,
}

#[derive(Debug, Default)]
pub struct MergeResult {
    pub nodes_updated: u32,
    pub jobs_updated: u32,
    pub assignments_updated: u32,
    pub drift_rejected: bool,
}

impl MergeResult {
    pub fn total_updates(&self) -> u32 {
        self.nodes_updated + self.jobs_updated + self.assignments_updated
    }
}

#[derive(Clone)]
pub struct GossipConfig {
    pub interval: Duration,
    pub fanout: usize,
    pub jitter_percent: u32,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            interval: GOSSIP_INTERVAL,
            fanout: GOSSIP_FANOUT,
            jitter_percent: GOSSIP_JITTER_PERCENT,
        }
    }
}

pub struct GossipEngine {
    node_id: NodeId,
    generation: u64,
    knowledge: Arc<KnowledgeStore>,
    config: GossipConfig,
    stats: Arc<parking_lot::RwLock<GossipStats>>,
    outbound_tx: mpsc::Sender<(SocketAddr, SwarmMessage)>,

    profile_store: Option<Arc<ProfileStore>>,
    collective_store: Option<Arc<CollectiveStore>>,
    reputation_store: Option<Arc<ReputationStore>>,
    policy_engine: Option<Arc<PolicyEngine>>,
    blob_store: Option<Arc<BlobStore>>,
    admission_store: Option<Arc<super::admission::AdmissionStore>>,
    self_region: Option<String>,
    witness_store: Option<Arc<WitnessStore>>,
    event_bus: Option<Arc<super::events::EventBus>>,
    metrics: Option<Arc<super::metrics::SwarmMetrics>>,
    fleet_store: Option<Arc<super::fleet::FleetStore>>,
    chaos_engine: Arc<parking_lot::RwLock<Option<Arc<crate::chaos::engine::ChaosEngine>>>>,
    work_engine: Arc<parking_lot::RwLock<Option<Arc<super::work::WorkEngine>>>>,
    retrovirus_engine: Arc<parking_lot::RwLock<Option<Arc<crate::swarm::neuromancer::retrovirus::RetrovirusEngine>>>>,
    identity: Option<Arc<NodeIdentity>>,
    marabunta_identity: Option<Arc<crate::marabunta::identity::NodeIdentity>>,
    known_keys: Arc<DashMap<NodeId, Vec<u8>>>,
    pub gossip_tuning: super::config::GossipTuningConfig,
    pg_manager: Option<Arc<super::postgres::PgManager>>,
    pub plumtree: Arc<RwLock<PlumtreeManager>>,
    allow_unsigned_gossip: bool,
    zone_cert_store: Option<Arc<crate::highestsec::zone_membership::ZoneCertificateStore>>,
    key_rotation_store: Option<Arc<super::key_rotation::KeyRotationStore>>,
    current_traits: Arc<RwLock<HashSet<Trait>>>,
}

impl GossipEngine {
    pub fn set_work_engine(&self, work: Arc<super::work::WorkEngine>) {
        *self.work_engine.write() = Some(work);
    }
    pub fn set_chaos_engine(&self, chaos: Arc<crate::chaos::engine::ChaosEngine>) {
        *self.chaos_engine.write() = Some(chaos);
    }
    pub fn set_retrovirus_engine(&self, retro: Arc<crate::swarm::neuromancer::retrovirus::RetrovirusEngine>) {
        *self.retrovirus_engine.write() = Some(retro);
    }
    pub fn new(
        node_id: NodeId,
        generation: u64,
        knowledge: Arc<KnowledgeStore>,
        outbound_tx: mpsc::Sender<(SocketAddr, SwarmMessage)>,
    ) -> Self {
        Self {
            node_id,
            generation,
            knowledge,
            config: GossipConfig::default(),
            stats: Arc::new(parking_lot::RwLock::new(GossipStats::default())),
            outbound_tx,
            profile_store: None,
            collective_store: None,
            reputation_store: None,
            policy_engine: None,
            blob_store: None,
            admission_store: None,
            self_region: None,
            witness_store: None,
            event_bus: None,
            metrics: None,
            fleet_store: None,
            identity: None,
            marabunta_identity: None,
            known_keys: Arc::new(DashMap::new()),
            gossip_tuning: super::config::GossipTuningConfig::default(),
            pg_manager: None,
            chaos_engine: Arc::new(parking_lot::RwLock::new(None)),
            work_engine: Arc::new(parking_lot::RwLock::new(None)),
            retrovirus_engine: Arc::new(parking_lot::RwLock::new(None)),
            plumtree: Arc::new(RwLock::new(PlumtreeManager::new())),
            allow_unsigned_gossip: false,
            zone_cert_store: None,
            key_rotation_store: None,
            current_traits: Arc::new(RwLock::new(HashSet::new())),
        }
    }
    pub fn with_config(mut self, config: GossipConfig) -> Self {
        self.config = config;
        self
    }
    pub fn with_region(mut self, region: Option<String>) -> Self {
        self.self_region = region;
        self
    }
    pub fn with_organic_stores(
        mut self,
        profile_store: Arc<ProfileStore>,
        collective_store: Arc<CollectiveStore>,
        reputation_store: Arc<ReputationStore>,
        policy_engine: Arc<PolicyEngine>,
    ) -> Self {
        self.profile_store = Some(profile_store);
        self.collective_store = Some(collective_store);
        self.reputation_store = Some(reputation_store);
        self.policy_engine = Some(policy_engine);
        self
    }
    pub fn with_metrics(mut self, metrics: Arc<super::metrics::SwarmMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_gossip_tuning(mut self, tuning: super::config::GossipTuningConfig) -> Self {
        self.gossip_tuning = tuning;
        self
    }
    pub fn with_admission_store(mut self, store: Arc<super::admission::AdmissionStore>) -> Self {
        self.admission_store = Some(store);
        self
    }
    pub fn with_witness_store(mut self, store: Arc<WitnessStore>) -> Self {
        self.witness_store = Some(store);
        self
    }
    pub fn with_blob_store(mut self, store: Arc<BlobStore>) -> Self {
        self.blob_store = Some(store);
        self
    }
    pub fn with_identity(mut self, identity: Arc<NodeIdentity>) -> Self {
        self.identity = Some(identity);
        self
    }
    pub fn with_marabunta_identity(mut self, identity: Arc<crate::marabunta::identity::NodeIdentity>) -> Self {
        self.marabunta_identity = Some(identity);
        self
    }
    pub fn with_event_bus(mut self, bus: Arc<super::events::EventBus>) -> Self {
        self.event_bus = Some(bus);
        self
    }
    pub fn with_fleet_store(mut self, store: Arc<super::fleet::FleetStore>) -> Self {
        self.fleet_store = Some(store);
        self
    }
    pub fn with_pg_manager(mut self, mgr: Arc<super::postgres::PgManager>) -> Self {
        self.pg_manager = Some(mgr);
        self
    }
    pub fn with_allow_unsigned_gossip(mut self, allow: bool) -> Self {
        self.allow_unsigned_gossip = allow;
        self
    }
    pub fn with_zone_cert_store(mut self, store: Arc<crate::highestsec::zone_membership::ZoneCertificateStore>) -> Self {
        self.zone_cert_store = Some(store);
        self
    }
    pub fn with_key_rotation_store(mut self, store: Arc<super::key_rotation::KeyRotationStore>) -> Self {
        self.key_rotation_store = Some(store);
        self
    }

    pub fn build_message(
        &self,
        my_traits: &HashSet<Trait>,
        my_load: f32,
        my_capacity: &ResourceSnapshot,
        is_backbone: bool,
    ) -> GossipMessage {
        let is_training = self.work_engine.read().as_ref().map(|we: &Arc<crate::swarm::work::WorkEngine>| we.is_training()).unwrap_or(false);
        let is_pgwire_active = self.work_engine.read().as_ref().map(|we| we.is_pgwire_active.load(std::sync::atomic::Ordering::SeqCst)).unwrap_or(false);
        let chaos_state = self.chaos_engine.read().as_ref().map(|ce| ce.get_state()).unwrap_or_default();
        
        let known_nodes = if is_backbone { self.knowledge.sample_nodes(5) } else { self.knowledge.sample_nodes(MAX_NODES_PER_MESSAGE) };
        let known_jobs = if is_backbone { self.knowledge.sample_jobs(5) } else { self.knowledge.sample_jobs(MAX_JOBS_PER_MESSAGE) };
        let known_assignments = if is_backbone { self.knowledge.sample_assignments(0) } else { self.knowledge.sample_assignments(MAX_ASSIGNMENTS_PER_MESSAGE) };
        let known_topologies = if is_backbone { self.knowledge.sample_topologies(5) } else { self.knowledge.sample_topologies(50) };

        let mut message = GossipMessage {
            sender_id: self.node_id,
            timestamp: Utc::now(),
            generation: self.generation,
            my_traits: my_traits.clone(),
            my_load,
            my_capacity: my_capacity.clone(),
            is_training,
            is_pgwire_active,
            chaos_state,
            known_nodes,
            known_jobs,
            known_assignments,
            known_topologies,
            ..Default::default()
        };

        if let Some(ref identity) = self.marabunta_identity {
            message.sender_public_key = identity.dilithium_public_key().to_vec();
            message.envelope_signature = super::auth::sign_gossip(identity, &message.dilithium_id, &message.timestamp, message.generation);
        }
        message
    }

    pub fn handle_gossip(&self, message: GossipMessage) -> MergeResult {
        let mut result = MergeResult::default();
        if !message.sender_id.meets_chrysalis_pow() { return result; }
        if let Some(ref m) = self.metrics { m.record_gossip_received(); }

        let sender_info = NodeInfo {
            node_id: message.sender_id,
            last_seen: message.timestamp,
            traits: message.my_traits,
            load: message.my_load,
            capacity: message.my_capacity,
            address: None,
            via: message.sender_id,
            status: NodeStatus::Alive,
            generation: message.generation,
            trust_level: super::types::TrustLevel::Direct,
            failure_domains: vec![],
            attestation: crate::swarm::types::LocationAttestation::SelfAttested,
            is_training: message.is_training,
            is_pgwire_active: message.is_pgwire_active,
            chaos_state: message.chaos_state.clone(),
        };

        if self.knowledge.merge_node(sender_info) { result.nodes_updated += 1; }
        for mut node_info in message.known_nodes {
            node_info.trust_level = super::types::TrustLevel::Hearsay;
            if self.knowledge.merge_node(node_info) { result.nodes_updated += 1; }
        }
        for job_info in message.known_jobs { if self.knowledge.merge_job(job_info) { result.jobs_updated += 1; } }
        for assignment in message.known_assignments { if self.knowledge.merge_assignment(assignment) { result.assignments_updated += 1; } }
        for topology in message.known_topologies { self.knowledge.merge_topology(topology); }

        {
            let mut stats = self.stats.write();
            stats.messages_received += 1;
            stats.nodes_updated += result.nodes_updated as u64;
            stats.jobs_updated += result.jobs_updated as u64;
            stats.assignments_updated += result.assignments_updated as u64;
        }
        result
    }

    pub fn effective_fanout(&self) -> usize {
        let node_count = self.knowledge.node_count();
        if node_count <= 1 { return GOSSIP_MIN_FANOUT; }
        let log2 = (node_count as f64).log2().ceil() as usize;
        log2.max(GOSSIP_MIN_FANOUT)
    }

    pub fn select_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        let my_traits = self.current_traits.read();
        let is_hub = my_traits.contains(&Trait::CanRelay);
        let fanout = self.effective_fanout();

        // 🛑 GOSSIP NETWORK BOMB FIX:
        // Do not fetch the entire live_nodes list and clone it. 
        // Just grab `fanout * 4` candidates using efficient randomized sampling.
        let mut candidates = self.knowledge.get_random_peers(&self.node_id, fanout * 4);

        if candidates.is_empty() { return Vec::new(); }

        let my_region = self.self_region.as_deref();

        let mut same_region = Vec::new();
        let mut other_region = Vec::new();

        for candidate in candidates.into_iter() {
            let peer_id = candidate.0;
            let p_region = self.profile_store.as_ref().and_then(|ps| ps.get(&peer_id)).and_then(|p| p.geo_region.clone());
            if my_region.is_some() && p_region.as_deref() == my_region { same_region.push(candidate); }
            else { other_region.push(candidate); }
        }

        let first_other = other_region.first().cloned();

        let mut result = Vec::with_capacity(fanout);
        if !is_hub {
            result.extend(same_region.into_iter().take(fanout));
        } else {
            let global_quota = (fanout as f32 * 0.2).ceil() as usize;
            let local_quota = fanout.saturating_sub(global_quota);
            result.extend(same_region.into_iter().take(local_quota));
            result.extend(other_region.into_iter().take(global_quota));
        }

        if result.is_empty() {
            if let Some(other) = first_other {
                result.push(other);
            }
        }

        result
    }

    pub async fn gossip_once(&self, my_traits: &HashSet<Trait>, my_load: f32, my_capacity: &ResourceSnapshot) -> u32 {
        let peers = self.select_peers();
        if peers.is_empty() { return 0; }
        let local_message = self.build_message(my_traits, my_load, my_capacity, false);
        let backbone_message = self.build_message(my_traits, my_load, my_capacity, true);
        let local_bytes = serde_json::to_vec(&local_message).map(|v| v.len() as u64).unwrap_or(0);
        let backbone_bytes = serde_json::to_vec(&backbone_message).map(|v| v.len() as u64).unwrap_or(0);
        let mut sent = 0;
        let mut total_bytes_sent = 0;
        for (peer_id, addr) in &peers {
            let p_region = self.profile_store.as_ref().and_then(|ps| ps.get(peer_id)).and_then(|p| p.geo_region.clone());
            let is_backbone = p_region.as_deref() != self.self_region.as_deref();
            let (msg, bytes) = if is_backbone { (backbone_message.clone(), backbone_bytes) } else { (local_message.clone(), local_bytes) };
            total_bytes_sent += bytes;
            if self.outbound_tx.send((*addr, SwarmMessage::Gossip(msg))).await.is_ok() {
                sent += 1;
            }
        }
        {
            let mut stats = self.stats.write();
            stats.messages_sent += sent as u64;
            stats.bytes_sent += total_bytes_sent;
        }
        sent
    }

    pub fn spawn_loop(
        self: Arc<Self>,
        traits: Arc<RwLock<HashSet<Trait>>>,
        load: Arc<RwLock<f32>>,
        capacity: Arc<RwLock<ResourceSnapshot>>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let engine = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
                }
                let current_traits = traits.read().clone();
                let current_load = *load.read();
                let current_capacity = capacity.read().clone();
                engine.gossip_once(&current_traits, current_load, &current_capacity).await;
            }
        })
    }
    pub fn stats(&self) -> GossipStats { self.stats.read().clone() }
}

#[derive(Default)]
pub struct PlumtreeManager {
    pub eager_push: HashSet<NodeId>,
    pub lazy_push: HashSet<NodeId>,
    pub history: HashMap<[u8; 32], Instant>,
    pub backoff_tracker: HashMap<NodeId, (u32, Instant)>,
}

impl PlumtreeManager {
    pub fn new() -> Self { Self::default() }
    pub fn handle_message(&mut self, msg_id: [u8; 32], from: NodeId) -> Vec<NodeId> {
        self.backoff_tracker.remove(&from);
        if self.history.contains_key(&msg_id) {
            if self.eager_push.contains(&from) { self.eager_push.remove(&from); self.lazy_push.insert(from); }
            return vec![];
        }
        self.history.insert(msg_id, Instant::now());
        self.eager_push.insert(from);
        self.eager_push.iter().filter(|&&p| p != from).cloned().collect()
    }
}
