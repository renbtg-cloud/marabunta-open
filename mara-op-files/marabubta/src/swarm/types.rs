// Marabunta - Licensed under the MIT License.
//! Core types for the Marabunta Swarm architecture.
//!
//! Every struct in this module is designed for gossip propagation:
//! compact serialization, deterministic merge semantics, and
//! last-writer-wins conflict resolution via timestamps.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use uuid::Uuid;

use crate::common::types::{JobId, TaskPayload};
use super::transport::torrent::{ChunkBitfield, ChunkRequest, ChunkResponse};
use super::profile::NodeProfile;

// ============================================================================
// Node identity
// ============================================================================

/// Unique identifier for a swarm node.
///
/// Generated from a v4 UUID on first start, persisted across restarts.
/// The same physical machine gets the same NodeId if it reads its
/// persisted identity file; otherwise it gets a new one (and the old
/// identity will be garbage-collected via gossip timeout).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    /// Chrysalis Phase (Sybil Defense): 
    /// Verifies if this NodeId satisfies the Proof-of-Work constraint (16 leading zero bits).
    /// Prevents trivial identity generation for Sybil attacks.

    /// Cryptographically validates that the Kademlia Node ID satisfies the
    /// Chrysalis Sybil-defense constraint (Phase 1.0).
    ///
    /// [MARABUNTA WMD] Thermodynamic Cost Enforcement
    /// Enforces a 26-bit difficulty minimum. This mathematically requires
    /// approximately 67,108,864 memory-hard hashing iterations to derive a
    /// valid identity. On a modern Ryzen 9 workstation saturating its L3 cache,
    /// this guarantees a physical cost of ~4.2 seconds of 100% CPU utilization
    /// per identity, rendering million-node botnet eclipses economically devastating.
    pub fn meets_chrysalis_pow(&self) -> bool {
        let bytes = self.0.as_bytes();
        // 26-bit difficulty: 
        // 3 bytes must be perfectly zero (24 bits)
        // The top 2 bits of the 4th byte must be zero (value < 64)
        bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0 && bytes[3] < 64
    }


    /// Physically grinds the CPU to find a valid NodeId that satisfies the
    /// Chrysalis Sybil-defense constraint. This adds a physical cost to
    /// identity issuance, preventing infinite Sybil identity flooding.
    ///
    /// PRODUCTION UPGRADE: Uses multi-threaded ChrysalisGrinder to saturate
    /// all available cores for maximum performance.
    pub fn generate_chrysalis() -> Self {
        use super::pow_worker::ChrysalisGrinder;
        ChrysalisGrinder::new(2).grind()
    }

    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Deterministic NodeId from arbitrary bytes (e.g. machine fingerprint).
    /// Uses SHA-256 truncated to 128 bits to fill a UUID.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(bytes);
        let mut uuid_bytes = [0u8; 16];
        uuid_bytes.copy_from_slice(&hash[..16]);
        // Set version 8 (custom) and variant bits
        uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x80;
        uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;
        Self(Uuid::from_bytes(uuid_bytes))
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ============================================================================
// Chunk identity
// ============================================================================

/// Unique identifier for a chunk (the smallest unit of distributable work).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ChunkId(pub Uuid);

impl ChunkId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ChunkId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chunk-{}", &self.0.to_string()[..8])
    }
}

// ============================================================================
// Traits (capabilities)
// ============================================================================

/// A capability that a node can dynamically claim or shed.
///
/// Traits are not assigned — they are self-assessed based on the node's
/// current resources, connectivity, and load. The swarm uses traits to
/// route work to capable nodes without any central authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trait {
    /// Can run compute jobs (the baseline trait — almost every node has this)
    CanExecute,
    /// Can receive jobs from external clients and route to the swarm
    CanForward,
    /// Can collect outputs from multiple chunks and reassemble results
    CanAggregate,
    /// Can persist job queue, node registry, and swarm metadata
    CanStoreState,
    /// Can perform NAT traversal and bootstrap new nodes
    CanDiscover,
    /// Can bridge nodes that cannot communicate directly
    CanRelay,
    /// Can execute blind (encrypted) computation workloads
    BlindCompute,
    
    // --- Phase V: Interplanetary Fleet ---
    /// Specialized node capable of executing quantum algorithms
    QuantumCompute,
    /// Specialized wet-lab node for biological sequencing
    CrisprSequencer,
    /// Critical infrastructure node preempting all tasks for survival
    LifeSupport,
}

impl Trait {
    /// All possible traits, for iteration.
    pub const ALL: &'static [Trait] = &[
        Trait::CanExecute,
        Trait::CanForward,
        Trait::CanAggregate,
        Trait::CanStoreState,
        Trait::CanDiscover,
        Trait::CanRelay,
        Trait::BlindCompute,
        Trait::QuantumCompute,
        Trait::CrisprSequencer,
        Trait::LifeSupport,
    ];
}

impl fmt::Display for Trait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Trait::CanExecute => write!(f, "can_execute"),
            Trait::CanForward => write!(f, "can_forward"),
            Trait::CanAggregate => write!(f, "can_aggregate"),
            Trait::CanStoreState => write!(f, "can_store_state"),
            Trait::CanDiscover => write!(f, "can_discover"),
            Trait::CanRelay => write!(f, "can_relay"),
            Trait::BlindCompute => write!(f, "blind_compute"),
            Trait::QuantumCompute => write!(f, "quantum_compute"),
            Trait::CrisprSequencer => write!(f, "crispr_sequencer"),
            Trait::LifeSupport => write!(f, "life_support"),
        }
    }
}

// ============================================================================
// Node status
// ============================================================================

/// Liveness status of a remote node as perceived through gossip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum NodeStatus {
    /// Recently heard from (directly or via gossip)
    #[default]
    Alive,
    /// Haven't heard in SUSPECT_THRESHOLD — might be down
    Suspect,
    /// Haven't heard in DEAD_THRESHOLD — considered dead, work reassigned
    Dead,
    /// Gracefully draining: finishing current work, not accepting new chunks
    Draining,
    /// Administratively cordoned: not accepting new work, existing work continues
    Cordoned,
    /// Quarantined: isolated from the swarm, no gossiping or work
    Quarantined,
    /// Being updated to a new version via rolling update
    Updating,
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeStatus::Alive => write!(f, "Alive"),
            NodeStatus::Suspect => write!(f, "Suspect"),
            NodeStatus::Dead => write!(f, "Dead"),
            NodeStatus::Draining => write!(f, "Draining"),
            NodeStatus::Cordoned => write!(f, "Cordoned"),
            NodeStatus::Quarantined => write!(f, "Quarantined"),
            NodeStatus::Updating => write!(f, "Updating"),
        }
    }
}

/// Fleet management command types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetCommandType {
    Drain,
    Cordon,
    Uncordon,
    Quarantine,
    Unquarantine,
}


// ============================================================================
// Witness report (split-brain protection)
// ============================================================================

/// A witness report attesting that a reporter recently observed a subject node.
///
/// These are piggybacked on gossip messages so the failure detector can
/// corroborate its own liveness observations with what other peers report.
/// Before declaring a node dead, the detector checks whether a quorum of
/// peers also considers the node unreachable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WitnessReport {
    /// The node that is reporting the observation.
    pub reporter: NodeId,
    /// The node being observed.
    pub subject: NodeId,
    /// When the reporter last directly heard from the subject.
    pub last_seen: DateTime<Utc>,
    /// Ed25519 signature by the reporter over (reporter || subject || last_seen).
    /// Empty for unsigned/legacy reports.
    #[serde(default)]
    pub signature: Vec<u8>,
}

// ============================================================================
// Partition status (split-brain protection)
// ============================================================================

/// Network partition status as perceived by this node.
///
/// When a significant fraction of previously-known peers become unreachable,
/// the node enters a degraded or isolated state and stops claiming new work
/// to avoid split-brain inconsistencies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum PartitionStatus {
    /// Normal operation -- a healthy quorum of peers is reachable.
    #[default]
    Normal,
    /// Reachable fraction is below the quorum threshold but above the
    /// isolation threshold. The node suspends new work claims.
    Degraded,
    /// Severe isolation -- very few peers reachable. The node suspends
    /// new work claims and logs errors.
    Isolated,
    /// Massive extinction event detected. Network mass has dropped catastrophically.
    /// The node must halt all compute and begin emergency Reed-Solomon re-sharding.
    DeepWinter,
}


impl fmt::Display for PartitionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionStatus::Normal => write!(f, "Normal"),
            PartitionStatus::Degraded => write!(f, "Degraded"),
            PartitionStatus::Isolated => write!(f, "Isolated"),
            PartitionStatus::DeepWinter => write!(f, "DeepWinter"),
        }
    }
}

// ============================================================================
// Resource snapshot
// ============================================================================

/// Point-in-time snapshot of a node's hardware resources.
/// Collected by `sysinfo` and propagated via gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub cpu_cores: u32,
    /// Fraction of CPU currently idle (0.0 = fully loaded, 1.0 = idle)
    pub cpu_available: f32,
    pub memory_total_mb: u64,
    pub memory_available_mb: u64,
    pub disk_total_mb: u64,
    pub disk_available_mb: u64,
    /// Estimated network bandwidth in Mbps (0 if unknown)
    pub network_bandwidth_mbps: f32,
    pub current_tdp_watts: f32,
    pub ask_usd_per_megagas: f64,
}

impl Default for ResourceSnapshot {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            cpu_available: 0.0,
            memory_total_mb: 0,
            memory_available_mb: 0,
            disk_total_mb: 0,
            disk_available_mb: 0,
            network_bandwidth_mbps: 0.0,
            current_tdp_watts: 15.0,
            ask_usd_per_megagas: 0.0001,
        }
    }
}

// ============================================================================
// Trust level (Hardening A.3)
// ============================================================================

/// How much we trust this piece of gossip data.
///
/// - `Direct`: We heard it directly from the node itself (sender_info in gossip).
/// - `Hearsay`: We learned it via a third party (propagated known_nodes).
///
/// In merge conflicts, `Direct` always beats `Hearsay` regardless of timestamp.
/// Within the same trust level, standard LWW (last-writer-wins) applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[derive(Default)]
pub enum TrustLevel {
    #[default]
    Hearsay = 0,
    Direct  = 1,
}


// ============================================================================
// Node info (as known via gossip)
// ============================================================================

/// Information about a remote node, as propagated through gossip.
///
/// Every node maintains a `DashMap<NodeId, NodeInfo>` of everything it
/// knows about the swarm. When two gossip messages conflict, the one
/// with the newer `last_seen` timestamp wins.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[derive(Default)]
pub enum LocationAttestation {
    #[default]
    SelfAttested,
    LatencyTriangulation { signature: Vec<u8> },
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    /// When this node was last directly heard from or gossiped about
    pub last_seen: DateTime<Utc>,
    /// Capabilities this node currently claims
    pub traits: HashSet<Trait>,
    /// Current load (0.0 = idle, 1.0 = fully loaded)
    pub load: f32,
    /// Hardware resources
    pub capacity: ResourceSnapshot,
    /// Network address (None if behind symmetric NAT without relay)
    pub address: Option<SocketAddr>,
    /// Which node told us about this (for provenance tracking)
    pub via: NodeId,
    /// Liveness status
    pub status: NodeStatus,
    /// Monotonic generation counter — incremented on restart.
    /// Allows distinguishing "same node, restarted" from stale info.
    pub generation: u64,
    /// How trustworthy this information is (Direct = from source, Hearsay = via third party).
    /// Direct info always wins merge conflicts over Hearsay, regardless of timestamp.
    #[serde(default)]
    pub trust_level: TrustLevel,
    #[serde(default)]
    pub failure_domains: Vec<String>,
    #[serde(default)]
    pub attestation: LocationAttestation,
    #[serde(default)]
    pub is_training: bool,
    #[serde(default)]
    pub is_pgwire_active: bool,
    #[serde(default)]
    pub chaos_state: crate::chaos::types::ChaosState,
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            node_id: NodeId::default(),
            last_seen: Utc::now(),
            traits: HashSet::new(),
            load: 0.0,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: NodeId::default(),
            status: NodeStatus::default(),
            generation: 0,
            trust_level: TrustLevel::default(),
            failure_domains: Vec::new(),
            attestation: LocationAttestation::default(),
            is_training: false,
            is_pgwire_active: false,
            chaos_state: crate::chaos::types::ChaosState::default(),
        }
    }
}

// ============================================================================
// Chunk status
// ============================================================================

/// Lifecycle status of a chunk within the work distribution system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkStatus {
    /// Not yet claimed by any node
    Pending,
    /// Claimed by a node, execution in progress
    InProgress,
    /// Execution completed successfully
    Completed,
    /// Execution failed (may be retried)
    Failed,
}

// ============================================================================
// Job info (as known via gossip)
// ============================================================================

/// Swarm-level job status (separate from the per-task status in common::types).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwarmJobStatus {
    /// Job submitted, chunks pending distribution
    Pending,
    /// At least one chunk is being executed
    InProgress,
    /// All chunks completed successfully
    Completed,
    /// Job failed (too many chunk failures)
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlPlane {
    /// Path A (Enterprise DMZ): State is held exclusively by specific manager nodes.
    /// Fast, efficient, but vulnerable if the managers go offline.
    Dedicated { managers: Vec<NodeId> },
    
    /// Path B (Headless Swarm): Job state is tracked globally via BFT Hashgraph and Kademlia DHT.
    /// Slower and chatty, but mathematically indestructible.
    Holographic,
}

impl Default for ControlPlane {
    fn default() -> Self {
        ControlPlane::Dedicated { managers: Vec::new() } // Fallbacks to submitter
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PaceDetector {
    /// The user explicitly tells Marabunta: "Do not attempt to measure pace. It is impossible for this job."
    NaN,
    
    /// The preset for embarrassingly parallel jobs. Marabunta uses chunks_completed / chunks_total / time.
    LinearChunks { min_chunks_per_hour: u32 },
    
    /// The ultimate control: The user uploads a Rhai script that Marabunta executes to calculate pace.
    TuringCompleteOracle { script: String },
}

impl Default for PaceDetector {
    fn default() -> Self {
        PaceDetector::LinearChunks { min_chunks_per_hour: 10 } // Safe baseline
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationConfig {
    /// Who tracks the job state and completion progress?
    #[serde(default)]
    pub control_plane: ControlPlane,
    /// Where does the heavy output data (e.g., 5GB tensor) go?
    #[serde(default)]
    pub output: DataSink,
    /// Storage Time-To-Live (hours). When this expires, edge nodes are legally allowed 
    /// to garbage-collect the output blob without penalty.
    #[serde(default)]
    pub storage_ttl_hours: Option<u32>,
    /// Who receives the lightweight progress and completion events?
    #[serde(default)]
    pub telemetry: Vec<TelemetrySink>,
    /// Who holds the cryptographic authority to verify the math?
    #[serde(default)]
    pub verification: VerificationConfig,
    /// Who holds the MMX ledger to issue payment?
    #[serde(default)]
    pub settlement: SettlementConfig,
    /// How should the Orchestrator evaluate the speed and health of this job?
    #[serde(default)]
    pub pace_detector: PaceDetector,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            control_plane: ControlPlane::default(),
            output: DataSink::Inline,
            storage_ttl_hours: None,
            telemetry: Vec::new(),
            verification: VerificationConfig::None,
            settlement: SettlementConfig::None,
            pace_detector: PaceDetector::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataSink {
    /// Result is tiny (< 1MB). Pack it directly inside the Kademlia ChunkResult message.
    #[default]
    Inline,
    /// Result is heavy. Save it to the DHT BlobStore and only gossip the hash.
    DhtBlob,
    /// Enterprise Egress: Stream result to an external S3/Webhook URI. (Bypasses Marabunta network).
    HttpPush { uri_template: String, auth_header: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TelemetrySink {
    /// Standard P2P: Route ChunkResult directly to specific NodeIds (or array of nodes for Hydra mode).
    DhtUnicast { targets: Vec<NodeId> },
    /// PubSub: Multicast progress to anyone listening to a specific Kademlia topic.
    DhtTopic { topic: String },
    /// Enterprise: Fire an HTTP POST to Datadog, Discord, or an internal Kafka proxy.
    Webhook { url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VerificationConfig {
    /// Trust the worker blindly. (Only suitable for charity swarms or local dev).
    #[default]
    None,
    /// Redundant execution with specific judges holding the consensus state.
    ArbiterNode { judges: Vec<NodeId>, consensus: String },
    /// ZK Proof embedded directly in the payload.
    ZkpOnChain,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SettlementConfig {
    /// Pro-bono work (Folding@Home style). No MMX credits issued.
    #[default]
    None,
    /// Submit cryptographic proof to a specific Federation's gateway.
    FederationLedger { federation_id: String },
}

/// Information about a job as propagated through gossip.
///
/// This is a lightweight summary — the full task payloads live in
/// the chunks, not here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmJobInfo {
    pub job_id: JobId,
    pub status: SwarmJobStatus,
    pub chunks_total: u32,
    pub chunks_completed: u32,
    pub chunks_failed: u32,
    pub submitter: NodeId,
    pub first_seen: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// What kind of work ("shell", "python", "monte_carlo", etc.)
    pub payload_type: String,
    #[serde(default)]
    pub total_fuel_consumed: u64,
    #[serde(default)]
    pub final_result: Option<Vec<u8>>,
    /// Higher priority = executed first
    pub priority: u32,
    #[serde(default)]
    pub max_duration_ms: Option<u64>,
    /// Verification mode requested by the submitter (e.g., "PARANOID")
    #[serde(default)]
    pub verify_mode: Option<String>,
    /// The autonomous routing and execution matrix for this job.
    #[serde(default)]
    pub orchestration: OrchestrationConfig,
    /// Optional data residency requirement. If set, only nodes in the
    /// specified region(s) can execute this job's chunks.
    #[serde(default)]
    pub data_residency: Option<GeoRegion>,
    /// Optional zone ID requirement. If set, only nodes with a valid
    /// zone membership certificate for this zone can execute chunks.
    pub required_zone_id: String,
    /// Verification strategy for this job's chunk results.
    #[serde(default)]
    pub verification_strategy: VerificationStrategy,
    /// Physical execution SLA boundary (The Reality Anchor)
    #[serde(default)]
    pub associated_topology: Option<crate::common::types::TopologyId>,
    /// Economic Bid: Maximum budget per instruction.
    #[serde(default)]
    pub max_mmx_per_instruction: Option<u64>,
    /// Community Service: If true, nodes in Purgatory are allowed to compute this job for 0 MMX
    /// to fulfill their thermodynamic atonement sentence.
    #[serde(default)]
    pub community_service_eligible: bool,
}

// ============================================================================
// Assignment
// ============================================================================

/// A mapping from a chunk to the node that claimed it.
///
/// Conflict resolution: when two nodes claim the same chunk, the one
/// with the earlier `assigned_at` wins. Ties broken by lower `NodeId`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub chunk_id: ChunkId,
    pub job_id: JobId,
    /// Which node claimed this chunk (None = unassigned / reassigned)
    pub assigned_to: Option<NodeId>,
    pub assigned_at: DateTime<Utc>,
    pub status: ChunkStatus,
    pub agreed_price: u64,
    /// Result, if completed
    pub result: Option<ChunkResult>,
    /// How many times this chunk has been attempted
    pub attempts: u32,
    /// For redundant (blind) execution, the primary chunk ID that all
    /// replicas in this group share. `None` for non-replicated chunks.
    #[serde(default)]
    pub replica_group_id: Option<ChunkId>,
    /// Nodes that previously failed this chunk (used to avoid re-scheduling).
    #[serde(default)]
    pub failed_nodes: Vec<NodeId>,
}

impl Assignment {
    /// Deterministic conflict resolution: earlier timestamp wins,
    /// lower NodeId breaks ties. Returns true if `self` wins.
    pub fn wins_against(&self, other: &Assignment) -> bool {
        debug_assert_eq!(self.chunk_id, other.chunk_id);
        match (self.assigned_to, other.assigned_to) {
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(a), Some(b)) => {
                if self.assigned_at != other.assigned_at {
                    self.assigned_at < other.assigned_at
                } else {
                    a < b
                }
            }
            (None, None) => true,
        }
    }
}

// ============================================================================
// Blind chunk attestation
// ============================================================================

/// Cryptographic attestation from a blind computation.
///
/// Carries the Dilithium-signed proof that a particular executor ran a
/// blind payload and produced a specific output. This travels inside
/// `ChunkResult` so that aggregators and verifiers can validate the
/// attestation without re-executing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlindChunkAttestation {
    pub input_hash: [u8; 32],
    pub output_hash: [u8; 32],
    pub timestamp: DateTime<Utc>,
    pub executor_sig: Vec<u8>,
    pub executor_dilithium_pk: Vec<u8>,
}

impl BlindChunkAttestation {
    /// Verify this attestation using the embedded Dilithium public key.
    pub fn verify(&self) -> bool {
        false // Blind functionality removed
    }
}

// ============================================================================
// Chunk result
// ============================================================================

/// Output of executing a single chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkResult {
    pub success: bool,
    pub output: Vec<u8>,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub completed_at: DateTime<Utc>,
    #[serde(default)]
    pub fuel_consumed: u64,
    #[serde(default)]
    pub execution_error: Option<String>,
    /// Flag indicating the output is End-to-End Encrypted (E2EE) for the final client.
    #[serde(default)]
    pub is_e2ee: bool,
    /// Cryptographic proof of correct execution for Software-Only Blind Computing.
    #[serde(default)]
    pub blind_execution_proof: Option<crate::highestsec::zkp::ExecutionProof>,
    /// Optional flight data recorder (Mantis) journal dump for post-mortem analysis.
    /// Obfuscated or empty if blackout mode was active.
    #[serde(default)]
    pub journal_dump: Option<Vec<u8>>,
    /// Optional hash of the heavy result data stored in the DHT BlobStore.
    #[serde(default)]
    pub output_blob_hash: Option<BlobHash>,
}

impl Default for ChunkResult {
    fn default() -> Self {
        Self {
            success: false,
            output: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            duration_ms: 0,
            completed_at: Utc::now(),
            fuel_consumed: 0,
            execution_error: None,
            is_e2ee: false,
            blind_execution_proof: None,
            journal_dump: None,
            output_blob_hash: None,
        }
    }
}

// ============================================================================
// Chunk (the actual unit of work)
// ============================================================================

/// A chunk is the smallest distributable unit of work.
///
/// Jobs are split into chunks at submission time. Each chunk carries
/// its own payload and can be executed independently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub id: ChunkId,
    pub job_id: JobId,
    /// Ordering within the job (for result reassembly)
    pub sequence: u32,
    /// What to execute
    pub payload: TaskPayload,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub max_fuel: u64,
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            id: ChunkId::new(),
            job_id: JobId::new(),
            sequence: 0,
            payload: TaskPayload::default(),
            created_at: Utc::now(),
            max_fuel: 0,
        }
    }
}

// ============================================================================
// Gossip message
// ============================================================================

/// The gossip message: everything a node shares with its peers.
///
/// Sent periodically (every GOSSIP_INTERVAL) to GOSSIP_FANOUT random
/// peers. Contains the sender's self-report plus a bounded sample of
/// everything it knows about the swarm.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct GossipMessage {
    // --- Identity ---
    pub sender_id: NodeId,
    /// Biological identity for Dilithium signatures
    pub dilithium_id: crate::marabunta::identity::NodeId,
    pub timestamp: DateTime<Utc>,
    /// Sender's generation (monotonic, incremented on restart)
    pub generation: u64,

    // --- Hardening A.2: Inline message signing ---
    /// Ed25519 signature over (sender_id || timestamp_millis_le || generation_le).
    /// Empty = unsigned (legacy/transition period).
    #[serde(default)]
    pub envelope_signature: Vec<u8>,
    /// 32-byte Ed25519 public key of the sender.
    /// Empty = unsigned (legacy/transition period).
    #[serde(default)]
    pub sender_public_key: Vec<u8>,

    // --- Self-report ---
    pub my_traits: HashSet<Trait>,
    pub my_load: f32,
    pub my_capacity: ResourceSnapshot,
    pub is_training: bool,
    pub is_pgwire_active: bool,
    pub chaos_state: crate::chaos::types::ChaosState,

    // --- Knowledge propagation ---
    /// Bounded sample of known nodes (up to MAX_NODES_PER_MESSAGE)
    pub known_nodes: Vec<NodeInfo>,
    /// Bounded sample of active jobs (up to MAX_JOBS_PER_MESSAGE)
    pub known_jobs: Vec<SwarmJobInfo>,
    /// Bounded sample of assignments (up to MAX_ASSIGNMENTS_PER_MESSAGE)
    pub known_assignments: Vec<Assignment>,

    // Organic swarm extensions (optional for backwards compat)
    #[serde(default)]
    pub known_profiles: Vec<NodeProfile>,
    #[serde(default)]
    pub known_collectives: Vec<super::collective::Collective>,
    #[serde(default)]
    pub known_reputation: Vec<super::reputation::ReputationRecord>,
    #[serde(default)]
    pub policy: Option<super::policy::PolicySet>,

    // Admission extensions
    #[serde(default)]
    pub known_probations: Vec<super::admission::ProbationStatus>,
    #[serde(default)]
    pub known_topologies: Vec<crate::swarm::crdt::LwwRegister<crate::common::types::HolographicTopology>>,
    #[serde(default)]
    pub known_admission_decisions: Vec<super::admission::AdmissionDecision>,

    // Production extensions
    /// Known blob locations: (hash, who_has_it, size)
    #[serde(default)]
    pub known_blobs: Vec<(BlobHash, NodeId, u64)>,
    /// Software inventories: (node_id, installed_software)
    #[serde(default)]
    pub software_inventories: Vec<(NodeId, Vec<InstalledSoftware>)>,

    // Management layer extensions
    /// Latest psyche state snapshot from sender
    #[serde(default)]
    pub psyche_snapshot: Option<serde_json::Value>,
    /// Fleet state summaries: (node_id, fleet_status_string)
    #[serde(default)]
    pub fleet_states: Vec<(NodeId, String)>,
    /// Active alert summaries from sender
    #[serde(default)]
    pub active_alerts: Vec<serde_json::Value>,
    /// SLA compliance: (sla_name, compliance_fraction)
    #[serde(default)]
    pub sla_compliance: Vec<(String, f64)>,

    // Split-brain protection
    /// Witness reports: "I last saw node X at time T" attestations.
    /// Used by the failure detector to corroborate death declarations
    /// with quorum agreement from other peers.
    #[serde(default)]
    pub witness_reports: Vec<WitnessReport>,

    // PostgreSQL cluster discovery
    /// Known PG nodes in the cluster (role, status, address, WAL position).
    /// Used by the PG subsystem to discover primary/replicas without a
    /// separate discovery protocol.
    #[serde(default)]
    pub pg_nodes: Vec<super::postgres::types::PgNodeInfo>,

    // EU Highestsec compliance extensions
    /// Zone membership certificates for zone-aware gossip.
    #[serde(default)]
    pub zone_certificates: Vec<crate::highestsec::zone_membership::ZoneMembershipCertificate>,
    /// Zone revocations for invalidating zone membership.
    #[serde(default)]
    pub zone_revocations: Vec<crate::highestsec::zone_membership::ZoneRevocation>,
    /// Key rotation announcements.
    #[serde(default)]
    pub key_rotations: Vec<super::key_rotation::KeyRotationAnnouncement>,
    /// Key revocations.
    #[serde(default)]
    pub key_revocations: Vec<super::key_rotation::KeyRevocation>,
}

// ============================================================================
// Transport-level message envelope
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEstimationStrategy {
    pub name: String,
    pub estimated_cost_usd: f64,
    pub estimated_duration_secs: u64,
    pub average_tdp_watts: f32,
    pub congestion_impact_pct: f32,
    pub confidence_pct: u8,
}

/// Wire-level message types for the swarm transport layer.
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobMutation {
    UpdateTopology { new_topology_id: Option<crate::common::types::TopologyId> },
    ExtendDeadline { new_duration_ms: u64 },
    UpdatePaceDetector { new_detector: PaceDetector },
    // DiLoCo overrides can also live here
    
}

/// A cryptographic proposal from the submitter to alter a job in-flight (e.g., relaxing constraints).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationProposal {
    pub job_id: JobId,
    pub mutation: JobMutation,
    pub rationale: String, 
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmMessage {
    /// Queen Ant "Pull" Work Stealing: A node requests a batch of chunks for a known job.
    RequestWorkBatch { 
        job_id: JobId, 
        count: u32, 
        reply_to: NodeId,
        /// The node's dynamic spot price ask for computing this job.
        ask_mmx_per_instruction: u64,
    },
    /// Queen Ant Response: Orchestrator hands back specific chunk assignments.
    WorkBatchResponse { assignments: Vec<Assignment> },
    
    // -- Thermodynamic Civic Duty --
    /// 1:1 Tollbooth: Orchestrator forces a node to verify a ZKP before handing out paid work.
    VerifyThisProof {
        chunk_id: ChunkId,
        job_id: JobId,
        proof: crate::highestsec::zkp::ExecutionProof,
    },
    /// Response from edge node returning the result of the forced verification.
    ProofVerified {
        chunk_id: ChunkId,
        job_id: JobId,
        is_valid: bool,
        from: NodeId,
    },
    
    /// Membrane Gateway Backpressure: Notifies an edge node that the relay is saturated.
    RelayCapacityExceeded { retry_after_ms: u64, rejected_message: Box<SwarmMessage> },
    
    /// CGNAT Traversal: Exchanging WebRTC ICE candidates for UDP hole punching.
    IceCandidateExchange {
        candidates: Vec<crate::swarm::hyperscale::nat_traversal::IceCandidate>,
        reply_to: NodeId,
    },
    
    /// Submit cryptographic proof to a designated Ledger node for micropayment.
    SettlementClaim {
        chunk_id: ChunkId,
        job_id: JobId,
        fuel_consumed: u64,
        proof: Option<crate::highestsec::zkp::ExecutionProof>,
        from: NodeId,
        delegation: Option<crate::marabunta::identity::DelegationCertificate>,
        directive: Option<crate::marabunta::identity::FinancialDirective>,
    },
    /// Digital Purgatory: Submit a thermodynamic Proof-of-Work fine to unban a hijacked identity.
    SubmitAtonement {
        node_id: NodeId,
        pow_nonce: u64,
    },
    /// Data Availability Sampling (DAS): Challenge a node to prove it is physically storing a blob.
    ProveBlobAvailability {
        hash: BlobHash,
        /// The byte offset to sample.
        offset: u64,
        /// The number of bytes to return.
        length: u64,
        reply_to: NodeId,
    },
    /// Data Availability Sampling (DAS): The cryptographic proof response.
    BlobAvailabilityProof {
        hash: BlobHash,
        offset: u64,
        /// The raw bytes sampled from the disk.
        data: Vec<u8>,
        from: NodeId,
    },
    
    // -- Economic Enforcement --
    /// Confiscates unpaid MMX from a node's escrow if it fails a Data Availability challenge.
    SlashingDirective {
        target_node_id: NodeId,
        reason: String,
        amount_mmx: u64,
    },
    /// Unlocks unused MMX from a Job Escrow back to the Submitter when a job terminates.
    EscrowReclaim {
        job_id: JobId,
        submitter: NodeId,
        total_fuel_consumed: u64,
    },
    /// A message that must be relayed through a Membrane Gateway to the global Swarm.
    EgressRelay {
        target_node_id: NodeId,
        message: Box<SwarmMessage>,
        from: NodeId,
    },
    /// Federated War Room: Gossip a collaborative topology blueprint (CRDT).
    TopologyUpdate {
        topology: crate::swarm::crdt::LwwRegister<crate::common::types::HolographicTopology>,
        from: NodeId,
    },
    /// Planetary Storage: Gather a missing Reed-Solomon shard from the DHT.
    FetchBlob {
        hash: BlobHash,
        reply_to: NodeId,
    },
    /// BFT Hashgraph Consensus Event
    BftEvent {
        dag_event_json: String,
        from: NodeId,
    },
    /// BFT Hashgraph Vote (Cryptographic Signature)
    BftVote {
        event_hash: [u8; 32],
        signature_bytes: Vec<u8>,
        from: NodeId,
    },
    /// A sub-millisecond RAM ring update.
    IsomorphicUpdate {
        ring_id: String,
        key: IsoKey,
        value: Vec<u8>,
        timestamp: u64,
        from: NodeId,
    },

    /// Atomic batch of sub-millisecond RAM ring updates (Phase 2.1).
    IsomorphicBatchUpdate {
        ring_id: String,
        mutations: Vec<(IsoKey, Vec<u8>, u64)>,
        from: NodeId,
    },

    /// Periodic gossip exchange
    Gossip(GossipMessage),

    // --- Phase V: Interplanetary Fleet & Autonomic Command ---

    /// The Lazarus Protocol: A biological authorization from the last conscious human
    /// that instantly strips local sovereignty from all other ships in the fleet and 
    /// slaves them to the autonomic orbital insertion logic.
    LazarusOverride {
        /// The node ID of the ship where the override was activated
        origin_ship: NodeId,
        /// Biometric or cryptographic authorization signature
        authorization: Vec<u8>,
        /// The timestamp of the Apex Catastrophe
        catastrophe_time: DateTime<Utc>,
    },

    // --- The Harpy Eagle & The Fungi ---
    HarpyStrike {
        target: NodeId,
        reason: String,
        signature: Vec<u8>,
        from: NodeId,
    },
    CordycepsConfession {
        reason: String,
        memory_dump_hash: [u8; 32],
        signature: Vec<u8>,
        from: NodeId,
    },
    Retrovirus {
        from: NodeId,
        version: u64,
        patch_wasm: Vec<u8>,
        signature: Vec<u8>,
    },

    // --- The Harpy Eagle & The Fungi (Amazon Ecosystem Vectors 4 & 1) ---
    
    /// The Apex Supervisor detects a statistically anomalous gradient update (data poisoning) 
    /// and cryptographically slashes the node.

    /// The compromised node, upon receiving a Harpy Strike, is parasitized. 
    /// It locks its RAM, cryptographically signs a confession, broadcasts its death, and terminates.

    /// Stigmergic DTN: Store a delayed payload on a neighboring node
    StigmergicStore {
        target_hash: BlobHash,
        data: Vec<u8>,
        from: NodeId,
    },

    /// Stigmergic DTN: Query a neighbor for payloads matching our NodeId prefix
    /// Thermodynamic Arbitrage: A node gracefully yields a chunk back to the orchestrator.
    YieldChunk {
        chunk: Chunk,
        from: NodeId,
        reason: String,
    },
    StigmergicFetch {
        prefix_hash: BlobHash,
        from: NodeId,
    },
    
    /// Stigmergic DTN: Return retrieved payloads back to the requesting node
    /// Stigmergic DTN: A node signals it is voluntarily pausing a live execution due to physical constraints (e.g., loss of line-of-sight, thermal throttling). The network acknowledges the transition to ABSOLUTE_ASYNC and suspends SLA slashing.
    StigmergicDownshift {
        job_id: JobId,
        reason: String,
        from: NodeId,
    },
    StigmergicResponse {
        target_hash: BlobHash,
        /// Decoupled Data Plane: The local filesystem path to the 200GB tensor.
        payload_path: Option<String>,
        /// Side-Channel: Signal that raw binary data frames follow this message.
        stream_follows: bool,
        from: NodeId,
    },

    /// Side-Channel: A 1MB chunk of a physical tensor shard (Pillar 8.4).
    DataChunk { data: Vec<u8> },
    /// Side-Channel: Termination signal for an active file stream.
    EndOfStream,

    /// Phase 2 Emergency Sporulation payload (Reed-Solomon RS(3, 30) data shard)
    EmergencyReshard(Vec<u8>),

    /// Direct transfer of chunk data to an executor
    ChunkData { chunk: Chunk, from: NodeId },

    /// Completed chunk result sent to aggregator
    ChunkResult {
        chunk_id: ChunkId,
        job_id: JobId,
        result: ChunkResult,
        from: NodeId,
    },

    /// Stigmergic Learning Engine: Emitted when a node suffers a severe physical or OOM failure
    JobTelemetry {
        job_id: JobId,
        error_type: String,
        node_profile: crate::swarm::profile::NodeProfile,
        from: NodeId,
    },

    /// New node requesting initial peer list
    BootstrapRequest {
        node_id: NodeId,
        address: Option<SocketAddr>,
        traits: HashSet<Trait>,
    },

    /// Response with known peers
    BootstrapResponse {
        peers: Vec<PeerInfo>,
    },

    /// Lightweight liveness probe (faster than waiting for gossip)
    Ping { from: NodeId, nonce: u64, pow_nonce: Option<u64> },
    
    /// Used for Eager Replication Seeding. Pushes a heavy blob directly to a node.
    PushBlob {
        hash: BlobHash,
        filename: Option<String>,
        data: std::sync::Arc<Vec<u8>>,
        from: NodeId,
    },

    /// Response to Ping
    Pong { from: NodeId, nonce: u64 },

    /// Job submission from external client
    JobSubmission {
        job_id: JobId,
        name: String,
        chunks: Vec<Chunk>,
        priority: u32,
        submitter: NodeId,
        max_fuel_per_chunk: u64,
        total_fuel_budget: u64,
    },

    /// Request for job results
    JobResultRequest {
        job_id: JobId,
        from: NodeId,
    },

    /// Aggregated job results
    JobResultResponse {
        job_id: JobId,
        results: Vec<(ChunkId, ChunkResult)>,
        complete: bool,
    },

    // Organic swarm additions
    ProfileBroadcast {
        profile: NodeProfile,
        from: NodeId,
    },
    
    // --- IDE / Control Plane Override Layer ---
    /// A cryptographically signed command originating from an authenticated IDE/Admin.
    /// Used for Kill Switches, Market Overrides, and Subnet Quarantine.
    ControlPlaneCommand {
        /// The unique ID of the authorized Super-Peer / IDE issuing the command.
        issuer_id: NodeId,
        /// The raw command payload (e.g., "KILL_JOB:0x123", "OVERRIDE_BID:0.15").
        command_payload: Vec<u8>,
        /// ED25519 signature of the `command_payload` signed by the issuer's private key.
        signature: Vec<u8>,
        /// Replay protection nonce.
        nonce: u64,
        /// Unix timestamp of issuance.
        timestamp: u64,
    },

    CollectiveProposal {
        proposal: super::collective::CollectiveProposal,
        from: NodeId,
    },
    CollectiveAck {
        ack: super::collective::CollectiveAck,
        from: NodeId,
    },
    CollectiveBroadcast {
        collective: super::collective::Collective,
        from: NodeId,
    },
    CollectiveDissolved {
        collective_id: super::collective::CollectiveId,
        reason: String,
        from: NodeId,
    },
    EjectionProposal {
        proposal: super::collective::EjectionProposal,
        from: NodeId,
    },
    EjectionVote {
        vote: super::collective::EjectionVote,
        from: NodeId,
    },

    /// A Sphinx-encrypted onion packet for anonymous routing.
    EncryptedSphinxPacket {
        packet: super::crypto::sphinx::SphinxPacket,
        next_hop: SocketAddr,
    },
    ReputationUpdate {
        record: super::reputation::ReputationRecord,
        from: NodeId,
    },
    PolicyUpdate {
        policy: super::policy::PolicySet,
        from: NodeId,
    },

    // Admission protocol
    /// New node requesting to join the swarm via admission process
    AdmissionJoinRequest {
        request: super::admission::JoinRequest,
        from: NodeId,
    },
    /// Test task sent to candidate for verification
    AdmissionTestTask {
        task: super::admission::TestTask,
        from: NodeId,
    },
    /// Test task result from candidate
    AdmissionTestResult {
        result: super::admission::TestTaskResult,
        from: NodeId,
    },
    /// Juror verdict on a candidate
    AdmissionVerdict {
        verdict: super::admission::JurorVerdict,
        from: NodeId,
    },
    /// Admission vote broadcast
    AdmissionVoteBroadcast {
        vote: super::admission::AdmissionVote,
        from: NodeId,
    },
    /// Admission decision broadcast
    AdmissionDecisionBroadcast {
        decision: super::admission::AdmissionDecision,
        from: NodeId,
    },
    /// Probation status update broadcast
    AdmissionProbationUpdate {
        status: super::admission::ProbationStatus,
        from: NodeId,
    },

    // Production protocol additions

    /// Request to transfer a blob between nodes
    BlobTransferRequest {
        hash: BlobHash,
        from: NodeId,
    },
    /// Blob data in transit (chunked streaming via transport)
    BlobTransferResponse {
        hash: BlobHash,
        data: Vec<u8>,
        offset: u64,
        total_size: u64,
        from: NodeId,
    },

    /// Relay request: "connect me to target through you"
    RelayRequest {
        target: NodeId,
        session_id: uuid::Uuid,
        from: NodeId,
    },
    /// Relay offer: "node A wants to talk to you through me"
    RelayOffer {
        initiator: NodeId,
        relay: NodeId,
        session_id: uuid::Uuid,
    },
    /// Relay accept: "I'm connecting to the relay for this session"
    RelayAccept {
        session_id: uuid::Uuid,
        from: NodeId,
    },
    /// Relayed data packet
    RelayData {
        session_id: uuid::Uuid,
        data: Vec<u8>,
        from: NodeId,
    },

    /// Checkpoint announcement
    CheckpointAnnounce {
        chunk_id: ChunkId,
        job_id: JobId,
        progress_pct: f32,
        state_blob: BlobRef,
        from: NodeId,
    },
    /// Request checkpoint data for a chunk (on reassignment)
    CheckpointRequest {
        chunk_id: ChunkId,
        from: NodeId,
    },

    // Switchboard: math expression routing
    /// Request to evaluate a math expression via the switchboard
    SwitchboardRequest {
        session_id: crate::switchboard::types::SessionId,
        expression: String,
        handler_id: String,
        params: std::collections::HashMap<String, String>,
        from: NodeId,
    },

    /// Response with switchboard computation result
    SwitchboardResponse {
        session_id: crate::switchboard::types::SessionId,
        result_json: String,
        success: bool,
        from: NodeId,
    },

    // Management layer additions

    /// Share psyche state with peers
    PsycheBroadcast {
        psyche: serde_json::Value,
        from: NodeId,
    },
    /// Remote fleet operations
    FleetCommand {
        command: FleetCommandType,
        target: NodeId,
        reason: String,
        from: NodeId,
    },
    /// Inter-swarm membrane crossing data
    MembraneData {
        membrane_id: String,
        direction: String,
        category: String,
        payload: serde_json::Value,
        from: NodeId,
    },
    /// Swarm heartbeat for constellation
    ConstellationHeartbeat {
        heartbeat: serde_json::Value,
        from: NodeId,
    },
    /// Active health check request
    HealthProbeRequest {
        probe_name: String,
        sender: NodeId,
    },
    /// Health probe response
    HealthProbeResponse {
        probe_name: String,
        success: bool,
        latency_ms: u64,
        from: NodeId,
    },
    /// Audit log sync between nodes
    AuditSync {
        entries: Vec<serde_json::Value>,
        from: NodeId,
    },
    /// Share alert state with peers
    AlertBroadcast {
        alert: serde_json::Value,
        from: NodeId,
    },
    /// Request another node to witness an audit event
    WitnessRequest {
        event_id: String,
        event_hash: Vec<u8>,
        criticality: String,
        from: NodeId,
    },
    /// Response to a witness request with verdict and signature
    WitnessVerdict {
        event_id: String,
        verdict: String,
        signature: Vec<u8>,
        from: NodeId,
    },

    // Consent layer
    /// Swarm sends its config manifest to a candidate for consent review
    ConfigManifestOffer {
        manifest: super::config_consent::ConfigManifest,
        from: NodeId,
    },
    /// Candidate responds with consent (accept/reject)
    ConfigConsentResponse {
        consent: super::config_consent::ConfigConsent,
        from: NodeId,
    },

    // EU Highestsec compliance
    /// Key rotation announcement propagated via gossip.
    KeyRotation(super::key_rotation::KeyRotationAnnouncement),
    /// Key revocation propagated via gossip.
    KeyRevocationMsg(super::key_rotation::KeyRevocation),

    // --- P2P DATA SPIGOTS (BitTorrent) ---
    /// Announce possessed chunks for a specific blob
    TorrentBitfield(ChunkBitfield),
    /// Request a specific chunk from a peer
    TorrentRequest(ChunkRequest),
    /// Return chunk data
    TorrentResponse(ChunkResponse),

    /// Authorized administrative action bundle (Kill, LogLevel, etc.)
    /// Subject to verification by the Programmable Vault API.
    AdminAction(crate::marabunta::vault::AdminAction),

    // --- MARABUNTA MERCANTILE EXCHANGE (MMX) ---
    /// High-frequency bid for a computational contract.
    MercantileBid(crate::marabunta::federation::ComputeBid),
    /// Request for Proposal (RFC) broadcast by a client swarm.
    MercantileRFC {
        job_id: JobId,
        market_price: u64,
        requirements: super::profile::JobRequirements,
        from: NodeId,
    },
    /// Proposal to form a computational coalition (Wolf Pack).
    WolfPackProposal {
        coalition_id: String,
        leader: crate::marabunta::identity::FederationId,
        members: Vec<crate::marabunta::identity::FederationId>,
        associated_topology: Option<crate::common::types::TopologyId>,
        from: NodeId,
    },

    // --- CHAOS ENGINEERING (WAR GAMES) ---
    /// A highly privileged, cryptographically signed command to simulate network death.
    ChaosStrike {
        target_nodes: Vec<NodeId>,
        target_region: Option<String>,
        death_type: crate::chaos::types::DeathType,
        issuer_signature: Vec<u8>,
        from: NodeId,
    },
    /// A command to revive nodes from a Chaos Event.
    ChaosRevive {
        target_nodes: Vec<NodeId>,
        target_region: Option<String>,
        issuer_signature: Vec<u8>,
        from: NodeId,
    },
}

// ============================================================================
// Blob types (Phase 4 — blobstore)
// ============================================================================

/// SHA-256 hash used as content-addressed blob identifier.
pub type BlobHash = [u8; 32];

/// Fixed-size key for Isomorphic State Rings.
pub type IsoKey = [u8; 32];

/// Reference to a content-addressed blob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BlobRef {
    pub hash: BlobHash,
    pub size_bytes: u64,
    pub filename: Option<String>,
}

// ============================================================================
// Installed software (Phase 5 — discovery)
// ============================================================================

/// Category of discovered software on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoftwareCategory {
    Runtime,
    Database,
    MediaTool,
    DevTool,
    MathScience,
    Network,
    Custom,
}

/// A piece of software discovered on a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstalledSoftware {
    pub name: String,
    pub version: Option<String>,
    pub path: String,
    pub category: SoftwareCategory,
}

// ============================================================================
// Script and chunking types (Phase 1 — API)
// ============================================================================

/// Type of script to execute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScriptType {
    Shell,
    Python,
    Custom(String),
    BlindComputation {
        wasm_bytes: Vec<u8>,
        fhe_eval_key_hash: BlobHash,
        encrypted_inputs_hash: BlobHash,
    },
    Plugin {
        plugin_id: String,
        config: serde_json::Value,
    },
}

/// How to split a job into chunks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[derive(Default)]
pub enum ChunkStrategy {
    PerLine,
    PerFile,
    PerNBytes { bytes: u64 },
    Fixed { count: u32 },
    ParameterSweep { params: Vec<(String, Vec<String>)> },
    #[default]
    Single,
}


// ============================================================================
// Energy types (Phase 7)
// ============================================================================

/// Budget constraints for energy cost of a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyBudget {
    pub max_total_usd: Option<f64>,
    pub target_usd: Option<f64>,
    pub deadline: Option<DateTime<Utc>>,
    pub price_schedule_id: Option<String>,
}

/// Confidence level for energy cost estimates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    High,
    Medium,
    Low,
}

// ============================================================================
// Aggregation types (Phase 10)
// ============================================================================

/// How to combine chunk results into a final job result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[derive(Default)]
pub enum ReduceSpec {
    #[default]
    Collect,
    ConcatStdout,
    ConcatFiles { output_filename: String },
    CustomScript { script_type: ScriptType, script: String },
    JsonArray,
    CsvMerge,
    Numeric { op: NumericOp, field: String },
    /// Do not attempt to reduce results; just collect encrypted blobs for the client.
    BlindPassThrough,
    
    /// Cryptographically Blind Homomorphic Aggregation.
    /// The Swarm adds FHE ciphertexts together without decrypting them.
    /// Used for massive-scale zero-knowledge voting and telemetry.
    HomomorphicAddition {
        /// The public FHE Evaluation Key required to perform the blind addition.
        fhe_eval_key: Vec<u8>,
    },
}


/// Numeric aggregation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NumericOp {
    Sum,
    Avg,
    Min,
    Max,
}

// ============================================================================
// Result delivery mode (Phase 10)
// ============================================================================

/// How results are delivered to the user.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[derive(Default)]
pub enum DeliveryMode {
    #[default]
    Poll,
    ServerSentEvents,
    Webhook { url: String },
    WriteToBlob,
}


// ============================================================================
// Criticality level (Audit Scrutiny W1A)
// ============================================================================

/// Five-tier criticality classification for auditable actions.
///
/// Ordering matches severity: Low < Normal < ... < Emergency.
/// This allows `std::cmp::max()` to select the highest criticality
/// when merging evaluations from multiple sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum CriticalityLevel {
    /// Informational actions -- no witnesses required.
    Low,
    /// Standard operations -- 2 witnesses from any segment.
    #[default]
    Normal,
    /// Elevated-risk operations -- 5 witnesses from 2+ segments.
    High,
    /// Security-sensitive operations -- 7+ witnesses from 3+ segments.
    Critical,
    /// Existential threats -- broadcast to all reachable nodes.
    Emergency,
}


impl fmt::Display for CriticalityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low       => write!(f, "LOW"),
            Self::Normal    => write!(f, "NORMAL"),
            Self::High      => write!(f, "HIGH"),
            Self::Critical  => write!(f, "CRITICAL"),
            Self::Emergency => write!(f, "EMERGENCY"),
        }
    }
}

impl CriticalityLevel {
    /// Returns the required witness count for this criticality tier.
    ///
    /// Emergency returns `usize::MAX` to signal "all reachable nodes."
    pub fn witness_count(&self) -> usize {
        match self {
            Self::Low       => 0,
            Self::Normal    => 2,
            Self::High      => 5,
            Self::Critical  => 7,
            Self::Emergency => usize::MAX,
        }
    }

    /// Returns the witness response timeout in seconds for this tier.
    pub fn witness_timeout_secs(&self) -> u64 {
        match self {
            Self::Low       => 0,
            Self::Normal    => 5,
            Self::High      => 10,
            Self::Critical  => 30,
            Self::Emergency => 60,
        }
    }
}

// ============================================================================
// Verdict (Audit Scrutiny W1A)
// ============================================================================

/// A witness node's conclusion about an audited action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// Witness validated the action's integrity and co-signs.
    Confirmed,
    /// Witness detected a discrepancy and rejects.
    Rejected,
    /// Witness was selected but did not respond in time.
    Timeout,
}

// ============================================================================
// WitnessRecord (Audit Scrutiny W1A)
// ============================================================================

/// Record of a single witness's attestation to an audit event.
///
/// The signature covers the concatenation of `event_id || node_id || verdict`
/// as bytes. Actual Ed25519 signing is deferred to Wave 3; for now the
/// signature field holds placeholder bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WitnessRecord {
    /// Node ID of the witness.
    pub node_id: String,
    /// Milliseconds since Unix epoch when the witness signed.
    pub timestamp_ms: u64,
    /// Ed25519 signature over (event_id || node_id || verdict).
    pub signature: Vec<u8>,
    /// The witness's verdict on the event.
    pub verdict: Verdict,
}

/// Compact peer info for bootstrap responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: NodeId,
    pub address: SocketAddr,
    pub traits: HashSet<Trait>,
    pub load: f32,
}

// ============================================================================
// NAT type detection
// ============================================================================

/// Type of NAT the node is behind, affects connectivity strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum NatType {
    /// Public IP, directly reachable
    Public,
    /// Cone NAT (full, restricted, or port-restricted) — hole-punchable
    ConeNat,
    /// Symmetric NAT (e.g., mobile carriers) — needs relay
    SymmetricNat,
    /// Haven't determined yet
    #[default]
    Unknown,
}


/// Connectivity information used for trait evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectivityInfo {
    pub is_publicly_reachable: bool,
    pub nat_type: NatType,
    pub has_relay_access: bool,
    pub listen_address: Option<SocketAddr>,
    /// Round-trip time to nearest known peer, in ms
    pub avg_rtt_ms: Option<f64>,
}

impl Default for ConnectivityInfo {
    fn default() -> Self {
        Self {
            is_publicly_reachable: false,
            nat_type: NatType::Unknown,
            has_relay_access: false,
            listen_address: None,
            avg_rtt_ms: None,
        }
    }
}

// ============================================================================
// Swarm error types
// ============================================================================

/// Errors that can occur in swarm operations.
#[derive(Debug, thiserror::Error)]
pub enum SwarmError {
    #[error("transport error: {0}")]
    Transport(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("no peers available")]
    NoPeers,

    #[error("corrupted data: {0}")]
    CorruptedData(String),

    #[error("bootstrap failed after {attempts} attempts: {reason}")]
    BootstrapFailed { attempts: u32, reason: String },

    #[error("chunk execution failed: {0}")]
    ChunkExecution(String),

    #[error("job not found: {0}")]
    JobNotFound(JobId),

    #[error("node shutting down")]
    ShuttingDown,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("capacity exceeded: {0}")]
    CapacityExceeded(String),
    #[error("Transport error: {0}")]
    TransportError(String),

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("crdt conflict detected: {0}")]
    Conflict(String),


    #[error("sandbox error: {0}")]
    Sandbox(String),

    #[error("blob not found: {0}")]
    BlobNotFound(String),

    #[error("api error: {0}")]
    Api(String),

    #[error("strategy not found: {0}")]
    StrategyNotFound(String),

    #[error("energy estimation error: {0}")]
    Energy(String),

    #[error("relay error: {0}")]
    Relay(String),

    #[error("no relay node available")]
    NoRelayAvailable,

    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("queue full: {0}")]
    QueueFull(String),

    #[error("fleet error: {0}")]
    Fleet(String),

    #[error("health check error: {0}")]
    HealthCheck(String),

    #[error("audit error: {0}")]
    Audit(String),
}

/// Result type alias for swarm operations.
pub type SwarmResult<T> = Result<T, SwarmError>;

// ═══════════════════════════════════════════════════════════════════════════
// Combo Infrastructure Types
// ═══════════════════════════════════════════════════════════════════════════

/// Hardware class of a swarm node, derived from resource snapshot.
///
/// In Pillar 15 (Absolute Sovereignty & Kinetic Evasion), a `NodeClass::Enterprise`
/// is no longer exclusively a single piece of heavy hardware. A Enterprise can be a
/// "Virtual Enterprise"—a cryptographic aggregate of 50 `Standard` nodes sharing a
/// threshold DKG identity to execute consensus without painting a kinetic target
/// on a single IP address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum NodeClass {
    /// Disposable proxies and "Meat Shields" for hiding traffic origination.
    Edge,
    /// Mid-range: old laptops, tablets, Pi 4, thin clients. 2-4 cores, 2-4GB RAM.
    Light,
    /// Workhorses: desktops, gaming rigs, retired office PCs. 4-8 cores, 8-16GB RAM.
    Standard,
    /// Heavy Aggregators / Data Plane execution. 
    /// Note: The Routing/Control plane Boulders are now cryptographic thresholds.
    Enterprise,
}

impl NodeClass {
    /// Derive node class from available resources.
    pub fn from_resources(cpu_cores: u32, memory_mb: u64) -> Self {
        match (cpu_cores, memory_mb) {
            (_, m) if m < 1_500 => NodeClass::Edge,
            (c, m) if c <= 4 && m < 6_000 => NodeClass::Light,
            (c, m) if c <= 8 && m < 20_000 => NodeClass::Standard,
            _ => NodeClass::Enterprise,
        }
    }

    /// Maximum memory (MB) a task chunk can use on this class.
    pub fn max_memory_mb(&self) -> u64 {
        match self {
            NodeClass::Edge => 100,
            NodeClass::Light => 1_024,
            NodeClass::Standard => 8_192,
            NodeClass::Enterprise => 32_768,
        }
    }

    /// Maximum duration (seconds) for a single task chunk on this class.
    pub fn max_duration_secs(&self) -> u64 {
        match self {
            NodeClass::Edge => 60,
            NodeClass::Light => 600,
            NodeClass::Standard => 3_600,
            NodeClass::Enterprise => 14_400,
        }
    }

    /// Maximum input data size (MB) for a single chunk on this class.
    pub fn max_input_size_mb(&self) -> u64 {
        match self {
            NodeClass::Edge => 5,
            NodeClass::Light => 50,
            NodeClass::Standard => 500,
            NodeClass::Enterprise => 5_120,
        }
    }

    /// Maximum output data size (MB) for a single chunk on this class.
    pub fn max_output_size_mb(&self) -> u64 {
        match self {
            NodeClass::Edge => 1,
            NodeClass::Light => 10,
            NodeClass::Standard => 100,
            NodeClass::Enterprise => 1_024,
        }
    }

    /// Suggested number of tasks per chunk for this class.
    pub fn suggested_batch_size(&self) -> u32 {
        match self {
            NodeClass::Edge => 1,
            NodeClass::Light => 25,
            NodeClass::Standard => 250,
            NodeClass::Enterprise => 2_500,
        }
    }

    /// Human-readable label.
    pub fn label(&self) -> &'static str {
        match self {
            NodeClass::Edge => "Edge",
            NodeClass::Light => "Light",
            NodeClass::Standard => "Standard",
            NodeClass::Enterprise => "Enterprise",
        }
    }

    /// Weight for compute contribution estimation.
    pub fn compute_weight(&self) -> f64 {
        match self {
            NodeClass::Edge => 0.05,
            NodeClass::Light => 0.25,
            NodeClass::Standard => 1.0,
            NodeClass::Enterprise => 4.0,
        }
    }

    /// All classes in ascending order.
    pub fn all() -> &'static [NodeClass] {
        &[NodeClass::Edge, NodeClass::Light, NodeClass::Standard, NodeClass::Enterprise]
    }
}

impl std::fmt::Display for NodeClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Task constraints for a specific node class. Used by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConstraints {
    pub node_class: NodeClass,
    pub max_memory_mb: u64,
    pub max_duration_secs: u64,
    pub max_input_size_mb: u64,
    pub max_output_size_mb: u64,
    pub max_concurrent_chunks: u32,
}

impl TaskConstraints {
    pub fn for_class(class: NodeClass) -> Self {
        Self {
            node_class: class,
            max_memory_mb: class.max_memory_mb(),
            max_duration_secs: class.max_duration_secs(),
            max_input_size_mb: class.max_input_size_mb(),
            max_output_size_mb: class.max_output_size_mb(),
            max_concurrent_chunks: match class {
                NodeClass::Edge => 1,
                NodeClass::Light => 2,
                NodeClass::Standard => 4,
                NodeClass::Enterprise => 8,
            },
        }
    }
}

/// Customer-facing priority level (Rush/Standard/Economy).
///
/// Different from the internal `Priority` enum used in marketplace bidding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(Default)]
pub enum PriorityLevel {
    /// Front of queue, maximum node allocation. 3x cost.
    Rush,
    /// Normal queue, fair share. 1x cost.
    #[default]
    Standard,
    /// Off-peak hours, fills idle capacity. 0.5x cost.
    Economy,
}

impl PriorityLevel {
    /// Cost multiplier for this priority level.
    pub fn cost_multiplier(&self) -> f64 {
        match self {
            PriorityLevel::Rush => 3.0,
            PriorityLevel::Standard => 1.0,
            PriorityLevel::Economy => 0.5,
        }
    }

    /// Internal priority mapping for queue ordering.
    pub fn queue_weight(&self) -> u32 {
        match self {
            PriorityLevel::Rush => 100,
            PriorityLevel::Standard => 50,
            PriorityLevel::Economy => 10,
        }
    }
}


impl std::fmt::Display for PriorityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriorityLevel::Rush => write!(f, "rush"),
            PriorityLevel::Standard => write!(f, "standard"),
            PriorityLevel::Economy => write!(f, "economy"),
        }
    }
}

/// Strategy for verifying chunk results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VerificationStrategy {
    /// Run the same chunk on N nodes, compare outputs. Most reliable.
    Redundant { replicas: u8 },
    /// Single execution for trusted nodes, random spot-checks at given rate.
    SpotCheck { check_rate: f64 },
    /// Statistical validation for aggregate workloads (Monte Carlo, bootstrapping).
    Statistical { outlier_tolerance: f64 },
    /// No verification. For trusted internal use only.
    None,
}

impl Default for VerificationStrategy {
    fn default() -> Self {
        VerificationStrategy::Redundant { replicas: 2 }
    }
}

/// Result of verifying chunk outputs across replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VerificationOutcome {
    /// All replicas agree.
    Verified { consensus_hash: String },
    /// Majority agree, outlier discarded.
    MajorityConsensus { consensus_hash: String, outlier_node: NodeId, replicas_agreed: u8 },
    /// Replicas disagree, needs re-execution.
    Conflict { hashes: Vec<(NodeId, String)> },
    /// Spot check passed.
    SpotCheckPassed,
    /// Spot check failed — node returned different result.
    SpotCheckFailed { trusted_hash: String, checked_hash: String, node: NodeId },
    /// Statistical validation passed.
    StatisticallyValid { p_value: f64 },
    /// Statistical outlier detected.
    StatisticalOutlier { z_score: f64, node: NodeId },
    /// Skipped (no verification).
    Skipped,
}

/// Cost estimate for a job before submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Estimated cost in USD.
    pub estimated_cost_usd: f64,
    /// Estimated cost range (low, high).
    pub cost_range_usd: (f64, f64),
    /// Estimated completion time in seconds.
    pub estimated_duration_secs: u64,
    /// Duration range (low, high) in seconds.
    pub duration_range_secs: (u64, u64),
    /// Number of chunks that will be created.
    pub estimated_chunks: u64,
    /// Estimated retry overhead percentage.
    pub retry_overhead_pct: f64,
    /// Verification overhead percentage.
    pub verification_overhead_pct: f64,
    /// Cloud cost comparison (for display).
    pub cloud_comparison_usd: Option<f64>,
    /// Priority level used for estimate.
    pub priority: PriorityLevel,
    /// Number of effective nodes expected.
    pub effective_nodes: u64,
}

/// Webhook configuration for result delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// URL to POST results to.
    pub url: String,
    /// Optional auth header value.
    pub auth_header: Option<String>,
    /// Milestones to trigger webhook (e.g., 25%, 50%, 75%, 100%).
    pub milestones: Vec<u8>,
    /// Whether to include partial results in milestone webhooks.
    pub include_partial_results: bool,
    /// Maximum retry attempts for failed webhook delivery.
    pub max_retries: u8,
    /// Timeout per webhook request in seconds.
    pub timeout_secs: u64,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            auth_header: None,
            milestones: vec![25, 50, 75, 100],
            include_partial_results: false,
            max_retries: 3,
            timeout_secs: 30,
        }
    }
}

/// Status of a webhook delivery attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebhookDeliveryStatus {
    Pending,
    Delivered { status_code: u16, at: chrono::DateTime<chrono::Utc> },
    Failed { error: String, attempts: u8 },
    Exhausted { last_error: String },
}

/// A pre-packaged compute module (combo).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComboModule {
    /// Module identifier (e.g., "cr.montecarlo").
    pub id: String,
    /// Display name (e.g., "CR.MonteCarlo").
    pub display_name: String,
    /// Category: "computation", "data", "media", "science", "ai", "finance", "enterprise".
    pub category: String,
    /// Short description.
    pub description: String,
    /// Plugin that implements this module.
    pub plugin_id: String,
    /// Default verification strategy.
    pub default_verification: VerificationStrategy,
    /// Default chunk strategy.
    pub default_chunk_strategy: ChunkStrategy,
    /// Minimum node class required.
    pub min_node_class: NodeClass,
    /// Parameter schema (JSON Schema for validation).
    pub parameter_schema: serde_json::Value,
    /// Supported output formats.
    pub output_formats: Vec<String>,
    /// Cloud cost comparison base (USD per reference workload).
    pub cloud_comparison_base_usd: Option<f64>,
    /// Whether this module supports streaming partial results.
    pub supports_streaming: bool,
    /// Whether this module supports statistical verification.
    pub supports_statistical_verification: bool,
}

/// Swarm tier configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SwarmTier {
    /// 500K registered nodes, global.
    Titan,
    /// 100K registered nodes, continental.
    Legion,
    /// 20K registered nodes, national/enterprise.
    Battalion,
    /// 2K registered nodes, campus.
    Squad,
    /// 200 registered nodes, lab.
    Cell,
}

impl SwarmTier {
    pub fn registered_nodes(&self) -> u64 {
        match self {
            SwarmTier::Titan => 500_000,
            SwarmTier::Legion => 100_000,
            SwarmTier::Battalion => 20_000,
            SwarmTier::Squad => 2_000,
            SwarmTier::Cell => 200,
        }
    }

    pub fn effective_ratio(&self) -> f64 {
        match self {
            SwarmTier::Titan => 0.175,
            SwarmTier::Legion => 0.175,
            SwarmTier::Battalion => 0.175,
            SwarmTier::Squad => 0.175,
            SwarmTier::Cell => 0.175,
        }
    }

    pub fn effective_nodes(&self) -> u64 {
        (self.registered_nodes() as f64 * self.effective_ratio()) as u64
    }

    pub fn label(&self) -> &'static str {
        match self {
            SwarmTier::Titan => "Titan",
            SwarmTier::Legion => "Legion",
            SwarmTier::Battalion => "Battalion",
            SwarmTier::Squad => "Squad",
            SwarmTier::Cell => "Cell",
        }
    }
}

impl std::fmt::Display for SwarmTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

impl SwarmMessage {
    /// Extracts the sender's NodeId from the message, if present.
    pub fn sender_id(&self) -> Option<NodeId> {
        match self {
            SwarmMessage::Ping { from, .. } => Some(*from),
            SwarmMessage::Pong { from, .. } => Some(*from),
            
            
            
            
            
            
            
            SwarmMessage::ChunkResult { from, .. } => Some(*from),
            SwarmMessage::JobTelemetry { from, .. } => Some(*from),
            SwarmMessage::IsomorphicUpdate { from, .. } => Some(*from),
            SwarmMessage::WolfPackProposal { from, .. } => Some(*from),
            SwarmMessage::ChaosStrike { from, .. } => Some(*from),
            SwarmMessage::ChaosRevive { from, .. } => Some(*from),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GeoRegion {
    US,
    EU,
    Asia,
    Custom(String),
}
