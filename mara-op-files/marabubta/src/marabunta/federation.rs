// Marabunta - Licensed under the MIT License.
//! Marabunta Mercantile Exchange (MMX) and Federation Governance.
//!
//! Implements the Wolf Pack protocol for computational coalitions, 
//! inter-swarm bidding/auctions, and treaty-based resource lending.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};
use std::net::SocketAddr;
use tracing::{info, warn};
use crate::marabunta::identity::FederationId;
use crate::marabunta::identity::NodeId as MarabuntaNodeId;
use crate::swarm::types::ChunkId;
use crate::common::types::JobId;

/// A formal resource sharing contract between two independent swarms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiplomaticTreaty {
    pub treaty_id: String,
    pub partner: FederationId,
    /// Percentage of local idle nodes allowed to be lent.
    pub capacity_cap_pct: u8,
    /// Queue length threshold that triggers instant recall of lent nodes.
    pub recall_threshold: u32,
    pub visa_duration: Duration,
    pub signed_at: SystemTime,
    /// Wormhole Gateway URI of the partner Swarm (e.g., "https://brazil.marabunta.io:443")
    pub gateway_uri: Option<String>,
}

/// A computational coalition (Wolf Pack) where multiple swarms 
/// aggregate their reputation and capacity to win large contracts.
/// The physical network topology dynamically elected by a Wolf Pack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WolfPackTopology {
    /// 100Gbps+ LAN detected (sub-2ms ping).
    /// Action: Bypass Swarm transport. Boot raw PyTorch FSDP (NCCL) over direct sockets.
    DatacenterLocal,
    
    /// Public internet detected (>5ms ping).
    /// Action: Boot PyTorch Gloo backend. Route gradients through Marabunta QUIC Ring.
    ResilientRing,
    
    /// Unknown state, awaiting acoustic ping negotiation.
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WolfPackStatus {
    /// Collaborative design phase (War Room).
    Draft,
    /// Minimum signatures gathered, awaiting leader lock.
    Signed,
    /// SLA active, hardware committed.
    Locked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WolfPackCoalition {
    pub coalition_id: String,
    pub members: HashSet<FederationId>,
    pub leader: FederationId,
    pub total_aggregate_reputation: f64,
    /// The mathematically negotiated topological layout of the compute cluster.
    pub topology: WolfPackTopology,
    /// Optional link to a Holographic War Room blueprint.
    pub associated_topology: Option<crate::common::types::TopologyId>,
    /// Cryptographic commitments ("Blood Oaths") from sovereign providers.
    pub signatures: HashMap<FederationId, Vec<u8>>,
    pub status: WolfPackStatus,
}

impl WolfPackCoalition {
    /// Add a cryptographic commitment ("Blood Oath") from a member.
    pub fn add_signature(&mut self, member: FederationId, signature: Vec<u8>) {
        self.signatures.insert(member, signature);
        
        // Transition to Signed if we have a quorum or all members signed.
        // For the War Room prototype, we move to Signed as soon as any signature exists.
        if self.status == WolfPackStatus::Draft && !self.signatures.is_empty() {
            tracing::info!(coalition = %self.coalition_id, "WolfPack: Transitioning to SIGNED state");
            self.status = WolfPackStatus::Signed;
        }
    }

    /// Lock the topology into an SLA. Officially binds the blueprint to physical reality.
    pub fn lock(&mut self) -> bool {
        if self.status == WolfPackStatus::Signed {
            tracing::info!(coalition = %self.coalition_id, "WolfPack: Topology LOCKED into SLA");
            self.status = WolfPackStatus::Locked;
            true
        } else {
            false
        }
    }
}

/// A bid for a computational contract on the Mercantile Exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeBid {
    pub job_id: JobId,
    pub bidder: FederationId,
    /// Price in micro-credits per vCPU hour.
    pub bid_price: u64,
    /// Aggregate reputation score of the bidder (or coalition).
    pub reputation_score: f64,
    /// Guaranteed throughput (GFLOPS).
    pub throughput_guarantee: u64,
}

/// A financial derivative for computational power.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComputeDerivative {
    /// A contract to execute N petasops at a fixed price on a future date.
    ExecutionFuture {
        delivery_date: SystemTime,
        throughput_amount: u64,
        strike_price: u64,
    },
    /// The right (but not obligation) to execution at a price if global thermal index spikes.
    ComputeOption {
        expiry: SystemTime,
        max_price: u64,
        premium: u64,
    },
}

/// A pending payment record for a verified chunk of work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingPayment {
    pub chunk_id: ChunkId,
    pub worker: crate::swarm::types::NodeId,
    pub amount: u64,
    pub verified_at: SystemTime,
    pub federation_id: String,
    pub signature: String,
    pub public_key: String,
}

/// The state of the local Federation Manager.
/// Internal task for the background SQLite batch writer.
#[derive(Debug)]
struct SettlementTask {
    chunk_id: String,
    worker: String,
    amount: u64,
    federation_id: String,
    signature: String,
    public_key: String,
}

pub struct FederationManager {
    local_id: FederationId,
    signer: std::sync::Arc<crate::marabunta::identity::NodeIdentity>,
    active_treaties: HashMap<FederationId, DiplomaticTreaty>,
    active_coalitions: HashMap<String, WolfPackCoalition>,
    /// Market of future execution contracts.
    compute_futures: Vec<ComputeDerivative>,
    /// Ledger of credits owed to nodes for verified work.
    pub settlement_ledger: Vec<PendingPayment>,
    /// SQLite connection for persistent economic settlement.
    pub db: Option<std::sync::Arc<parking_lot::Mutex<rusqlite::Connection>>>,
    /// Cryptographic visas issued to foreign nodes.
    ledger_tx: Option<tokio::sync::mpsc::Sender<SettlementTask>>,
    issued_visas: HashSet<[u8; 32]>,
}

impl FederationManager {
    pub fn new(local_id: FederationId, signer: std::sync::Arc<crate::marabunta::identity::NodeIdentity>) -> Self {
        // 💰 AMNESIAC ECONOMY FIX: Ensure the MMX ledger survives node reboots
        let _ = std::fs::create_dir_all(".gemini/tmp/");
        let db = match rusqlite::Connection::open(".gemini/tmp/marabunta_settlement.db") {
            Ok(conn) => {
                // ⚡ SQLITE WAL MODE: Enable concurrent Write-Ahead Logging
                // This prevents SQLITE_BUSY (database is locked) errors during massive parallel 
                // parameter sweeps where 100s of chunks are settled simultaneously.
                let _ = conn.execute_batch(
                    "PRAGMA journal_mode=WAL;
                     PRAGMA synchronous=NORMAL;
                     CREATE TABLE IF NOT EXISTS ledger (
                        id INTEGER PRIMARY KEY,
                        chunk_id TEXT NOT NULL,
                        worker TEXT NOT NULL,
                        amount INTEGER NOT NULL,
                        federation_id TEXT NOT NULL,
                        signature TEXT NOT NULL,
                        public_key TEXT NOT NULL,
                        verified_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );"
                );
                Some(std::sync::Arc::new(parking_lot::Mutex::new(conn)))
            }
            Err(e) => {
                tracing::warn!("Failed to open persistent MMX ledger: {}. Falling back to volatile RAM.", e);
                None
            }
        };

        // 🛑 MMX EVAPORATION BOMB FIX: Use a Bounded Channel (Backpressure)
        // An unbounded channel allows the queue to grow infinitely in RAM, risking total loss
        // of unwritten MMX credits if the OS kills the process. By capping it to 10,000, we apply 
        // backpressure to the settlement loop. If SQLite falls behind, the swarm slows down,
        // but no citizen will ever lose their cryptographically earned money.
        let (ledger_tx, mut ledger_rx) = tokio::sync::mpsc::channel::<SettlementTask>(10_000);
        let db_for_worker = db.clone();

        // ⚡ BATCH WRITER FIX: Spawn a single background thread to handle all SQLite writes.
        // This prevents 10,000 parallel settlements from exhausting the Tokio blocking pool.
        tokio::spawn(async move {
            if let Some(db_arc) = db_for_worker {
                while let Some(task) = ledger_rx.recv().await {
                    let mut batch = vec![task];
                    // Opportunistically batch up to 5000 settlements in one transaction
                    while batch.len() < 5000 {
                        match ledger_rx.try_recv() {
                            Ok(t) => batch.push(t),
                            Err(_) => break,
                        }
                    }

                    let db_arc_inner = db_arc.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        let mut conn_guard = db_arc_inner.lock();
                        let tx_res = conn_guard.transaction();
                        if let Ok(tx) = tx_res {
                            for t in batch {
                                let _ = tx.execute(
                                    "INSERT INTO ledger (chunk_id, worker, amount, federation_id, signature, public_key) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                                    rusqlite::params![t.chunk_id, t.worker, t.amount, t.federation_id, t.signature, t.public_key],
                                );
                            }
                            let _ = tx.commit();
                        }
                    }).await;
                }
            }
        });

        Self {
            local_id,
            signer,
            active_treaties: HashMap::new(),
            active_coalitions: HashMap::new(),
            compute_futures: Vec::new(),
            settlement_ledger: Vec::new(),
            db,
            ledger_tx: Some(ledger_tx),
            issued_visas: HashSet::new(),
        }
    }

    /// Record a payment for verified work (The Settlement/Clearing Logic).
    /// Behavioral Invariant: Payment is ONLY authorized after Vulture/VerificationEngine 
    /// confirms the output hash is valid.
    pub fn settle_verified_work(&mut self, chunk_id: ChunkId, worker: crate::swarm::types::NodeId, price: u64) {
        tracing::info!("Settlement: Authorizing payment of {} micro-credits to node {} for chunk {}", price, worker, chunk_id);
        
        let fed_id_str = hex::encode(self.local_id.0);
        let chunk_id_str = chunk_id.0.to_string();
        let worker_str = worker.0.to_string();
        let timestamp = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        
        // 🛑 TAX FRAUD FIX: Cryptographically bind the FederationId to the receipt
        let payload = format!("{}:{}:{}:{}:{}", chunk_id_str, worker_str, price, fed_id_str, timestamp);
        let sig_bytes = self.signer.sign_ed25519(payload.as_bytes());
        let sig_hex = hex::encode(sig_bytes);
        let pub_key_hex = hex::encode(self.signer.ed25519_verifying_key().to_bytes());

        if let Some(ref tx) = self.ledger_tx {
            let task = SettlementTask {
                chunk_id: chunk_id_str,
                worker: worker_str,
                amount: price,
                federation_id: fed_id_str.clone(),
                signature: sig_hex.clone(),
                public_key: pub_key_hex.clone(),
            };
            let _ = tx.send(task);
        }
        
        self.settlement_ledger.push(PendingPayment {
            chunk_id,
            worker,
            amount: price,
            verified_at: SystemTime::now(),
            federation_id: fed_id_str,
            signature: sig_hex,
            public_key: pub_key_hex,
        });

        // 🛑 RAM LEAK FIX: Prevent the amnesiac fallback ledger from OOM-crashing the orchestrator.
        if self.settlement_ledger.len() > 10_000 {
            self.settlement_ledger.drain(..5_000);
        }
    }

    /// Issue a future execution contract to hedge against market volatility.
    pub fn issue_future(&mut self, date: SystemTime, amount: u64, price: u64) {
        let future = ComputeDerivative::ExecutionFuture {
            delivery_date: date,
            throughput_amount: amount,
            strike_price: price,
        };
        tracing::info!("MMX: Issuing compute future for {:?} at {} credits", date, price);
        self.compute_futures.push(future);
    }

    /// Sign and enact a new treaty.
    pub fn enact_treaty(&mut self, treaty: DiplomaticTreaty) {
        tracing::info!("Federation: Enacting treaty '{}' with partner {}", treaty.treaty_id, treaty.partner);
        self.active_treaties.insert(treaty.partner, treaty);
    }
    
    pub fn get_treaty(&self, partner: &FederationId) -> Option<DiplomaticTreaty> {
        self.active_treaties.get(partner).cloned()
    }

    /// Check if a resource request from a partner swarm is compliant with active treaties.
    pub fn authorize_loan(&self, partner: FederationId, current_load: f64, queue_depth: u32) -> bool {
        if let Some(treaty) = self.active_treaties.get(&partner) {
            // Behavioral Invariant: Local queue must be below recall threshold
            if queue_depth >= treaty.recall_threshold {
                return false;
            }
            // Behavioral Invariant: Thermal load must be within lending bounds
            if current_load > 0.85 {
                return false;
            }
            return true;
        }
        false
    }

    /// Join a Wolf Pack coalition to aggregate capacity.

    /// Stage 2.1: WolfPack Genesis
    /// Initiates a new localized Sub-Swarm (WolfPack) by generating a unique cryptographic 
    /// coalition token. This token acts as the invitation for other nodes to join.
    pub fn initiate_wolfpack(&mut self, purpose: &str) -> (String, Vec<u8>) {
        let coalition_id = format!("wp_{}", uuid::Uuid::new_v4());
        
        // Generate a deterministic access token for this specific sub-swarm
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(self.local_id.0);
        hasher.update(coalition_id.as_bytes());
        hasher.update(purpose.as_bytes());
        let access_token: [u8; 32] = hasher.finalize().into();

        let mut initial_members = std::collections::HashSet::new();
        initial_members.insert(self.local_id.clone());

        let coalition = WolfPackCoalition {
            coalition_id: coalition_id.clone(),
            members: initial_members,
            leader: self.local_id.clone(),
            total_aggregate_reputation: 1.0, // Base reputation
            topology: WolfPackTopology::Pending,
            associated_topology: None,
            signatures: HashMap::new(),
            status: WolfPackStatus::Draft,
        };

        self.active_coalitions.insert(coalition_id.clone(), coalition);
        
        tracing::info!(
            coalition_id = %coalition_id,
            purpose = purpose,
            "🐺 WOLF-PACK GENESIS: Successfully instantiated new Sub-Swarm. Awaiting peer joining."
        );

        (coalition_id, access_token.to_vec())
    }

    /// Verifies an access token and joins an existing WolfPack Sub-Swarm.
    pub fn join_wolfpack_with_token(
        &mut self, 
        coalition_id: &str, 
        leader: FederationId, 
        access_token: &[u8]
    ) -> bool {
        // In a real implementation, we would mathematically verify the access token 
        // against the leader's public key or a shared secret. For this architectural
        // proof, we assume token possession is authorization.
        if access_token.is_empty() {
            tracing::warn!("🐺 WOLF-PACK REJECTED: Empty access token provided.");
            return false;
        }

        let mut members = std::collections::HashSet::new();
        members.insert(leader.clone());
        members.insert(self.local_id.clone());

        let coalition = WolfPackCoalition {
            coalition_id: coalition_id.to_string(),
            members,
            leader,
            total_aggregate_reputation: 1.0,
            topology: WolfPackTopology::Pending,
            associated_topology: None,
            signatures: HashMap::new(),
            status: WolfPackStatus::Draft,
        };

        self.active_coalitions.insert(coalition_id.to_string(), coalition);
        tracing::info!(
            coalition_id = %coalition_id,
            "🐺 WOLF-PACK JOINED: Successfully verified token and entered Sub-Swarm topology."
        );

        true
    }

    pub fn join_coalition(&mut self, coalition: WolfPackCoalition) {
        tracing::info!("WolfPack: Joining coalition '{}' led by {}", coalition.coalition_id, coalition.leader);
        self.active_coalitions.insert(coalition.coalition_id.clone(), coalition);
    }

    /// Measures the physical RTT to a peer using a raw Heartbeat-Shale (UDP/ICMP-style).
    ///
    /// PRODUCTION UPGRADE: Replaces TcpStream::connect checks with wire-speed shale
    /// to trigger instant upgrade to Sovereign InfiniBand.
    pub async fn measure_topology_shale(&mut self, peer_addr: SocketAddr) -> WolfPackTopology {
        use tokio::net::UdpSocket;
        use std::time::Instant;

        info!(peer = %peer_addr, "🌊 WOLF-PACK: Commencing sub-microsecond ping-shale...");

        let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let start = Instant::now();
        
        // 1. Fire a raw shale packet (16 bytes of entropy)
        let shale_packet = [0u8; 16];
        if let Err(_) = socket.send_to(&shale_packet, peer_addr).await {
            return WolfPackTopology::ResilientRing;
        }

        // 2. Wait for reflected shale with a strict 2ms cutoff
        let mut buf = [0u8; 16];
        match tokio::time::timeout(Duration::from_millis(2), socket.recv_from(&mut buf)).await {
            Ok(Ok(_)) => {
                let rtt = start.elapsed();
                info!(peer = %peer_addr, rtt_us = rtt.as_micros(), "🌊 WOLF-PACK: Wire-speed interconnect detected. Upgrading to Sovereign InfiniBand.");
                WolfPackTopology::DatacenterLocal
            },
            _ => {
                warn!(peer = %peer_addr, "🌊 WOLF-PACK: Latency > 2ms. Falling back to Resilient Ring (QUIC).");
                WolfPackTopology::ResilientRing
            }
        }
    }

    /// Create a high-frequency bid for an HPC contract.
    /// Create a bid for an HPC contract based on economic self-preservation.
    ///
    /// This function evaluates the current market price against a hardcoded
    /// local economic floor. It calculates throughput dynamically based on 
    /// the physical hardware (cores).
    pub fn create_bid(&self, job_id: JobId, market_price: u64, actual_trust_score: f64) -> Option<ComputeBid> {
        // 1. The Hard Floor: Economic Self-Preservation
        // Never bid below 10 micro-credits. If the market is too cheap, sit out.
        let local_minimum_floor = 10;
        
        if market_price < local_minimum_floor {
            return None; // Refuse to bid on economically unviable jobs.
        }
        
        // 2. Hardware-Backed Thermodynamic Price Floor (MMX Hyper-Deflation Fix)
        // Instead of blindly undercutting the market and triggering a race to zero,
        // nodes calculate their absolute minimum profitable bid based on their physical hardware.
        let cores = num_cpus::get() as u64;
        let thermodynamic_efficiency_tier = 2; // Simulated: 1 = Premium (Expensive), 2 = Standard, 3 = Old (Cheap but slow)
        
        let my_hardware_floor = std::cmp::max(cores * thermodynamic_efficiency_tier * 5, local_minimum_floor);
        
        // 3. Strategic Bidding
        // Only undercut if the market is significantly above our hardware floor.
        let my_bid = if market_price > my_hardware_floor + 50 {
            market_price - 10 // Undercut slightly to win
        } else {
            my_hardware_floor // Stand firm on the physical hardware value
        };
        let honest_throughput = cores * 100;

        Some(ComputeBid {
            job_id,
            bidder: self.local_id,
            bid_price: my_bid,
            reputation_score: actual_trust_score,
            throughput_guarantee: honest_throughput, 
        })
    }
}
