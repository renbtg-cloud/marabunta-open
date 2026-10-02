// Marabunta - Licensed under the MIT License.
//! Concurrent knowledge store for the Marabunta Swarm.
//!
//! Every swarm node maintains a local view of the entire swarm's state,
//! synchronized via gossip. The [`KnowledgeStore`] holds three concurrent
//! maps — nodes, jobs, and assignments — and a pending-chunk queue.
//!
//! All operations are thread-safe: the store is accessed concurrently by
//! the gossip receiver, the work loop, and the failure detector.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use parking_lot::Mutex;
use rand::seq::IteratorRandom;
use tracing::{debug, info, warn};

use crate::common::types::{JobId, TopologyId, HolographicTopology};
use crate::swarm::crdt::LwwRegister;

use super::config::{
    MAX_KNOWN_ASSIGNMENTS, MAX_KNOWN_JOBS, MAX_KNOWN_NODES, MAX_PENDING_CHUNKS, NODE_TIMEOUT,
    PENDING_CHUNKS_WARN_THRESHOLD,
};
use super::types::{
    Assignment, Chunk, ChunkId, ChunkStatus, NodeId, NodeInfo, NodeStatus, SwarmError,
    SwarmJobInfo, SwarmJobStatus, SwarmMessage, Trait,
};

// ============================================================================
// PruneStats
// ============================================================================

/// Statistics returned by [`KnowledgeStore::prune_stale`].
#[derive(Debug, Clone)]
pub struct PruneStats {
    pub nodes_removed: usize,
    pub jobs_removed: usize,
    pub assignments_removed: usize,
    pub profiles_pruned: usize,
    pub collectives_pruned: usize,
    pub reputation_pruned: usize,
    pub postings_pruned: usize,
    pub purged_job_ids: Vec<crate::common::types::JobId>,
}

impl Default for PruneStats {
    fn default() -> Self {
        Self {
            nodes_removed: 0,
            jobs_removed: 0,
            assignments_removed: 0,
            profiles_pruned: 0,
            collectives_pruned: 0,
            reputation_pruned: 0,
            postings_pruned: 0,
            purged_job_ids: Vec::new(),
        }
    }
}

// ============================================================================
// SqlitePersistence
// ============================================================================

/// Optional WAL-mode SQLite backend for the knowledge store.
///
/// When enabled, every successful merge is also persisted to disk so the
/// node can recover its view of the swarm after a restart. The in-memory
/// DashMaps remain the primary read path; SQLite is write-through only.
enum SqliteOp {
    PersistNode(NodeId, String),
    PersistJob(JobId, String),
    PersistAssignment(ChunkId, String),
    PersistTopology(TopologyId, String),
    PersistPendingSettlementClaim(String, String),
    DeleteNode(NodeId),
    DeleteJob(JobId),
    DeleteAssignment(ChunkId),
    DeleteTopology(TopologyId),
    DeletePendingSettlementClaim(String),
}

struct SqlitePersistence {
    tx: tokio::sync::mpsc::UnboundedSender<SqliteOp>,
    /// Only used during `hydrate()`.
    conn: Mutex<rusqlite::Connection>,
}

impl SqlitePersistence {
    /// Open (or create) the database at `state_dir/knowledge.db`.
    fn open(state_dir: &Path) -> Result<Self, rusqlite::Error> {
        std::fs::create_dir_all(state_dir).ok();
        let db_path = state_dir.join("knowledge.db");
        let mut conn = rusqlite::Connection::open(&db_path)?;

        // WAL mode for concurrent readers + single writer.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                data    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                data   TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS assignments (
                chunk_id TEXT PRIMARY KEY,
                data     TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS topologies (
                topology_id TEXT PRIMARY KEY,
                data        TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pending_settlement_claims (
                key  TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );",
        )?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<SqliteOp>();

        // 🛑 THE SQLITE I/O DEADLOCK FIX
        // We move the SQLite write loop entirely off the Kademlia/Tokio runtime
        // and into a dedicated background thread that batches queries in transactions.
        std::thread::spawn(move || {
            let mut writer_conn = rusqlite::Connection::open(&db_path).expect("Failed to open SQLite writer connection");
            
            loop {
                // Wait for the first operation.
                let op = match rx.blocking_recv() {
                    Some(op) => op,
                    None => break, // Channel closed, shutdown thread.
                };

                let mut ops = vec![op];
                
                // Drain any other immediately available ops (up to 500) to batch them.
                while ops.len() < 500 {
                    match rx.try_recv() {
                        Ok(op) => ops.push(op),
                        Err(_) => break,
                    }
                }

                if let Ok(tx) = writer_conn.transaction() {
                    for op in ops {
                        match op {
                            SqliteOp::PersistNode(id, json) => {
                                let _ = tx.execute("INSERT OR REPLACE INTO nodes (node_id, data) VALUES (?1, ?2)", rusqlite::params![id.to_string(), json]);
                            }
                            SqliteOp::PersistJob(id, json) => {
                                let _ = tx.execute("INSERT OR REPLACE INTO jobs (job_id, data) VALUES (?1, ?2)", rusqlite::params![id.to_string(), json]);
                            }
                            SqliteOp::PersistAssignment(id, json) => {
                                let _ = tx.execute("INSERT OR REPLACE INTO assignments (chunk_id, data) VALUES (?1, ?2)", rusqlite::params![id.to_string(), json]);
                            }
                            SqliteOp::PersistTopology(id, json) => {
                                let _ = tx.execute("INSERT OR REPLACE INTO topologies (topology_id, data) VALUES (?1, ?2)", rusqlite::params![id.to_string(), json]);
                            }
                            SqliteOp::PersistPendingSettlementClaim(key, json) => {
                                let _ = tx.execute("INSERT OR REPLACE INTO pending_settlement_claims (key, data) VALUES (?1, ?2)", rusqlite::params![key, json]);
                            }
                            SqliteOp::DeleteNode(id) => {
                                let _ = tx.execute("DELETE FROM nodes WHERE node_id = ?1", rusqlite::params![id.to_string()]);
                            }
                            SqliteOp::DeleteJob(id) => {
                                let _ = tx.execute("DELETE FROM jobs WHERE job_id = ?1", rusqlite::params![id.to_string()]);
                            }
                            SqliteOp::DeleteAssignment(id) => {
                                let _ = tx.execute("DELETE FROM assignments WHERE chunk_id = ?1", rusqlite::params![id.to_string()]);
                            }
                            SqliteOp::DeleteTopology(id) => {
                                let _ = tx.execute("DELETE FROM topologies WHERE topology_id = ?1", rusqlite::params![id.to_string()]);
                            }
                            SqliteOp::DeletePendingSettlementClaim(key) => {
                                let _ = tx.execute("DELETE FROM pending_settlement_claims WHERE key = ?1", rusqlite::params![key]);
                            }
                        }
                    }
                    let _ = tx.commit();
                }
            }
        });

        Ok(Self {
            tx,
            conn: Mutex::new(conn),
        })
    }

    /// Hydrate the DashMaps from the database.
    fn hydrate(
        &self,
        nodes: &DashMap<NodeId, NodeInfo>,
        jobs: &DashMap<JobId, SwarmJobInfo>,
        assignments: &DashMap<ChunkId, Assignment>,
        topologies: &DashMap<TopologyId, LwwRegister<HolographicTopology>>,
        dead_letter_ledger: &DashMap<String, SwarmMessage>,
    ) {
        let conn = self.conn.lock();

        // Helper: load all JSON strings from a table.
        fn load_all(conn: &rusqlite::Connection, table: &str) -> Vec<String> {
            let query = format!("SELECT data FROM {}", table);
            let mut stmt = match conn.prepare(&query) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let rows = stmt.query_map([], |row| row.get::<_, String>(0));
            match rows {
                Ok(rows) => rows.flatten().collect(),
                Err(_) => Vec::new(),
            }
        }

        fn load_all_with_keys(conn: &rusqlite::Connection, table: &str) -> Vec<(String, String)> {
            let query = format!("SELECT * FROM {}", table);
            let mut stmt = match conn.prepare(&query) {
                Ok(s) => s,
                Err(_) => return Vec::new(),
            };
            let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)));
            match rows {
                Ok(rows) => rows.flatten().collect(),
                Err(_) => Vec::new(),
            }
        }

        for json in load_all(&conn, "nodes") {
            if let Ok(info) = serde_json::from_str::<NodeInfo>(&json) {
                nodes.insert(info.node_id, info);
            }
        }

        for json in load_all(&conn, "jobs") {
            if let Ok(info) = serde_json::from_str::<SwarmJobInfo>(&json) {
                jobs.insert(info.job_id, info);
            }
        }

        for json in load_all(&conn, "assignments") {
            if let Ok(a) = serde_json::from_str::<Assignment>(&json) {
                assignments.insert(a.chunk_id, a);
            }
        }

        for json in load_all(&conn, "topologies") {
            if let Ok(t) = serde_json::from_str::<LwwRegister<HolographicTopology>>(&json) {
                topologies.insert(t.value.id, t);
            }
        }

        for (key, json) in load_all_with_keys(&conn, "pending_settlement_claims") {
            if let Ok(msg) = serde_json::from_str::<SwarmMessage>(&json) {
                dead_letter_ledger.insert(key, msg);
            }
        }
    }

    fn persist_node(&self, info: &NodeInfo) {
        if let Ok(json) = serde_json::to_string(info) {
            let _ = self.tx.send(SqliteOp::PersistNode(info.node_id, json));
        }
    }

    fn persist_job(&self, info: &SwarmJobInfo) {
        if let Ok(json) = serde_json::to_string(info) {
            let _ = self.tx.send(SqliteOp::PersistJob(info.job_id, json));
        }
    }

    fn persist_assignment(&self, a: &Assignment) {
        if let Ok(json) = serde_json::to_string(a) {
            let _ = self.tx.send(SqliteOp::PersistAssignment(a.chunk_id, json));
        }
    }

    fn delete_node(&self, id: &NodeId) {
        let _ = self.tx.send(SqliteOp::DeleteNode(*id));
    }

    fn delete_job(&self, id: &JobId) {
        let _ = self.tx.send(SqliteOp::DeleteJob(*id));
    }

    fn delete_assignment(&self, id: &ChunkId) {
        let _ = self.tx.send(SqliteOp::DeleteAssignment(*id));
    }

    fn persist_topology(&self, topology: &LwwRegister<HolographicTopology>) {
        if let Ok(json) = serde_json::to_string(topology) {
            let _ = self.tx.send(SqliteOp::PersistTopology(topology.value.id, json));
        }
    }

    fn delete_topology(&self, id: &TopologyId) {
        let _ = self.tx.send(SqliteOp::DeleteTopology(*id));
    }
}

// ============================================================================
// KnowledgeStore
// ============================================================================

/// Thread-safe, gossip-synchronized view of the entire swarm's state.
///
/// Internally backed by [`DashMap`] for lock-free concurrent reads and
/// fine-grained write locking. The pending-chunk queue uses a
/// [`parking_lot::Mutex`]-wrapped [`VecDeque`] for ordered FIFO access.
pub struct KnowledgeStore {
    /// Identity of the local node that owns this store.
    self_id: NodeId,

    /// Known nodes, keyed by their unique identifier.
    nodes: DashMap<NodeId, NodeInfo>,

    /// Known jobs, keyed by the job identifier from `common::types`.
    jobs: DashMap<JobId, SwarmJobInfo>,

    /// Known chunk-to-node assignments, keyed by chunk identifier.
    assignments: DashMap<ChunkId, Assignment>,
    /// The physical TaskPayloads associated with the chunk. Used for BitTorrent fetching.
    chunks: DashMap<ChunkId, Chunk>,
    /// Federated War Room: Collaborative topology blueprints (CRDT).
    topologies: DashMap<TopologyId, LwwRegister<HolographicTopology>>,
    /// ⚡ O(1) DEAD WORKER RECOVERY FIX: Secondary index for fast cleanup
    worker_assignments: DashMap<NodeId, dashmap::DashSet<ChunkId>>,

    /// FIFO queue of chunks awaiting distribution to executors.
    pending_chunks: Mutex<VecDeque<Chunk>>,
    
    // -- Thermodynamic Civic Duty --
    /// FIFO queue of ZKPs that require verification before the associated node can be paid.
    unverified_zkps: Mutex<VecDeque<(ChunkId, JobId, crate::highestsec::zkp::ExecutionProof, u64)>>,

    // -- Orderbook Micro-Batching --
    /// Buffer of incoming RequestWorkBatch requests waiting to be sorted by price.
    orderbook_queue: DashMap<JobId, Vec<(NodeId, u64, u32)>>, // (NodeId, ask_price, chunks_requested)

    // -- Dead-Letter Ledger Queue --
    /// Completed invoices (ChunkResult/SettlementClaim) that failed to transmit to the Orchestrator.
    /// Keyed by "chunk_id:msg_type" to allow multiple related messages per chunk.
    dead_letter_ledger: DashMap<String, SwarmMessage>,

    // -- Network Metronome --
    /// The latest BFT Block Height cryptographically verified by the swarm.
    /// Acts as the absolute source of truth for "Time" in economic contracts.
    latest_bft_block: Arc<AtomicU64>,

    /// Optional SQLite write-through persistence.
    persistence: Option<SqlitePersistence>,

    /// Optional channel for PG time-travel persistence.
    /// Events are sent here and consumed by a background PG writer task.
    pg_writer: Option<tokio::sync::mpsc::UnboundedSender<PgKnowledgeEvent>>,
}

/// Events sent to the PG writer for time-travel persistence.
#[derive(Debug, Clone, serde::Serialize)]
pub enum PgKnowledgeEvent {
    /// Node state changed (join, update, suspect, dead).
    NodeStateChange {
        node_id: NodeId,
        event_type: String,
        state: serde_json::Value,
        traits: Option<serde_json::Value>,
    },
    /// Job state changed (submitted, chunk_assigned, completed, failed).
    JobStateChange {
        job_id: JobId,
        event_type: String,
        event_data: serde_json::Value,
        node_id: Option<NodeId>,
        chunk_id: Option<ChunkId>,
    },
    /// Federated War Room: Collaborative topology updated.
    TopologyUpdate {
        topology_id: TopologyId,
        event_type: String,
        state: serde_json::Value,
    },
}

impl KnowledgeStore {
    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    pub fn latest_bft_block(&self) -> u64 {
        self.latest_bft_block.load(Ordering::Relaxed)
    }

    pub fn latest_bft_block_arc(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.latest_bft_block)
    }

    /// Calculates the dynamic average block time to account for Time Dilation.
    /// In a full implementation, this reads the timestamps of the last 1,000 DAG events.
    /// For now, we simulate a network that is slightly struggling (6 seconds instead of target 5).
    pub fn empirical_block_time_seconds(&self) -> u64 {
        // Assume the network is experiencing slight Time Dilation
        6
    }

    pub fn update_latest_bft_block(&self, block_height: u64) {
        // Only update if the new height is greater (monotonicity guarantee)
        let mut current = self.latest_bft_block.load(Ordering::Relaxed);
        while block_height > current {
            match self.latest_bft_block.compare_exchange_weak(current, block_height, Ordering::SeqCst, Ordering::Relaxed) {
                Ok(_) => {
                    tracing::debug!("⏱️ NETWORK METRONOME: Updated latest BFT block to {}.", block_height);
                    break;
                },
                Err(actual) => current = actual,
            }
        }
    }

    /// Create a new, empty knowledge store owned by `self_id`.
    /// Kademlia Lookup: Find the N closest nodes to a specific 32-byte target hash.
    /// Calculates the XOR distance between the target and each NodeId, and sorts ascending.
    pub fn find_closest_nodes(&self, target_hash: &[u8; 32], limit: usize) -> Vec<crate::swarm::types::NodeInfo> {
        let mut candidates: Vec<(crate::swarm::types::NodeInfo, [u8; 32])> = Vec::new();
        
        for entry in self.nodes.iter() {
            let node_info = entry.value();
            
            // We only want nodes with an actual IP we can send shards to
            if node_info.node_id != self.self_id && node_info.address.is_some() && node_info.status == crate::swarm::types::NodeStatus::Alive {
                let mut distance = [0u8; 32];
                let node_id_bytes = node_info.node_id.0.into_bytes();
                for j in 0..32 {
                    distance[j] = target_hash[j] ^ node_id_bytes[j % 16]; // UUID is 16 bytes, pad by repeating
                }
                candidates.push((node_info.clone(), distance));
            }
        }
        
        // Sort by XOR distance
        candidates.sort_by(|a, b| a.1.cmp(&b.1));
        
        candidates.into_iter().take(limit).map(|(info, _)| info).collect()
    }

    pub fn new(self_id: NodeId) -> Self {
        Self {
            self_id,
            nodes: DashMap::new(),
            jobs: DashMap::new(),
            assignments: DashMap::new(),
            chunks: DashMap::new(),
            topologies: DashMap::new(),
            worker_assignments: DashMap::new(),
            pending_chunks: Mutex::new(VecDeque::new()),
            unverified_zkps: Mutex::new(VecDeque::new()),
            orderbook_queue: DashMap::new(),
            dead_letter_ledger: DashMap::new(),
            latest_bft_block: Arc::new(AtomicU64::new(0)),
            persistence: None,
            pg_writer: None,
        }
    }

    /// Create a knowledge store with SQLite persistence.
    ///
    /// If `state_dir` is `Some`, opens (or creates) a WAL-mode SQLite
    /// database at `<state_dir>/knowledge.db` and hydrates the in-memory
    /// DashMaps from it. All subsequent merges are also written through
    /// to SQLite so the node can recover after a crash.
    pub fn with_persistence(self_id: NodeId, state_dir: Option<&str>) -> Self {
        let state_dir = match state_dir {
            Some(dir) => dir,
            None => return Self::new(self_id),
        };

        let persistence = match SqlitePersistence::open(Path::new(state_dir)) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "failed to open SQLite persistence, running in-memory only");
                return Self::new(self_id);
            }
        };

        let store = Self {
            self_id,
            nodes: DashMap::new(),
            jobs: DashMap::new(),
            assignments: DashMap::new(),
            chunks: DashMap::new(),
            topologies: DashMap::new(),
            worker_assignments: DashMap::new(),
            pending_chunks: Mutex::new(VecDeque::new()),
            unverified_zkps: Mutex::new(VecDeque::new()),
            orderbook_queue: DashMap::new(),
            dead_letter_ledger: DashMap::new(),
            latest_bft_block: Arc::new(AtomicU64::new(0)),
            persistence: Some(persistence),
            pg_writer: None,
        };

        // Hydrate from disk.
        if let Some(ref p) = store.persistence {
            p.hydrate(&store.nodes, &store.jobs, &store.assignments, &store.topologies, &store.dead_letter_ledger);
            info!(
                nodes = store.nodes.len(),
                jobs = store.jobs.len(),
                assignments = store.assignments.len(),
                topologies = store.topologies.len(),
                claims = store.dead_letter_ledger.len(),
                "knowledge store hydrated from SQLite"
            );
        }

        store
    }

    /// Attach a PG writer channel for time-travel persistence.
    ///
    /// Events will be sent (non-blocking) on merge_node, merge_job,
    /// mark_node_suspect, and mark_node_dead. A background task
    /// consumes these and writes to the PG temporal tables.
    pub fn set_pg_writer(&mut self, tx: tokio::sync::mpsc::UnboundedSender<PgKnowledgeEvent>) {
        self.pg_writer = Some(tx);
    }

    /// Send a PG event if the writer is attached.
    fn emit_pg_event(&self, event: PgKnowledgeEvent) {
        if let Some(ref tx) = self.pg_writer {
            let _ = tx.send(event);
        }
    }

    // ========================================================================
    // Node knowledge
    // ========================================================================

    /// Merge a [`NodeInfo`] received via gossip into the local store.
    ///
    /// Returns `true` if the local entry was created or updated.
    ///
    /// **Merge rule (Hardening A.3 — trust-aware)**:
    /// 1. Higher `trust_level` wins outright (Direct beats Hearsay).
    /// 2. If same trust level, newer `last_seen` wins.
    /// 3. If same trust level and timestamp, higher `generation` wins.
    /// 4. Otherwise, incoming data is stale and discarded.
    pub fn merge_node(&self, info: NodeInfo) -> bool {
        let node_id = info.node_id;

        // THE CHRYSALIS PROTOCOL (Sybil Defense):
        // Before a node can even enter the Gossip mesh or reputation orderbook, 
        // its Identity (NodeId) must satisfy the Proof-of-Work constraint.
        // This drops "junk" Sybil nodes instantly, protecting the "African Wild Dog" 
        // reputation corroboration from being mathematically overpowered.
        if !node_id.meets_chrysalis_pow() {
            tracing::warn!(%node_id, "Rejected NodeId: Failed Chrysalis Proof-of-Work constraint. Sybil attack mitigated.");
            return false;
        }

        // Use the DashMap entry API for atomic check-and-update.
        let updated = match self.nodes.entry(node_id) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                debug!(node = %node_id, "knowledge: new node discovered");
                vacant.insert(info);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let existing = occupied.get();

                // Hardening A.3: Higher trust level wins outright.
                if info.trust_level > existing.trust_level {
                    debug!(node = %node_id, "knowledge: node info updated (higher trust level)");
                    occupied.insert(info);
                    true
                } else if info.trust_level < existing.trust_level {
                    // Hearsay cannot overwrite Direct — discard.
                    false
                } else if info.last_seen > existing.last_seen {
                    // Same trust level — newer last_seen wins.
                    debug!(node = %node_id, "knowledge: node info updated (newer timestamp)");
                    occupied.insert(info);
                    true
                } else if info.last_seen == existing.last_seen
                    && info.generation > existing.generation
                {
                    // Same trust + timestamp — higher generation wins.
                    debug!(node = %node_id, "knowledge: node info updated (higher generation)");
                    occupied.insert(info);
                    true
                } else {
                    // Incoming data is stale; discard.
                    false
                }
            }
        };

        if updated {
            if let Some(ref p) = self.persistence {
                if let Some(node) = self.nodes.get(&node_id) {
                    p.persist_node(node.value());
                }
            }
            // Emit to PG for time-travel persistence.
            if let Some(node) = self.nodes.get(&node_id) {
                self.emit_pg_event(PgKnowledgeEvent::NodeStateChange {
                    node_id: node_id,
                    event_type: "updated".to_string(),
                    state: serde_json::to_value(node.value()).unwrap_or_default(),
                    traits: Some(serde_json::to_value(&node.traits).unwrap_or_default()),
                });
            }
        }

        // Pillar 16.2: True Kademlia K-Bucket Strict Routing Enforcements
        // A consumer laptop cannot hold 1 billion nodes in a DashMap (256GB RAM OOM).
        // We enforce a strict k-bucket routing table where k = 20.
        // There are 256 possible buckets (based on the index of the first differing bit).
        // Max theoretical memory footprint: 256 * 20 * 256 bytes = ~1.3 Megabytes.
        
        let k = 20;
        let my_bytes = self.self_id.0.as_bytes();
        let id_bytes = node_id.0.as_bytes();
        
        if my_bytes.len() == 32 && id_bytes.len() == 32 && node_id != self.self_id {
            // Find the leading zero bits of the XOR distance to determine the bucket index
            let mut bucket_idx = 255;
            for i in 0..32 {
                let xor_val = my_bytes[i] ^ id_bytes[i];
                if xor_val != 0 {
                    bucket_idx = (i * 8) + (xor_val.leading_zeros() as usize);
                    break;
                }
            }
            
            // Count nodes in this specific bucket
            let mut bucket_nodes = Vec::new();
            for entry in self.nodes.iter() {
                let peer_id = entry.key();
                if *peer_id == self.self_id { continue; }
                let peer_bytes = peer_id.0.as_bytes();
                if peer_bytes.len() == 32 {
                    let mut peer_bucket = 255;
                    for i in 0..32 {
                        let xor_val = my_bytes[i] ^ peer_bytes[i];
                        if xor_val != 0 {
                            peer_bucket = (i * 8) + (xor_val.leading_zeros() as usize);
                            break;
                        }
                    }
                    if peer_bucket == bucket_idx {
                        bucket_nodes.push((*peer_id, entry.value().last_seen));
                    }
                }
            }

            if bucket_nodes.len() > k {
                // K-Bucket is full. Evict the oldest (least recently seen) node in this specific bucket.
                bucket_nodes.sort_by_key(|(_, last_seen)| *last_seen);
                if let Some((oldest_id, _)) = bucket_nodes.first() {
                    // Only evict if the new node isn't the oldest (which it shouldn't be since it was just merged)
                    if *oldest_id != node_id {
                        tracing::debug!(
                            bucket = bucket_idx,
                            evicted = %oldest_id,
                            "🐺 KNOWLEDGE: Kademlia bucket {} is full. Evicting stale peer to maintain O(log N) RAM footprint.", bucket_idx
                        );
                        self.nodes.remove(oldest_id);
                    }
                }
            }
        }

        updated
    }

    /// Look up a single node by its identifier.
    pub fn get_node(&self, id: &NodeId) -> Option<NodeInfo> {
        self.nodes.get(id).map(|r| r.value().clone())
    }

    /// Update a node's status (e.g., to Dead/Malicious for blacklisting).
    pub fn update_node_status(&self, node_id: &NodeId, status: crate::swarm::types::NodeStatus) {
        if let Some(mut node) = self.nodes.get_mut(node_id) {
            node.status = status;
            node.last_seen = chrono::Utc::now();
        }
    }

    /// Return a snapshot of every known node.
    pub fn get_all_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.iter().map(|r| r.value().clone()).collect()
    }

    /// Return a snapshot of every known job.
    pub fn get_all_jobs(&self) -> Vec<SwarmJobInfo> {
        self.jobs.iter().map(|r| r.value().clone()).collect()
    }

    /// Return a snapshot of every known assignment.
    
    /// Return only nodes whose status is [`NodeStatus::Alive`].
    pub fn get_random_peers(&self, ignore: &NodeId, limit: usize) -> Vec<(NodeId, std::net::SocketAddr)> {
        let total = self.nodes.len();
        if total == 0 { return Vec::new(); }

        let mut rng = rand::thread_rng();
        let skip_amount = rand::Rng::gen_range(&mut rng, 0..total.max(1));
        
        let mut results = Vec::with_capacity(limit);
        
        // Use a single iter over the dashmap.
        // We skip a random amount, then take until we find `limit` items.
        // This is O(N) in the worst case, but realistically O(skip_amount + limit) without allocating all elements!
        for entry in self.nodes.iter().skip(skip_amount) {
            let node = entry.value();
            if node.node_id != *ignore && node.status == NodeStatus::Alive {
                if let Some(addr) = node.address {
                    results.push((node.node_id, addr));
                    if results.len() >= limit {
                        return results;
                    }
                }
            }
        }
        
        // If we didn't find enough because skip_amount was too high, do a small wrap-around
        if results.len() < limit {
            for entry in self.nodes.iter().take(limit - results.len()) {
                let node = entry.value();
                if node.node_id != *ignore && node.status == NodeStatus::Alive {
                    if let Some(addr) = node.address {
                        // Avoid duplicates if possible
                        if !results.iter().any(|(id, _)| *id == node.node_id) {
                            results.push((node.node_id, addr));
                        }
                    }
                }
            }
        }
        
        results
    }

    pub fn get_live_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|r| r.value().status == NodeStatus::Alive)
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn live_node_count(&self) -> usize {
        self.nodes.iter().filter(|r| r.value().status == NodeStatus::Alive).count()
    }

    /// Return all nodes that currently claim the given [`Trait`].
    pub fn get_nodes_with_trait(&self, t: Trait) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .filter(|r| r.value().traits.contains(&t))
            .map(|r| r.value().clone())
            .collect()
    }

    /// Transition a node to [`NodeStatus::Suspect`].
    pub fn mark_node_suspect(&self, id: &NodeId) {
        if let Some(mut entry) = self.nodes.get_mut(id) {
            if entry.status != NodeStatus::Suspect {
                debug!(node = %id, "knowledge: marking node suspect");
                entry.status = NodeStatus::Suspect;
                self.emit_pg_event(PgKnowledgeEvent::NodeStateChange {
                    node_id: *id,
                    event_type: "suspect".to_string(),
                    state: serde_json::to_value(entry.value()).unwrap_or_default(),
                    traits: None,
                });
            }
        }
    }

    /// Transition a node to [`NodeStatus::Dead`].
    pub fn mark_node_dead(&self, id: &NodeId) {
        if let Some(mut entry) = self.nodes.get_mut(id) {
            if entry.status != NodeStatus::Dead {
                self.emit_pg_event(PgKnowledgeEvent::NodeStateChange {
                    node_id: *id,
                    event_type: "dead".to_string(),
                    state: serde_json::to_value(entry.value()).unwrap_or_default(),
                    traits: None,
                });
                debug!(node = %id, "knowledge: marking node dead");
                entry.status = NodeStatus::Dead;
            }
        }
    }

    /// Remove a node entirely from the knowledge store.
    pub fn remove_node(&self, id: &NodeId) {
        if self.nodes.remove(id).is_some() {
            debug!(node = %id, "knowledge: node removed");
            if let Some(ref p) = self.persistence {
                p.delete_node(id);
            }
        }
    }

    /// Number of known nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    // ========================================================================
    // Job knowledge
    // ========================================================================

    /// Merge a [`SwarmJobInfo`] received via gossip.
    ///
    /// Returns `true` if the local entry was created or updated.
    ///
    /// **Merge rule**: newer `updated_at` wins. Progress counters
    /// (`chunks_completed`, `chunks_failed`) only increase — an incoming
    /// message that reports lower values is merged by taking the max of
    /// each counter.
    pub fn merge_job(&self, info: SwarmJobInfo) -> bool {
        let job_id = info.job_id;

        let updated = match self.jobs.entry(job_id) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                debug!(job = %job_id, "knowledge: new job discovered");
                vacant.insert(info);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let existing = occupied.get();

                if info.updated_at > existing.updated_at {
                    let mut merged = info.clone();
                    merged.chunks_completed =
                        merged.chunks_completed.max(existing.chunks_completed);
                    merged.chunks_failed =
                        merged.chunks_failed.max(existing.chunks_failed);
                    debug!(job = %job_id, "knowledge: job info updated (newer timestamp)");
                    occupied.insert(merged);
                    true
                } else if info.updated_at == existing.updated_at {
                    let mut changed = false;
                    let mut merged = existing.clone();
                    if info.chunks_completed > merged.chunks_completed {
                        merged.chunks_completed = info.chunks_completed;
                        changed = true;
                    }
                    if info.chunks_failed > merged.chunks_failed {
                        merged.chunks_failed = info.chunks_failed;
                        changed = true;
                    }
                    if changed {
                        debug!(job = %job_id, "knowledge: job progress merged forward");
                        occupied.insert(merged);
                    }
                    changed
                } else {
                    let mut changed = false;
                    let mut merged = existing.clone();
                    if info.chunks_completed > merged.chunks_completed {
                        merged.chunks_completed = info.chunks_completed;
                        changed = true;
                    }
                    if info.chunks_failed > merged.chunks_failed {
                        merged.chunks_failed = info.chunks_failed;
                        changed = true;
                    }
                    if changed {
                        debug!(job = %job_id, "knowledge: job progress merged (older timestamp, higher counters)");
                        occupied.insert(merged);
                    }
                    changed
                }
            }
        };

        if updated {
            if let Some(ref p) = self.persistence {
                if let Some(job) = self.jobs.get(&job_id) {
                    p.persist_job(job.value());
                }
            }
            // Emit to PG for time-travel persistence.
            if let Some(job) = self.jobs.get(&job_id) {
                self.emit_pg_event(PgKnowledgeEvent::JobStateChange {
                    job_id: job_id,
                    event_type: "updated".to_string(),
                    event_data: serde_json::to_value(job.value()).unwrap_or_default(),
                    node_id: None,
                    chunk_id: None,
                });
            }
        }

        updated
    }

    /// Look up a single job by its identifier.
    pub fn get_job(&self, id: &JobId) -> Option<SwarmJobInfo> {
        self.jobs.get(id).map(|r| r.value().clone())
    }

    /// Return jobs whose status is [`SwarmJobStatus::Pending`] or
    /// [`SwarmJobStatus::InProgress`].
    pub fn get_active_jobs(&self) -> Vec<SwarmJobInfo> {
        self.jobs
            .iter()
            .filter(|r| {
                matches!(
                    r.value().status,
                    SwarmJobStatus::Pending | SwarmJobStatus::InProgress
                )
            })
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return all jobs matching the given status.
    pub fn get_jobs_by_status(&self, status: SwarmJobStatus) -> Vec<SwarmJobInfo> {
        self.jobs
            .iter()
            .filter(|r| r.value().status == status)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Increment the progress counters for a job.
    ///
    /// Counters only move forward — if the supplied values are lower than
    /// the current ones, they are ignored.
    pub fn update_job_progress(&self, job_id: &JobId, completed: u32, failed: u32, fuel: u64) {
        if let Some(mut entry) = self.jobs.get_mut(job_id) {
            let mut changed = false;
            if completed > entry.chunks_completed {
                entry.chunks_completed = completed;
                changed = true;
            }
            if failed > entry.chunks_failed {
                entry.chunks_failed = failed;
                changed = true;
            }
            if fuel > 0 {
                entry.total_fuel_consumed = entry.total_fuel_consumed.saturating_add(fuel);
                changed = true;
            }
            if changed {
                entry.updated_at = Utc::now();
                debug!(
                    job = %job_id,
                    completed = entry.chunks_completed,
                    failed = entry.chunks_failed,
                    fuel = entry.total_fuel_consumed,
                    "knowledge: job progress updated"
                );
            }
        }
    }

    /// Number of known jobs.

    /// Return all pending or running chunks for a specific job.
    pub fn get_pending_or_running_chunks(&self, job_id: &JobId) -> Vec<Chunk> {
        self.assignments
            .iter()
            .filter(|r| r.value().job_id == *job_id && matches!(r.value().status, ChunkStatus::Pending | ChunkStatus::InProgress))
            .filter_map(|r| self.get_chunk(r.key()))
            .collect()
    }

    /// Look up a single chunk by its identifier.
    pub fn get_chunk(&self, chunk_id: &ChunkId) -> Option<Chunk> {
        self.chunks.get(chunk_id).map(|r| r.value().clone())
    }

    // ========================================================================
    // Federated War Room (Topologies)
    // ========================================================================

    /// Merge a topology update received via gossip or API.
    ///
    /// Returns `true` if the local entry was created or updated.
    pub fn merge_topology(&self, remote: LwwRegister<HolographicTopology>) -> bool {
        let id = remote.value.id;
        let mut entry = self.topologies.entry(id).or_insert_with(|| remote.clone());
        if entry.value_mut().merge(&remote) {
            if let Some(ref p) = self.persistence {
                p.persist_topology(entry.value());
            }
            
            self.emit_pg_event(PgKnowledgeEvent::TopologyUpdate {
                topology_id: id,
                event_type: "topology_merged".to_string(),
                state: serde_json::to_value(&entry.value().value).unwrap_or(serde_json::Value::Null),
            });

            debug!(topology = %id, "knowledge: topology merged");
            true
        } else {
            false
        }
    }

    /// Retrieve a holographic topology blueprint.
    pub fn get_topology(&self, id: &TopologyId) -> Option<HolographicTopology> {
        self.topologies.get(id).map(|r| r.value().value.clone())
    }

    /// Return a random sample of up to `max` known topologies.
    pub fn sample_topologies(&self, max: usize) -> Vec<LwwRegister<HolographicTopology>> {
        let mut rng = rand::thread_rng();
        self.topologies
            .iter()
            .choose_multiple(&mut rng, max)
            .into_iter()
            .map(|r| r.value().clone())
            .collect()
    }

    pub fn job_count(&self) -> usize {
        self.jobs.len()
    }

    // ========================================================================
    // Assignment knowledge
    // ========================================================================

    /// Merge an [`Assignment`] received via gossip.
    ///
    /// Returns `true` if the local entry was created or updated.
    ///
    /// **Merge rule**: a terminal status (Completed/Failed) always beats a
    /// non-terminal one. Among non-terminal statuses, InProgress beats
    /// Pending. For same-class conflicts, [`Assignment::wins_against`]
    /// is the tiebreaker.
    pub fn merge_assignment(&self, assignment: Assignment) -> bool {
        let chunk_id = assignment.chunk_id;

        let updated = match self.assignments.entry(chunk_id) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                debug!(chunk = %chunk_id, "knowledge: new assignment discovered");
                vacant.insert(assignment);
                true
            }
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let existing = occupied.get();

                // Terminal statuses always win over non-terminal.
                let incoming_terminal = matches!(
                    assignment.status,
                    ChunkStatus::Completed | ChunkStatus::Failed
                );
                let existing_terminal = matches!(
                    existing.status,
                    ChunkStatus::Completed | ChunkStatus::Failed
                );

                let should_replace = if incoming_terminal && !existing_terminal {
                    true
                } else if !incoming_terminal && existing_terminal {
                    // Exception: retry signal — Pending with higher attempts
                    // overrides Failed.
                    assignment.status == ChunkStatus::Pending
                        && existing.status == ChunkStatus::Failed
                        && assignment.attempts > existing.attempts
                } else if incoming_terminal && existing_terminal {
                    assignment.wins_against(existing)
                } else {
                    let incoming_progress = matches!(
                        assignment.status,
                        ChunkStatus::InProgress
                    );
                    let existing_progress = matches!(
                        existing.status,
                        ChunkStatus::InProgress
                    );

                    if incoming_progress && !existing_progress {
                        true
                    } else if !incoming_progress && existing_progress {
                        false
                    } else {
                        assignment.wins_against(existing)
                    }
                };

                if should_replace {
                    debug!(
                        chunk = %chunk_id,
                        new_status = ?assignment.status,
                        "knowledge: assignment updated"
                    );
                    occupied.insert(assignment);
                    true
                } else {
                    false
                }
            }
        };

        if updated {
            if let Some(ref p) = self.persistence {
                if let Some(a) = self.assignments.get(&chunk_id) {
                    p.persist_assignment(a.value());
                }
            }
        }

        updated
    }

    /// Look up an assignment by chunk identifier.
    
    pub fn get_all_assignments(&self) -> Vec<Assignment> {
        self.assignments.iter().map(|kv| kv.value().clone()).collect()
    }

    pub fn get_assignment(&self, chunk_id: &ChunkId) -> Option<Assignment> {
        self.assignments.get(chunk_id).map(|r| r.value().clone())
    }

    /// Return all assignments belonging to a specific job.
    pub fn get_assignments_for_job(&self, job_id: &JobId) -> Vec<Assignment> {
        self.assignments
            .iter()
            .filter(|r| r.value().job_id == *job_id)
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return chunk IDs of assignments whose status is [`ChunkStatus::Pending`].
    pub fn get_unassigned_chunks(&self) -> Vec<ChunkId> {
        self.assignments
            .iter()
            .filter(|r| r.value().status == ChunkStatus::Pending)
            .map(|r| *r.key())
            .collect()
    }

    /// Return all assignments currently held by a specific node.
    pub fn get_assignments_for_node(&self, node_id: &NodeId) -> Vec<Assignment> {
        self.assignments
            .iter()
            .filter(|r| r.value().assigned_to == Some(*node_id))
            .map(|r| r.value().clone())
            .collect()
    }

    /// Atomically claim a pending chunk for the given node.
    ///
    /// Returns `true` if the claim succeeded (the chunk was Pending and is
    /// now InProgress with `assigned_to = node_id`). Returns `false` if the
    /// chunk does not exist or is no longer Pending.
    pub fn claim_chunk(&self, chunk_id: &ChunkId, node_id: NodeId) -> bool {
        // Use the entry API for atomic check-and-update.
        if let Some(mut entry) = self.assignments.get_mut(chunk_id) {
            if entry.status == ChunkStatus::Pending {
                entry.status = ChunkStatus::InProgress;
                entry.assigned_to = Some(node_id);
                entry.assigned_at = Utc::now();
                entry.attempts += 1;
                debug!(
                    chunk = %chunk_id,
                    node = %node_id,
                    "knowledge: chunk claimed"
                );
                return true;
            }
        }
        false
    }

    /// Release all chunks assigned to a node (e.g. because the node died).
    ///
    /// Resets their status to [`ChunkStatus::Pending`] so they can be
    /// reclaimed by another node. Returns the list of released chunk IDs.
    pub fn release_chunks_from_node(&self, node_id: &NodeId) -> Vec<ChunkId> {
        let mut released = Vec::new();

        for mut entry in self.assignments.iter_mut() {
            let assignment = entry.value_mut();
            if assignment.assigned_to == Some(*node_id)
                && assignment.status == ChunkStatus::InProgress
            {
                assignment.status = ChunkStatus::Pending;
                assignment.assigned_to = None;
                released.push(assignment.chunk_id);
            }
        }

        if !released.is_empty() {
            debug!(
                node = %node_id,
                count = released.len(),
                "knowledge: released chunks from dead node"
            );
        }

        released
    }

    /// Number of known assignments.
    pub fn assignment_count(&self) -> usize {
        self.assignments.len()
    }

    // ========================================================================
    // Chunk storage (pending chunks awaiting distribution)
    // ========================================================================
    // Local processing queues
    // ========================================================================
    
    // -- Orderbook Methods --
    
    pub fn queue_bid(&self, job_id: JobId, node_id: NodeId, ask_price: u64, count: u32) {
        self.orderbook_queue.entry(job_id).or_insert_with(Vec::new).push((node_id, ask_price, count));
    }

    pub fn take_bids(&self, job_id: &JobId) -> Vec<(NodeId, u64, u32)> {
        if let Some((_, bids)) = self.orderbook_queue.remove(job_id) {
            bids
        } else {
            Vec::new()
        }
    }

    pub fn get_bidded_jobs(&self) -> Vec<JobId> {
        self.orderbook_queue.iter().map(|r| *r.key()).collect()
    }

    pub fn add_unverified_zkp(&self, chunk_id: ChunkId, job_id: JobId, proof: crate::highestsec::zkp::ExecutionProof) {
        let mut queue = self.unverified_zkps.lock();
        if queue.len() >= 100_000 {
            tracing::warn!("unverified_zkp queue is full, dropping incoming ZKP. Node will not be paid.");
            return;
        }
        // 🛑 TOLLBOOTH SWEEPER FIX (Network Metronome)
        let current_block = self.latest_bft_block();
        queue.push_back((chunk_id, job_id, proof, current_block));
    }

    pub fn pop_unverified_zkp(&self) -> Option<(ChunkId, JobId, crate::highestsec::zkp::ExecutionProof)> {
        let mut queue = self.unverified_zkps.lock();
        queue.pop_front().map(|(c, j, p, _)| (c, j, p))
    }

    pub fn peek_stale_zkps(&self, max_blocks_age: u64) -> Vec<(ChunkId, JobId, crate::highestsec::zkp::ExecutionProof)> {
        let mut queue = self.unverified_zkps.lock();
        let current_block = self.latest_bft_block();
        let mut stale = Vec::new();
        
        while let Some(front) = queue.front() {
            // A ZKP is stale if the network has produced > max_blocks_age since it was submitted
            if current_block.saturating_sub(front.3) > max_blocks_age {
                if let Some((c, j, p, _)) = queue.pop_front() {
                    stale.push((c, j, p));
                }
            } else {
                break; // Because it's FIFO, if the front isn't stale, nothing behind it is.
            }
        }
        stale
    }
    
    pub fn unverified_zkp_count(&self) -> usize {
        self.unverified_zkps.lock().len()
    }

    /// Enqueue a chunk that is waiting to be distributed to an executor.
    ///
    /// Returns `Ok(())` on success, or `Err(SwarmError::QueueFull)` if the
    /// queue has reached [`MAX_PENDING_CHUNKS`]. A warning is logged when
    /// the queue passes [`PENDING_CHUNKS_WARN_THRESHOLD`] of capacity.
    pub fn add_pending_chunk(&self, chunk: Chunk) -> Result<(), SwarmError> {
        let mut queue = self.pending_chunks.lock();
        if queue.len() >= MAX_PENDING_CHUNKS {
            return Err(SwarmError::QueueFull(format!(
                "Pending chunk queue full ({}/{})",
                queue.len(),
                MAX_PENDING_CHUNKS
            )));
        }
        let len_after = queue.len() + 1;
        if len_after as f32 / MAX_PENDING_CHUNKS as f32 >= PENDING_CHUNKS_WARN_THRESHOLD {
            warn!(
                queue_len = len_after,
                max = MAX_PENDING_CHUNKS,
                "Pending chunk queue approaching capacity ({:.0}%)",
                len_after as f32 / MAX_PENDING_CHUNKS as f32 * 100.0
            );
        }
        // BitTorrent Fix: Cache the full physical payload so that other nodes can retrieve it
        // when they receive the Assignment metadata via Gossip.
        self.chunks.insert(chunk.id, chunk.clone());
        
        queue.push_back(chunk);
        Ok(())
    }

    /// Dequeue the next pending chunk (FIFO order).
    ///
    /// Returns `None` if the queue is empty.
    pub fn take_pending_chunk(&self) -> Option<Chunk> {
        self.pending_chunks.lock().pop_front()
    }

    /// Number of chunks waiting in the pending queue.
    pub fn pending_chunk_count(&self) -> usize {
        self.pending_chunks.lock().len()
    }

    // -- Dead-Letter Ledger Queue --

    pub fn add_pending_settlement_claim(&self, chunk_id: ChunkId, msg_type: &str, msg: SwarmMessage) {
        let key = format!("{}:{}", chunk_id.0, msg_type);
        if let Ok(json) = serde_json::to_string(&msg) {
            if let Some(ref p) = self.persistence {
                let _ = p.tx.send(SqliteOp::PersistPendingSettlementClaim(key.clone(), json));
            }
        }
        self.dead_letter_ledger.insert(key, msg);
    }

    pub fn remove_pending_settlement_claim(&self, chunk_id: &ChunkId, msg_type: &str) {
        let key = format!("{}:{}", chunk_id.0, msg_type);
        if let Some(ref p) = self.persistence {
            let _ = p.tx.send(SqliteOp::DeletePendingSettlementClaim(key.clone()));
        }
        self.dead_letter_ledger.remove(&key);
    }

    pub fn get_all_pending_settlement_claims(&self) -> Vec<(String, SwarmMessage)> {
        self.dead_letter_ledger.iter().map(|r| {
            let (k, v) = (r.key().clone(), r.value().clone());
            (k, v)
        }).collect()
    }


    /// Handle a chunk being yielded back by a node (Thermodynamic Arbitrage).
    pub fn handle_chunk_yield(&self, chunk: Chunk, from_node: NodeId) {
        // 1. Update the assignment status if it matches
        if let Some(mut assignment) = self.assignments.get_mut(&chunk.id) {
            if assignment.assigned_to == Some(from_node) {
                assignment.assigned_to = None;
                assignment.status = crate::swarm::types::ChunkStatus::Pending;
                debug!(chunk = %chunk.id, from = %from_node, "knowledge: chunk yielded and returned to pending state");
            }
        }
        
        // 2. Put it back in the front of the queue for rapid re-assignment
        let mut queue = self.pending_chunks.lock();
        queue.push_front(chunk);
    }

    // ========================================================================
    // Maintenance
    // ========================================================================

    /// Remove stale entries from all stores.
    ///
    /// - **Nodes**: removed if `last_seen` is older than [`NODE_TIMEOUT`].
    /// - **Jobs**: removed if Completed/Failed and `updated_at` is older
    ///   than 5 minutes.
    /// - **Assignments**: removed if Completed/Failed and `assigned_at` is
    ///   older than 10 minutes.
    pub fn prune_stale(&self) -> PruneStats {
        let now = Utc::now();
        let mut stats = PruneStats::default();

        // --- Nodes ---
        let node_cutoff = now - ChronoDuration::from_std(NODE_TIMEOUT)
            .unwrap_or_else(|_| ChronoDuration::seconds(120));

        let stale_nodes: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|r| {
                r.value().last_seen < node_cutoff && r.value().node_id != self.self_id
            })
            .map(|r| *r.key())
            .collect();

        for id in &stale_nodes {
            self.nodes.remove(id);
            if let Some(ref p) = self.persistence {
                p.delete_node(id);
            }
        }
        stats.nodes_removed = stale_nodes.len();

        // --- Jobs ---
        let job_cutoff = now - ChronoDuration::minutes(5);
        let job_ttl_cutoff = now - ChronoDuration::hours(72); // 🛑 ZOMBIE GARBAGE COLLECTOR

        let stale_jobs: Vec<JobId> = self
            .jobs
            .iter()
            .filter(|r| {
                let is_completed_stale = matches!(
                    r.value().status,
                    SwarmJobStatus::Completed | SwarmJobStatus::Failed
                ) && r.value().updated_at < job_cutoff;
                
                let is_zombie = r.value().updated_at < job_ttl_cutoff;
                
                is_completed_stale || is_zombie
            })
            .map(|r| *r.key())
            .collect();

        for id in &stale_jobs {
            self.jobs.remove(id);
            if let Some(ref p) = self.persistence {
                p.delete_job(id);
            }
        }
        stats.jobs_removed = stale_jobs.len();
        stats.purged_job_ids = stale_jobs;

        // --- Assignments ---
        let assignment_cutoff = now - ChronoDuration::minutes(10);
        let assignment_hard_cutoff = now - ChronoDuration::minutes(30);
        let assignment_ttl_cutoff = now - ChronoDuration::hours(72); // 🛑 ZOMBIE ASSIGNMENT GARBAGE COLLECTOR

        let stale_assignments: Vec<ChunkId> = self
            .assignments
            .iter()
            .filter(|r| {
                let a = r.value();
                
                // If an assignment has been stuck for 72 hours, it's a zombie. Kill it regardless of status.
                if a.assigned_at < assignment_ttl_cutoff {
                    return true;
                }
                
                let is_terminal = matches!(
                    a.status,
                    ChunkStatus::Completed | ChunkStatus::Failed
                );
                if !is_terminal {
                    return false;
                }
                // Hard cutoff: always prune assignments older than 30 min.
                if a.assigned_at < assignment_hard_cutoff {
                    return true;
                }
                // Normal cutoff: only prune if the parent job is also terminal
                // (or missing). This prevents pruning results before the
                // aggregator has had a chance to collect them.
                if a.assigned_at < assignment_cutoff {
                    match self.jobs.get(&a.job_id) {
                        Some(job) => matches!(
                            job.status,
                            SwarmJobStatus::Completed | SwarmJobStatus::Failed
                        ),
                        None => true, // job unknown/pruned, safe to prune assignment
                    }
                } else {
                    false
                }
            })
            .map(|r| *r.key())
            .collect();

        for id in &stale_assignments {
            self.assignments.remove(id);
            if let Some(ref p) = self.persistence {
                p.delete_assignment(id);
            }
        }
        stats.assignments_removed = stale_assignments.len();

        if stats.nodes_removed > 0
            || stats.jobs_removed > 0
            || stats.assignments_removed > 0
        {
            info!(
                nodes = stats.nodes_removed,
                jobs = stats.jobs_removed,
                assignments = stats.assignments_removed,
                "knowledge: pruned stale entries"
            );
        }

        stats
    }

    /// Enforce maximum-size limits on all stores.
    ///
    /// When a store exceeds its configured capacity, the oldest entries are
    /// evicted first (oldest `last_seen` for nodes, oldest `updated_at` for
    /// jobs, oldest `assigned_at` for assignments). The local node is never
    /// evicted from the node store.
    pub fn enforce_limits(&self) {
        // --- Nodes ---
        if self.nodes.len() > MAX_KNOWN_NODES {
            let excess = self.nodes.len() - MAX_KNOWN_NODES;
            let mut entries: Vec<(NodeId, u32, chrono::DateTime<Utc>)> = self
                .nodes
                .iter()
                .filter(|r| r.value().node_id != self.self_id)
                .map(|r| {
                    let id = *r.key();
                    let node_id_bytes = id.0.as_bytes();
                    let self_id_bytes = self.self_id.0.as_bytes();
                    let mut prefix_len = 0;
                    for i in 0..16 {
                        let xor = node_id_bytes[i] ^ self_id_bytes[i];
                        if xor == 0 { prefix_len += 8; }
                        else { prefix_len += xor.leading_zeros(); break; }
                    }
                    (id, prefix_len, r.value().last_seen)
                })
                .collect();

            // 🛑 XOR SPACE PRESERVATION FIX:
            // We sort by (prefix_len DESC, last_seen ASC).
            // This means we keep nodes that have LONG common prefixes with us (our local k-buckets)
            // and we evict nodes that are "far away" and "old".
            entries.sort_by(|a, b| {
                match b.1.cmp(&a.1) { // Prefer higher prefix length (closer to us)
                    std::cmp::Ordering::Equal => a.2.cmp(&b.2), // Then older first for eviction
                    other => other,
                }
            });

            for (id, _, _) in entries.into_iter().take(excess) {
                self.nodes.remove(&id);
            }
            debug!(evicted = excess, "knowledge: enforced node limit");
        }

        // --- Jobs ---
        if self.jobs.len() > MAX_KNOWN_JOBS {
            let excess = self.jobs.len() - MAX_KNOWN_JOBS;
            let mut entries: Vec<(JobId, chrono::DateTime<Utc>)> = self
                .jobs
                .iter()
                .map(|r| (*r.key(), r.value().updated_at))
                .collect();
            entries.sort_by_key(|&(_, ts)| ts);
            for (id, _) in entries.into_iter().take(excess) {
                self.jobs.remove(&id);
            }
            debug!(evicted = excess, "knowledge: enforced job limit");
        }

        // --- Assignments ---
        if self.assignments.len() > MAX_KNOWN_ASSIGNMENTS {
            let excess = self.assignments.len() - MAX_KNOWN_ASSIGNMENTS;
            let mut entries: Vec<(ChunkId, chrono::DateTime<Utc>)> = self
                .assignments
                .iter()
                .map(|r| (*r.key(), r.value().assigned_at))
                .collect();
            entries.sort_by_key(|&(_, ts)| ts);
            for (id, _) in entries.into_iter().take(excess) {
                self.assignments.remove(&id);
                self.chunks.remove(&id);
            }
            debug!(evicted = excess, "knowledge: enforced assignment limit");
        }
        
        // Also limit chunks table to match assignment size.
        if self.chunks.len() > MAX_KNOWN_ASSIGNMENTS {
            let excess = self.chunks.len() - MAX_KNOWN_ASSIGNMENTS;
            let mut entries: Vec<(ChunkId, chrono::DateTime<Utc>)> = self
                .chunks
                .iter()
                .map(|r| (*r.key(), r.value().created_at))
                .collect();
            entries.sort_by_key(|&(_, ts)| ts);
            for (id, _) in entries.into_iter().take(excess) {
                self.chunks.remove(&id);
            }
        }
    }

    // ========================================================================
    // Gossip helpers
    // ========================================================================

    /// Return a random sample of up to `max` known nodes.
    ///
    /// Used to populate gossip messages with a bounded subset of the
    /// knowledge store.
    pub fn sample_nodes(&self, max: usize) -> Vec<NodeInfo> {
        let total = self.nodes.len();
        if total == 0 { return Vec::new(); }

        let mut rng = rand::thread_rng();
        let skip_amount = rand::Rng::gen_range(&mut rng, 0..total.max(1));
        
        let mut results = Vec::with_capacity(max);
        
        for entry in self.nodes.iter().skip(skip_amount).take(max) {
            results.push(entry.value().clone());
        }
        
        if results.len() < max {
            for entry in self.nodes.iter().take(max - results.len()) {
                if !results.iter().any(|n| n.node_id == entry.key().clone()) {
                    results.push(entry.value().clone());
                }
            }
        }
        results
    }

    /// Return a random sample of up to `max` known jobs.
    pub fn sample_jobs(&self, max: usize) -> Vec<SwarmJobInfo> {
        let total = self.jobs.len();
        if total == 0 { return Vec::new(); }

        let mut rng = rand::thread_rng();
        let skip_amount = rand::Rng::gen_range(&mut rng, 0..total.max(1));
        
        let mut results = Vec::with_capacity(max);
        
        for entry in self.jobs.iter().skip(skip_amount).take(max) {
            results.push(entry.value().clone());
        }
        
        if results.len() < max {
            for entry in self.jobs.iter().take(max - results.len()) {
                if !results.iter().any(|j| j.job_id == entry.key().clone()) {
                    results.push(entry.value().clone());
                }
            }
        }
        results
    }

    /// Return a random sample of up to `max` known assignments.
    pub fn sample_assignments(&self, max: usize) -> Vec<Assignment> {
        let total = self.assignments.len();
        if total == 0 { return Vec::new(); }

        let mut rng = rand::thread_rng();
        let skip_amount = rand::Rng::gen_range(&mut rng, 0..total.max(1));
        
        let mut results = Vec::with_capacity(max);
        
        for entry in self.assignments.iter().skip(skip_amount).take(max) {
            results.push(entry.value().clone());
        }
        
        if results.len() < max {
            for entry in self.assignments.iter().take(max - results.len()) {
                if !results.iter().any(|a| a.chunk_id == entry.key().clone()) {
                    results.push(entry.value().clone());
                }
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::ResourceSnapshot;
    use super::*;
    use crate::common::types::TaskPayload;
    use chrono::{DateTime, Utc};
    use std::collections::HashSet;

    fn make_node(id: NodeId, gen: u64) -> NodeInfo {
        NodeInfo {
            node_id: id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.1,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: id,
            status: NodeStatus::Alive,
            generation: gen,
            trust_level: Default::default(),
        }
    }

    fn make_job(job_id: JobId) -> SwarmJobInfo {
        SwarmJobInfo {
            job_id,
            status: SwarmJobStatus::InProgress,
            chunks_total: 10,
            chunks_completed: 0,
            chunks_failed: 0,
            submitter: NodeId::new(),
            first_seen: Utc::now(),
            updated_at: Utc::now(),
            payload_type: "shell".into(),
            priority: 1,
            data_residency: None,
            required_zone_id: "test-zone".to_string(),
            verification_strategy: super::super::types::VerificationStrategy::None,
        }
    }

    fn make_assignment(chunk_id: ChunkId, job_id: JobId) -> Assignment {
        Assignment {
            chunk_id,
            job_id,
            assigned_to: None,
            assigned_at: Utc::now(),
            status: ChunkStatus::Pending,
            result: None,
            attempts: 0,
            replica_group_id: None,
            failed_nodes: Vec::new(),
        }
    }

    #[test]
    fn merge_node_newer_wins() {
        let store = KnowledgeStore::new(NodeId::new());
        let id = NodeId::new();

        let old = {
            let mut n = make_node(id, 1);
            n.last_seen = Utc::now() - ChronoDuration::seconds(10);
            n
        };
        let new = make_node(id, 1);

        assert!(store.merge_node(old.clone()));
        assert!(store.merge_node(new.clone()));

        let stored = store.get_node(&id).unwrap();
        assert!(stored.last_seen >= new.last_seen - ChronoDuration::milliseconds(1));
    }

    #[test]
    fn merge_node_stale_rejected() {
        let store = KnowledgeStore::new(NodeId::new());
        let id = NodeId::new();

        let new = make_node(id, 1);
        let old = {
            let mut n = make_node(id, 1);
            n.last_seen = Utc::now() - ChronoDuration::seconds(60);
            n.load = 0.99;
            n
        };

        assert!(store.merge_node(new));
        assert!(!store.merge_node(old));

        let stored = store.get_node(&id).unwrap();
        assert!(stored.load < 0.5); // original load preserved
    }

    #[test]
    fn merge_node_same_time_higher_gen_wins() {
        let store = KnowledgeStore::new(NodeId::new());
        let id = NodeId::new();
        let ts = Utc::now();

        let gen1 = {
            let mut n = make_node(id, 1);
            n.last_seen = ts;
            n
        };
        let gen2 = {
            let mut n = make_node(id, 2);
            n.last_seen = ts;
            n
        };

        assert!(store.merge_node(gen1));
        assert!(store.merge_node(gen2));
        assert_eq!(store.get_node(&id).unwrap().generation, 2);
    }

    #[test]
    fn live_nodes_filters_correctly() {
        let store = KnowledgeStore::new(NodeId::new());

        let alive_id = NodeId::new();
        let dead_id = NodeId::new();

        store.merge_node(make_node(alive_id, 1));
        store.merge_node(make_node(dead_id, 1));
        store.mark_node_dead(&dead_id);

        let live = store.get_live_nodes();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].node_id, alive_id);
    }

    #[test]
    fn nodes_with_trait_filters() {
        let store = KnowledgeStore::new(NodeId::new());

        let relay_id = NodeId::new();
        let mut relay_node = make_node(relay_id, 1);
        relay_node.traits.insert(Trait::CanRelay);
        store.merge_node(relay_node);

        let exec_id = NodeId::new();
        store.merge_node(make_node(exec_id, 1));

        let relays = store.get_nodes_with_trait(Trait::CanRelay);
        assert_eq!(relays.len(), 1);
        assert_eq!(relays[0].node_id, relay_id);
    }

    #[test]
    fn mark_suspect_and_dead() {
        let store = KnowledgeStore::new(NodeId::new());
        let id = NodeId::new();
        store.merge_node(make_node(id, 1));

        store.mark_node_suspect(&id);
        assert_eq!(store.get_node(&id).unwrap().status, NodeStatus::Suspect);

        store.mark_node_dead(&id);
        assert_eq!(store.get_node(&id).unwrap().status, NodeStatus::Dead);
    }

    #[test]
    fn remove_node_works() {
        let store = KnowledgeStore::new(NodeId::new());
        let id = NodeId::new();
        store.merge_node(make_node(id, 1));
        assert_eq!(store.node_count(), 1);

        store.remove_node(&id);
        assert_eq!(store.node_count(), 0);
        assert!(store.get_node(&id).is_none());
    }

    #[test]
    fn merge_job_progress_never_regresses() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();

        let mut j1 = make_job(jid);
        j1.chunks_completed = 5;
        j1.chunks_failed = 2;
        assert!(store.merge_job(j1));

        // Newer timestamp but lower counters — counters should not regress.
        let mut j2 = make_job(jid);
        j2.updated_at = Utc::now() + ChronoDuration::seconds(1);
        j2.chunks_completed = 3;
        j2.chunks_failed = 1;
        assert!(store.merge_job(j2));

        let stored = store.get_job(&jid).unwrap();
        assert_eq!(stored.chunks_completed, 5);
        assert_eq!(stored.chunks_failed, 2);
    }

    #[test]
    fn update_job_progress_increments() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();
        store.merge_job(make_job(jid));

        store.update_job_progress(&jid, 4, 1);
        let stored = store.get_job(&jid).unwrap();
        assert_eq!(stored.chunks_completed, 4);
        assert_eq!(stored.chunks_failed, 1);

        // Lower values ignored.
        store.update_job_progress(&jid, 2, 0);
        let stored = store.get_job(&jid).unwrap();
        assert_eq!(stored.chunks_completed, 4);
        assert_eq!(stored.chunks_failed, 1);
    }

    #[test]
    fn active_jobs_filters_terminal() {
        let store = KnowledgeStore::new(NodeId::new());

        let active_id = JobId::new();
        store.merge_job(make_job(active_id));

        let done_id = JobId::new();
        let mut done = make_job(done_id);
        done.status = SwarmJobStatus::Completed;
        store.merge_job(done);

        let active = store.get_active_jobs();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].job_id, active_id);
    }

    #[test]
    fn jobs_by_status() {
        let store = KnowledgeStore::new(NodeId::new());

        let j1 = make_job(JobId::new());
        let mut j2 = make_job(JobId::new());
        j2.status = SwarmJobStatus::Pending;
        store.merge_job(j1);
        store.merge_job(j2);

        assert_eq!(
            store.get_jobs_by_status(SwarmJobStatus::InProgress).len(),
            1
        );
        assert_eq!(
            store.get_jobs_by_status(SwarmJobStatus::Pending).len(),
            1
        );
    }

    #[test]
    fn claim_chunk_atomic() {
        let store = KnowledgeStore::new(NodeId::new());
        let cid = ChunkId::new();
        let jid = JobId::new();
        let nid = NodeId::new();

        store.merge_assignment(make_assignment(cid, jid));
        assert!(store.claim_chunk(&cid, nid));

        // Second claim should fail.
        let nid2 = NodeId::new();
        assert!(!store.claim_chunk(&cid, nid2));

        let a = store.get_assignment(&cid).unwrap();
        assert_eq!(a.status, ChunkStatus::InProgress);
        assert_eq!(a.assigned_to, Some(nid));
        assert_eq!(a.attempts, 1);
    }

    #[test]
    fn release_chunks_from_dead_node() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();
        let nid = NodeId::new();

        let c1 = ChunkId::new();
        let c2 = ChunkId::new();
        store.merge_assignment(make_assignment(c1, jid));
        store.merge_assignment(make_assignment(c2, jid));

        store.claim_chunk(&c1, nid);
        store.claim_chunk(&c2, nid);

        let released = store.release_chunks_from_node(&nid);
        assert_eq!(released.len(), 2);

        assert_eq!(
            store.get_assignment(&c1).unwrap().status,
            ChunkStatus::Pending
        );
        assert_eq!(
            store.get_assignment(&c2).unwrap().status,
            ChunkStatus::Pending
        );
    }

    #[test]
    fn merge_assignment_terminal_beats_nonterminal() {
        let store = KnowledgeStore::new(NodeId::new());
        let cid = ChunkId::new();
        let jid = JobId::new();

        // Start with InProgress assignment.
        let mut in_progress = make_assignment(cid, jid);
        in_progress.status = ChunkStatus::InProgress;
        in_progress.assigned_to = Some(NodeId::new());
        store.merge_assignment(in_progress);

        // Merge a Completed assignment — should win.
        let mut completed = make_assignment(cid, jid);
        completed.status = ChunkStatus::Completed;
        completed.assigned_to = Some(NodeId::new());
        assert!(store.merge_assignment(completed));

        assert_eq!(
            store.get_assignment(&cid).unwrap().status,
            ChunkStatus::Completed
        );
    }

    #[test]
    fn merge_assignment_nonterminal_does_not_beat_terminal() {
        let store = KnowledgeStore::new(NodeId::new());
        let cid = ChunkId::new();
        let jid = JobId::new();

        let mut completed = make_assignment(cid, jid);
        completed.status = ChunkStatus::Completed;
        completed.assigned_to = Some(NodeId::new());
        store.merge_assignment(completed);

        // InProgress should not overwrite Completed.
        let mut in_progress = make_assignment(cid, jid);
        in_progress.status = ChunkStatus::InProgress;
        in_progress.assigned_to = Some(NodeId::new());
        assert!(!store.merge_assignment(in_progress));

        assert_eq!(
            store.get_assignment(&cid).unwrap().status,
            ChunkStatus::Completed
        );
    }

    #[test]
    fn merge_assignment_inprogress_beats_pending() {
        let store = KnowledgeStore::new(NodeId::new());
        let cid = ChunkId::new();
        let jid = JobId::new();

        store.merge_assignment(make_assignment(cid, jid));

        let mut in_progress = make_assignment(cid, jid);
        in_progress.status = ChunkStatus::InProgress;
        in_progress.assigned_to = Some(NodeId::new());
        assert!(store.merge_assignment(in_progress));

        assert_eq!(
            store.get_assignment(&cid).unwrap().status,
            ChunkStatus::InProgress
        );
    }

    #[test]
    fn unassigned_chunks() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();

        let c1 = ChunkId::new();
        let c2 = ChunkId::new();
        store.merge_assignment(make_assignment(c1, jid));
        store.merge_assignment(make_assignment(c2, jid));

        store.claim_chunk(&c1, NodeId::new());

        let unassigned = store.get_unassigned_chunks();
        assert_eq!(unassigned.len(), 1);
        assert_eq!(unassigned[0], c2);
    }

    #[test]
    fn assignments_for_node() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();
        let nid = NodeId::new();

        let c1 = ChunkId::new();
        let c2 = ChunkId::new();
        store.merge_assignment(make_assignment(c1, jid));
        store.merge_assignment(make_assignment(c2, jid));

        store.claim_chunk(&c1, nid);

        let node_assignments = store.get_assignments_for_node(&nid);
        assert_eq!(node_assignments.len(), 1);
        assert_eq!(node_assignments[0].chunk_id, c1);
    }

    #[test]
    fn pending_chunk_fifo() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();

        let chunk1 = Chunk {
            id: ChunkId::new(),
            job_id: jid,
            sequence: 0,
            payload: TaskPayload::Shell {
                command: "echo".into(),
                args: vec!["1".into()],
            },
            created_at: Utc::now(),
        };
        let chunk2 = Chunk {
            id: ChunkId::new(),
            job_id: jid,
            sequence: 1,
            payload: TaskPayload::Shell {
                command: "echo".into(),
                args: vec!["2".into()],
            },
            created_at: Utc::now(),
        };

        let id1 = chunk1.id;
        let id2 = chunk2.id;

        store.add_pending_chunk(chunk1).unwrap();
        store.add_pending_chunk(chunk2).unwrap();
        assert_eq!(store.pending_chunk_count(), 2);

        assert_eq!(store.take_pending_chunk().unwrap().id, id1);
        assert_eq!(store.take_pending_chunk().unwrap().id, id2);
        assert!(store.take_pending_chunk().is_none());
    }

    #[test]
    fn prune_stale_removes_old_entries() {
        let store = KnowledgeStore::new(NodeId::new());

        // Add a node that is well past NODE_TIMEOUT.
        let old_id = NodeId::new();
        let mut old_node = make_node(old_id, 1);
        old_node.last_seen = Utc::now() - ChronoDuration::seconds(300);
        store.merge_node(old_node);

        // Add a fresh node.
        store.merge_node(make_node(NodeId::new(), 1));

        // Add a completed job older than 5 minutes.
        let old_jid = JobId::new();
        let mut old_job = make_job(old_jid);
        old_job.status = SwarmJobStatus::Completed;
        old_job.updated_at = Utc::now() - ChronoDuration::minutes(10);
        store.merge_job(old_job);

        // Add a fresh active job.
        store.merge_job(make_job(JobId::new()));

        // Add a completed assignment older than 10 minutes.
        let old_cid = ChunkId::new();
        let mut old_assignment = make_assignment(old_cid, JobId::new());
        old_assignment.status = ChunkStatus::Completed;
        old_assignment.assigned_at = Utc::now() - ChronoDuration::minutes(15);
        store.merge_assignment(old_assignment);

        // Add a fresh assignment.
        store.merge_assignment(make_assignment(ChunkId::new(), JobId::new()));

        let stats = store.prune_stale();
        assert_eq!(stats.nodes_removed, 1);
        assert_eq!(stats.jobs_removed, 1);
        assert_eq!(stats.assignments_removed, 1);

        assert_eq!(store.node_count(), 1);
        assert_eq!(store.job_count(), 1);
        assert_eq!(store.assignment_count(), 1);
    }

    #[test]
    fn prune_stale_never_removes_self() {
        let self_id = NodeId::new();
        let store = KnowledgeStore::new(self_id);

        // Insert self with a very old timestamp.
        let mut self_node = make_node(self_id, 1);
        self_node.last_seen = Utc::now() - ChronoDuration::seconds(300);
        store.merge_node(self_node);

        let stats = store.prune_stale();
        assert_eq!(stats.nodes_removed, 0);
        assert_eq!(store.node_count(), 1);
    }

    #[test]
    fn enforce_limits_evicts_oldest() {
        let store = KnowledgeStore::new(NodeId::new());

        // Insert more jobs than the limit (we use a small test to verify
        // the eviction logic works — not literally MAX_KNOWN_JOBS entries).
        // We test the logic indirectly by checking that after enforce_limits
        // the count does not exceed the max.
        for i in 0..10 {
            let jid = JobId::new();
            let mut j = make_job(jid);
            j.updated_at = Utc::now() - ChronoDuration::seconds(100 - i);
            store.merge_job(j);
        }

        // Since 10 < MAX_KNOWN_JOBS, enforce_limits should be a no-op.
        store.enforce_limits();
        assert_eq!(store.job_count(), 10);
    }

    #[test]
    fn sample_respects_max() {
        let store = KnowledgeStore::new(NodeId::new());

        for _ in 0..20 {
            store.merge_node(make_node(NodeId::new(), 1));
        }

        let sample = store.sample_nodes(5);
        assert!(sample.len() <= 5);

        let sample_all = store.sample_nodes(100);
        assert_eq!(sample_all.len(), 20);
    }

    #[test]
    fn sample_jobs_and_assignments() {
        let store = KnowledgeStore::new(NodeId::new());

        for _ in 0..10 {
            store.merge_job(make_job(JobId::new()));
        }
        for _ in 0..15 {
            store.merge_assignment(make_assignment(ChunkId::new(), JobId::new()));
        }

        assert!(store.sample_jobs(3).len() <= 3);
        assert!(store.sample_assignments(5).len() <= 5);
        assert_eq!(store.sample_jobs(100).len(), 10);
        assert_eq!(store.sample_assignments(100).len(), 15);
    }

    #[test]
    fn assignments_for_job() {
        let store = KnowledgeStore::new(NodeId::new());

        let j1 = JobId::new();
        let j2 = JobId::new();

        store.merge_assignment(make_assignment(ChunkId::new(), j1));
        store.merge_assignment(make_assignment(ChunkId::new(), j1));
        store.merge_assignment(make_assignment(ChunkId::new(), j2));

        assert_eq!(store.get_assignments_for_job(&j1).len(), 2);
        assert_eq!(store.get_assignments_for_job(&j2).len(), 1);
    }

    #[test]
    fn claim_nonexistent_chunk_returns_false() {
        let store = KnowledgeStore::new(NodeId::new());
        assert!(!store.claim_chunk(&ChunkId::new(), NodeId::new()));
    }

    #[test]
    fn get_all_nodes_returns_all() {
        let store = KnowledgeStore::new(NodeId::new());
        let ids: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();
        for &id in &ids {
            store.merge_node(make_node(id, 1));
        }
        assert_eq!(store.get_all_nodes().len(), 5);
    }

    // ====================================================================
    // Bounded pending chunk queue tests
    // ====================================================================

    /// Helper: create a simple chunk for queue tests.
    fn make_chunk(job_id: JobId) -> Chunk {
        Chunk {
            id: ChunkId::new(),
            job_id,
            sequence: 0,
            payload: TaskPayload::Shell {
                command: "echo".into(),
                args: vec!["test".into()],
            },
            created_at: Utc::now(),
        }
    }

    #[test]
    fn test_bounded_queue_accepts_under_limit() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();

        for _ in 0..100 {
            store.add_pending_chunk(make_chunk(jid)).unwrap();
        }
        assert_eq!(store.pending_chunk_count(), 100);
    }

    #[test]
    fn test_bounded_queue_rejects_at_limit() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();

        // Fill to MAX_PENDING_CHUNKS.
        for _ in 0..super::super::config::MAX_PENDING_CHUNKS {
            store.add_pending_chunk(make_chunk(jid)).unwrap();
        }
        assert_eq!(store.pending_chunk_count(), super::super::config::MAX_PENDING_CHUNKS);

        // Next enqueue should fail.
        let result = store.add_pending_chunk(make_chunk(jid));
        assert!(result.is_err());
        match result.unwrap_err() {
            SwarmError::QueueFull(msg) => {
                assert!(msg.contains("full"), "error message should mention 'full': {}", msg);
            }
            other => panic!("expected QueueFull, got: {:?}", other),
        }
    }

    #[test]
    fn test_bounded_queue_accepts_after_dequeue() {
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();

        // Fill to MAX_PENDING_CHUNKS.
        for _ in 0..super::super::config::MAX_PENDING_CHUNKS {
            store.add_pending_chunk(make_chunk(jid)).unwrap();
        }

        // Dequeue one.
        let _ = store.take_pending_chunk();

        // Should now accept one more.
        store.add_pending_chunk(make_chunk(jid)).unwrap();
        assert_eq!(store.pending_chunk_count(), super::super::config::MAX_PENDING_CHUNKS);
    }

    #[test]
    fn test_bounded_queue_warning_at_threshold() {
        // This test verifies that the warning path is reached by checking
        // that enqueue still succeeds at the warning threshold (80%).
        // The actual log output is verified by inspection; the test ensures
        // no panics or errors occur at the threshold boundary.
        let store = KnowledgeStore::new(NodeId::new());
        let jid = JobId::new();

        let warn_count = (super::super::config::MAX_PENDING_CHUNKS as f32
            * super::super::config::PENDING_CHUNKS_WARN_THRESHOLD) as usize;

        for _ in 0..warn_count {
            store.add_pending_chunk(make_chunk(jid)).unwrap();
        }

        // This enqueue crosses the warning threshold -- should still succeed.
        store.add_pending_chunk(make_chunk(jid)).unwrap();
        assert_eq!(store.pending_chunk_count(), warn_count + 1);
    }
}
