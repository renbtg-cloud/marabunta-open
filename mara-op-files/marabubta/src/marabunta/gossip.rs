// Marabunta - Licensed under the MIT License.
//! Epidemic gossip protocol for the Marabunta protocol.
//!
//! CRDT last-writer-wins peer state, adaptive gossip intervals, weighted
//! peer selection, and replay prevention via monotonic sequences.

use crate::marabunta::config;
use crate::marabunta::identity::NodeId;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Node capability vector advertised via gossip.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CapabilityVector {
    pub cores: u32,
    pub memory_mb: u64,
    pub bandwidth_kbps: u32,
    pub storage_mb: u64,
    pub gpu: bool,
    pub blind_capable: bool,
    pub relay_capable: bool,
    pub aggregate_capable: bool,
    pub uptime_hours: u64,
    /// A 256-bit Bloom Filter representing hashed strings of installed proprietary binaries
    /// (e.g., `blake3("msexcel")`, `blake3("postgres15")`). Powers the Global Dependency Resolver.
    pub software_bloom_filter: [u8; 32],
}

impl Default for CapabilityVector {
    fn default() -> Self {
        Self {
            cores: 1,
            memory_mb: 512,
            bandwidth_kbps: 1000,
            storage_mb: 1024,
            gpu: false,
            blind_capable: false,
            relay_capable: false,
            aggregate_capable: false,
            uptime_hours: 0,
            software_bloom_filter: [0u8; 32],
        }
    }
}


impl CapabilityVector {
    /// Mathematically verifies if a node possesses a specific software license or binary
    /// by checking the 256-bit Bloom Filter. Allows the Swarm to dynamically route
    /// chaotic corporate workflows (e.g., MS Excel macros -> Oracle DB).
    pub fn has_software_capability(&self, software_name: &str) -> bool {
        let hash = blake3::hash(software_name.as_bytes());
        let hash_bytes = hash.as_bytes();
        
        // Fast bitwise subset check. If the node's bloom filter contains all the 
        // bits of the software hash, it technically possesses the capability.
        for i in 0..32 {
            if (self.software_bloom_filter[i] & hash_bytes[i]) != hash_bytes[i] {
                return false;
            }
        }
        true
    }
}


/// Load metrics snapshot from a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoadMetrics {
    pub cpu_percent: f32,
    pub active_jobs: u32,
    pub queue_depth: u32,
    pub bandwidth_used_kbps: u32,
}

impl Default for LoadMetrics {
    fn default() -> Self {
        Self {
            cpu_percent: 0.0,
            active_jobs: 0,
            queue_depth: 0,
            bandwidth_used_kbps: 0,
        }
    }
}

/// A single gossip entry representing a node's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipEntry {
    pub node_id: NodeId,
    pub capabilities: CapabilityVector,
    pub load: LoadMetrics,
    pub last_seen: DateTime<Utc>,
    pub reputation: u16,
    pub version: u64,
    pub connectivity_score: u8,
}

impl GossipEntry {
    pub fn new(node_id: NodeId, capabilities: CapabilityVector) -> Self {
        Self {
            node_id,
            capabilities,
            load: LoadMetrics::default(),
            last_seen: Utc::now(),
            reputation: 100,
            version: 1,
            connectivity_score: 100,
        }
    }
}

/// A gossip message payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipPayload {
    pub sender_id: NodeId,
    pub sequence: u64,
    pub ttl: u8,
    pub timestamp: DateTime<Utc>,
    pub entries: Vec<GossipEntry>,
}

/// DashMap-based gossip state store.
pub struct GossipStore {
    entries: DashMap<NodeId, GossipEntry>,
    /// Track last seen sequence per sender for replay prevention.
    sequences: DashMap<NodeId, u64>,
}

impl GossipStore {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            sequences: DashMap::new(),
        }
    }

    /// Merge a gossip entry using last-writer-wins by version.
    /// Returns true if the entry was newer and was merged.
    pub fn merge_entry(&self, entry: GossipEntry) -> bool {
        let node_id = entry.node_id;
        match self.entries.entry(node_id) {
            dashmap::mapref::entry::Entry::Occupied(mut existing) => {
                if entry.version > existing.get().version {
                    existing.insert(entry);
                    true
                } else {
                    false
                }
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(entry);
                true
            }
        }
    }

    /// Get a snapshot of an entry.
    pub fn get(&self, node_id: &NodeId) -> Option<GossipEntry> {
        self.entries.get(node_id).map(|e| e.clone())
    }

    /// Get all known entries.
    pub fn all_entries(&self) -> Vec<GossipEntry> {
        self.entries.iter().map(|e| e.value().clone()).collect()
    }

    /// Number of known nodes.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Check if a sequence number is a replay (already seen or older).
    pub fn is_replay(&self, sender_id: &NodeId, sequence: u64) -> bool {
        self.sequences
            .get(sender_id)
            .is_some_and(|last| sequence <= *last)
    }

    /// Record a sequence number for replay prevention.
    pub fn record_sequence(&self, sender_id: &NodeId, sequence: u64) {
        self.sequences
            .entry(*sender_id)
            .and_modify(|last| {
                if sequence > *last {
                    *last = sequence;
                }
            })
            .or_insert(sequence);
    }

    /// Prune entries older than max_age.
    pub fn prune_stale(&self, max_age: Duration) -> usize {
        let cutoff = Utc::now() - chrono::Duration::from_std(max_age).unwrap_or_default();
        let mut pruned = 0;
        self.entries.retain(|_, entry| {
            if entry.last_seen < cutoff {
                pruned += 1;
                false
            } else {
                true
            }
        });
        pruned
    }

    /// Remove a specific node.
    pub fn remove(&self, node_id: &NodeId) {
        self.entries.remove(node_id);
        self.sequences.remove(node_id);
    }
}

impl Default for GossipStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Gossip engine handling peer selection, payload creation, and incoming processing.
pub struct GossipEngine {
    pub store: Arc<GossipStore>,
    my_node_id: NodeId,
    sequence_counter: AtomicU64,
    churn_events: AtomicU64,
    last_churn_check: std::sync::Mutex<std::time::Instant>,
}

impl GossipEngine {
    pub fn new(my_node_id: NodeId, store: Arc<GossipStore>) -> Self {
        Self {
            store,
            my_node_id,
            sequence_counter: AtomicU64::new(0),
            churn_events: AtomicU64::new(0),
            last_churn_check: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }

    /// Select peers for gossip fan-out using weighted random selection.
    pub fn select_peers(&self, count: usize) -> Vec<NodeId> {
        let entries = self.store.all_entries();
        if entries.is_empty() {
            return vec![];
        }

        let mut candidates: Vec<_> = entries
            .iter()
            .filter(|e| e.node_id != self.my_node_id)
            .collect();

        if candidates.len() <= count {
            return candidates.iter().map(|e| e.node_id).collect();
        }

        // Weighted selection: prefer recently-seen, higher-reputation nodes
        let mut rng = rand::thread_rng();
        let mut selected = Vec::with_capacity(count);
        for _ in 0..count {
            if candidates.is_empty() {
                break;
            }
            let weights: Vec<f64> = candidates
                .iter()
                .map(|e| {
                    let recency = 1.0
                        / (1.0
                            + (Utc::now() - e.last_seen).num_seconds().abs() as f64 / 60.0);
                    let rep = e.reputation as f64 / 100.0;
                    recency * 0.6 + rep * 0.4
                })
                .collect();
            let total: f64 = weights.iter().sum();
            if total <= 0.0 {
                break;
            }
            let threshold: f64 = rng.gen::<f64>() * total;
            let mut cumulative = 0.0;
            let mut pick = 0;
            for (i, w) in weights.iter().enumerate() {
                cumulative += w;
                if cumulative >= threshold {
                    pick = i;
                    break;
                }
            }
            selected.push(candidates[pick].node_id);
            candidates.swap_remove(pick);
        }

        selected
    }

    /// Create a gossip payload from our current knowledge.
    pub fn create_payload(&self) -> GossipPayload {
        let seq = self.sequence_counter.fetch_add(1, Ordering::Relaxed) + 1;
        GossipPayload {
            sender_id: self.my_node_id,
            sequence: seq,
            ttl: config::DEFAULT_TTL,
            timestamp: Utc::now(),
            entries: self.store.all_entries(),
        }
    }

    /// Handle an incoming gossip payload. Returns the number of entries merged.
    pub fn handle_incoming(&self, payload: GossipPayload) -> usize {
        // Replay prevention
        if self.store.is_replay(&payload.sender_id, payload.sequence) {
            return 0;
        }
        self.store
            .record_sequence(&payload.sender_id, payload.sequence);

        // TTL check
        if payload.ttl == 0 {
            return 0;
        }

        // Merge entries
        let mut merged = 0;
        for entry in payload.entries {
            if self.store.merge_entry(entry) {
                merged += 1;
            }
        }

        if merged > 0 {
            self.churn_events.fetch_add(1, Ordering::Relaxed);
        }

        merged
    }

    /// Get adaptive gossip interval based on churn rate.
    pub fn adaptive_interval(&self) -> Duration {
        let churn = self.churn_events.load(Ordering::Relaxed);
        let elapsed = {
            let guard = self.last_churn_check.lock().unwrap();
            guard.elapsed()
        };

        let churn_rate = if elapsed.as_secs() > 0 {
            churn as f64 / elapsed.as_secs() as f64
        } else {
            churn as f64
        };

        if churn_rate > 5.0 {
            Duration::from_millis(config::GOSSIP_CHURN_INTERVAL_MS)
        } else if churn_rate < 0.1
            && elapsed.as_secs() > config::GOSSIP_STABLE_THRESHOLD_S
        {
            Duration::from_millis(config::GOSSIP_STABLE_INTERVAL_MS)
        } else {
            Duration::from_millis(config::GOSSIP_BASE_INTERVAL_MS)
        }
    }

    /// Reset churn tracking (call periodically).
    pub fn reset_churn(&self) {
        self.churn_events.store(0, Ordering::Relaxed);
        let mut guard = self.last_churn_check.lock().unwrap();
        *guard = std::time::Instant::now();
    }
}

use rand::Rng;

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id(b: u8) -> NodeId {
        NodeId([b; 32])
    }

    fn make_entry(b: u8) -> GossipEntry {
        GossipEntry::new(make_node_id(b), CapabilityVector::default())
    }

    #[test]
    fn test_merge_new_entry() {
        let store = GossipStore::new();
        let entry = make_entry(1);
        assert!(store.merge_entry(entry));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_merge_newer_version_wins() {
        let store = GossipStore::new();
        let mut e1 = make_entry(1);
        e1.version = 1;
        let mut e2 = make_entry(1);
        e2.version = 2;
        e2.reputation = 50;

        store.merge_entry(e1);
        assert!(store.merge_entry(e2));
        assert_eq!(store.get(&make_node_id(1)).unwrap().reputation, 50);
    }

    #[test]
    fn test_merge_older_version_rejected() {
        let store = GossipStore::new();
        let mut e1 = make_entry(1);
        e1.version = 5;
        let mut e2 = make_entry(1);
        e2.version = 3;

        store.merge_entry(e1);
        assert!(!store.merge_entry(e2));
        assert_eq!(store.get(&make_node_id(1)).unwrap().version, 5);
    }

    #[test]
    fn test_replay_prevention() {
        let store = GossipStore::new();
        let sender = make_node_id(1);

        assert!(!store.is_replay(&sender, 1));
        store.record_sequence(&sender, 1);
        assert!(store.is_replay(&sender, 1));
        assert!(!store.is_replay(&sender, 2));
    }

    #[test]
    fn test_prune_stale() {
        let store = GossipStore::new();
        let mut entry = make_entry(1);
        entry.last_seen = Utc::now() - chrono::Duration::seconds(60);
        store.merge_entry(entry);

        let mut fresh = make_entry(2);
        fresh.last_seen = Utc::now();
        store.merge_entry(fresh);

        let pruned = store.prune_stale(Duration::from_secs(30));
        assert_eq!(pruned, 1);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_select_peers_excludes_self() {
        let store = Arc::new(GossipStore::new());
        let my_id = make_node_id(0);
        let engine = GossipEngine::new(my_id, store.clone());

        store.merge_entry(make_entry(0)); // self
        store.merge_entry(make_entry(1));
        store.merge_entry(make_entry(2));

        let peers = engine.select_peers(5);
        assert!(!peers.contains(&my_id));
    }

    #[test]
    fn test_select_peers_count() {
        let store = Arc::new(GossipStore::new());
        let engine = GossipEngine::new(make_node_id(0), store.clone());

        for i in 1..=10 {
            store.merge_entry(make_entry(i));
        }

        let peers = engine.select_peers(config::GOSSIP_FANOUT);
        assert_eq!(peers.len(), config::GOSSIP_FANOUT);
    }

    #[test]
    fn test_create_payload_increments_sequence() {
        let store = Arc::new(GossipStore::new());
        let engine = GossipEngine::new(make_node_id(0), store);

        let p1 = engine.create_payload();
        let p2 = engine.create_payload();
        assert_eq!(p1.sequence, 1);
        assert_eq!(p2.sequence, 2);
    }

    #[test]
    fn test_handle_incoming_merges() {
        let store = Arc::new(GossipStore::new());
        let engine = GossipEngine::new(make_node_id(0), store.clone());

        let payload = GossipPayload {
            sender_id: make_node_id(1),
            sequence: 1,
            ttl: 3,
            timestamp: Utc::now(),
            entries: vec![make_entry(2), make_entry(3)],
        };

        let merged = engine.handle_incoming(payload);
        assert_eq!(merged, 2);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_handle_incoming_replay_rejected() {
        let store = Arc::new(GossipStore::new());
        let engine = GossipEngine::new(make_node_id(0), store.clone());

        let payload = GossipPayload {
            sender_id: make_node_id(1),
            sequence: 1,
            ttl: 3,
            timestamp: Utc::now(),
            entries: vec![make_entry(2)],
        };

        engine.handle_incoming(payload.clone());
        let merged = engine.handle_incoming(payload);
        assert_eq!(merged, 0);
    }

    #[test]
    fn test_handle_incoming_ttl_zero_rejected() {
        let store = Arc::new(GossipStore::new());
        let engine = GossipEngine::new(make_node_id(0), store.clone());

        let payload = GossipPayload {
            sender_id: make_node_id(1),
            sequence: 1,
            ttl: 0,
            timestamp: Utc::now(),
            entries: vec![make_entry(2)],
        };

        let merged = engine.handle_incoming(payload);
        assert_eq!(merged, 0);
    }

    #[test]
    fn test_adaptive_interval_normal() {
        let store = Arc::new(GossipStore::new());
        let engine = GossipEngine::new(make_node_id(0), store);
        let interval = engine.adaptive_interval();
        assert_eq!(
            interval,
            Duration::from_millis(config::GOSSIP_BASE_INTERVAL_MS)
        );
    }
}
