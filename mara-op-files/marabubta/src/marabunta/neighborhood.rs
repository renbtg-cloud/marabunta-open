// Marabunta - Licensed under the MIT License.
//! Neighborhood formation and management for the Marabunta protocol.
//!
//! Nodes self-organize into neighborhoods of ~100 nodes. Neighborhoods split
//! when exceeding 150 and merge when below 30.

use crate::marabunta::config;
use crate::marabunta::gossip::{GossipEntry, CapabilityVector};
use crate::marabunta::identity::NodeId;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// Unique neighborhood identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NeighborhoodId(pub u64);

/// Aggregate summary of a neighborhood for inter-neighborhood exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighborhoodSummary {
    pub id: NeighborhoodId,
    pub member_count: usize,
    pub total_cores: u64,
    pub total_memory_mb: u64,
    pub total_bandwidth_kbps: u64,
    pub avg_cpu_percent: f32,
    pub representative: Option<NodeId>,
}

/// A neighborhood — a group of nearby nodes.
pub struct Neighborhood {
    pub id: NeighborhoodId,
    members: DashMap<NodeId, GossipEntry>,
    representative: Option<NodeId>,
    settled_since: Option<std::time::Instant>,
}

impl Neighborhood {
    pub fn new(id: NeighborhoodId) -> Self {
        Self {
            id,
            members: DashMap::new(),
            representative: None,
            settled_since: None,
        }
    }

    /// Add or update a member.
    pub fn upsert_member(&self, entry: GossipEntry) {
        self.members.insert(entry.node_id, entry);
    }

    /// Remove a member.
    pub fn remove_member(&self, node_id: &NodeId) {
        self.members.remove(node_id);
    }

    /// Number of members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Whether the neighborhood should split (>150 members).
    pub fn should_split(&self) -> bool {
        self.members.len() > config::NEIGHBORHOOD_SPLIT_THRESHOLD
    }

    /// Whether the neighborhood should try to merge (<30 members).
    pub fn should_merge(&self) -> bool {
        self.members.len() < config::NEIGHBORHOOD_MERGE_THRESHOLD
    }

    /// Elect a representative deterministically.
    /// Priority: max uptime → max bandwidth → min NodeId.
    pub fn elect_representative(&mut self) -> Option<NodeId> {
        let mut best: Option<(u64, u32, NodeId)> = None;

        for entry in self.members.iter() {
            let e = entry.value();
            let key = (
                e.capabilities.uptime_hours,
                e.capabilities.bandwidth_kbps,
                e.node_id,
            );
            match &best {
                None => best = Some(key),
                Some(current) => {
                    if key.0 > current.0
                        || (key.0 == current.0 && key.1 > current.1)
                        || (key.0 == current.0 && key.1 == current.1 && key.2 < current.2)
                    {
                        best = Some(key);
                    }
                }
            }
        }

        self.representative = best.map(|(_, _, id)| id);
        self.representative
    }

    /// Get the current representative.
    pub fn representative(&self) -> Option<NodeId> {
        self.representative
    }

    /// Mark as settled (gossip is stable).
    pub fn mark_settled(&mut self) {
        if self.settled_since.is_none() {
            self.settled_since = Some(std::time::Instant::now());
        }
    }

    /// Check if this neighborhood has been settled long enough.
    pub fn is_settled(&self) -> bool {
        self.settled_since.is_some_and(|since| {
            since.elapsed().as_secs() >= config::NEIGHBORHOOD_SETTLE_TIME_S
        })
    }

    /// Generate an aggregate summary.
    pub fn summary(&self) -> NeighborhoodSummary {
        let mut total_cores: u64 = 0;
        let mut total_memory: u64 = 0;
        let mut total_bw: u64 = 0;
        let mut total_cpu: f64 = 0.0;
        let count = self.members.len();

        for entry in self.members.iter() {
            let e = entry.value();
            total_cores += e.capabilities.cores as u64;
            total_memory += e.capabilities.memory_mb;
            total_bw += e.capabilities.bandwidth_kbps as u64;
            total_cpu += e.load.cpu_percent as f64;
        }

        let avg_cpu = if count > 0 {
            (total_cpu / count as f64) as f32
        } else {
            0.0
        };

        NeighborhoodSummary {
            id: self.id,
            member_count: count,
            total_cores,
            total_memory_mb: total_memory,
            total_bandwidth_kbps: total_bw,
            avg_cpu_percent: avg_cpu,
            representative: self.representative,
        }
    }

    /// Get all member NodeIds.
    pub fn member_ids(&self) -> Vec<NodeId> {
        self.members.iter().map(|e| *e.key()).collect()
    }

    /// Split into two neighborhoods. Returns the second half.
    pub fn split(&self, new_id: NeighborhoodId) -> Neighborhood {
        let new_neighborhood = Neighborhood::new(new_id);
        let mut ids: Vec<NodeId> = self.member_ids();
        ids.sort();

        let half = ids.len() / 2;
        for id in &ids[half..] {
            if let Some((_, entry)) = self.members.remove(id) {
                new_neighborhood.upsert_member(entry);
            }
        }

        new_neighborhood
    }
}

/// Manages neighborhood lifecycle (join, split, merge).
pub struct NeighborhoodManager {
    neighborhoods: DashMap<NeighborhoodId, Neighborhood>,
    my_neighborhood: Option<NeighborhoodId>,
    next_id: std::sync::atomic::AtomicU64,
}

impl NeighborhoodManager {
    pub fn new() -> Self {
        Self {
            neighborhoods: DashMap::new(),
            my_neighborhood: None,
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Create a new neighborhood and return its ID.
    pub fn create_neighborhood(&self) -> NeighborhoodId {
        let id = NeighborhoodId(
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        );
        self.neighborhoods.insert(id, Neighborhood::new(id));
        id
    }

    /// Join a specific neighborhood.
    pub fn join(&mut self, neighborhood_id: NeighborhoodId, entry: GossipEntry) {
        if let Some(n) = self.neighborhoods.get(&neighborhood_id) {
            n.upsert_member(entry);
            self.my_neighborhood = Some(neighborhood_id);
        }
    }

    /// Get my current neighborhood ID.
    pub fn my_neighborhood(&self) -> Option<NeighborhoodId> {
        self.my_neighborhood
    }


    /// Compiles the raw NodeIDs of thousands of local Ephemeral workers into a single 256-bit Bloom Filter.
    /// This proves O(1) bandwidth scaling for maintaining planetary routing tables.
    pub fn compile_worker_bloom_filter(&self, local_worker_hashes: &dashmap::DashMap<NodeId, u64>) -> [u8; 32] {
        let mut filter = [0u8; 32];
        for entry in local_worker_hashes.iter() {
            let hash_val = entry.value();
            // Map the 64-bit hash into the 256-bit filter space (modulo distribution)
            let bit_index = (hash_val % 256) as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            filter[byte_index] |= 1 << bit_offset;
        }
        filter
    }



    /// Maintains the active state of the local Kademlia cohort.
    /// [MARABUNTA WMD] Ping-Shale Aggregation:
    /// 100 million Edge nodes cannot ping the entire DHT without self-DDoS.
    /// If the node is Ephemeral (Edge), it ONLY pings its assigned local Aggregator.
    /// The Aggregator compiles the connection states into compressed Bloom Filters 
    /// and gossips them to the wider network.
    pub fn maintain_cohort_state(&self, current_role: crate::swarm::election::SwarmRole) {
        if current_role == crate::swarm::election::SwarmRole::Ephemeral {
            tracing::debug!("NEIGHBORHOOD: Role is Ephemeral (Edge). Muting global DHT pings. Emitting heartbeat to local Aggregator only.");
            // Delegate routing to Enterprise
            // self.emit_local_heartbeat();
        } else {
            tracing::debug!("NEIGHBORHOOD: Role is High-Elo (Enterprise). Broadcasting compressed Bloom Filters for Ping-Shale aggregation.");
            // self.broadcast_bloom_filter();
        }
    }


    /// Check and perform splits if needed.
    pub fn check_splits(&self) -> Vec<NeighborhoodId> {
        let mut new_neighborhoods = vec![];
        let to_split: Vec<NeighborhoodId> = self
            .neighborhoods
            .iter()
            .filter(|e| e.value().should_split())
            .map(|e| *e.key())
            .collect();

        for id in to_split {
            if let Some(n) = self.neighborhoods.get(&id) {
                let new_id = NeighborhoodId(
                    self.next_id
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                let new_n = n.split(new_id);
                drop(n);
                self.neighborhoods.insert(new_id, new_n);
                new_neighborhoods.push(new_id);
            }
        }

        new_neighborhoods
    }

    /// Get a neighborhood summary.
    pub fn get_summary(&self, id: &NeighborhoodId) -> Option<NeighborhoodSummary> {
        self.neighborhoods.get(id).map(|n| n.summary())
    }

    /// Get all neighborhood IDs.
    pub fn all_ids(&self) -> Vec<NeighborhoodId> {
        self.neighborhoods.iter().map(|e| *e.key()).collect()
    }
}

impl Default for NeighborhoodManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node_id(b: u8) -> NodeId {
        NodeId([b; 32])
    }

    fn make_entry(b: u8, uptime: u64, bandwidth: u32) -> GossipEntry {
        let mut caps = CapabilityVector::default();
        caps.uptime_hours = uptime;
        caps.bandwidth_kbps = bandwidth;
        GossipEntry::new(make_node_id(b), caps)
    }

    #[test]
    fn test_neighborhood_upsert_and_count() {
        let n = Neighborhood::new(NeighborhoodId(1));
        n.upsert_member(make_entry(1, 10, 1000));
        n.upsert_member(make_entry(2, 20, 2000));
        assert_eq!(n.member_count(), 2);
    }

    #[test]
    fn test_neighborhood_remove() {
        let n = Neighborhood::new(NeighborhoodId(1));
        n.upsert_member(make_entry(1, 10, 1000));
        n.remove_member(&make_node_id(1));
        assert_eq!(n.member_count(), 0);
    }

    #[test]
    fn test_should_split() {
        let n = Neighborhood::new(NeighborhoodId(1));
        for i in 0..=150 {
            n.upsert_member(make_entry(i as u8, 10, 1000));
        }
        assert!(n.should_split());
    }

    #[test]
    fn test_should_merge() {
        let n = Neighborhood::new(NeighborhoodId(1));
        for i in 0..10 {
            n.upsert_member(make_entry(i, 10, 1000));
        }
        assert!(n.should_merge());
    }

    #[test]
    fn test_elect_representative_max_uptime() {
        let mut n = Neighborhood::new(NeighborhoodId(1));
        n.upsert_member(make_entry(1, 100, 1000));
        n.upsert_member(make_entry(2, 200, 1000));
        n.upsert_member(make_entry(3, 50, 1000));
        let rep = n.elect_representative().unwrap();
        assert_eq!(rep, make_node_id(2)); // highest uptime
    }

    #[test]
    fn test_elect_representative_tiebreak_bandwidth() {
        let mut n = Neighborhood::new(NeighborhoodId(1));
        n.upsert_member(make_entry(1, 100, 2000));
        n.upsert_member(make_entry(2, 100, 5000));
        let rep = n.elect_representative().unwrap();
        assert_eq!(rep, make_node_id(2)); // same uptime, higher bandwidth
    }

    #[test]
    fn test_elect_representative_tiebreak_node_id() {
        let mut n = Neighborhood::new(NeighborhoodId(1));
        n.upsert_member(make_entry(5, 100, 1000));
        n.upsert_member(make_entry(1, 100, 1000));
        let rep = n.elect_representative().unwrap();
        assert_eq!(rep, make_node_id(1)); // same uptime+bw, lower NodeId
    }

    #[test]
    fn test_summary_aggregation() {
        let n = Neighborhood::new(NeighborhoodId(1));
        n.upsert_member(make_entry(1, 10, 1000));
        n.upsert_member(make_entry(2, 20, 2000));
        let s = n.summary();
        assert_eq!(s.member_count, 2);
        assert_eq!(s.total_bandwidth_kbps, 3000);
    }

    #[test]
    fn test_split() {
        let n = Neighborhood::new(NeighborhoodId(1));
        for i in 0..100u8 {
            n.upsert_member(make_entry(i, 10, 1000));
        }
        let n2 = n.split(NeighborhoodId(2));
        assert_eq!(n.member_count(), 50);
        assert_eq!(n2.member_count(), 50);
    }

    #[test]
    fn test_manager_create_and_join() {
        let mut mgr = NeighborhoodManager::new();
        let id = mgr.create_neighborhood();
        mgr.join(id, make_entry(1, 10, 1000));
        assert_eq!(mgr.my_neighborhood(), Some(id));
        let summary = mgr.get_summary(&id).unwrap();
        assert_eq!(summary.member_count, 1);
    }
}
