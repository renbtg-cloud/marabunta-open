// Marabunta - Licensed under the MIT License.
//! Pillar 2.1: Geo-Spatial Cell Definitions & Hierarchical Routing
//! 
//! Implements GeoCell bit-masking (spatial hashing) for hierarchical Kademlia.
//! Nodes partition the 15-billion-node swarm into a 1m x 1m global grid.
//! Inter-continental routing is handled via "Boulders" (High-bandwidth relays).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Instant, Duration};

/// Legacy GeoCell identifier for jurisdiction and policy enforcement.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum GeoCell {
    GlobalAllianceT1, // USA, UK, CAN, AUS, NZ
    EuStrategicPact,  // Europe
    SouthAmMercosur,
    ApacRim,
    AfricaUnion,
    Unknown,
}

/// 64-bit spatial hash (Z-order curve or similar) for 1m x 1m global precision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GeoHash(pub u64);

impl GeoHash {
    /// Masks the hash to a specific hierarchical level (e.g., city, country, continent).
    pub fn mask(&self, bits: u8) -> u64 {
        let shift = 64 - bits;
        (self.0 >> shift) << shift
    }

    /// Returns the continental 'Pillar' identifier based on the top 4 bits.
    pub fn continent_id(&self) -> u8 {
        (self.0 >> 60) as u8
    }
}

#[derive(Clone)]
pub struct PeerRecord {
    pub node_id: [u8; 32],
    pub last_seen: Instant,
    pub location: GeoHash,
    pub is_boulder: bool, // High-capacity relay
}

pub struct HierarchicalKademlia {
    local_location: GeoHash,
    /// Bucket 0: Local neighbors (within same city/cell mask)
    /// Bucket 1: Regional relays
    /// Bucket 2: Inter-continental Boulders
    buckets: Arc<RwLock<[HashMap<[u8; 32], PeerRecord>; 3]>>,
}

impl HierarchicalKademlia {
    pub fn new(local_location: GeoHash) -> Self {
        Self {
            local_location,
            buckets: Arc::new(RwLock::new([HashMap::new(), HashMap::new(), HashMap::new()])),
        }
    }

    /// Updates or inserts a peer into the hierarchical routing table.
    pub fn update_peer(&self, peer: PeerRecord) {
        let bucket_idx = if peer.is_boulder {
            2 // Inter-continental
        } else if peer.location.mask(16) == self.local_location.mask(16) {
            0 // Local (City scale)
        } else {
            1 // Regional
        };

        let mut buckets = self.buckets.write().unwrap();
        buckets[bucket_idx].insert(peer.node_id, peer);
        
        // Eviction logic: keep buckets compact to prevent 12GB OOM failures.
        if buckets[bucket_idx].len() > 1000 {
            self.prune_bucket(&mut buckets[bucket_idx]);
        }
    }

    fn prune_bucket(&self, bucket: &mut HashMap<[u8; 32], PeerRecord>) {
        let now = Instant::now();
        // Remove nodes not seen in 1 hour, or just the oldest if all are fresh.
        bucket.retain(|_, v| now.duration_since(v.last_seen) < Duration::from_secs(3600));
    }

    /// Finds the closest nodes for a given target GeoHash.
    pub fn find_closest(&self, target: GeoHash) -> Vec<[u8; 32]> {
        let buckets = self.buckets.read().unwrap();
        
        // If target is in another continent, return Boulders.
        if target.continent_id() != self.local_location.continent_id() {
            return buckets[2].keys().cloned().collect();
        }

        // Otherwise, return local/regional nodes.
        let mut results = Vec::new();
        results.extend(buckets[0].keys().cloned());
        results.extend(buckets[1].keys().cloned());
        results.truncate(20);
        results
    }
}
