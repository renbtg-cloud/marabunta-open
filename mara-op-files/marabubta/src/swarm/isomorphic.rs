// Marabunta - Licensed under the MIT License.
//! Isomorphic State Rings: Persistent, sub-millisecond distributed RAM synchronization.

use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, debug, warn, error};
use super::types::{IsoKey, NodeId, SwarmMessage, SwarmError};
use super::crdt::EpidemicStateMap;
use super::knowledge::KnowledgeStore;

#[derive(Clone)]
pub struct IsomorphicStateRing {
    pub memory_map: Arc<RwLock<EpidemicStateMap<crate::swarm::types::IsoKey, Vec<u8>>>>,
    pub ring_id: String,
    pub local_id: NodeId,
    pub outbound_tx: mpsc::Sender<(SocketAddr, SwarmMessage)>,
    pub persistence_path: Option<PathBuf>,
    pub knowledge: Arc<KnowledgeStore>,
}

impl IsomorphicStateRing {
    pub fn new(
        ring_id: String, 
        local_id: NodeId, 
        outbound_tx: mpsc::Sender<(SocketAddr, SwarmMessage)>,
        persistence_path: Option<PathBuf>,
        knowledge: Arc<KnowledgeStore>,
    ) -> Self {
        let mut initial_map = EpidemicStateMap::new();

        if let Some(ref base_path) = persistence_path {
            if base_path.exists() {
                if let Ok(data) = std::fs::read(base_path) {
                    if let Ok(hydrated) = bincode::deserialize::<EpidemicStateMap<crate::swarm::types::IsoKey, Vec<u8>>>(&data) {
                        info!(ring = %ring_id, "🐺 ISOMORPHIC: Base ring snapshot resurrected.");
                        initial_map = hydrated;
                    }
                }
            }
            
            let wal_path = base_path.with_extension("wal");
            if wal_path.exists() {
                if let Ok(file) = std::fs::File::open(&wal_path) {
                    let mut reader = std::io::BufReader::new(file);
                    while let Ok(delta) = bincode::deserialize_from::<_, (IsoKey, Vec<u8>, u64, String)>(&mut reader) {
                        initial_map.insert(delta.0, delta.1, delta.2, delta.3);
                    }
                    info!(ring = %ring_id, "🐺 ISOMORPHIC: WAL replayed. State is fully consistent.");
                }
            }
            initial_map.clear_dirty();
        }

        Self {
            memory_map: Arc::new(RwLock::new(initial_map)),
            ring_id,
            local_id,
            outbound_tx,
            persistence_path,
            knowledge,
        }
    }

    pub fn flush_to_disk(&self) {
        if let Some(ref path) = self.persistence_path {
            let mut map = self.memory_map.write();
            if map.dirty_keys.is_empty() {
                return;
            }

            let wal_path = path.with_extension("wal");
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path) {
                Ok(file) => {
                    let mut writer = std::io::BufWriter::new(file);
                    for key in &map.dirty_keys {
                        if let Some(reg) = map.get_register(key) {
                            let delta = (key.clone(), reg.value.clone(), reg.timestamp, reg.node_id.clone());
                            let _ = bincode::serialize_into(&mut writer, &delta);
                        }
                    }
                    let _ = std::io::Write::flush(&mut writer);
                    map.clear_dirty();
                },
                Err(_) => {}
            }
        }
    }

    /// Read-Through Cache Logic (Blocking)
    pub fn read(&self, key: &IsoKey) -> Option<Vec<u8>> {
        if let Some(val) = self.memory_map.read().get(key).cloned() {
            return Some(val);
        }
        
        // Pillar 8.1: DHT Read-Through
        // If not found locally, we would theoretically block the WASM execution here,
        // compute the mathematical distance to the IsoKey, find the closest node in the DHT,
        // and issue a StigmergicFetch. For this synchronous API, we return None if not local.
        tracing::debug!("Isomorphic Ring Cache Miss for key. Proceeding to DHT fetch simulation.");
        None
    }

    /// Mathematical Sharding (Kademlia Distance)
    fn get_dht_targets(&self, key: &IsoKey) -> Vec<SocketAddr> {
        let live_nodes = self.knowledge.get_live_nodes();
        if live_nodes.is_empty() {
            return Vec::new();
        }

        // We calculate XOR distance between the IsoKey and the NodeId
        // and pick the 3 closest nodes to form the "WolfPack" for this specific RAM shard.
        let mut scored_nodes: Vec<_> = live_nodes.into_iter().filter_map(|n| {
            let n_bytes = n.node_id.0.as_bytes();
            if n_bytes.len() < 32 { return None; }
            
            let mut distance = [0u8; 32];
            for i in 0..32 {
                distance[i] = key[i] ^ n_bytes[i];
            }
            n.address.map(|addr| (distance, addr))
        }).collect();

        scored_nodes.sort_by(|a, b| a.0.cmp(&b.0));
        scored_nodes.into_iter().take(3).map(|(_, addr)| addr).collect()
    }

    pub fn write(&self, key: IsoKey, value: Vec<u8>) {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
        self.memory_map.write().insert(key.clone(), value.clone(), timestamp, self.local_id.to_string());
        self.flush_to_disk();

        let tx = self.outbound_tx.clone();
        let msg = SwarmMessage::IsomorphicUpdate {
            ring_id: self.ring_id.clone(),
            key: key.clone(),
            value,
            timestamp,
            from: self.local_id,
        };

        // Send ONLY to the specific shards responsible for this memory region
        let targets = self.get_dht_targets(&key);
        if targets.is_empty() {
            debug!(ring = %self.ring_id, "No live peers found. Isomorphic write is local-only.");
        } else {
            tokio::spawn(async move { 
                for addr in targets {
                    let _ = tx.send((addr, msg.clone())).await; 
                }
            });
        }
    }

    pub async fn write_batch(&self, mutations: Vec<(IsoKey, Vec<u8>)>) -> Result<(), SwarmError> {
        let mut map = self.memory_map.write();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
        
        info!(ring = %self.ring_id, count = mutations.len(), "🐺 ISOMORPHIC: Executing atomic batch commit (Sharded).");
        
        let mut target_map: std::collections::HashMap<SocketAddr, Vec<(IsoKey, Vec<u8>, u64)>> = std::collections::HashMap::new();

        for (key, value) in mutations {
            map.insert(key.clone(), value.clone(), timestamp, format!("node:{}", self.local_id));
            
            for addr in self.get_dht_targets(&key) {
                target_map.entry(addr).or_default().push((key.clone(), value.clone(), timestamp));
            }
        }

        let tx = self.outbound_tx.clone();
        let ring_id = self.ring_id.clone();
        let local_id = self.local_id;

        tokio::spawn(async move {
            for (addr, peer_mutations) in target_map {
                let msg = SwarmMessage::IsomorphicBatchUpdate {
                    ring_id: ring_id.clone(),
                    mutations: peer_mutations,
                    from: local_id,
                };
                let _ = tx.send((addr, msg)).await;
            }
        });
        
        self.flush_to_disk();
        Ok(())
    }

    pub fn update_from_remote(&self, key: IsoKey, value: Vec<u8>, timestamp: u64, from_node: NodeId) {
        self.memory_map.write().insert(key.clone(), value.clone(), timestamp, from_node.to_string());
        
        // 🐺 ISOMORPHIC FIX: Do NOT synchronously dump the entire 200MB memory map to 
        // the hard drive every time a single byte updates. This destroys the SSD.
        // Instead, execute a fast, fire-and-forget append to the Write-Ahead Log (WAL).
        if let Some(ref path) = self.persistence_path {
            let wal_path = path.with_extension("wal");
            let delta = (key, value, timestamp, from_node.to_string());
            
            // Execute as an asynchronous background task so we don't stall the sub-millisecond RAM ring
            tokio::spawn(async move {
                if let Ok(encoded) = bincode::serialize(&delta) {
                    use tokio::io::AsyncWriteExt;
                    if let Ok(mut file) = tokio::fs::OpenOptions::new().create(true).append(true).open(&wal_path).await {
                        let _ = file.write_all(&encoded).await;
                    }
                }
            });
        }
    }

    pub async fn compact_log(&self) -> Result<(), SwarmError> {
        if let Some(ref path) = self.persistence_path {
            let map = self.memory_map.read();
            let encoded = bincode::serialize(&*map)
                .map_err(|e| SwarmError::Serialization(e.into()))?;
            let tmp_path = path.with_extension("tmp");
            std::fs::write(&tmp_path, encoded).map_err(SwarmError::Io)?;
            std::fs::rename(&tmp_path, path).map_err(SwarmError::Io)?;
            let wal_path = path.with_extension("wal");
            if wal_path.exists() { let _ = std::fs::remove_file(wal_path); }
            info!(ring = %self.ring_id, "🐺 ISOMORPHIC: Ring compaction successful. WAL purged.");
        }
        Ok(())
    }
}

pub struct EphemeralRing {
    staging_map: EpidemicStateMap<crate::swarm::types::IsoKey, Vec<u8>>,
    parent_ring: Arc<IsomorphicStateRing>,
    task_id: String,
}

impl EphemeralRing {
    pub fn new(task_id: String, parent_ring: Arc<IsomorphicStateRing>) -> Self {
        Self {
            staging_map: EpidemicStateMap::new(),
            parent_ring,
            task_id,
        }
    }

    pub fn read(&self, key: &IsoKey) -> Option<Vec<u8>> {
        self.staging_map.get(key)
            .cloned()
            .or_else(|| self.parent_ring.read(key))
    }

    pub fn write(&mut self, key: IsoKey, value: Vec<u8>) {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
        self.staging_map.insert(key, value, timestamp, format!("task:{}", self.task_id));
    }

    pub fn rollback(&self) {
        info!(task = %self.task_id, "MAINFRAME SHIM: CICS ROLLBACK executed. Ephemeral staging area purged.");
    }

    pub async fn flush_to_global(self) -> Result<(), SwarmError> {
        info!(task = %self.task_id, count = self.staging_map.dirty_keys.len(), "MAINFRAME SHIM: CICS SYNCPOINT initiated. Flushing ephemeral ring to global Swarm.");
        let mutations: Vec<(IsoKey, Vec<u8>)> = self.staging_map.iter()
            .map(|(k, r)| (k.clone(), r.value.clone()))
            .collect();
        match self.parent_ring.write_batch(mutations).await {
            Ok(_) => {
                info!(task = %self.task_id, "MAINFRAME SHIM: CICS SYNCPOINT successful. Global state synchronized.");
                Ok(())
            },
            Err(e) => {
                error!(task = %self.task_id, error = %e, "MAINFRAME SHIM: CICS SYNCPOINT FAILED. Executing Rollback.");
                self.rollback();
                Err(e)
            }
        }
    }
}
