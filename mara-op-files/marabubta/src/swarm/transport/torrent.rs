// Marabunta - Licensed under the MIT License.
//! BitTorrent-style P2P Data Spigots for Marabunta Swarm.
//! 
//! Instead of broadcasting massive payloads, nodes share chunks of data 
//! (e.g. model weights) with each other, forming a global mesh CDN.

use serde::{Deserialize, Serialize};
use crate::swarm::types::{BlobHash, NodeId};

/// A bitmap of chunks a node possesses for a specific blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkBitfield {
    pub blob_hash: BlobHash,
    pub total_chunks: u32,
    pub bitfield: Vec<u8>,
}

impl ChunkBitfield {
    pub fn has_chunk(&self, index: u32) -> bool {
        if index >= self.total_chunks { return false; }
        let byte = (index / 8) as usize;
        let bit = (index % 8) as u8;
        self.bitfield.get(byte).is_some_and(|&b| (b & (1 << bit)) != 0)
    }

    pub fn set_chunk(&mut self, index: u32) {
        if index >= self.total_chunks { return; }
        let byte = (index / 8) as usize;
        let bit = (index % 8) as u8;
        if byte >= self.bitfield.len() {
            self.bitfield.resize(byte + 1, 0);
        }
        self.bitfield[byte] |= 1 << bit;
    }
}

/// Request a specific chunk from a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRequest {
    pub blob_hash: BlobHash,
    pub chunk_index: u32,
    pub from: NodeId,
}

/// Response containing raw chunk data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkResponse {
    pub blob_hash: BlobHash,
    pub chunk_index: u32,
    pub data: Vec<u8>,
    pub from: NodeId,
}


use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tracing::{info, debug};

/// Tracks peer availability for specific chunks
#[derive(Default)]
pub struct SwarmTracker {
    /// Maps a BlobHash to a list of NodeIds that have advertised possessing it completely
    pub seeders: HashMap<BlobHash, HashSet<NodeId>>,
    /// Maps a BlobHash to NodeIds and their respective Bitfields
    pub leechers: HashMap<BlobHash, HashMap<NodeId, ChunkBitfield>>,
}

/// The local P2P CDN Engine for resolving massive data payloads without central bottlenecks.
pub struct TorrentEngine {
    self_id: NodeId,
    tracker: Arc<RwLock<SwarmTracker>>,
    /// Locally cached chunk data: BlobHash -> (ChunkIndex -> Data)
    local_storage: Arc<RwLock<HashMap<BlobHash, HashMap<u32, Vec<u8>>>>>,
    outbound_tx: mpsc::Sender<(crate::swarm::types::NodeId, crate::swarm::types::SwarmMessage)>,
}

impl TorrentEngine {
    pub fn new(self_id: NodeId, outbound_tx: mpsc::Sender<(crate::swarm::types::NodeId, crate::swarm::types::SwarmMessage)>) -> Self {
        Self {
            self_id,
            tracker: Arc::new(RwLock::new(SwarmTracker::default())),
            local_storage: Arc::new(RwLock::new(HashMap::new())),
            outbound_tx,
        }
    }

    /// Announce to the swarm what chunks we currently possess
    pub async fn broadcast_bitfield(&self, blob_hash: BlobHash, total_chunks: u32, indices: Vec<u32>) {
        let mut bitfield = ChunkBitfield {
            blob_hash: blob_hash,
            total_chunks,
            bitfield: vec![0; total_chunks.div_ceil(8) as usize],
        };
        for idx in indices {
            bitfield.set_chunk(idx);
        }
        // In a real implementation, this broadcasts to neighborhood/gossip
        info!(blob_hash = ?blob_hash, "P2P Spigot: Broadcasting Bitfield (have {}/{} chunks)", bitfield.bitfield.len() * 8, total_chunks);
    }

    /// Process an incoming bitfield from a peer
    pub async fn handle_bitfield(&self, from: NodeId, bitfield: ChunkBitfield) {
        let mut tracker = self.tracker.write().await;
        let blob_map = tracker.leechers.entry(bitfield.blob_hash).or_default();
        blob_map.insert(from, bitfield);
    }

    /// Request a missing chunk from the rarest peer
    pub async fn request_chunk(&self, blob_hash: BlobHash, chunk_index: u32) {
        let tracker = self.tracker.read().await;
        if let Some(peers) = tracker.leechers.get(&blob_hash) {
            // Find peers that have this chunk
            let valid_peers: Vec<&NodeId> = peers.iter()
                .filter(|(_, bf)| bf.has_chunk(chunk_index))
                .map(|(id, _)| id)
                .collect();
            
            if let Some(&peer_id) = valid_peers.first() { // Naive selection, should be rarest-first
                let req = ChunkRequest {
                    blob_hash,
                    chunk_index,
                    from: self.self_id,
                };
                debug!(%peer_id, chunk_index, "P2P Spigot: Requesting chunk from peer");
                let _ = self.outbound_tx.send((*peer_id, crate::swarm::types::SwarmMessage::TorrentRequest(req))).await;
            }
        }
    }

    /// Handle an incoming request for data we (hopefully) hold
    pub async fn handle_request(&self, req: ChunkRequest) {
        let storage = self.local_storage.read().await;
        if let Some(blob_chunks) = storage.get(&req.blob_hash) {
            if let Some(data) = blob_chunks.get(&req.chunk_index) {
                let res = ChunkResponse {
                    blob_hash: req.blob_hash,
                    chunk_index: req.chunk_index,
                    data: data.clone(),
                    from: self.self_id,
                };
                let _ = self.outbound_tx.send((req.from, crate::swarm::types::SwarmMessage::TorrentResponse(res))).await;
            }
        }
    }

    /// Store a received chunk and potentially serve it to others
    pub async fn handle_response(&self, res: ChunkResponse) {
        let mut storage = self.local_storage.write().await;
        let blob_chunks = storage.entry(res.blob_hash).or_default();
        blob_chunks.insert(res.chunk_index, res.data);
        info!(chunk = res.chunk_index, "P2P Spigot: Chunk downloaded successfully. Now seeding to mesh.");
        // We would now broadcast our updated bitfield to the neighborhood
    }
}
