// Marabunta - Licensed under the MIT License.
//! Lazarus — Checkpoint-based task resurrection.
//!
//! Provides fault-tolerant checkpointing using Reed-Solomon erasure coding.
//! Task state is split into data fragments, parity fragments are computed,
//! and all fragments are stored locally (with the intent that they can be
//! dispersed to peer nodes via gossip in a production deployment).
//!
//! When a task needs to be resurrected, fragments are collected, missing
//! ones are reconstructed via Reed-Solomon, the original blob is reassembled,
//! and its integrity is verified against a blake3 hash.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::LazarusConfig;
use super::types::{Blake3Hash, MarabuntaEvent, CheckpointId, NodeId, TaskId};

// ============================================================================
// Error type
// ============================================================================

/// Errors that can occur during checkpoint or resurrection operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CheckpointError {
    /// Failed to serialize task state into a checkpoint blob.
    Serialization(String),
    /// Failed to deserialize a checkpoint blob back into task state.
    Deserialization(String),
    /// The checkpoint blob exceeds the configured maximum size.
    StateTooLarge(usize),
    /// The task type does not support checkpointing.
    NotCheckpointable,
    /// A required fragment is missing and cannot be reconstructed.
    FragmentMissing { index: u32 },
    /// The reconstructed blob does not match its recorded blake3 hash.
    IntegrityFailure,
    /// Reed-Solomon reconstruction failed.
    ReconstructionFailed(String),
    /// No checkpoint exists for the requested task.
    NoCheckpointFound,
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(msg) => write!(f, "serialization error: {}", msg),
            Self::Deserialization(msg) => write!(f, "deserialization error: {}", msg),
            Self::StateTooLarge(size) => write!(f, "state too large: {} bytes", size),
            Self::NotCheckpointable => write!(f, "task is not checkpointable"),
            Self::FragmentMissing { index } => write!(f, "fragment {} missing", index),
            Self::IntegrityFailure => write!(f, "integrity check failed"),
            Self::ReconstructionFailed(msg) => write!(f, "reconstruction failed: {}", msg),
            Self::NoCheckpointFound => write!(f, "no checkpoint found for task"),
        }
    }
}

impl std::error::Error for CheckpointError {}

// ============================================================================
// Checkpointable trait
// ============================================================================

/// Trait that task types implement to support checkpoint/restore.
pub trait Checkpointable {
    /// Serialize the current state into a byte blob.
    fn checkpoint(&self) -> Result<Vec<u8>, CheckpointError>;

    /// Restore state from a previously checkpointed byte blob.
    fn restore(data: &[u8]) -> Result<Self, CheckpointError>
    where
        Self: Sized;

    /// Estimated size of the checkpoint blob in bytes.
    /// Used for pre-allocation and size-limit checks.
    fn estimated_checkpoint_size(&self) -> usize {
        1024 * 1024
    }

    /// If the task has a preferred checkpoint interval, return it.
    /// Otherwise the system default from [`LazarusConfig`] is used.
    fn preferred_checkpoint_interval(&self) -> Option<Duration> {
        None
    }
}

// ============================================================================
// Checkpoint + FragmentMap
// ============================================================================

/// Metadata for a single checkpoint of a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Unique identifier for this checkpoint (blake3 of task_id + sequence).
    pub id: CheckpointId,
    /// The task this checkpoint belongs to.
    pub task_id: TaskId,
    /// Monotonically increasing sequence number within the task.
    pub sequence: u64,
    /// Blake3 hash of the original (unpadded) checkpoint blob.
    pub blob_hash: Blake3Hash,
    /// Size of the original checkpoint blob in bytes.
    pub blob_size: u64,
    /// The node that created this checkpoint.
    pub origin_node: NodeId,
    /// When the checkpoint was created.
    pub created_at: SystemTime,
    /// Map describing where each fragment is stored.
    pub fragment_map: FragmentMap,
}

/// Describes the fragment layout of a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentMap {
    /// Total number of fragments (data + parity).
    pub total_fragments: u32,
    /// Number of parity (redundancy) fragments.
    pub redundancy_fragments: u32,
    /// Maps fragment index to the node that stores it.
    pub locations: HashMap<u32, NodeId>,
    /// Size of each fragment in bytes (all fragments are the same size).
    pub fragment_size: u64,
}

// ============================================================================
// CheckpointIndex
// ============================================================================

/// In-memory index of all known checkpoints and locally stored fragments.
#[derive(Debug, Clone)]
pub struct CheckpointIndex {
    /// The latest checkpoint for each task.
    pub latest: HashMap<TaskId, Checkpoint>,
    /// All checkpoints indexed by (task_id, sequence) -> checkpoint_id.
    pub all: BTreeMap<(TaskId, u64), CheckpointId>,
    /// Locally stored fragment data, keyed by (checkpoint_id, fragment_index).
    pub local_fragments: HashMap<(CheckpointId, u32), Vec<u8>>,
}

impl CheckpointIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self {
            latest: HashMap::new(),
            all: BTreeMap::new(),
            local_fragments: HashMap::new(),
        }
    }
}

impl Default for CheckpointIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// ResurrectionRequest / ResurrectionResult
// ============================================================================

/// A request to resurrect a task from its latest checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResurrectionRequest {
    /// The task to resurrect.
    pub task_id: TaskId,
    /// The node requesting the resurrection.
    pub requesting_node: NodeId,
    /// When the request was made.
    pub requested_at: SystemTime,
}

/// The result of a successful task resurrection.
#[derive(Debug, Clone)]
pub struct ResurrectionResult {
    /// The checkpoint that was used for resurrection.
    pub checkpoint: Checkpoint,
    /// The restored task state blob.
    pub blob: Vec<u8>,
    /// When the resurrection completed.
    pub restored_at: SystemTime,
    /// Number of fragments that had to be reconstructed (were missing).
    pub fragments_reconstructed: u32,
}

// ============================================================================
// Lazarus
// ============================================================================

/// The Lazarus checkpoint and resurrection engine.
///
/// Handles creating Reed-Solomon-encoded checkpoints, storing fragments,
/// and resurrecting tasks by reassembling and verifying checkpoint data.
pub struct Lazarus {
    /// Shared reference to the Neuromancer event bus.
    bus: Arc<NeuromancerBus>,
    /// Configuration for checkpoint behaviour.
    config: LazarusConfig,
    /// Identity of this node.
    local_node: NodeId,
    /// In-memory checkpoint and fragment index.
    index: CheckpointIndex,
    /// Monotonically increasing counter for generating checkpoint sequences.
    sequence_counter: u64,
}

impl Lazarus {
    /// Create a new Lazarus instance.
    pub fn new(config: LazarusConfig, bus: Arc<NeuromancerBus>, local_node: NodeId) -> Self {
        info!(
            node = %local_node,
            data_fragments = config.data_fragments,
            redundancy_fragments = config.redundancy_fragments,
            max_size = config.max_checkpoint_size,
            "Lazarus checkpoint engine initialized"
        );
        Self {
            bus,
            config,
            local_node,
            index: CheckpointIndex::new(),
            sequence_counter: 0,
        }
    }

    /// Create a Reed-Solomon-encoded checkpoint from a raw state blob.
    ///
    /// The blob is split into `data_fragments` equal-size chunks (the last
    /// chunk is zero-padded), then `redundancy_fragments` parity shards are
    /// computed.  All fragments are stored locally and a [`Checkpoint`]
    /// descriptor is returned.
    pub fn disperse_checkpoint(
        &mut self,
        task_id: TaskId,
        checkpoint_blob: &[u8],
    ) -> Result<Checkpoint, CheckpointError> {
        let blob_size = checkpoint_blob.len() as u64;

        // --- size check ---
        if blob_size > self.config.max_checkpoint_size as u64 {
            return Err(CheckpointError::StateTooLarge(checkpoint_blob.len()));
        }

        let data_count = self.config.data_fragments;
        let parity_count = self.config.redundancy_fragments;
        let total_fragments = data_count + parity_count;

        // --- compute shard size (ceil division, all shards same length) ---
        let shard_size = if checkpoint_blob.is_empty() {
            1 // avoid zero-length shards
        } else {
            checkpoint_blob.len().div_ceil(data_count)
        };

        // --- split blob into data shards, pad last with zeros ---
        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total_fragments);
        for i in 0..data_count {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, checkpoint_blob.len());
            let mut shard = if start < checkpoint_blob.len() {
                checkpoint_blob[start..end].to_vec()
            } else {
                Vec::new()
            };
            // Pad to shard_size
            shard.resize(shard_size, 0u8);
            shards.push(shard);
        }

        // --- add empty parity shards ---
        for _ in 0..parity_count {
            shards.push(vec![0u8; shard_size]);
        }

        // --- Reed-Solomon encode ---
        let rs = ReedSolomon::new(data_count, parity_count)
            .map_err(|e| CheckpointError::Serialization(format!("RS init: {}", e)))?;
        rs.encode(&mut shards)
            .map_err(|e| CheckpointError::Serialization(format!("RS encode: {}", e)))?;

        // --- compute checkpoint id and blob hash ---
        let blob_hash: Blake3Hash = *blake3::hash(checkpoint_blob).as_bytes();

        let sequence = self.sequence_counter;
        let mut id_input = Vec::with_capacity(32 + 8);
        id_input.extend_from_slice(&task_id);
        id_input.extend_from_slice(&sequence.to_le_bytes());
        let checkpoint_id: CheckpointId = *blake3::hash(&id_input).as_bytes();

        // --- store all fragments locally ---
        let mut locations = HashMap::new();
        for (i, shard) in shards.iter().enumerate() {
            let frag_idx = i as u32;
            self.index
                .local_fragments
                .insert((checkpoint_id, frag_idx), shard.clone());
            locations.insert(frag_idx, self.local_node);
        }

        let fragment_map = FragmentMap {
            total_fragments: total_fragments as u32,
            redundancy_fragments: parity_count as u32,
            locations,
            fragment_size: shard_size as u64,
        };

        let checkpoint = Checkpoint {
            id: checkpoint_id,
            task_id,
            sequence,
            blob_hash,
            blob_size,
            origin_node: self.local_node,
            created_at: SystemTime::now(),
            fragment_map,
        };

        // --- add to index ---
        self.index.latest.insert(task_id, checkpoint.clone());
        self.index.all.insert((task_id, sequence), checkpoint_id);

        // --- increment sequence counter ---
        self.sequence_counter += 1;

        // --- emit event ---
        let fragment_nodes: Vec<NodeId> = vec![self.local_node];
        self.bus.emit(MarabuntaEvent::CheckpointCreated {
            task_id,
            checkpoint_hash: blob_hash,
            fragment_nodes,
            timestamp: SystemTime::now(),
        });

        debug!(
            task_id = hex::encode(task_id),
            sequence,
            blob_size,
            total_fragments,
            shard_size,
            "checkpoint dispersed"
        );

        Ok(checkpoint)
    }

    /// Resurrect a task from its latest checkpoint.
    ///
    /// Collects all available fragments, reconstructs any missing ones via
    /// Reed-Solomon, reassembles the original blob, and verifies its blake3
    /// hash.
    pub fn resurrect_task(
        &self,
        task_id: TaskId,
    ) -> Result<ResurrectionResult, CheckpointError> {
        // --- find the latest checkpoint ---
        let checkpoint = self
            .index
            .latest
            .get(&task_id)
            .ok_or(CheckpointError::NoCheckpointFound)?
            .clone();

        let data_count = self.config.data_fragments;
        let parity_count = self.config.redundancy_fragments;
        let total_fragments = checkpoint.fragment_map.total_fragments as usize;
        let shard_size = checkpoint.fragment_map.fragment_size as usize;

        // --- collect fragments, noting which are missing ---
        let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(total_fragments);
        let mut missing_count: u32 = 0;

        for i in 0..total_fragments {
            let frag_idx = i as u32;
            match self.index.local_fragments.get(&(checkpoint.id, frag_idx)) {
                Some(data) => shards.push(Some(data.clone())),
                None => {
                    missing_count += 1;
                    shards.push(None);
                }
            }
        }

        // --- reconstruct if needed ---
        let fragments_reconstructed = missing_count;
        if missing_count > 0 {
            if missing_count as usize > parity_count {
                return Err(CheckpointError::ReconstructionFailed(format!(
                    "too many missing fragments: {} missing but only {} parity shards",
                    missing_count, parity_count
                )));
            }

            let rs = ReedSolomon::new(data_count, parity_count)
                .map_err(|e| CheckpointError::ReconstructionFailed(format!("RS init: {}", e)))?;

            // reed_solomon_erasure v6 reconstruct takes &mut [Option<Vec<u8>>]:
            // Some(data) for present shards, None for missing shards.
            let mut recon_shards: Vec<Option<Vec<u8>>> = shards
                .iter()
                .map(|opt| opt.clone())
                .collect();

            rs.reconstruct(&mut recon_shards).map_err(|e| {
                CheckpointError::ReconstructionFailed(format!("RS reconstruct: {}", e))
            })?;

            // Reassemble from the reconstructed data shards
            let mut blob = Vec::with_capacity(data_count * shard_size);
            for shard in recon_shards.iter().take(data_count).flatten() {
                blob.extend_from_slice(shard);
            }
            blob.truncate(checkpoint.blob_size as usize);

            // --- verify integrity ---
            let computed_hash: Blake3Hash = *blake3::hash(&blob).as_bytes();
            if computed_hash != checkpoint.blob_hash {
                return Err(CheckpointError::IntegrityFailure);
            }

            // --- emit events ---
            self.bus.emit(MarabuntaEvent::CheckpointRestored {
                task_id,
                checkpoint_hash: checkpoint.blob_hash,
                timestamp: SystemTime::now(),
            });

            self.bus.emit(MarabuntaEvent::TaskResurrected {
                id: task_id,
                from_checkpoint: checkpoint.blob_hash,
                new_node: self.local_node,
                timestamp: SystemTime::now(),
            });

            info!(
                task_id = hex::encode(task_id),
                sequence = checkpoint.sequence,
                fragments_reconstructed,
                blob_size = checkpoint.blob_size,
                "task resurrected (with reconstruction)"
            );

            return Ok(ResurrectionResult {
                checkpoint,
                blob,
                restored_at: SystemTime::now(),
                fragments_reconstructed,
            });
        }

        // --- all fragments present: reassemble directly ---
        let mut blob = Vec::with_capacity(data_count * shard_size);
        for data in shards.iter().take(data_count).flatten() {
            blob.extend_from_slice(data);
        }
        blob.truncate(checkpoint.blob_size as usize);

        // --- verify integrity ---
        let computed_hash: Blake3Hash = *blake3::hash(&blob).as_bytes();
        if computed_hash != checkpoint.blob_hash {
            return Err(CheckpointError::IntegrityFailure);
        }

        // --- emit events ---
        self.bus.emit(MarabuntaEvent::CheckpointRestored {
            task_id,
            checkpoint_hash: checkpoint.blob_hash,
            timestamp: SystemTime::now(),
        });

        self.bus.emit(MarabuntaEvent::TaskResurrected {
            id: task_id,
            from_checkpoint: checkpoint.blob_hash,
            new_node: self.local_node,
            timestamp: SystemTime::now(),
        });

        info!(
            task_id = hex::encode(task_id),
            sequence = checkpoint.sequence,
            blob_size = checkpoint.blob_size,
            "task resurrected"
        );

        Ok(ResurrectionResult {
            checkpoint,
            blob,
            restored_at: SystemTime::now(),
            fragments_reconstructed: 0,
        })
    }

    /// Store a fragment in the local fragment store.
    ///
    /// Called when receiving fragments from remote nodes (e.g. via gossip).
    pub fn store_fragment(
        &mut self,
        checkpoint_id: CheckpointId,
        fragment_index: u32,
        data: Vec<u8>,
    ) {
        debug!(
            checkpoint_id = hex::encode(checkpoint_id),
            fragment_index,
            size = data.len(),
            "storing fragment"
        );
        self.index
            .local_fragments
            .insert((checkpoint_id, fragment_index), data);
    }

    /// Retrieve a fragment from the local fragment store.
    pub fn get_fragment(
        &self,
        checkpoint_id: CheckpointId,
        fragment_index: u32,
    ) -> Result<Vec<u8>, CheckpointError> {
        self.index
            .local_fragments
            .get(&(checkpoint_id, fragment_index))
            .cloned()
            .ok_or(CheckpointError::FragmentMissing {
                index: fragment_index,
            })
    }

    /// Remove a locally stored fragment (useful in tests to simulate node failures).
    pub fn remove_fragment(&mut self, checkpoint_id: CheckpointId, fragment_index: u32) {
        self.index.local_fragments.remove(&(checkpoint_id, fragment_index));
    }

    /// Remove old checkpoints, keeping only the most recent `retention_per_task`
    /// checkpoints for each task.
    ///
    /// Fragments belonging to pruned checkpoints are also removed.
    pub fn cleanup_old_checkpoints(&mut self) {
        let retention = self.config.retention_per_task;

        // Group all checkpoint entries by task_id
        let mut by_task: HashMap<TaskId, Vec<(u64, CheckpointId)>> = HashMap::new();
        for (&(task_id, seq), &ckpt_id) in &self.index.all {
            by_task.entry(task_id).or_default().push((seq, ckpt_id));
        }

        let mut removed_count: usize = 0;

        for (task_id, mut entries) in by_task {
            if entries.len() <= retention {
                continue;
            }

            // Sort by sequence ascending
            entries.sort_by_key(|&(seq, _)| seq);

            // Remove the oldest entries, keeping only `retention` latest
            let to_remove = entries.len() - retention;
            for &(seq, ckpt_id) in entries.iter().take(to_remove) {
                // Remove from the `all` index
                self.index.all.remove(&(task_id, seq));

                // Find total fragments for this checkpoint by checking the
                // fragment map if this is the latest, or by scanning local_fragments.
                // We scan local_fragments keys to find all fragments for this checkpoint.
                let frag_keys: Vec<(CheckpointId, u32)> = self
                    .index
                    .local_fragments
                    .keys()
                    .filter(|(cid, _)| *cid == ckpt_id)
                    .cloned()
                    .collect();

                for key in frag_keys {
                    self.index.local_fragments.remove(&key);
                }

                removed_count += 1;
            }

            // Update `latest` if the current latest was removed (shouldn't happen
            // since we keep the newest, but be defensive).
            if let Some(latest) = self.index.latest.get(&task_id) {
                if !self.index.all.values().any(|id| *id == latest.id) {
                    // The latest was removed — update to the newest remaining
                    if let Some((&(_, _seq), &newest_id)) = self
                        .index
                        .all
                        .range((task_id, 0)..=(task_id, u64::MAX))
                        .next_back()
                    {
                        // We don't have the full Checkpoint struct for older entries,
                        // so only remove the stale latest pointer if it was pruned.
                        // In practice, the latest should always be retained.
                        let _ = newest_id;
                    } else {
                        self.index.latest.remove(&task_id);
                    }
                }
            }
        }

        if removed_count > 0 {
            info!(removed_count, "cleaned up old checkpoints");
        }
    }

    /// Return the current checkpoint index (read-only reference).
    pub fn index(&self) -> &CheckpointIndex {
        &self.index
    }

    /// Return the current sequence counter.
    pub fn sequence_counter(&self) -> u64 {
        self.sequence_counter
    }

    /// Return a reference to the configuration.
    pub fn config(&self) -> &LazarusConfig {
        &self.config
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::neuromancer::bus::NeuromancerBus;
    use crate::swarm::neuromancer::config::LazarusConfig;

    /// Helper: create a Lazarus instance with default config.
    fn make_lazarus() -> Lazarus {
        let config = LazarusConfig::default();
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        Lazarus::new(config, bus, node)
    }

    /// Helper: create a Lazarus with custom data/parity fragment counts.
    fn make_lazarus_custom(data: usize, parity: usize, retention: usize) -> Lazarus {
        let config = LazarusConfig {
            data_fragments: data,
            redundancy_fragments: parity,
            retention_per_task: retention,
            ..LazarusConfig::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        Lazarus::new(config, bus, node)
    }

    // -----------------------------------------------------------------------
    // 1. Checkpoint roundtrip: create -> disperse -> resurrect -> verify
    // -----------------------------------------------------------------------
    #[test]
    fn test_checkpoint_roundtrip() {
        let mut lazarus = make_lazarus();
        let task_id: TaskId = [42u8; 32];
        let blob = b"hello world, this is a checkpoint blob for testing purposes!";

        let ckpt = lazarus.disperse_checkpoint(task_id, blob).unwrap();
        assert_eq!(ckpt.task_id, task_id);
        assert_eq!(ckpt.blob_size, blob.len() as u64);
        assert_eq!(ckpt.sequence, 0);

        let result = lazarus.resurrect_task(task_id).unwrap();
        assert_eq!(result.blob, blob.to_vec());
        assert_eq!(result.fragments_reconstructed, 0);
        assert_eq!(result.checkpoint.id, ckpt.id);
    }

    // -----------------------------------------------------------------------
    // 2. Reed-Solomon encode/decode: encode 4+2, drop 2 data shards, recover
    // -----------------------------------------------------------------------
    #[test]
    fn test_reed_solomon_encode_decode_drop_two() {
        let mut lazarus = make_lazarus_custom(4, 2, 3);
        let task_id: TaskId = [7u8; 32];
        let blob: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();

        let ckpt = lazarus.disperse_checkpoint(task_id, &blob).unwrap();

        // Remove data shards 0 and 1 (simulate loss)
        lazarus
            .index
            .local_fragments
            .remove(&(ckpt.id, 0));
        lazarus
            .index
            .local_fragments
            .remove(&(ckpt.id, 1));

        let result = lazarus.resurrect_task(task_id).unwrap();
        assert_eq!(result.blob, blob);
        assert_eq!(result.fragments_reconstructed, 2);
    }

    // -----------------------------------------------------------------------
    // 3. RS: dropping 3 of 6 shards fails (only 2 redundancy)
    // -----------------------------------------------------------------------
    #[test]
    fn test_reed_solomon_too_many_missing_fails() {
        let mut lazarus = make_lazarus_custom(4, 2, 3);
        let task_id: TaskId = [8u8; 32];
        let blob: Vec<u8> = (0..100).collect();

        let ckpt = lazarus.disperse_checkpoint(task_id, &blob).unwrap();

        // Remove 3 shards — exceeds the 2 parity shards available
        lazarus.index.local_fragments.remove(&(ckpt.id, 0));
        lazarus.index.local_fragments.remove(&(ckpt.id, 2));
        lazarus.index.local_fragments.remove(&(ckpt.id, 4));

        let result = lazarus.resurrect_task(task_id);
        assert!(result.is_err());
        match result.unwrap_err() {
            CheckpointError::ReconstructionFailed(msg) => {
                assert!(msg.contains("too many missing"));
            }
            other => panic!("expected ReconstructionFailed, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // 4. Integrity check: tamper with blob hash -> verify hash mismatch
    // -----------------------------------------------------------------------
    #[test]
    fn test_integrity_check_detects_tampering() {
        let mut lazarus = make_lazarus();
        let task_id: TaskId = [9u8; 32];
        let blob = b"original data that must not be tampered with";

        let ckpt = lazarus.disperse_checkpoint(task_id, blob).unwrap();

        // Tamper with a data fragment (shard 0)
        let shard_key = (ckpt.id, 0u32);
        if let Some(shard) = lazarus.index.local_fragments.get_mut(&shard_key) {
            if !shard.is_empty() {
                shard[0] ^= 0xFF; // flip bits
            }
        }

        let result = lazarus.resurrect_task(task_id);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CheckpointError::IntegrityFailure);
    }

    // -----------------------------------------------------------------------
    // 5. Fragment storage: store + get roundtrip
    // -----------------------------------------------------------------------
    #[test]
    fn test_fragment_store_and_get() {
        let mut lazarus = make_lazarus();
        let ckpt_id: CheckpointId = [10u8; 32];
        let frag_data = vec![1u8, 2, 3, 4, 5];

        lazarus.store_fragment(ckpt_id, 0, frag_data.clone());
        let retrieved = lazarus.get_fragment(ckpt_id, 0).unwrap();
        assert_eq!(retrieved, frag_data);

        // Non-existent fragment returns error
        let missing = lazarus.get_fragment(ckpt_id, 99);
        assert!(missing.is_err());
        assert_eq!(
            missing.unwrap_err(),
            CheckpointError::FragmentMissing { index: 99 }
        );
    }

    // -----------------------------------------------------------------------
    // 6. Checkpoint index: latest per task updated correctly
    // -----------------------------------------------------------------------
    #[test]
    fn test_checkpoint_index_latest_updated() {
        let mut lazarus = make_lazarus();
        let task_id: TaskId = [11u8; 32];

        let ckpt1 = lazarus
            .disperse_checkpoint(task_id, b"state v1")
            .unwrap();
        assert_eq!(lazarus.index.latest[&task_id].sequence, 0);

        let ckpt2 = lazarus
            .disperse_checkpoint(task_id, b"state v2")
            .unwrap();
        assert_eq!(lazarus.index.latest[&task_id].sequence, 1);
        assert_ne!(ckpt1.id, ckpt2.id);

        // The latest should be the second checkpoint
        assert_eq!(lazarus.index.latest[&task_id].id, ckpt2.id);

        // Both should be in the `all` index
        assert!(lazarus.index.all.contains_key(&(task_id, 0)));
        assert!(lazarus.index.all.contains_key(&(task_id, 1)));
    }

    // -----------------------------------------------------------------------
    // 7. Cleanup: old checkpoints removed, latest retained
    // -----------------------------------------------------------------------
    #[test]
    fn test_cleanup_old_checkpoints() {
        // retention = 2, so after 4 checkpoints, cleanup keeps only the latest 2
        let mut lazarus = make_lazarus_custom(4, 2, 2);
        let task_id: TaskId = [12u8; 32];

        let ckpt0 = lazarus.disperse_checkpoint(task_id, b"v0").unwrap();
        let _ckpt1 = lazarus.disperse_checkpoint(task_id, b"v1").unwrap();
        let ckpt2 = lazarus.disperse_checkpoint(task_id, b"v2").unwrap();
        let ckpt3 = lazarus.disperse_checkpoint(task_id, b"v3").unwrap();

        assert_eq!(lazarus.index.all.len(), 4);

        lazarus.cleanup_old_checkpoints();

        // Should have 2 remaining
        assert_eq!(lazarus.index.all.len(), 2);

        // The oldest two (seq 0,1) should be gone
        assert!(!lazarus.index.all.contains_key(&(task_id, 0)));
        assert!(!lazarus.index.all.contains_key(&(task_id, 1)));

        // The newest two (seq 2,3) should remain
        assert!(lazarus.index.all.contains_key(&(task_id, 2)));
        assert!(lazarus.index.all.contains_key(&(task_id, 3)));

        // Fragments for ckpt0 should be gone
        for i in 0..6u32 {
            assert!(lazarus.index.local_fragments.get(&(ckpt0.id, i)).is_none());
        }

        // Fragments for ckpt2 and ckpt3 should still be present
        assert!(lazarus.index.local_fragments.get(&(ckpt2.id, 0)).is_some());
        assert!(lazarus.index.local_fragments.get(&(ckpt3.id, 0)).is_some());

        // Latest should still point to ckpt3
        assert_eq!(lazarus.index.latest[&task_id].id, ckpt3.id);
    }

    // -----------------------------------------------------------------------
    // 8. CheckpointTooLarge error on oversized blob
    // -----------------------------------------------------------------------
    #[test]
    fn test_checkpoint_too_large_error() {
        let config = LazarusConfig {
            max_checkpoint_size: 100, // very small limit
            ..LazarusConfig::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let mut lazarus = Lazarus::new(config, bus, node);

        let task_id: TaskId = [13u8; 32];
        let big_blob = vec![0u8; 200]; // exceeds 100 byte limit

        let result = lazarus.disperse_checkpoint(task_id, &big_blob);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CheckpointError::StateTooLarge(200));
    }

    // -----------------------------------------------------------------------
    // 9. No checkpoint found error
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_checkpoint_found_error() {
        let lazarus = make_lazarus();
        let task_id: TaskId = [99u8; 32];

        let result = lazarus.resurrect_task(task_id);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CheckpointError::NoCheckpointFound);
    }

    // -----------------------------------------------------------------------
    // 10. Multiple tasks — checkpoints are independent
    // -----------------------------------------------------------------------
    #[test]
    fn test_multiple_tasks_independent() {
        let mut lazarus = make_lazarus();
        let task_a: TaskId = [0xAAu8; 32];
        let task_b: TaskId = [0xBBu8; 32];

        let blob_a = b"task A state";
        let blob_b = b"task B state which is different and longer";

        lazarus.disperse_checkpoint(task_a, blob_a).unwrap();
        lazarus.disperse_checkpoint(task_b, blob_b).unwrap();

        let result_a = lazarus.resurrect_task(task_a).unwrap();
        let result_b = lazarus.resurrect_task(task_b).unwrap();

        assert_eq!(result_a.blob, blob_a.to_vec());
        assert_eq!(result_b.blob, blob_b.to_vec());
    }

    // -----------------------------------------------------------------------
    // 11. Empty blob checkpoint roundtrip
    // -----------------------------------------------------------------------
    #[test]
    fn test_empty_blob_roundtrip() {
        let mut lazarus = make_lazarus();
        let task_id: TaskId = [14u8; 32];

        let ckpt = lazarus.disperse_checkpoint(task_id, b"").unwrap();
        assert_eq!(ckpt.blob_size, 0);

        let result = lazarus.resurrect_task(task_id).unwrap();
        assert!(result.blob.is_empty());
    }

    // -----------------------------------------------------------------------
    // 12. Sequence counter increments correctly
    // -----------------------------------------------------------------------
    #[test]
    fn test_sequence_counter() {
        let mut lazarus = make_lazarus();
        let task_a: TaskId = [0xA0u8; 32];
        let task_b: TaskId = [0xB0u8; 32];

        assert_eq!(lazarus.sequence_counter(), 0);

        lazarus.disperse_checkpoint(task_a, b"a1").unwrap();
        assert_eq!(lazarus.sequence_counter(), 1);

        lazarus.disperse_checkpoint(task_b, b"b1").unwrap();
        assert_eq!(lazarus.sequence_counter(), 2);

        lazarus.disperse_checkpoint(task_a, b"a2").unwrap();
        assert_eq!(lazarus.sequence_counter(), 3);
    }
}
