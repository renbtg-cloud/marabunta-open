// Marabunta - Licensed under the MIT License.
//! Job checkpointing for fault tolerance (Phase 9).
//!
//! Long-running chunks can write intermediate state to a checkpoint
//! directory. The [`CheckpointManager`] periodically scans that directory,
//! hashes the contents, and creates a [`Checkpoint`] record. If the node
//! executing a chunk dies, the chunk is reassigned to a different node
//! which can resume from the latest checkpoint instead of restarting
//! from scratch.
//!
//! # Checkpoint protocol
//!
//! 1. Before executing a chunk, the work engine calls
//!    [`CheckpointManager::prepare_chunk_dir`] to create a per-chunk
//!    directory and set the `MARABUNTA_CHECKPOINT_DIR` environment variable.
//!
//! 2. The running script writes arbitrary files to `$MARABUNTA_CHECKPOINT_DIR`
//!    whenever it reaches a stable state.
//!
//! 3. Periodically (every [`CHECKPOINT_INTERVAL`]) the checkpoint manager
//!    calls [`snapshot_chunk`] which hashes the directory contents and
//!    creates a [`Checkpoint`] that is announced via gossip
//!    ([`SwarmMessage::CheckpointAnnounce`]).
//!
//! 4. When a chunk is reassigned (e.g. after the original node dies), the
//!    new executor calls [`restore_to_dir`] to copy checkpoint data into
//!    the new working directory, then sets `MARABUNTA_RESUME=1` so the
//!    script knows to read its saved state.
//!
//! 5. Once a chunk completes, [`remove_checkpoint`] cleans up.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::common::types::JobId;
use super::config::{CHECKPOINT_DIR_ENV, CHECKPOINT_INTERVAL, CHECKPOINT_RESUME_ENV};
use super::types::{BlobHash, BlobRef, ChunkId, NodeId, SwarmError};

// ============================================================================
// Checkpoint
// ============================================================================

/// A checkpoint snapshot for a running chunk.
///
/// Contains a reference to the state blob (the hashed contents of the
/// checkpoint directory) and metadata about when and where it was taken.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Which chunk this checkpoint belongs to.
    pub chunk_id: ChunkId,
    /// The parent job.
    pub job_id: JobId,
    /// Estimated progress (0.0 to 100.0).
    pub progress_pct: f32,
    /// Reference to the serialized checkpoint state.
    pub state_blob: BlobRef,
    /// When this checkpoint was created.
    pub created_at: DateTime<Utc>,
    /// Which node created this checkpoint.
    pub node_id: NodeId,
}

// ============================================================================
// RetryPolicy
// ============================================================================

/// Retry policy for chunk execution.
///
/// Controls how many times a chunk can be retried, the backoff between
/// retries, and whether speculative execution should be used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of execution attempts (including the first).
    pub max_attempts: u32,
    /// Backoff duration between retries.
    pub backoff: Duration,
    /// If a chunk has been running for longer than `speculative_threshold`,
    /// a speculative copy may be launched on another node. The first to
    /// complete wins.
    pub speculative_threshold: Duration,
    /// Minimum progress percentage to consider a partial success (0..100).
    /// If a chunk fails but has checkpointed past this threshold, it can
    /// be resumed instead of restarted.
    pub partial_success_pct: f32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Duration::from_secs(5),
            speculative_threshold: Duration::from_secs(600), // 2x default chunk timeout
            partial_success_pct: 100.0,
        }
    }
}

// ============================================================================
// CheckpointManager
// ============================================================================

/// Manages checkpoint state for running chunks.
///
/// Thread-safe: all mutable state is stored in a [`DashMap`].
pub struct CheckpointManager {
    /// Latest checkpoint per chunk.
    checkpoints: DashMap<ChunkId, Checkpoint>,
    /// Base directory under which per-chunk checkpoint directories are created.
    checkpoint_dir: PathBuf,
    /// How often to snapshot running chunks.
    interval: Duration,
}

impl CheckpointManager {
    /// Create a new checkpoint manager.
    ///
    /// `checkpoint_dir` is the base directory; each chunk gets a subdirectory
    /// named `<job_id>/<chunk_id>/`.
    pub fn new(checkpoint_dir: PathBuf, interval: Duration) -> Self {
        Self {
            checkpoints: DashMap::new(),
            checkpoint_dir,
            interval,
        }
    }

    /// Create with default interval from config.
    pub fn with_default_interval(checkpoint_dir: PathBuf) -> Self {
        Self::new(checkpoint_dir, CHECKPOINT_INTERVAL)
    }

    /// Return the configured snapshot interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Return the base checkpoint directory.
    pub fn base_dir(&self) -> &Path {
        &self.checkpoint_dir
    }

    // ========================================================================
    // Directory management
    // ========================================================================

    /// Prepare the checkpoint directory for a chunk.
    ///
    /// Creates the directory `<base>/<job_id>/<chunk_id>/` and returns
    /// the path. The caller should set `MARABUNTA_CHECKPOINT_DIR` to this
    /// path before spawning the chunk process.
    pub fn prepare_chunk_dir(
        &self,
        chunk_id: &ChunkId,
        job_id: &JobId,
    ) -> Result<PathBuf, SwarmError> {
        let dir = self
            .checkpoint_dir
            .join(format!("{}", job_id.0))
            .join(format!("{}", chunk_id.0));

        std::fs::create_dir_all(&dir).map_err(|e| {
            SwarmError::Checkpoint(format!(
                "failed to create checkpoint dir {}: {}",
                dir.display(),
                e
            ))
        })?;

        debug!(
            chunk_id = %chunk_id,
            dir = %dir.display(),
            "checkpoint directory prepared"
        );

        Ok(dir)
    }

    /// Get the path to the checkpoint directory for a chunk (without creating it).
    fn chunk_dir(&self, chunk_id: &ChunkId, job_id: &JobId) -> PathBuf {
        self.checkpoint_dir
            .join(format!("{}", job_id.0))
            .join(format!("{}", chunk_id.0))
    }

    // ========================================================================
    // Snapshot
    // ========================================================================

    /// Scan the checkpoint directory for a running chunk, hash all files,
    /// and create a [`Checkpoint`].
    ///
    /// Returns `None` if the directory does not exist, is empty, or
    /// cannot be read.
    pub async fn snapshot_chunk(
        &self,
        chunk_id: &ChunkId,
        job_id: &JobId,
        node_id: NodeId,
    ) -> Option<Checkpoint> {
        let dir = self.chunk_dir(chunk_id, job_id);

        if !dir.exists() {
            return None;
        }

        // Read all files in the checkpoint directory and compute a
        // composite hash. We do this on the blocking pool since it involves
        // filesystem I/O.
        let dir_clone = dir.clone();
        let hash_result = tokio::task::spawn_blocking(move || {
            hash_directory(&dir_clone)
        })
        .await;

        let (hash, total_size, file_count) = match hash_result {
            Ok(Some(result)) => result,
            Ok(None) => return None,
            Err(e) => {
                warn!(
                    chunk_id = %chunk_id,
                    error = %e,
                    "failed to hash checkpoint directory"
                );
                return None;
            }
        };

        if file_count == 0 {
            return None;
        }

        // Estimate progress from a progress file if the script writes one.
        let progress_pct = read_progress_file(&dir).unwrap_or(0.0);

        let state_blob = BlobRef {
            hash,
            size_bytes: total_size,
            filename: Some(format!("checkpoint-{}", chunk_id)),
        };

        let checkpoint = Checkpoint {
            chunk_id: *chunk_id,
            job_id: *job_id,
            progress_pct,
            state_blob,
            created_at: Utc::now(),
            node_id,
        };

        // Store locally.
        self.checkpoints.insert(*chunk_id, checkpoint.clone());

        info!(
            chunk_id = %chunk_id,
            progress = progress_pct,
            files = file_count,
            total_bytes = total_size,
            "checkpoint snapshot created"
        );

        Some(checkpoint)
    }

    // ========================================================================
    // Storage and retrieval
    // ========================================================================

    /// Store a checkpoint received from gossip or a direct message.
    ///
    /// Only replaces the existing checkpoint if the new one is newer.
    pub fn store_checkpoint(&self, checkpoint: Checkpoint) {
        let chunk_id = checkpoint.chunk_id;

        let should_store = match self.checkpoints.get(&chunk_id) {
            Some(existing) => checkpoint.created_at > existing.created_at,
            None => true,
        };

        if should_store {
            debug!(
                chunk_id = %chunk_id,
                progress = checkpoint.progress_pct,
                "storing checkpoint"
            );
            self.checkpoints.insert(chunk_id, checkpoint);
        }
    }

    /// Get the latest checkpoint for a chunk (if any).
    pub fn get_checkpoint(&self, chunk_id: &ChunkId) -> Option<Checkpoint> {
        self.checkpoints.get(chunk_id).map(|r| r.value().clone())
    }

    /// Restore checkpoint data to a target job directory.
    ///
    /// Copies all files from the checkpoint directory into `target_dir`.
    /// Returns `true` if files were restored, `false` if no checkpoint
    /// data was found.
    pub async fn restore_to_dir(
        &self,
        chunk_id: &ChunkId,
        target_dir: &Path,
    ) -> Result<bool, SwarmError> {
        let checkpoint = match self.checkpoints.get(chunk_id) {
            Some(cp) => cp.clone(),
            None => return Ok(false),
        };

        let source_dir = self.chunk_dir(chunk_id, &checkpoint.job_id);
        if !source_dir.exists() {
            return Ok(false);
        }

        let target = target_dir.to_path_buf();
        let result = tokio::task::spawn_blocking(move || {
            copy_directory_contents(&source_dir, &target)
        })
        .await
        .map_err(|e| SwarmError::Checkpoint(format!("restore task panicked: {}", e)))?;

        match result {
            Ok(count) => {
                info!(
                    chunk_id = %chunk_id,
                    files_restored = count,
                    target = %target_dir.display(),
                    "checkpoint restored"
                );
                Ok(count > 0)
            }
            Err(e) => Err(e),
        }
    }

    /// Get the environment variables to set for a chunk execution.
    ///
    /// Always sets `MARABUNTA_CHECKPOINT_DIR`. If a checkpoint exists for
    /// this chunk, also sets `MARABUNTA_RESUME=1`.
    pub fn env_vars(&self, chunk_id: &ChunkId, job_dir: &Path) -> Vec<(String, String)> {
        let checkpoint_dir = job_dir.join("checkpoint");
        let mut vars = vec![(
            CHECKPOINT_DIR_ENV.to_string(),
            checkpoint_dir.to_string_lossy().into_owned(),
        )];

        if self.checkpoints.contains_key(chunk_id) {
            vars.push((CHECKPOINT_RESUME_ENV.to_string(), "1".to_string()));
        }

        vars
    }

    /// Remove checkpoint data for a completed chunk.
    pub fn remove_checkpoint(&self, chunk_id: &ChunkId) {
        if let Some((_, checkpoint)) = self.checkpoints.remove(chunk_id) {
            let dir = self.chunk_dir(chunk_id, &checkpoint.job_id);
            if dir.exists() {
                if let Err(e) = std::fs::remove_dir_all(&dir) {
                    debug!(
                        chunk_id = %chunk_id,
                        error = %e,
                        "failed to remove checkpoint directory"
                    );
                }
            }
            debug!(chunk_id = %chunk_id, "checkpoint removed");
        }
    }

    /// List all stored checkpoints.
    pub fn list(&self) -> Vec<Checkpoint> {
        self.checkpoints
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    /// Number of stored checkpoints.
    pub fn count(&self) -> usize {
        self.checkpoints.len()
    }

    /// Remove all checkpoints for a given job.
    pub fn remove_job_checkpoints(&self, job_id: &JobId) {
        let to_remove: Vec<ChunkId> = self
            .checkpoints
            .iter()
            .filter(|r| r.job_id == *job_id)
            .map(|r| r.chunk_id)
            .collect();

        for chunk_id in to_remove {
            self.remove_checkpoint(&chunk_id);
        }
    }
}

// ============================================================================
// Filesystem helpers
// ============================================================================

/// Hash all files in a directory, returning (composite_hash, total_size, file_count).
///
/// The composite hash is computed by hashing each file's path (relative to
/// `dir`) and contents in sorted order, then hashing all those individual
/// hashes together. This makes the composite hash deterministic regardless
/// of filesystem iteration order.
fn hash_directory(dir: &Path) -> Option<(BlobHash, u64, usize)> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return None,
    };

    let mut file_hashes: Vec<(String, BlobHash, u64)> = Vec::new();

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let relative = path
            .strip_prefix(dir)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();

        match std::fs::read(&path) {
            Ok(contents) => {
                let mut hasher = Sha256::new();
                hasher.update(relative.as_bytes());
                hasher.update(&contents);
                let hash: BlobHash = hasher.finalize().into();
                let size = contents.len() as u64;
                file_hashes.push((relative, hash, size));
            }
            Err(e) => {
                debug!(
                    path = %path.display(),
                    error = %e,
                    "failed to read file for checkpoint hash"
                );
            }
        }
    }

    if file_hashes.is_empty() {
        return None;
    }

    // Sort by relative path for deterministic ordering.
    file_hashes.sort_by(|a, b| a.0.cmp(&b.0));

    let total_size: u64 = file_hashes.iter().map(|(_, _, s)| s).sum();
    let file_count = file_hashes.len();

    // Compute composite hash.
    let mut composite_hasher = Sha256::new();
    for (name, hash, size) in &file_hashes {
        composite_hasher.update(name.as_bytes());
        composite_hasher.update(hash);
        composite_hasher.update(size.to_le_bytes());
    }

    let composite: BlobHash = composite_hasher.finalize().into();
    Some((composite, total_size, file_count))
}

/// Read a progress file (`progress.txt` or `progress.json`) from the
/// checkpoint directory. The file should contain a single float value
/// (0.0 to 100.0) or a JSON object with a `progress` field.
fn read_progress_file(dir: &Path) -> Option<f32> {
    // Try plain text first.
    let txt_path = dir.join("progress.txt");
    if let Ok(contents) = std::fs::read_to_string(&txt_path) {
        if let Ok(pct) = contents.trim().parse::<f32>() {
            return Some(pct.clamp(0.0, 100.0));
        }
    }

    // Try JSON.
    let json_path = dir.join("progress.json");
    if let Ok(contents) = std::fs::read_to_string(&json_path) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&contents) {
            if let Some(pct) = value.get("progress").and_then(|v| v.as_f64()) {
                return Some((pct as f32).clamp(0.0, 100.0));
            }
        }
    }

    None
}

/// Copy all files from `src_dir` into `dst_dir`, creating `dst_dir` if needed.
/// Returns the number of files copied.
fn copy_directory_contents(src_dir: &Path, dst_dir: &Path) -> Result<usize, SwarmError> {
    std::fs::create_dir_all(dst_dir).map_err(|e| {
        SwarmError::Checkpoint(format!(
            "failed to create target dir {}: {}",
            dst_dir.display(),
            e
        ))
    })?;

    let entries = std::fs::read_dir(src_dir).map_err(|e| {
        SwarmError::Checkpoint(format!(
            "failed to read checkpoint dir {}: {}",
            src_dir.display(),
            e
        ))
    })?;

    let mut count = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            let filename = path.file_name().unwrap_or_default();
            let dst_path = dst_dir.join(filename);
            std::fs::copy(&path, &dst_path).map_err(|e| {
                SwarmError::Checkpoint(format!(
                    "failed to copy {} -> {}: {}",
                    path.display(),
                    dst_path.display(),
                    e
                ))
            })?;
            count += 1;
        }
    }

    Ok(count)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_manager(dir: &Path) -> CheckpointManager {
        CheckpointManager::new(dir.to_path_buf(), Duration::from_secs(60))
    }

    // -----------------------------------------------------------------------
    // RetryPolicy
    // -----------------------------------------------------------------------

    #[test]
    fn retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.backoff, Duration::from_secs(5));
        assert_eq!(policy.speculative_threshold, Duration::from_secs(600));
        assert!((policy.partial_success_pct - 100.0).abs() < f32::EPSILON);
    }

    #[test]
    fn retry_policy_serialize_deserialize() {
        let policy = RetryPolicy {
            max_attempts: 5,
            backoff: Duration::from_secs(10),
            speculative_threshold: Duration::from_secs(120),
            partial_success_pct: 50.0,
        };

        let json = serde_json::to_string(&policy).unwrap();
        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_attempts, 5);
        assert_eq!(deserialized.backoff, Duration::from_secs(10));
        assert!((deserialized.partial_success_pct - 50.0).abs() < f32::EPSILON);
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: prepare_chunk_dir
    // -----------------------------------------------------------------------

    #[test]
    fn prepare_chunk_dir_creates_directory() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();

        let dir = mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();
        assert!(dir.exists());
        assert!(dir.is_dir());

        // Path should contain both job_id and chunk_id.
        let dir_str = dir.to_string_lossy();
        assert!(dir_str.contains(&job_id.0.to_string()));
        assert!(dir_str.contains(&chunk_id.0.to_string()));
    }

    #[test]
    fn prepare_chunk_dir_idempotent() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();

        let dir1 = mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();
        let dir2 = mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();
        assert_eq!(dir1, dir2);
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: snapshot_chunk
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn snapshot_chunk_empty_dir_returns_none() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let node_id = NodeId::new();

        // Create the directory but leave it empty.
        mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();

        let result = mgr.snapshot_chunk(&chunk_id, &job_id, node_id).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn snapshot_chunk_nonexistent_dir_returns_none() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let node_id = NodeId::new();

        // Don't create any directory.
        let result = mgr.snapshot_chunk(&chunk_id, &job_id, node_id).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn snapshot_chunk_with_files() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let node_id = NodeId::new();

        let dir = mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();

        // Write some checkpoint files.
        std::fs::write(dir.join("state.bin"), b"some state data").unwrap();
        std::fs::write(dir.join("weights.dat"), b"model weights").unwrap();

        let result = mgr.snapshot_chunk(&chunk_id, &job_id, node_id).await;
        assert!(result.is_some());

        let checkpoint = result.unwrap();
        assert_eq!(checkpoint.chunk_id, chunk_id);
        assert_eq!(checkpoint.job_id, job_id);
        assert_eq!(checkpoint.node_id, node_id);
        assert!(checkpoint.state_blob.size_bytes > 0);
        assert!(checkpoint.state_blob.hash != [0u8; 32]);

        // Should be stored in the manager.
        assert!(mgr.get_checkpoint(&chunk_id).is_some());
    }

    #[tokio::test]
    async fn snapshot_chunk_with_progress_txt() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let node_id = NodeId::new();

        let dir = mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();
        std::fs::write(dir.join("state.bin"), b"data").unwrap();
        std::fs::write(dir.join("progress.txt"), "42.5").unwrap();

        let checkpoint = mgr
            .snapshot_chunk(&chunk_id, &job_id, node_id)
            .await
            .unwrap();
        assert!((checkpoint.progress_pct - 42.5).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn snapshot_chunk_with_progress_json() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let node_id = NodeId::new();

        let dir = mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();
        std::fs::write(dir.join("state.bin"), b"data").unwrap();
        std::fs::write(
            dir.join("progress.json"),
            r#"{"progress": 75.0, "epoch": 15}"#,
        )
        .unwrap();

        let checkpoint = mgr
            .snapshot_chunk(&chunk_id, &job_id, node_id)
            .await
            .unwrap();
        assert!((checkpoint.progress_pct - 75.0).abs() < f32::EPSILON);
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: store_checkpoint
    // -----------------------------------------------------------------------

    #[test]
    fn store_checkpoint_basic() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();

        let checkpoint = Checkpoint {
            chunk_id,
            job_id: JobId::new(),
            progress_pct: 30.0,
            state_blob: BlobRef {
                hash: [1u8; 32],
                size_bytes: 1024,
                filename: None,
            },
            created_at: Utc::now(),
            node_id: NodeId::new(),
        };

        mgr.store_checkpoint(checkpoint.clone());
        let stored = mgr.get_checkpoint(&chunk_id).unwrap();
        assert_eq!(stored.chunk_id, chunk_id);
        assert!((stored.progress_pct - 30.0).abs() < f32::EPSILON);
    }

    #[test]
    fn store_checkpoint_newer_replaces_older() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();

        let old = Checkpoint {
            chunk_id,
            job_id,
            progress_pct: 10.0,
            state_blob: BlobRef {
                hash: [1u8; 32],
                size_bytes: 100,
                filename: None,
            },
            created_at: Utc::now() - chrono::Duration::seconds(60),
            node_id: NodeId::new(),
        };

        let new = Checkpoint {
            chunk_id,
            job_id,
            progress_pct: 50.0,
            state_blob: BlobRef {
                hash: [2u8; 32],
                size_bytes: 200,
                filename: None,
            },
            created_at: Utc::now(),
            node_id: NodeId::new(),
        };

        mgr.store_checkpoint(old);
        mgr.store_checkpoint(new);

        let stored = mgr.get_checkpoint(&chunk_id).unwrap();
        assert!((stored.progress_pct - 50.0).abs() < f32::EPSILON);
    }

    #[test]
    fn store_checkpoint_older_does_not_replace_newer() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();

        let new = Checkpoint {
            chunk_id,
            job_id,
            progress_pct: 50.0,
            state_blob: BlobRef {
                hash: [2u8; 32],
                size_bytes: 200,
                filename: None,
            },
            created_at: Utc::now(),
            node_id: NodeId::new(),
        };

        let old = Checkpoint {
            chunk_id,
            job_id,
            progress_pct: 10.0,
            state_blob: BlobRef {
                hash: [1u8; 32],
                size_bytes: 100,
                filename: None,
            },
            created_at: Utc::now() - chrono::Duration::seconds(60),
            node_id: NodeId::new(),
        };

        mgr.store_checkpoint(new);
        mgr.store_checkpoint(old); // Should be ignored.

        let stored = mgr.get_checkpoint(&chunk_id).unwrap();
        assert!((stored.progress_pct - 50.0).abs() < f32::EPSILON);
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: restore_to_dir
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn restore_to_dir_no_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let target = tmp.path().join("target");

        let restored = mgr.restore_to_dir(&chunk_id, &target).await.unwrap();
        assert!(!restored);
    }

    #[tokio::test]
    async fn restore_to_dir_with_files() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_id = JobId::new();
        let node_id = NodeId::new();

        // Create checkpoint files.
        let dir = mgr.prepare_chunk_dir(&chunk_id, &job_id).unwrap();
        std::fs::write(dir.join("state.bin"), b"saved state").unwrap();
        std::fs::write(dir.join("model.pt"), b"model data").unwrap();

        // Snapshot to register the checkpoint.
        mgr.snapshot_chunk(&chunk_id, &job_id, node_id).await;

        // Restore to a new directory.
        let target = tmp.path().join("restored");
        let restored = mgr.restore_to_dir(&chunk_id, &target).await.unwrap();
        assert!(restored);

        // Verify files were copied.
        assert!(target.join("state.bin").exists());
        assert!(target.join("model.pt").exists());
        assert_eq!(
            std::fs::read(target.join("state.bin")).unwrap(),
            b"saved state"
        );
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: env_vars
    // -----------------------------------------------------------------------

    #[test]
    fn env_vars_without_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_dir = tmp.path().join("job");

        let vars = mgr.env_vars(&chunk_id, &job_dir);
        assert_eq!(vars.len(), 1);
        assert_eq!(vars[0].0, CHECKPOINT_DIR_ENV);
        assert!(vars[0].1.contains("checkpoint"));
    }

    #[test]
    fn env_vars_with_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();
        let job_dir = tmp.path().join("job");

        // Store a checkpoint so MARABUNTA_RESUME is set.
        let checkpoint = Checkpoint {
            chunk_id,
            job_id: JobId::new(),
            progress_pct: 50.0,
            state_blob: BlobRef {
                hash: [0u8; 32],
                size_bytes: 0,
                filename: None,
            },
            created_at: Utc::now(),
            node_id: NodeId::new(),
        };
        mgr.store_checkpoint(checkpoint);

        let vars = mgr.env_vars(&chunk_id, &job_dir);
        assert_eq!(vars.len(), 2);

        let var_map: HashMap<_, _> = vars.into_iter().collect();
        assert!(var_map.contains_key(CHECKPOINT_DIR_ENV));
        assert_eq!(var_map.get(CHECKPOINT_RESUME_ENV).unwrap(), "1");
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: remove_checkpoint
    // -----------------------------------------------------------------------

    #[test]
    fn remove_checkpoint_clears_entry() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let chunk_id = ChunkId::new();

        let checkpoint = Checkpoint {
            chunk_id,
            job_id: JobId::new(),
            progress_pct: 0.0,
            state_blob: BlobRef {
                hash: [0u8; 32],
                size_bytes: 0,
                filename: None,
            },
            created_at: Utc::now(),
            node_id: NodeId::new(),
        };
        mgr.store_checkpoint(checkpoint);
        assert!(mgr.get_checkpoint(&chunk_id).is_some());

        mgr.remove_checkpoint(&chunk_id);
        assert!(mgr.get_checkpoint(&chunk_id).is_none());
    }

    #[test]
    fn remove_checkpoint_nonexistent_is_noop() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        // Should not panic.
        mgr.remove_checkpoint(&ChunkId::new());
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: list and count
    // -----------------------------------------------------------------------

    #[test]
    fn list_and_count() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());

        assert_eq!(mgr.count(), 0);
        assert!(mgr.list().is_empty());

        for _ in 0..3 {
            let checkpoint = Checkpoint {
                chunk_id: ChunkId::new(),
                job_id: JobId::new(),
                progress_pct: 0.0,
                state_blob: BlobRef {
                    hash: [0u8; 32],
                    size_bytes: 0,
                    filename: None,
                },
                created_at: Utc::now(),
                node_id: NodeId::new(),
            };
            mgr.store_checkpoint(checkpoint);
        }

        assert_eq!(mgr.count(), 3);
        assert_eq!(mgr.list().len(), 3);
    }

    // -----------------------------------------------------------------------
    // CheckpointManager: remove_job_checkpoints
    // -----------------------------------------------------------------------

    #[test]
    fn remove_job_checkpoints() {
        let tmp = TempDir::new().unwrap();
        let mgr = make_manager(tmp.path());
        let job_id = JobId::new();
        let other_job = JobId::new();

        for _ in 0..2 {
            mgr.store_checkpoint(Checkpoint {
                chunk_id: ChunkId::new(),
                job_id,
                progress_pct: 0.0,
                state_blob: BlobRef {
                    hash: [0u8; 32],
                    size_bytes: 0,
                    filename: None,
                },
                created_at: Utc::now(),
                node_id: NodeId::new(),
            });
        }
        mgr.store_checkpoint(Checkpoint {
            chunk_id: ChunkId::new(),
            job_id: other_job,
            progress_pct: 0.0,
            state_blob: BlobRef {
                hash: [0u8; 32],
                size_bytes: 0,
                filename: None,
            },
            created_at: Utc::now(),
            node_id: NodeId::new(),
        });

        assert_eq!(mgr.count(), 3);
        mgr.remove_job_checkpoints(&job_id);
        assert_eq!(mgr.count(), 1);
    }

    // -----------------------------------------------------------------------
    // hash_directory
    // -----------------------------------------------------------------------

    #[test]
    fn hash_directory_empty() {
        let tmp = TempDir::new().unwrap();
        assert!(hash_directory(tmp.path()).is_none());
    }

    #[test]
    fn hash_directory_with_files() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("a.txt"), b"hello").unwrap();
        std::fs::write(tmp.path().join("b.txt"), b"world").unwrap();

        let result = hash_directory(tmp.path());
        assert!(result.is_some());

        let (hash, total_size, file_count) = result.unwrap();
        assert_eq!(file_count, 2);
        assert_eq!(total_size, 10); // "hello" + "world"
        assert_ne!(hash, [0u8; 32]);
    }

    #[test]
    fn hash_directory_deterministic() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("x.dat"), b"data1").unwrap();
        std::fs::write(tmp.path().join("y.dat"), b"data2").unwrap();

        let result1 = hash_directory(tmp.path()).unwrap();
        let result2 = hash_directory(tmp.path()).unwrap();
        assert_eq!(result1.0, result2.0);
    }

    #[test]
    fn hash_directory_nonexistent() {
        let result = hash_directory(Path::new("/nonexistent/path/abc123"));
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // read_progress_file
    // -----------------------------------------------------------------------

    #[test]
    fn read_progress_txt() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("progress.txt"), "65.5").unwrap();

        let pct = read_progress_file(tmp.path());
        assert!(pct.is_some());
        assert!((pct.unwrap() - 65.5).abs() < f32::EPSILON);
    }

    #[test]
    fn read_progress_json() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(
            tmp.path().join("progress.json"),
            r#"{"progress": 88.0}"#,
        )
        .unwrap();

        let pct = read_progress_file(tmp.path());
        assert!(pct.is_some());
        assert!((pct.unwrap() - 88.0).abs() < f32::EPSILON);
    }

    #[test]
    fn read_progress_clamped() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("progress.txt"), "150.0").unwrap();

        let pct = read_progress_file(tmp.path()).unwrap();
        assert!((pct - 100.0).abs() < f32::EPSILON);
    }

    #[test]
    fn read_progress_no_file() {
        let tmp = TempDir::new().unwrap();
        assert!(read_progress_file(tmp.path()).is_none());
    }

    #[test]
    fn read_progress_invalid_contents() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("progress.txt"), "not a number").unwrap();
        assert!(read_progress_file(tmp.path()).is_none());
    }

    // -----------------------------------------------------------------------
    // copy_directory_contents
    // -----------------------------------------------------------------------

    #[test]
    fn copy_directory_contents_basic() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();
        let dst_subdir = dst.path().join("output");

        std::fs::write(src.path().join("file1.txt"), b"content1").unwrap();
        std::fs::write(src.path().join("file2.txt"), b"content2").unwrap();

        let count = copy_directory_contents(src.path(), &dst_subdir).unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            std::fs::read(dst_subdir.join("file1.txt")).unwrap(),
            b"content1"
        );
        assert_eq!(
            std::fs::read(dst_subdir.join("file2.txt")).unwrap(),
            b"content2"
        );
    }

    #[test]
    fn copy_directory_contents_creates_dst() {
        let src = TempDir::new().unwrap();
        let dst = TempDir::new().unwrap();
        let nested = dst.path().join("a").join("b").join("c");

        std::fs::write(src.path().join("f.txt"), b"data").unwrap();

        let count = copy_directory_contents(src.path(), &nested).unwrap();
        assert_eq!(count, 1);
        assert!(nested.join("f.txt").exists());
    }

    // -----------------------------------------------------------------------
    // Checkpoint serialization
    // -----------------------------------------------------------------------

    #[test]
    fn checkpoint_serialize_deserialize() {
        let checkpoint = Checkpoint {
            chunk_id: ChunkId::new(),
            job_id: JobId::new(),
            progress_pct: 42.5,
            state_blob: BlobRef {
                hash: [7u8; 32],
                size_bytes: 4096,
                filename: Some("test-checkpoint".to_string()),
            },
            created_at: Utc::now(),
            node_id: NodeId::new(),
        };

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: Checkpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.chunk_id, checkpoint.chunk_id);
        assert_eq!(deserialized.job_id, checkpoint.job_id);
        assert!((deserialized.progress_pct - 42.5).abs() < f32::EPSILON);
        assert_eq!(deserialized.state_blob.size_bytes, 4096);
    }
}
