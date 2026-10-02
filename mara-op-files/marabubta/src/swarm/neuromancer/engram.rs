// Marabunta - Licensed under the MIT License.
//! Engram — Content-addressable fossil (result) cache.
//!
//! Provides a disk-backed, content-addressable cache for computation results
//! (called "fossils"). Each fossil is keyed by a blake3 hash of its task type
//! and input data.  The result blob is stored on disk, while metadata lives in
//! a WAL-mode SQLite database.
//!
//! # Disk layout
//!
//! ```text
//! {data_dir}/engram/
//! +-- index.db
//! +-- blobs/
//!     +-- a1b2c3d4/                 (first 8 hex chars as shard dir)
//!     |   +-- a1b2c3d4e5f6...      (full hex hash, raw bytes)
//! ```
//!
//! # Eviction
//!
//! LRU-based: when storage usage exceeds `eviction_trigger_percent`, the
//! oldest-accessed non-checkpoint fossils are deleted until usage drops to
//! `eviction_target_percent`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::EngramConfig;
use super::types::{
    Blake3Hash, MarabuntaEvent, EvictionReason, NodeId, ProvenanceInfo, TaskId, TrustStatus,
};

// ============================================================================
// Public data types
// ============================================================================

/// Full fossil record stored locally (metadata + access stats).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FossilRecord {
    pub content_hash: Blake3Hash,
    pub result_hash: Blake3Hash,
    pub result_size: u64,
    pub provenance: ProvenanceInfo,
    pub created_at: SystemTime,
    pub last_accessed: SystemTime,
    pub access_count: u64,
    pub is_dream: bool,
    pub is_checkpoint: bool,
    pub trust_status: TrustStatus,
}

/// Lightweight fossil metadata propagated via gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FossilMetadata {
    pub content_hash: Blake3Hash,
    pub result_hash: Blake3Hash,
    pub result_size: u64,
    pub computation_type: String,
    pub created_at: SystemTime,
    pub storage_node: NodeId,
    pub trust_status: TrustStatus,
}

// ============================================================================
// Wire types
// ============================================================================

/// Request sent to a remote Engram node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngramRequest {
    /// Pull a fossil blob by its content hash.
    Pull { content_hash: Blake3Hash },
    /// Check whether a fossil exists.
    Has { content_hash: Blake3Hash },
}

/// Response from a remote Engram node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngramResponse {
    /// The requested blob data.
    Blob {
        content_hash: Blake3Hash,
        data: Vec<u8>,
    },
    /// The fossil exists.
    HasResult {
        content_hash: Blake3Hash,
        result_size: u64,
    },
    /// The fossil was not found.
    NotFound { content_hash: Blake3Hash },
    /// The fossil exists but is untrusted.
    Untrusted {
        content_hash: Blake3Hash,
        reason: String,
    },
}

// ============================================================================
// Error type
// ============================================================================

/// Errors produced by the Engram subsystem.
#[derive(Debug)]
pub enum EngramError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    Serialization(String),
    IntegrityFailure {
        content_hash: Blake3Hash,
        expected: Blake3Hash,
        actual: Blake3Hash,
    },
    StorageFull,
    NotFound {
        content_hash: Blake3Hash,
    },
    Untrusted {
        content_hash: Blake3Hash,
        reason: String,
    },
}

impl std::fmt::Display for EngramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "engram I/O error: {}", e),
            Self::Sqlite(e) => write!(f, "engram SQLite error: {}", e),
            Self::Serialization(msg) => write!(f, "engram serialization error: {}", msg),
            Self::IntegrityFailure {
                content_hash,
                expected,
                actual,
            } => write!(
                f,
                "engram integrity failure for {}: expected {}, got {}",
                hex::encode(content_hash),
                hex::encode(expected),
                hex::encode(actual),
            ),
            Self::StorageFull => write!(f, "engram storage full"),
            Self::NotFound { content_hash } => {
                write!(f, "fossil not found: {}", hex::encode(content_hash))
            }
            Self::Untrusted {
                content_hash,
                reason,
            } => write!(
                f,
                "fossil untrusted {}: {}",
                hex::encode(content_hash),
                reason
            ),
        }
    }
}

impl std::error::Error for EngramError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for EngramError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<rusqlite::Error> for EngramError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}

// ============================================================================
// Engram — the fossil cache
// ============================================================================

/// Content-addressable fossil cache with disk-backed blob storage and
/// SQLite metadata.
pub struct Engram {
    config: EngramConfig,
    bus: Arc<NeuromancerBus>,
    local_node: NodeId,
    metadata_db: Mutex<Connection>,
    distributed_index: Mutex<HashMap<Blake3Hash, HashSet<NodeId>>>,
    blob_dir: PathBuf,
    current_storage_bytes: AtomicU64,
}

impl Engram {
    /// Create a new Engram instance.
    ///
    /// Creates the directory structure, opens the SQLite database with WAL
    /// journaling, runs the schema migration, and computes the initial
    /// storage size from the database.
    pub fn new(
        config: EngramConfig,
        bus: Arc<NeuromancerBus>,
        local_node: NodeId,
    ) -> Result<Self, EngramError> {
        let engram_dir = config.data_dir.join("engram");
        let blob_dir = engram_dir.join("blobs");
        let db_path = engram_dir.join("index.db");

        // Create directories.
        std::fs::create_dir_all(&blob_dir)?;

        // Open SQLite with WAL mode.
        let conn = Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.pragma_update(None, "synchronous", "normal")?;

        // Run schema.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS fossils (
                content_hash BLOB PRIMARY KEY,
                result_hash BLOB NOT NULL,
                result_size INTEGER NOT NULL,
                task_id BLOB NOT NULL,
                origin_node BLOB NOT NULL,
                input_hash BLOB NOT NULL,
                computation_type TEXT NOT NULL,
                computation_duration_ms INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                last_accessed INTEGER NOT NULL,
                access_count INTEGER NOT NULL DEFAULT 0,
                is_dream INTEGER NOT NULL DEFAULT 0,
                is_checkpoint INTEGER NOT NULL DEFAULT 0,
                trust_status TEXT NOT NULL DEFAULT 'trusted'
            );
            CREATE INDEX IF NOT EXISTS idx_fossils_origin_node ON fossils(origin_node);
            CREATE INDEX IF NOT EXISTS idx_fossils_last_accessed ON fossils(last_accessed);
            CREATE INDEX IF NOT EXISTS idx_fossils_trust_status ON fossils(trust_status);
            CREATE INDEX IF NOT EXISTS idx_fossils_computation_type ON fossils(computation_type);",
        )?;

        // Compute initial storage size.
        let initial_bytes: u64 = conn
            .query_row(
                "SELECT COALESCE(SUM(result_size), 0) FROM fossils WHERE trust_status = 'trusted'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        info!(
            initial_bytes,
            blob_dir = %blob_dir.display(),
            "Engram initialized"
        );

        Ok(Self {
            config,
            bus,
            local_node,
            metadata_db: Mutex::new(conn),
            distributed_index: Mutex::new(HashMap::new()),
            blob_dir,
            current_storage_bytes: AtomicU64::new(initial_bytes),
        })
    }

    // ========================================================================
    // Core methods
    // ========================================================================

    /// Store a computation result as a fossil.
    ///
    /// Computes the content hash from (`task_type` + `input`), the result
    /// hash from `result`, and writes the blob atomically via a `.tmp` +
    /// rename pattern.  If a fossil with the same content hash already exists
    /// (idempotent), returns the existing hash without re-writing.
    pub fn store(
        &self,
        task_type: &str,
        input: &[u8],
        result: &[u8],
        provenance: ProvenanceInfo,
        is_dream: bool,
        is_checkpoint: bool,
    ) -> Result<Blake3Hash, EngramError> {
        let content_hash = Self::blake3_content_hash(task_type, input);
        let result_hash = *blake3::hash(result).as_bytes();
        let result_size = result.len() as u64;
        let now = systemtime_to_epoch_ms(SystemTime::now());

        // Check for duplicate.
        {
            let db = self.metadata_db.lock();
            let exists: bool = db
                .query_row(
                    "SELECT COUNT(*) > 0 FROM fossils WHERE content_hash = ?1",
                    params![&content_hash[..]],
                    |row| row.get(0),
                )
                .unwrap_or(false);

            if exists {
                debug!(
                    content_hash = hex::encode(content_hash),
                    "Fossil already exists, skipping store"
                );
                return Ok(content_hash);
            }
        }

        // Check storage capacity and maybe evict.
        self.maybe_evict()?;

        // Check if we still have room after eviction.
        let max_bytes = self.config.max_storage_mb * 1024 * 1024;
        let current = self.current_storage_bytes.load(Ordering::Relaxed);
        if current + result_size > max_bytes {
            return Err(EngramError::StorageFull);
        }

        // Atomic blob write.
        self.write_blob(&content_hash, result)?;

        // SQLite INSERT.
        {
            let db = self.metadata_db.lock();
            db.execute(
                "INSERT OR IGNORE INTO fossils (
                    content_hash, result_hash, result_size,
                    task_id, origin_node, input_hash,
                    computation_type, computation_duration_ms,
                    created_at, last_accessed, access_count,
                    is_dream, is_checkpoint, trust_status
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11, ?12, ?13)",
                params![
                    &content_hash[..],
                    &result_hash[..],
                    result_size as i64,
                    &provenance.task_id[..],
                    provenance.node.0.as_bytes().as_slice(),
                    &provenance.input_hash[..],
                    provenance.computation_type,
                    provenance.computation_duration_ms as i64,
                    now as i64,
                    now as i64,
                    is_dream as i32,
                    is_checkpoint as i32,
                    TrustStatus::Trusted.to_string(),
                ],
            )?;
        }

        // Update storage counter.
        self.current_storage_bytes
            .fetch_add(result_size, Ordering::Relaxed);

        // Emit event.
        self.bus.emit(MarabuntaEvent::FossilStored {
            hash: content_hash,
            size: result_size,
            provenance,
            timestamp: SystemTime::now(),
        });

        info!(
            content_hash = hex::encode(content_hash),
            result_size, "Fossil stored"
        );

        Ok(content_hash)
    }

    /// Look up a fossil by task type and input.
    ///
    /// Computes the content hash, checks the SQLite metadata, verifies
    /// trust status, reads the blob, verifies blake3 integrity, updates
    /// access statistics, and emits a `FossilHit` event.  Returns `None`
    /// if the fossil is missing or untrusted.
    pub fn lookup(
        &self,
        task_type: &str,
        input: &[u8],
    ) -> Result<Option<Vec<u8>>, EngramError> {
        let content_hash = Self::blake3_content_hash(task_type, input);

        // Check SQLite for this fossil.
        let (result_hash, trust_status_str, task_id_bytes): (Vec<u8>, String, Vec<u8>) = {
            let db = self.metadata_db.lock();
            match db
                .query_row(
                    "SELECT result_hash, trust_status, task_id FROM fossils WHERE content_hash = ?1",
                    params![&content_hash[..]],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                )
                .optional()?
            {
                Some(row) => row,
                None => return Ok(None),
            }
        };

        // Check trust status.
        let trust_status: TrustStatus = trust_status_str
            .parse()
            .map_err(|e: String| EngramError::Serialization(e))?;

        if !trust_status.is_trusted() {
            debug!(
                content_hash = hex::encode(content_hash),
                "Fossil lookup skipped: untrusted"
            );
            return Ok(None);
        }

        // Read blob.
        let data = match self.read_blob(&content_hash) {
            Ok(d) => d,
            Err(EngramError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                warn!(
                    content_hash = hex::encode(content_hash),
                    "Blob missing on disk but present in SQLite"
                );
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        // Verify blake3 integrity.
        let actual_hash = *blake3::hash(&data).as_bytes();
        let mut expected = [0u8; 32];
        if result_hash.len() == 32 {
            expected.copy_from_slice(&result_hash);
        }

        if actual_hash != expected {
            return Err(EngramError::IntegrityFailure {
                content_hash,
                expected,
                actual: actual_hash,
            });
        }

        // Update access stats.
        let now = systemtime_to_epoch_ms(SystemTime::now());
        {
            let db = self.metadata_db.lock();
            db.execute(
                "UPDATE fossils SET last_accessed = ?1, access_count = access_count + 1 WHERE content_hash = ?2",
                params![now as i64, &content_hash[..]],
            )?;
        }

        // Reconstruct task_id for event.
        let mut task_id: TaskId = [0u8; 32];
        if task_id_bytes.len() == 32 {
            task_id.copy_from_slice(&task_id_bytes);
        }

        // Emit hit event.
        self.bus.emit(MarabuntaEvent::FossilHit {
            task_id,
            fossil_hash: content_hash,
            timestamp: SystemTime::now(),
        });

        Ok(Some(data))
    }

    /// Mark all fossils from a given node as untrusted.
    ///
    /// Used when a node is killed by the security subsystem.  Emits a
    /// `FossilEvicted` event for each affected fossil.
    pub fn purge_by_node(&self, killed_node: NodeId) -> Result<(), EngramError> {
        let node_bytes = killed_node.0.as_bytes().as_slice();

        // Gather affected content hashes before updating.
        let affected: Vec<Vec<u8>> = {
            let db = self.metadata_db.lock();
            let mut stmt = db.prepare(
                "SELECT content_hash FROM fossils WHERE origin_node = ?1 AND trust_status = 'trusted'",
            )?;
            let rows = stmt
                .query_map(params![node_bytes], |row| row.get::<_, Vec<u8>>(0))?
                .filter_map(|r| r.ok())
                .collect();
            rows
        };

        // Mark all as untrusted.
        let reason = format!("node_killed:{}", killed_node.0);
        {
            let db = self.metadata_db.lock();
            db.execute(
                "UPDATE fossils SET trust_status = ?1 WHERE origin_node = ?2 AND trust_status = 'trusted'",
                params![
                    TrustStatus::Untrusted {
                        reason: reason.clone()
                    }
                    .to_string(),
                    node_bytes,
                ],
            )?;
        }

        // Emit events.
        for hash_bytes in &affected {
            let mut hash: Blake3Hash = [0u8; 32];
            if hash_bytes.len() == 32 {
                hash.copy_from_slice(hash_bytes);
            }
            self.bus.emit(MarabuntaEvent::FossilEvicted {
                hash,
                reason: EvictionReason::Untrusted {
                    killed_node,
                },
                timestamp: SystemTime::now(),
            });
        }

        info!(
            killed_node = %killed_node.0,
            affected_count = affected.len(),
            "Purged fossils from killed node"
        );

        Ok(())
    }

    /// Evict oldest-accessed fossils when storage exceeds the trigger threshold.
    ///
    /// Skips checkpoint fossils.  Deletes both the blob on disk and the
    /// SQLite row, then decrements the storage counter.  Stops when
    /// usage drops to or below `eviction_target_percent`.
    pub fn maybe_evict(&self) -> Result<(), EngramError> {
        let max_bytes = self.config.max_storage_mb * 1024 * 1024;
        let trigger_bytes = max_bytes * self.config.eviction_trigger_percent / 100;
        let target_bytes = max_bytes * self.config.eviction_target_percent / 100;
        let current = self.current_storage_bytes.load(Ordering::Relaxed);

        if current < trigger_bytes {
            return Ok(());
        }

        info!(
            current_bytes = current,
            trigger_bytes,
            target_bytes,
            "Eviction triggered"
        );

        // Query candidates: oldest accessed first, skip checkpoints.
        let candidates: Vec<(Vec<u8>, u64)> = {
            let db = self.metadata_db.lock();
            let mut stmt = db.prepare(
                "SELECT content_hash, result_size FROM fossils
                 WHERE is_checkpoint = 0 AND trust_status = 'trusted'
                 ORDER BY last_accessed ASC",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u64>(1)?))
                })?
                .filter_map(|r| r.ok())
                .collect();
            rows
        };

        let mut freed: u64 = 0;
        for (hash_bytes, size) in candidates {
            let remaining = current.saturating_sub(freed);
            if remaining <= target_bytes {
                break;
            }

            let mut hash: Blake3Hash = [0u8; 32];
            if hash_bytes.len() == 32 {
                hash.copy_from_slice(&hash_bytes);
            }

            // Delete blob.
            let blob_path = self.blob_path(&hash);
            if blob_path.exists() {
                let _ = std::fs::remove_file(&blob_path);
            }

            // Delete SQLite row.
            {
                let db = self.metadata_db.lock();
                db.execute(
                    "DELETE FROM fossils WHERE content_hash = ?1",
                    params![&hash_bytes[..]],
                )?;
            }

            freed += size;

            // Emit event.
            self.bus.emit(MarabuntaEvent::FossilEvicted {
                hash,
                reason: EvictionReason::StoragePressure,
                timestamp: SystemTime::now(),
            });

            debug!(
                content_hash = hex::encode(hash),
                freed_bytes = size,
                "Evicted fossil"
            );
        }

        // Update storage counter.
        self.current_storage_bytes
            .fetch_sub(freed, Ordering::Relaxed);

        info!(total_freed = freed, "Eviction complete");

        Ok(())
    }

    /// Record that a remote node has a copy of a fossil.
    pub fn handle_fossil_announcement(&self, meta: FossilMetadata) {
        let mut index = self.distributed_index.lock();
        index
            .entry(meta.content_hash)
            .or_default()
            .insert(meta.storage_node);
        debug!(
            content_hash = hex::encode(meta.content_hash),
            storage_node = %meta.storage_node.0,
            "Recorded fossil announcement"
        );
    }

    /// Remove a node from all distributed index entries.
    pub fn handle_node_left(&self, node: NodeId) {
        let mut index = self.distributed_index.lock();
        for nodes in index.values_mut() {
            nodes.remove(&node);
        }
        // Remove entries with no remaining nodes.
        index.retain(|_, nodes| !nodes.is_empty());
        debug!(node = %node.0, "Removed node from distributed index");
    }

    /// Query all fossil content hashes originating from a given node.
    pub fn fossils_by_node(&self, node: NodeId) -> Vec<Blake3Hash> {
        let node_bytes = node.0.as_bytes().as_slice();
        let db = self.metadata_db.lock();
        let mut stmt = match db.prepare(
            "SELECT content_hash FROM fossils WHERE origin_node = ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        stmt.query_map(params![node_bytes], |row| row.get::<_, Vec<u8>>(0))
            .ok()
            .map(|rows| {
                rows.filter_map(|r| r.ok())
                    .filter_map(|bytes| {
                        if bytes.len() == 32 {
                            let mut hash = [0u8; 32];
                            hash.copy_from_slice(&bytes);
                            Some(hash)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Return the current storage usage as a percentage of `max_storage_mb`.
    pub fn storage_usage_percent(&self) -> f64 {
        let max_bytes = self.config.max_storage_mb * 1024 * 1024;
        if max_bytes == 0 {
            return 0.0;
        }
        let current = self.current_storage_bytes.load(Ordering::Relaxed);
        (current as f64 / max_bytes as f64) * 100.0
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    /// Compute the blob file path for a given content hash.
    ///
    /// Layout: `blob_dir / hex[..8] / hex`
    fn blob_path(&self, content_hash: &Blake3Hash) -> PathBuf {
        let hex_str = hex::encode(content_hash);
        let shard = &hex_str[..8];
        self.blob_dir.join(shard).join(&hex_str)
    }

    /// Compute the content hash from task type and input data.
    ///
    /// `content_hash = blake3(task_type_bytes || input_bytes)`
    pub fn blake3_content_hash(task_type: &str, input: &[u8]) -> Blake3Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(task_type.as_bytes());
        hasher.update(input);
        *hasher.finalize().as_bytes()
    }

    /// Atomically write a blob to disk.
    ///
    /// Writes to a `.tmp` sibling file, then renames to the final path.
    /// This ensures that a concurrent reader never sees a partial blob.
    fn write_blob(&self, content_hash: &Blake3Hash, data: &[u8]) -> Result<(), EngramError> {
        let final_path = self.blob_path(content_hash);
        if let Some(parent) = final_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp_path = final_path.with_extension("tmp");
        std::fs::write(&tmp_path, data)?;
        std::fs::rename(&tmp_path, &final_path)?;

        Ok(())
    }

    /// Read a blob from disk.
    fn read_blob(&self, content_hash: &Blake3Hash) -> Result<Vec<u8>, EngramError> {
        let path = self.blob_path(content_hash);
        Ok(std::fs::read(path)?)
    }

    /// Return the raw storage byte count (for testing).
    pub fn current_storage_bytes(&self) -> u64 {
        self.current_storage_bytes.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Time helpers
// ============================================================================

/// Convert a `SystemTime` to milliseconds since the Unix epoch.
fn systemtime_to_epoch_ms(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Convert milliseconds since the Unix epoch to a `SystemTime`.
#[allow(dead_code)]
fn epoch_ms_to_systemtime(ms: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(ms)
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Create a test Engram with a temporary directory and small storage limit.
    fn test_engram(max_mb: u64) -> (Engram, TempDir) {
        let tmp = TempDir::new().unwrap();
        let config = EngramConfig {
            max_storage_mb: max_mb,
            eviction_trigger_percent: 90,
            eviction_target_percent: 75,
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let engram = Engram::new(config, bus, node).unwrap();
        (engram, tmp)
    }

    fn make_provenance(node: NodeId) -> ProvenanceInfo {
        ProvenanceInfo {
            task_id: [1u8; 32],
            node,
            input_hash: [2u8; 32],
            computation_type: "test".into(),
            computation_duration_ms: 100,
        }
    }

    // -- Test 1: store + lookup roundtrip --

    #[test]
    fn test_store_and_lookup_roundtrip() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();
        let prov = make_provenance(node);
        let input = b"hello world";
        let result = b"computed result";

        let hash = engram
            .store("test_task", input, result, prov, false, false)
            .unwrap();

        let data = engram.lookup("test_task", input).unwrap();
        assert!(data.is_some());
        assert_eq!(data.unwrap(), result);

        // Content hash should be deterministic.
        let hash2 = Engram::blake3_content_hash("test_task", input);
        assert_eq!(hash, hash2);
    }

    // -- Test 2: lookup miss --

    #[test]
    fn test_lookup_miss() {
        let (engram, _tmp) = test_engram(100);
        let data = engram.lookup("nonexistent", b"no such input").unwrap();
        assert!(data.is_none());
    }

    // -- Test 3: store idempotent --

    #[test]
    fn test_store_idempotent() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();
        let prov = make_provenance(node);
        let input = b"same input";
        let result = b"same result";

        let h1 = engram
            .store("dup_task", input, result, prov.clone(), false, false)
            .unwrap();
        let h2 = engram
            .store("dup_task", input, result, prov, false, false)
            .unwrap();

        assert_eq!(h1, h2);

        // Storage counter should only count once.
        assert_eq!(engram.current_storage_bytes(), result.len() as u64);
    }

    // -- Test 4: hash integrity (tamper blob) --

    #[test]
    fn test_hash_integrity_tampered_blob() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();
        let prov = make_provenance(node);
        let input = b"integrity test";
        let result = b"original data";

        let hash = engram
            .store("integrity_task", input, result, prov, false, false)
            .unwrap();

        // Tamper with the blob file.
        let blob_path = engram.blob_path(&hash);
        std::fs::write(&blob_path, b"tampered data").unwrap();

        // Lookup should fail with integrity error.
        let err = engram.lookup("integrity_task", input);
        match err {
            Err(EngramError::IntegrityFailure { .. }) => { /* expected */ }
            other => panic!("Expected IntegrityFailure, got {:?}", other),
        }
    }

    // -- Test 5: LRU eviction (fill to trigger, verify oldest evicted) --

    #[test]
    fn test_lru_eviction_oldest_evicted() {
        // 1 MB max, 90% trigger = 921600 bytes, 75% target = 786432 bytes.
        let tmp = TempDir::new().unwrap();
        let config = EngramConfig {
            max_storage_mb: 1,
            eviction_trigger_percent: 90,
            eviction_target_percent: 75,
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let engram = Engram::new(config, bus, node).unwrap();

        // Store chunks of 200 KB each. 5 chunks = 1 MB, which is 100% usage.
        let chunk = vec![0xAAu8; 200 * 1024]; // 200 KB
        let mut hashes = Vec::new();
        for i in 0..5u8 {
            let prov = ProvenanceInfo {
                task_id: [i; 32],
                node,
                input_hash: [i; 32],
                computation_type: "evict_test".into(),
                computation_duration_ms: 10,
            };
            let input = [i; 1];

            // Touch access times with a small sleep to ensure ordering.
            std::thread::sleep(std::time::Duration::from_millis(10));

            let h = engram
                .store("evict_task", &input, &chunk, prov, false, false)
                .unwrap();
            hashes.push(h);

            // Touch the access timestamp so older ones have lower last_accessed.
            // (store already sets last_accessed = now, and we sleep between stores)
        }

        // After storing 5 * 200 KB = 1,000,000 bytes into a 1 MB (1,048,576) cache,
        // usage is ~95%, which is above the 90% trigger. The 6th store triggers eviction.
        let prov_extra = ProvenanceInfo {
            task_id: [99; 32],
            node,
            input_hash: [99; 32],
            computation_type: "evict_test".into(),
            computation_duration_ms: 10,
        };
        let _ = engram
            .store("evict_task", &[99u8; 1], &chunk, prov_extra, false, false)
            .unwrap();

        // The oldest fossils (first stored) should have been evicted.
        // Check that the first fossil is gone.
        let data = engram.lookup("evict_task", &[0u8; 1]).unwrap();
        assert!(data.is_none(), "Oldest fossil should have been evicted");

        // The most recent fossil should still be there.
        let data = engram.lookup("evict_task", &[99u8; 1]).unwrap();
        assert!(data.is_some(), "Newest fossil should still exist");
    }

    // -- Test 6: eviction stops at target --

    #[test]
    fn test_eviction_stops_at_target() {
        let tmp = TempDir::new().unwrap();
        let config = EngramConfig {
            max_storage_mb: 1,
            eviction_trigger_percent: 90,
            eviction_target_percent: 50,
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let engram = Engram::new(config, bus, node).unwrap();

        // Store 10 chunks of 100 KB each = 1 MB total (95%+ of 1 MB).
        let chunk = vec![0xBBu8; 100 * 1024];
        for i in 0..10u8 {
            let prov = ProvenanceInfo {
                task_id: [i; 32],
                node,
                input_hash: [i; 32],
                computation_type: "target_test".into(),
                computation_duration_ms: 10,
            };
            std::thread::sleep(std::time::Duration::from_millis(5));
            let _ = engram.store("target_task", &[i], &chunk, prov, false, false);
        }

        // Force eviction.
        engram.maybe_evict().unwrap();

        // Usage should be at or below 50%.
        let usage = engram.storage_usage_percent();
        assert!(
            usage <= 55.0,
            "Usage should be near or below target 50%, got {:.1}%",
            usage
        );
    }

    // -- Test 7: checkpoint never evicted --

    #[test]
    fn test_checkpoint_never_evicted() {
        let tmp = TempDir::new().unwrap();
        let config = EngramConfig {
            max_storage_mb: 1,
            eviction_trigger_percent: 50,
            eviction_target_percent: 25,
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let engram = Engram::new(config, bus, node).unwrap();

        // Store a checkpoint first (oldest).
        let checkpoint_data = vec![0xCCu8; 300 * 1024]; // 300 KB
        let prov_cp = ProvenanceInfo {
            task_id: [0; 32],
            node,
            input_hash: [0; 32],
            computation_type: "checkpoint_test".into(),
            computation_duration_ms: 10,
        };
        engram
            .store("cp_task", b"checkpoint_input", &checkpoint_data, prov_cp, false, true)
            .unwrap();

        // Store more non-checkpoint data to exceed trigger.
        let chunk = vec![0xDDu8; 200 * 1024];
        for i in 1..5u8 {
            let prov = ProvenanceInfo {
                task_id: [i; 32],
                node,
                input_hash: [i; 32],
                computation_type: "filler".into(),
                computation_duration_ms: 10,
            };
            std::thread::sleep(std::time::Duration::from_millis(5));
            let _ = engram.store("filler_task", &[i], &chunk, prov, false, false);
        }

        // Force eviction.
        engram.maybe_evict().unwrap();

        // Checkpoint should still be accessible.
        let data = engram.lookup("cp_task", b"checkpoint_input").unwrap();
        assert!(
            data.is_some(),
            "Checkpoint fossil should survive eviction"
        );
        assert_eq!(data.unwrap().len(), checkpoint_data.len());
    }

    // -- Test 8: purge_by_node marks untrusted --

    #[test]
    fn test_purge_by_node_marks_untrusted() {
        let (engram, _tmp) = test_engram(100);
        let bad_node = NodeId::new();
        let good_node = NodeId::new();

        // Store from bad node.
        let prov_bad = ProvenanceInfo {
            task_id: [10; 32],
            node: bad_node,
            input_hash: [10; 32],
            computation_type: "bad".into(),
            computation_duration_ms: 50,
        };
        engram
            .store("bad_task", b"bad_input", b"bad_result", prov_bad, false, false)
            .unwrap();

        // Store from good node.
        let prov_good = ProvenanceInfo {
            task_id: [20; 32],
            node: good_node,
            input_hash: [20; 32],
            computation_type: "good".into(),
            computation_duration_ms: 50,
        };
        engram
            .store("good_task", b"good_input", b"good_result", prov_good, false, false)
            .unwrap();

        // Purge the bad node.
        engram.purge_by_node(bad_node).unwrap();

        // Bad node's fossil should no longer be accessible.
        let data = engram.lookup("bad_task", b"bad_input").unwrap();
        assert!(data.is_none(), "Purged fossil should return None");

        // Good node's fossil should still work.
        let data = engram.lookup("good_task", b"good_input").unwrap();
        assert!(data.is_some(), "Unrelated fossil should still be accessible");
    }

    // -- Test 9: untrusted lookup returns None --

    #[test]
    fn test_untrusted_lookup_returns_none() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();
        let prov = make_provenance(node);

        engram
            .store("untrust_task", b"input", b"result", prov, false, false)
            .unwrap();

        // Manually mark as untrusted.
        {
            let content_hash = Engram::blake3_content_hash("untrust_task", b"input");
            let db = engram.metadata_db.lock();
            db.execute(
                "UPDATE fossils SET trust_status = ?1 WHERE content_hash = ?2",
                params![
                    TrustStatus::Untrusted {
                        reason: "test".into()
                    }
                    .to_string(),
                    &content_hash[..],
                ],
            )
            .unwrap();
        }

        let data = engram.lookup("untrust_task", b"input").unwrap();
        assert!(data.is_none(), "Untrusted fossil should return None");
    }

    // -- Test 10: distributed index add/remove --

    #[test]
    fn test_distributed_index_add_remove() {
        let (engram, _tmp) = test_engram(100);
        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let hash = [42u8; 32];

        let meta_a = FossilMetadata {
            content_hash: hash,
            result_hash: [0; 32],
            result_size: 100,
            computation_type: "test".into(),
            created_at: SystemTime::now(),
            storage_node: node_a,
            trust_status: TrustStatus::Trusted,
        };
        engram.handle_fossil_announcement(meta_a);

        let meta_b = FossilMetadata {
            content_hash: hash,
            result_hash: [0; 32],
            result_size: 100,
            computation_type: "test".into(),
            created_at: SystemTime::now(),
            storage_node: node_b,
            trust_status: TrustStatus::Trusted,
        };
        engram.handle_fossil_announcement(meta_b);

        // Both nodes should be in the index.
        {
            let index = engram.distributed_index.lock();
            let nodes = index.get(&hash).unwrap();
            assert!(nodes.contains(&node_a));
            assert!(nodes.contains(&node_b));
            assert_eq!(nodes.len(), 2);
        }

        // Remove node_a.
        engram.handle_node_left(node_a);

        {
            let index = engram.distributed_index.lock();
            let nodes = index.get(&hash).unwrap();
            assert!(!nodes.contains(&node_a));
            assert!(nodes.contains(&node_b));
            assert_eq!(nodes.len(), 1);
        }

        // Remove node_b — entry should be cleaned up.
        engram.handle_node_left(node_b);

        {
            let index = engram.distributed_index.lock();
            assert!(index.get(&hash).is_none());
        }
    }

    // -- Test 11: fossils_by_node --

    #[test]
    fn test_fossils_by_node() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();

        for i in 0..3u8 {
            let prov = ProvenanceInfo {
                task_id: [i; 32],
                node,
                input_hash: [i; 32],
                computation_type: "by_node".into(),
                computation_duration_ms: 10,
            };
            engram
                .store("by_node_task", &[i], &[i; 64], prov, false, false)
                .unwrap();
        }

        let fossils = engram.fossils_by_node(node);
        assert_eq!(fossils.len(), 3);

        // Another node should have zero.
        let other = NodeId::new();
        assert!(engram.fossils_by_node(other).is_empty());
    }

    // -- Test 12: disk persistence (drop + reopen) --

    #[test]
    fn test_disk_persistence_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let config = EngramConfig {
            max_storage_mb: 100,
            eviction_trigger_percent: 90,
            eviction_target_percent: 75,
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();

        // First instance: store a fossil.
        {
            let engram = Engram::new(config.clone(), bus.clone(), node).unwrap();
            let prov = make_provenance(node);
            engram
                .store("persist_task", b"persist_input", b"persist_result", prov, false, false)
                .unwrap();
        }
        // Drop the first Engram.

        // Second instance: reopen from the same directory.
        {
            let engram = Engram::new(config, bus, node).unwrap();

            // Storage counter should be restored.
            assert_eq!(
                engram.current_storage_bytes(),
                b"persist_result".len() as u64
            );

            // Lookup should work.
            let data = engram.lookup("persist_task", b"persist_input").unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap(), b"persist_result");
        }
    }

    // -- Test 13: atomic write (.tmp cleanup) --

    #[test]
    fn test_atomic_write_no_leftover_tmp() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();
        let prov = make_provenance(node);
        let input = b"atomic_input";
        let result = b"atomic_result";

        let hash = engram
            .store("atomic_task", input, result, prov, false, false)
            .unwrap();

        // The .tmp file should not exist.
        let tmp_path = engram.blob_path(&hash).with_extension("tmp");
        assert!(
            !tmp_path.exists(),
            "Temporary file should not persist after store"
        );

        // The final blob should exist.
        let final_path = engram.blob_path(&hash);
        assert!(final_path.exists(), "Final blob should exist");
    }

    // -- Test 14: blob missing but SQLite present --

    #[test]
    fn test_blob_missing_returns_none() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();
        let prov = make_provenance(node);
        let input = b"orphan_input";
        let result = b"orphan_result";

        let hash = engram
            .store("orphan_task", input, result, prov, false, false)
            .unwrap();

        // Delete the blob file manually.
        let blob_path = engram.blob_path(&hash);
        std::fs::remove_file(&blob_path).unwrap();

        // Lookup should return None (not error).
        let data = engram.lookup("orphan_task", input).unwrap();
        assert!(data.is_none(), "Missing blob should return None, not error");
    }

    // -- Test 15: storage counter accuracy --

    #[test]
    fn test_storage_counter_accuracy() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();

        assert_eq!(engram.current_storage_bytes(), 0);

        let data_a = vec![0xAAu8; 1000];
        let prov_a = ProvenanceInfo {
            task_id: [1; 32],
            node,
            input_hash: [1; 32],
            computation_type: "counter_a".into(),
            computation_duration_ms: 10,
        };
        engram
            .store("counter_task", b"a", &data_a, prov_a, false, false)
            .unwrap();
        assert_eq!(engram.current_storage_bytes(), 1000);

        let data_b = vec![0xBBu8; 2000];
        let prov_b = ProvenanceInfo {
            task_id: [2; 32],
            node,
            input_hash: [2; 32],
            computation_type: "counter_b".into(),
            computation_duration_ms: 10,
        };
        engram
            .store("counter_task", b"b", &data_b, prov_b, false, false)
            .unwrap();
        assert_eq!(engram.current_storage_bytes(), 3000);
    }

    // -- Test 16: storage_usage_percent --

    #[test]
    fn test_storage_usage_percent() {
        let tmp = TempDir::new().unwrap();
        let config = EngramConfig {
            max_storage_mb: 1, // 1 MB = 1,048,576 bytes
            eviction_trigger_percent: 90,
            eviction_target_percent: 75,
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let engram = Engram::new(config, bus, node).unwrap();

        assert_eq!(engram.storage_usage_percent(), 0.0);

        // Store exactly half.
        let half = vec![0u8; 524_288]; // 512 KB
        let prov = make_provenance(node);
        engram
            .store("pct_task", b"half", &half, prov, false, false)
            .unwrap();

        let pct = engram.storage_usage_percent();
        assert!(
            (pct - 50.0).abs() < 1.0,
            "Expected ~50%, got {:.1}%",
            pct
        );
    }

    // -- Test 17: dream flag preserved --

    #[test]
    fn test_dream_flag_preserved() {
        let (engram, _tmp) = test_engram(100);
        let node = NodeId::new();
        let prov = make_provenance(node);
        let hash = engram
            .store("dream_task", b"dream_input", b"dream_result", prov, true, false)
            .unwrap();

        // Verify the flag is set in SQLite.
        let db = engram.metadata_db.lock();
        let is_dream: bool = db
            .query_row(
                "SELECT is_dream = 1 FROM fossils WHERE content_hash = ?1",
                params![&hash[..]],
                |row| row.get(0),
            )
            .unwrap();
        assert!(is_dream, "Dream flag should be set");
    }

    // -- Test 18: content hash determinism --

    #[test]
    fn test_content_hash_determinism() {
        let h1 = Engram::blake3_content_hash("task_a", b"input_1");
        let h2 = Engram::blake3_content_hash("task_a", b"input_1");
        let h3 = Engram::blake3_content_hash("task_a", b"input_2");
        let h4 = Engram::blake3_content_hash("task_b", b"input_1");

        assert_eq!(h1, h2, "Same task+input should produce same hash");
        assert_ne!(h1, h3, "Different input should produce different hash");
        assert_ne!(h1, h4, "Different task type should produce different hash");
    }

    // -- Test 19: bus events emitted on store --

    #[test]
    fn test_bus_event_emitted_on_store() {
        let tmp = TempDir::new().unwrap();
        let config = EngramConfig {
            max_storage_mb: 100,
            eviction_trigger_percent: 90,
            eviction_target_percent: 75,
            data_dir: tmp.path().to_path_buf(),
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let mut rx = bus.subscribe();
        let node = NodeId::new();
        let engram = Engram::new(config, bus, node).unwrap();

        let prov = make_provenance(node);
        engram
            .store("event_task", b"event_input", b"event_result", prov, false, false)
            .unwrap();

        // The bus should have received a FossilStored event.
        match rx.try_recv() {
            Ok(MarabuntaEvent::FossilStored { hash, size, .. }) => {
                assert_eq!(size, b"event_result".len() as u64);
                let expected_hash =
                    Engram::blake3_content_hash("event_task", b"event_input");
                assert_eq!(hash, expected_hash);
            }
            other => panic!("Expected FossilStored event, got {:?}", other),
        }
    }

    // -- Test 20: blob_path shard structure --

    #[test]
    fn test_blob_path_shard_structure() {
        let (engram, _tmp) = test_engram(100);
        let hash = [0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89,
                     0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                     0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
                     0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let path = engram.blob_path(&hash);
        let hex_full = hex::encode(hash);
        let shard = &hex_full[..8];

        // Path should be blob_dir / shard / full_hex.
        assert!(path.to_str().unwrap().contains(shard));
        assert!(path.file_name().unwrap().to_str().unwrap() == hex_full);
    }
}
