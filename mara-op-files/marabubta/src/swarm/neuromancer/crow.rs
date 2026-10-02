// Marabunta - Licensed under the MIT License.
//! Crow — Forensic append-only event logger for the Neuromancer subsystem.
//!
//! Provides a tamper-evident, hash-chained log of every [`MarabuntaEvent`] that
//! flows through the swarm.  Events are recorded as JSONL to rotating daily
//! files and indexed in a local SQLite database for fast querying.
//!
//! # Design
//!
//! - **Append-only JSONL** files for durability and human readability.
//! - **Blake3 hash chain**: every entry hashes (seq, event, origin, timestamp,
//!   prev_hash) so tampering is detectable.
//! - **SQLite WAL-mode index** for fast range/filter queries without parsing
//!   every JSONL line.
//! - **In-memory ring buffer** for fast access to recent events.
//! - **Dedup set** prevents double-recording of gossip-forwarded events.
//! - All file and SQLite I/O is synchronous behind `parking_lot::Mutex` to
//!   avoid async file I/O complexity — operations are fast enough that holding
//!   the lock briefly is acceptable.

use std::collections::{HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::CrowConfig;
use super::types::*;

// ============================================================================
// Public types
// ============================================================================

/// A single entry in the Crow forensic log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrowEntry {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// The event that was recorded.
    pub event: MarabuntaEvent,
    /// Node that originally produced the event.
    pub origin_node: NodeId,
    /// Node that wrote this entry to its local log.
    pub recorded_by: NodeId,
    /// Wall-clock time when this entry was recorded.
    pub recorded_at: SystemTime,
    /// Number of gossip hops the event traversed before reaching us.
    pub gossip_hops: u8,
    /// Blake3 hash of the previous entry (forming a hash chain).
    pub prev_hash: Blake3Hash,
    /// Blake3 hash of this entry's canonical content.
    pub entry_hash: Blake3Hash,
}

/// Structured query against the Crow log.
#[derive(Debug, Clone)]
pub struct CrowQuery {
    pub task_id: Option<TaskId>,
    pub node_id: Option<NodeId>,
    pub event_types: Option<Vec<String>>,
    pub time_range: Option<(SystemTime, SystemTime)>,
    pub limit: Option<usize>,
    pub order: QueryOrder,
}

impl Default for CrowQuery {
    fn default() -> Self {
        Self {
            task_id: None,
            node_id: None,
            event_types: None,
            time_range: None,
            limit: None,
            order: QueryOrder::Chronological,
        }
    }
}

/// Ordering for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOrder {
    Chronological,
    ReverseChronological,
}

/// Errors produced by Crow operations.
#[derive(Debug)]
pub enum CrowError {
    Io(std::io::Error),
    Sqlite(rusqlite::Error),
    Serialization(String),
    LogCorrupted(String),
}

impl std::fmt::Display for CrowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "crow I/O error: {}", e),
            Self::Sqlite(e) => write!(f, "crow SQLite error: {}", e),
            Self::Serialization(msg) => write!(f, "crow serialization error: {}", msg),
            Self::LogCorrupted(msg) => write!(f, "crow log corrupted: {}", msg),
        }
    }
}

impl std::error::Error for CrowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CrowError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<rusqlite::Error> for CrowError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}

impl From<serde_json::Error> for CrowError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

// ============================================================================
// Crow — the forensic event logger
// ============================================================================

/// Forensic append-only event logger with hash-chained integrity.
pub struct Crow {
    config: CrowConfig,
    bus: Arc<NeuromancerBus>,
    local_node: NodeId,
    log_writer: Mutex<File>,
    index_db: Mutex<Connection>,
    seq_counter: AtomicU64,
    prev_hash: Mutex<Blake3Hash>,
    current_date: Mutex<String>,
    recent: Mutex<VecDeque<CrowEntry>>,
    seen_hashes: Mutex<HashSet<Blake3Hash>>,
    /// Optional channel for PG time-travel mirror.
    /// Forensic log entries are sent here for the PG audit_events table.
    pg_writer: Option<tokio::sync::mpsc::UnboundedSender<CrowEntry>>,
}

/// Canonical data structure hashed to produce `entry_hash`.
#[derive(Serialize)]
struct HashPayload {
    seq: u64,
    event: MarabuntaEvent,
    origin_node: NodeId,
    recorded_at_ms: u128,
    prev_hash: Blake3Hash,
}

// ============================================================================
// Implementation
// ============================================================================

impl Crow {
    /// Create a new Crow instance, initializing on-disk state.
    ///
    /// - Creates the data directory if missing.
    /// - Opens (or creates) `index.db` in WAL mode, runs schema migration.
    /// - Opens `current.jsonl` for append, recovering from partial writes.
    /// - Recovers seq counter and prev_hash from SQLite.
    /// - Pre-populates the in-memory ring buffer from the JSONL tail.
    pub fn new(
        config: CrowConfig,
        bus: Arc<NeuromancerBus>,
        local_node: NodeId,
    ) -> Result<Self, CrowError> {
        let crow_dir = config.data_dir.join("crow");
        fs::create_dir_all(&crow_dir)?;

        // -- SQLite index --
        let db_path = crow_dir.join("index.db");
        let conn = Connection::open(&db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        Self::run_schema_migration(&conn)?;

        // -- Recover state --
        let (max_seq, last_hash) = Self::recover_state(&conn)?;

        // -- Open current.jsonl --
        let today = Self::today_string();
        let jsonl_path = crow_dir.join("current.jsonl");
        Self::repair_jsonl_if_needed(&jsonl_path)?;
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&jsonl_path)?;

        // -- Pre-populate ring buffer from current.jsonl --
        let recent_entries = Self::load_recent_from_jsonl(
            &jsonl_path,
            config.ring_buffer_capacity,
        );

        // -- Build seen_hashes from recent entries --
        let mut seen = HashSet::new();
        for entry in &recent_entries {
            seen.insert(entry.entry_hash);
        }

        let recent_deque: VecDeque<CrowEntry> = recent_entries.into();

        Ok(Self {
            config,
            bus,
            local_node,
            log_writer: Mutex::new(log_file),
            index_db: Mutex::new(conn),
            seq_counter: AtomicU64::new(max_seq + 1),
            prev_hash: Mutex::new(last_hash),
            current_date: Mutex::new(today),
            recent: Mutex::new(recent_deque),
            seen_hashes: Mutex::new(seen),
            pg_writer: None,
        })
    }

    /// Attach a PG writer channel for time-travel mirror.
    ///
    /// Forensic log entries will be sent here in addition to the JSONL file
    /// and SQLite index. The consumer writes them to the PG audit_events table.
    pub fn set_pg_mirror(&mut self, tx: tokio::sync::mpsc::UnboundedSender<CrowEntry>) {
        self.pg_writer = Some(tx);
    }

    /// Main event loop: subscribe to the bus and record every event.
    ///
    /// Handles `Lagged` by logging a warning (events are lost but the log
    /// continues). Breaks on `Closed`.
    pub async fn run(&self) {
        let mut rx = self.bus.subscribe();
        info!("Crow forensic logger started for node {}", self.local_node);

        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Err(e) = self.record(event, self.local_node, 0) {
                        error!("Crow failed to record event: {}", e);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Crow lagged, missed {} events", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    info!("Crow bus closed, shutting down");
                    break;
                }
            }
        }
    }

    /// Record an event to the forensic log.
    ///
    /// 1. Assign a monotonic sequence number.
    /// 2. Compute the entry hash over canonical JSON of (seq, event, origin, timestamp, prev_hash).
    /// 3. Check dedup — skip if already seen.
    /// 4. Write a JSONL line to the current log file.
    /// 5. Index the entry in SQLite.
    /// 6. Update prev_hash and push to ring buffer.
    /// 7. Check if daily rotation is needed.
    pub fn record(
        &self,
        event: MarabuntaEvent,
        origin_node: NodeId,
        gossip_hops: u8,
    ) -> Result<CrowEntry, CrowError> {
        let seq = self.seq_counter.fetch_add(1, Ordering::SeqCst);
        let recorded_at = SystemTime::now();
        let recorded_at_ms = recorded_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis();

        let prev_hash = {
            let guard = self.prev_hash.lock();
            *guard
        };

        let payload = HashPayload {
            seq,
            event: event.clone(),
            origin_node,
            recorded_at_ms,
            prev_hash,
        };

        let canonical_bytes = serde_json::to_vec(&payload)
            .map_err(|e| CrowError::Serialization(e.to_string()))?;
        let entry_hash: Blake3Hash = *blake3::hash(&canonical_bytes).as_bytes();

        // Dedup check
        {
            let mut seen = self.seen_hashes.lock();
            if seen.contains(&entry_hash) {
                // Already recorded — decrement seq since we won't use it
                // (best-effort; seq gaps are harmless)
                debug!("Crow dedup: skipping already-seen entry hash");
                return Ok(CrowEntry {
                    seq,
                    event,
                    origin_node,
                    recorded_by: self.local_node,
                    recorded_at,
                    gossip_hops,
                    prev_hash,
                    entry_hash,
                });
            }
            // Cap dedup set
            if seen.len() >= self.config.dedup_set_capacity {
                seen.clear();
            }
            seen.insert(entry_hash);
        }

        let entry = CrowEntry {
            seq,
            event,
            origin_node,
            recorded_by: self.local_node,
            recorded_at,
            gossip_hops,
            prev_hash,
            entry_hash,
        };

        // Write JSONL line
        let current_log_file = {
            let _date = self.current_date.lock();
            "current.jsonl".to_string()
        };
        {
            let json_line = serde_json::to_string(&entry)
                .map_err(|e| CrowError::Serialization(e.to_string()))?;
            let mut writer = self.log_writer.lock();
            writeln!(writer, "{}", json_line)?;
            writer.flush()?;
        }

        // Index in SQLite
        {
            let task_id_bytes = entry.event.task_id().map(|t| t.to_vec());
            let target_node_bytes = entry
                .event
                .target_node()
                .map(|n| n.0.as_bytes().to_vec());
            let recorded_at_epoch = entry
                .recorded_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;

            let db = self.index_db.lock();
            db.execute(
                "INSERT OR IGNORE INTO crow_index \
                 (seq, event_type, origin_node, task_id, target_node, \
                  recorded_at, entry_hash, log_file) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    entry.seq as i64,
                    entry.event.type_name(),
                    entry.origin_node.0.as_bytes().as_slice(),
                    task_id_bytes,
                    target_node_bytes,
                    recorded_at_epoch,
                    entry.entry_hash.as_slice(),
                    current_log_file,
                ],
            )?;
        }

        // Update prev_hash
        {
            let mut ph = self.prev_hash.lock();
            *ph = entry_hash;
        }

        // Push to ring buffer
        {
            let mut ring = self.recent.lock();
            if ring.len() >= self.config.ring_buffer_capacity {
                ring.pop_front();
            }
            ring.push_back(entry.clone());
        }

        // Mirror to PG for time-travel queries.
        if let Some(ref tx) = self.pg_writer {
            let _ = tx.send(entry.clone());
        }

        // Maybe rotate
        let _ = self.maybe_rotate();

        Ok(entry)
    }

    /// Query the Crow log with structured filters.
    ///
    /// Builds a dynamic SQL WHERE clause, executes against the index, then
    /// loads matching entries from the ring buffer or JSONL files.
    pub fn query(&self, q: &CrowQuery) -> Result<Vec<CrowEntry>, CrowError> {
        let mut conditions = Vec::new();
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref tid) = q.task_id {
            conditions.push("task_id = ?".to_string());
            bind_values.push(Box::new(tid.to_vec()));
        }
        if let Some(ref nid) = q.node_id {
            conditions.push("origin_node = ?".to_string());
            bind_values.push(Box::new(nid.0.as_bytes().to_vec()));
        }
        if let Some(ref types) = q.event_types {
            if !types.is_empty() {
                let placeholders: Vec<String> =
                    types.iter().map(|_| "?".to_string()).collect();
                conditions.push(format!(
                    "event_type IN ({})",
                    placeholders.join(", ")
                ));
                for t in types {
                    bind_values.push(Box::new(t.clone()));
                }
            }
        }
        if let Some((start, end)) = q.time_range {
            let start_epoch = start
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            let end_epoch = end
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as i64;
            conditions.push("recorded_at >= ? AND recorded_at <= ?".to_string());
            bind_values.push(Box::new(start_epoch));
            bind_values.push(Box::new(end_epoch));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let order_clause = match q.order {
            QueryOrder::Chronological => "ORDER BY seq ASC",
            QueryOrder::ReverseChronological => "ORDER BY seq DESC",
        };

        let limit_clause = match q.limit {
            Some(n) => format!(" LIMIT {}", n),
            None => String::new(),
        };

        let sql = format!(
            "SELECT seq, log_file FROM crow_index{}  {} {}",
            where_clause, order_clause, limit_clause
        );

        let db = self.index_db.lock();
        let mut stmt = db.prepare(&sql)?;

        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();

        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            let seq: i64 = row.get(0)?;
            let log_file: String = row.get(1)?;
            Ok((seq as u64, log_file))
        })?;

        let mut targets: Vec<(u64, String)> = Vec::new();
        for row in rows {
            targets.push(row?);
        }

        // Collect from ring buffer + JSONL files
        let ring = self.recent.lock();
        let ring_map: std::collections::HashMap<u64, &CrowEntry> =
            ring.iter().map(|e| (e.seq, e)).collect();

        let mut results = Vec::new();
        let crow_dir = self.config.data_dir.join("crow");

        for (seq, log_file) in &targets {
            if let Some(entry) = ring_map.get(seq) {
                results.push((*entry).clone());
            } else {
                // Load from JSONL
                let file_path = crow_dir.join(log_file);
                if let Some(entry) = Self::find_entry_in_jsonl(&file_path, *seq) {
                    results.push(entry);
                }
            }
        }

        Ok(results)
    }

    /// Get all events for a specific task, ordered chronologically.
    pub fn task_timeline(&self, task_id: TaskId) -> Result<Vec<CrowEntry>, CrowError> {
        self.query(&CrowQuery {
            task_id: Some(task_id),
            order: QueryOrder::Chronological,
            ..CrowQuery::default()
        })
    }

    /// Get all events originating from a specific node, ordered chronologically.
    pub fn node_history(&self, node_id: NodeId) -> Result<Vec<CrowEntry>, CrowError> {
        self.query(&CrowQuery {
            node_id: Some(node_id),
            order: QueryOrder::Chronological,
            ..CrowQuery::default()
        })
    }

    /// Get security-related events (anomalies, threats, quarantines, kills, hunts,
    /// deception, autopsies).
    pub fn security_log(&self) -> Result<Vec<CrowEntry>, CrowError> {
        self.query(&CrowQuery {
            event_types: Some(vec![
                "AnomalyDetected".into(),
                "ThreatConfirmed".into(),
                "NodeQuarantined".into(),
                "NodeKilled".into(),
                "PackHuntInitiated".into(),
                "DeceptionStarted".into(),
                "AutopsyCompleted".into(),
            ]),
            order: QueryOrder::Chronological,
            ..CrowQuery::default()
        })
    }

    /// Get recent events from the in-memory ring buffer within the given time window.
    pub fn get_recent_events(&self, window_minutes: u64) -> Vec<CrowEntry> {
        let cutoff = SystemTime::now()
            .checked_sub(Duration::from_secs(window_minutes * 60))
            .unwrap_or(UNIX_EPOCH);

        let ring = self.recent.lock();
        ring.iter()
            .filter(|e| e.recorded_at >= cutoff)
            .cloned()
            .collect()
    }

    /// Ingest events received from remote nodes via gossip.
    ///
    /// Deduplicates by entry_hash and re-records with the original origin_node
    /// but incremented gossip_hops.
    pub fn ingest_remote_events(
        &self,
        entries: Vec<CrowEntry>,
    ) -> Result<Vec<CrowEntry>, CrowError> {
        let mut ingested = Vec::new();
        for entry in entries {
            // Check dedup
            {
                let seen = self.seen_hashes.lock();
                if seen.contains(&entry.entry_hash) {
                    continue;
                }
            }
            // Re-record with incremented hops
            let hops = entry.gossip_hops.saturating_add(1);
            match self.record(entry.event.clone(), entry.origin_node, hops) {
                Ok(new_entry) => ingested.push(new_entry),
                Err(e) => {
                    warn!("Crow failed to ingest remote event: {}", e);
                }
            }
        }
        Ok(ingested)
    }

    /// Check if a daily rotation is needed and perform it.
    ///
    /// If the date has changed since the last write:
    /// 1. Rename `current.jsonl` to `YYYY-MM-DD.jsonl`.
    /// 2. Open a fresh `current.jsonl`.
    /// 3. Compact old logs.
    fn maybe_rotate(&self) -> Result<(), CrowError> {
        let today = Self::today_string();
        let needs_rotation = {
            let current = self.current_date.lock();
            *current != today
        };

        if !needs_rotation {
            return Ok(());
        }

        let crow_dir = self.config.data_dir.join("crow");

        // Get the old date before updating
        let old_date = {
            let mut current = self.current_date.lock();
            let old = current.clone();
            *current = today;
            old
        };

        let current_path = crow_dir.join("current.jsonl");
        let archive_path = crow_dir.join(format!("{}.jsonl", old_date));

        // Flush and close current writer by replacing it
        {
            let mut writer = self.log_writer.lock();
            let _ = writer.flush();

            // Rename current to dated archive
            if current_path.exists() {
                if let Err(e) = fs::rename(&current_path, &archive_path) {
                    warn!("Crow failed to rotate log: {}", e);
                }
            }

            // Open new current.jsonl
            let new_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&current_path)?;
            *writer = new_file;
        }

        // Update log_file in index for entries that reference current.jsonl
        // belonging to the old date
        {
            let db = self.index_db.lock();
            let archived_name = format!("{}.jsonl", old_date);
            let _ = db.execute(
                "UPDATE crow_index SET log_file = ?1 WHERE log_file = 'current.jsonl'",
                params![archived_name],
            );
        }

        // Compact
        let _ = self.compact_old_logs();

        info!(
            "Crow rotated log: current.jsonl -> {}.jsonl",
            old_date
        );

        Ok(())
    }

    /// Delete JSONL files and corresponding SQLite rows older than `retention_days`.
    fn compact_old_logs(&self) -> Result<(), CrowError> {
        let crow_dir = self.config.data_dir.join("crow");
        let retention_secs =
            self.config.compact_after_days as u64 * 24 * 60 * 60;
        let cutoff = SystemTime::now()
            .checked_sub(Duration::from_secs(retention_secs))
            .unwrap_or(UNIX_EPOCH);

        let entries = fs::read_dir(&crow_dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            // Only compact dated JSONL files (YYYY-MM-DD.jsonl), not current.jsonl
            if file_name == "current.jsonl" || !file_name.ends_with(".jsonl") {
                continue;
            }

            if let Ok(metadata) = fs::metadata(&path) {
                let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
                if modified < cutoff {
                    info!("Crow compacting old log: {}", file_name);
                    let _ = fs::remove_file(&path);

                    // Remove SQLite rows
                    let db = self.index_db.lock();
                    let _ = db.execute(
                        "DELETE FROM crow_index WHERE log_file = ?1",
                        params![file_name],
                    );
                }
            }
        }

        Ok(())
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    /// Run the schema migration, creating tables and indexes if they don't exist.
    fn run_schema_migration(conn: &Connection) -> Result<(), CrowError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS crow_index (
                seq INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL,
                origin_node BLOB NOT NULL,
                task_id BLOB,
                target_node BLOB,
                recorded_at INTEGER NOT NULL,
                entry_hash BLOB NOT NULL UNIQUE,
                log_file TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_crow_event_type
                ON crow_index(event_type);
            CREATE INDEX IF NOT EXISTS idx_crow_origin_node
                ON crow_index(origin_node);
            CREATE INDEX IF NOT EXISTS idx_crow_task_id
                ON crow_index(task_id);
            CREATE INDEX IF NOT EXISTS idx_crow_target_node
                ON crow_index(target_node);
            CREATE INDEX IF NOT EXISTS idx_crow_recorded_at
                ON crow_index(recorded_at);",
        )?;
        Ok(())
    }

    /// Recover the maximum sequence number and last entry hash from SQLite.
    fn recover_state(conn: &Connection) -> Result<(u64, Blake3Hash), CrowError> {
        let max_seq: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(seq), -1) FROM crow_index",
                [],
                |row| row.get(0),
            )
            .unwrap_or(-1);

        let last_hash = if max_seq >= 0 {
            let hash_blob: Vec<u8> = conn
                .query_row(
                    "SELECT entry_hash FROM crow_index WHERE seq = ?1",
                    params![max_seq],
                    |row| row.get(0),
                )
                .unwrap_or_else(|_| vec![0u8; 32]);
            let mut h = [0u8; 32];
            if hash_blob.len() == 32 {
                h.copy_from_slice(&hash_blob);
            }
            h
        } else {
            [0u8; 32]
        };

        let recovered_seq = if max_seq >= 0 { max_seq as u64 } else { 0 };
        Ok((recovered_seq, last_hash))
    }

    /// Detect and truncate an incomplete last line in a JSONL file (crash recovery).
    fn repair_jsonl_if_needed(path: &Path) -> Result<(), CrowError> {
        if !path.exists() {
            return Ok(());
        }

        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            return Ok(());
        }

        let reader = BufReader::new(&file);
        let mut last_valid_pos: u64 = 0;

        for line in reader.lines() {
            match line {
                Ok(ref text) => {
                    if text.is_empty() {
                        last_valid_pos += 1; // newline
                        continue;
                    }
                    // Try to parse as a CrowEntry
                    if serde_json::from_str::<CrowEntry>(text).is_ok() {
                        last_valid_pos += text.len() as u64 + 1; // +1 for newline
                    } else {
                        // Corrupt / partial line — truncate here
                        warn!(
                            "Crow crash recovery: truncating partial line at offset {}",
                            last_valid_pos
                        );
                        break;
                    }
                }
                Err(_) => {
                    // I/O error reading line — truncate
                    break;
                }
            }
        }

        drop(file);

        // Truncate if needed
        if last_valid_pos < metadata.len() {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(last_valid_pos)?;
            info!(
                "Crow repaired JSONL: truncated from {} to {} bytes",
                metadata.len(),
                last_valid_pos
            );
        }

        Ok(())
    }

    /// Load the most recent N entries from a JSONL file into a Vec.
    fn load_recent_from_jsonl(path: &Path, capacity: usize) -> Vec<CrowEntry> {
        if !path.exists() {
            return Vec::new();
        }

        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };

        let reader = BufReader::new(file);
        let mut entries = VecDeque::with_capacity(capacity);

        for line in reader.lines().flatten() {
            if line.is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<CrowEntry>(&line) {
                if entries.len() >= capacity {
                    entries.pop_front();
                }
                entries.push_back(entry);
            }
        }

        entries.into()
    }

    /// Find a specific entry by sequence number in a JSONL file.
    fn find_entry_in_jsonl(path: &Path, seq: u64) -> Option<CrowEntry> {
        if !path.exists() {
            return None;
        }

        let file = File::open(path).ok()?;
        let reader = BufReader::new(file);

        for line in reader.lines().flatten() {
            if line.is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<CrowEntry>(&line) {
                if entry.seq == seq {
                    return Some(entry);
                }
            }
        }

        None
    }

    /// Get today's date as a YYYY-MM-DD string.
    fn today_string() -> String {
        let now = chrono::Utc::now();
        now.format("%Y-%m-%d").to_string()
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;

    /// Helper: create a Crow instance in a temp directory.
    fn make_crow(tmp: &TempDir) -> Crow {
        let config = CrowConfig {
            data_dir: tmp.path().to_path_buf(),
            ring_buffer_capacity: 100,
            dedup_set_capacity: 200,
            retention_days: 30,
            max_log_size_mb: 1024,
            rotation: "daily".into(),
            gossip_share_window_minutes: 10,
            compact_after_days: 7,
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        Crow::new(config, bus, node).expect("failed to create Crow")
    }

    /// Helper: create a sample event with a specific task id.
    fn sample_event(task_id: TaskId) -> MarabuntaEvent {
        MarabuntaEvent::TaskSubmitted {
            id: task_id,
            input_hash: [0u8; 32],
            requirements: ResourceRequirements::default(),
            timestamp: SystemTime::now(),
        }
    }

    /// Helper: create a security event targeting a specific node.
    fn security_event(node: NodeId) -> MarabuntaEvent {
        MarabuntaEvent::AnomalyDetected {
            node,
            score: 5.0,
            details: "test anomaly".into(),
            timestamp: SystemTime::now(),
        }
    }

    // -- Test 1: query by task_id --
    #[test]
    fn test_query_by_task_id() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);
        let tid = [1u8; 32];
        let other_tid = [2u8; 32];

        crow.record(sample_event(tid), crow.local_node, 0).unwrap();
        crow.record(sample_event(other_tid), crow.local_node, 0).unwrap();
        crow.record(sample_event(tid), crow.local_node, 0).unwrap();

        let results = crow
            .query(&CrowQuery {
                task_id: Some(tid),
                ..CrowQuery::default()
            })
            .unwrap();

        assert_eq!(results.len(), 2);
        for r in &results {
            assert_eq!(r.event.task_id(), Some(tid));
        }
    }

    // -- Test 2: query by node_id --
    #[test]
    fn test_query_by_node_id() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);
        let node_a = NodeId::new();
        let node_b = NodeId::new();

        crow.record(sample_event([1; 32]), node_a, 0).unwrap();
        crow.record(sample_event([2; 32]), node_b, 0).unwrap();
        crow.record(sample_event([3; 32]), node_a, 0).unwrap();

        let results = crow
            .query(&CrowQuery {
                node_id: Some(node_a),
                ..CrowQuery::default()
            })
            .unwrap();

        assert_eq!(results.len(), 2);
        for r in &results {
            assert_eq!(r.origin_node, node_a);
        }
    }

    // -- Test 3: query by event_type --
    #[test]
    fn test_query_by_event_type() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);
        let target = NodeId::new();

        crow.record(sample_event([1; 32]), crow.local_node, 0).unwrap();
        crow.record(security_event(target), crow.local_node, 0).unwrap();
        crow.record(sample_event([2; 32]), crow.local_node, 0).unwrap();

        let results = crow
            .query(&CrowQuery {
                event_types: Some(vec!["AnomalyDetected".into()]),
                ..CrowQuery::default()
            })
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].event.type_name(), "AnomalyDetected");
    }

    // -- Test 4: query by time_range --
    #[test]
    fn test_query_by_time_range() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);

        let before = SystemTime::now();
        std::thread::sleep(Duration::from_millis(50));

        crow.record(sample_event([1; 32]), crow.local_node, 0).unwrap();
        crow.record(sample_event([2; 32]), crow.local_node, 0).unwrap();

        std::thread::sleep(Duration::from_millis(50));
        let after = SystemTime::now();

        // Record one more after the window
        std::thread::sleep(Duration::from_secs(1));
        crow.record(sample_event([3; 32]), crow.local_node, 0).unwrap();

        let results = crow
            .query(&CrowQuery {
                time_range: Some((before, after)),
                ..CrowQuery::default()
            })
            .unwrap();

        assert_eq!(results.len(), 2);
    }

    // -- Test 5: chain integrity (10 events, verify prev_hash chain) --
    #[test]
    fn test_chain_integrity() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);

        let mut entries = Vec::new();
        for i in 0..10u8 {
            let entry = crow
                .record(sample_event([i; 32]), crow.local_node, 0)
                .unwrap();
            entries.push(entry);
        }

        // Verify chain: entry[i].prev_hash == entry[i-1].entry_hash
        for i in 1..entries.len() {
            assert_eq!(
                entries[i].prev_hash, entries[i - 1].entry_hash,
                "chain broken at index {}",
                i
            );
        }

        // First entry's prev_hash should be all zeros (genesis)
        assert_eq!(entries[0].prev_hash, [0u8; 32]);
    }

    // -- Test 6: dedup (same event recorded twice) --
    #[test]
    fn test_dedup_same_event() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);
        let event = sample_event([42; 32]);

        let entry1 = crow
            .record(event.clone(), crow.local_node, 0)
            .unwrap();

        // Recording the same event with the same origin at the same seq
        // produces a different seq (atomic incremented) so it gets a different
        // hash — dedup only catches exact hash collisions. For true dedup
        // we test via ingest_remote_events.
        let entry2 = crow
            .record(event.clone(), crow.local_node, 0)
            .unwrap();

        // Both succeed but with different seq numbers, hence different hashes
        assert_ne!(entry1.seq, entry2.seq);

        // Query should find both since they are distinct entries
        let results = crow
            .query(&CrowQuery {
                task_id: Some([42; 32]),
                ..CrowQuery::default()
            })
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    // -- Test 7: gossip ingest dedup --
    #[test]
    fn test_gossip_ingest_dedup() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);

        // Record locally
        let entry = crow
            .record(sample_event([10; 32]), crow.local_node, 0)
            .unwrap();

        // Try to ingest the same entry as a remote event
        let ingested = crow
            .ingest_remote_events(vec![entry.clone()])
            .unwrap();

        // Should be empty — the hash is already seen
        assert!(
            ingested.is_empty(),
            "expected no entries to be ingested (dedup)"
        );
    }

    // -- Test 8: ring buffer overflow --
    #[test]
    fn test_ring_buffer_overflow() {
        let tmp = TempDir::new().unwrap();
        let config = CrowConfig {
            data_dir: tmp.path().to_path_buf(),
            ring_buffer_capacity: 5,
            dedup_set_capacity: 200,
            retention_days: 30,
            max_log_size_mb: 1024,
            rotation: "daily".into(),
            gossip_share_window_minutes: 10,
            compact_after_days: 7,
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let crow = Crow::new(config, bus, node).unwrap();

        // Record 10 events into a ring buffer of capacity 5
        for i in 0..10u8 {
            crow.record(sample_event([i; 32]), crow.local_node, 0).unwrap();
        }

        let ring = crow.recent.lock();
        assert_eq!(ring.len(), 5);

        // The oldest entries should be seq 5..9 (the last 5)
        let seqs: Vec<u64> = ring.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![6, 7, 8, 9, 10]);
    }

    // -- Test 9: JSONL persistence (drop + reopen) --
    #[test]
    fn test_jsonl_persistence_across_restart() {
        let tmp = TempDir::new().unwrap();
        let config = CrowConfig {
            data_dir: tmp.path().to_path_buf(),
            ring_buffer_capacity: 100,
            dedup_set_capacity: 200,
            retention_days: 30,
            max_log_size_mb: 1024,
            rotation: "daily".into(),
            gossip_share_window_minutes: 10,
            compact_after_days: 7,
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();

        // First instance: record events
        {
            let crow = Crow::new(config.clone(), bus.clone(), node).unwrap();
            for i in 0..5u8 {
                crow.record(sample_event([i; 32]), crow.local_node, 0).unwrap();
            }
        }

        // Second instance: should recover the events
        {
            let crow2 = Crow::new(config.clone(), bus.clone(), node).unwrap();

            // Ring buffer should be populated from JSONL
            let ring = crow2.recent.lock();
            assert_eq!(ring.len(), 5);

            // Seq counter should resume after the last entry
            let next_seq = crow2.seq_counter.load(Ordering::SeqCst);
            assert!(next_seq >= 5, "seq counter should be at least 5, got {}", next_seq);

            // prev_hash should match the last entry's hash
            let ph = crow2.prev_hash.lock();
            let last_entry = ring.back().unwrap();
            assert_eq!(*ph, last_entry.entry_hash);
        }
    }

    // -- Test 10: rotation --
    #[test]
    fn test_rotation() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);

        // Record an event
        crow.record(sample_event([1; 32]), crow.local_node, 0).unwrap();

        // Simulate a date change
        {
            let mut date = crow.current_date.lock();
            *date = "2020-01-01".to_string();
        }

        // Record another event — should trigger rotation
        crow.record(sample_event([2; 32]), crow.local_node, 0).unwrap();

        // Check that the archived file exists
        let crow_dir = tmp.path().join("crow");
        let archived = crow_dir.join("2020-01-01.jsonl");
        assert!(
            archived.exists(),
            "expected archived JSONL at {:?}",
            archived
        );

        // current.jsonl should still exist
        let current = crow_dir.join("current.jsonl");
        assert!(current.exists());
    }

    // -- Test 11: compaction --
    #[test]
    fn test_compaction() {
        let tmp = TempDir::new().unwrap();
        let config = CrowConfig {
            data_dir: tmp.path().to_path_buf(),
            ring_buffer_capacity: 100,
            dedup_set_capacity: 200,
            retention_days: 30,
            max_log_size_mb: 1024,
            rotation: "daily".into(),
            gossip_share_window_minutes: 10,
            compact_after_days: 0, // Compact everything older than now
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let crow = Crow::new(config, bus, node).unwrap();

        // Create a fake old log file
        let crow_dir = tmp.path().join("crow");
        let old_log = crow_dir.join("2020-01-01.jsonl");
        fs::write(&old_log, "fake data\n").unwrap();

        // Set its mtime to the past by creating it (already old enough with compact_after_days=0)
        // The file was just created so its mtime is "now". We need to set compact_after_days
        // to 0, but the file's mtime will be approximately now. To make it work, we also
        // insert a matching SQLite row and verify the row gets deleted.
        {
            let db = crow.index_db.lock();
            db.execute(
                "INSERT INTO crow_index (seq, event_type, origin_node, task_id, \
                 target_node, recorded_at, entry_hash, log_file) \
                 VALUES (-1, 'test', X'00', NULL, NULL, 0, X'AA', '2020-01-01.jsonl')",
                [],
            )
            .unwrap();
        }

        // We need the file to appear old. Overwrite with filetime workaround:
        // Since we set compact_after_days=0, any file older than "now" should be compacted.
        // The file was just created (<1s ago) so with compact_after_days=0 the cutoff is
        // essentially "now". We need to wait or use a different approach.
        // Instead, let's just set compact_after_days high and verify nothing happens,
        // then set it to 0 after sleeping briefly.
        std::thread::sleep(Duration::from_millis(1100));

        crow.compact_old_logs().unwrap();

        // The old log file should be deleted (its mtime is ~1s ago, cutoff is now - 0 days = now)
        assert!(
            !old_log.exists(),
            "expected old log to be deleted after compaction"
        );

        // Verify SQLite row was cleaned up
        {
            let db = crow.index_db.lock();
            let count: i64 = db
                .query_row(
                    "SELECT COUNT(*) FROM crow_index WHERE log_file = '2020-01-01.jsonl'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0, "expected SQLite row to be deleted");
        }
    }

    // -- Test 12: crash recovery (partial line) --
    #[test]
    fn test_crash_recovery_partial_line() {
        let tmp = TempDir::new().unwrap();
        let crow_dir = tmp.path().join("crow");
        fs::create_dir_all(&crow_dir).unwrap();

        let config = CrowConfig {
            data_dir: tmp.path().to_path_buf(),
            ring_buffer_capacity: 100,
            dedup_set_capacity: 200,
            retention_days: 30,
            max_log_size_mb: 1024,
            rotation: "daily".into(),
            gossip_share_window_minutes: 10,
            compact_after_days: 7,
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();

        // First: create a valid log and then append junk to simulate crash
        {
            let crow = Crow::new(config.clone(), bus.clone(), node).unwrap();
            crow.record(sample_event([1; 32]), crow.local_node, 0).unwrap();
            crow.record(sample_event([2; 32]), crow.local_node, 0).unwrap();
        }

        // Append a partial line (simulating a crash mid-write)
        let jsonl_path = crow_dir.join("current.jsonl");
        {
            let mut f = OpenOptions::new().append(true).open(&jsonl_path).unwrap();
            write!(f, "{{\"seq\":999,\"event\":").unwrap();
            f.flush().unwrap();
        }

        // Reopen — should repair the partial line
        let crow2 = Crow::new(config.clone(), bus.clone(), node).unwrap();

        // Should have exactly 2 entries in ring buffer (the partial line is gone)
        let ring = crow2.recent.lock();
        assert_eq!(ring.len(), 2);

        // Should be able to record new events without error
        drop(ring);
        crow2.record(sample_event([3; 32]), crow2.local_node, 0).unwrap();
        let ring = crow2.recent.lock();
        assert_eq!(ring.len(), 3);
    }

    // -- Test 13: query ordering --
    #[test]
    fn test_query_ordering() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);

        for i in 0..5u8 {
            crow.record(sample_event([i; 32]), crow.local_node, 0).unwrap();
        }

        // Chronological
        let asc = crow
            .query(&CrowQuery {
                order: QueryOrder::Chronological,
                ..CrowQuery::default()
            })
            .unwrap();

        assert_eq!(asc.len(), 5);
        for i in 1..asc.len() {
            assert!(asc[i].seq > asc[i - 1].seq);
        }

        // Reverse chronological
        let desc = crow
            .query(&CrowQuery {
                order: QueryOrder::ReverseChronological,
                ..CrowQuery::default()
            })
            .unwrap();

        assert_eq!(desc.len(), 5);
        for i in 1..desc.len() {
            assert!(desc[i].seq < desc[i - 1].seq);
        }
    }

    // -- Test 14: task_timeline convenience --
    #[test]
    fn test_task_timeline() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);
        let tid = [77u8; 32];

        crow.record(sample_event(tid), crow.local_node, 0).unwrap();
        crow.record(sample_event([0; 32]), crow.local_node, 0).unwrap();
        crow.record(
            MarabuntaEvent::TaskCompleted {
                id: tid,
                result_hash: [0; 32],
                node: crow.local_node,
                duration_ms: 100,
                timestamp: SystemTime::now(),
            },
            crow.local_node,
            0,
        )
        .unwrap();

        let timeline = crow.task_timeline(tid).unwrap();
        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].event.type_name(), "TaskSubmitted");
        assert_eq!(timeline[1].event.type_name(), "TaskCompleted");
    }

    // -- Test 15: node_history convenience --
    #[test]
    fn test_node_history() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);
        let target_node = NodeId::new();

        crow.record(sample_event([1; 32]), target_node, 0).unwrap();
        crow.record(sample_event([2; 32]), crow.local_node, 0).unwrap();
        crow.record(sample_event([3; 32]), target_node, 0).unwrap();

        let history = crow.node_history(target_node).unwrap();
        assert_eq!(history.len(), 2);
    }

    // -- Test 16: security_log convenience --
    #[test]
    fn test_security_log() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);
        let target = NodeId::new();

        crow.record(sample_event([1; 32]), crow.local_node, 0).unwrap();
        crow.record(security_event(target), crow.local_node, 0).unwrap();
        crow.record(
            MarabuntaEvent::NodeQuarantined {
                node: target,
                reason: "test".into(),
                timestamp: SystemTime::now(),
            },
            crow.local_node,
            0,
        )
        .unwrap();
        crow.record(sample_event([2; 32]), crow.local_node, 0).unwrap();

        let sec = crow.security_log().unwrap();
        assert_eq!(sec.len(), 2);
    }

    // -- Test 17: get_recent_events window --
    #[test]
    fn test_get_recent_events() {
        let tmp = TempDir::new().unwrap();
        let crow = make_crow(&tmp);

        for i in 0..5u8 {
            crow.record(sample_event([i; 32]), crow.local_node, 0).unwrap();
        }

        // All events are within the last minute
        let recent = crow.get_recent_events(1);
        assert_eq!(recent.len(), 5);

        // Zero-minute window should return nothing (or only events from "now")
        // Since events were just recorded, they are within 0 minutes iff
        // the recorded_at >= now - 0s which is now. Due to clock granularity
        // they may or may not match, so we just check it doesn't panic.
        let _zero = crow.get_recent_events(0);
    }

    // -- Test 18: dedup set capacity clearing --
    #[test]
    fn test_dedup_set_capacity_clear() {
        let tmp = TempDir::new().unwrap();
        let config = CrowConfig {
            data_dir: tmp.path().to_path_buf(),
            ring_buffer_capacity: 1000,
            dedup_set_capacity: 5, // Very small to trigger clearing
            retention_days: 30,
            max_log_size_mb: 1024,
            rotation: "daily".into(),
            gossip_share_window_minutes: 10,
            compact_after_days: 7,
            ..Default::default()
        };
        let bus = Arc::new(NeuromancerBus::default());
        let node = NodeId::new();
        let crow = Crow::new(config, bus, node).unwrap();

        // Record more events than dedup capacity
        for i in 0..10u8 {
            crow.record(sample_event([i; 32]), crow.local_node, 0).unwrap();
        }

        // All 10 should be recorded (dedup set cleared, not blocking new entries)
        let results = crow
            .query(&CrowQuery::default())
            .unwrap();
        assert_eq!(results.len(), 10);

        // Dedup set should have been cleared at least once
        let seen = crow.seen_hashes.lock();
        assert!(
            seen.len() <= 5,
            "dedup set should have been capped, got {}",
            seen.len()
        );
    }
}
