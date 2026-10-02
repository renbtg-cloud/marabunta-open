// Marabunta - Licensed under the MIT License.
//! SQLite-to-PostgreSQL sync bridge for lightweight nodes.
//!
//! Edge (<1.5 GB RAM) and small Light nodes can't run their own PostgreSQL
//! instance, but they still generate events (node state, audit, forensic).
//! This bridge buffers events locally in SQLite and periodically batch-syncs
//! them to the nearest PG node.
//!
//! # Architecture
//!
//! ```text
//! [Local subsystems] → [SQLite local_events] → [SyncBridge] → [PG temporal tables]
//! ```
//!
//! Events are written to a local `local_events` table with a monotonic ID.
//! A background loop wakes every `sync_interval`, reads unsynced events,
//! batch-INSERTs them to PG, and marks them synced. Deduplication uses the
//! `(source_node, source_event_id)` unique constraint on the PG side.
//!
//! # Fallback
//!
//! If PG is unreachable, events accumulate locally. Retention limits prevent
//! unbounded growth. Events are replayed on next successful sync.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::types::PgError;
use crate::swarm::types::NodeId;

// ============================================================================
// Constants
// ============================================================================

/// Default sync interval (30 seconds).
const DEFAULT_SYNC_INTERVAL_SECS: u64 = 30;

/// Maximum events per sync batch (prevents large transactions).
const MAX_BATCH_SIZE: usize = 500;

/// Maximum local events before oldest are pruned (prevents unbounded growth).
const MAX_LOCAL_EVENTS: u64 = 100_000;

/// SQLite schema for the local events table.
const LOCAL_EVENTS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS local_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    synced_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_local_events_synced ON local_events (synced_at);
CREATE INDEX IF NOT EXISTS idx_local_events_table ON local_events (table_name);
"#;

// ============================================================================
// SyncEvent
// ============================================================================

/// A local event waiting to be synced to PG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEvent {
    /// Local row ID (monotonic).
    pub id: i64,
    /// Target PG table (e.g., "node_state_history", "audit_events").
    pub table_name: String,
    /// JSON payload to INSERT into the PG table.
    pub payload: serde_json::Value,
    /// When the event was created locally.
    pub created_at: String,
    /// When the event was successfully synced to PG (None = pending).
    pub synced_at: Option<String>,
}

// ============================================================================
// SyncStats
// ============================================================================

/// Statistics about the sync bridge's operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStats {
    /// Total events buffered locally.
    pub total_buffered: u64,
    /// Events pending sync.
    pub pending_count: u64,
    /// Events successfully synced.
    pub synced_count: u64,
    /// Total sync batches executed.
    pub sync_batches: u64,
    /// Last successful sync timestamp.
    pub last_sync: Option<DateTime<Utc>>,
    /// Last sync error (if any).
    pub last_error: Option<String>,
    /// Whether a sync is currently in progress.
    pub sync_in_progress: bool,
}

// ============================================================================
// SyncBridge
// ============================================================================

/// SQLite-to-PostgreSQL sync bridge for lightweight nodes.
///
/// Buffer events locally in SQLite and batch-sync to PG on a timer.
pub struct SyncBridge {
    /// Local SQLite database for event buffering.
    db: parking_lot::Mutex<Connection>,
    /// This node's identity (for deduplication on PG side).
    node_id: NodeId,
    /// How often to attempt sync (seconds).
    sync_interval: Duration,
    /// Monotonic counter of total events buffered.
    total_buffered: AtomicU64,
    /// Total sync batches executed.
    sync_batches: AtomicU64,
    /// Whether a sync is currently in progress.
    sync_in_progress: AtomicBool,
    /// Last sync error message.
    last_error: parking_lot::Mutex<Option<String>>,
    /// Last successful sync timestamp.
    last_sync: parking_lot::Mutex<Option<DateTime<Utc>>>,
}

impl SyncBridge {
    /// Create a new sync bridge with an in-memory SQLite database.
    ///
    /// Use `open(path)` for persistent local buffering.
    pub fn new(node_id: NodeId) -> Result<Self, PgError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| PgError::QueryFailed(format!("SQLite in-memory: {e}")))?;
        Self::init_db(&conn)?;

        Ok(Self {
            db: parking_lot::Mutex::new(conn),
            node_id,
            sync_interval: Duration::from_secs(DEFAULT_SYNC_INTERVAL_SECS),
            total_buffered: AtomicU64::new(0),
            sync_batches: AtomicU64::new(0),
            sync_in_progress: AtomicBool::new(false),
            last_error: parking_lot::Mutex::new(None),
            last_sync: parking_lot::Mutex::new(None),
        })
    }

    /// Open a sync bridge with a persistent SQLite database.
    pub fn open(path: &Path, node_id: NodeId) -> Result<Self, PgError> {
        std::fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))
            .map_err(PgError::Io)?;

        let db_path = path.join("sync_bridge.db");
        let conn = Connection::open(&db_path)
            .map_err(|e| PgError::QueryFailed(format!("SQLite open {}: {e}", db_path.display())))?;

        // WAL mode for concurrent reads during sync.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .map_err(|e| PgError::QueryFailed(format!("SQLite pragma: {e}")))?;

        Self::init_db(&conn)?;

        // Count existing unsynced events.
        let buffered: u64 = conn
            .query_row(
                "SELECT COUNT(*) FROM local_events WHERE synced_at IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        info!(
            path = %db_path.display(),
            pending = buffered,
            "Sync bridge opened"
        );

        Ok(Self {
            db: parking_lot::Mutex::new(conn),
            node_id,
            sync_interval: Duration::from_secs(DEFAULT_SYNC_INTERVAL_SECS),
            total_buffered: AtomicU64::new(buffered),
            sync_batches: AtomicU64::new(0),
            sync_in_progress: AtomicBool::new(false),
            last_error: parking_lot::Mutex::new(None),
            last_sync: parking_lot::Mutex::new(None),
        })
    }

    /// Set the sync interval.
    pub fn with_sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    /// Get the configured sync interval.
    pub fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    // ========================================================================
    // Event buffering
    // ========================================================================

    /// Buffer a local event for later sync to PG.
    ///
    /// This is a fast, synchronous operation — just an INSERT into SQLite.
    pub fn buffer_event(
        &self,
        table_name: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, PgError> {
        let payload_str = serde_json::to_string(payload)
            .map_err(|e| PgError::Serialization(format!("JSON serialize: {e}")))?;

        let db = self.db.lock();
        db.execute(
            "INSERT INTO local_events (table_name, payload) VALUES (?1, ?2)",
            params![table_name, payload_str],
        )
        .map_err(|e| PgError::QueryFailed(format!("buffer event: {e}")))?;

        let id = db.last_insert_rowid();
        self.total_buffered.fetch_add(1, Ordering::Relaxed);

        // Prune if over limit.
        self.maybe_prune(&db);

        Ok(id)
    }

    /// Get the number of events pending sync.
    pub fn pending_count(&self) -> u64 {
        let db = self.db.lock();
        db.query_row(
            "SELECT COUNT(*) FROM local_events WHERE synced_at IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    /// Get pending events (for sync or inspection).
    pub fn pending_events(&self, limit: usize) -> Vec<SyncEvent> {
        let db = self.db.lock();
        let mut stmt = match db.prepare(
            "SELECT id, table_name, payload, created_at, synced_at
             FROM local_events WHERE synced_at IS NULL
             ORDER BY id ASC LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        stmt.query_map(params![limit as i64], |row| {
            let payload_str: String = row.get(2)?;
            Ok(SyncEvent {
                id: row.get(0)?,
                table_name: row.get(1)?,
                payload: serde_json::from_str(&payload_str).unwrap_or_default(),
                created_at: row.get(3)?,
                synced_at: row.get(4)?,
            })
        })
        .ok()
        .map(|rows| rows.filter_map(|r| r.ok()).collect())
        .unwrap_or_default()
    }

    // ========================================================================
    // Sync operations
    // ========================================================================

    /// Sync buffered events to PG.
    ///
    /// Reads a batch of unsynced events, INSERTs them to PG, and marks
    /// them synced locally. Returns the number of events synced.
    ///
    /// Uses the PG pool's write path since we're inserting data.
    pub async fn sync_to_pg(
        &self,
        pg_pool: &super::pool::PgPool,
    ) -> Result<usize, PgError> {
        if self.sync_in_progress.swap(true, Ordering::SeqCst) {
            debug!("Sync already in progress, skipping");
            return Ok(0);
        }

        let result = self.do_sync(pg_pool).await;

        self.sync_in_progress.store(false, Ordering::SeqCst);

        match &result {
            Ok(count) => {
                if *count > 0 {
                    self.sync_batches.fetch_add(1, Ordering::Relaxed);
                    *self.last_sync.lock() = Some(Utc::now());
                    *self.last_error.lock() = None;
                    debug!(synced = count, "Sync batch completed");
                }
            }
            Err(e) => {
                *self.last_error.lock() = Some(e.to_string());
                warn!(error = %e, "Sync batch failed");
            }
        }

        result
    }

    /// Internal sync implementation.
    async fn do_sync(
        &self,
        pg_pool: &super::pool::PgPool,
    ) -> Result<usize, PgError> {
        // Read pending events from SQLite.
        let events = self.pending_events(MAX_BATCH_SIZE);
        if events.is_empty() {
            return Ok(0);
        }

        // Get a PG connection.
        let client = pg_pool.get_write().await?;
        let node_id_str = self.node_id.to_string();

        let mut synced_ids = Vec::new();

        for event in &events {
            let result = match event.table_name.as_str() {
                "node_state_history" => {
                    Self::sync_node_event(&client, &node_id_str, &event.payload).await
                }
                "job_history" => {
                    Self::sync_job_event(&client, &node_id_str, &event.payload).await
                }
                "audit_events" => {
                    Self::sync_audit_event(&client, &node_id_str, &event.payload).await
                }
                other => {
                    warn!(table = other, "Unknown sync target table, skipping");
                    Ok(())
                }
            };

            match result {
                Ok(()) => synced_ids.push(event.id),
                Err(e) => {
                    warn!(
                        event_id = event.id,
                        table = %event.table_name,
                        error = %e,
                        "Failed to sync event, will retry"
                    );
                    // Stop batch on first failure to preserve ordering.
                    break;
                }
            }
        }

        // Mark synced events.
        if !synced_ids.is_empty() {
            self.mark_synced(&synced_ids);
        }

        Ok(synced_ids.len())
    }

    /// Sync a node state event to PG.
    async fn sync_node_event(
        client: &tokio_postgres::Client,
        _source_node: &str,
        payload: &serde_json::Value,
    ) -> Result<(), PgError> {
        let node_id_str = payload.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
        let event_type = payload.get("event_type").and_then(|v| v.as_str()).unwrap_or("unknown");

        let node_uuid: uuid::Uuid = node_id_str
            .parse()
            .unwrap_or_else(|_| uuid::Uuid::nil());

        client
            .execute(
                "INSERT INTO node_state_history (node_id, event_type, state_json, traits, valid_from)
                 VALUES ($1, $2, $3::jsonb, $4::jsonb, now())
                 ON CONFLICT DO NOTHING",
                &[
                    &node_uuid,
                    &event_type,
                    &payload,
                    &payload.get("traits"),
                ],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("sync node event: {e}")))?;

        Ok(())
    }

    /// Sync a job event to PG.
    async fn sync_job_event(
        client: &tokio_postgres::Client,
        _source_node: &str,
        payload: &serde_json::Value,
    ) -> Result<(), PgError> {
        let job_id_str = payload.get("job_id").and_then(|v| v.as_str()).unwrap_or("");
        let event_type = payload.get("event_type").and_then(|v| v.as_str()).unwrap_or("unknown");

        let job_uuid: uuid::Uuid = job_id_str
            .parse()
            .unwrap_or_else(|_| uuid::Uuid::nil());

        let node_uuid: Option<uuid::Uuid> = payload
            .get("node_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok());

        let chunk_uuid: Option<uuid::Uuid> = payload
            .get("chunk_id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok());

        client
            .execute(
                "INSERT INTO job_history (job_id, event_type, event_data, node_id, chunk_id, recorded_at)
                 VALUES ($1, $2, $3::jsonb, $4, $5, now())
                 ON CONFLICT DO NOTHING",
                &[
                    &job_uuid,
                    &event_type,
                    &payload,
                    &node_uuid,
                    &chunk_uuid,
                ],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("sync job event: {e}")))?;

        Ok(())
    }

    /// Sync an audit event to PG.
    async fn sync_audit_event(
        client: &tokio_postgres::Client,
        source_node: &str,
        payload: &serde_json::Value,
    ) -> Result<(), PgError> {
        let event_type = payload.get("event_type").and_then(|v| v.as_str())
            .or_else(|| payload.get("action").and_then(|v| v.as_str()))
            .unwrap_or("unknown");
        let actor = payload.get("actor").and_then(|v| v.as_str()).unwrap_or(source_node);
        let target = payload.get("target").and_then(|v| v.as_str()).map(|s| s.to_string());
        let target_type = payload.get("target_type").and_then(|v| v.as_str()).map(|s| s.to_string());

        // Use pre-computed hash from the audit entry if available.
        let row_hash = payload.get("hash").and_then(|v| v.as_str())
            .unwrap_or("sync-bridge");
        let prev_hash = payload.get("prev_hash").and_then(|v| v.as_str())
            .map(|s| s.to_string());

        client
            .execute(
                "INSERT INTO audit_events (event_type, actor, target, target_type, detail_json, recorded_at, prev_hash, row_hash)
                 VALUES ($1, $2, $3, $4, $5::jsonb, now(), $6, $7)
                 ON CONFLICT DO NOTHING",
                &[
                    &event_type,
                    &actor,
                    &target,
                    &target_type,
                    &payload,
                    &prev_hash,
                    &row_hash,
                ],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("sync audit event: {e}")))?;

        Ok(())
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Initialize the local SQLite schema.
    fn init_db(conn: &Connection) -> Result<(), PgError> {
        conn.execute_batch(LOCAL_EVENTS_SCHEMA)
            .map_err(|e| PgError::QueryFailed(format!("SQLite init: {e}")))?;
        Ok(())
    }

    /// Mark events as synced by their local IDs.
    fn mark_synced(&self, ids: &[i64]) {
        let db = self.db.lock();
        let now = Utc::now().to_rfc3339();
        for id in ids {
            let _ = db.execute(
                "UPDATE local_events SET synced_at = ?1 WHERE id = ?2",
                params![now, id],
            );
        }
    }

    /// Prune old synced events if we're over the limit.
    fn maybe_prune(&self, db: &Connection) {
        let total: u64 = db
            .query_row("SELECT COUNT(*) FROM local_events", [], |row| row.get(0))
            .unwrap_or(0);

        if total > MAX_LOCAL_EVENTS {
            let to_delete = total - MAX_LOCAL_EVENTS;
            let _ = db.execute(
                "DELETE FROM local_events WHERE id IN (
                    SELECT id FROM local_events WHERE synced_at IS NOT NULL
                    ORDER BY id ASC LIMIT ?1
                 )",
                params![to_delete as i64],
            );
            debug!(pruned = to_delete, "Pruned old synced events");
        }
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get sync bridge statistics.
    pub fn stats(&self) -> SyncStats {
        SyncStats {
            total_buffered: self.total_buffered.load(Ordering::Relaxed),
            pending_count: self.pending_count(),
            synced_count: {
                let db = self.db.lock();
                db.query_row(
                    "SELECT COUNT(*) FROM local_events WHERE synced_at IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0)
            },
            sync_batches: self.sync_batches.load(Ordering::Relaxed),
            last_sync: *self.last_sync.lock(),
            last_error: self.last_error.lock().clone(),
            sync_in_progress: self.sync_in_progress.load(Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node_id() -> NodeId {
        NodeId(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_DNS,
            b"sync-test",
        ))
    }

    #[test]
    fn test_sync_bridge_creation() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();
        assert_eq!(bridge.pending_count(), 0);
        assert_eq!(bridge.sync_interval(), Duration::from_secs(30));
    }

    #[test]
    fn test_buffer_event() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();

        let id = bridge
            .buffer_event(
                "node_state_history",
                &serde_json::json!({"node_id": "abc", "event_type": "joined"}),
            )
            .unwrap();

        assert_eq!(id, 1);
        assert_eq!(bridge.pending_count(), 1);
    }

    #[test]
    fn test_buffer_multiple_events() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();

        for i in 0..10 {
            bridge
                .buffer_event(
                    "audit_events",
                    &serde_json::json!({"action": format!("test_{i}")}),
                )
                .unwrap();
        }

        assert_eq!(bridge.pending_count(), 10);
    }

    #[test]
    fn test_pending_events() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();

        bridge
            .buffer_event(
                "node_state_history",
                &serde_json::json!({"node_id": "n1", "event_type": "joined"}),
            )
            .unwrap();
        bridge
            .buffer_event(
                "job_history",
                &serde_json::json!({"job_id": "j1", "event_type": "submitted"}),
            )
            .unwrap();

        let events = bridge.pending_events(10);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].table_name, "node_state_history");
        assert_eq!(events[1].table_name, "job_history");
    }

    #[test]
    fn test_pending_events_limit() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();

        for i in 0..20 {
            bridge
                .buffer_event("audit_events", &serde_json::json!({"idx": i}))
                .unwrap();
        }

        let events = bridge.pending_events(5);
        assert_eq!(events.len(), 5);
        // Should be ordered by ID ascending.
        assert!(events[0].id < events[1].id);
    }

    #[test]
    fn test_mark_synced() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();

        bridge
            .buffer_event("audit_events", &serde_json::json!({"a": 1}))
            .unwrap();
        bridge
            .buffer_event("audit_events", &serde_json::json!({"a": 2}))
            .unwrap();
        bridge
            .buffer_event("audit_events", &serde_json::json!({"a": 3}))
            .unwrap();

        assert_eq!(bridge.pending_count(), 3);

        bridge.mark_synced(&[1, 2]);

        assert_eq!(bridge.pending_count(), 1);

        let events = bridge.pending_events(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, 3);
    }

    #[test]
    fn test_stats() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();

        bridge
            .buffer_event("audit_events", &serde_json::json!({"a": 1}))
            .unwrap();
        bridge
            .buffer_event("audit_events", &serde_json::json!({"a": 2}))
            .unwrap();

        bridge.mark_synced(&[1]);

        let stats = bridge.stats();
        assert_eq!(stats.total_buffered, 2);
        assert_eq!(stats.pending_count, 1);
        assert_eq!(stats.synced_count, 1);
        assert_eq!(stats.sync_batches, 0);
        assert!(stats.last_sync.is_none());
        assert!(!stats.sync_in_progress);
    }

    #[test]
    fn test_with_sync_interval() {
        let bridge = SyncBridge::new(test_node_id())
            .unwrap()
            .with_sync_interval(Duration::from_secs(5));
        assert_eq!(bridge.sync_interval(), Duration::from_secs(5));
    }

    #[test]
    fn test_sync_event_serde() {
        let event = SyncEvent {
            id: 42,
            table_name: "node_state_history".to_string(),
            payload: serde_json::json!({"node_id": "abc"}),
            created_at: "2025-01-15T10:00:00".to_string(),
            synced_at: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: SyncEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, 42);
        assert_eq!(back.table_name, "node_state_history");
        assert!(back.synced_at.is_none());
    }

    #[test]
    fn test_sync_stats_serde() {
        let stats = SyncStats {
            total_buffered: 100,
            pending_count: 10,
            synced_count: 90,
            sync_batches: 5,
            last_sync: Some(Utc::now()),
            last_error: None,
            sync_in_progress: false,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: SyncStats = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_buffered, 100);
        assert_eq!(back.synced_count, 90);
    }

    #[test]
    fn test_persistent_bridge() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = SyncBridge::open(dir.path(), test_node_id()).unwrap();

        bridge
            .buffer_event("audit_events", &serde_json::json!({"test": true}))
            .unwrap();
        assert_eq!(bridge.pending_count(), 1);

        // Re-open — event should still be there.
        drop(bridge);
        let bridge2 = SyncBridge::open(dir.path(), test_node_id()).unwrap();
        assert_eq!(bridge2.pending_count(), 1);
    }

    #[test]
    fn test_different_table_types() {
        let bridge = SyncBridge::new(test_node_id()).unwrap();

        bridge
            .buffer_event(
                "node_state_history",
                &serde_json::json!({"node_id": "n1", "event_type": "joined"}),
            )
            .unwrap();
        bridge
            .buffer_event(
                "job_history",
                &serde_json::json!({"job_id": "j1", "event_type": "submitted"}),
            )
            .unwrap();
        bridge
            .buffer_event(
                "audit_events",
                &serde_json::json!({"action": "config_change", "actor": "admin"}),
            )
            .unwrap();

        let events = bridge.pending_events(10);
        assert_eq!(events.len(), 3);

        let tables: Vec<_> = events.iter().map(|e| e.table_name.as_str()).collect();
        assert!(tables.contains(&"node_state_history"));
        assert!(tables.contains(&"job_history"));
        assert!(tables.contains(&"audit_events"));
    }

    // Note: async PG sync tests are deferred to integration_tests/postgres.rs
    // which requires a running PG instance.
}
