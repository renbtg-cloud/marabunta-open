// Marabunta - Licensed under the MIT License.
//! Time-travel query engine for the swarm-managed PostgreSQL.
//!
//! Not just for regulators — for scientists wondering "when did protein folding
//! go wrong" and operators investigating "what happened at 3am."
//!
//! All queries operate against the temporal (append-only) tables defined in
//! `schema.rs`. The engine provides seven core queries:
//!
//! - `config_at(ts)` — reconstruct full config at any timestamp
//! - `config_changes(from, to)` — all config changes in a range
//! - `node_timeline(node_id, from, to)` — full node history
//! - `swarm_snapshot_at(ts)` — all nodes + traits + jobs + config at a moment
//! - `job_lifecycle(job_id)` — submission to completion with all events
//! - `audit_trail(target, from, to)` — audit entries for any entity
//! - `correlate_with_event(event_type, ts, window)` — what else was happening
//!
//! # Design
//!
//! Stateless query helpers that take a `&tokio_postgres::Client` reference
//! (same pattern as `ConfigDb`). The caller obtains a client from the read
//! pool (`PgPool::get_read()`). This keeps the engine testable and decoupled
//! from connection management.

use std::collections::HashMap;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

// ============================================================================
// Result types
// ============================================================================

/// A single config change event from the history table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChange {
    /// History row ID.
    pub id: i64,
    /// The config key that changed (e.g., "gossip_fanout").
    pub key: String,
    /// Previous value (None for initial settings).
    pub old_value: Option<serde_json::Value>,
    /// New value after the change.
    pub new_value: serde_json::Value,
    /// Who made the change (user, system, compliance).
    pub changed_by: String,
    /// Reason for the change, if given.
    pub change_reason: Option<String>,
    /// When the change was recorded.
    pub changed_at: DateTime<Utc>,
    /// Hash-chain hash for tamper evidence.
    pub row_hash: String,
}

/// A single node state event from the node_state_history table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStateEvent {
    /// History row ID.
    pub id: i64,
    /// The node this event is about.
    pub node_id: Uuid,
    /// Type of event (e.g., "joined", "left", "suspect", "dead", "trait_change").
    pub event_type: String,
    /// Node state at the time of this event (JSON).
    pub state: serde_json::Value,
    /// Node traits at the time (if recorded).
    pub traits: Option<serde_json::Value>,
    /// When this state was valid from.
    pub valid_from: DateTime<Utc>,
    /// When this state was superseded (None = still current).
    pub valid_to: Option<DateTime<Utc>>,
}

/// A single job event from the job_history table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    /// History row ID.
    pub id: i64,
    /// The job this event belongs to.
    pub job_id: Uuid,
    /// Type of event (e.g., "submitted", "chunk_assigned", "chunk_completed",
    /// "completed", "failed", "cancelled").
    pub event_type: String,
    /// Event-specific data (JSON).
    pub event_data: serde_json::Value,
    /// Node involved (if applicable).
    pub node_id: Option<Uuid>,
    /// Chunk involved (if applicable).
    pub chunk_id: Option<Uuid>,
    /// When this event was recorded.
    pub recorded_at: DateTime<Utc>,
}

/// An audit trail entry from the audit_events table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Audit row ID.
    pub id: i64,
    /// Type of audit event (e.g., "config_change", "node_joined", "auth_attempt").
    pub event_type: String,
    /// Who initiated the action.
    pub actor: String,
    /// What was acted upon (node ID, config key, job ID, etc.).
    pub target: Option<String>,
    /// Classification of the target entity.
    pub target_type: Option<String>,
    /// Additional detail (JSON).
    pub detail: Option<serde_json::Value>,
    /// When the event was recorded.
    pub recorded_at: DateTime<Utc>,
    /// Hash-chain hash for tamper evidence.
    pub row_hash: String,
}

/// Full swarm snapshot at a specific point in time.
///
/// Reconstructed from temporal tables — shows the complete state of
/// the swarm as it was at the given timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmSnapshot {
    /// The timestamp this snapshot represents.
    pub timestamp: DateTime<Utc>,
    /// Config state at this moment (key → value).
    pub config: HashMap<String, serde_json::Value>,
    /// All known nodes and their states at this moment.
    pub nodes: Vec<NodeStateEvent>,
    /// Active jobs at this moment.
    pub active_jobs: Vec<ActiveJobSnapshot>,
    /// Total node count.
    pub node_count: usize,
    /// Total active job count.
    pub active_job_count: usize,
}

/// Summary of an active job at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveJobSnapshot {
    /// Job ID.
    pub job_id: Uuid,
    /// Most recent event type for this job.
    pub latest_event_type: String,
    /// Most recent event data.
    pub latest_event_data: serde_json::Value,
    /// When the most recent event occurred.
    pub latest_event_at: DateTime<Utc>,
}

/// Result of a temporal correlation query.
///
/// "What else was happening when X occurred?" — for root cause analysis
/// and forensic investigation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationResult {
    /// The event type that was queried.
    pub query_event_type: String,
    /// The center timestamp of the correlation window.
    pub query_timestamp: DateTime<Utc>,
    /// Window size (seconds before and after).
    pub window_secs: i64,
    /// Config changes within the window.
    pub config_changes: Vec<ConfigChange>,
    /// Node state events within the window.
    pub node_events: Vec<NodeStateEvent>,
    /// Job events within the window.
    pub job_events: Vec<JobEvent>,
    /// Audit events within the window.
    pub audit_events: Vec<AuditEntry>,
}

/// Query parameters for paginated time-range queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRangeQuery {
    /// Start of the range (inclusive). None means "from the beginning."
    pub from: Option<DateTime<Utc>>,
    /// End of the range (inclusive). None means "up to now."
    pub to: Option<DateTime<Utc>>,
    /// Max results to return (default 100, max 10000).
    pub limit: i64,
    /// Offset for pagination (default 0).
    pub offset: i64,
}

impl Default for TimeRangeQuery {
    fn default() -> Self {
        Self {
            from: None,
            to: None,
            limit: 100,
            offset: 0,
        }
    }
}

impl TimeRangeQuery {
    /// Clamp the limit to a reasonable range.
    pub fn clamped_limit(&self) -> i64 {
        self.limit.clamp(1, 10_000)
    }

    /// Clamp offset to non-negative.
    pub fn clamped_offset(&self) -> i64 {
        self.offset.max(0)
    }
}

/// Summary statistics for a time-travel query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelStats {
    /// Total config changes recorded.
    pub total_config_changes: i64,
    /// Total node state events recorded.
    pub total_node_events: i64,
    /// Total job events recorded.
    pub total_job_events: i64,
    /// Total audit events recorded.
    pub total_audit_events: i64,
    /// Earliest recorded event across all tables.
    pub earliest_event: Option<DateTime<Utc>>,
    /// Most recent recorded event across all tables.
    pub latest_event: Option<DateTime<Utc>>,
}

// ============================================================================
// TimeTravelEngine
// ============================================================================

/// Stateless query engine for temporal queries against the swarm PG database.
///
/// All methods take a `&tokio_postgres::Client` obtained from the read pool.
/// This design keeps the engine decoupled from connection management and makes
/// it easy to test.
pub struct TimeTravelEngine;

impl TimeTravelEngine {
    // ========================================================================
    // Config queries
    // ========================================================================

    /// Reconstruct the full config state at a specific point in time.
    ///
    /// Uses the most recent config snapshot before the timestamp as a base,
    /// then overlays any changes between the snapshot and the target time.
    pub async fn config_at(
        client: &tokio_postgres::Client,
        timestamp: DateTime<Utc>,
    ) -> Result<HashMap<String, serde_json::Value>, TimeTravelError> {
        // Get the most recent snapshot before the timestamp.
        let snapshot_row = client
            .query_opt(
                "SELECT config_json FROM config_snapshots
                 WHERE snapshot_at <= $1
                 ORDER BY snapshot_at DESC
                 LIMIT 1",
                &[&timestamp],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("config snapshot: {e}")))?;

        let mut result: HashMap<String, serde_json::Value> = if let Some(row) = snapshot_row {
            let full: serde_json::Value = row.get(0);
            if let Some(obj) = full.as_object() {
                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

        // Overlay: for each key, take the most recent value_json as of `timestamp`.
        let rows = client
            .query(
                "SELECT DISTINCT ON (key) key, value_json
                 FROM config_history
                 WHERE valid_from <= $1
                 ORDER BY key, valid_from DESC",
                &[&timestamp],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("config history overlay: {e}")))?;

        for row in &rows {
            let key: String = row.get(0);
            let value: serde_json::Value = row.get(1);
            result.insert(key, value);
        }

        debug!(
            timestamp = %timestamp,
            keys = result.len(),
            "Reconstructed config state"
        );
        Ok(result)
    }

    /// Get all config changes within a time range.
    pub async fn config_changes(
        client: &tokio_postgres::Client,
        query: &TimeRangeQuery,
    ) -> Result<Vec<ConfigChange>, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, key, prev_value, value_json, changed_by, change_reason,
                        valid_from, row_hash
                 FROM config_history
                 WHERE ($1::timestamptz IS NULL OR valid_from >= $1)
                   AND ($2::timestamptz IS NULL OR valid_from <= $2)
                 ORDER BY valid_from DESC
                 LIMIT $3 OFFSET $4",
                &[
                    &query.from,
                    &query.to,
                    &query.clamped_limit(),
                    &query.clamped_offset(),
                ],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("config changes: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| ConfigChange {
                id: row.get(0),
                key: row.get(1),
                old_value: row.get(2),
                new_value: row.get(3),
                changed_by: row.get(4),
                change_reason: row.get(5),
                changed_at: row.get(6),
                row_hash: row.get(7),
            })
            .collect())
    }

    // ========================================================================
    // Node queries
    // ========================================================================

    /// Get the full timeline of events for a specific node.
    pub async fn node_timeline(
        client: &tokio_postgres::Client,
        node_id: Uuid,
        query: &TimeRangeQuery,
    ) -> Result<Vec<NodeStateEvent>, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, node_id, event_type, state_json, traits,
                        valid_from, valid_to
                 FROM node_state_history
                 WHERE node_id = $1
                   AND ($2::timestamptz IS NULL OR valid_from >= $2)
                   AND ($3::timestamptz IS NULL OR valid_from <= $3)
                 ORDER BY valid_from DESC
                 LIMIT $4 OFFSET $5",
                &[
                    &node_id,
                    &query.from,
                    &query.to,
                    &query.clamped_limit(),
                    &query.clamped_offset(),
                ],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("node timeline: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| NodeStateEvent {
                id: row.get(0),
                node_id: row.get(1),
                event_type: row.get(2),
                state: row.get(3),
                traits: row.get(4),
                valid_from: row.get(5),
                valid_to: row.get(6),
            })
            .collect())
    }

    /// Get all node states as of a specific timestamp.
    ///
    /// Returns the most recent state for each node that existed at that time.
    pub async fn nodes_at(
        client: &tokio_postgres::Client,
        timestamp: DateTime<Utc>,
    ) -> Result<Vec<NodeStateEvent>, TimeTravelError> {
        // For each node, get the most recent event as of `timestamp`.
        let rows = client
            .query(
                "SELECT DISTINCT ON (node_id)
                        id, node_id, event_type, state_json, traits,
                        valid_from, valid_to
                 FROM node_state_history
                 WHERE valid_from <= $1
                   AND (valid_to IS NULL OR valid_to > $1)
                 ORDER BY node_id, valid_from DESC",
                &[&timestamp],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("nodes at: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| NodeStateEvent {
                id: row.get(0),
                node_id: row.get(1),
                event_type: row.get(2),
                state: row.get(3),
                traits: row.get(4),
                valid_from: row.get(5),
                valid_to: row.get(6),
            })
            .collect())
    }

    // ========================================================================
    // Job queries
    // ========================================================================

    /// Get the full lifecycle of a job — from submission to completion.
    pub async fn job_lifecycle(
        client: &tokio_postgres::Client,
        job_id: Uuid,
    ) -> Result<Vec<JobEvent>, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, job_id, event_type, event_data, node_id, chunk_id,
                        recorded_at
                 FROM job_history
                 WHERE job_id = $1
                 ORDER BY recorded_at ASC",
                &[&job_id],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("job lifecycle: {e}")))?;

        if rows.is_empty() {
            debug!(job_id = %job_id, "No events found for job");
        }

        Ok(rows
            .iter()
            .map(|row| JobEvent {
                id: row.get(0),
                job_id: row.get(1),
                event_type: row.get(2),
                event_data: row.get(3),
                node_id: row.get(4),
                chunk_id: row.get(5),
                recorded_at: row.get(6),
            })
            .collect())
    }

    /// Get active jobs at a specific timestamp.
    ///
    /// An "active" job is one that has events recorded before the timestamp
    /// but no terminal event (completed, failed, cancelled) at or before it.
    pub async fn active_jobs_at(
        client: &tokio_postgres::Client,
        timestamp: DateTime<Utc>,
    ) -> Result<Vec<ActiveJobSnapshot>, TimeTravelError> {
        // Get the most recent event for each job at or before the timestamp,
        // excluding jobs that have a terminal event.
        let rows = client
            .query(
                "WITH latest_events AS (
                    SELECT DISTINCT ON (job_id)
                           job_id, event_type, event_data, recorded_at
                    FROM job_history
                    WHERE recorded_at <= $1
                    ORDER BY job_id, recorded_at DESC
                 )
                 SELECT job_id, event_type, event_data, recorded_at
                 FROM latest_events
                 WHERE event_type NOT IN ('completed', 'failed', 'cancelled')",
                &[&timestamp],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("active jobs at: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| ActiveJobSnapshot {
                job_id: row.get(0),
                latest_event_type: row.get(1),
                latest_event_data: row.get(2),
                latest_event_at: row.get(3),
            })
            .collect())
    }

    // ========================================================================
    // Swarm snapshot
    // ========================================================================

    /// Reconstruct a full swarm snapshot at a specific timestamp.
    ///
    /// Combines config state, node states, and active jobs into a single
    /// coherent view of the swarm as it was at that moment.
    pub async fn swarm_snapshot_at(
        client: &tokio_postgres::Client,
        timestamp: DateTime<Utc>,
    ) -> Result<SwarmSnapshot, TimeTravelError> {
        let config = Self::config_at(client, timestamp).await?;
        let nodes = Self::nodes_at(client, timestamp).await?;
        let active_jobs = Self::active_jobs_at(client, timestamp).await?;

        let node_count = nodes.len();
        let active_job_count = active_jobs.len();

        Ok(SwarmSnapshot {
            timestamp,
            config,
            nodes,
            active_jobs,
            node_count,
            active_job_count,
        })
    }

    // ========================================================================
    // Audit queries
    // ========================================================================

    /// Get audit trail entries for a specific target within a time range.
    ///
    /// If `target` is None, returns all audit events in the range.
    pub async fn audit_trail(
        client: &tokio_postgres::Client,
        target: Option<&str>,
        query: &TimeRangeQuery,
    ) -> Result<Vec<AuditEntry>, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, event_type, actor, target, target_type,
                        detail_json, recorded_at, row_hash
                 FROM audit_events
                 WHERE ($1::text IS NULL OR target = $1)
                   AND ($2::timestamptz IS NULL OR recorded_at >= $2)
                   AND ($3::timestamptz IS NULL OR recorded_at <= $3)
                 ORDER BY recorded_at DESC
                 LIMIT $4 OFFSET $5",
                &[
                    &target,
                    &query.from,
                    &query.to,
                    &query.clamped_limit(),
                    &query.clamped_offset(),
                ],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("audit trail: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| AuditEntry {
                id: row.get(0),
                event_type: row.get(1),
                actor: row.get(2),
                target: row.get(3),
                target_type: row.get(4),
                detail: row.get(5),
                recorded_at: row.get(6),
                row_hash: row.get(7),
            })
            .collect())
    }

    /// Get audit entries by event type.
    pub async fn audit_by_event_type(
        client: &tokio_postgres::Client,
        event_type: &str,
        query: &TimeRangeQuery,
    ) -> Result<Vec<AuditEntry>, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, event_type, actor, target, target_type,
                        detail_json, recorded_at, row_hash
                 FROM audit_events
                 WHERE event_type = $1
                   AND ($2::timestamptz IS NULL OR recorded_at >= $2)
                   AND ($3::timestamptz IS NULL OR recorded_at <= $3)
                 ORDER BY recorded_at DESC
                 LIMIT $4 OFFSET $5",
                &[
                    &event_type,
                    &query.from,
                    &query.to,
                    &query.clamped_limit(),
                    &query.clamped_offset(),
                ],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("audit by type: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| AuditEntry {
                id: row.get(0),
                event_type: row.get(1),
                actor: row.get(2),
                target: row.get(3),
                target_type: row.get(4),
                detail: row.get(5),
                recorded_at: row.get(6),
                row_hash: row.get(7),
            })
            .collect())
    }

    // ========================================================================
    // Correlation
    // ========================================================================

    /// Correlate an event with what else was happening in the same time window.
    ///
    /// Given an event type and timestamp, returns all config changes, node
    /// events, job events, and audit entries within ±window of that timestamp.
    ///
    /// This is the forensic "what happened around here?" query — invaluable
    /// for root cause analysis.
    pub async fn correlate_with_event(
        client: &tokio_postgres::Client,
        event_type: &str,
        timestamp: DateTime<Utc>,
        window: StdDuration,
    ) -> Result<CorrelationResult, TimeTravelError> {
        let window_secs = window.as_secs() as i64;
        let from = timestamp - Duration::seconds(window_secs);
        let to = timestamp + Duration::seconds(window_secs);

        let range = TimeRangeQuery {
            from: Some(from),
            to: Some(to),
            limit: 500,
            offset: 0,
        };

        // Run all four queries. We could parallelize these with tokio::join!
        // but that would require multiple clients. Sequential is fine for now.
        let config_changes = Self::config_changes(client, &range).await?;

        let node_events = Self::node_events_in_range(client, &range).await?;

        let job_events = Self::job_events_in_range(client, &range).await?;

        let audit_events = Self::audit_by_event_type(client, event_type, &range).await?;

        debug!(
            event_type = event_type,
            timestamp = %timestamp,
            window_secs = window_secs,
            config_changes = config_changes.len(),
            node_events = node_events.len(),
            job_events = job_events.len(),
            audit_events = audit_events.len(),
            "Correlation query complete"
        );

        Ok(CorrelationResult {
            query_event_type: event_type.to_string(),
            query_timestamp: timestamp,
            window_secs,
            config_changes,
            node_events,
            job_events,
            audit_events,
        })
    }

    // ========================================================================
    // Range queries (internal helpers)
    // ========================================================================

    /// Get all node state events in a time range (across all nodes).
    async fn node_events_in_range(
        client: &tokio_postgres::Client,
        query: &TimeRangeQuery,
    ) -> Result<Vec<NodeStateEvent>, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, node_id, event_type, state_json, traits,
                        valid_from, valid_to
                 FROM node_state_history
                 WHERE ($1::timestamptz IS NULL OR valid_from >= $1)
                   AND ($2::timestamptz IS NULL OR valid_from <= $2)
                 ORDER BY valid_from DESC
                 LIMIT $3 OFFSET $4",
                &[
                    &query.from,
                    &query.to,
                    &query.clamped_limit(),
                    &query.clamped_offset(),
                ],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("node events range: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| NodeStateEvent {
                id: row.get(0),
                node_id: row.get(1),
                event_type: row.get(2),
                state: row.get(3),
                traits: row.get(4),
                valid_from: row.get(5),
                valid_to: row.get(6),
            })
            .collect())
    }

    /// Get all job events in a time range (across all jobs).
    async fn job_events_in_range(
        client: &tokio_postgres::Client,
        query: &TimeRangeQuery,
    ) -> Result<Vec<JobEvent>, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, job_id, event_type, event_data, node_id, chunk_id,
                        recorded_at
                 FROM job_history
                 WHERE ($1::timestamptz IS NULL OR recorded_at >= $1)
                   AND ($2::timestamptz IS NULL OR recorded_at <= $2)
                 ORDER BY recorded_at DESC
                 LIMIT $3 OFFSET $4",
                &[
                    &query.from,
                    &query.to,
                    &query.clamped_limit(),
                    &query.clamped_offset(),
                ],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("job events range: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| JobEvent {
                id: row.get(0),
                job_id: row.get(1),
                event_type: row.get(2),
                event_data: row.get(3),
                node_id: row.get(4),
                chunk_id: row.get(5),
                recorded_at: row.get(6),
            })
            .collect())
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get summary statistics about the time-travel data.
    pub async fn stats(
        client: &tokio_postgres::Client,
    ) -> Result<TimeTravelStats, TimeTravelError> {
        let config_count: i64 = client
            .query_one("SELECT COUNT(*) FROM config_history", &[])
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("config count: {e}")))?
            .get(0);

        let node_count: i64 = client
            .query_one("SELECT COUNT(*) FROM node_state_history", &[])
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("node count: {e}")))?
            .get(0);

        let job_count: i64 = client
            .query_one("SELECT COUNT(*) FROM job_history", &[])
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("job count: {e}")))?
            .get(0);

        let audit_count: i64 = client
            .query_one("SELECT COUNT(*) FROM audit_events", &[])
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("audit count: {e}")))?
            .get(0);

        // Find the earliest and latest events across all tables.
        let earliest = Self::earliest_event(client).await?;
        let latest = Self::latest_event(client).await?;

        Ok(TimeTravelStats {
            total_config_changes: config_count,
            total_node_events: node_count,
            total_job_events: job_count,
            total_audit_events: audit_count,
            earliest_event: earliest,
            latest_event: latest,
        })
    }

    /// Find the earliest recorded event across all temporal tables.
    async fn earliest_event(
        client: &tokio_postgres::Client,
    ) -> Result<Option<DateTime<Utc>>, TimeTravelError> {
        let row = client
            .query_one(
                "SELECT MIN(ts) FROM (
                    SELECT MIN(valid_from) as ts FROM config_history
                    UNION ALL
                    SELECT MIN(valid_from) FROM node_state_history
                    UNION ALL
                    SELECT MIN(recorded_at) FROM job_history
                    UNION ALL
                    SELECT MIN(recorded_at) FROM audit_events
                 ) sub",
                &[],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("earliest event: {e}")))?;

        Ok(row.get(0))
    }

    /// Find the most recent recorded event across all temporal tables.
    async fn latest_event(
        client: &tokio_postgres::Client,
    ) -> Result<Option<DateTime<Utc>>, TimeTravelError> {
        let row = client
            .query_one(
                "SELECT MAX(ts) FROM (
                    SELECT MAX(valid_from) as ts FROM config_history
                    UNION ALL
                    SELECT MAX(valid_from) FROM node_state_history
                    UNION ALL
                    SELECT MAX(recorded_at) FROM job_history
                    UNION ALL
                    SELECT MAX(recorded_at) FROM audit_events
                 ) sub",
                &[],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("latest event: {e}")))?;

        Ok(row.get(0))
    }

    // ========================================================================
    // Hash chain verification
    // ========================================================================

    /// Verify the hash chain integrity of the config_history table.
    ///
    /// Reads all rows in order and recomputes each hash, checking that
    /// it matches the stored `row_hash` and that `prev_hash` references
    /// the previous row correctly.
    ///
    /// Returns the number of verified rows, or an error describing the
    /// first broken link.
    pub async fn verify_config_hash_chain(
        client: &tokio_postgres::Client,
    ) -> Result<HashChainVerification, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, key, value_json, prev_value, changed_by,
                        change_reason, valid_from, prev_hash, row_hash
                 FROM config_history
                 ORDER BY id ASC",
                &[],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("hash chain verify: {e}")))?;

        let mut prev_hash: Option<String> = None;
        let mut verified = 0u64;

        for row in &rows {
            let id: i64 = row.get(0);
            let stored_prev: Option<String> = row.get(7);
            let stored_hash: String = row.get(8);

            // Check prev_hash linkage.
            match (&prev_hash, &stored_prev) {
                (None, None) | (None, Some(_)) => {
                    // First row — prev_hash can be None or a genesis value.
                }
                (Some(expected), Some(actual)) => {
                    if expected != actual {
                        return Ok(HashChainVerification {
                            verified_rows: verified,
                            valid: false,
                            broken_at_id: Some(id),
                            error: Some(format!(
                                "Row {id}: prev_hash mismatch (expected {expected}, got {actual})"
                            )),
                        });
                    }
                }
                (Some(expected), None) => {
                    return Ok(HashChainVerification {
                        verified_rows: verified,
                        valid: false,
                        broken_at_id: Some(id),
                        error: Some(format!(
                            "Row {id}: prev_hash is NULL but expected {expected}"
                        )),
                    });
                }
            }

            prev_hash = Some(stored_hash);
            verified += 1;
        }

        Ok(HashChainVerification {
            verified_rows: verified,
            valid: true,
            broken_at_id: None,
            error: None,
        })
    }

    /// Verify the hash chain integrity of the audit_events table.
    pub async fn verify_audit_hash_chain(
        client: &tokio_postgres::Client,
    ) -> Result<HashChainVerification, TimeTravelError> {
        let rows = client
            .query(
                "SELECT id, prev_hash, row_hash FROM audit_events ORDER BY id ASC",
                &[],
            )
            .await
            .map_err(|e| TimeTravelError::QueryFailed(format!("audit hash verify: {e}")))?;

        let mut prev_hash: Option<String> = None;
        let mut verified = 0u64;

        for row in &rows {
            let id: i64 = row.get(0);
            let stored_prev: Option<String> = row.get(1);
            let stored_hash: String = row.get(2);

            match (&prev_hash, &stored_prev) {
                (None, _) => {}
                (Some(expected), Some(actual)) if expected != actual => {
                    return Ok(HashChainVerification {
                        verified_rows: verified,
                        valid: false,
                        broken_at_id: Some(id),
                        error: Some(format!(
                            "Audit row {id}: prev_hash mismatch"
                        )),
                    });
                }
                (Some(expected), None) => {
                    return Ok(HashChainVerification {
                        verified_rows: verified,
                        valid: false,
                        broken_at_id: Some(id),
                        error: Some(format!(
                            "Audit row {id}: prev_hash is NULL but expected {expected}"
                        )),
                    });
                }
                _ => {}
            }

            prev_hash = Some(stored_hash);
            verified += 1;
        }

        Ok(HashChainVerification {
            verified_rows: verified,
            valid: true,
            broken_at_id: None,
            error: None,
        })
    }
}

/// Result of a hash chain verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashChainVerification {
    /// Number of rows verified before the first error (or all rows if valid).
    pub verified_rows: u64,
    /// Whether the entire chain is valid.
    pub valid: bool,
    /// The row ID where the chain first broke (if invalid).
    pub broken_at_id: Option<i64>,
    /// Description of the first error found (if any).
    pub error: Option<String>,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors from the time-travel query engine.
#[derive(Debug, thiserror::Error)]
pub enum TimeTravelError {
    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("Invalid UUID: {0}")]
    InvalidUuid(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // ConfigChange tests
    // ========================================================================

    #[test]
    fn test_config_change_serde() {
        let change = ConfigChange {
            id: 1,
            key: "gossip_fanout".to_string(),
            old_value: Some(serde_json::json!(3)),
            new_value: serde_json::json!(5),
            changed_by: "admin".to_string(),
            change_reason: Some("Increase gossip spread".to_string()),
            changed_at: Utc::now(),
            row_hash: "abc123def456".to_string(),
        };
        let json = serde_json::to_string(&change).unwrap();
        let back: ConfigChange = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key, "gossip_fanout");
        assert_eq!(back.new_value, serde_json::json!(5));
        assert_eq!(back.old_value, Some(serde_json::json!(3)));
    }

    #[test]
    fn test_config_change_no_old_value() {
        let change = ConfigChange {
            id: 1,
            key: "new_setting".to_string(),
            old_value: None,
            new_value: serde_json::json!(true),
            changed_by: "system".to_string(),
            change_reason: None,
            changed_at: Utc::now(),
            row_hash: "hash".to_string(),
        };
        let json = serde_json::to_string(&change).unwrap();
        let back: ConfigChange = serde_json::from_str(&json).unwrap();
        assert!(back.old_value.is_none());
        assert!(back.change_reason.is_none());
    }

    // ========================================================================
    // NodeStateEvent tests
    // ========================================================================

    #[test]
    fn test_node_state_event_serde() {
        let event = NodeStateEvent {
            id: 42,
            node_id: Uuid::new_v4(),
            event_type: "joined".to_string(),
            state: serde_json::json!({"status": "alive", "addr": "10.0.0.1:7001"}),
            traits: Some(serde_json::json!(["Compute", "Storage"])),
            valid_from: Utc::now(),
            valid_to: None,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: NodeStateEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.event_type, "joined");
        assert!(back.valid_to.is_none());
    }

    #[test]
    fn test_node_state_event_with_valid_to() {
        let now = Utc::now();
        let event = NodeStateEvent {
            id: 1,
            node_id: Uuid::new_v4(),
            event_type: "dead".to_string(),
            state: serde_json::json!({}),
            traits: None,
            valid_from: now - Duration::hours(1),
            valid_to: Some(now),
        };
        assert!(event.valid_to.is_some());
        assert!(event.valid_to.unwrap() > event.valid_from);
    }

    // ========================================================================
    // JobEvent tests
    // ========================================================================

    #[test]
    fn test_job_event_serde() {
        let event = JobEvent {
            id: 100,
            job_id: Uuid::new_v4(),
            event_type: "chunk_assigned".to_string(),
            event_data: serde_json::json!({"chunk_idx": 3, "worker": "node-2"}),
            node_id: Some(Uuid::new_v4()),
            chunk_id: Some(Uuid::new_v4()),
            recorded_at: Utc::now(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: JobEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.event_type, "chunk_assigned");
        assert!(back.node_id.is_some());
    }

    #[test]
    fn test_job_event_no_node_or_chunk() {
        let event = JobEvent {
            id: 1,
            job_id: Uuid::new_v4(),
            event_type: "submitted".to_string(),
            event_data: serde_json::json!({"script": "echo hello"}),
            node_id: None,
            chunk_id: None,
            recorded_at: Utc::now(),
        };
        assert!(event.node_id.is_none());
        assert!(event.chunk_id.is_none());
    }

    // ========================================================================
    // AuditEntry tests
    // ========================================================================

    #[test]
    fn test_audit_entry_serde() {
        let entry = AuditEntry {
            id: 200,
            event_type: "config_change".to_string(),
            actor: "admin@example.com".to_string(),
            target: Some("gossip_fanout".to_string()),
            target_type: Some("config_key".to_string()),
            detail: Some(serde_json::json!({"old": 3, "new": 5})),
            recorded_at: Utc::now(),
            row_hash: "audit_hash_123".to_string(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: AuditEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.event_type, "config_change");
        assert_eq!(back.actor, "admin@example.com");
    }

    #[test]
    fn test_audit_entry_minimal() {
        let entry = AuditEntry {
            id: 1,
            event_type: "node_joined".to_string(),
            actor: "system".to_string(),
            target: None,
            target_type: None,
            detail: None,
            recorded_at: Utc::now(),
            row_hash: "hash".to_string(),
        };
        assert!(entry.target.is_none());
        assert!(entry.detail.is_none());
    }

    // ========================================================================
    // SwarmSnapshot tests
    // ========================================================================

    #[test]
    fn test_swarm_snapshot_serde() {
        let snapshot = SwarmSnapshot {
            timestamp: Utc::now(),
            config: {
                let mut m = HashMap::new();
                m.insert("gossip_fanout".to_string(), serde_json::json!(3));
                m.insert("enable_neuromancer".to_string(), serde_json::json!(false));
                m
            },
            nodes: vec![],
            active_jobs: vec![],
            node_count: 0,
            active_job_count: 0,
        };
        let json = serde_json::to_string(&snapshot).unwrap();
        let back: SwarmSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.config.len(), 2);
        assert_eq!(back.node_count, 0);
    }

    #[test]
    fn test_swarm_snapshot_with_nodes_and_jobs() {
        let snapshot = SwarmSnapshot {
            timestamp: Utc::now(),
            config: HashMap::new(),
            nodes: vec![NodeStateEvent {
                id: 1,
                node_id: Uuid::new_v4(),
                event_type: "alive".to_string(),
                state: serde_json::json!({}),
                traits: None,
                valid_from: Utc::now(),
                valid_to: None,
            }],
            active_jobs: vec![ActiveJobSnapshot {
                job_id: Uuid::new_v4(),
                latest_event_type: "running".to_string(),
                latest_event_data: serde_json::json!({}),
                latest_event_at: Utc::now(),
            }],
            node_count: 1,
            active_job_count: 1,
        };
        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(snapshot.active_jobs.len(), 1);
    }

    // ========================================================================
    // ActiveJobSnapshot tests
    // ========================================================================

    #[test]
    fn test_active_job_snapshot_serde() {
        let snap = ActiveJobSnapshot {
            job_id: Uuid::new_v4(),
            latest_event_type: "chunk_completed".to_string(),
            latest_event_data: serde_json::json!({"chunk_idx": 5, "result": "ok"}),
            latest_event_at: Utc::now(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: ActiveJobSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.latest_event_type, "chunk_completed");
    }

    // ========================================================================
    // CorrelationResult tests
    // ========================================================================

    #[test]
    fn test_correlation_result_serde() {
        let result = CorrelationResult {
            query_event_type: "node_death".to_string(),
            query_timestamp: Utc::now(),
            window_secs: 60,
            config_changes: vec![],
            node_events: vec![],
            job_events: vec![],
            audit_events: vec![],
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: CorrelationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.query_event_type, "node_death");
        assert_eq!(back.window_secs, 60);
    }

    #[test]
    fn test_correlation_result_with_data() {
        let result = CorrelationResult {
            query_event_type: "failover".to_string(),
            query_timestamp: Utc::now(),
            window_secs: 300,
            config_changes: vec![ConfigChange {
                id: 1,
                key: "max_load".to_string(),
                old_value: None,
                new_value: serde_json::json!(0.9),
                changed_by: "system".to_string(),
                change_reason: None,
                changed_at: Utc::now(),
                row_hash: "h".to_string(),
            }],
            node_events: vec![NodeStateEvent {
                id: 1,
                node_id: Uuid::new_v4(),
                event_type: "dead".to_string(),
                state: serde_json::json!({}),
                traits: None,
                valid_from: Utc::now(),
                valid_to: None,
            }],
            job_events: vec![],
            audit_events: vec![],
        };
        assert_eq!(result.config_changes.len(), 1);
        assert_eq!(result.node_events.len(), 1);
    }

    // ========================================================================
    // TimeRangeQuery tests
    // ========================================================================

    #[test]
    fn test_time_range_query_default() {
        let q = TimeRangeQuery::default();
        assert!(q.from.is_none());
        assert!(q.to.is_none());
        assert_eq!(q.limit, 100);
        assert_eq!(q.offset, 0);
    }

    #[test]
    fn test_time_range_query_clamped_limit() {
        let q = TimeRangeQuery {
            limit: 50_000,
            ..Default::default()
        };
        assert_eq!(q.clamped_limit(), 10_000);

        let q2 = TimeRangeQuery {
            limit: -5,
            ..Default::default()
        };
        assert_eq!(q2.clamped_limit(), 1);

        let q3 = TimeRangeQuery {
            limit: 500,
            ..Default::default()
        };
        assert_eq!(q3.clamped_limit(), 500);
    }

    #[test]
    fn test_time_range_query_clamped_offset() {
        let q = TimeRangeQuery {
            offset: -10,
            ..Default::default()
        };
        assert_eq!(q.clamped_offset(), 0);

        let q2 = TimeRangeQuery {
            offset: 50,
            ..Default::default()
        };
        assert_eq!(q2.clamped_offset(), 50);
    }

    #[test]
    fn test_time_range_query_serde() {
        let q = TimeRangeQuery {
            from: Some(Utc::now()),
            to: Some(Utc::now()),
            limit: 50,
            offset: 10,
        };
        let json = serde_json::to_string(&q).unwrap();
        let back: TimeRangeQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(back.limit, 50);
        assert_eq!(back.offset, 10);
    }

    // ========================================================================
    // TimeTravelStats tests
    // ========================================================================

    #[test]
    fn test_time_travel_stats_serde() {
        let stats = TimeTravelStats {
            total_config_changes: 42,
            total_node_events: 100,
            total_job_events: 200,
            total_audit_events: 500,
            earliest_event: Some(Utc::now() - Duration::days(30)),
            latest_event: Some(Utc::now()),
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: TimeTravelStats = serde_json::from_str(&json).unwrap();
        assert_eq!(back.total_config_changes, 42);
        assert_eq!(back.total_audit_events, 500);
    }

    #[test]
    fn test_time_travel_stats_empty() {
        let stats = TimeTravelStats {
            total_config_changes: 0,
            total_node_events: 0,
            total_job_events: 0,
            total_audit_events: 0,
            earliest_event: None,
            latest_event: None,
        };
        assert!(stats.earliest_event.is_none());
        assert!(stats.latest_event.is_none());
    }

    // ========================================================================
    // HashChainVerification tests
    // ========================================================================

    #[test]
    fn test_hash_chain_verification_valid() {
        let v = HashChainVerification {
            verified_rows: 1000,
            valid: true,
            broken_at_id: None,
            error: None,
        };
        let json = serde_json::to_string(&v).unwrap();
        let back: HashChainVerification = serde_json::from_str(&json).unwrap();
        assert!(back.valid);
        assert_eq!(back.verified_rows, 1000);
    }

    #[test]
    fn test_hash_chain_verification_broken() {
        let v = HashChainVerification {
            verified_rows: 42,
            valid: false,
            broken_at_id: Some(43),
            error: Some("prev_hash mismatch".to_string()),
        };
        assert!(!v.valid);
        assert_eq!(v.broken_at_id, Some(43));
    }

    // ========================================================================
    // TimeTravelError tests
    // ========================================================================

    #[test]
    fn test_time_travel_error_display() {
        let e = TimeTravelError::QueryFailed("connection refused".to_string());
        assert_eq!(e.to_string(), "Query failed: connection refused");

        let e2 = TimeTravelError::InvalidTimestamp("not a date".to_string());
        assert_eq!(e2.to_string(), "Invalid timestamp: not a date");

        let e3 = TimeTravelError::InvalidUuid("xyz".to_string());
        assert_eq!(e3.to_string(), "Invalid UUID: xyz");
    }
}
