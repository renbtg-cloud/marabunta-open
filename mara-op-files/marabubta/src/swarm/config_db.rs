// Marabunta - Licensed under the MIT License.
//! PostgreSQL persistence for configuration state.
//!
//! Provides query helpers for:
//! - Recording config changes (history + current value)
//! - Point-in-time config reconstruction
//! - Config snapshot storage with hash chains
//!
//! These functions operate on the tables created by the schema migration
//! in `postgres/schema.rs` (tables: config_history, config_current,
//! config_snapshots).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::postgres::schema::compute_row_hash;

// ============================================================================
// Types
// ============================================================================

/// A single row from config_history — records one key change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigHistoryEntry {
    pub id: i64,
    pub key: String,
    pub old_value: Option<serde_json::Value>,
    pub new_value: serde_json::Value,
    pub changed_by: String,
    pub source: String,
    pub changed_at: DateTime<Utc>,
    pub row_hash: String,
}

/// A single row from config_current — the latest value for a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigCurrentEntry {
    pub key: String,
    pub value: serde_json::Value,
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

/// A config snapshot — full SwarmConfig JSON at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub id: i64,
    pub full_config: serde_json::Value,
    pub triggered_by: String,
    pub changed_keys: Vec<String>,
    pub node_id: String,
    pub snapshot_at: DateTime<Utc>,
    pub row_hash: String,
}

// ============================================================================
// ConfigDb query helpers
// ============================================================================

/// PostgreSQL query helpers for config persistence.
///
/// All methods are stateless and take a client reference. The actual pool
/// management lives in `PgPool`.
pub struct ConfigDb;

impl ConfigDb {
    /// Record a config key change in the history table.
    ///
    /// Computes a hash chain: each row's hash includes the previous row's hash.
    pub async fn insert_history(
        client: &tokio_postgres::Client,
        key: &str,
        old_value: Option<&serde_json::Value>,
        new_value: &serde_json::Value,
        changed_by: &str,
        source: &str,
    ) -> Result<i64, tokio_postgres::Error> {
        // Get the hash of the most recent row for the chain.
        let prev_hash = Self::last_history_hash(client).await?;
        let now = Utc::now();

        // Compute row hash: key + old + new + changed_by + source + timestamp + prev
        let hash_fields = [key.to_string(),
            old_value.map(|v| v.to_string()).unwrap_or_default(),
            new_value.to_string(),
            changed_by.to_string(),
            source.to_string(),
            now.to_rfc3339()];
        let field_refs: Vec<&str> = hash_fields.iter().map(|s| s.as_str()).collect();
        let row_hash = compute_row_hash(Some(prev_hash.as_str()), &field_refs);

        let old_json = old_value.map(|v| serde_json::to_string(v).unwrap_or_default());

        let row = client.query_one(
            "INSERT INTO config_history (key, old_value, new_value, changed_by, source, changed_at, prev_hash, row_hash)
             VALUES ($1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8)
             RETURNING id",
            &[
                &key,
                &old_json.as_deref().and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
                &new_value,
                &changed_by,
                &source,
                &now,
                &prev_hash,
                &row_hash,
            ],
        ).await?;

        let id: i64 = row.get(0);
        debug!(key = key, id = id, "Recorded config change in history");
        Ok(id)
    }

    /// Upsert the current value for a config key.
    pub async fn upsert_current(
        client: &tokio_postgres::Client,
        key: &str,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<(), tokio_postgres::Error> {
        client.execute(
            "INSERT INTO config_current (key, value, updated_by, updated_at)
             VALUES ($1, $2::jsonb, $3, $4)
             ON CONFLICT (key) DO UPDATE SET value = $2::jsonb, updated_by = $3, updated_at = $4",
            &[&key, &value, &updated_by, &Utc::now()],
        ).await?;
        Ok(())
    }

    /// Get the change history for a config key within a time range.
    pub async fn get_history(
        client: &tokio_postgres::Client,
        key: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<ConfigHistoryEntry>, tokio_postgres::Error> {
        let rows = client.query(
            "SELECT id, key, old_value, new_value, changed_by, source, changed_at, row_hash
             FROM config_history
             WHERE ($1::text IS NULL OR key = $1)
               AND ($2::timestamptz IS NULL OR changed_at >= $2)
               AND ($3::timestamptz IS NULL OR changed_at <= $3)
             ORDER BY changed_at DESC
             LIMIT $4",
            &[&key, &from, &to, &limit],
        ).await?;

        Ok(rows.iter().map(|row| ConfigHistoryEntry {
            id: row.get(0),
            key: row.get(1),
            old_value: row.get(2),
            new_value: row.get(3),
            changed_by: row.get(4),
            source: row.get(5),
            changed_at: row.get(6),
            row_hash: row.get(7),
        }).collect())
    }

    /// Reconstruct the config state at a specific point in time.
    ///
    /// Returns a map of key → value as of the given timestamp by replaying
    /// the latest config_current values that existed at that time, overlaid
    /// with history entries up to that timestamp.
    pub async fn get_config_at(
        client: &tokio_postgres::Client,
        timestamp: DateTime<Utc>,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, tokio_postgres::Error> {
        // Use the most recent snapshot before the timestamp as a base,
        // then overlay any changes between the snapshot and the target time.
        //
        // If no snapshot exists, start from an empty map and replay all history.
        let snapshot_row = client.query_opt(
            "SELECT full_config FROM config_snapshots
             WHERE snapshot_at <= $1
             ORDER BY snapshot_at DESC
             LIMIT 1",
            &[&timestamp],
        ).await?;

        let mut result: std::collections::HashMap<String, serde_json::Value> =
            if let Some(row) = snapshot_row {
                let full: serde_json::Value = row.get(0);
                if let Some(obj) = full.as_object() {
                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                } else {
                    std::collections::HashMap::new()
                }
            } else {
                std::collections::HashMap::new()
            };

        // Overlay history entries up to the timestamp.
        // We take the LAST value for each key (DISTINCT ON key, ordered by changed_at DESC).
        let rows = client.query(
            "SELECT DISTINCT ON (key) key, new_value
             FROM config_history
             WHERE changed_at <= $1
             ORDER BY key, changed_at DESC",
            &[&timestamp],
        ).await?;

        for row in &rows {
            let key: String = row.get(0);
            let value: serde_json::Value = row.get(1);
            result.insert(key, value);
        }

        Ok(result)
    }

    /// Get all current runtime overrides (Tier 3 values).
    pub async fn get_current_overrides(
        client: &tokio_postgres::Client,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, tokio_postgres::Error> {
        let rows = client.query(
            "SELECT key, value FROM config_current ORDER BY key",
            &[],
        ).await?;

        Ok(rows.iter().map(|row| {
            let key: String = row.get(0);
            let value: serde_json::Value = row.get(1);
            (key, value)
        }).collect())
    }

    /// Insert a full config snapshot (taken after each change batch).
    pub async fn insert_snapshot(
        client: &tokio_postgres::Client,
        full_config: &serde_json::Value,
        triggered_by: &str,
        changed_keys: &[String],
        node_id: &str,
    ) -> Result<i64, tokio_postgres::Error> {
        let prev_hash = Self::last_snapshot_hash(client).await?;
        let now = Utc::now();

        let hash_fields = [full_config.to_string(),
            triggered_by.to_string(),
            changed_keys.join(","),
            node_id.to_string(),
            now.to_rfc3339()];
        let field_refs: Vec<&str> = hash_fields.iter().map(|s| s.as_str()).collect();
        let row_hash = compute_row_hash(Some(prev_hash.as_str()), &field_refs);
        let changed_keys_json = serde_json::to_value(changed_keys).unwrap_or_default();

        let row = client.query_one(
            "INSERT INTO config_snapshots (full_config, triggered_by, changed_keys, node_id, snapshot_at, prev_hash, row_hash)
             VALUES ($1::jsonb, $2, $3::jsonb, $4, $5, $6, $7)
             RETURNING id",
            &[
                &full_config,
                &triggered_by,
                &changed_keys_json,
                &node_id,
                &now,
                &prev_hash,
                &row_hash,
            ],
        ).await?;

        let id: i64 = row.get(0);
        debug!(id = id, triggered_by = triggered_by, "Inserted config snapshot");
        Ok(id)
    }

    // ========================================================================
    // Private helpers
    // ========================================================================

    /// Get the hash of the most recent config_history row (for hash chain).
    async fn last_history_hash(
        client: &tokio_postgres::Client,
    ) -> Result<String, tokio_postgres::Error> {
        let row = client.query_opt(
            "SELECT row_hash FROM config_history ORDER BY id DESC LIMIT 1",
            &[],
        ).await?;
        Ok(row.map(|r| r.get::<_, String>(0)).unwrap_or_else(|| "genesis".to_string()))
    }

    /// Get the hash of the most recent config_snapshots row (for hash chain).
    async fn last_snapshot_hash(
        client: &tokio_postgres::Client,
    ) -> Result<String, tokio_postgres::Error> {
        let row = client.query_opt(
            "SELECT row_hash FROM config_snapshots ORDER BY id DESC LIMIT 1",
            &[],
        ).await?;
        Ok(row.map(|r| r.get::<_, String>(0)).unwrap_or_else(|| "genesis".to_string()))
    }

    // ========================================================================
    // Consent record queries
    // ========================================================================

    /// Insert a consent record into the consent_records table.
    pub async fn insert_consent(
        client: &tokio_postgres::Client,
        node_id: &str,
        manifest_hash: &str,
        accepted: bool,
        rejection_reason: Option<&str>,
        auto_accepted: bool,
    ) -> Result<i64, tokio_postgres::Error> {
        let row = client
            .query_one(
                "INSERT INTO consent_records (node_id, manifest_hash, accepted, rejection_reason, auto_accepted, recorded_at)
                 VALUES ($1, $2, $3, $4, $5, now())
                 RETURNING id",
                &[&node_id, &manifest_hash, &accepted, &rejection_reason, &auto_accepted],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Get the most recent consent record for a node.
    pub async fn get_consent_for_node(
        client: &tokio_postgres::Client,
        node_id: &str,
    ) -> Result<Option<super::config_consent::ConsentRecord>, tokio_postgres::Error> {
        let row = client
            .query_opt(
                "SELECT id, node_id, manifest_hash, accepted, rejection_reason, auto_accepted, recorded_at
                 FROM consent_records
                 WHERE node_id = $1
                 ORDER BY recorded_at DESC
                 LIMIT 1",
                &[&node_id],
            )
            .await?;

        Ok(row.map(|r| super::config_consent::ConsentRecord {
            id: r.get(0),
            node_id: r.get(1),
            manifest_hash: r.get(2),
            accepted: r.get(3),
            rejection_reason: r.get(4),
            auto_accepted: r.get(5),
            recorded_at: r.get(6),
        }))
    }

    /// List consent records with pagination.
    pub async fn list_consents(
        client: &tokio_postgres::Client,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<super::config_consent::ConsentRecord>, tokio_postgres::Error> {
        let rows = client
            .query(
                "SELECT id, node_id, manifest_hash, accepted, rejection_reason, auto_accepted, recorded_at
                 FROM consent_records
                 ORDER BY recorded_at DESC
                 LIMIT $1 OFFSET $2",
                &[&limit, &offset],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|r| super::config_consent::ConsentRecord {
                id: r.get(0),
                node_id: r.get(1),
                manifest_hash: r.get(2),
                accepted: r.get(3),
                rejection_reason: r.get(4),
                auto_accepted: r.get(5),
                recorded_at: r.get(6),
            })
            .collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_history_entry_serde() {
        let entry = ConfigHistoryEntry {
            id: 1,
            key: "gossip.fanout".to_string(),
            old_value: Some(serde_json::json!(3)),
            new_value: serde_json::json!(5),
            changed_by: "admin".to_string(),
            source: "api".to_string(),
            changed_at: Utc::now(),
            row_hash: "abc123".to_string(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ConfigHistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key, "gossip.fanout");
        assert_eq!(back.new_value, serde_json::json!(5));
    }

    #[test]
    fn test_config_current_entry_serde() {
        let entry = ConfigCurrentEntry {
            key: "work.max_load".to_string(),
            value: serde_json::json!(0.7),
            updated_by: "operator".to_string(),
            updated_at: Utc::now(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ConfigCurrentEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key, "work.max_load");
    }

    #[test]
    fn test_config_snapshot_serde() {
        let snap = ConfigSnapshot {
            id: 42,
            full_config: serde_json::json!({"gossip_fanout": 5}),
            triggered_by: "patch".to_string(),
            changed_keys: vec!["gossip.fanout".to_string()],
            node_id: "node-1".to_string(),
            snapshot_at: Utc::now(),
            row_hash: "def456".to_string(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: ConfigSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.changed_keys.len(), 1);
    }

    // Note: async PG tests (insert_history, get_config_at, etc.) are in
    // integration_tests/config_compliance.rs, which requires a running PG instance.
}
