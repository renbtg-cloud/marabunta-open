// Marabunta - Licensed under the MIT License.
//! Schema management and migration runner for the swarm-managed PostgreSQL.
//!
//! All tables follow an **append-only temporal pattern**: rows are never updated
//! or deleted (except by the retention cleanup task). Each mutation inserts a new
//! row with `valid_from` / `valid_to` timestamps, allowing full point-in-time
//! reconstruction.
//!
//! Critical tables (config_history, audit_events, config_snapshots) additionally
//! carry `prev_hash` / `row_hash` columns forming a hash chain for tamper
//! evidence.
//!
//! # Migration system
//!
//! Migrations are embedded in the binary as `(version, description, sql)` tuples.
//! The runner reads `schema_migrations` to determine which have been applied,
//! then applies any pending migrations in order inside a transaction.

use tracing::{debug, info, warn};

use super::types::PgError;

// ============================================================================
// Embedded SQL
// ============================================================================

/// Initial schema — all temporal tables, indexes, and the schema_migrations
/// table itself.
const MIGRATION_001_INITIAL: &str = r#"
-- ============================================================================
-- Schema migrations tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    description TEXT    NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    checksum    TEXT    NOT NULL
);

-- ============================================================================
-- Config history (temporal, hash-chained)
-- ============================================================================

CREATE TABLE IF NOT EXISTS config_history (
    id          BIGSERIAL   PRIMARY KEY,
    key         TEXT        NOT NULL,
    value_json  JSONB       NOT NULL,
    prev_value  JSONB,
    valid_from  TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to    TIMESTAMPTZ,
    changed_by  TEXT        NOT NULL DEFAULT 'system',
    change_reason TEXT,
    prev_hash   TEXT,
    row_hash    TEXT        NOT NULL,
    CONSTRAINT config_history_time_check CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE INDEX IF NOT EXISTS idx_config_history_key ON config_history (key);
CREATE INDEX IF NOT EXISTS idx_config_history_valid_from ON config_history (valid_from);
CREATE INDEX IF NOT EXISTS idx_config_history_valid_to ON config_history (valid_to);

-- ============================================================================
-- Config current (latest runtime values, upserted on change)
-- ============================================================================

CREATE TABLE IF NOT EXISTS config_current (
    key         TEXT        PRIMARY KEY,
    value_json  JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT        NOT NULL DEFAULT 'system'
);

-- ============================================================================
-- Config snapshots (full SwarmConfig JSON, hash-chained)
-- ============================================================================

CREATE TABLE IF NOT EXISTS config_snapshots (
    id          BIGSERIAL   PRIMARY KEY,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    config_json JSONB       NOT NULL,
    trigger     TEXT        NOT NULL DEFAULT 'change',
    prev_hash   TEXT,
    row_hash    TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_config_snapshots_at ON config_snapshots (snapshot_at);

-- ============================================================================
-- Node state history (temporal)
-- ============================================================================

CREATE TABLE IF NOT EXISTS node_state_history (
    id          BIGSERIAL   PRIMARY KEY,
    node_id     UUID        NOT NULL,
    event_type  TEXT        NOT NULL,
    state_json  JSONB       NOT NULL,
    traits      JSONB,
    valid_from  TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to    TIMESTAMPTZ,
    CONSTRAINT node_state_time_check CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE INDEX IF NOT EXISTS idx_node_state_node_id ON node_state_history (node_id);
CREATE INDEX IF NOT EXISTS idx_node_state_valid_from ON node_state_history (valid_from);
CREATE INDEX IF NOT EXISTS idx_node_state_event_type ON node_state_history (event_type);

-- ============================================================================
-- Job history (temporal)
-- ============================================================================

CREATE TABLE IF NOT EXISTS job_history (
    id          BIGSERIAL   PRIMARY KEY,
    job_id      UUID        NOT NULL,
    event_type  TEXT        NOT NULL,
    event_data  JSONB       NOT NULL,
    node_id     UUID,
    chunk_id    UUID,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_history_job_id ON job_history (job_id);
CREATE INDEX IF NOT EXISTS idx_job_history_recorded_at ON job_history (recorded_at);
CREATE INDEX IF NOT EXISTS idx_job_history_event_type ON job_history (event_type);

-- ============================================================================
-- Audit events (temporal, hash-chained)
-- ============================================================================

CREATE TABLE IF NOT EXISTS audit_events (
    id          BIGSERIAL   PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    actor       TEXT        NOT NULL,
    target      TEXT,
    target_type TEXT,
    detail_json JSONB,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    prev_hash   TEXT,
    row_hash    TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_events_recorded_at ON audit_events (recorded_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_event_type ON audit_events (event_type);
CREATE INDEX IF NOT EXISTS idx_audit_events_actor ON audit_events (actor);
CREATE INDEX IF NOT EXISTS idx_audit_events_target ON audit_events (target);

-- ============================================================================
-- Consent records
-- ============================================================================

CREATE TABLE IF NOT EXISTS consent_records (
    id              BIGSERIAL   PRIMARY KEY,
    node_id         UUID        NOT NULL,
    manifest_hash   TEXT        NOT NULL,
    manifest_version TEXT       NOT NULL,
    accepted        BOOLEAN     NOT NULL,
    signature       TEXT,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_consent_records_node_id ON consent_records (node_id);
CREATE INDEX IF NOT EXISTS idx_consent_records_recorded_at ON consent_records (recorded_at);

-- ============================================================================
-- Compliance snapshots (periodic posture captures)
-- ============================================================================

CREATE TABLE IF NOT EXISTS compliance_snapshots (
    id          BIGSERIAL   PRIMARY KEY,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    profile     TEXT        NOT NULL,
    posture_json JSONB      NOT NULL,
    overall_status TEXT     NOT NULL DEFAULT 'unknown'
);

CREATE INDEX IF NOT EXISTS idx_compliance_snapshots_at ON compliance_snapshots (snapshot_at);
CREATE INDEX IF NOT EXISTS idx_compliance_snapshots_profile ON compliance_snapshots (profile);

-- ============================================================================
-- Compliance violations
-- ============================================================================

CREATE TABLE IF NOT EXISTS compliance_violations (
    id          BIGSERIAL   PRIMARY KEY,
    control_id  TEXT        NOT NULL,
    severity    TEXT        NOT NULL DEFAULT 'warning',
    description TEXT        NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    resolution  TEXT,
    node_id     UUID
);

CREATE INDEX IF NOT EXISTS idx_compliance_violations_detected ON compliance_violations (detected_at);
CREATE INDEX IF NOT EXISTS idx_compliance_violations_control ON compliance_violations (control_id);
CREATE INDEX IF NOT EXISTS idx_compliance_violations_severity ON compliance_violations (severity);

-- ============================================================================
-- PG cluster state (self-management metadata)
-- ============================================================================

CREATE TABLE IF NOT EXISTS pg_cluster_state (
    id          BIGSERIAL   PRIMARY KEY,
    node_id     UUID        NOT NULL,
    role        TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    pg_version  TEXT,
    listen_addr TEXT,
    pg_port     INTEGER     NOT NULL DEFAULT 5433,
    wal_position TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pg_cluster_state_node_id ON pg_cluster_state (node_id);
CREATE INDEX IF NOT EXISTS idx_pg_cluster_state_recorded_at ON pg_cluster_state (recorded_at);
"#;

// ============================================================================
// Migration registry
// ============================================================================

/// A single migration: (version, description, SQL).
struct Migration {
    version: u32,
    description: &'static str,
    sql: &'static str,
}

/// All migrations in order. New migrations are appended here.
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    description: "Initial temporal schema",
    sql: MIGRATION_001_INITIAL,
}];

// ============================================================================
// SchemaManager
// ============================================================================

/// Manages schema creation and migrations for the PG subsystem.
///
/// Migrations are applied transactionally — if one fails, the entire
/// migration is rolled back and the error is reported.
pub struct SchemaManager;

impl SchemaManager {
    /// Apply all pending migrations to the database.
    ///
    /// Reads the current version from `schema_migrations`, then applies
    /// any migrations with a higher version number.
    pub async fn apply_pending(
        client: &tokio_postgres::Client,
    ) -> Result<Vec<u32>, PgError> {
        // Ensure schema_migrations table exists (bootstrap)
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                    version     INTEGER PRIMARY KEY,
                    description TEXT    NOT NULL,
                    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    checksum    TEXT    NOT NULL
                )",
            )
            .await
            .map_err(|e| PgError::MigrationFailed(format!("bootstrap: {e}")))?;

        let current = Self::current_version(client).await?;
        let mut applied = Vec::new();

        for migration in MIGRATIONS {
            if migration.version <= current {
                debug!(
                    version = migration.version,
                    "Migration already applied, skipping"
                );
                continue;
            }

            info!(
                version = migration.version,
                description = migration.description,
                "Applying migration"
            );

            let checksum = Self::checksum(migration.sql);

            // Apply migration SQL
            client
                .batch_execute(migration.sql)
                .await
                .map_err(|e| {
                    PgError::MigrationFailed(format!(
                        "migration v{}: {e}",
                        migration.version
                    ))
                })?;

            // Record in schema_migrations
            client
                .execute(
                    "INSERT INTO schema_migrations (version, description, checksum)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (version) DO NOTHING",
                    &[
                        &(migration.version as i32),
                        &migration.description,
                        &checksum,
                    ],
                )
                .await
                .map_err(|e| {
                    PgError::MigrationFailed(format!(
                        "recording migration v{}: {e}",
                        migration.version
                    ))
                })?;

            applied.push(migration.version);
            info!(version = migration.version, "Migration applied successfully");
        }

        if applied.is_empty() {
            debug!("All migrations are up to date");
        }

        Ok(applied)
    }

    /// Get the current (highest applied) schema version.
    ///
    /// Returns 0 if no migrations have been applied yet.
    pub async fn current_version(
        client: &tokio_postgres::Client,
    ) -> Result<u32, PgError> {
        // Check if table exists first
        let exists = client
            .query_one(
                "SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'schema_migrations'
                )",
                &[],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("check schema_migrations: {e}")))?;

        let table_exists: bool = exists.get(0);
        if !table_exists {
            return Ok(0);
        }

        let row = client
            .query_opt(
                "SELECT COALESCE(MAX(version), 0) as max_ver FROM schema_migrations",
                &[],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("get version: {e}")))?;

        match row {
            Some(r) => {
                let v: i32 = r.get("max_ver");
                Ok(v as u32)
            }
            None => Ok(0),
        }
    }

    /// Validate that all expected tables exist.
    ///
    /// Returns a list of missing table names (empty = valid).
    pub async fn validate(
        client: &tokio_postgres::Client,
    ) -> Result<Vec<String>, PgError> {
        let expected_tables = vec![
            "schema_migrations",
            "config_history",
            "config_current",
            "config_snapshots",
            "node_state_history",
            "job_history",
            "audit_events",
            "consent_records",
            "compliance_snapshots",
            "compliance_violations",
            "pg_cluster_state",
        ];

        let mut missing = Vec::new();

        for table in expected_tables {
            let row = client
                .query_one(
                    "SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = $1
                    )",
                    &[&table],
                )
                .await
                .map_err(|e| PgError::QueryFailed(format!("validate {table}: {e}")))?;

            let exists: bool = row.get(0);
            if !exists {
                missing.push(table.to_string());
            }
        }

        if !missing.is_empty() {
            warn!(missing = ?missing, "Schema validation found missing tables");
        }

        Ok(missing)
    }

    /// Get details of all applied migrations.
    pub async fn applied_migrations(
        client: &tokio_postgres::Client,
    ) -> Result<Vec<MigrationRecord>, PgError> {
        let rows = client
            .query(
                "SELECT version, description, applied_at, checksum
                 FROM schema_migrations
                 ORDER BY version ASC",
                &[],
            )
            .await
            .map_err(|e| PgError::QueryFailed(format!("list migrations: {e}")))?;

        Ok(rows
            .iter()
            .map(|row| MigrationRecord {
                version: row.get::<_, i32>("version") as u32,
                description: row.get("description"),
                applied_at: row.get("applied_at"),
                checksum: row.get("checksum"),
            })
            .collect())
    }

    /// Compute a simple checksum for a migration SQL string.
    fn checksum(sql: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(sql.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Get the total number of registered migrations.
    pub fn total_migrations() -> usize {
        MIGRATIONS.len()
    }

    /// Get all registered migration versions and descriptions.
    pub fn registered_migrations() -> Vec<(u32, &'static str)> {
        MIGRATIONS
            .iter()
            .map(|m| (m.version, m.description))
            .collect()
    }
}

// ============================================================================
// MigrationRecord
// ============================================================================

/// Record of an applied migration (from the database).
#[derive(Debug, Clone, serde::Serialize)]
pub struct MigrationRecord {
    pub version: u32,
    pub description: String,
    pub applied_at: chrono::DateTime<chrono::Utc>,
    pub checksum: String,
}

// ============================================================================
// SQL helpers for hash-chain tables
// ============================================================================

/// Compute a SHA-256 hash for a row in a hash-chained table.
///
/// The hash covers all significant columns concatenated with `|` separators.
pub fn compute_row_hash(prev_hash: Option<&str>, fields: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    if let Some(prev) = prev_hash {
        hasher.update(prev.as_bytes());
        hasher.update(b"|");
    }
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            hasher.update(b"|");
        }
        hasher.update(field.as_bytes());
    }
    hex::encode(hasher.finalize())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_001_sql_parses() {
        // Verify the SQL is syntactically valid using sqlparser
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let dialect = PostgreSqlDialect {};
        let result = Parser::parse_sql(&dialect, MIGRATION_001_INITIAL);
        assert!(
            result.is_ok(),
            "Migration 001 SQL failed to parse: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_migrations_ordered() {
        let mut prev_version = 0;
        for m in MIGRATIONS {
            assert!(
                m.version > prev_version,
                "Migration versions must be strictly increasing: {} <= {}",
                m.version,
                prev_version
            );
            prev_version = m.version;
        }
    }

    #[test]
    fn test_migrations_have_descriptions() {
        for m in MIGRATIONS {
            assert!(
                !m.description.is_empty(),
                "Migration v{} has empty description",
                m.version
            );
        }
    }

    #[test]
    fn test_migration_checksums_stable() {
        // Checksums should be deterministic
        let c1 = SchemaManager::checksum(MIGRATION_001_INITIAL);
        let c2 = SchemaManager::checksum(MIGRATION_001_INITIAL);
        assert_eq!(c1, c2);
        assert!(!c1.is_empty());
    }

    #[test]
    fn test_total_migrations() {
        assert_eq!(SchemaManager::total_migrations(), 1);
    }

    #[test]
    fn test_registered_migrations() {
        let registered = SchemaManager::registered_migrations();
        assert_eq!(registered.len(), 1);
        assert_eq!(registered[0].0, 1);
        assert_eq!(registered[0].1, "Initial temporal schema");
    }

    #[test]
    fn test_compute_row_hash_deterministic() {
        let h1 = compute_row_hash(None, &["key1", "value1"]);
        let h2 = compute_row_hash(None, &["key1", "value1"]);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_compute_row_hash_chains() {
        let h1 = compute_row_hash(None, &["key1", "value1"]);
        let h2 = compute_row_hash(Some(&h1), &["key2", "value2"]);
        let h3 = compute_row_hash(Some(&h2), &["key3", "value3"]);

        // Each hash should be different
        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_compute_row_hash_prev_matters() {
        let h_no_prev = compute_row_hash(None, &["key1", "value1"]);
        let h_with_prev = compute_row_hash(Some("abc123"), &["key1", "value1"]);
        assert_ne!(h_no_prev, h_with_prev);
    }

    #[test]
    fn test_compute_row_hash_fields_matter() {
        let h1 = compute_row_hash(None, &["field_a", "field_b"]);
        let h2 = compute_row_hash(None, &["field_a", "field_c"]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_migration_001_has_all_expected_tables() {
        let sql = MIGRATION_001_INITIAL;
        let expected_tables = [
            "schema_migrations",
            "config_history",
            "config_current",
            "config_snapshots",
            "node_state_history",
            "job_history",
            "audit_events",
            "consent_records",
            "compliance_snapshots",
            "compliance_violations",
            "pg_cluster_state",
        ];
        for table in expected_tables {
            assert!(
                sql.contains(&format!("CREATE TABLE IF NOT EXISTS {table}")),
                "Migration 001 missing CREATE TABLE for: {table}"
            );
        }
    }

    #[test]
    fn test_migration_001_has_hash_chain_columns() {
        let sql = MIGRATION_001_INITIAL;
        // These tables should have prev_hash + row_hash
        let hash_chained_tables = ["config_history", "audit_events", "config_snapshots"];
        for table in hash_chained_tables {
            // Find the CREATE TABLE block for this table
            let table_start = sql
                .find(&format!("CREATE TABLE IF NOT EXISTS {table}"))
                .unwrap_or_else(|| panic!("Missing table: {table}"));
            let table_sql = &sql[table_start..];
            let block_end = table_sql.find(");").unwrap() + 2;
            let block = &table_sql[..block_end];

            assert!(
                block.contains("prev_hash"),
                "Table {table} missing prev_hash column"
            );
            assert!(
                block.contains("row_hash"),
                "Table {table} missing row_hash column"
            );
        }
    }

    #[test]
    fn test_migration_001_temporal_constraints() {
        let sql = MIGRATION_001_INITIAL;
        // Check that temporal tables have valid_from/valid_to and time check constraints
        let temporal_tables = ["config_history", "node_state_history"];
        for table in temporal_tables {
            let table_start = sql
                .find(&format!("CREATE TABLE IF NOT EXISTS {table}"))
                .unwrap_or_else(|| panic!("Missing table: {table}"));
            let table_sql = &sql[table_start..];
            let block_end = table_sql.find(");").unwrap() + 2;
            let block = &table_sql[..block_end];

            assert!(
                block.contains("valid_from"),
                "Table {table} missing valid_from"
            );
            assert!(
                block.contains("valid_to"),
                "Table {table} missing valid_to"
            );
            assert!(
                block.contains("time_check"),
                "Table {table} missing time constraint"
            );
        }
    }
}
