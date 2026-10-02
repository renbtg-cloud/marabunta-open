// Marabunta - Licensed under the MIT License.
//! Configuration for the swarm-managed PostgreSQL subsystem.
//!
//! Follows the same pattern as [`NeuromancerConfig`]: all fields have sensible
//! defaults, Duration fields use `humantime_serde`, and the struct is fully
//! TOML-serializable.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

// ============================================================================
// PgConfig
// ============================================================================

/// Configuration for swarm-managed PostgreSQL.
///
/// When `SwarmConfig.enable_postgres` is true (the default for non-minimal
/// deployments), the swarm automatically deploys and manages PostgreSQL on
/// capable nodes. This config controls resource thresholds, tuning knobs,
/// and replication behavior.
///
/// # Example TOML
/// ```toml
/// [postgres]
/// min_ram_for_pg_mb = 4096
/// replication_factor = 2
/// pg_port = 5433
/// history_retention_days = 90
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConfig {
    /// Minimum total RAM (MB) for a node to be eligible to host PostgreSQL.
    /// Light-class nodes (4-6 GB) are the floor.
    #[serde(default = "default_min_ram_for_pg_mb")]
    pub min_ram_for_pg_mb: u64,

    /// Minimum available disk space (MB) for PG data directory.
    #[serde(default = "default_min_disk_for_pg_mb")]
    pub min_disk_for_pg_mb: u64,

    /// Fraction of total RAM to allocate to `shared_buffers`.
    /// PostgreSQL recommendation: ~25% of system RAM.
    #[serde(default = "default_shared_buffers_fraction")]
    pub shared_buffers_fraction: f32,

    /// Fraction of total RAM for `effective_cache_size`.
    /// PostgreSQL recommendation: ~50-75% of system RAM.
    #[serde(default = "default_effective_cache_fraction")]
    pub effective_cache_fraction: f32,

    /// Maximum connections per PG instance.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Data directory for PG files. Defaults to `<state_dir>/pgdata`.
    /// If `state_dir` is not set, uses `./data/marabunta/pgdata`.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Port for the swarm-managed PG to listen on.
    /// Defaults to 5433 to avoid conflict with system PostgreSQL on 5432.
    #[serde(default = "default_pg_port")]
    pub pg_port: u16,

    /// Target number of streaming replicas. The swarm will attempt to
    /// maintain this many replicas across eligible nodes.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: usize,

    /// Size of the deadpool connection pool on client nodes.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// How often to run PG health checks (SELECT 1 on primary).
    #[serde(default = "default_health_check_interval", with = "humantime_serde")]
    pub health_check_interval: Duration,

    /// Enable temporal (time-travel) tables. Default: true.
    /// Disabling this drops the temporal schema and saves disk/CPU.
    #[serde(default = "default_enable_time_travel")]
    pub enable_time_travel: bool,

    /// Retention period (days) for temporal history tables.
    /// Rows older than this are purged by a background cleanup task.
    #[serde(default = "default_history_retention_days")]
    pub history_retention_days: u32,

    /// Database name for the swarm's operational database.
    #[serde(default = "default_database_name")]
    pub database_name: String,

    /// Whether to allow automatic PG deployment on eligible nodes.
    /// When false, only pre-existing (system-installed) PG is used.
    #[serde(default = "default_allow_auto_deploy")]
    pub allow_auto_deploy: bool,

    /// External PG connection URL. When set, the swarm connects to this
    /// PG instance instead of deploying its own. Overrides auto-deploy.
    /// Example: "host=db.example.com port=5432 dbname=marabunta user=marabunta password=secret"
    #[serde(default)]
    pub external_url: Option<String>,

    /// Maximum percentage of system RAM that PG is allowed to use.
    /// Prevents PG from starving compute workloads.
    #[serde(default = "default_max_ram_percent")]
    pub max_ram_percent: f32,

    /// How often to sync local SQLite events to PG on non-PG nodes.
    #[serde(default = "default_sync_interval", with = "humantime_serde")]
    pub sync_interval: Duration,
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            min_ram_for_pg_mb: default_min_ram_for_pg_mb(),
            min_disk_for_pg_mb: default_min_disk_for_pg_mb(),
            shared_buffers_fraction: default_shared_buffers_fraction(),
            effective_cache_fraction: default_effective_cache_fraction(),
            max_connections: default_max_connections(),
            data_dir: default_data_dir(),
            pg_port: default_pg_port(),
            replication_factor: default_replication_factor(),
            pool_size: default_pool_size(),
            health_check_interval: default_health_check_interval(),
            enable_time_travel: default_enable_time_travel(),
            history_retention_days: default_history_retention_days(),
            database_name: default_database_name(),
            allow_auto_deploy: default_allow_auto_deploy(),
            external_url: None,
            max_ram_percent: default_max_ram_percent(),
            sync_interval: default_sync_interval(),
        }
    }
}

// ============================================================================
// Default value functions
// ============================================================================

fn default_min_ram_for_pg_mb() -> u64 {
    4096 // 4 GB — Light-class floor
}

fn default_min_disk_for_pg_mb() -> u64 {
    2048 // 2 GB minimum disk
}

fn default_shared_buffers_fraction() -> f32 {
    0.25
}

fn default_effective_cache_fraction() -> f32 {
    0.50
}

fn default_max_connections() -> u32 {
    100
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data/marabunta/pgdata")
}

fn default_pg_port() -> u16 {
    5433
}

fn default_replication_factor() -> usize {
    2
}

fn default_pool_size() -> u32 {
    10
}

fn default_health_check_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_enable_time_travel() -> bool {
    true
}

fn default_history_retention_days() -> u32 {
    90
}

fn default_database_name() -> String {
    "marabunta".to_string()
}

fn default_allow_auto_deploy() -> bool {
    true
}

fn default_max_ram_percent() -> f32 {
    0.40 // PG gets at most 40% of RAM, rest for compute
}

fn default_sync_interval() -> Duration {
    Duration::from_secs(30)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let cfg = PgConfig::default();
        assert_eq!(cfg.min_ram_for_pg_mb, 4096);
        assert_eq!(cfg.min_disk_for_pg_mb, 2048);
        assert!((cfg.shared_buffers_fraction - 0.25).abs() < f32::EPSILON);
        assert!((cfg.effective_cache_fraction - 0.50).abs() < f32::EPSILON);
        assert_eq!(cfg.max_connections, 100);
        assert_eq!(cfg.pg_port, 5433);
        assert_eq!(cfg.replication_factor, 2);
        assert_eq!(cfg.pool_size, 10);
        assert_eq!(cfg.health_check_interval, Duration::from_secs(5));
        assert!(cfg.enable_time_travel);
        assert_eq!(cfg.history_retention_days, 90);
        assert_eq!(cfg.database_name, "marabunta");
        assert!(cfg.allow_auto_deploy);
        assert!(cfg.external_url.is_none());
        assert!((cfg.max_ram_percent - 0.40).abs() < f32::EPSILON);
        assert_eq!(cfg.sync_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_serde_roundtrip() {
        let cfg = PgConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: PgConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.pg_port, cfg.pg_port);
        assert_eq!(back.replication_factor, cfg.replication_factor);
        assert_eq!(back.database_name, cfg.database_name);
    }

    #[test]
    fn test_toml_partial_parse() {
        // Partial TOML should fill in defaults for missing fields
        let toml_str = r#"
            pg_port = 5434
            replication_factor = 3
        "#;
        let cfg: PgConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.pg_port, 5434);
        assert_eq!(cfg.replication_factor, 3);
        // Defaults for unspecified fields
        assert_eq!(cfg.min_ram_for_pg_mb, 4096);
        assert!(cfg.enable_time_travel);
    }

    #[test]
    fn test_external_url() {
        let toml_str = r#"
            external_url = "host=db.prod.internal port=5432 dbname=marabunta"
        "#;
        let cfg: PgConfig = toml::from_str(toml_str).unwrap();
        assert!(cfg.external_url.is_some());
        assert!(cfg.external_url.unwrap().contains("db.prod.internal"));
    }

    #[test]
    fn test_data_dir_default() {
        let cfg = PgConfig::default();
        assert_eq!(cfg.data_dir, PathBuf::from("./data/marabunta/pgdata"));
    }
}
