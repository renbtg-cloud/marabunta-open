// Marabunta - Licensed under the MIT License.
//! Shared types for the swarm-managed PostgreSQL subsystem.
//!
//! These types are used across all postgres sub-modules: deployment,
//! connection pooling, replication, failover, and time-travel queries.

use std::fmt;
use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::super::types::NodeId;

// ============================================================================
// PG node roles
// ============================================================================

/// Role of a node in the PostgreSQL cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgNodeRole {
    /// This node runs the primary (read-write) PG instance.
    Primary,
    /// This node runs a streaming replica (read-only).
    Replica,
    /// This node doesn't run PG but connects to the cluster as a client.
    Client,
    /// Not eligible to participate (insufficient resources).
    Ineligible,
}

impl fmt::Display for PgNodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Replica => write!(f, "replica"),
            Self::Client => write!(f, "client"),
            Self::Ineligible => write!(f, "ineligible"),
        }
    }
}

// ============================================================================
// PG instance status
// ============================================================================

/// Lifecycle status of a PostgreSQL instance managed by this node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgStatus {
    /// No PG installed on this node.
    NotInstalled,
    /// PG binary being installed / downloaded.
    Installing,
    /// Running `initdb` to create the data directory.
    Initializing,
    /// Writing `postgresql.conf`, `pg_hba.conf`.
    Configuring,
    /// `pg_ctl start` in progress.
    Starting,
    /// PG is running and accepting connections.
    Running,
    /// Setting up streaming replication.
    Replicating,
    /// PG failed to start or crashed.
    Failed(String),
    /// Graceful shutdown in progress.
    Stopping,
    /// PG is stopped.
    Stopped,
}

impl PgStatus {
    /// Whether the instance is healthy and accepting connections.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    /// Whether the instance is in a terminal error state.
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
}

impl fmt::Display for PgStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInstalled => write!(f, "not_installed"),
            Self::Installing => write!(f, "installing"),
            Self::Initializing => write!(f, "initializing"),
            Self::Configuring => write!(f, "configuring"),
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::Replicating => write!(f, "replicating"),
            Self::Failed(reason) => write!(f, "failed: {reason}"),
            Self::Stopping => write!(f, "stopping"),
            Self::Stopped => write!(f, "stopped"),
        }
    }
}

// ============================================================================
// PG node info (gossiped across the cluster)
// ============================================================================

/// Information about a PostgreSQL instance on a node, shared via gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgNodeInfo {
    /// The swarm node running this PG instance.
    pub node_id: NodeId,
    /// Role in the PG cluster.
    pub role: PgNodeRole,
    /// Current lifecycle status.
    pub status: PgStatus,
    /// PostgreSQL version string (e.g., "16.2").
    pub pg_version: Option<String>,
    /// Address to connect to this PG instance.
    pub listen_addr: Option<SocketAddr>,
    /// PG port (typically 5433 for swarm-managed, 5432 for system).
    pub pg_port: u16,
    /// Current WAL position (for replication / primary election).
    pub wal_position: Option<String>,
    /// Last successful health check.
    pub last_health_check: Option<DateTime<Utc>>,
    /// Replication lag in milliseconds (replicas only).
    pub replication_lag_ms: Option<u64>,
}

impl PgNodeInfo {
    /// Connection string for this PG instance.
    pub fn connection_string(&self, database: &str) -> Option<String> {
        self.listen_addr.map(|addr| {
            format!(
                "host={} port={} dbname={} connect_timeout=5",
                addr.ip(),
                self.pg_port,
                database,
            )
        })
    }
}

// ============================================================================
// PG cluster status (aggregate)
// ============================================================================

/// Aggregate status of the PostgreSQL cluster in the swarm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgClusterStatus {
    /// Primary node (if any).
    pub primary: Option<PgNodeInfo>,
    /// Replica nodes.
    pub replicas: Vec<PgNodeInfo>,
    /// Client-only nodes.
    pub clients: Vec<NodeId>,
    /// Ineligible nodes.
    pub ineligible: Vec<NodeId>,
    /// Whether the cluster is healthy (has a primary).
    pub healthy: bool,
    /// Total number of PG-hosting nodes.
    pub pg_node_count: usize,
}

// ============================================================================
// PG instance handle (local)
// ============================================================================

/// Handle to a locally-managed PostgreSQL instance.
#[derive(Debug, Clone)]
pub struct PgInstance {
    /// Path to the PG binary directory (e.g., /usr/lib/postgresql/16/bin/).
    pub bin_dir: std::path::PathBuf,
    /// Path to the PG data directory.
    pub data_dir: std::path::PathBuf,
    /// Port the instance listens on.
    pub port: u16,
    /// PG version string.
    pub version: Option<String>,
    /// Whether this is a system-installed PG (vs swarm-deployed).
    pub is_system_pg: bool,
}

// ============================================================================
// Resource snapshot (for PG eligibility checks)
// ============================================================================

/// Snapshot of node resources relevant to PG hosting decisions.
#[derive(Debug, Clone)]
pub struct PgResourceSnapshot {
    /// Total system memory in MB.
    pub memory_total_mb: u64,
    /// Available memory in MB.
    pub memory_available_mb: u64,
    /// Total disk space in MB.
    pub disk_total_mb: u64,
    /// Available disk space in MB.
    pub disk_available_mb: u64,
    /// Number of CPU cores.
    pub cpu_cores: usize,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors from the PostgreSQL subsystem.
#[derive(Debug, Error)]
pub enum PgError {
    #[error("PostgreSQL not installed and auto-deployment is disabled")]
    NotInstalled,

    #[error("Insufficient resources: {0}")]
    InsufficientResources(String),

    #[error("PostgreSQL failed to start: {0}")]
    StartFailed(String),

    #[error("PostgreSQL initdb failed: {0}")]
    InitDbFailed(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Replication setup failed: {0}")]
    ReplicationFailed(String),

    #[error("Failover failed: {0}")]
    FailoverFailed(String),

    #[error("Schema migration failed: {0}")]
    MigrationFailed(String),

    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("No primary available")]
    NoPrimary,

    #[error("Pool exhausted")]
    PoolExhausted,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Timeout waiting for {0}")]
    Timeout(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_node_role_display() {
        assert_eq!(PgNodeRole::Primary.to_string(), "primary");
        assert_eq!(PgNodeRole::Replica.to_string(), "replica");
        assert_eq!(PgNodeRole::Client.to_string(), "client");
        assert_eq!(PgNodeRole::Ineligible.to_string(), "ineligible");
    }

    #[test]
    fn test_pg_status_predicates() {
        assert!(PgStatus::Running.is_running());
        assert!(!PgStatus::Stopped.is_running());
        assert!(PgStatus::Failed("crash".into()).is_failed());
        assert!(!PgStatus::Running.is_failed());
    }

    #[test]
    fn test_pg_status_display() {
        assert_eq!(PgStatus::Running.to_string(), "running");
        assert_eq!(
            PgStatus::Failed("oom".into()).to_string(),
            "failed: oom"
        );
    }

    #[test]
    fn test_pg_node_role_serde_roundtrip() {
        let role = PgNodeRole::Primary;
        let json = serde_json::to_string(&role).unwrap();
        assert_eq!(json, "\"primary\"");
        let back: PgNodeRole = serde_json::from_str(&json).unwrap();
        assert_eq!(back, role);
    }

    #[test]
    fn test_pg_status_serde_roundtrip() {
        let status = PgStatus::Running;
        let json = serde_json::to_string(&status).unwrap();
        let back: PgStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);

        let failed = PgStatus::Failed("connection refused".into());
        let json = serde_json::to_string(&failed).unwrap();
        let back: PgStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, failed);
    }

    #[test]
    fn test_pg_node_info_connection_string() {
        let info = PgNodeInfo {
            node_id: NodeId::default(),
            role: PgNodeRole::Primary,
            status: PgStatus::Running,
            pg_version: Some("16.2".into()),
            listen_addr: Some("192.168.1.10:5433".parse().unwrap()),
            pg_port: 5433,
            wal_position: None,
            last_health_check: Some(Utc::now()),
            replication_lag_ms: None,
        };
        let conn = info.connection_string("marabunta").unwrap();
        assert!(conn.contains("host=192.168.1.10"));
        assert!(conn.contains("port=5433"));
        assert!(conn.contains("dbname=marabunta"));
    }

    #[test]
    fn test_pg_node_info_no_listen_addr() {
        let info = PgNodeInfo {
            node_id: NodeId::default(),
            role: PgNodeRole::Client,
            status: PgStatus::NotInstalled,
            pg_version: None,
            listen_addr: None,
            pg_port: 5433,
            wal_position: None,
            last_health_check: None,
            replication_lag_ms: None,
        };
        assert!(info.connection_string("marabunta").is_none());
    }

    #[test]
    fn test_pg_cluster_status_healthy() {
        let status = PgClusterStatus {
            primary: Some(PgNodeInfo {
                node_id: NodeId::default(),
                role: PgNodeRole::Primary,
                status: PgStatus::Running,
                pg_version: Some("16.2".into()),
                listen_addr: Some("10.0.0.1:5433".parse().unwrap()),
                pg_port: 5433,
                wal_position: None,
                last_health_check: Some(Utc::now()),
                replication_lag_ms: None,
            }),
            replicas: vec![],
            clients: vec![],
            ineligible: vec![],
            healthy: true,
            pg_node_count: 1,
        };
        assert!(status.healthy);
        assert_eq!(status.pg_node_count, 1);
    }
}
