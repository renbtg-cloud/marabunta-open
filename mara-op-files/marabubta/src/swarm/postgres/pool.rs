// Marabunta - Licensed under the MIT License.
//! Connection pooling for the swarm-managed PostgreSQL subsystem.
//!
//! Provides a thin wrapper around `deadpool-postgres` with:
//!
//! - **Primary pool**: writes go here (exactly one primary in the cluster)
//! - **Replica pools**: reads are load-balanced across replicas, falling back
//!   to the primary when no replicas exist
//! - **Health checks**: periodic `SELECT 1` on the primary
//! - **Reconfiguration**: rebuild pools when cluster topology changes (failover,
//!   new replica, node death)
//!
//! The pool does **not** own the connection config — it receives connection
//! parameters from `PgNodeInfo` gossip payloads.

use std::sync::atomic::{AtomicU64, Ordering};

use deadpool_postgres::{
    Client as PgClient, Config as DeadpoolConfig, ManagerConfig, Pool, RecyclingMethod, Runtime,
};
use parking_lot::RwLock;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

use super::config::PgConfig;
use super::types::{PgError, PgNodeInfo, PgNodeRole};
use crate::swarm::types::NodeId;

// ============================================================================
// ReplicaEntry
// ============================================================================

/// A replica pool with its associated metadata.
#[derive(Clone)]
struct ReplicaEntry {
    node_id: NodeId,
    pool: Pool,
}

// ============================================================================
// PgPool
// ============================================================================

/// Connection pool manager for the PG cluster.
///
/// Maintains separate pools for the primary (writes) and replicas (reads).
/// Read queries are round-robin distributed across available replicas;
/// if no replicas are available, reads fall back to the primary.
pub struct PgPool {
    config: PgConfig,

    /// Primary connection pool (write path).
    primary: RwLock<Option<PrimaryEntry>>,

    /// Replica connection pools (read path, round-robin).
    replicas: RwLock<Vec<ReplicaEntry>>,

    /// Round-robin counter for replica selection.
    read_counter: AtomicU64,
}

/// Primary pool with its associated node ID.
#[derive(Clone)]
struct PrimaryEntry {
    node_id: NodeId,
    pool: Pool,
}

impl PgPool {
    /// Create a new PgPool from configuration.
    ///
    /// Does **not** connect to any PG instance. Use `connect_primary()` and
    /// `add_replica()` to establish connections.
    pub fn new(config: PgConfig) -> Self {
        Self {
            config,
            primary: RwLock::new(None),
            replicas: RwLock::new(Vec::new()),
            read_counter: AtomicU64::new(0),
        }
    }

    // ========================================================================
    // Connection management
    // ========================================================================

    /// Establish a connection pool to the primary PG instance.
    ///
    /// Replaces any existing primary pool.
    pub fn connect_primary(&self, info: &PgNodeInfo) -> Result<(), PgError> {
        let pool = self.build_pool(info)?;
        info!(
            node_id = %info.node_id,
            addr = ?info.listen_addr,
            "Connected to PG primary"
        );
        *self.primary.write() = Some(PrimaryEntry {
            node_id: info.node_id,
            pool,
        });
        Ok(())
    }

    /// Add a replica pool for read load-balancing.
    ///
    /// If a pool for this node already exists, it is replaced.
    pub fn add_replica(&self, info: &PgNodeInfo) -> Result<(), PgError> {
        let pool = self.build_pool(info)?;
        let entry = ReplicaEntry {
            node_id: info.node_id,
            pool,
        };

        let mut replicas = self.replicas.write();
        // Replace existing entry for this node, or append
        if let Some(pos) = replicas.iter().position(|r| r.node_id == info.node_id) {
            replicas[pos] = entry;
            debug!(node_id = %info.node_id, "Replaced existing replica pool");
        } else {
            replicas.push(entry);
            info!(
                node_id = %info.node_id,
                replica_count = replicas.len(),
                "Added replica pool"
            );
        }
        Ok(())
    }

    /// Get a connection from the primary pool (write path).
    pub async fn get_write(
        &self,
    ) -> Result<PgClient, PgError> {
        // Clone the pool handle before awaiting so we don't hold the
        // parking_lot RwLockReadGuard across an .await point (which would
        // make the future !Send and break axum handler compatibility).
        let pool = {
            let guard = self.primary.read();
            let primary = guard.as_ref().ok_or(PgError::NoPrimary)?;
            primary.pool.clone()
        };
        pool.get()
            .await
            .map_err(|e| PgError::ConnectionFailed(format!("primary pool: {e}")))
    }

    /// Get a connection from a replica pool (read path).
    ///
    /// Uses round-robin selection across available replicas. Falls back to
    /// the primary if no replicas are available.
    pub async fn get_read(
        &self,
    ) -> Result<PgClient, PgError> {
        // Try replicas first. Clone the pool handle before awaiting to
        // avoid holding a parking_lot guard across .await (which is !Send).
        let replica_pool = {
            let replicas = self.replicas.read();
            if !replicas.is_empty() {
                let idx =
                    self.read_counter.fetch_add(1, Ordering::Relaxed) as usize % replicas.len();
                Some(replicas[idx].pool.clone())
            } else {
                None
            }
        };

        if let Some(pool) = replica_pool {
            match pool.get().await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    warn!(
                        error = %e,
                        "Replica pool get failed, falling back to primary"
                    );
                }
            }
        }

        // Fallback to primary
        self.get_write().await
    }

    /// Run a health check on the primary (`SELECT 1`).
    ///
    /// Returns `true` if the primary is reachable and responsive.
    pub async fn health_check(&self) -> bool {
        match self.get_write().await {
            Ok(client) => match client.simple_query("SELECT 1").await {
                Ok(_) => true,
                Err(e) => {
                    warn!(error = %e, "PG primary health check failed");
                    false
                }
            },
            Err(e) => {
                warn!(error = %e, "PG primary health check: no connection");
                false
            }
        }
    }

    /// Reconfigure pools based on updated cluster topology.
    ///
    /// This is called when gossip delivers new `PgNodeInfo` for the cluster.
    /// It rebuilds primary/replica pools to match the current topology.
    pub fn reconfigure(
        &self,
        pg_nodes: &[PgNodeInfo],
        database: &str,
    ) -> Result<(), PgError> {
        let _ = database; // Database name comes from config
        let mut new_primary: Option<&PgNodeInfo> = None;
        let mut new_replicas: Vec<&PgNodeInfo> = Vec::new();

        for node in pg_nodes {
            if !node.status.is_running() {
                continue;
            }
            match node.role {
                PgNodeRole::Primary => {
                    new_primary = Some(node);
                }
                PgNodeRole::Replica => {
                    new_replicas.push(node);
                }
                _ => {}
            }
        }

        // Update primary
        if let Some(primary_info) = new_primary {
            let needs_update = {
                let guard = self.primary.read();
                guard
                    .as_ref()
                    .map(|p| p.node_id != primary_info.node_id)
                    .unwrap_or(true)
            };
            if needs_update {
                self.connect_primary(primary_info)?;
            }
        }

        // Update replicas: rebuild the list
        let current_replica_ids: Vec<NodeId> = self
            .replicas
            .read()
            .iter()
            .map(|r| r.node_id)
            .collect();
        let new_replica_ids: Vec<&NodeId> = new_replicas.iter().map(|r| &r.node_id).collect();

        // Add new replicas
        for replica_info in &new_replicas {
            if !current_replica_ids.contains(&replica_info.node_id) {
                self.add_replica(replica_info)?;
            }
        }

        // Remove dead replicas
        {
            let mut replicas = self.replicas.write();
            replicas.retain(|r| new_replica_ids.contains(&&r.node_id));
        }

        Ok(())
    }

    /// Remove a specific node's pool (called when failure detector declares it dead).
    pub fn remove_node(&self, node_id: &NodeId) {
        // Check if it's the primary
        {
            let guard = self.primary.read();
            if let Some(primary) = guard.as_ref() {
                if &primary.node_id == node_id {
                    drop(guard);
                    *self.primary.write() = None;
                    warn!(node_id = %node_id, "Removed dead PG primary pool");
                    return;
                }
            }
        }

        // Check replicas
        let mut replicas = self.replicas.write();
        let before = replicas.len();
        replicas.retain(|r| &r.node_id != node_id);
        if replicas.len() < before {
            info!(
                node_id = %node_id,
                remaining = replicas.len(),
                "Removed dead PG replica pool"
            );
        }
    }

    /// Whether the primary pool is connected.
    pub fn has_primary(&self) -> bool {
        self.primary.read().is_some()
    }

    /// Get the node ID of the current primary, if any.
    pub fn primary_node_id(&self) -> Option<NodeId> {
        self.primary.read().as_ref().map(|p| p.node_id)
    }

    /// Number of active replica pools.
    pub fn replica_count(&self) -> usize {
        self.replicas.read().len()
    }

    /// Get the status of all pools for diagnostics.
    pub fn pool_status(&self) -> PgPoolStatus {
        let primary = self.primary.read();
        let replicas = self.replicas.read();

        PgPoolStatus {
            primary_node_id: primary.as_ref().map(|p| p.node_id),
            primary_pool_size: primary.as_ref().map(|p| p.pool.status().size as u32),
            primary_pool_available: primary
                .as_ref()
                .map(|p| p.pool.status().available as u32),
            replica_count: replicas.len(),
            replica_node_ids: replicas.iter().map(|r| r.node_id).collect(),
        }
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /// Build a deadpool connection pool from PgNodeInfo.
    fn build_pool(&self, info: &PgNodeInfo) -> Result<Pool, PgError> {
        let addr = info
            .listen_addr
            .ok_or_else(|| PgError::ConnectionFailed("no listen address".into()))?;

        let mut cfg = DeadpoolConfig::new();
        cfg.host = Some(addr.ip().to_string());
        cfg.port = Some(info.pg_port);
        cfg.dbname = Some(self.config.database_name.clone());
        cfg.user = Some("marabunta".to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });

        let pool = cfg
            .builder(NoTls)
            .map_err(|e| PgError::ConnectionFailed(format!("pool builder: {e}")))?
            .max_size(self.config.pool_size as usize)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| PgError::ConnectionFailed(format!("pool build: {e}")))?;

        Ok(pool)
    }
}

// ============================================================================
// PgPoolStatus (diagnostics)
// ============================================================================

/// Diagnostic snapshot of pool state.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PgPoolStatus {
    pub primary_node_id: Option<NodeId>,
    pub primary_pool_size: Option<u32>,
    pub primary_pool_available: Option<u32>,
    pub replica_count: usize,
    pub replica_node_ids: Vec<NodeId>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::postgres::types::{PgNodeRole, PgStatus};
    use std::net::SocketAddr;

    fn test_config() -> PgConfig {
        PgConfig {
            pool_size: 5,
            ..PgConfig::default()
        }
    }

    fn test_node_id(suffix: &str) -> NodeId {
        use uuid::Uuid;
        NodeId(Uuid::new_v5(&Uuid::NAMESPACE_DNS, suffix.as_bytes()))
    }

    fn make_pg_info(suffix: &str, role: PgNodeRole, addr: &str) -> PgNodeInfo {
        PgNodeInfo {
            node_id: test_node_id(suffix),
            role,
            status: PgStatus::Running,
            pg_version: Some("16.2".into()),
            listen_addr: Some(addr.parse::<SocketAddr>().unwrap()),
            pg_port: 5433,
            wal_position: None,
            last_health_check: None,
            replication_lag_ms: None,
        }
    }

    #[test]
    fn test_pool_creation() {
        let pool = PgPool::new(test_config());
        assert!(!pool.has_primary());
        assert_eq!(pool.replica_count(), 0);
    }

    #[test]
    fn test_connect_primary() {
        let pool = PgPool::new(test_config());
        let info = make_pg_info("primary-1", PgNodeRole::Primary, "10.0.0.1:5433");

        // This builds the pool config but won't actually connect (no PG running)
        let result = pool.connect_primary(&info);
        assert!(result.is_ok());
        assert!(pool.has_primary());
        assert_eq!(pool.primary_node_id(), Some(info.node_id));
    }

    #[test]
    fn test_add_replica() {
        let pool = PgPool::new(test_config());
        let r1 = make_pg_info("replica-1", PgNodeRole::Replica, "10.0.0.2:5433");
        let r2 = make_pg_info("replica-2", PgNodeRole::Replica, "10.0.0.3:5433");

        pool.add_replica(&r1).unwrap();
        assert_eq!(pool.replica_count(), 1);

        pool.add_replica(&r2).unwrap();
        assert_eq!(pool.replica_count(), 2);
    }

    #[test]
    fn test_add_replica_replaces_existing() {
        let pool = PgPool::new(test_config());
        let r1 = make_pg_info("replica-1", PgNodeRole::Replica, "10.0.0.2:5433");

        pool.add_replica(&r1).unwrap();
        assert_eq!(pool.replica_count(), 1);

        // Adding same node again should replace, not duplicate
        pool.add_replica(&r1).unwrap();
        assert_eq!(pool.replica_count(), 1);
    }

    #[test]
    fn test_remove_primary() {
        let pool = PgPool::new(test_config());
        let info = make_pg_info("primary-1", PgNodeRole::Primary, "10.0.0.1:5433");

        pool.connect_primary(&info).unwrap();
        assert!(pool.has_primary());

        pool.remove_node(&info.node_id);
        assert!(!pool.has_primary());
    }

    #[test]
    fn test_remove_replica() {
        let pool = PgPool::new(test_config());
        let r1 = make_pg_info("replica-1", PgNodeRole::Replica, "10.0.0.2:5433");
        let r2 = make_pg_info("replica-2", PgNodeRole::Replica, "10.0.0.3:5433");

        pool.add_replica(&r1).unwrap();
        pool.add_replica(&r2).unwrap();
        assert_eq!(pool.replica_count(), 2);

        pool.remove_node(&r1.node_id);
        assert_eq!(pool.replica_count(), 1);
    }

    #[test]
    fn test_reconfigure_adds_and_removes() {
        let pool = PgPool::new(test_config());
        let primary = make_pg_info("primary-1", PgNodeRole::Primary, "10.0.0.1:5433");
        let r1 = make_pg_info("replica-1", PgNodeRole::Replica, "10.0.0.2:5433");

        // Initial: primary + 1 replica
        pool.reconfigure(&[primary.clone(), r1.clone()], "marabunta")
            .unwrap();
        assert!(pool.has_primary());
        assert_eq!(pool.replica_count(), 1);

        // Add a second replica
        let r2 = make_pg_info("replica-2", PgNodeRole::Replica, "10.0.0.3:5433");
        pool.reconfigure(&[primary.clone(), r1.clone(), r2.clone()], "marabunta")
            .unwrap();
        assert_eq!(pool.replica_count(), 2);

        // Remove r1 (only r2 remains)
        pool.reconfigure(&[primary.clone(), r2.clone()], "marabunta")
            .unwrap();
        assert_eq!(pool.replica_count(), 1);
    }

    #[test]
    fn test_reconfigure_skips_non_running() {
        let pool = PgPool::new(test_config());
        let primary = make_pg_info("primary-1", PgNodeRole::Primary, "10.0.0.1:5433");
        let mut stopped = make_pg_info("replica-1", PgNodeRole::Replica, "10.0.0.2:5433");
        stopped.status = PgStatus::Stopped;

        pool.reconfigure(&[primary, stopped], "marabunta").unwrap();
        assert!(pool.has_primary());
        assert_eq!(pool.replica_count(), 0);
    }

    #[test]
    fn test_connect_primary_no_listen_addr() {
        let pool = PgPool::new(test_config());
        let mut info = make_pg_info("primary-1", PgNodeRole::Primary, "10.0.0.1:5433");
        info.listen_addr = None;

        let result = pool.connect_primary(&info);
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_status() {
        let pool = PgPool::new(test_config());
        let primary = make_pg_info("primary-1", PgNodeRole::Primary, "10.0.0.1:5433");
        let r1 = make_pg_info("replica-1", PgNodeRole::Replica, "10.0.0.2:5433");

        pool.connect_primary(&primary).unwrap();
        pool.add_replica(&r1).unwrap();

        let status = pool.pool_status();
        assert_eq!(status.primary_node_id, Some(primary.node_id));
        assert_eq!(status.replica_count, 1);
        assert_eq!(status.replica_node_ids.len(), 1);
    }

    #[test]
    fn test_reconfigure_primary_change() {
        let pool = PgPool::new(test_config());
        let p1 = make_pg_info("primary-1", PgNodeRole::Primary, "10.0.0.1:5433");
        let p2 = make_pg_info("primary-2", PgNodeRole::Primary, "10.0.0.4:5433");

        pool.connect_primary(&p1).unwrap();
        assert_eq!(pool.primary_node_id(), Some(p1.node_id.clone()));

        // Failover: p2 is now primary
        pool.reconfigure(&[p2.clone()], "marabunta").unwrap();
        assert_eq!(pool.primary_node_id(), Some(p2.node_id));
    }

    #[tokio::test]
    async fn test_get_write_no_primary() {
        let pool = PgPool::new(test_config());
        let result = pool.get_write().await;
        assert!(result.is_err());
        match result {
            Err(PgError::NoPrimary) => {}
            other => panic!("Expected NoPrimary, got {:?}", other),
        }
    }
}
