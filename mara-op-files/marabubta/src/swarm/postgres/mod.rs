// Marabunta - Licensed under the MIT License.
//! Swarm-managed PostgreSQL subsystem.
//!
//! This module provides automatic PostgreSQL deployment, management, and
//! time-travel query capabilities for the Marabunta Swarm. PostgreSQL is
//! the **default** operational database for any swarm with ordinary machines
//! (8 GB+ RAM). It stores configuration history, node state, job events,
//! audit trails, and compliance records in temporal (append-only) tables.
//!
//! # Architecture
//!
//! ```text
//!                     ┌─────────────────────┐
//!                     │     PgManager        │
//!                     │  (orchestrator)      │
//!                     └──────┬──────────────┘
//!                            │
//!          ┌─────────┬───────┼───────┬──────────┐
//!          │         │       │       │          │
//!      PgDeployer  PgPool  Schema  Replication Failover
//!      (lifecycle) (conns) (DDL)   (streaming) (HA)
//!          │         │       │       │          │
//!          └─────────┴───────┼───────┴──────────┘
//!                            │
//!                    TimeTravelEngine
//!                    (temporal queries)
//! ```
//!
//! # Node eligibility
//!
//! - **Edge** (<1.5 GB): Never hosts PG. Uses SQLite locally, syncs to PG.
//! - **Light** (4-6 GB): Can host PG if RAM threshold met (default 4 GB).
//! - **Standard** / **Enterprise**: Always eligible. Primary and replica candidates.
//!
//! # Wiring
//!
//! The `PgManager` is wired into `SwarmNode` following the Neuromancer
//! pattern: `pg_manager: Option<Arc<PgManager>>`, gated by
//! `config.enable_postgres`.

pub mod config;
pub mod pool;
pub mod types;
pub mod schema;
pub mod deploy;
pub mod replication;
pub mod failover;

pub mod timetravel;
pub mod sync;


use parking_lot::RwLock;
use tracing::{debug, info};

use self::config::PgConfig;
use self::deploy::PgDeployer;
use self::failover::{FailoverDecision, FailoverEvent, PgFailoverHandler};
use self::pool::PgPool;
use self::replication::ReplicationManager;
use self::types::{
    PgClusterStatus, PgError, PgInstance, PgNodeInfo, PgNodeRole, PgResourceSnapshot, PgStatus,
};
use super::types::NodeId;

// ============================================================================
// PgManager
// ============================================================================

/// Central orchestrator for the swarm-managed PostgreSQL subsystem.
///
/// Created in `SwarmNode::new()` when `config.enable_postgres` is true.
/// Manages the full lifecycle: detection, deployment, connection pooling,
/// replication, failover, and schema migrations.
///
/// # Subsystems
///
/// - **PgDeployer**: detect, install, configure, start/stop PG
/// - **PgPool**: connection pooling (primary write, replica reads)
/// - **ReplicationManager**: primary election, replica setup, promotion
/// - **PgFailoverHandler**: death/rejoin handling for PG nodes
/// - **SchemaManager**: DDL migrations (applied on pool connect)
pub struct PgManager {
    config: PgConfig,
    node_id: NodeId,

    /// This node's role in the PG cluster.
    local_role: RwLock<PgNodeRole>,

    /// This node's PG instance status (if hosting PG).
    local_status: RwLock<PgStatus>,

    /// Known PG nodes in the cluster (populated via gossip).
    known_pg_nodes: dashmap::DashMap<NodeId, PgNodeInfo>,

    /// Connection pool (primary + replicas).
    pool: PgPool,

    /// Deployer (detection, lifecycle, config generation).
    deployer: PgDeployer,

    /// Replication manager (election, replica setup, promotion).
    replication_mgr: ReplicationManager,

    /// Failover handler (integrates with FailureDetector).
    failover_handler: PgFailoverHandler,

    /// Local PG instance handle (set after deployment/detection).
    local_instance: RwLock<Option<PgInstance>>,
}

impl PgManager {
    /// Create a new PgManager.
    ///
    /// Does not start any background tasks or connect to PG. Call
    /// `start()` after construction to initialize the subsystem.
    pub fn new(config: PgConfig, node_id: NodeId) -> Self {
        let pool = PgPool::new(config.clone());
        let deployer = PgDeployer::new(config.clone());
        let replication_mgr = ReplicationManager::new(config.clone());
        let failover_handler = PgFailoverHandler::new(config.clone());

        Self {
            config,
            node_id,
            local_role: RwLock::new(PgNodeRole::Ineligible),
            local_status: RwLock::new(PgStatus::NotInstalled),
            known_pg_nodes: dashmap::DashMap::new(),
            pool,
            deployer,
            replication_mgr,
            failover_handler,
            local_instance: RwLock::new(None),
        }
    }

    // ========================================================================
    // Accessors
    // ========================================================================

    /// Get the current PG configuration.
    pub fn config(&self) -> &PgConfig {
        &self.config
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the deployer.
    pub fn deployer(&self) -> &PgDeployer {
        &self.deployer
    }

    /// Get the replication manager.
    pub fn replication_mgr(&self) -> &ReplicationManager {
        &self.replication_mgr
    }

    /// Get the failover handler.
    pub fn failover_handler(&self) -> &PgFailoverHandler {
        &self.failover_handler
    }

    /// Get the local PG instance, if one has been detected/deployed.
    pub fn local_instance(&self) -> Option<PgInstance> {
        self.local_instance.read().clone()
    }

    // ========================================================================
    // Role and status
    // ========================================================================

    /// Get this node's current role in the PG cluster.
    pub fn local_role(&self) -> PgNodeRole {
        *self.local_role.read()
    }

    /// Get this node's current PG instance status.
    pub fn local_status(&self) -> PgStatus {
        self.local_status.read().clone()
    }

    /// Update this node's role.
    pub fn set_local_role(&self, role: PgNodeRole) {
        info!(node_id = %self.node_id, role = %role, "PG role changed");
        *self.local_role.write() = role;
    }

    /// Update this node's PG status.
    pub fn set_local_status(&self, status: PgStatus) {
        debug!(node_id = %self.node_id, status = %status, "PG status changed");
        *self.local_status.write() = status;
    }

    /// Set the local PG instance handle.
    pub fn set_local_instance(&self, instance: PgInstance) {
        *self.local_instance.write() = Some(instance);
    }

    // ========================================================================
    // Node tracking (gossip)
    // ========================================================================

    /// Register or update a PG node discovered via gossip.
    pub fn update_pg_node(&self, info: PgNodeInfo) {
        self.known_pg_nodes.insert(info.node_id, info);
    }

    /// Remove a PG node (e.g., when declared dead by failure detector).
    pub fn remove_pg_node(&self, node_id: &NodeId) {
        self.known_pg_nodes.remove(node_id);
        self.pool.remove_node(node_id);
    }

    /// Merge a batch of PG node info from gossip.
    pub fn merge_gossip_pg_nodes(&self, pg_nodes: &[PgNodeInfo]) {
        for info in pg_nodes {
            self.known_pg_nodes.insert(info.node_id, info.clone());
        }
    }

    /// Get all known PG nodes as a Vec (for gossip propagation).
    pub fn known_pg_nodes_vec(&self) -> Vec<PgNodeInfo> {
        self.known_pg_nodes
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Check whether a given node is known to host PG.
    pub fn is_pg_node(&self, node_id: &NodeId) -> bool {
        self.known_pg_nodes
            .get(node_id)
            .map(|info| matches!(info.role, PgNodeRole::Primary | PgNodeRole::Replica))
            .unwrap_or(false)
    }

    // ========================================================================
    // Cluster topology queries
    // ========================================================================

    /// Get the current primary node, if any.
    pub fn primary(&self) -> Option<PgNodeInfo> {
        self.known_pg_nodes
            .iter()
            .find(|entry| entry.role == PgNodeRole::Primary && entry.status.is_running())
            .map(|entry| entry.value().clone())
    }

    /// Get all known replica nodes.
    pub fn replicas(&self) -> Vec<PgNodeInfo> {
        self.known_pg_nodes
            .iter()
            .filter(|entry| entry.role == PgNodeRole::Replica && entry.status.is_running())
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Build an aggregate cluster status report.
    pub fn cluster_status(&self) -> PgClusterStatus {
        let primary = self.primary();
        let replicas = self.replicas();
        let mut clients = Vec::new();
        let mut ineligible = Vec::new();

        for entry in self.known_pg_nodes.iter() {
            match entry.role {
                PgNodeRole::Client => clients.push(entry.node_id),
                PgNodeRole::Ineligible => ineligible.push(entry.node_id),
                _ => {}
            }
        }

        let pg_node_count = if primary.is_some() { 1 } else { 0 } + replicas.len();
        let healthy = primary.is_some();

        PgClusterStatus {
            primary,
            replicas,
            clients,
            ineligible,
            healthy,
            pg_node_count,
        }
    }

    // ========================================================================
    // Failover integration
    // ========================================================================

    /// Handle a node death event from the failure detector.
    ///
    /// Returns the failover decision so the caller can take action
    /// (e.g., promote replica, reconfigure pools).
    pub fn handle_node_death(&self, dead_node_id: &NodeId) -> FailoverDecision {
        let dead_role = self
            .known_pg_nodes
            .get(dead_node_id)
            .map(|entry| entry.role)
            .unwrap_or(PgNodeRole::Ineligible);

        let all_nodes: Vec<PgNodeInfo> = self.known_pg_nodes_vec();

        let decision =
            self.failover_handler
                .evaluate_node_death(dead_node_id, dead_role, &all_nodes);

        // Apply immediate side-effects
        match &decision {
            FailoverDecision::RemoveReplica { dead_replica_id } => {
                self.remove_pg_node(dead_replica_id);
            }
            FailoverDecision::PromoteReplica {
                new_primary_id,
                dead_primary_id,
            } => {
                // Update cluster state: old primary removed, new primary role updated
                self.remove_pg_node(dead_primary_id);
                if let Some(mut entry) = self.known_pg_nodes.get_mut(new_primary_id) {
                    entry.role = PgNodeRole::Primary;
                }
            }
            _ => {}
        }

        decision
    }

    /// Handle a node rejoin event.
    ///
    /// Returns the primary's NodeId if the rejoining node should set up
    /// as a replica.
    pub fn handle_node_rejoin(&self, rejoining_node_id: &NodeId) -> Option<NodeId> {
        let primary = self.primary();
        self.failover_handler
            .evaluate_node_rejoin(rejoining_node_id, primary.as_ref())
    }

    /// Get recent failover events for diagnostics.
    pub fn failover_events(&self) -> Vec<FailoverEvent> {
        self.failover_handler.recent_events()
    }

    // ========================================================================
    // Pool reconfiguration
    // ========================================================================

    /// Reconfigure connection pools based on current cluster topology.
    pub fn reconfigure_pools(&self) -> Result<(), PgError> {
        let all_nodes: Vec<PgNodeInfo> = self.known_pg_nodes_vec();
        self.pool.reconfigure(&all_nodes, &self.config.database_name)
    }

    // ========================================================================
    // Local info for gossip
    // ========================================================================

    /// Get the PgNodeInfo for this node (for gossip propagation).
    pub fn local_info(&self) -> PgNodeInfo {
        let instance = self.local_instance.read();
        PgNodeInfo {
            node_id: self.node_id,
            role: self.local_role(),
            status: self.local_status(),
            pg_version: instance.as_ref().and_then(|i| i.version.clone()),
            listen_addr: None, // Set by caller with actual listen address
            pg_port: self.config.pg_port,
            wal_position: None,
            last_health_check: None,
            replication_lag_ms: None,
        }
    }

    /// Check resource eligibility for PG hosting.
    pub fn should_host_pg(&self, resources: &PgResourceSnapshot) -> bool {
        self.deployer.should_host_pg(resources)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node_id(suffix: &str) -> NodeId {
        use uuid::Uuid;
        NodeId(Uuid::new_v5(&Uuid::NAMESPACE_DNS, suffix.as_bytes()))
    }

    fn make_pg_info(suffix: &str, role: PgNodeRole, running: bool) -> PgNodeInfo {
        PgNodeInfo {
            node_id: test_node_id(suffix),
            role,
            status: if running {
                PgStatus::Running
            } else {
                PgStatus::Stopped
            },
            pg_version: Some("16.2".into()),
            listen_addr: Some(
                format!("10.0.0.{}:5433", suffix.len())
                    .parse()
                    .unwrap(),
            ),
            pg_port: 5433,
            wal_position: Some("0/1000".into()),
            last_health_check: None,
            replication_lag_ms: None,
        }
    }

    #[test]
    fn test_pg_manager_creation() {
        let config = PgConfig::default();
        let node_id = test_node_id("test-node-1");
        let mgr = PgManager::new(config, node_id.clone());

        assert_eq!(mgr.local_role(), PgNodeRole::Ineligible);
        assert!(!mgr.local_status().is_running());
        assert_eq!(mgr.node_id(), &node_id);
        assert!(!mgr.pool().has_primary());
    }

    #[test]
    fn test_role_and_status_updates() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));

        mgr.set_local_role(PgNodeRole::Primary);
        assert_eq!(mgr.local_role(), PgNodeRole::Primary);

        mgr.set_local_status(PgStatus::Running);
        assert!(mgr.local_status().is_running());
    }

    #[test]
    fn test_pg_node_tracking() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        let node2 = test_node_id("node-2");
        let node3 = test_node_id("node-3");

        mgr.update_pg_node(make_pg_info("node-2", PgNodeRole::Primary, true));
        mgr.update_pg_node(make_pg_info("node-3", PgNodeRole::Replica, true));

        assert!(mgr.is_pg_node(&node2));
        assert!(mgr.is_pg_node(&node3));
        assert!(!mgr.is_pg_node(&test_node_id("unknown")));

        let primary = mgr.primary().unwrap();
        assert_eq!(primary.node_id, node2);

        let replicas = mgr.replicas();
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].node_id, node3);
    }

    #[test]
    fn test_cluster_status() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));

        // Empty cluster
        let status = mgr.cluster_status();
        assert!(!status.healthy);
        assert_eq!(status.pg_node_count, 0);

        // Add primary
        mgr.update_pg_node(make_pg_info("node-2", PgNodeRole::Primary, true));

        let status = mgr.cluster_status();
        assert!(status.healthy);
        assert_eq!(status.pg_node_count, 1);
    }

    #[test]
    fn test_remove_pg_node() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        let node2 = test_node_id("node-2");

        mgr.update_pg_node(make_pg_info("node-2", PgNodeRole::Primary, true));

        assert!(mgr.is_pg_node(&node2));
        mgr.remove_pg_node(&node2);
        assert!(!mgr.is_pg_node(&node2));
    }

    #[test]
    fn test_local_info() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        mgr.set_local_role(PgNodeRole::Client);
        mgr.set_local_status(PgStatus::NotInstalled);

        let info = mgr.local_info();
        assert_eq!(info.role, PgNodeRole::Client);
        assert_eq!(info.pg_port, 5433);
        assert!(info.listen_addr.is_none());
    }

    #[test]
    fn test_merge_gossip_pg_nodes() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        let nodes = vec![
            make_pg_info("node-2", PgNodeRole::Primary, true),
            make_pg_info("node-3", PgNodeRole::Replica, true),
        ];

        mgr.merge_gossip_pg_nodes(&nodes);
        assert_eq!(mgr.known_pg_nodes_vec().len(), 2);
        assert!(mgr.is_pg_node(&test_node_id("node-2")));
        assert!(mgr.is_pg_node(&test_node_id("node-3")));
    }

    #[test]
    fn test_handle_node_death_primary() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        mgr.update_pg_node(make_pg_info("primary", PgNodeRole::Primary, false));
        mgr.update_pg_node(make_pg_info("replica-1", PgNodeRole::Replica, true));

        let decision = mgr.handle_node_death(&test_node_id("primary"));
        match decision {
            FailoverDecision::PromoteReplica {
                new_primary_id,
                dead_primary_id,
            } => {
                assert_eq!(dead_primary_id, test_node_id("primary"));
                assert_eq!(new_primary_id, test_node_id("replica-1"));
            }
            other => panic!("Expected PromoteReplica, got {:?}", other),
        }
    }

    #[test]
    fn test_handle_node_death_replica() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        mgr.update_pg_node(make_pg_info("primary", PgNodeRole::Primary, true));
        mgr.update_pg_node(make_pg_info("replica-1", PgNodeRole::Replica, true));

        let decision = mgr.handle_node_death(&test_node_id("replica-1"));
        match decision {
            FailoverDecision::RemoveReplica { dead_replica_id } => {
                assert_eq!(dead_replica_id, test_node_id("replica-1"));
            }
            other => panic!("Expected RemoveReplica, got {:?}", other),
        }

        // Replica should be removed from tracking
        assert!(!mgr.is_pg_node(&test_node_id("replica-1")));
    }

    #[test]
    fn test_handle_node_rejoin() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        mgr.update_pg_node(make_pg_info("primary", PgNodeRole::Primary, true));

        let result = mgr.handle_node_rejoin(&test_node_id("returning"));
        assert_eq!(result, Some(test_node_id("primary")));
    }

    #[test]
    fn test_local_instance() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));
        assert!(mgr.local_instance().is_none());

        mgr.set_local_instance(PgInstance {
            bin_dir: "/usr/lib/postgresql/16/bin".into(),
            data_dir: "/data/pgdata".into(),
            port: 5433,
            version: Some("16.2".into()),
            is_system_pg: true,
        });

        let inst = mgr.local_instance().unwrap();
        assert_eq!(inst.port, 5433);
        assert_eq!(inst.version, Some("16.2".into()));
    }

    #[test]
    fn test_should_host_pg() {
        let mgr = PgManager::new(PgConfig::default(), test_node_id("node-1"));

        let good = PgResourceSnapshot {
            memory_total_mb: 8192,
            memory_available_mb: 6144,
            disk_total_mb: 256_000,
            disk_available_mb: 200_000,
            cpu_cores: 4,
        };
        assert!(mgr.should_host_pg(&good));

        let edge = PgResourceSnapshot {
            memory_total_mb: 1024,
            memory_available_mb: 512,
            disk_total_mb: 16_000,
            disk_available_mb: 8_000,
            cpu_cores: 1,
        };
        assert!(!mgr.should_host_pg(&edge));
    }
}
