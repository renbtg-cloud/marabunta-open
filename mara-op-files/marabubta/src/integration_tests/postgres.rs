// Marabunta - Licensed under the MIT License.
//! PostgreSQL lifecycle and failover integration tests.
//!
//! Tests the PG subsystem's core decision-making logic:
//! - Resource eligibility (should_host_pg)
//! - PgManager state management (roles, status, cluster topology)
//! - Failover handler (primary death, replica death, rejoin)
//! - Replication manager (deterministic election)
//! - Schema manager (migration listing, hash computation)
//! - Deployer (config generation, resource gating)
//!
//! These tests run without a real PostgreSQL instance. They verify the
//! in-memory logic and decision-making paths. Full PG integration tests
//! (requiring Docker) are gated behind `#[cfg(feature = "pg-tests")]`.

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use chrono::Utc;

    use crate::swarm::postgres::config::PgConfig;
    use crate::swarm::postgres::deploy::PgDeployer;
    use crate::swarm::postgres::failover::{FailoverDecision, PgFailoverHandler};
    use crate::swarm::postgres::replication::{ElectionCandidate, ReplicationManager};
    use crate::swarm::postgres::schema::SchemaManager;
    use crate::swarm::postgres::types::{
        PgClusterStatus, PgNodeInfo, PgNodeRole, PgResourceSnapshot, PgStatus,
    };
    use crate::swarm::postgres::PgManager;
    use crate::swarm::types::NodeId;

    // ========================================================================
    // Helpers
    // ========================================================================

    fn default_pg_config() -> PgConfig {
        PgConfig::default()
    }

    fn make_node_id(suffix: &str) -> NodeId {
        NodeId(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_DNS,
            suffix.as_bytes(),
        ))
    }

    fn make_pg_node_info(
        suffix: &str,
        role: PgNodeRole,
        status: PgStatus,
        port: u16,
    ) -> PgNodeInfo {
        PgNodeInfo {
            node_id: make_node_id(suffix),
            role,
            status,
            pg_version: Some("16.2".to_string()),
            listen_addr: Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                port,
            )),
            pg_port: port,
            wal_position: Some("0/1000000".to_string()),
            last_health_check: Some(Utc::now()),
            replication_lag_ms: None,
        }
    }

    fn make_resources(memory_mb: u64, disk_mb: u64, cores: usize) -> PgResourceSnapshot {
        PgResourceSnapshot {
            memory_total_mb: memory_mb,
            memory_available_mb: memory_mb / 2,
            disk_total_mb: disk_mb,
            disk_available_mb: disk_mb / 2,
            cpu_cores: cores,
        }
    }

    // ========================================================================
    // PgManager lifecycle tests
    // ========================================================================

    #[test]
    fn pg_manager_initial_state() {
        let config = default_pg_config();
        let node_id = make_node_id("test-node-1");
        let mgr = PgManager::new(config.clone(), node_id.clone());

        assert_eq!(mgr.local_role(), PgNodeRole::Ineligible);
        assert!(!mgr.local_status().is_running());
        assert!(mgr.primary().is_none());
        assert!(mgr.replicas().is_empty());
        assert_eq!(mgr.known_pg_nodes_vec().len(), 0);
    }

    #[test]
    fn pg_manager_role_and_status_updates() {
        let config = default_pg_config();
        let node_id = make_node_id("test-node-2");
        let mgr = PgManager::new(config, node_id);

        mgr.set_local_role(PgNodeRole::Primary);
        assert_eq!(mgr.local_role(), PgNodeRole::Primary);

        mgr.set_local_status(PgStatus::Running);
        assert!(mgr.local_status().is_running());
    }

    #[test]
    fn pg_manager_cluster_tracking() {
        let config = default_pg_config();
        let node_id = make_node_id("manager-node");
        let mgr = PgManager::new(config, node_id);

        // Add primary
        let primary_info =
            make_pg_node_info("primary-1", PgNodeRole::Primary, PgStatus::Running, 5433);
        mgr.update_pg_node(primary_info.clone());

        assert!(mgr.is_pg_node(&make_node_id("primary-1")));
        assert!(mgr.primary().is_some());
        assert_eq!(mgr.known_pg_nodes_vec().len(), 1);

        // Add replicas
        let replica1 =
            make_pg_node_info("replica-1", PgNodeRole::Replica, PgStatus::Running, 5433);
        let replica2 =
            make_pg_node_info("replica-2", PgNodeRole::Replica, PgStatus::Running, 5433);
        mgr.update_pg_node(replica1);
        mgr.update_pg_node(replica2);

        assert_eq!(mgr.replicas().len(), 2);
        assert_eq!(mgr.known_pg_nodes_vec().len(), 3);

        // Remove a replica
        mgr.remove_pg_node(&make_node_id("replica-2"));
        assert_eq!(mgr.replicas().len(), 1);
        assert_eq!(mgr.known_pg_nodes_vec().len(), 2);
    }

    #[test]
    fn pg_manager_gossip_merge() {
        let config = default_pg_config();
        let mgr = PgManager::new(config, make_node_id("local"));

        let nodes = vec![
            make_pg_node_info("gn-1", PgNodeRole::Primary, PgStatus::Running, 5433),
            make_pg_node_info("gn-2", PgNodeRole::Replica, PgStatus::Running, 5433),
            make_pg_node_info("gn-3", PgNodeRole::Replica, PgStatus::Running, 5433),
        ];

        mgr.merge_gossip_pg_nodes(&nodes);
        assert_eq!(mgr.known_pg_nodes_vec().len(), 3);
        assert!(mgr.primary().is_some());
    }

    #[test]
    fn pg_manager_cluster_status() {
        let config = default_pg_config();
        let mgr = PgManager::new(config, make_node_id("local"));

        let primary =
            make_pg_node_info("cs-primary", PgNodeRole::Primary, PgStatus::Running, 5433);
        let replica =
            make_pg_node_info("cs-replica", PgNodeRole::Replica, PgStatus::Running, 5433);

        mgr.update_pg_node(primary);
        mgr.update_pg_node(replica);

        let status = mgr.cluster_status();
        assert!(status.healthy);
        assert!(status.primary.is_some());
        assert_eq!(status.replicas.len(), 1);
        assert_eq!(status.pg_node_count, 2);
    }

    // ========================================================================
    // Deployer resource eligibility tests
    // ========================================================================

    #[test]
    fn deployer_should_host_sufficient_resources() {
        let config = default_pg_config();
        let deployer = PgDeployer::new(config);

        // 8GB RAM, 50GB disk, 4 cores — should be eligible
        let resources = make_resources(8192, 51200, 4);
        assert!(deployer.should_host_pg(&resources));
    }

    #[test]
    fn deployer_dust_node_excluded() {
        let config = default_pg_config();
        let deployer = PgDeployer::new(config);

        // Edge node: 1.5GB RAM — too small for PG
        let resources = make_resources(1536, 20000, 2);
        assert!(!deployer.should_host_pg(&resources));
    }

    #[test]
    fn deployer_low_disk_excluded() {
        let config = default_pg_config();
        let deployer = PgDeployer::new(config);

        // Good RAM but minimal disk
        let resources = make_resources(8192, 500, 4);
        assert!(!deployer.should_host_pg(&resources));
    }

    #[test]
    fn deployer_pebble_node_eligible() {
        let config = default_pg_config();
        let deployer = PgDeployer::new(config);

        // Light: 4GB RAM, 10GB disk — should be eligible
        let resources = make_resources(4096, 10240, 2);
        assert!(deployer.should_host_pg(&resources));
    }

    #[test]
    fn deployer_config_generation() {
        let config = default_pg_config();
        let deployer = PgDeployer::new(config);

        let resources = make_resources(16384, 102400, 8);
        let pg_conf = deployer.generate_postgresql_conf(&resources);

        // Should contain tuned shared_buffers
        assert!(pg_conf.contains("shared_buffers"));
        assert!(pg_conf.contains("effective_cache_size"));
        assert!(pg_conf.contains("max_connections"));
    }

    #[test]
    fn deployer_hba_conf_generation() {
        let config = default_pg_config();
        let deployer = PgDeployer::new(config);

        let hba = deployer.generate_pg_hba_conf(&[
            "10.0.0.0/24".to_string(),
            "192.168.1.0/24".to_string(),
        ]);

        assert!(hba.contains("10.0.0.0/24"));
        assert!(hba.contains("192.168.1.0/24"));
        assert!(hba.contains("local"));
    }

    // ========================================================================
    // Failover handler tests
    // ========================================================================

    #[test]
    fn failover_primary_death_promotes_replica() {
        let config = default_pg_config();
        let handler = PgFailoverHandler::new(config);

        let dead_primary_id = make_node_id("dead-primary");
        let replicas = vec![
            make_pg_node_info("replica-a", PgNodeRole::Replica, PgStatus::Running, 5433),
            make_pg_node_info("replica-b", PgNodeRole::Replica, PgStatus::Running, 5433),
        ];

        let decision = handler.evaluate_node_death(
            &dead_primary_id,
            PgNodeRole::Primary,
            &replicas,
        );

        match decision {
            FailoverDecision::PromoteReplica {
                new_primary_id,
                dead_primary_id: dead_id,
            } => {
                assert_eq!(dead_id, dead_primary_id);
                // Should select one of the replicas
                assert!(
                    new_primary_id == make_node_id("replica-a")
                        || new_primary_id == make_node_id("replica-b")
                );
            }
            _ => panic!("Expected PromoteReplica, got {:?}", decision),
        }
    }

    #[test]
    fn failover_primary_death_no_replicas() {
        let config = default_pg_config();
        let handler = PgFailoverHandler::new(config);

        let dead_primary_id = make_node_id("lonely-primary");

        let decision = handler.evaluate_node_death(
            &dead_primary_id,
            PgNodeRole::Primary,
            &[], // No replicas
        );

        match decision {
            FailoverDecision::NoCandidates { dead_primary_id: id } => {
                assert_eq!(id, dead_primary_id);
            }
            _ => panic!("Expected NoCandidates, got {:?}", decision),
        }
    }

    #[test]
    fn failover_replica_death_removes() {
        let config = default_pg_config();
        let handler = PgFailoverHandler::new(config);

        let dead_replica_id = make_node_id("dead-replica");
        let other_nodes = vec![make_pg_node_info(
            "primary-still-alive",
            PgNodeRole::Primary,
            PgStatus::Running,
            5433,
        )];

        let decision = handler.evaluate_node_death(
            &dead_replica_id,
            PgNodeRole::Replica,
            &other_nodes,
        );

        match decision {
            FailoverDecision::RemoveReplica {
                dead_replica_id: id,
            } => {
                assert_eq!(id, dead_replica_id);
            }
            _ => panic!("Expected RemoveReplica, got {:?}", decision),
        }
    }

    #[test]
    fn failover_non_pg_node_death() {
        let config = default_pg_config();
        let handler = PgFailoverHandler::new(config);

        let client_id = make_node_id("client-node");

        let decision =
            handler.evaluate_node_death(&client_id, PgNodeRole::Client, &[]);

        match decision {
            FailoverDecision::NotPgNode => {}
            _ => panic!("Expected NotPgNode, got {:?}", decision),
        }
    }

    #[test]
    fn failover_event_tracking() {
        let config = default_pg_config();
        let handler = PgFailoverHandler::new(config);

        // Initially no events
        assert_eq!(handler.event_count(), 0);

        // Trigger a failover
        let replicas = vec![make_pg_node_info(
            "replica-x",
            PgNodeRole::Replica,
            PgStatus::Running,
            5433,
        )];
        let _ = handler.evaluate_node_death(
            &make_node_id("dead-primary-x"),
            PgNodeRole::Primary,
            &replicas,
        );

        assert!(handler.event_count() > 0);
        let events = handler.recent_events();
        assert!(!events.is_empty());
    }

    #[test]
    fn failover_node_rejoin() {
        let config = default_pg_config();
        let handler = PgFailoverHandler::new(config);

        let current_primary = make_pg_node_info(
            "current-primary",
            PgNodeRole::Primary,
            PgStatus::Running,
            5433,
        );
        let rejoining_id = make_node_id("returning-node");

        let result = handler.evaluate_node_rejoin(&rejoining_id, Some(&current_primary));

        // Should return the primary's node_id so the rejoining node can replicate from it
        assert!(result.is_some());
        assert_eq!(result.unwrap(), make_node_id("current-primary"));
    }

    // ========================================================================
    // Replication manager tests
    // ========================================================================

    #[test]
    fn replication_deterministic_election() {
        let config = default_pg_config();
        let mgr = ReplicationManager::new(config);

        let candidates = vec![
            ElectionCandidate {
                node_id: make_node_id("cand-a"),
                wal_position: Some("0/2000000".to_string()),
                replication_lag_ms: Some(10),
                last_health_check: Some(Utc::now()),
                status: PgStatus::Running,
            },
            ElectionCandidate {
                node_id: make_node_id("cand-b"),
                wal_position: Some("0/3000000".to_string()),
                replication_lag_ms: Some(5),
                last_health_check: Some(Utc::now()),
                status: PgStatus::Running,
            },
        ];

        let elected1 = mgr.elect_primary(&candidates);
        let elected2 = mgr.elect_primary(&candidates);

        // Deterministic — same result both times
        assert_eq!(elected1, elected2);
        assert!(elected1.is_some());
    }

    #[test]
    fn replication_election_no_candidates() {
        let config = default_pg_config();
        let mgr = ReplicationManager::new(config);

        let elected = mgr.elect_primary(&[]);
        assert!(elected.is_none());
    }

    #[test]
    fn replication_election_prefers_healthy_node() {
        let config = default_pg_config();
        let mgr = ReplicationManager::new(config);

        let candidates = vec![
            ElectionCandidate {
                node_id: make_node_id("healthy"),
                wal_position: Some("0/1000000".to_string()),
                replication_lag_ms: Some(5),
                last_health_check: Some(Utc::now()),
                status: PgStatus::Running,
            },
            ElectionCandidate {
                node_id: make_node_id("lagging"),
                wal_position: Some("0/1000000".to_string()),
                replication_lag_ms: Some(5000),
                last_health_check: Some(Utc::now()),
                status: PgStatus::Running,
            },
        ];

        let elected = mgr.elect_primary(&candidates);
        assert!(elected.is_some());
        // The healthy node should be preferred (lower lag)
        assert_eq!(elected.unwrap(), make_node_id("healthy"));
    }

    // ========================================================================
    // Schema manager tests
    // ========================================================================

    #[test]
    fn schema_migrations_registered() {
        let total = SchemaManager::total_migrations();
        assert!(total >= 1, "Should have at least the initial migration");

        let migrations = SchemaManager::registered_migrations();
        assert_eq!(migrations.len(), total);

        // First migration should be version 1
        assert_eq!(migrations[0].0, 1);
    }

    #[test]
    fn schema_compute_row_hash_deterministic() {
        use crate::swarm::postgres::schema::compute_row_hash;

        let hash1 = compute_row_hash(Some("genesis"), &["key1", "value1", "admin"]);
        let hash2 = compute_row_hash(Some("genesis"), &["key1", "value1", "admin"]);

        assert_eq!(hash1, hash2);
        // blake3 produces 64 hex chars
        assert_eq!(hash1.len(), 64);
    }

    #[test]
    fn schema_compute_row_hash_changes_with_input() {
        use crate::swarm::postgres::schema::compute_row_hash;

        let hash_a = compute_row_hash(Some("genesis"), &["key1", "value1"]);
        let hash_b = compute_row_hash(Some("genesis"), &["key1", "value2"]);

        assert_ne!(hash_a, hash_b);
    }

    #[test]
    fn schema_hash_chain_includes_prev() {
        use crate::swarm::postgres::schema::compute_row_hash;

        let hash_with_genesis = compute_row_hash(Some("genesis"), &["data"]);
        let hash_with_different_prev = compute_row_hash(Some("abc123"), &["data"]);

        assert_ne!(hash_with_genesis, hash_with_different_prev);
    }

    // ========================================================================
    // PgNodeInfo tests
    // ========================================================================

    #[test]
    fn pg_node_info_connection_string() {
        let info = make_pg_node_info("conn-test", PgNodeRole::Primary, PgStatus::Running, 5433);
        let conn_str = info.connection_string("marabunta_swarm");

        assert!(conn_str.is_some());
        let conn = conn_str.unwrap();
        assert!(conn.contains("10.0.0.1"));
        assert!(conn.contains("5433"));
        assert!(conn.contains("marabunta_swarm"));
    }

    #[test]
    fn pg_node_info_no_addr_no_connection_string() {
        let mut info =
            make_pg_node_info("no-addr", PgNodeRole::Replica, PgStatus::Running, 5433);
        info.listen_addr = None;

        assert!(info.connection_string("test_db").is_none());
    }

    #[test]
    fn pg_status_predicates() {
        assert!(PgStatus::Running.is_running());
        assert!(!PgStatus::Running.is_failed());
        assert!(!PgStatus::Stopped.is_running());
        assert!(!PgStatus::NotInstalled.is_running());
        assert!(PgStatus::Failed("OOM".to_string()).is_failed());
    }

    #[test]
    fn pg_node_info_serde_roundtrip() {
        let info = make_pg_node_info("serde-test", PgNodeRole::Primary, PgStatus::Running, 5433);
        let json = serde_json::to_string(&info).unwrap();
        let back: PgNodeInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(back.node_id, info.node_id);
        assert_eq!(back.role, PgNodeRole::Primary);
        assert_eq!(back.pg_port, 5433);
    }

    #[test]
    fn pg_cluster_status_serde() {
        let status = PgClusterStatus {
            primary: Some(make_pg_node_info(
                "p",
                PgNodeRole::Primary,
                PgStatus::Running,
                5433,
            )),
            replicas: vec![],
            clients: vec![],
            ineligible: vec![],
            healthy: true,
            pg_node_count: 1,
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: PgClusterStatus = serde_json::from_str(&json).unwrap();
        assert!(back.healthy);
    }

    // ========================================================================
    // PgConfig tests
    // ========================================================================

    #[test]
    fn pg_config_defaults() {
        let config = PgConfig::default();
        assert!(config.min_ram_for_pg_mb >= 2048);
        assert!(config.pg_port > 0);
        assert!(config.replication_factor >= 1);
        assert!(config.pool_size >= 1);
        assert!(config.shared_buffers_fraction > 0.0);
    }

    #[test]
    fn pg_config_serde_roundtrip() {
        let config = PgConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let back: PgConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.pg_port, config.pg_port);
        assert_eq!(back.replication_factor, config.replication_factor);
    }
}
