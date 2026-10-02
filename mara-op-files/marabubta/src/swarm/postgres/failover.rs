// Marabunta - Licensed under the MIT License.
//! Failover handler for the swarm-managed PostgreSQL cluster.
//!
//! Integrates with the existing [`FailureDetector`] — when it declares a PG
//! node dead, the failover handler:
//!
//! 1. If the dead node was the **primary**: triggers election among replicas,
//!    winner promotes itself, all pools reconfigure.
//! 2. If the dead node was a **replica**: removes its pool, drops its
//!    replication slot on the primary, adjusts replication factor tracking.
//! 3. On **node rejoin**: the returning node becomes a replica automatically,
//!    setting up streaming replication from the current primary.
//!
//! The handler is designed to be called from the failure detector's sweep loop,
//! not to run its own polling. It's a stateless callback — all state lives
//! in `PgManager`.


use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use super::config::PgConfig;
use super::replication::{ElectionCandidate, ReplicationManager};
use super::types::{PgNodeInfo, PgNodeRole};
use crate::swarm::types::NodeId;

// ============================================================================
// Failover event types
// ============================================================================

/// Events produced by the failover handler for logging and gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailoverEvent {
    /// A PG primary was declared dead and failover was initiated.
    PrimaryDead {
        dead_node_id: NodeId,
        timestamp: chrono::DateTime<Utc>,
    },
    /// A new primary was elected from the replicas.
    PrimaryElected {
        new_primary_id: NodeId,
        previous_primary_id: NodeId,
        timestamp: chrono::DateTime<Utc>,
    },
    /// A replica was declared dead and removed.
    ReplicaDead {
        dead_node_id: NodeId,
        timestamp: chrono::DateTime<Utc>,
    },
    /// A node rejoined and is setting up as a replica.
    NodeRejoining {
        node_id: NodeId,
        timestamp: chrono::DateTime<Utc>,
    },
    /// Failover failed — no eligible replicas to promote.
    FailoverFailed {
        dead_primary_id: NodeId,
        reason: String,
        timestamp: chrono::DateTime<Utc>,
    },
}

// ============================================================================
// FailoverDecision
// ============================================================================

/// The decision made by the failover handler after evaluating a node death.
#[derive(Debug, Clone)]
pub enum FailoverDecision {
    /// Promote a replica to primary.
    PromoteReplica {
        new_primary_id: NodeId,
        dead_primary_id: NodeId,
    },
    /// Simply remove the dead replica (no promotion needed).
    RemoveReplica {
        dead_replica_id: NodeId,
    },
    /// The dead node was not part of the PG cluster (no action needed).
    NotPgNode,
    /// No viable replica to promote — cluster is degraded.
    NoCandidates {
        dead_primary_id: NodeId,
    },
}

// ============================================================================
// PgFailoverHandler
// ============================================================================

/// Stateless failover handler for the PG cluster.
///
/// Called by the failure detector when a PG node is declared dead.
/// All persistent state is accessed via the methods' parameters.
pub struct PgFailoverHandler {
    config: PgConfig,
    replication_mgr: ReplicationManager,
    /// Failover event log (bounded ring buffer for diagnostics).
    events: RwLock<Vec<FailoverEvent>>,
}

/// Maximum number of failover events to retain.
const MAX_FAILOVER_EVENTS: usize = 100;

impl PgFailoverHandler {
    pub fn new(config: PgConfig) -> Self {
        Self {
            replication_mgr: ReplicationManager::new(config.clone()),
            config,
            events: RwLock::new(Vec::new()),
        }
    }

    /// Evaluate what action to take when a node is declared dead.
    ///
    /// This is a pure decision function — it doesn't execute the failover,
    /// just decides what should happen. The caller (PgManager) executes it.
    pub fn evaluate_node_death(
        &self,
        dead_node_id: &NodeId,
        dead_node_role: PgNodeRole,
        known_pg_nodes: &[PgNodeInfo],
    ) -> FailoverDecision {
        match dead_node_role {
            PgNodeRole::Primary => {
                self.record_event(FailoverEvent::PrimaryDead {
                    dead_node_id: *dead_node_id,
                    timestamp: Utc::now(),
                });

                // Gather replica candidates
                let candidates: Vec<ElectionCandidate> = known_pg_nodes
                    .iter()
                    .filter(|n| {
                        n.node_id != *dead_node_id
                            && n.role == PgNodeRole::Replica
                            && n.status.is_running()
                    })
                    .map(ElectionCandidate::from_node_info)
                    .collect();

                match self.replication_mgr.elect_primary(&candidates) {
                    Some(new_primary_id) => {
                        self.record_event(FailoverEvent::PrimaryElected {
                            new_primary_id: new_primary_id,
                            previous_primary_id: *dead_node_id,
                            timestamp: Utc::now(),
                        });
                        info!(
                            dead_primary = %dead_node_id,
                            new_primary = %new_primary_id,
                            "Failover: elected new primary"
                        );
                        FailoverDecision::PromoteReplica {
                            new_primary_id,
                            dead_primary_id: *dead_node_id,
                        }
                    }
                    None => {
                        let reason = format!(
                            "No eligible replicas (candidates: {})",
                            candidates.len()
                        );
                        self.record_event(FailoverEvent::FailoverFailed {
                            dead_primary_id: *dead_node_id,
                            reason: reason.clone(),
                            timestamp: Utc::now(),
                        });
                        error!(
                            dead_primary = %dead_node_id,
                            reason = %reason,
                            "Failover FAILED"
                        );
                        FailoverDecision::NoCandidates {
                            dead_primary_id: *dead_node_id,
                        }
                    }
                }
            }

            PgNodeRole::Replica => {
                self.record_event(FailoverEvent::ReplicaDead {
                    dead_node_id: *dead_node_id,
                    timestamp: Utc::now(),
                });
                info!(dead_replica = %dead_node_id, "Removing dead replica from cluster");
                FailoverDecision::RemoveReplica {
                    dead_replica_id: *dead_node_id,
                }
            }

            PgNodeRole::Client | PgNodeRole::Ineligible => {
                FailoverDecision::NotPgNode
            }
        }
    }

    /// Evaluate what to do when a previously-dead node comes back.
    ///
    /// The default policy: returning nodes always become replicas.
    pub fn evaluate_node_rejoin(
        &self,
        rejoining_node_id: &NodeId,
        current_primary: Option<&PgNodeInfo>,
    ) -> Option<NodeId> {
        if let Some(primary) = current_primary {
            self.record_event(FailoverEvent::NodeRejoining {
                node_id: *rejoining_node_id,
                timestamp: Utc::now(),
            });
            info!(
                node = %rejoining_node_id,
                primary = %primary.node_id,
                "Rejoining node will become replica"
            );
            Some(primary.node_id)
        } else {
            warn!(
                node = %rejoining_node_id,
                "Node rejoining but no primary exists"
            );
            None
        }
    }

    /// Get recent failover events for diagnostics.
    pub fn recent_events(&self) -> Vec<FailoverEvent> {
        self.events.read().clone()
    }

    /// Get the number of failover events recorded.
    pub fn event_count(&self) -> usize {
        self.events.read().len()
    }

    /// Record a failover event, maintaining the bounded buffer.
    fn record_event(&self, event: FailoverEvent) {
        let mut events = self.events.write();
        if events.len() >= MAX_FAILOVER_EVENTS {
            events.remove(0);
        }
        events.push(event);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::postgres::types::{PgNodeRole, PgStatus};
    use std::net::SocketAddr;
    use uuid::Uuid;

    fn test_config() -> PgConfig {
        PgConfig::default()
    }

    fn test_node_id(suffix: &str) -> NodeId {
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
                    .parse::<SocketAddr>()
                    .unwrap(),
            ),
            pg_port: 5433,
            wal_position: Some("0/1000".into()),
            last_health_check: Some(Utc::now()),
            replication_lag_ms: Some(10),
        }
    }

    #[test]
    fn test_primary_death_triggers_election() {
        let handler = PgFailoverHandler::new(test_config());
        let dead = test_node_id("primary-1");
        let nodes = vec![
            make_pg_info("primary-1", PgNodeRole::Primary, false),
            make_pg_info("replica-1", PgNodeRole::Replica, true),
            make_pg_info("replica-2", PgNodeRole::Replica, true),
        ];

        let decision = handler.evaluate_node_death(&dead, PgNodeRole::Primary, &nodes);

        match decision {
            FailoverDecision::PromoteReplica {
                new_primary_id,
                dead_primary_id,
            } => {
                assert_eq!(dead_primary_id, dead);
                assert_ne!(new_primary_id, dead);
            }
            other => panic!("Expected PromoteReplica, got {:?}", other),
        }
    }

    #[test]
    fn test_primary_death_no_replicas() {
        let handler = PgFailoverHandler::new(test_config());
        let dead = test_node_id("primary-1");
        let nodes = vec![
            make_pg_info("primary-1", PgNodeRole::Primary, false),
            make_pg_info("client-1", PgNodeRole::Client, true),
        ];

        let decision = handler.evaluate_node_death(&dead, PgNodeRole::Primary, &nodes);

        match decision {
            FailoverDecision::NoCandidates { dead_primary_id } => {
                assert_eq!(dead_primary_id, dead);
            }
            other => panic!("Expected NoCandidates, got {:?}", other),
        }
    }

    #[test]
    fn test_replica_death_removes() {
        let handler = PgFailoverHandler::new(test_config());
        let dead = test_node_id("replica-1");
        let nodes = vec![
            make_pg_info("primary-1", PgNodeRole::Primary, true),
            make_pg_info("replica-1", PgNodeRole::Replica, false),
        ];

        let decision = handler.evaluate_node_death(&dead, PgNodeRole::Replica, &nodes);

        match decision {
            FailoverDecision::RemoveReplica { dead_replica_id } => {
                assert_eq!(dead_replica_id, dead);
            }
            other => panic!("Expected RemoveReplica, got {:?}", other),
        }
    }

    #[test]
    fn test_client_death_no_action() {
        let handler = PgFailoverHandler::new(test_config());
        let dead = test_node_id("client-1");

        let decision = handler.evaluate_node_death(&dead, PgNodeRole::Client, &[]);

        assert!(matches!(decision, FailoverDecision::NotPgNode));
    }

    #[test]
    fn test_node_rejoin_becomes_replica() {
        let handler = PgFailoverHandler::new(test_config());
        let rejoining = test_node_id("returning-node");
        let primary = make_pg_info("primary-1", PgNodeRole::Primary, true);

        let result = handler.evaluate_node_rejoin(&rejoining, Some(&primary));
        assert_eq!(result, Some(primary.node_id));
    }

    #[test]
    fn test_node_rejoin_no_primary() {
        let handler = PgFailoverHandler::new(test_config());
        let rejoining = test_node_id("returning-node");

        let result = handler.evaluate_node_rejoin(&rejoining, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_failover_events_recorded() {
        let handler = PgFailoverHandler::new(test_config());
        let dead = test_node_id("primary-1");
        let nodes = vec![
            make_pg_info("primary-1", PgNodeRole::Primary, false),
            make_pg_info("replica-1", PgNodeRole::Replica, true),
        ];

        assert_eq!(handler.event_count(), 0);

        handler.evaluate_node_death(&dead, PgNodeRole::Primary, &nodes);

        // Should record: PrimaryDead + PrimaryElected
        assert_eq!(handler.event_count(), 2);
    }

    #[test]
    fn test_failover_event_buffer_bounded() {
        let handler = PgFailoverHandler::new(test_config());

        // Fill the buffer past its limit
        for i in 0..(MAX_FAILOVER_EVENTS + 20) {
            let dead = test_node_id(&format!("replica-{i}"));
            handler.evaluate_node_death(&dead, PgNodeRole::Replica, &[]);
        }

        assert!(handler.event_count() <= MAX_FAILOVER_EVENTS);
    }

    #[test]
    fn test_primary_death_with_stopped_replicas() {
        let handler = PgFailoverHandler::new(test_config());
        let dead = test_node_id("primary-1");
        let nodes = vec![
            make_pg_info("primary-1", PgNodeRole::Primary, false),
            make_pg_info("replica-1", PgNodeRole::Replica, false), // stopped
            make_pg_info("replica-2", PgNodeRole::Replica, true),  // running
        ];

        let decision = handler.evaluate_node_death(&dead, PgNodeRole::Primary, &nodes);

        match decision {
            FailoverDecision::PromoteReplica { new_primary_id, .. } => {
                assert_eq!(new_primary_id, test_node_id("replica-2"));
            }
            other => panic!("Expected PromoteReplica, got {:?}", other),
        }
    }
}
