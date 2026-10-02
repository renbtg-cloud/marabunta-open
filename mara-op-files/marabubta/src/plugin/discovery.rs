// Marabunta - Licensed under the MIT License.
//! Node discovery bridge for the plugin system.
//!
//! Translates plugin-facing discovery requests into swarm knowledge store
//! queries. Plugins see only sanitized [`PluginNodeInfo`] values — they
//! never touch raw gossip vectors, reputation scores, or swarm internals.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;

use crate::plugin::types::{
    FindNodesRequest, FindNodesResponse, GetNodeInfoResponse, IsNodeAliveRequest,
    IsNodeAliveResponse, PluginError, PluginNodeInfo, PluginResult,
};
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::{NodeId, NodeStatus, Trait};

// ============================================================================
// Helpers
// ============================================================================

/// Parse a trait name string into a [`Trait`] enum value.
///
/// Accepts both PascalCase (`"CanExecute"`) and snake_case (`"can_execute"`)
/// forms, case-insensitively.
pub fn parse_trait(name: &str) -> Option<Trait> {
    let lower = name.to_lowercase();
    match lower.as_str() {
        "canexecute" | "can_execute" => Some(Trait::CanExecute),
        "canforward" | "can_forward" => Some(Trait::CanForward),
        "canaggregate" | "can_aggregate" => Some(Trait::CanAggregate),
        "canstorestate" | "can_store_state" => Some(Trait::CanStoreState),
        "candiscover" | "can_discover" => Some(Trait::CanDiscover),
        "canrelay" | "can_relay" => Some(Trait::CanRelay),
        "blindexecution" | "blind_compute" => Some(Trait::BlindCompute),
        _ => None,
    }
}

/// Convert a [`Trait`] to its canonical string representation using Display.
pub fn trait_to_string(t: &Trait) -> String {
    t.to_string()
}

// ============================================================================
// DiscoveryBridge
// ============================================================================

/// Bridge between the plugin discovery API and the swarm knowledge store.
///
/// Holds a reference to the local node's identity, region, knowledge store,
/// and current trait set. All methods are synchronous because the knowledge
/// store uses `DashMap` for lock-free reads.
pub struct DiscoveryBridge {
    node_id: NodeId,
    region: String,
    knowledge: Arc<KnowledgeStore>,
    current_traits: Arc<parking_lot::RwLock<HashSet<Trait>>>,
}

impl DiscoveryBridge {
    /// Create a new discovery bridge.
    pub fn new(
        node_id: NodeId,
        region: String,
        knowledge: Arc<KnowledgeStore>,
        current_traits: Arc<parking_lot::RwLock<HashSet<Trait>>>,
    ) -> Self {
        Self {
            node_id,
            region,
            knowledge,
            current_traits,
        }
    }

    /// Find nodes matching the criteria in the request.
    ///
    /// Steps:
    /// 1. Get all live nodes from the knowledge store.
    /// 2. Filter to nodes that possess every required trait.
    /// 3. Sort by region preference (matching region first), then by load ascending.
    /// 4. Truncate to the requested limit.
    /// 5. Convert each `NodeInfo` into a sanitized `PluginNodeInfo`.
    pub fn find_nodes(&self, request: FindNodesRequest) -> PluginResult<FindNodesResponse> {
        // Parse required trait strings into Trait enum values.
        let required_traits: Vec<Trait> = request
            .required_traits
            .iter()
            .filter_map(|name| parse_trait(name))
            .collect();

        // Get all live nodes from the knowledge store.
        let all_nodes = self.knowledge.get_all_nodes();

        // Filter nodes that have all required traits.
        let mut matching: Vec<_> = all_nodes
            .into_iter()
            .filter(|node| {
                required_traits.iter().all(|t| node.traits.contains(t))
            })
            .collect();

        // Sort: prefer_region matches first, then ascending load.
        let prefer_region = request.prefer_region.clone();
        matching.sort_by(|a, b| {
            // Compute a region score: nodes "in" the preferred region sort first.
            // We use the node_id string as a stand-in for region since NodeInfo
            // does not carry a region field. For now, the prefer_region hint is
            // matched against the node's address string if available.
            let a_region_match = Self::node_region_matches(a, &prefer_region);
            let b_region_match = Self::node_region_matches(b, &prefer_region);

            // Region match first (true < false so matching sorts first)
            b_region_match
                .cmp(&a_region_match)
                .then_with(|| a.load.partial_cmp(&b.load).unwrap_or(std::cmp::Ordering::Equal))
        });

        // Apply limit.
        let limit = request.limit as usize;
        if limit > 0 {
            matching.truncate(limit);
        }

        // Convert to PluginNodeInfo.
        let nodes = matching
            .into_iter()
            .map(|node| {
                let health_score = match node.status {
                    NodeStatus::Alive => 1.0 - node.load,
                    NodeStatus::Suspect
                    | NodeStatus::Draining
                    | NodeStatus::Cordoned
                    | NodeStatus::Updating => 0.5,
                    NodeStatus::Dead | NodeStatus::Quarantined => 0.0,
                };
                PluginNodeInfo {
                    node_id: node.node_id.0.to_string(),
                    traits: node.traits.iter().map(trait_to_string).collect(),
                    region: String::new(),
                    health_score,
                    load: node.load,
                }
            })
            .collect();

        Ok(FindNodesResponse { nodes })
    }

    /// Check whether a specific node is alive.
    ///
    /// Parses the node_id string as a UUID, looks it up in the knowledge
    /// store, and returns the liveness status plus milliseconds since last seen.
    pub fn is_node_alive(&self, request: IsNodeAliveRequest) -> PluginResult<IsNodeAliveResponse> {
        let uuid = uuid::Uuid::parse_str(&request.node_id).map_err(|e| {
            PluginError::Discovery(format!("invalid node_id UUID: {}", e))
        })?;
        let target_id = NodeId(uuid);

        match self.knowledge.get_node(&target_id) {
            Some(info) => {
                let alive = info.status == NodeStatus::Alive;
                let now = Utc::now();
                let delta = now.signed_duration_since(info.last_seen);
                let last_seen_ms = delta.num_milliseconds().max(0) as u64;
                Ok(IsNodeAliveResponse {
                    alive,
                    last_seen_ms,
                })
            }
            None => Ok(IsNodeAliveResponse {
                alive: false,
                last_seen_ms: 0,
            }),
        }
    }

    /// Return this node's own identity, region, and current traits.
    pub fn get_node_info(&self) -> PluginResult<GetNodeInfoResponse> {
        let traits = self.current_traits.read();
        Ok(GetNodeInfoResponse {
            node_id: self.node_id.0.to_string(),
            region: self.region.clone(),
            traits: traits.iter().map(trait_to_string).collect(),
        })
    }

    /// Heuristic region match for a node.
    ///
    /// Since `NodeInfo` does not carry a region field, this returns `true`
    /// if the prefer_region string is empty (all nodes match), or `false`
    /// otherwise. When the swarm gains region metadata this can be refined.
    fn node_region_matches(
        node: &crate::swarm::types::NodeInfo,
        prefer_region: &str,
    ) -> bool {
        if prefer_region.is_empty() {
            return true;
        }
        // NodeInfo does not expose a region field directly. If an address
        // is available we could geo-locate it, but for now we conservatively
        // return false for any non-empty region hint, letting the sort put
        // region-unknown nodes after region-matched ones if any ever appear.
        let _ = node;
        false
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::types::{NodeInfo, NodeStatus, ResourceSnapshot, Trait};
    use chrono::Utc;
    use std::collections::HashSet;
    use std::sync::Arc;

    /// Helper: create a `KnowledgeStore` and insert some test nodes.
    fn setup_knowledge(nodes: Vec<NodeInfo>) -> Arc<KnowledgeStore> {
        let store = Arc::new(KnowledgeStore::new(NodeId::new()));
        for node in nodes {
            store.merge_node(node);
        }
        store
    }

    /// Helper: build a `NodeInfo` with specified traits, load, and status.
    fn make_node_info(
        traits: HashSet<Trait>,
        load: f32,
        status: NodeStatus,
    ) -> NodeInfo {
        let id = NodeId::new();
        NodeInfo {
            node_id: id,
            last_seen: Utc::now(),
            traits,
            load,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: id,
            status,
            generation: 1,
            trust_level: Default::default(),
        }
    }

    fn make_bridge(knowledge: Arc<KnowledgeStore>) -> DiscoveryBridge {
        let traits: HashSet<Trait> = [Trait::CanExecute, Trait::CanForward].into_iter().collect();
        DiscoveryBridge::new(
            NodeId::new(),
            "us-east-1".to_string(),
            knowledge,
            Arc::new(parking_lot::RwLock::new(traits)),
        )
    }

    // ---- parse_trait ----

    #[test]
    fn parse_trait_snake_case() {
        assert_eq!(parse_trait("can_execute"), Some(Trait::CanExecute));
        assert_eq!(parse_trait("can_forward"), Some(Trait::CanForward));
        assert_eq!(parse_trait("can_aggregate"), Some(Trait::CanAggregate));
        assert_eq!(parse_trait("can_store_state"), Some(Trait::CanStoreState));
        assert_eq!(parse_trait("can_discover"), Some(Trait::CanDiscover));
        assert_eq!(parse_trait("can_relay"), Some(Trait::CanRelay));
    }

    #[test]
    fn parse_trait_pascal_case() {
        assert_eq!(parse_trait("CanExecute"), Some(Trait::CanExecute));
        assert_eq!(parse_trait("CanForward"), Some(Trait::CanForward));
        assert_eq!(parse_trait("CanAggregate"), Some(Trait::CanAggregate));
        assert_eq!(parse_trait("CanStoreState"), Some(Trait::CanStoreState));
        assert_eq!(parse_trait("CanDiscover"), Some(Trait::CanDiscover));
        assert_eq!(parse_trait("CanRelay"), Some(Trait::CanRelay));
    }

    #[test]
    fn parse_trait_case_insensitive() {
        assert_eq!(parse_trait("CANEXECUTE"), Some(Trait::CanExecute));
        assert_eq!(parse_trait("CAN_EXECUTE"), Some(Trait::CanExecute));
        assert_eq!(parse_trait("Can_Forward"), Some(Trait::CanForward));
    }

    #[test]
    fn parse_trait_unknown_returns_none() {
        assert_eq!(parse_trait("unknown_trait"), None);
        assert_eq!(parse_trait(""), None);
        assert_eq!(parse_trait("CanFly"), None);
    }

    // ---- trait_to_string ----

    #[test]
    fn trait_to_string_all_variants() {
        assert_eq!(trait_to_string(&Trait::CanExecute), "can_execute");
        assert_eq!(trait_to_string(&Trait::CanForward), "can_forward");
        assert_eq!(trait_to_string(&Trait::CanAggregate), "can_aggregate");
        assert_eq!(trait_to_string(&Trait::CanStoreState), "can_store_state");
        assert_eq!(trait_to_string(&Trait::CanDiscover), "can_discover");
        assert_eq!(trait_to_string(&Trait::CanRelay), "can_relay");
    }

    // ---- find_nodes ----

    #[test]
    fn find_nodes_empty_store_returns_empty() {
        let knowledge = setup_knowledge(vec![]);
        let bridge = make_bridge(knowledge);
        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec!["CanExecute".into()],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        assert!(response.nodes.is_empty());
    }

    #[test]
    fn find_nodes_filters_by_trait() {
        let nodes = vec![
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.2,
                NodeStatus::Alive,
            ),
            make_node_info(
                [Trait::CanRelay].into_iter().collect(),
                0.1,
                NodeStatus::Alive,
            ),
            make_node_info(
                [Trait::CanExecute, Trait::CanRelay].into_iter().collect(),
                0.3,
                NodeStatus::Alive,
            ),
        ];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec!["CanExecute".into()],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        // Should match node 0 and node 2 (both have CanExecute).
        assert_eq!(response.nodes.len(), 2);
    }

    #[test]
    fn find_nodes_filters_multiple_traits() {
        let nodes = vec![
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.2,
                NodeStatus::Alive,
            ),
            make_node_info(
                [Trait::CanExecute, Trait::CanRelay].into_iter().collect(),
                0.3,
                NodeStatus::Alive,
            ),
        ];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec!["CanExecute".into(), "CanRelay".into()],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        // Only node 1 has both CanExecute and CanRelay.
        assert_eq!(response.nodes.len(), 1);
    }

    #[test]
    fn find_nodes_sorts_by_load_ascending() {
        let nodes = vec![
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.8,
                NodeStatus::Alive,
            ),
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.1,
                NodeStatus::Alive,
            ),
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.5,
                NodeStatus::Alive,
            ),
        ];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec!["CanExecute".into()],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        assert_eq!(response.nodes.len(), 3);
        // Loads should be in ascending order.
        assert!(response.nodes[0].load <= response.nodes[1].load);
        assert!(response.nodes[1].load <= response.nodes[2].load);
    }

    #[test]
    fn find_nodes_respects_limit() {
        let nodes: Vec<_> = (0..10)
            .map(|i| {
                make_node_info(
                    [Trait::CanExecute].into_iter().collect(),
                    i as f32 * 0.1,
                    NodeStatus::Alive,
                )
            })
            .collect();
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec!["CanExecute".into()],
                limit: 3,
                prefer_region: String::new(),
            })
            .unwrap();
        assert_eq!(response.nodes.len(), 3);
    }

    #[test]
    fn find_nodes_health_score_alive() {
        let nodes = vec![make_node_info(
            [Trait::CanExecute].into_iter().collect(),
            0.3,
            NodeStatus::Alive,
        )];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        assert_eq!(response.nodes.len(), 1);
        let health = response.nodes[0].health_score;
        // health_score = 1.0 - load = 1.0 - 0.3 = 0.7
        assert!((health - 0.7).abs() < 0.01);
    }

    #[test]
    fn find_nodes_health_score_suspect_and_dead() {
        let nodes = vec![
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.2,
                NodeStatus::Suspect,
            ),
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.1,
                NodeStatus::Dead,
            ),
        ];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        assert_eq!(response.nodes.len(), 2);

        let suspect_node = response
            .nodes
            .iter()
            .find(|n| (n.health_score - 0.5).abs() < 0.01)
            .expect("should find suspect node with health_score 0.5");
        assert!((suspect_node.health_score - 0.5).abs() < 0.01);

        let dead_node = response
            .nodes
            .iter()
            .find(|n| n.health_score == 0.0)
            .expect("should find dead node with health_score 0.0");
        assert_eq!(dead_node.health_score, 0.0);
    }

    #[test]
    fn find_nodes_ignores_unknown_trait_names() {
        let nodes = vec![make_node_info(
            [Trait::CanExecute].into_iter().collect(),
            0.1,
            NodeStatus::Alive,
        )];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        // "UnknownTrait" is not parseable, so it gets filtered out,
        // leaving an empty required_traits set. All nodes should match.
        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec!["UnknownTrait".into()],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        assert_eq!(response.nodes.len(), 1);
    }

    #[test]
    fn find_nodes_no_required_traits_returns_all() {
        let nodes = vec![
            make_node_info(
                [Trait::CanExecute].into_iter().collect(),
                0.1,
                NodeStatus::Alive,
            ),
            make_node_info(
                [Trait::CanRelay].into_iter().collect(),
                0.2,
                NodeStatus::Alive,
            ),
        ];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        assert_eq!(response.nodes.len(), 2);
    }

    // ---- is_node_alive ----

    #[test]
    fn is_node_alive_alive_node() {
        let node = make_node_info(
            [Trait::CanExecute].into_iter().collect(),
            0.1,
            NodeStatus::Alive,
        );
        let node_id_str = node.node_id.0.to_string();
        let knowledge = setup_knowledge(vec![node]);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .is_node_alive(IsNodeAliveRequest {
                node_id: node_id_str,
            })
            .unwrap();
        assert!(response.alive);
        // last_seen_ms should be very small (just created).
        assert!(response.last_seen_ms < 5000);
    }

    #[test]
    fn is_node_alive_dead_node() {
        let mut node = make_node_info(
            [Trait::CanExecute].into_iter().collect(),
            0.1,
            NodeStatus::Dead,
        );
        node.last_seen = Utc::now() - chrono::Duration::seconds(60);
        let node_id_str = node.node_id.0.to_string();
        let knowledge = setup_knowledge(vec![node]);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .is_node_alive(IsNodeAliveRequest {
                node_id: node_id_str,
            })
            .unwrap();
        assert!(!response.alive);
        assert!(response.last_seen_ms >= 59_000);
    }

    #[test]
    fn is_node_alive_unknown_node() {
        let knowledge = setup_knowledge(vec![]);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .is_node_alive(IsNodeAliveRequest {
                node_id: uuid::Uuid::new_v4().to_string(),
            })
            .unwrap();
        assert!(!response.alive);
        assert_eq!(response.last_seen_ms, 0);
    }

    #[test]
    fn is_node_alive_invalid_uuid() {
        let knowledge = setup_knowledge(vec![]);
        let bridge = make_bridge(knowledge);

        let result = bridge.is_node_alive(IsNodeAliveRequest {
            node_id: "not-a-valid-uuid".into(),
        });
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::Discovery(msg) => {
                assert!(msg.contains("invalid node_id UUID"));
            }
            other => panic!("expected Discovery error, got {:?}", other),
        }
    }

    // ---- get_node_info ----

    #[test]
    fn get_node_info_returns_local_info() {
        let node_id = NodeId::new();
        let traits: HashSet<Trait> = [Trait::CanExecute, Trait::CanRelay].into_iter().collect();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        let bridge = DiscoveryBridge::new(
            node_id,
            "eu-west-1".to_string(),
            knowledge,
            Arc::new(parking_lot::RwLock::new(traits)),
        );

        let response = bridge.get_node_info().unwrap();
        assert_eq!(response.node_id, node_id.0.to_string());
        assert_eq!(response.region, "eu-west-1");
        assert_eq!(response.traits.len(), 2);
        assert!(response.traits.contains(&"can_execute".to_string()));
        assert!(response.traits.contains(&"can_relay".to_string()));
    }

    #[test]
    fn get_node_info_empty_traits() {
        let node_id = NodeId::new();
        let traits: HashSet<Trait> = HashSet::new();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        let bridge = DiscoveryBridge::new(
            node_id,
            "ap-southeast-1".to_string(),
            knowledge,
            Arc::new(parking_lot::RwLock::new(traits)),
        );

        let response = bridge.get_node_info().unwrap();
        assert!(response.traits.is_empty());
        assert_eq!(response.region, "ap-southeast-1");
    }

    // ---- roundtrip: parse_trait <-> trait_to_string ----

    #[test]
    fn parse_and_display_roundtrip() {
        for t in Trait::ALL {
            let s = trait_to_string(t);
            let parsed = parse_trait(&s);
            assert_eq!(parsed, Some(*t), "roundtrip failed for {:?} -> {}", t, s);
        }
    }

    // ---- DiscoveryBridge construction ----

    #[test]
    fn bridge_construction() {
        let node_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        let traits = Arc::new(parking_lot::RwLock::new(HashSet::new()));
        let bridge = DiscoveryBridge::new(
            node_id,
            "us-west-2".to_string(),
            knowledge,
            traits,
        );
        // Verify we can call methods without panicking.
        let info = bridge.get_node_info().unwrap();
        assert_eq!(info.region, "us-west-2");
    }

    #[test]
    fn find_nodes_limit_zero_returns_all() {
        let nodes: Vec<_> = (0..5)
            .map(|i| {
                make_node_info(
                    [Trait::CanExecute].into_iter().collect(),
                    i as f32 * 0.1,
                    NodeStatus::Alive,
                )
            })
            .collect();
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 0,
                prefer_region: String::new(),
            })
            .unwrap();
        assert_eq!(response.nodes.len(), 5);
    }

    #[test]
    fn find_nodes_node_id_is_valid_uuid() {
        let nodes = vec![make_node_info(
            [Trait::CanExecute].into_iter().collect(),
            0.1,
            NodeStatus::Alive,
        )];
        let knowledge = setup_knowledge(nodes);
        let bridge = make_bridge(knowledge);

        let response = bridge
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 10,
                prefer_region: String::new(),
            })
            .unwrap();
        // Verify the returned node_id is a parseable UUID.
        for node in &response.nodes {
            assert!(uuid::Uuid::parse_str(&node.node_id).is_ok());
        }
    }
}
