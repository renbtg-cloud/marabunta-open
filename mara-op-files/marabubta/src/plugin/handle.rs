// Marabunta - Licensed under the MIT License.
//! SwarmHandle implementation that bridges plugin API calls to swarm internals.
//!
//! [`SwarmHandleImpl`] is the concrete implementation of the [`SwarmHandle`]
//! trait defined in `crate::plugin::types`. Plugins interact with the swarm
//! exclusively through this handle -- they never touch gossip, knowledge stores,
//! or any other internal machinery directly.
//!
//! The handle delegates to purpose-built subsystems:
//! - [`crate::plugin::storage::PluginStorage`] for key-value Store/Fetch/Delete
//! - [`crate::plugin::scatter::ScatterEngine`] for distributed work scatter/gather
//! - [`crate::plugin::pubsub::PubSubBroker`] for topic-based publish/subscribe
//! - [`crate::plugin::registry::PluginRegistry`] for plugin registration

use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::debug;
use uuid::Uuid;

use crate::plugin::data_channels::DataChannelStore;
use crate::plugin::pubsub::PubSubBroker;
use crate::plugin::registry::PluginRegistry;
use crate::plugin::scatter::ScatterEngine;
use crate::plugin::storage::PluginStorage;
use crate::plugin::types::*;
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::{NodeId, NodeInfo, NodeStatus, Trait};

// ============================================================================
// MigrationRecord — internal tracker for in-flight migrations
// ============================================================================

/// Record of an accepted migration request.
#[derive(Debug, Clone)]
struct MigrationRecord {
    migration_id: String,
    data_key: Vec<u8>,
    from_node: String,
    preferred_traits: Vec<String>,
    priority: Priority,
    reason: String,
}

// ============================================================================
// SwarmHandleImpl
// ============================================================================

/// Concrete [`SwarmHandle`] implementation that bridges plugin API calls to
/// the swarm's internal subsystems.
///
/// Plugins receive an `Arc<dyn SwarmHandle>` at startup and use it for all
/// interaction with the swarm. The handle never exposes gossip, knowledge
/// store internals, or any other swarm machinery -- it presents a clean,
/// opaque API surface.
pub struct SwarmHandleImpl {
    /// This node's swarm identifier.
    node_id: NodeId,
    /// This node's configured region.
    region: String,
    /// Gossip-synchronized knowledge of the entire swarm.
    knowledge: Arc<KnowledgeStore>,
    /// Key-value storage engine for plugins.
    storage: Arc<PluginStorage>,
    /// Distributed work scatter/gather engine.
    scatter_engine: Arc<ScatterEngine>,
    /// Topic-based publish/subscribe broker.
    pubsub: Arc<PubSubBroker>,
    /// Plugin registration book-keeping.
    registry: Arc<PluginRegistry>,
    /// Snapshot of this node's current capability traits.
    current_traits: Arc<RwLock<HashSet<Trait>>>,
    /// In-flight migration tracker.
    migrations: DashMap<String, MigrationRecord>,
    /// Data channel store for plugin-to-dashboard communication.
    data_channels: Arc<DataChannelStore>,
}

impl SwarmHandleImpl {
    /// Create a new SwarmHandle wired to the given subsystems.
    pub fn new(
        node_id: NodeId,
        region: String,
        knowledge: Arc<KnowledgeStore>,
        storage: Arc<PluginStorage>,
        scatter_engine: Arc<ScatterEngine>,
        pubsub: Arc<PubSubBroker>,
        registry: Arc<PluginRegistry>,
        current_traits: Arc<RwLock<HashSet<Trait>>>,
        data_channels: Arc<DataChannelStore>,
    ) -> Self {
        Self {
            node_id,
            region,
            knowledge,
            storage,
            scatter_engine,
            pubsub,
            registry,
            current_traits,
            migrations: DashMap::new(),
            data_channels,
        }
    }

    /// Get a reference to the data channel store.
    pub fn data_channels(&self) -> &Arc<DataChannelStore> {
        &self.data_channels
    }

    /// Resolve a node identifier string to a [`NodeId`].
    ///
    /// Accepts both the display format (`"node-a1b2c3d4"`) and a raw UUID
    /// string. Searches the local node identity first, then the knowledge store.
    /// Returns `None` if no match is found.
    fn resolve_node_id(&self, id_str: &str) -> Option<NodeId> {
        // Try parsing as a raw UUID first.
        if let Ok(uuid) = Uuid::parse_str(id_str) {
            let candidate = NodeId(uuid);
            if candidate == self.node_id {
                return Some(self.node_id);
            }
            if self.knowledge.get_node(&candidate).is_some() {
                return Some(candidate);
            }
        }

        // Fall back to matching against display strings (e.g. "node-a1b2c3d4").
        if self.node_id.to_string() == id_str {
            return Some(self.node_id);
        }
        for info in self.knowledge.get_all_nodes() {
            if info.node_id.to_string() == id_str {
                return Some(info.node_id);
            }
        }

        None
    }

    /// Convert an internal [`NodeInfo`] to the plugin-facing [`PluginNodeInfo`].
    ///
    /// Health score is `1.0 - load` for alive nodes, clamped to `[0.0, 1.0]`.
    /// Dead and suspect nodes always get `0.0`.
    fn to_plugin_node_info(info: &NodeInfo) -> PluginNodeInfo {
        let health_score = match info.status {
            NodeStatus::Alive => (1.0 - info.load).max(0.0),
            NodeStatus::Draining
            | NodeStatus::Cordoned
            | NodeStatus::Updating => (0.5_f32).min((1.0 - info.load).max(0.0)),
            NodeStatus::Dead | NodeStatus::Quarantined | NodeStatus::Suspect => 0.0,
        };

        PluginNodeInfo {
            node_id: info.node_id.to_string(),
            traits: info.traits.iter().map(|t| t.to_string()).collect(),
            region: String::new(), // NodeInfo does not carry a region field
            health_score,
            load: info.load,
        }
    }
}

/// Parse a trait name string into a [`Trait`] enum value.
///
/// Accepts snake_case names matching the `Trait` Display output.
/// Returns `None` for unrecognized names.
fn parse_trait(name: &str) -> Option<Trait> {
    match name {
        "can_execute" => Some(Trait::CanExecute),
        "can_forward" => Some(Trait::CanForward),
        "can_aggregate" => Some(Trait::CanAggregate),
        "can_store_state" => Some(Trait::CanStoreState),
        "can_discover" => Some(Trait::CanDiscover),
        "can_relay" => Some(Trait::CanRelay),
        _ => None,
    }
}

#[async_trait::async_trait]
impl SwarmHandle for SwarmHandleImpl {
    async fn register(&self, request: RegisterRequest) -> PluginResult<RegisterResponse> {
        self.registry.register_plugin(request)
    }

    async fn store(&self, request: StoreRequest) -> PluginResult<StoreResponse> {
        self.storage.store(request).await
    }

    async fn fetch(&self, request: FetchRequest) -> PluginResult<FetchResponse> {
        self.storage.fetch(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> PluginResult<DeleteResponse> {
        self.storage.delete(request).await
    }

    async fn scatter(&self, request: ScatterRequest) -> PluginResult<ScatterResponse> {
        self.scatter_engine.scatter(request).await
    }

    async fn find_nodes(&self, request: FindNodesRequest) -> PluginResult<FindNodesResponse> {
        let all_nodes = self.knowledge.get_all_nodes();

        // Parse required traits from string names.
        let required: HashSet<Trait> = request
            .required_traits
            .iter()
            .filter_map(|name| parse_trait(name))
            .collect();

        let mut matching: Vec<PluginNodeInfo> = all_nodes
            .iter()
            .filter(|info| {
                if required.is_empty() {
                    return true;
                }
                required.iter().all(|t| info.traits.contains(t))
            })
            .map(Self::to_plugin_node_info)
            .collect();

        // Sort by health_score descending so the healthiest nodes come first.
        matching.sort_by(|a, b| {
            b.health_score
                .partial_cmp(&a.health_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Apply limit.
        if request.limit > 0 {
            matching.truncate(request.limit as usize);
        }

        Ok(FindNodesResponse { nodes: matching })
    }

    async fn publish(&self, request: PublishRequest) -> PluginResult<PublishResponse> {
        self.pubsub.publish(request)
    }

    async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> PluginResult<tokio::sync::mpsc::UnboundedReceiver<SubscribeEvent>> {
        self.pubsub.subscribe(request)
    }

    async fn is_node_alive(
        &self,
        request: IsNodeAliveRequest,
    ) -> PluginResult<IsNodeAliveResponse> {
        let node_id = match self.resolve_node_id(&request.node_id) {
            Some(id) => id,
            None => {
                return Ok(IsNodeAliveResponse {
                    alive: false,
                    last_seen_ms: 0,
                });
            }
        };

        match self.knowledge.get_node(&node_id) {
            Some(info) => {
                let alive = info.status == NodeStatus::Alive;
                let elapsed_ms = Utc::now()
                    .signed_duration_since(info.last_seen)
                    .num_milliseconds()
                    .max(0) as u64;
                Ok(IsNodeAliveResponse {
                    alive,
                    last_seen_ms: elapsed_ms,
                })
            }
            None => Ok(IsNodeAliveResponse {
                alive: false,
                last_seen_ms: 0,
            }),
        }
    }

    async fn request_migration(
        &self,
        request: MigrationRequest,
    ) -> PluginResult<MigrationResponse> {
        let migration_id = Uuid::new_v4().to_string();

        let record = MigrationRecord {
            migration_id: migration_id.clone(),
            data_key: request.data_key,
            from_node: request.from_node,
            preferred_traits: request.preferred_traits,
            priority: request.priority,
            reason: request.reason,
        };

        self.migrations.insert(migration_id.clone(), record);

        debug!(migration_id = %migration_id, "migration request accepted");

        Ok(MigrationResponse {
            accepted: true,
            migration_id,
            error: String::new(),
        })
    }

    async fn get_node_info(&self) -> PluginResult<GetNodeInfoResponse> {
        let traits = self
            .current_traits
            .read()
            .iter()
            .map(|t| t.to_string())
            .collect();

        Ok(GetNodeInfoResponse {
            node_id: self.node_id.to_string(),
            region: self.region.clone(),
            traits,
        })
    }

    async fn push_data_channel(&self, channel: String, payload: serde_json::Value) -> PluginResult<()> {
        // Use the node_id as a fallback plugin_id; real plugin_id comes from the wire message handler.
        self.data_channels.push(
            channel,
            self.node_id.to_string(),
            vec![],
            payload,
        );
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::types::ResourceSnapshot;
    use std::collections::HashSet;

    /// Build a fully wired [`SwarmHandleImpl`] for testing.
    fn make_handle() -> (SwarmHandleImpl, NodeId) {
        let node_id = NodeId::new();
        let node_id_str = node_id.to_string();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        let storage = Arc::new(PluginStorage::new(0, 0));
        let scatter_engine = Arc::new(ScatterEngine::new(
            node_id,
            Arc::clone(&knowledge),
            128,
        ));
        let pubsub = Arc::new(PubSubBroker::new(node_id_str.clone()));
        let registry = Arc::new(PluginRegistry::new(node_id_str, 64));
        let traits = Arc::new(RwLock::new(HashSet::from([
            Trait::CanExecute,
            Trait::CanStoreState,
        ])));
        let data_channels = Arc::new(DataChannelStore::new());

        let handle = SwarmHandleImpl::new(
            node_id,
            "us-west-2".to_string(),
            knowledge,
            storage,
            scatter_engine,
            pubsub,
            registry,
            traits,
            data_channels,
        );

        (handle, node_id)
    }

    /// Insert a fake node into the knowledge store.
    fn insert_node(
        knowledge: &KnowledgeStore,
        node_id: NodeId,
        traits: HashSet<Trait>,
        load: f32,
        status: NodeStatus,
    ) {
        let info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits,
            load,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        };
        knowledge.merge_node(info);
        if status == NodeStatus::Suspect {
            knowledge.mark_node_suspect(&node_id);
        } else if status == NodeStatus::Dead {
            knowledge.mark_node_dead(&node_id);
        }
    }

    // ====================================================================
    // register()
    // ====================================================================

    #[tokio::test]
    async fn register_returns_plugin_id_and_node_id() {
        let (handle, node_id) = make_handle();
        let req = RegisterRequest {
            name: "postgres".into(),
            version: "1.0.0".into(),
            traits: vec!["can_store_state".into()],
            endpoints: vec![Endpoint {
                name: "pgwire".into(),
                protocol: "tcp".into(),
                default_port: 5432,
            }],
        };

        let resp = handle.register(req).await.unwrap();
        assert!(resp.plugin_id.starts_with("plugin-postgres-"));
        assert_eq!(resp.node_id, node_id.to_string());
        assert!(resp.swarm_config.is_empty());
    }

    #[tokio::test]
    async fn register_duplicate_name_fails() {
        let (handle, _) = make_handle();
        let req = RegisterRequest {
            name: "redis".into(),
            version: "1.0.0".into(),
            traits: vec![],
            endpoints: vec![],
        };

        handle.register(req.clone()).await.unwrap();
        let result = handle.register(req).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::AlreadyRegistered(msg) => assert!(msg.contains("redis")),
            other => panic!("expected AlreadyRegistered, got {:?}", other),
        }
    }

    // ====================================================================
    // store() / fetch() / delete()
    // ====================================================================

    #[tokio::test]
    async fn store_and_fetch_roundtrip() {
        let (handle, _) = make_handle();

        let store_resp = handle
            .store(StoreRequest {
                key: b"greeting".to_vec(),
                value: b"hello".to_vec(),
                options: StoreOptions::default(),
            })
            .await
            .unwrap();
        assert!(store_resp.success);
        assert_eq!(store_resp.version, 1);

        let fetch_resp = handle
            .fetch(FetchRequest {
                key: b"greeting".to_vec(),
                options: FetchOptions::default(),
            })
            .await
            .unwrap();
        assert!(fetch_resp.found);
        assert_eq!(fetch_resp.value, b"hello");
        assert_eq!(fetch_resp.version, 1);
    }

    #[tokio::test]
    async fn store_increments_version() {
        let (handle, _) = make_handle();
        let key = b"counter".to_vec();

        for expected_version in 1u64..=5 {
            let resp = handle
                .store(StoreRequest {
                    key: key.clone(),
                    value: vec![expected_version as u8],
                    options: StoreOptions::default(),
                })
                .await
                .unwrap();
            assert_eq!(resp.version, expected_version);
        }
    }

    #[tokio::test]
    async fn fetch_missing_key() {
        let (handle, _) = make_handle();
        let resp = handle
            .fetch(FetchRequest {
                key: b"nonexistent".to_vec(),
                options: FetchOptions::default(),
            })
            .await
            .unwrap();
        assert!(!resp.found);
        assert!(resp.value.is_empty());
        assert_eq!(resp.version, 0);
    }

    #[tokio::test]
    async fn fetch_with_min_version_rejects_stale() {
        let (handle, _) = make_handle();

        handle
            .store(StoreRequest {
                key: b"data".to_vec(),
                value: b"v1".to_vec(),
                options: StoreOptions::default(),
            })
            .await
            .unwrap();

        let resp = handle
            .fetch(FetchRequest {
                key: b"data".to_vec(),
                options: FetchOptions {
                    consistency: Consistency::Eventual,
                    min_version: 5,
                },
            })
            .await
            .unwrap();
        assert!(!resp.found);
        assert!(!resp.error.is_empty());
    }

    #[tokio::test]
    async fn delete_existing_key() {
        let (handle, _) = make_handle();

        handle
            .store(StoreRequest {
                key: b"ephemeral".to_vec(),
                value: b"data".to_vec(),
                options: StoreOptions::default(),
            })
            .await
            .unwrap();

        let del_resp = handle
            .delete(DeleteRequest {
                key: b"ephemeral".to_vec(),
            })
            .await
            .unwrap();
        assert!(del_resp.success);
        assert!(del_resp.error.is_empty());

        let fetch_resp = handle
            .fetch(FetchRequest {
                key: b"ephemeral".to_vec(),
                options: FetchOptions::default(),
            })
            .await
            .unwrap();
        assert!(!fetch_resp.found);
    }

    #[tokio::test]
    async fn delete_missing_key_succeeds() {
        let (handle, _) = make_handle();
        // The real PluginStorage returns success: true even for missing keys.
        let resp = handle
            .delete(DeleteRequest {
                key: b"ghost".to_vec(),
            })
            .await
            .unwrap();
        assert!(resp.success);
    }

    // ====================================================================
    // scatter()
    // ====================================================================

    #[tokio::test]
    async fn scatter_empty_units_returns_empty() {
        let (handle, _) = make_handle();
        let resp = handle
            .scatter(ScatterRequest {
                units: vec![],
                hints: ScatterHints::default(),
            })
            .await
            .unwrap();
        assert!(resp.results.is_empty());
    }

    // ====================================================================
    // find_nodes()
    // ====================================================================

    #[tokio::test]
    async fn find_nodes_returns_matching_traits() {
        let (handle, _) = make_handle();

        let n1 = NodeId::new();
        let n2 = NodeId::new();
        insert_node(
            &handle.knowledge,
            n1,
            HashSet::from([Trait::CanExecute, Trait::CanRelay]),
            0.2,
            NodeStatus::Alive,
        );
        insert_node(
            &handle.knowledge,
            n2,
            HashSet::from([Trait::CanExecute]),
            0.5,
            NodeStatus::Alive,
        );

        let resp = handle
            .find_nodes(FindNodesRequest {
                required_traits: vec!["can_relay".into()],
                limit: 10,
                prefer_region: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].node_id, n1.to_string());
    }

    #[tokio::test]
    async fn find_nodes_empty_traits_returns_all() {
        let (handle, _) = make_handle();

        let n1 = NodeId::new();
        let n2 = NodeId::new();
        insert_node(
            &handle.knowledge,
            n1,
            HashSet::from([Trait::CanExecute]),
            0.1,
            NodeStatus::Alive,
        );
        insert_node(
            &handle.knowledge,
            n2,
            HashSet::from([Trait::CanRelay]),
            0.9,
            NodeStatus::Alive,
        );

        let resp = handle
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 0,
                prefer_region: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(resp.nodes.len(), 2);
    }

    #[tokio::test]
    async fn find_nodes_health_score_for_alive_nodes() {
        let (handle, _) = make_handle();

        let n1 = NodeId::new();
        insert_node(
            &handle.knowledge,
            n1,
            HashSet::from([Trait::CanExecute]),
            0.3,
            NodeStatus::Alive,
        );

        let resp = handle
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 0,
                prefer_region: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(resp.nodes.len(), 1);
        let health = resp.nodes[0].health_score;
        assert!((health - 0.7).abs() < 0.01, "expected ~0.7, got {}", health);
    }

    #[tokio::test]
    async fn find_nodes_dead_node_has_zero_health() {
        let (handle, _) = make_handle();

        let n1 = NodeId::new();
        insert_node(
            &handle.knowledge,
            n1,
            HashSet::from([Trait::CanExecute]),
            0.2,
            NodeStatus::Dead,
        );

        let resp = handle
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 0,
                prefer_region: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].health_score, 0.0);
    }

    #[tokio::test]
    async fn find_nodes_suspect_node_has_zero_health() {
        let (handle, _) = make_handle();

        let n1 = NodeId::new();
        insert_node(
            &handle.knowledge,
            n1,
            HashSet::from([Trait::CanExecute]),
            0.1,
            NodeStatus::Suspect,
        );

        let resp = handle
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 0,
                prefer_region: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(resp.nodes.len(), 1);
        assert_eq!(resp.nodes[0].health_score, 0.0);
    }

    #[tokio::test]
    async fn find_nodes_sorted_by_health_descending() {
        let (handle, _) = make_handle();

        let healthy = NodeId::new();
        let loaded = NodeId::new();
        insert_node(
            &handle.knowledge,
            healthy,
            HashSet::from([Trait::CanExecute]),
            0.1,
            NodeStatus::Alive,
        );
        insert_node(
            &handle.knowledge,
            loaded,
            HashSet::from([Trait::CanExecute]),
            0.8,
            NodeStatus::Alive,
        );

        let resp = handle
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 0,
                prefer_region: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(resp.nodes.len(), 2);
        assert!(resp.nodes[0].health_score >= resp.nodes[1].health_score);
        assert_eq!(resp.nodes[0].node_id, healthy.to_string());
    }

    #[tokio::test]
    async fn find_nodes_respects_limit() {
        let (handle, _) = make_handle();

        for _ in 0..10 {
            insert_node(
                &handle.knowledge,
                NodeId::new(),
                HashSet::from([Trait::CanExecute]),
                0.1,
                NodeStatus::Alive,
            );
        }

        let resp = handle
            .find_nodes(FindNodesRequest {
                required_traits: vec![],
                limit: 3,
                prefer_region: String::new(),
            })
            .await
            .unwrap();

        assert_eq!(resp.nodes.len(), 3);
    }

    // ====================================================================
    // publish() / subscribe()
    // ====================================================================

    #[tokio::test]
    async fn publish_and_subscribe() {
        let (handle, node_id) = make_handle();

        let mut rx = handle
            .subscribe(SubscribeRequest {
                topic: "events".into(),
            })
            .await
            .unwrap();

        let pub_resp = handle
            .publish(PublishRequest {
                topic: "events".into(),
                payload: b"hello-pubsub".to_vec(),
            })
            .await
            .unwrap();

        assert!(pub_resp.success);
        assert_eq!(pub_resp.recipients, 1);

        let event = rx.recv().await.unwrap();
        assert_eq!(event.topic, "events");
        assert_eq!(event.payload, b"hello-pubsub");
        assert_eq!(event.from_node, node_id.to_string());
        assert!(event.timestamp > 0);
    }

    #[tokio::test]
    async fn publish_to_topic_with_no_subscribers() {
        let (handle, _) = make_handle();
        let resp = handle
            .publish(PublishRequest {
                topic: "empty-topic".into(),
                payload: b"lonely".to_vec(),
            })
            .await
            .unwrap();

        assert!(resp.success);
        assert_eq!(resp.recipients, 0);
    }

    #[tokio::test]
    async fn multiple_subscribers_same_topic() {
        let (handle, _) = make_handle();

        let mut rx1 = handle
            .subscribe(SubscribeRequest {
                topic: "shared".into(),
            })
            .await
            .unwrap();
        let mut rx2 = handle
            .subscribe(SubscribeRequest {
                topic: "shared".into(),
            })
            .await
            .unwrap();

        let resp = handle
            .publish(PublishRequest {
                topic: "shared".into(),
                payload: b"broadcast".to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(resp.recipients, 2);

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1.payload, b"broadcast");
        assert_eq!(e2.payload, b"broadcast");
    }

    #[tokio::test]
    async fn subscribe_different_topics_isolated() {
        let (handle, _) = make_handle();

        let mut rx_a = handle
            .subscribe(SubscribeRequest {
                topic: "topic-a".into(),
            })
            .await
            .unwrap();
        let mut rx_b = handle
            .subscribe(SubscribeRequest {
                topic: "topic-b".into(),
            })
            .await
            .unwrap();

        handle
            .publish(PublishRequest {
                topic: "topic-a".into(),
                payload: b"for-a".to_vec(),
            })
            .await
            .unwrap();

        let event_a = rx_a.recv().await.unwrap();
        assert_eq!(event_a.payload, b"for-a");

        // rx_b should have nothing.
        assert!(rx_b.try_recv().is_err());
    }

    // ====================================================================
    // is_node_alive()
    // ====================================================================

    #[tokio::test]
    async fn is_node_alive_returns_true_for_alive_node() {
        let (handle, _) = make_handle();

        let n = NodeId::new();
        insert_node(
            &handle.knowledge,
            n,
            HashSet::from([Trait::CanExecute]),
            0.1,
            NodeStatus::Alive,
        );

        let resp = handle
            .is_node_alive(IsNodeAliveRequest {
                node_id: n.to_string(),
            })
            .await
            .unwrap();

        assert!(resp.alive);
        assert!(resp.last_seen_ms < 1000);
    }

    #[tokio::test]
    async fn is_node_alive_returns_false_for_dead_node() {
        let (handle, _) = make_handle();

        let n = NodeId::new();
        insert_node(
            &handle.knowledge,
            n,
            HashSet::from([Trait::CanExecute]),
            0.1,
            NodeStatus::Dead,
        );

        let resp = handle
            .is_node_alive(IsNodeAliveRequest {
                node_id: n.to_string(),
            })
            .await
            .unwrap();

        assert!(!resp.alive);
    }

    #[tokio::test]
    async fn is_node_alive_returns_false_for_unknown_node() {
        let (handle, _) = make_handle();
        let resp = handle
            .is_node_alive(IsNodeAliveRequest {
                node_id: "node-00000000".into(),
            })
            .await
            .unwrap();

        assert!(!resp.alive);
        assert_eq!(resp.last_seen_ms, 0);
    }

    #[tokio::test]
    async fn is_node_alive_accepts_uuid_string() {
        let (handle, _) = make_handle();

        let n = NodeId::new();
        insert_node(
            &handle.knowledge,
            n,
            HashSet::from([Trait::CanExecute]),
            0.1,
            NodeStatus::Alive,
        );

        // Pass the raw UUID string instead of the display format.
        let resp = handle
            .is_node_alive(IsNodeAliveRequest {
                node_id: n.0.to_string(),
            })
            .await
            .unwrap();

        assert!(resp.alive);
    }

    // ====================================================================
    // request_migration()
    // ====================================================================

    #[tokio::test]
    async fn request_migration_accepted() {
        let (handle, _) = make_handle();
        let resp = handle
            .request_migration(MigrationRequest {
                data_key: b"shard_42".to_vec(),
                from_node: "node-origin".into(),
                preferred_traits: vec!["can_store_state".into()],
                priority: Priority::High,
                reason: "rebalance".into(),
            })
            .await
            .unwrap();

        assert!(resp.accepted);
        assert!(!resp.migration_id.is_empty());
        assert!(resp.error.is_empty());

        // Verify migration was tracked internally.
        assert!(handle.migrations.contains_key(&resp.migration_id));
        let record = handle.migrations.get(&resp.migration_id).unwrap();
        assert_eq!(record.data_key, b"shard_42");
        assert_eq!(record.from_node, "node-origin");
        assert_eq!(record.reason, "rebalance");
    }

    #[tokio::test]
    async fn request_migration_unique_ids() {
        let (handle, _) = make_handle();

        let resp1 = handle
            .request_migration(MigrationRequest {
                data_key: b"key1".to_vec(),
                from_node: "n1".into(),
                preferred_traits: vec![],
                priority: Priority::Normal,
                reason: "test".into(),
            })
            .await
            .unwrap();

        let resp2 = handle
            .request_migration(MigrationRequest {
                data_key: b"key2".to_vec(),
                from_node: "n2".into(),
                preferred_traits: vec![],
                priority: Priority::Normal,
                reason: "test".into(),
            })
            .await
            .unwrap();

        assert_ne!(resp1.migration_id, resp2.migration_id);
        assert_eq!(handle.migrations.len(), 2);
    }

    // ====================================================================
    // get_node_info()
    // ====================================================================

    #[tokio::test]
    async fn get_node_info_returns_self() {
        let (handle, node_id) = make_handle();
        let resp = handle.get_node_info().await.unwrap();

        assert_eq!(resp.node_id, node_id.to_string());
        assert_eq!(resp.region, "us-west-2");
        assert_eq!(resp.traits.len(), 2);
        assert!(resp.traits.contains(&"can_execute".to_string()));
        assert!(resp.traits.contains(&"can_store_state".to_string()));
    }

    #[tokio::test]
    async fn get_node_info_reflects_trait_changes() {
        let (handle, _) = make_handle();

        // Add a new trait.
        handle.current_traits.write().insert(Trait::CanRelay);

        let resp = handle.get_node_info().await.unwrap();
        assert_eq!(resp.traits.len(), 3);
        assert!(resp.traits.contains(&"can_relay".to_string()));

        // Remove a trait.
        handle.current_traits.write().remove(&Trait::CanExecute);

        let resp = handle.get_node_info().await.unwrap();
        assert_eq!(resp.traits.len(), 2);
        assert!(!resp.traits.contains(&"can_execute".to_string()));
    }

    // ====================================================================
    // parse_trait()
    // ====================================================================

    #[test]
    fn parse_trait_all_variants() {
        assert_eq!(parse_trait("can_execute"), Some(Trait::CanExecute));
        assert_eq!(parse_trait("can_forward"), Some(Trait::CanForward));
        assert_eq!(parse_trait("can_aggregate"), Some(Trait::CanAggregate));
        assert_eq!(parse_trait("can_store_state"), Some(Trait::CanStoreState));
        assert_eq!(parse_trait("can_discover"), Some(Trait::CanDiscover));
        assert_eq!(parse_trait("can_relay"), Some(Trait::CanRelay));
    }

    #[test]
    fn parse_trait_unknown_returns_none() {
        assert_eq!(parse_trait("unknown_trait"), None);
        assert_eq!(parse_trait(""), None);
    }

    // ====================================================================
    // to_plugin_node_info()
    // ====================================================================

    #[test]
    fn to_plugin_node_info_alive() {
        let node_id = NodeId::new();
        let info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute, Trait::CanRelay]),
            load: 0.4,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        };

        let plugin_info = SwarmHandleImpl::to_plugin_node_info(&info);
        assert_eq!(plugin_info.node_id, node_id.to_string());
        assert_eq!(plugin_info.traits.len(), 2);
        assert!((plugin_info.health_score - 0.6).abs() < 0.01);
        assert!((plugin_info.load - 0.4).abs() < 0.01);
    }

    #[test]
    fn to_plugin_node_info_dead() {
        let node_id = NodeId::new();
        let info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load: 0.1,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: node_id,
            status: NodeStatus::Dead,
            generation: 1,
            trust_level: Default::default(),
        };

        let plugin_info = SwarmHandleImpl::to_plugin_node_info(&info);
        assert_eq!(plugin_info.health_score, 0.0);
    }

    #[test]
    fn to_plugin_node_info_fully_loaded() {
        let node_id = NodeId::new();
        let info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: HashSet::from([Trait::CanExecute]),
            load: 1.0,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        };

        let plugin_info = SwarmHandleImpl::to_plugin_node_info(&info);
        assert!((plugin_info.health_score - 0.0).abs() < 0.01);
    }

    #[test]
    fn to_plugin_node_info_load_exceeds_one_clamped() {
        let node_id = NodeId::new();
        let info = NodeInfo {
            node_id,
            last_seen: Utc::now(),
            traits: HashSet::new(),
            load: 1.5,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: node_id,
            status: NodeStatus::Alive,
            generation: 1,
            trust_level: Default::default(),
        };

        let plugin_info = SwarmHandleImpl::to_plugin_node_info(&info);
        // 1.0 - 1.5 = -0.5 => clamped to 0.0
        assert_eq!(plugin_info.health_score, 0.0);
    }

    // ====================================================================
    // resolve_node_id()
    // ====================================================================

    #[test]
    fn resolve_node_id_self_display() {
        let (handle, node_id) = make_handle();
        let resolved = handle.resolve_node_id(&node_id.to_string());
        assert_eq!(resolved, Some(node_id));
    }

    #[test]
    fn resolve_node_id_self_uuid() {
        let (handle, node_id) = make_handle();
        let resolved = handle.resolve_node_id(&node_id.0.to_string());
        assert_eq!(resolved, Some(node_id));
    }

    #[test]
    fn resolve_node_id_from_knowledge_display() {
        let (handle, _) = make_handle();

        let n = NodeId::new();
        insert_node(
            &handle.knowledge,
            n,
            HashSet::new(),
            0.0,
            NodeStatus::Alive,
        );

        let resolved = handle.resolve_node_id(&n.to_string());
        assert_eq!(resolved, Some(n));
    }

    #[test]
    fn resolve_node_id_from_knowledge_uuid() {
        let (handle, _) = make_handle();

        let n = NodeId::new();
        insert_node(
            &handle.knowledge,
            n,
            HashSet::new(),
            0.0,
            NodeStatus::Alive,
        );

        let resolved = handle.resolve_node_id(&n.0.to_string());
        assert_eq!(resolved, Some(n));
    }

    #[test]
    fn resolve_node_id_unknown() {
        let (handle, _) = make_handle();
        let resolved = handle.resolve_node_id("node-00000000");
        assert!(resolved.is_none());
    }
}
