// Marabunta - Licensed under the MIT License.
//! Plugin API types for the Marabunta Swarm plugin system.
//!
//! These types mirror the protobuf definitions in `proto/plugin_api.proto`
//! as native Rust types with serde support. They form the complete
//! language-agnostic API surface that plugins use to interact with the swarm.
//!
//! Plugins see these types. They see NOTHING else. No gossip, no collectives,
//! no reputation, no marketplace. Just this API.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

// ============================================================================
// Error
// ============================================================================

/// Errors that can occur in plugin API operations.
#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("plugin not registered: {0}")]
    NotRegistered(String),

    #[error("plugin not found: {0}")]
    NotFound(String),

    #[error("plugin already registered: {0}")]
    AlreadyRegistered(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("scatter error: {0}")]
    Scatter(String),

    #[error("pubsub error: {0}")]
    PubSub(String),

    #[error("discovery error: {0}")]
    Discovery(String),

    #[error("migration error: {0}")]
    Migration(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("process error: {0}")]
    Process(String),

    #[error("lifecycle error: {0}")]
    Lifecycle(String),

    #[error("health check failed: {0}")]
    HealthCheck(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("capacity exceeded: {0}")]
    CapacityExceeded(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<serde_json::Error> for PluginError {
    fn from(e: serde_json::Error) -> Self {
        PluginError::Serialization(e.to_string())
    }
}

/// Result type for plugin operations.
pub type PluginResult<T> = Result<T, PluginError>;

// ============================================================================
// Enums
// ============================================================================

/// Consistency level for storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum Consistency {
    /// Reads may return stale data. Writes propagate eventually.
    #[default]
    Eventual,
    /// Reads always return latest committed value. Writes block until majority ack.
    Strong,
}


/// Data locality preference for scatter operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum Locality {
    /// No locality preference.
    #[default]
    Any,
    /// Prefer nodes in the same region but allow remote.
    PreferLocal,
    /// Require nodes in the same region; fail if none available.
    RequireLocal,
}


/// Priority level for operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}


// ============================================================================
// Plugin identity
// ============================================================================

/// Unique identifier assigned to a registered plugin by the swarm.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PluginId(pub String);

impl std::fmt::Display for PluginId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Current state of a plugin in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum PluginState {
    /// Plugin process spawned but not yet registered.
    #[default]
    Spawned,
    /// Plugin has called Register() and received its ID.
    Registered,
    /// Swarm has called Start() and plugin is initializing.
    Starting,
    /// Plugin is fully operational and receiving requests.
    Running,
    /// Plugin is shutting down (Stop() called).
    Stopping,
    /// Plugin has stopped cleanly.
    Stopped,
    /// Plugin crashed or became unresponsive.
    Failed,
}


// ============================================================================
// Registration
// ============================================================================

/// Plugin registration request. Sent by a plugin to the swarm on startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// Human-readable plugin name (e.g., "postgres", "redis").
    pub name: String,
    /// Semver version string.
    pub version: String,
    /// Node traits required by this plugin (e.g., ["CanStoreState"]).
    pub traits: Vec<String>,
    /// Endpoints this plugin exposes to external clients.
    pub endpoints: Vec<Endpoint>,
}

/// A network endpoint exposed by a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    /// Endpoint name (e.g., "pgwire", "http", "grpc").
    pub name: String,
    /// Transport protocol ("tcp", "udp", "unix").
    pub protocol: String,
    /// Default port number.
    pub default_port: u32,
}

/// Plugin registration response from the swarm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    /// Unique ID assigned to this plugin instance.
    pub plugin_id: String,
    /// This node's identifier in the swarm.
    pub node_id: String,
    /// Opaque swarm configuration blob (plugin-specific).
    pub swarm_config: Vec<u8>,
}

// ============================================================================
// Storage API
// ============================================================================

/// Request to store a key-value pair in the swarm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRequest {
    /// Key bytes.
    pub key: Vec<u8>,
    /// Value bytes.
    pub value: Vec<u8>,
    /// Storage options.
    pub options: StoreOptions,
}

/// Options for a Store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreOptions {
    /// Consistency level.
    pub consistency: Consistency,
    /// Number of replicas (0 = use default, typically 3).
    pub replicas: u32,
    /// Time-to-live in seconds (0 = forever).
    pub ttl_seconds: u32,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            consistency: Consistency::Eventual,
            replicas: 3,
            ttl_seconds: 0,
        }
    }
}

/// Response to a Store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreResponse {
    /// Whether the store succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: String,
    /// Version counter for optimistic concurrency.
    pub version: u64,
}

/// Request to fetch a value by key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    /// Key to look up.
    pub key: Vec<u8>,
    /// Fetch options.
    pub options: FetchOptions,
}

/// Options for a Fetch operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchOptions {
    /// Consistency level.
    pub consistency: Consistency,
    /// Reject values older than this version.
    pub min_version: u64,
}

impl Default for FetchOptions {
    fn default() -> Self {
        Self {
            consistency: Consistency::Eventual,
            min_version: 0,
        }
    }
}

/// Response to a Fetch operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    /// Whether the key was found.
    pub found: bool,
    /// Value bytes (empty if not found).
    pub value: Vec<u8>,
    /// Current version of this key.
    pub version: u64,
    /// Error message if failed.
    pub error: String,
}

/// Request to delete a key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Key to delete.
    pub key: Vec<u8>,
}

/// Response to a Delete operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    /// Whether the delete succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: String,
}

// ============================================================================
// Scatter API (Distributed Compute)
// ============================================================================

/// Request to scatter work units across the swarm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterRequest {
    /// Work units to distribute.
    pub units: Vec<ScatterUnit>,
    /// Hints for placement and execution.
    pub hints: ScatterHints,
}

/// A single unit of work to scatter to a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterUnit {
    /// Specific target node (empty = any eligible node).
    pub target_node: String,
    /// Opaque work payload.
    pub payload: Vec<u8>,
    /// Required traits on the target node.
    pub required_traits: Vec<String>,
}

/// Hints that guide scatter placement and execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterHints {
    /// Data locality preference.
    pub locality: Locality,
    /// Consistency level.
    pub consistency: Consistency,
    /// Priority level.
    pub priority: Priority,
    /// Execution timeout in milliseconds (0 = default).
    pub timeout_ms: u32,
}

impl Default for ScatterHints {
    fn default() -> Self {
        Self {
            locality: Locality::Any,
            consistency: Consistency::Eventual,
            priority: Priority::Normal,
            timeout_ms: 30_000,
        }
    }
}

/// Aggregated response from a Scatter operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterResponse {
    /// Results from each scatter unit.
    pub results: Vec<ScatterResult>,
}

/// Result of executing a single scatter unit on a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterResult {
    /// Node that executed this unit.
    pub node_id: String,
    /// Whether execution succeeded.
    pub success: bool,
    /// Response payload.
    pub response: Vec<u8>,
    /// Error message if failed.
    pub error: String,
    /// Execution latency in milliseconds.
    pub latency_ms: u32,
}

// ============================================================================
// Node Discovery
// ============================================================================

/// Request to find nodes matching criteria.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindNodesRequest {
    /// Required traits on target nodes.
    pub required_traits: Vec<String>,
    /// Maximum number of nodes to return.
    pub limit: u32,
    /// Preferred region (hint, not requirement).
    pub prefer_region: String,
}

/// Response with matching nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindNodesResponse {
    /// Discovered nodes matching criteria.
    pub nodes: Vec<PluginNodeInfo>,
}

/// Node information visible to plugins.
///
/// This is a sanitized subset of the internal NodeInfo — plugins never
/// see raw swarm internals like gossip vectors or reputation scores.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginNodeInfo {
    /// Node identifier.
    pub node_id: String,
    /// Node's current traits.
    pub traits: Vec<String>,
    /// Node's region (if known).
    pub region: String,
    /// Health score 0.0 to 1.0.
    pub health_score: f32,
    /// Current load 0.0 to 1.0.
    pub load: f32,
}

/// Request to check if a specific node is alive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsNodeAliveRequest {
    /// Node to check.
    pub node_id: String,
}

/// Response to a node liveness check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsNodeAliveResponse {
    /// Whether the node is considered alive.
    pub alive: bool,
    /// Milliseconds since last seen (0 if never seen).
    pub last_seen_ms: u64,
}

/// Request for this node's own info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetNodeInfoRequest {}

/// This node's info (from the swarm's perspective).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetNodeInfoResponse {
    /// This node's identifier.
    pub node_id: String,
    /// This node's region.
    pub region: String,
    /// This node's current traits.
    pub traits: Vec<String>,
}

// ============================================================================
// Pub/Sub
// ============================================================================

/// Request to publish a message to a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishRequest {
    /// Topic name.
    pub topic: String,
    /// Message payload.
    pub payload: Vec<u8>,
}

/// Response to a Publish operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishResponse {
    /// Whether the publish succeeded.
    pub success: bool,
    /// Number of recipients that received the message.
    pub recipients: u32,
}

/// Request to subscribe to a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeRequest {
    /// Topic to subscribe to.
    pub topic: String,
}

/// An event received from a subscribed topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeEvent {
    /// Topic this event belongs to.
    pub topic: String,
    /// Event payload.
    pub payload: Vec<u8>,
    /// Node that published this event.
    pub from_node: String,
    /// Unix timestamp (milliseconds) when published.
    pub timestamp: u64,
}

// ============================================================================
// Migration
// ============================================================================

/// Request to migrate data to a different node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRequest {
    /// Key of the data to migrate.
    pub data_key: Vec<u8>,
    /// Current node holding the data.
    pub from_node: String,
    /// Preferred traits for the target node.
    pub preferred_traits: Vec<String>,
    /// Migration priority.
    pub priority: Priority,
    /// Reason for migration (for logging: "rebalance", "failure", etc.).
    pub reason: String,
}

/// Response to a migration request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationResponse {
    /// Whether the migration was accepted.
    pub accepted: bool,
    /// Unique migration ID for tracking.
    pub migration_id: String,
    /// Error message if rejected.
    pub error: String,
}

// ============================================================================
// Plugin Handler (Swarm → Plugin)
// ============================================================================

/// Request sent from swarm to plugin to handle incoming work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleRequest {
    /// Unique request identifier.
    pub request_id: String,
    /// Plugin-specific payload.
    pub payload: Vec<u8>,
    /// Node that originated this request.
    pub from_node: String,
    /// Arbitrary key-value metadata.
    pub metadata: HashMap<String, String>,
}

/// Response from a plugin after handling a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleResponse {
    /// Response payload.
    pub payload: Vec<u8>,
    /// Error message if handling failed.
    pub error: String,
}

/// Request to start a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRequest {
    /// Plugin-specific configuration blob.
    pub config: Vec<u8>,
}

/// Response to a Start request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartResponse {
    /// Whether the plugin started successfully.
    pub success: bool,
    /// Error message if startup failed.
    pub error: String,
}

/// Request to stop a plugin gracefully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopRequest {
    /// Maximum seconds to wait for graceful shutdown.
    pub timeout_seconds: u32,
}

/// Response to a Stop request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopResponse {
    /// Whether shutdown was clean (true) or forced (false).
    pub clean: bool,
}

/// Health check request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthRequest {}

/// Health check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Whether the plugin considers itself healthy.
    pub healthy: bool,
    /// Status string ("running", "degraded", "initializing", etc.).
    pub status: String,
    /// Arbitrary key-value details for diagnostics.
    pub details: HashMap<String, String>,
}

// ============================================================================
// Wire protocol envelope
// ============================================================================

/// Wire-level envelope for plugin ↔ swarm communication.
///
/// All messages between a plugin process and the swarm host are wrapped in
/// this envelope, length-delimited and JSON-serialized over a Unix socket
/// or TCP connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum PluginWireMessage {
    // Plugin → Swarm (PluginHost service)
    RegisterReq(RegisterRequest),
    RegisterResp(RegisterResponse),
    StoreReq(StoreRequest),
    StoreResp(StoreResponse),
    FetchReq(FetchRequest),
    FetchResp(FetchResponse),
    DeleteReq(DeleteRequest),
    DeleteResp(DeleteResponse),
    ScatterReq(ScatterRequest),
    ScatterResp(ScatterResponse),
    FindNodesReq(FindNodesRequest),
    FindNodesResp(FindNodesResponse),
    PublishReq(PublishRequest),
    PublishResp(PublishResponse),
    SubscribeReq(SubscribeRequest),
    SubscribeEvt(SubscribeEvent),
    IsNodeAliveReq(IsNodeAliveRequest),
    IsNodeAliveResp(IsNodeAliveResponse),
    MigrationReq(MigrationRequest),
    MigrationResp(MigrationResponse),
    GetNodeInfoReq(GetNodeInfoRequest),
    GetNodeInfoResp(GetNodeInfoResponse),

    // Data channel messages (Plugin ↔ Swarm)
    DataChannelPush { channel: String, payload: serde_json::Value },
    DataChannelPushAck { channel: String, success: bool },
    DataChannelQuery { channel: String, query: serde_json::Value, request_id: String },
    DataChannelQueryResp { request_id: String, result: serde_json::Value },

    // Swarm → Plugin (Plugin service)
    StartReq(StartRequest),
    StartResp(StartResponse),
    StopReq(StopRequest),
    StopResp(StopResponse),
    HandleReq(HandleRequest),
    HandleResp(HandleResponse),
    HealthReq(HealthRequest),
    HealthResp(HealthResponse),
}

// ============================================================================
// Traits (Plugin API interfaces)
// ============================================================================

/// The Plugin trait — what every plugin must implement.
///
/// In-process Rust plugins implement this trait directly.
/// Out-of-process plugins have a proxy implementation that forwards
/// calls over the wire.
#[async_trait::async_trait]
pub trait PluginService: Send + Sync + 'static {
    /// Called by the swarm to start the plugin with its configuration.
    async fn start(&self, request: StartRequest) -> PluginResult<StartResponse>;

    /// Called by the swarm to gracefully stop the plugin.
    async fn stop(&self, request: StopRequest) -> PluginResult<StopResponse>;

    /// Called by the swarm to forward an incoming request to this plugin.
    async fn handle(&self, request: HandleRequest) -> PluginResult<HandleResponse>;

    /// Called by the swarm periodically to check plugin health.
    async fn health(&self, request: HealthRequest) -> PluginResult<HealthResponse>;

    /// Plugin's human-readable name (e.g., "postgres").
    fn name(&self) -> &str;

    /// Plugin's semver version.
    fn version(&self) -> &str;
}

/// The SwarmHandle — what the swarm provides to plugins.
///
/// Plugins use this handle to interact with the swarm. It abstracts
/// all swarm internals behind a clean, opaque API. Plugins have NO
/// other way to touch the swarm.
#[async_trait::async_trait]
pub trait SwarmHandle: Send + Sync + 'static {
    /// Register this plugin with the swarm.
    async fn register(&self, request: RegisterRequest) -> PluginResult<RegisterResponse>;

    /// Store a key-value pair in the swarm's distributed storage.
    async fn store(&self, request: StoreRequest) -> PluginResult<StoreResponse>;

    /// Fetch a value by key from the swarm's distributed storage.
    async fn fetch(&self, request: FetchRequest) -> PluginResult<FetchResponse>;

    /// Delete a key from the swarm's distributed storage.
    async fn delete(&self, request: DeleteRequest) -> PluginResult<DeleteResponse>;

    /// Scatter work units across the swarm and gather results.
    async fn scatter(&self, request: ScatterRequest) -> PluginResult<ScatterResponse>;

    /// Find nodes matching the given criteria.
    async fn find_nodes(&self, request: FindNodesRequest) -> PluginResult<FindNodesResponse>;

    /// Publish a message to a topic.
    async fn publish(&self, request: PublishRequest) -> PluginResult<PublishResponse>;

    /// Subscribe to a topic. Returns a receiver for incoming events.
    async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> PluginResult<tokio::sync::mpsc::UnboundedReceiver<SubscribeEvent>>;

    /// Check if a specific node is alive.
    async fn is_node_alive(
        &self,
        request: IsNodeAliveRequest,
    ) -> PluginResult<IsNodeAliveResponse>;

    /// Request data migration to a different node.
    async fn request_migration(
        &self,
        request: MigrationRequest,
    ) -> PluginResult<MigrationResponse>;

    /// Get this node's own info.
    async fn get_node_info(&self) -> PluginResult<GetNodeInfoResponse>;

    /// Push a data channel update to the swarm's data channel store.
    async fn push_data_channel(&self, channel: String, payload: serde_json::Value) -> PluginResult<()>;
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistency_default_is_eventual() {
        assert_eq!(Consistency::default(), Consistency::Eventual);
    }

    #[test]
    fn priority_ordering() {
        assert!(Priority::Low < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
    }

    #[test]
    fn plugin_state_default_is_spawned() {
        assert_eq!(PluginState::default(), PluginState::Spawned);
    }

    #[test]
    fn store_options_default() {
        let opts = StoreOptions::default();
        assert_eq!(opts.consistency, Consistency::Eventual);
        assert_eq!(opts.replicas, 3);
        assert_eq!(opts.ttl_seconds, 0);
    }

    #[test]
    fn scatter_hints_default() {
        let hints = ScatterHints::default();
        assert_eq!(hints.locality, Locality::Any);
        assert_eq!(hints.priority, Priority::Normal);
        assert_eq!(hints.timeout_ms, 30_000);
    }

    #[test]
    fn wire_message_roundtrip_register() {
        let req = RegisterRequest {
            name: "postgres".into(),
            version: "1.0.0".into(),
            traits: vec!["CanStoreState".into()],
            endpoints: vec![Endpoint {
                name: "pgwire".into(),
                protocol: "tcp".into(),
                default_port: 5432,
            }],
        };
        let wire = PluginWireMessage::RegisterReq(req);
        let json = serde_json::to_string(&wire).unwrap();
        let back: PluginWireMessage = serde_json::from_str(&json).unwrap();
        match back {
            PluginWireMessage::RegisterReq(r) => {
                assert_eq!(r.name, "postgres");
                assert_eq!(r.endpoints.len(), 1);
                assert_eq!(r.endpoints[0].default_port, 5432);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn wire_message_roundtrip_store() {
        let req = StoreRequest {
            key: b"test-key".to_vec(),
            value: b"test-value".to_vec(),
            options: StoreOptions::default(),
        };
        let wire = PluginWireMessage::StoreReq(req);
        let json = serde_json::to_string(&wire).unwrap();
        let back: PluginWireMessage = serde_json::from_str(&json).unwrap();
        match back {
            PluginWireMessage::StoreReq(r) => {
                assert_eq!(r.key, b"test-key");
                assert_eq!(r.value, b"test-value");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn wire_message_roundtrip_scatter() {
        let req = ScatterRequest {
            units: vec![ScatterUnit {
                target_node: String::new(),
                payload: b"work".to_vec(),
                required_traits: vec!["CanExecute".into()],
            }],
            hints: ScatterHints::default(),
        };
        let wire = PluginWireMessage::ScatterReq(req);
        let json = serde_json::to_string(&wire).unwrap();
        let back: PluginWireMessage = serde_json::from_str(&json).unwrap();
        match back {
            PluginWireMessage::ScatterReq(r) => {
                assert_eq!(r.units.len(), 1);
                assert_eq!(r.units[0].payload, b"work");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn wire_message_roundtrip_health() {
        let mut details = HashMap::new();
        details.insert("connections".into(), "42".into());
        details.insert("uptime_s".into(), "3600".into());
        let resp = HealthResponse {
            healthy: true,
            status: "running".into(),
            details,
        };
        let wire = PluginWireMessage::HealthResp(resp);
        let json = serde_json::to_string(&wire).unwrap();
        let back: PluginWireMessage = serde_json::from_str(&json).unwrap();
        match back {
            PluginWireMessage::HealthResp(r) => {
                assert!(r.healthy);
                assert_eq!(r.status, "running");
                assert_eq!(r.details.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn plugin_id_display() {
        let id = PluginId("postgres-abc123".into());
        assert_eq!(format!("{}", id), "postgres-abc123");
    }

    #[test]
    fn wire_message_roundtrip_subscribe_event() {
        let evt = SubscribeEvent {
            topic: "pg:catalog:sales".into(),
            payload: b"catalog_change".to_vec(),
            from_node: "node-abc".into(),
            timestamp: 1706000000000,
        };
        let wire = PluginWireMessage::SubscribeEvt(evt);
        let json = serde_json::to_string(&wire).unwrap();
        let back: PluginWireMessage = serde_json::from_str(&json).unwrap();
        match back {
            PluginWireMessage::SubscribeEvt(e) => {
                assert_eq!(e.topic, "pg:catalog:sales");
                assert_eq!(e.from_node, "node-abc");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn wire_message_roundtrip_migration() {
        let req = MigrationRequest {
            data_key: b"pg:sales:orders:shard_42:data".to_vec(),
            from_node: "node-abc".into(),
            preferred_traits: vec!["CanStoreState".into()],
            priority: Priority::High,
            reason: "rebalance".into(),
        };
        let wire = PluginWireMessage::MigrationReq(req);
        let json = serde_json::to_string(&wire).unwrap();
        let back: PluginWireMessage = serde_json::from_str(&json).unwrap();
        match back {
            PluginWireMessage::MigrationReq(r) => {
                assert_eq!(r.reason, "rebalance");
                assert_eq!(r.priority, Priority::High);
            }
            _ => panic!("wrong variant"),
        }
    }
}
