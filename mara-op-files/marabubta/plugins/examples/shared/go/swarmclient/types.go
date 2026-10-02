// Marabunta - Licensed under the MIT License.
// Package swarmclient provides a Go client library for communicating with the
// Marabunta Swarm via the plugin wire protocol. All types here mirror the Rust
// PluginWireMessage enum defined in src/plugin/types.rs, serialized as
// {"type":"VariantName","payload":{...}} with 4-byte big-endian length framing.
package swarmclient

// --------------------------------------------------------------------------
// Enums
// --------------------------------------------------------------------------

// Consistency level for storage operations.
type Consistency string

const (
	ConsistencyEventual Consistency = "eventual"
	ConsistencyStrong   Consistency = "strong"
)

// Locality preference for scatter operations.
type Locality string

const (
	LocalityAny          Locality = "any"
	LocalityPreferLocal  Locality = "prefer_local"
	LocalityRequireLocal Locality = "require_local"
)

// Priority level for operations.
type Priority string

const (
	PriorityLow      Priority = "low"
	PriorityNormal   Priority = "normal"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"
)

// --------------------------------------------------------------------------
// Registration
// --------------------------------------------------------------------------

// Endpoint describes a network endpoint exposed by a plugin.
type Endpoint struct {
	Name        string `json:"name"`
	Protocol    string `json:"protocol"`
	DefaultPort uint32 `json:"default_port"`
}

// RegisterRequest is sent by a plugin to register itself with the swarm.
type RegisterRequest struct {
	Name      string     `json:"name"`
	Version   string     `json:"version"`
	Traits    []string   `json:"traits"`
	Endpoints []Endpoint `json:"endpoints"`
}

// RegisterResponse is the swarm's reply to a registration request.
type RegisterResponse struct {
	PluginID    string `json:"plugin_id"`
	NodeID      string `json:"node_id"`
	SwarmConfig []byte `json:"swarm_config"`
}

// --------------------------------------------------------------------------
// Storage
// --------------------------------------------------------------------------

// StoreOptions controls storage behavior.
type StoreOptions struct {
	Consistency Consistency `json:"consistency"`
	Replicas    uint32      `json:"replicas"`
	TTLSeconds  uint32      `json:"ttl_seconds"`
}

// DefaultStoreOptions returns options matching the Rust defaults.
func DefaultStoreOptions() StoreOptions {
	return StoreOptions{
		Consistency: ConsistencyEventual,
		Replicas:    3,
		TTLSeconds:  0,
	}
}

// StoreRequest asks the swarm to store a key-value pair.
type StoreRequest struct {
	Key     []byte       `json:"key"`
	Value   []byte       `json:"value"`
	Options StoreOptions `json:"options"`
}

// StoreResponse is the swarm's reply to a store request.
type StoreResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
	Version uint64 `json:"version"`
}

// FetchOptions controls fetch behavior.
type FetchOptions struct {
	Consistency Consistency `json:"consistency"`
	MinVersion  uint64      `json:"min_version"`
}

// DefaultFetchOptions returns options matching the Rust defaults.
func DefaultFetchOptions() FetchOptions {
	return FetchOptions{
		Consistency: ConsistencyEventual,
		MinVersion:  0,
	}
}

// FetchRequest asks the swarm to retrieve a value by key.
type FetchRequest struct {
	Key     []byte       `json:"key"`
	Options FetchOptions `json:"options"`
}

// FetchResponse is the swarm's reply to a fetch request.
type FetchResponse struct {
	Found   bool   `json:"found"`
	Value   []byte `json:"value"`
	Version uint64 `json:"version"`
	Error   string `json:"error"`
}

// DeleteRequest asks the swarm to delete a key.
type DeleteRequest struct {
	Key []byte `json:"key"`
}

// DeleteResponse is the swarm's reply to a delete request.
type DeleteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

// --------------------------------------------------------------------------
// Scatter (Distributed Compute)
// --------------------------------------------------------------------------

// ScatterUnit is a single unit of work to be sent to a node.
type ScatterUnit struct {
	TargetNode     string   `json:"target_node"`
	Payload        []byte   `json:"payload"`
	RequiredTraits []string `json:"required_traits"`
}

// ScatterHints guides scatter placement and execution.
type ScatterHints struct {
	Locality    Locality    `json:"locality"`
	Consistency Consistency `json:"consistency"`
	Priority    Priority    `json:"priority"`
	TimeoutMS   uint32      `json:"timeout_ms"`
}

// DefaultScatterHints returns hints matching the Rust defaults.
func DefaultScatterHints() ScatterHints {
	return ScatterHints{
		Locality:    LocalityAny,
		Consistency: ConsistencyEventual,
		Priority:    PriorityNormal,
		TimeoutMS:   30000,
	}
}

// ScatterRequest asks the swarm to distribute work units across nodes.
type ScatterRequest struct {
	Units []ScatterUnit `json:"units"`
	Hints ScatterHints  `json:"hints"`
}

// ScatterResult is the outcome of a single scatter unit.
type ScatterResult struct {
	NodeID    string `json:"node_id"`
	Success   bool   `json:"success"`
	Response  []byte `json:"response"`
	Error     string `json:"error"`
	LatencyMS uint32 `json:"latency_ms"`
}

// ScatterResponse aggregates results from all scatter units.
type ScatterResponse struct {
	Results []ScatterResult `json:"results"`
}

// --------------------------------------------------------------------------
// Node Discovery
// --------------------------------------------------------------------------

// FindNodesRequest asks the swarm to find nodes matching criteria.
type FindNodesRequest struct {
	RequiredTraits []string `json:"required_traits"`
	Limit          uint32   `json:"limit"`
	PreferRegion   string   `json:"prefer_region"`
}

// PluginNodeInfo is the sanitized node information visible to plugins.
type PluginNodeInfo struct {
	NodeID      string   `json:"node_id"`
	Traits      []string `json:"traits"`
	Region      string   `json:"region"`
	HealthScore float32  `json:"health_score"`
	Load        float32  `json:"load"`
}

// FindNodesResponse returns nodes matching the query criteria.
type FindNodesResponse struct {
	Nodes []PluginNodeInfo `json:"nodes"`
}

// IsNodeAliveRequest checks if a specific node is alive.
type IsNodeAliveRequest struct {
	NodeID string `json:"node_id"`
}

// IsNodeAliveResponse returns the liveness status of a node.
type IsNodeAliveResponse struct {
	Alive      bool   `json:"alive"`
	LastSeenMS uint64 `json:"last_seen_ms"`
}

// GetNodeInfoRequest asks for this node's own info.
type GetNodeInfoRequest struct{}

// GetNodeInfoResponse returns this node's info from the swarm's perspective.
type GetNodeInfoResponse struct {
	NodeID string   `json:"node_id"`
	Region string   `json:"region"`
	Traits []string `json:"traits"`
}

// --------------------------------------------------------------------------
// Pub/Sub
// --------------------------------------------------------------------------

// PublishRequest publishes a message to a topic.
type PublishRequest struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
}

// PublishResponse is the swarm's reply to a publish request.
type PublishResponse struct {
	Success    bool   `json:"success"`
	Recipients uint32 `json:"recipients"`
}

// SubscribeRequest subscribes to a topic.
type SubscribeRequest struct {
	Topic string `json:"topic"`
}

// SubscribeEvent is delivered when a message arrives on a subscribed topic.
type SubscribeEvent struct {
	Topic     string `json:"topic"`
	Payload   []byte `json:"payload"`
	FromNode  string `json:"from_node"`
	Timestamp uint64 `json:"timestamp"`
}

// --------------------------------------------------------------------------
// Migration
// --------------------------------------------------------------------------

// MigrationRequest asks the swarm to migrate data to a different node.
type MigrationRequest struct {
	DataKey         []byte   `json:"data_key"`
	FromNode        string   `json:"from_node"`
	PreferredTraits []string `json:"preferred_traits"`
	Priority        Priority `json:"priority"`
	Reason          string   `json:"reason"`
}

// MigrationResponse is the swarm's reply to a migration request.
type MigrationResponse struct {
	Accepted    bool   `json:"accepted"`
	MigrationID string `json:"migration_id"`
	Error       string `json:"error"`
}

// --------------------------------------------------------------------------
// Lifecycle (Swarm -> Plugin)
// --------------------------------------------------------------------------

// StartRequest is sent by the swarm to start a plugin.
type StartRequest struct {
	Config []byte `json:"config"`
}

// StartResponse is the plugin's reply to a start request.
type StartResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

// StopRequest is sent by the swarm to stop a plugin gracefully.
type StopRequest struct {
	TimeoutSeconds uint32 `json:"timeout_seconds"`
}

// StopResponse is the plugin's reply to a stop request.
type StopResponse struct {
	Clean bool `json:"clean"`
}

// HandleRequest is sent by the swarm to forward an incoming request to a plugin.
type HandleRequest struct {
	RequestID string            `json:"request_id"`
	Payload   []byte            `json:"payload"`
	FromNode  string            `json:"from_node"`
	Metadata  map[string]string `json:"metadata"`
}

// HandleResponse is the plugin's reply after handling a request.
type HandleResponse struct {
	Payload []byte `json:"payload"`
	Error   string `json:"error"`
}

// HealthRequest is sent by the swarm to check plugin health.
type HealthRequest struct{}

// HealthResponse is the plugin's reply to a health check.
type HealthResponse struct {
	Healthy bool              `json:"healthy"`
	Status  string            `json:"status"`
	Details map[string]string `json:"details"`
}

// --------------------------------------------------------------------------
// Wire envelope
// --------------------------------------------------------------------------

// WireMessage is the top-level wire protocol envelope. It is serialized as
// {"type":"VariantName","payload":{...}} matching the Rust PluginWireMessage.
type WireMessage struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// MessageType constants matching every variant in PluginWireMessage.
const (
	TypeRegisterReq    = "RegisterReq"
	TypeRegisterResp   = "RegisterResp"
	TypeStoreReq       = "StoreReq"
	TypeStoreResp      = "StoreResp"
	TypeFetchReq       = "FetchReq"
	TypeFetchResp      = "FetchResp"
	TypeDeleteReq      = "DeleteReq"
	TypeDeleteResp     = "DeleteResp"
	TypeScatterReq     = "ScatterReq"
	TypeScatterResp    = "ScatterResp"
	TypeFindNodesReq   = "FindNodesReq"
	TypeFindNodesResp  = "FindNodesResp"
	TypePublishReq     = "PublishReq"
	TypePublishResp    = "PublishResp"
	TypeSubscribeReq    = "SubscribeReq"
	TypeSubscribeEvt    = "SubscribeEvt"
	TypeIsNodeAliveReq  = "IsNodeAliveReq"
	TypeIsNodeAliveResp = "IsNodeAliveResp"
	TypeMigrationReq    = "MigrationReq"
	TypeMigrationResp   = "MigrationResp"
	TypeGetNodeInfoReq  = "GetNodeInfoReq"
	TypeGetNodeInfoResp = "GetNodeInfoResp"
	TypeStartReq        = "StartReq"
	TypeStartResp       = "StartResp"
	TypeStopReq         = "StopReq"
	TypeStopResp        = "StopResp"
	TypeHandleReq       = "HandleReq"
	TypeHandleResp      = "HandleResp"
	TypeHealthReq       = "HealthReq"
	TypeHealthResp      = "HealthResp"
)
