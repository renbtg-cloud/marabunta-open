// Marabunta - Licensed under the MIT License.
// Package plugin provides PostgreSQL-specific types used by the plugin layer.
package plugin

// ShardInfo describes a shard in the distributed system.
type ShardInfo struct {
	ID       int    `json:"id"`
	Table    string `json:"table"`
	Instance string `json:"instance"`
	NodeID   string `json:"node_id,omitempty"`
	Size     int64  `json:"size"`
}

// CatalogChange describes a change to the distributed catalog.
type CatalogChange struct {
	Action    string `json:"action"` // "create_table", "drop_table", "create_index", "alter_table"
	Table     string `json:"table"`
	Instance  string `json:"instance"`
	DDL       string `json:"ddl"`
	Timestamp int64  `json:"timestamp"`
	NodeID    string `json:"node_id"`
}

// QueryMetrics tracks per-query statistics.
type QueryMetrics struct {
	ParseTimeUS    int64 `json:"parse_time_us"`
	PlanTimeUS     int64 `json:"plan_time_us"`
	ExecTimeUS     int64 `json:"exec_time_us"`
	ShardsAccessed int   `json:"shards_accessed"`
	RowsReturned   int64 `json:"rows_returned"`
	RowsAffected   int64 `json:"rows_affected"`
}

// DistributedConfig holds configuration for distributed operations.
type DistributedConfig struct {
	Instance       string `json:"instance"`
	ShardCount     int    `json:"shard_count"`
	ShardKeyColumn string `json:"shard_key_column"`
	ReplicaCount   int    `json:"replica_count"`
}
