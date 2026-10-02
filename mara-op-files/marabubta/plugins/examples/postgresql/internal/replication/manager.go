// Marabunta - Licensed under the MIT License.
// Package replication provides async replication for the distributed PostgreSQL
// plugin. It manages shard placement across nodes (primary + replicas), ships
// WAL entries from primaries to followers, and handles automatic failover when
// a primary node becomes unresponsive.
//
// The replication model is statement-based: after each successful write
// operation on a primary shard, the SQL statement is captured as a WALEntry
// and published via the swarm's Pub/Sub layer. Follower nodes subscribe to
// these topics and replay the statements against their local SQLite replicas.
package replication

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ShardPlacement describes the placement of a single shard across the cluster.
type ShardPlacement struct {
	Table       string   `json:"table"`
	ShardID     int      `json:"shard_id"`
	PrimaryNode string   `json:"primary_node"`
	Replicas    []string `json:"replicas"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// NodeInfo holds metadata about a node for placement decisions.
type NodeInfo struct {
	NodeID string `json:"node_id"`
	Region string `json:"region"`
	Load   float32 `json:"load"`
}

// shardKey is a composite key for table+shardID lookups.
type shardKey struct {
	table   string
	shardID int
}

// PlacementManager tracks primary and replica assignments for every shard.
// It persists placement state to a local SQLite database (placement.db) so
// that assignments survive restarts. All methods are safe for concurrent use.
type PlacementManager struct {
	mu           sync.RWMutex
	dataDir      string
	db           *sql.DB
	placements   map[shardKey]*ShardPlacement
	nodeShards   map[string][]shardKey // nodeID -> shards on that node
	replicaCount int                   // target number of replicas per shard
}

// NewPlacementManager creates a PlacementManager backed by a SQLite database
// in the given data directory. replicaCount controls how many replicas each
// shard should have (typically 2).
func NewPlacementManager(dataDir string, replicaCount int) (*PlacementManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create placement dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "placement.db")
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open placement db: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping placement db: %w", err)
	}

	pm := &PlacementManager{
		dataDir:      dataDir,
		db:           db,
		placements:   make(map[shardKey]*ShardPlacement),
		nodeShards:   make(map[string][]shardKey),
		replicaCount: replicaCount,
	}

	if err := pm.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("init placement schema: %w", err)
	}

	if err := pm.loadFromDB(); err != nil {
		db.Close()
		return nil, fmt.Errorf("load placement state: %w", err)
	}

	return pm, nil
}

// initSchema creates the placement tables if they do not exist.
func (pm *PlacementManager) initSchema() error {
	schema := `
		CREATE TABLE IF NOT EXISTS shard_placements (
			tbl       TEXT    NOT NULL,
			shard_id  INTEGER NOT NULL,
			primary_node TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (tbl, shard_id)
		);
		CREATE TABLE IF NOT EXISTS shard_replicas (
			tbl       TEXT    NOT NULL,
			shard_id  INTEGER NOT NULL,
			node_id   TEXT    NOT NULL,
			PRIMARY KEY (tbl, shard_id, node_id)
		);
	`
	_, err := pm.db.Exec(schema)
	return err
}

// loadFromDB restores placement state from the SQLite database.
func (pm *PlacementManager) loadFromDB() error {
	// Load primaries.
	rows, err := pm.db.Query("SELECT tbl, shard_id, primary_node, updated_at FROM shard_placements")
	if err != nil {
		return fmt.Errorf("query placements: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tbl, primaryNode, updatedStr string
		var sid int
		if err := rows.Scan(&tbl, &sid, &primaryNode, &updatedStr); err != nil {
			return fmt.Errorf("scan placement: %w", err)
		}
		updated, _ := time.Parse(time.RFC3339, updatedStr)
		key := shardKey{table: tbl, shardID: sid}
		pm.placements[key] = &ShardPlacement{
			Table:       tbl,
			ShardID:     sid,
			PrimaryNode: primaryNode,
			UpdatedAt:   updated,
		}
		pm.addNodeShard(primaryNode, key)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate placements: %w", err)
	}

	// Load replicas.
	rRows, err := pm.db.Query("SELECT tbl, shard_id, node_id FROM shard_replicas")
	if err != nil {
		return fmt.Errorf("query replicas: %w", err)
	}
	defer rRows.Close()

	for rRows.Next() {
		var tbl, nodeID string
		var sid int
		if err := rRows.Scan(&tbl, &sid, &nodeID); err != nil {
			return fmt.Errorf("scan replica: %w", err)
		}
		key := shardKey{table: tbl, shardID: sid}
		if sp, ok := pm.placements[key]; ok {
			sp.Replicas = append(sp.Replicas, nodeID)
			pm.addNodeShard(nodeID, key)
		}
	}
	return rRows.Err()
}

// addNodeShard adds a shard key to a node's tracked shards (internal, no lock).
func (pm *PlacementManager) addNodeShard(nodeID string, key shardKey) {
	shards := pm.nodeShards[nodeID]
	for _, s := range shards {
		if s == key {
			return
		}
	}
	pm.nodeShards[nodeID] = append(pm.nodeShards[nodeID], key)
}

// removeNodeShard removes a shard key from a node's tracked shards (internal, no lock).
func (pm *PlacementManager) removeNodeShard(nodeID string, key shardKey) {
	shards := pm.nodeShards[nodeID]
	for i, s := range shards {
		if s == key {
			pm.nodeShards[nodeID] = append(shards[:i], shards[i+1:]...)
			return
		}
	}
}

// PlacePrimary assigns a node as the primary for a shard. If the shard already
// has a placement, the primary is replaced. Returns an error if the assignment
// cannot be persisted.
func (pm *PlacementManager) PlacePrimary(table string, shardID int, nodeID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}
	now := time.Now().UTC()

	// Remove old primary from node index.
	if sp, ok := pm.placements[key]; ok && sp.PrimaryNode != "" {
		pm.removeNodeShard(sp.PrimaryNode, key)
	}

	// Persist.
	_, err := pm.db.Exec(
		`INSERT OR REPLACE INTO shard_placements (tbl, shard_id, primary_node, updated_at) VALUES (?, ?, ?, ?)`,
		table, shardID, nodeID, now.Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("persist primary placement: %w", err)
	}

	// Update in-memory state.
	sp, ok := pm.placements[key]
	if !ok {
		sp = &ShardPlacement{
			Table:   table,
			ShardID: shardID,
		}
		pm.placements[key] = sp
	}
	sp.PrimaryNode = nodeID
	sp.UpdatedAt = now
	pm.addNodeShard(nodeID, key)

	return nil
}

// PlaceReplica assigns a node as a replica for a shard. The node must not be
// the current primary for the same shard (anti-affinity). Returns an error if
// the node is already the primary or the replica already exists.
func (pm *PlacementManager) PlaceReplica(table string, shardID int, nodeID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}

	sp, ok := pm.placements[key]
	if !ok {
		return fmt.Errorf("shard %s:%d has no primary; place primary first", table, shardID)
	}

	// Anti-affinity: replica must not be on the same node as the primary.
	if sp.PrimaryNode == nodeID {
		return fmt.Errorf("anti-affinity violation: node %s is already primary for %s:%d", nodeID, table, shardID)
	}

	// Check for duplicate replica.
	for _, r := range sp.Replicas {
		if r == nodeID {
			return fmt.Errorf("node %s is already a replica for %s:%d", nodeID, table, shardID)
		}
	}

	// Persist.
	_, err := pm.db.Exec(
		`INSERT OR REPLACE INTO shard_replicas (tbl, shard_id, node_id) VALUES (?, ?, ?)`,
		table, shardID, nodeID,
	)
	if err != nil {
		return fmt.Errorf("persist replica placement: %w", err)
	}

	sp.Replicas = append(sp.Replicas, nodeID)
	sp.UpdatedAt = time.Now().UTC()
	pm.addNodeShard(nodeID, key)

	return nil
}

// GetPrimary returns the primary node ID for a shard, or empty string if not placed.
func (pm *PlacementManager) GetPrimary(table string, shardID int) string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	key := shardKey{table: table, shardID: shardID}
	if sp, ok := pm.placements[key]; ok {
		return sp.PrimaryNode
	}
	return ""
}

// GetReplicas returns the replica node IDs for a shard.
func (pm *PlacementManager) GetReplicas(table string, shardID int) []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	key := shardKey{table: table, shardID: shardID}
	if sp, ok := pm.placements[key]; ok {
		result := make([]string, len(sp.Replicas))
		copy(result, sp.Replicas)
		return result
	}
	return nil
}

// GetPlacement returns the full placement for a shard, or nil if not placed.
func (pm *PlacementManager) GetPlacement(table string, shardID int) *ShardPlacement {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	key := shardKey{table: table, shardID: shardID}
	if sp, ok := pm.placements[key]; ok {
		copy := *sp
		copy.Replicas = make([]string, len(sp.Replicas))
		for i, r := range sp.Replicas {
			copy.Replicas[i] = r
		}
		return &copy
	}
	return nil
}

// GetNodeShards returns all shards (primary + replica) assigned to a node.
func (pm *PlacementManager) GetNodeShards(nodeID string) []ShardPlacement {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	keys := pm.nodeShards[nodeID]
	var result []ShardPlacement
	for _, key := range keys {
		if sp, ok := pm.placements[key]; ok {
			copy := *sp
			copy.Replicas = make([]string, len(sp.Replicas))
			for i, r := range sp.Replicas {
				copy.Replicas[i] = r
			}
			result = append(result, copy)
		}
	}
	return result
}

// RemoveNode removes a node from all placements and triggers failover logic.
// For each shard where the removed node was primary, it returns the list of
// shards that need failover. The caller is responsible for actually promoting
// a new primary via the FailoverManager.
func (pm *PlacementManager) RemoveNode(nodeID string) []ShardPlacement {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var needsFailover []ShardPlacement

	keys := make([]shardKey, len(pm.nodeShards[nodeID]))
	copy(keys, pm.nodeShards[nodeID])

	for _, key := range keys {
		sp, ok := pm.placements[key]
		if !ok {
			continue
		}

		if sp.PrimaryNode == nodeID {
			// This shard needs failover.
			copy := *sp
			copy.Replicas = make([]string, len(sp.Replicas))
			for i, r := range sp.Replicas {
				copy.Replicas[i] = r
			}
			needsFailover = append(needsFailover, copy)

			// Clear the primary in our state.
			sp.PrimaryNode = ""
			sp.UpdatedAt = time.Now().UTC()
			pm.db.Exec("UPDATE shard_placements SET primary_node = '', updated_at = ? WHERE tbl = ? AND shard_id = ?",
				sp.UpdatedAt.Format(time.RFC3339), key.table, key.shardID)
		}

		// Remove from replicas if present.
		newReplicas := make([]string, 0, len(sp.Replicas))
		for _, r := range sp.Replicas {
			if r != nodeID {
				newReplicas = append(newReplicas, r)
			}
		}
		if len(newReplicas) != len(sp.Replicas) {
			sp.Replicas = newReplicas
			pm.db.Exec("DELETE FROM shard_replicas WHERE tbl = ? AND shard_id = ? AND node_id = ?",
				key.table, key.shardID, nodeID)
		}
	}

	delete(pm.nodeShards, nodeID)

	return needsFailover
}

// PromoteReplica promotes a replica to primary for a given shard. The old
// primary (if any) is removed. Returns an error if the replica is not found.
func (pm *PlacementManager) PromoteReplica(table string, shardID int, replicaNodeID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}
	sp, ok := pm.placements[key]
	if !ok {
		return fmt.Errorf("shard %s:%d not found in placement", table, shardID)
	}

	// Check that the replica exists.
	found := false
	newReplicas := make([]string, 0, len(sp.Replicas))
	for _, r := range sp.Replicas {
		if r == replicaNodeID {
			found = true
		} else {
			newReplicas = append(newReplicas, r)
		}
	}
	if !found {
		return fmt.Errorf("node %s is not a replica for %s:%d", replicaNodeID, table, shardID)
	}

	now := time.Now().UTC()

	// Remove old primary from node index.
	if sp.PrimaryNode != "" {
		pm.removeNodeShard(sp.PrimaryNode, key)
	}

	// Persist: update primary, remove from replicas.
	tx, err := pm.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if _, err := tx.Exec(
		`INSERT OR REPLACE INTO shard_placements (tbl, shard_id, primary_node, updated_at) VALUES (?, ?, ?, ?)`,
		table, shardID, replicaNodeID, now.Format(time.RFC3339),
	); err != nil {
		tx.Rollback()
		return fmt.Errorf("update primary: %w", err)
	}
	if _, err := tx.Exec(
		`DELETE FROM shard_replicas WHERE tbl = ? AND shard_id = ? AND node_id = ?`,
		table, shardID, replicaNodeID,
	); err != nil {
		tx.Rollback()
		return fmt.Errorf("remove promoted replica: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit promotion: %w", err)
	}

	// Update in-memory state.
	sp.PrimaryNode = replicaNodeID
	sp.Replicas = newReplicas
	sp.UpdatedAt = now
	// replicaNodeID is already in nodeShards from when it was added as replica.

	return nil
}

// SelectBestReplica picks the best replica for promotion using anti-affinity
// scoring. Nodes with fewer existing primaries are preferred. If nodeInfos is
// provided, nodes in different regions from the existing replicas are scored
// higher. Returns the node ID of the best candidate, or empty string if none.
func (pm *PlacementManager) SelectBestReplica(table string, shardID int, nodeInfos []NodeInfo) string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	key := shardKey{table: table, shardID: shardID}
	sp, ok := pm.placements[key]
	if !ok || len(sp.Replicas) == 0 {
		return ""
	}

	// Build a map of node info for quick lookup.
	infoMap := make(map[string]*NodeInfo, len(nodeInfos))
	for i := range nodeInfos {
		infoMap[nodeInfos[i].NodeID] = &nodeInfos[i]
	}

	// Count how many primaries each candidate already has.
	primaryCounts := make(map[string]int)
	for _, sp := range pm.placements {
		if sp.PrimaryNode != "" {
			primaryCounts[sp.PrimaryNode]++
		}
	}

	type candidate struct {
		nodeID string
		score  int // lower is better
	}

	var candidates []candidate
	for _, r := range sp.Replicas {
		score := primaryCounts[r] * 10 // penalize nodes with many primaries

		// Bonus for different region from other replicas.
		if info, ok := infoMap[r]; ok {
			for _, otherR := range sp.Replicas {
				if otherR == r {
					continue
				}
				if otherInfo, ok := infoMap[otherR]; ok {
					if info.Region != otherInfo.Region {
						score -= 5 // bonus for region diversity
					}
				}
			}
		}

		candidates = append(candidates, candidate{nodeID: r, score: score})
	}

	if len(candidates) == 0 {
		return ""
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score < candidates[j].score
	})

	return candidates[0].nodeID
}

// AllPlacements returns a copy of all current placements.
func (pm *PlacementManager) AllPlacements() []ShardPlacement {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make([]ShardPlacement, 0, len(pm.placements))
	for _, sp := range pm.placements {
		copy := *sp
		copy.Replicas = make([]string, len(sp.Replicas))
		for i, r := range sp.Replicas {
			copy.Replicas[i] = r
		}
		result = append(result, copy)
	}
	return result
}

// Close closes the underlying placement database.
func (pm *PlacementManager) Close() error {
	return pm.db.Close()
}
