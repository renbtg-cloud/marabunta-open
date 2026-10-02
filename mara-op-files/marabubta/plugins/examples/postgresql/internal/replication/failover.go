// Marabunta - Licensed under the MIT License.
package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

const (
	// leaseDuration is how long a failover lease is valid.
	leaseDuration = 30 * time.Second
)

// FailoverConfig holds configuration for the failover manager.
type FailoverConfig struct {
	// PingInterval is how often to check primary health (default 5s).
	PingInterval time.Duration
	// MaxMissedPings is how many consecutive missed pings before failover (default 3).
	MaxMissedPings int
	// Instance is the PostgreSQL instance name for topic namespacing.
	Instance string
}

// DefaultFailoverConfig returns a FailoverConfig with sensible defaults.
func DefaultFailoverConfig() FailoverConfig {
	return FailoverConfig{
		PingInterval:   5 * time.Second,
		MaxMissedPings: 3,
	}
}

// PromotionEvent is published when a replica is promoted to primary.
type PromotionEvent struct {
	Table     string    `json:"table"`
	ShardID   int       `json:"shard_id"`
	OldPrimary string   `json:"old_primary"`
	NewPrimary string   `json:"new_primary"`
	LSN        uint64   `json:"lsn"`
	Timestamp  time.Time `json:"timestamp"`
	Instance   string   `json:"instance"`
}

// FenceRecord tracks fenced-out primaries. Writes from a fenced primary
// with LSN <= FencedLSN are rejected by replicas.
type FenceRecord struct {
	Table      string    `json:"table"`
	ShardID    int       `json:"shard_id"`
	NodeID     string    `json:"node_id"`
	FencedLSN  uint64    `json:"fenced_lsn"`
	FencedAt   time.Time `json:"fenced_at"`
}

// primaryHealth tracks the health state of a monitored primary.
type primaryHealth struct {
	table       string
	shardID     int
	nodeID      string
	missedPings int
	lastSeen    time.Time
}

// FailoverManager monitors primary health and orchestrates automatic failover
// when a primary becomes unresponsive. It uses lease-based consensus via
// the swarm KV store to prevent split-brain promotions, and persists fences
// globally so all nodes respect fencing decisions.
type FailoverManager struct {
	mu          sync.Mutex
	client      *swarm.SwarmClient
	placement   *PlacementManager
	follower    *Follower
	config      FailoverConfig
	nodeID      string                      // this node's swarm ID
	monitors    map[shardKey]*primaryHealth
	fences      map[shardKey]*FenceRecord
	cancel      context.CancelFunc
	ctx         context.Context
	wg          sync.WaitGroup
	stopped     bool
	onFailover  func(event PromotionEvent) // optional callback
}

// NewFailoverManager creates a new FailoverManager. The nodeID parameter
// identifies this node in the swarm for lease-based consensus.
func NewFailoverManager(
	client *swarm.SwarmClient,
	placement *PlacementManager,
	follower *Follower,
	config FailoverConfig,
	nodeID ...string, // optional for backward compatibility
) *FailoverManager {
	ctx, cancel := context.WithCancel(context.Background())

	nid := ""
	if len(nodeID) > 0 {
		nid = nodeID[0]
	}

	return &FailoverManager{
		client:    client,
		placement: placement,
		follower:  follower,
		config:    config,
		nodeID:    nid,
		monitors:  make(map[shardKey]*primaryHealth),
		fences:    make(map[shardKey]*FenceRecord),
		cancel:    cancel,
		ctx:       ctx,
	}
}

// SetOnFailover registers a callback that is invoked after a successful
// failover. This can be used to update the executor's routing table.
func (fm *FailoverManager) SetOnFailover(fn func(event PromotionEvent)) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.onFailover = fn
}

// MonitorShard begins monitoring the primary for a specific shard.
func (fm *FailoverManager) MonitorShard(table string, shardID int) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}
	primaryNode := fm.placement.GetPrimary(table, shardID)
	if primaryNode == "" {
		log.Printf("failover: cannot monitor %s:%d — no primary assigned", table, shardID)
		return
	}

	fm.monitors[key] = &primaryHealth{
		table:    table,
		shardID:  shardID,
		nodeID:   primaryNode,
		lastSeen: time.Now(),
	}

	log.Printf("failover: monitoring primary %s for %s:%d", primaryNode, table, shardID)
}

// StopMonitorShard stops monitoring a specific shard's primary.
func (fm *FailoverManager) StopMonitorShard(table string, shardID int) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}
	delete(fm.monitors, key)
}

// Start begins the health check loop. It should be called once and runs
// until Stop is called.
func (fm *FailoverManager) Start() {
	fm.wg.Add(1)
	go fm.healthCheckLoop()
}

// healthCheckLoop periodically pings primaries and triggers failover if needed.
func (fm *FailoverManager) healthCheckLoop() {
	defer fm.wg.Done()

	ticker := time.NewTicker(fm.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fm.ctx.Done():
			return
		case <-ticker.C:
			fm.checkPrimaries()
		}
	}
}

// checkPrimaries checks the health of all monitored primaries.
func (fm *FailoverManager) checkPrimaries() {
	fm.mu.Lock()
	// Snapshot the monitors to avoid holding the lock during I/O.
	monitors := make(map[shardKey]*primaryHealth, len(fm.monitors))
	for k, v := range fm.monitors {
		copy := *v
		monitors[k] = &copy
	}
	fm.mu.Unlock()

	for key, health := range monitors {
		alive := fm.isNodeAlive(health.nodeID)

		fm.mu.Lock()
		// Re-fetch from the live map (it may have been removed).
		liveHealth, ok := fm.monitors[key]
		if !ok {
			fm.mu.Unlock()
			continue
		}

		if alive {
			liveHealth.missedPings = 0
			liveHealth.lastSeen = time.Now()
		} else {
			liveHealth.missedPings++
			log.Printf("failover: primary %s for %s:%d missed ping (%d/%d)",
				liveHealth.nodeID, key.table, key.shardID,
				liveHealth.missedPings, fm.config.MaxMissedPings)

			if liveHealth.missedPings >= fm.config.MaxMissedPings {
				// Trigger failover. Copy values before releasing lock.
				table := key.table
				shardID := key.shardID
				fm.mu.Unlock()
				if err := fm.TriggerFailover(table, shardID); err != nil {
					log.Printf("failover: failed for %s:%d: %v", table, shardID, err)
				}
				continue
			}
		}
		fm.mu.Unlock()
	}
}

// isNodeAlive checks whether a node is alive via the swarm.
func (fm *FailoverManager) isNodeAlive(nodeID string) bool {
	resp, err := fm.client.IsNodeAlive(nodeID)
	if err != nil {
		log.Printf("failover: IsNodeAlive(%s) error: %v", nodeID, err)
		return false
	}
	return resp.Alive
}

// TriggerFailover manually triggers a failover for a shard. It acquires a
// distributed lease via the swarm KV store to prevent split-brain promotions,
// selects the best replica, promotes it, and fences the old primary.
func (fm *FailoverManager) TriggerFailover(table string, shardID int) error {
	// Acquire a distributed lease to prevent split-brain.
	acquired, err := fm.acquireLease(table, shardID)
	if err != nil {
		return fmt.Errorf("acquire failover lease: %w", err)
	}
	if !acquired {
		log.Printf("failover: another node is handling failover for %s:%d", table, shardID)
		return nil
	}
	defer fm.releaseLease(table, shardID)

	placement := fm.placement.GetPlacement(table, shardID)
	if placement == nil {
		return fmt.Errorf("no placement found for %s:%d", table, shardID)
	}

	oldPrimary := placement.PrimaryNode
	if len(placement.Replicas) == 0 {
		return fmt.Errorf("no replicas available for failover of %s:%d", table, shardID)
	}

	// Find the replica with the highest applied LSN.
	bestReplica, bestLSN := fm.findBestReplica(table, shardID, placement.Replicas)
	if bestReplica == "" {
		// Fall back to first available replica.
		bestReplica = placement.Replicas[0]
	}

	log.Printf("failover: promoting %s to primary for %s:%d (old primary: %s, LSN: %d)",
		bestReplica, table, shardID, oldPrimary, bestLSN)

	// Fence the old primary (locally + globally in swarm KV).
	fm.fencePrimary(table, shardID, oldPrimary, bestLSN)

	// Promote the replica.
	if err := fm.placement.PromoteReplica(table, shardID, bestReplica); err != nil {
		return fmt.Errorf("promote replica %s: %w", bestReplica, err)
	}

	// Publish promotion event.
	event := PromotionEvent{
		Table:      table,
		ShardID:    shardID,
		OldPrimary: oldPrimary,
		NewPrimary: bestReplica,
		LSN:        bestLSN,
		Timestamp:  time.Now().UTC(),
		Instance:   fm.config.Instance,
	}

	if err := fm.publishPromotion(event); err != nil {
		log.Printf("failover: failed to publish promotion event: %v", err)
		// Not fatal; the promotion itself succeeded.
	}

	// Update our monitor to track the new primary.
	fm.mu.Lock()
	key := shardKey{table: table, shardID: shardID}
	fm.monitors[key] = &primaryHealth{
		table:    table,
		shardID:  shardID,
		nodeID:   bestReplica,
		lastSeen: time.Now(),
	}
	callback := fm.onFailover
	fm.mu.Unlock()

	if callback != nil {
		callback(event)
	}

	log.Printf("failover: completed for %s:%d — new primary: %s", table, shardID, bestReplica)
	return nil
}

// findBestReplica returns the replica with the highest LSN.
func (fm *FailoverManager) findBestReplica(table string, shardID int, replicas []string) (string, uint64) {
	if fm.follower == nil {
		return "", 0
	}

	var bestNode string
	var bestLSN uint64

	for _, nodeID := range replicas {
		// In a real system, we would query each replica's LSN remotely.
		// For local follower, we use the locally tracked LSN.
		lsn := fm.follower.GetLSN(table, shardID)
		if bestNode == "" || lsn > bestLSN {
			bestNode = nodeID
			bestLSN = lsn
		}
	}

	return bestNode, bestLSN
}

// fencePrimary records a fence for the old primary, preventing it from
// accepting writes with LSN <= the fenced LSN. The fence is stored both
// locally and in the swarm KV store for global visibility.
func (fm *FailoverManager) fencePrimary(table string, shardID int, nodeID string, lsn uint64) {
	fence := &FenceRecord{
		Table:     table,
		ShardID:   shardID,
		NodeID:    nodeID,
		FencedLSN: lsn,
		FencedAt:  time.Now().UTC(),
	}

	fm.mu.Lock()
	key := shardKey{table: table, shardID: shardID}
	fm.fences[key] = fence
	fm.mu.Unlock()

	// Persist fence globally in swarm KV (when client is available).
	if fm.client != nil {
		kvKey := fmt.Sprintf("pg:%s:fence:%s:%d", fm.config.Instance, table, shardID)
		data, err := json.Marshal(fence)
		if err == nil {
			if _, storeErr := fm.client.StoreString(kvKey, string(data)); storeErr != nil {
				log.Printf("failover: persist fence to KV: %v", storeErr)
			}
		}
	}

	log.Printf("failover: fenced old primary %s for %s:%d at LSN %d",
		nodeID, table, shardID, lsn)
}

// IsFenced returns true if the given node is fenced for the specified shard.
// A fenced node should not accept writes.
func (fm *FailoverManager) IsFenced(table string, shardID int, nodeID string) bool {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}
	fence, ok := fm.fences[key]
	if !ok {
		return false
	}
	return fence.NodeID == nodeID
}

// GetFence returns the fence record for a shard, or nil if no fence exists.
func (fm *FailoverManager) GetFence(table string, shardID int) *FenceRecord {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}
	fence, ok := fm.fences[key]
	if !ok {
		return nil
	}
	copy := *fence
	return &copy
}

// acquireLease attempts to acquire a distributed lease for failover of a
// specific shard. Uses the swarm KV store as a lightweight lock. Returns
// true if the lease was acquired, false if another node holds it.
func (fm *FailoverManager) acquireLease(table string, shardID int) (bool, error) {
	if fm.client == nil {
		return true, nil // no swarm client — always succeed locally
	}
	key := fmt.Sprintf("pg:%s:failover_lease:%s:%d", fm.config.Instance, table, shardID)

	existing, found, err := fm.client.FetchString(key)
	if err != nil {
		return false, fmt.Errorf("fetch lease: %w", err)
	}

	if found && existing != "" && !isLeaseExpired(existing) {
		return false, nil // another node holds a valid lease
	}

	// Write our lease.
	leaseVal := fmt.Sprintf("%s:%d", fm.nodeID, time.Now().UnixMilli())
	if _, err := fm.client.StoreString(key, leaseVal); err != nil {
		return false, fmt.Errorf("store lease: %w", err)
	}

	return true, nil
}

// releaseLease releases a failover lease by clearing it from swarm KV.
func (fm *FailoverManager) releaseLease(table string, shardID int) {
	if fm.client == nil {
		return
	}
	key := fmt.Sprintf("pg:%s:failover_lease:%s:%d", fm.config.Instance, table, shardID)
	if _, err := fm.client.StoreString(key, ""); err != nil {
		log.Printf("failover: release lease for %s:%d: %v", table, shardID, err)
	}
}

// isLeaseExpired checks whether a lease value has expired. Lease values are
// formatted as "nodeID:timestampMillis".
func isLeaseExpired(leaseVal string) bool {
	parts := strings.SplitN(leaseVal, ":", 2)
	if len(parts) < 2 {
		return true
	}
	ms, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return true
	}
	leaseTime := time.UnixMilli(ms)
	return time.Since(leaseTime) > leaseDuration
}

// publishPromotion publishes a promotion event to the swarm.
func (fm *FailoverManager) publishPromotion(event PromotionEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal promotion event: %w", err)
	}

	topic := PromotedTopic(fm.config.Instance, event.Table, event.ShardID)
	_, err = fm.client.Publish(swarm.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
	return err
}

// OnNodeDeath is called by the swarm failure detector when a node dies.
// It triggers failover for all shards where the dead node was primary.
func (fm *FailoverManager) OnNodeDeath(nodeID string) {
	// Remove the node from placement and get shards needing failover.
	needsFailover := fm.placement.RemoveNode(nodeID)

	for _, sp := range needsFailover {
		log.Printf("failover: node %s died, triggering failover for %s:%d",
			nodeID, sp.Table, sp.ShardID)
		if err := fm.TriggerFailover(sp.Table, sp.ShardID); err != nil {
			log.Printf("failover: failed for %s:%d after node death: %v",
				sp.Table, sp.ShardID, err)
		}
	}
}

// Stop shuts down the failover manager's health check loop.
func (fm *FailoverManager) Stop() {
	fm.mu.Lock()
	if fm.stopped {
		fm.mu.Unlock()
		return
	}
	fm.stopped = true
	fm.mu.Unlock()

	fm.cancel()
	fm.wg.Wait()
}

// MonitoredShards returns the number of shards currently being monitored.
func (fm *FailoverManager) MonitoredShards() int {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return len(fm.monitors)
}

// FencedNodes returns the number of currently fenced nodes.
func (fm *FailoverManager) FencedNodes() int {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return len(fm.fences)
}
