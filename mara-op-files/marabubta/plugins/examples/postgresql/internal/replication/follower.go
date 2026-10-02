// Marabunta - Licensed under the MIT License.
package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-postgres/internal/storage"
)

// FollowerConfig holds configuration for a WAL follower.
type FollowerConfig struct {
	// MaxLag is the maximum acceptable replication lag in LSN entries.
	// If the lag exceeds this threshold, IsHealthy returns false.
	MaxLag uint64
	// Instance is the PostgreSQL instance name for topic namespacing.
	Instance string
}

// DefaultFollowerConfig returns a FollowerConfig with sensible defaults.
func DefaultFollowerConfig() FollowerConfig {
	return FollowerConfig{
		MaxLag: 1000,
	}
}

// Follower subscribes to WAL topics for specific shards and applies received
// WALEntry statements to a local SQLite replica. It tracks the last-applied
// LSN per shard and can detect gaps that require a resync from the primary.
type Follower struct {
	mu         sync.RWMutex
	client     *swarm.SwarmClient
	shardMgr   *storage.ShardManager
	config     FollowerConfig
	appliedLSN map[shardKey]*uint64 // last applied LSN per shard
	primaryLSN map[shardKey]*uint64 // last known primary LSN per shard
	following  map[shardKey]bool    // which shards we are following
	gapCh      chan shardKey         // channel for gap notifications
	cancel     context.CancelFunc
	ctx        context.Context
	wg         sync.WaitGroup
	stopped    bool
}

// NewFollower creates a new Follower that applies WAL entries to local shards.
func NewFollower(client *swarm.SwarmClient, shardMgr *storage.ShardManager, config FollowerConfig) *Follower {
	if config.MaxLag == 0 {
		config.MaxLag = 1000
	}

	ctx, cancel := context.WithCancel(context.Background())

	f := &Follower{
		client:     client,
		shardMgr:   shardMgr,
		config:     config,
		appliedLSN: make(map[shardKey]*uint64),
		primaryLSN: make(map[shardKey]*uint64),
		following:  make(map[shardKey]bool),
		gapCh:      make(chan shardKey, 64),
		cancel:     cancel,
		ctx:        ctx,
	}

	f.startGapHandler()

	return f
}

// Start begins following WAL entries for a specific shard. It subscribes to
// the shard's WAL Pub/Sub topic and starts applying received entries.
func (f *Follower) Start(table string, shardID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := shardKey{table: table, shardID: shardID}
	if f.following[key] {
		return nil // already following
	}

	topic := WALTopic(f.config.Instance, table, shardID)
	if err := f.client.Subscribe(topic); err != nil {
		return fmt.Errorf("subscribe to WAL topic %s: %w", topic, err)
	}

	// Initialize LSN tracking.
	var applied uint64
	f.appliedLSN[key] = &applied
	var primary uint64
	f.primaryLSN[key] = &primary
	f.following[key] = true

	log.Printf("follower: started following %s:%d on topic %s", table, shardID, topic)

	return nil
}

// StartEventProcessor launches a goroutine that reads events from the swarm
// client's EventCh and dispatches WAL batches to be applied. This should be
// called once, and it handles events for all followed shards.
func (f *Follower) StartEventProcessor() {
	f.wg.Add(1)
	go f.processEvents()
}

// processEvents reads incoming Pub/Sub events and applies WAL batches.
func (f *Follower) processEvents() {
	defer f.wg.Done()

	for {
		select {
		case <-f.ctx.Done():
			return
		case evt, ok := <-f.client.EventCh:
			if !ok {
				return
			}
			f.handleEvent(evt)
		}
	}
}

// handleEvent processes a single Pub/Sub event. It checks whether the event
// is a WAL batch for a shard we are following and applies the entries.
func (f *Follower) handleEvent(evt swarm.SubscribeEvent) {
	var batch WALBatch
	if err := json.Unmarshal(evt.Payload, &batch); err != nil {
		// Not a WAL batch; might be a different event type.
		return
	}

	if batch.Table == "" || len(batch.Entries) == 0 {
		return
	}

	key := shardKey{table: batch.Table, shardID: batch.ShardID}

	f.mu.RLock()
	isFollowing := f.following[key]
	f.mu.RUnlock()

	if !isFollowing {
		return
	}

	if err := f.applyBatch(key, batch); err != nil {
		log.Printf("follower: error applying batch for %s:%d: %v", batch.Table, batch.ShardID, err)
	}
}

// applyBatch applies a batch of WAL entries to the local shard replica.
// It detects gaps in LSN sequence and signals for resync if needed.
func (f *Follower) applyBatch(key shardKey, batch WALBatch) error {
	f.mu.RLock()
	appliedPtr := f.appliedLSN[key]
	primaryPtr := f.primaryLSN[key]
	f.mu.RUnlock()

	if appliedPtr == nil || primaryPtr == nil {
		return fmt.Errorf("shard %s:%d is not being followed", key.table, key.shardID)
	}

	currentLSN := atomic.LoadUint64(appliedPtr)

	// Update the known primary LSN.
	if len(batch.Entries) > 0 {
		lastEntry := batch.Entries[len(batch.Entries)-1]
		atomic.StoreUint64(primaryPtr, lastEntry.LSN)
	}

	// Collect SQL statements for batch execution, detecting gaps.
	var stmts []string
	var highestLSN uint64
	gapDetected := false

	for _, entry := range batch.Entries {
		// Skip already-applied entries.
		if entry.LSN <= currentLSN {
			continue
		}

		// Check for gaps.
		if currentLSN > 0 && entry.LSN > currentLSN+1 && !gapDetected {
			gapDetected = true
			log.Printf("follower: gap detected for %s:%d: expected LSN %d, got %d",
				key.table, key.shardID, currentLSN+1, entry.LSN)
			// Signal gap for potential resync.
			select {
			case f.gapCh <- key:
			default:
			}
		}

		stmts = append(stmts, entry.SQL)
		highestLSN = entry.LSN
	}

	if len(stmts) == 0 {
		return nil
	}

	// Apply all statements as a batch within a transaction.
	ctx := context.Background()
	if err := f.shardMgr.BatchExec(ctx, key.table, key.shardID, stmts); err != nil {
		return fmt.Errorf("apply WAL batch: %w", err)
	}

	// Update applied LSN.
	atomic.StoreUint64(appliedPtr, highestLSN)

	log.Printf("follower: applied %d entries for %s:%d (LSN up to %d)",
		len(stmts), key.table, key.shardID, highestLSN)

	return nil
}

// Stop stops following all shards and shuts down the event processor.
func (f *Follower) Stop() {
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return
	}
	f.stopped = true
	f.mu.Unlock()

	f.cancel()
	f.wg.Wait()
}

// GetLSN returns the last-applied LSN for a specific shard.
func (f *Follower) GetLSN(table string, shardID int) uint64 {
	key := shardKey{table: table, shardID: shardID}
	f.mu.RLock()
	ptr := f.appliedLSN[key]
	f.mu.RUnlock()
	if ptr == nil {
		return 0
	}
	return atomic.LoadUint64(ptr)
}

// GetPrimaryLSN returns the last-known primary LSN for a specific shard.
func (f *Follower) GetPrimaryLSN(table string, shardID int) uint64 {
	key := shardKey{table: table, shardID: shardID}
	f.mu.RLock()
	ptr := f.primaryLSN[key]
	f.mu.RUnlock()
	if ptr == nil {
		return 0
	}
	return atomic.LoadUint64(ptr)
}

// Lag returns the replication lag in LSN entries for a shard.
func (f *Follower) Lag(table string, shardID int) uint64 {
	applied := f.GetLSN(table, shardID)
	primary := f.GetPrimaryLSN(table, shardID)
	if primary > applied {
		return primary - applied
	}
	return 0
}

// IsHealthy returns true if the follower's replication lag for the given shard
// is below the configured MaxLag threshold.
func (f *Follower) IsHealthy(table string, shardID int) bool {
	return f.Lag(table, shardID) < f.config.MaxLag
}

// IsFollowing returns true if this follower is actively following the given shard.
func (f *Follower) IsFollowing(table string, shardID int) bool {
	key := shardKey{table: table, shardID: shardID}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.following[key]
}

// GapCh returns a channel that receives shard keys when a gap is detected.
// The caller can use this to trigger a full resync from the primary.
func (f *Follower) GapCh() <-chan shardKey {
	return f.gapCh
}

// SetAppliedLSN sets the applied LSN for a shard. This is used during
// initialization to restore state from persistent storage.
func (f *Follower) SetAppliedLSN(table string, shardID int, lsn uint64) {
	key := shardKey{table: table, shardID: shardID}
	f.mu.Lock()
	ptr, ok := f.appliedLSN[key]
	if !ok {
		ptr = new(uint64)
		f.appliedLSN[key] = ptr
	}
	f.mu.Unlock()
	atomic.StoreUint64(ptr, lsn)
}

// FollowedShards returns the list of shards currently being followed.
func (f *Follower) FollowedShards() []shardKey {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var result []shardKey
	for key, active := range f.following {
		if active {
			result = append(result, key)
		}
	}
	return result
}

// startGapHandler starts a goroutine that consumes gap notifications from
// gapCh and attempts to request a resync from the primary for each gap.
func (f *Follower) startGapHandler() {
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			select {
			case <-f.ctx.Done():
				return
			case key, ok := <-f.gapCh:
				if !ok {
					return
				}
				log.Printf("follower: gap at %s:%d, requesting resync", key.table, key.shardID)
				f.requestResync(key.table, key.shardID)
			}
		}
	}()
}

// snapshotRequest is the JSON payload sent to the primary via scatter to
// request a shard snapshot. It mirrors executor.ScatterQuery but is defined
// locally to avoid an import cycle.
type snapshotRequest struct {
	SQL      string `json:"sql"`
	Table    string `json:"table"`
	ShardID  int    `json:"shard_id"`
	Instance string `json:"instance"`
	TxnOp    string `json:"txn_op"`
}

// snapshotResponse is the JSON payload returned from a snapshot scatter
// request. Mirrors executor.ScatterQueryResult.
type snapshotResponse struct {
	Columns  []string   `json:"columns"`
	Rows     [][]string `json:"rows"`
	Error    string     `json:"error"`
	Affected int64      `json:"affected"`
}

// requestResync sends a snapshot request to the primary via the swarm scatter
// API for the given table and shard. On receiving snapshot data, it imports
// the statements via shardMgr. This is a best-effort operation; failures are
// logged but do not propagate.
func (f *Follower) requestResync(table string, shardID int) {
	if f.client == nil {
		log.Printf("follower: cannot resync %s:%d — no swarm client available", table, shardID)
		return
	}

	req := snapshotRequest{
		Table:    table,
		ShardID:  shardID,
		Instance: f.config.Instance,
		TxnOp:    "snapshot",
	}
	payload, err := json.Marshal(req)
	if err != nil {
		log.Printf("follower: resync marshal error for %s:%d: %v", table, shardID, err)
		return
	}

	resp, err := f.client.Scatter(swarm.ScatterRequest{
		Units: []swarm.ScatterUnit{
			{
				Payload:        payload,
				RequiredTraits: []string{"CanStoreState"},
			},
		},
		Hints: swarm.ScatterHints{
			Locality:    swarm.LocalityPreferLocal,
			Consistency: swarm.ConsistencyEventual,
			Priority:    swarm.PriorityNormal,
			TimeoutMS:   30000,
		},
	})
	if err != nil {
		log.Printf("follower: resync scatter error for %s:%d: %v", table, shardID, err)
		return
	}

	if len(resp.Results) == 0 {
		log.Printf("follower: resync got no results for %s:%d", table, shardID)
		return
	}

	result := resp.Results[0]
	if !result.Success {
		log.Printf("follower: resync failed for %s:%d: %s", table, shardID, result.Error)
		return
	}

	var snap snapshotResponse
	if err := json.Unmarshal(result.Response, &snap); err != nil {
		log.Printf("follower: resync unmarshal error for %s:%d: %v", table, shardID, err)
		return
	}

	if snap.Error != "" {
		log.Printf("follower: resync returned error for %s:%d: %s", table, shardID, snap.Error)
		return
	}

	// Import snapshot data by executing each row as a SQL statement via
	// shardMgr. The snapshot response's first column contains SQL statements.
	if len(snap.Rows) > 0 && len(snap.Columns) > 0 {
		var stmts []string
		for _, row := range snap.Rows {
			if len(row) > 0 {
				stmts = append(stmts, row[0])
			}
		}
		if len(stmts) > 0 {
			ctx := context.Background()
			if err := f.shardMgr.BatchExec(ctx, table, shardID, stmts); err != nil {
				log.Printf("follower: resync import error for %s:%d: %v", table, shardID, err)
				return
			}
			log.Printf("follower: resync imported %d statements for %s:%d", len(stmts), table, shardID)
		}
	}

	log.Printf("follower: resync completed for %s:%d", table, shardID)
}
