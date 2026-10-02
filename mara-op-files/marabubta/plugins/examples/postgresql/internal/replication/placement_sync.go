// Marabunta - Licensed under the MIT License.
package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// PlacementEvent describes a placement change broadcast via Pub/Sub.
type PlacementEvent struct {
	Table     string    `json:"table"`
	ShardID   int       `json:"shard_id"`
	NodeID    string    `json:"node_id"`
	Role      string    `json:"role"` // "primary" or "replica"
	Instance  string    `json:"instance"`
	Timestamp time.Time `json:"timestamp"`
}

// PlacementSync synchronizes shard-to-node placement data across the swarm.
// It publishes local placement changes to a Pub/Sub topic and subscribes to
// receive changes from other nodes. On startup, it loads the global placement
// state from the swarm KV store.
type PlacementSync struct {
	mu       sync.Mutex
	mgr      *PlacementManager
	client   *swarm.SwarmClient
	instance string
	nodeID   string
	cancel   context.CancelFunc
	ctx      context.Context
	wg       sync.WaitGroup
	stopped  bool
}

// NewPlacementSync creates a new PlacementSync.
func NewPlacementSync(mgr *PlacementManager, client *swarm.SwarmClient, instance, nodeID string) *PlacementSync {
	ctx, cancel := context.WithCancel(context.Background())
	return &PlacementSync{
		mgr:      mgr,
		client:   client,
		instance: instance,
		nodeID:   nodeID,
		cancel:   cancel,
		ctx:      ctx,
	}
}

// placementTopic returns the Pub/Sub topic for placement events.
func (ps *PlacementSync) placementTopic() string {
	return fmt.Sprintf("pg.placement.%s", ps.instance)
}

// placementKVKey returns the KV key for persisting global placement state.
func (ps *PlacementSync) placementKVKey() string {
	return fmt.Sprintf("pg:%s:placement", ps.instance)
}

// Start subscribes to placement events and begins processing them.
// It also loads the initial global placement state from swarm KV.
func (ps *PlacementSync) Start(ctx context.Context) error {
	// Load initial placement state from swarm KV.
	if err := ps.loadFromKV(); err != nil {
		log.Printf("placement_sync: failed to load from KV (starting fresh): %v", err)
	}

	// Subscribe to placement topic.
	topic := ps.placementTopic()
	if err := ps.client.Subscribe(topic); err != nil {
		return fmt.Errorf("subscribe to placement topic %s: %w", topic, err)
	}

	// Start event processor.
	ps.wg.Add(1)
	go ps.processEvents()

	log.Printf("placement_sync: started for instance %s", ps.instance)
	return nil
}

// loadFromKV loads the global placement state from the swarm KV store.
func (ps *PlacementSync) loadFromKV() error {
	key := ps.placementKVKey()
	value, found, err := ps.client.FetchString(key)
	if err != nil {
		return fmt.Errorf("fetch placement from KV: %w", err)
	}
	if !found || value == "" {
		return nil // no existing placement state
	}

	var placements []PlacementEvent
	if err := json.Unmarshal([]byte(value), &placements); err != nil {
		return fmt.Errorf("unmarshal placement state: %w", err)
	}

	for _, p := range placements {
		ps.applyPlacement(p)
	}

	log.Printf("placement_sync: loaded %d placements from KV", len(placements))
	return nil
}

// processEvents reads incoming Pub/Sub events and applies placement updates.
func (ps *PlacementSync) processEvents() {
	defer ps.wg.Done()

	for {
		select {
		case <-ps.ctx.Done():
			return
		case evt, ok := <-ps.client.EventCh:
			if !ok {
				return
			}
			ps.handleEvent(evt)
		}
	}
}

// handleEvent processes a single placement event.
func (ps *PlacementSync) handleEvent(evt swarm.SubscribeEvent) {
	var pe PlacementEvent
	if err := json.Unmarshal(evt.Payload, &pe); err != nil {
		return // not a placement event
	}

	if pe.Instance != ps.instance || pe.Table == "" {
		return
	}

	// Don't apply our own events (we already applied locally).
	if pe.NodeID == ps.nodeID {
		return
	}

	ps.applyPlacement(pe)
}

// applyPlacement applies a placement event to the local PlacementManager.
func (ps *PlacementSync) applyPlacement(pe PlacementEvent) {
	switch pe.Role {
	case "primary":
		if err := ps.mgr.PlacePrimary(pe.Table, pe.ShardID, pe.NodeID); err != nil {
			log.Printf("placement_sync: apply primary %s for %s:%d: %v",
				pe.NodeID, pe.Table, pe.ShardID, err)
		}
	case "replica":
		if err := ps.mgr.PlaceReplica(pe.Table, pe.ShardID, pe.NodeID); err != nil {
			log.Printf("placement_sync: apply replica %s for %s:%d: %v",
				pe.NodeID, pe.Table, pe.ShardID, err)
		}
	}
}

// PublishPlacement broadcasts a placement change to the swarm.
func (ps *PlacementSync) PublishPlacement(table string, shardID int, nodeID, role string) error {
	pe := PlacementEvent{
		Table:     table,
		ShardID:   shardID,
		NodeID:    nodeID,
		Role:      role,
		Instance:  ps.instance,
		Timestamp: time.Now().UTC(),
	}

	payload, err := json.Marshal(pe)
	if err != nil {
		return fmt.Errorf("marshal placement event: %w", err)
	}

	topic := ps.placementTopic()
	if _, err := ps.client.Publish(swarm.PublishRequest{
		Topic:   topic,
		Payload: payload,
	}); err != nil {
		return fmt.Errorf("publish placement to %s: %w", topic, err)
	}

	// Also persist to KV for new nodes joining later.
	ps.persistToKV()

	return nil
}

// persistToKV saves the current global placement state to the swarm KV store.
func (ps *PlacementSync) persistToKV() {
	allPlacements := ps.mgr.AllPlacements()

	var events []PlacementEvent
	for _, sp := range allPlacements {
		if sp.PrimaryNode != "" {
			events = append(events, PlacementEvent{
				Table:    sp.Table,
				ShardID:  sp.ShardID,
				NodeID:   sp.PrimaryNode,
				Role:     "primary",
				Instance: ps.instance,
			})
		}
		for _, r := range sp.Replicas {
			events = append(events, PlacementEvent{
				Table:    sp.Table,
				ShardID:  sp.ShardID,
				NodeID:   r,
				Role:     "replica",
				Instance: ps.instance,
			})
		}
	}

	data, err := json.Marshal(events)
	if err != nil {
		log.Printf("placement_sync: marshal for KV persist: %v", err)
		return
	}

	key := ps.placementKVKey()
	if _, err := ps.client.StoreString(key, string(data)); err != nil {
		log.Printf("placement_sync: persist to KV: %v", err)
	}
}

// Stop shuts down the placement sync.
func (ps *PlacementSync) Stop() {
	ps.mu.Lock()
	if ps.stopped {
		ps.mu.Unlock()
		return
	}
	ps.stopped = true
	ps.mu.Unlock()

	ps.cancel()
	ps.wg.Wait()
}
