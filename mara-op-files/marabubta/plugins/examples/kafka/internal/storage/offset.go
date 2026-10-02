// Marabunta - Licensed under the MIT License.
package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// OffsetManager tracks committed consumer offsets per (topic, partition, group).
// Supports async commit with periodic persistence to the swarm (spec K.2).
type OffsetManager struct {
	mu         sync.RWMutex
	client     *swarm.SwarmClient
	offsets    map[string]*GroupOffsets // key: "topic:group"
	dirty      map[string]bool
	commitInterval time.Duration
	stopCh     chan struct{}
	doneCh     chan struct{}
}

// GroupOffsets holds committed offsets for all partitions in a consumer group.
type GroupOffsets struct {
	Topic      string          `json:"topic"`
	Group      string          `json:"group"`
	Partitions map[int32]int64 `json:"partitions"` // partition -> committed offset
	UpdatedAt  time.Time       `json:"updated_at"`
}

// NewOffsetManager creates a new OffsetManager with async commit.
func NewOffsetManager(client *swarm.SwarmClient, commitInterval time.Duration) *OffsetManager {
	om := &OffsetManager{
		client:         client,
		offsets:        make(map[string]*GroupOffsets),
		dirty:          make(map[string]bool),
		commitInterval: commitInterval,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
	go om.commitLoop()
	return om
}

// Commit records a consumer's committed offset for a partition.
// The offset is stored locally and periodically flushed to the swarm.
func (om *OffsetManager) Commit(topic, group string, partition int32, offset int64) {
	om.mu.Lock()
	defer om.mu.Unlock()

	key := fmt.Sprintf("%s:%s", topic, group)
	go_, ok := om.offsets[key]
	if !ok {
		go_ = &GroupOffsets{
			Topic:      topic,
			Group:      group,
			Partitions: make(map[int32]int64),
		}
		om.offsets[key] = go_
	}
	go_.Partitions[partition] = offset
	go_.UpdatedAt = time.Now()
	om.dirty[key] = true
}

// Fetch returns the committed offset for a partition, or -1 if not found.
func (om *OffsetManager) Fetch(topic, group string, partition int32) int64 {
	om.mu.RLock()
	defer om.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", topic, group)
	go_, ok := om.offsets[key]
	if !ok {
		return -1
	}
	offset, ok := go_.Partitions[partition]
	if !ok {
		return -1
	}
	return offset
}

// FetchAll returns all committed offsets for a group on a topic.
func (om *OffsetManager) FetchAll(topic, group string) map[int32]int64 {
	om.mu.RLock()
	defer om.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", topic, group)
	go_, ok := om.offsets[key]
	if !ok {
		return nil
	}

	result := make(map[int32]int64)
	for p, o := range go_.Partitions {
		result[p] = o
	}
	return result
}

// Load retrieves committed offsets from the swarm.
func (om *OffsetManager) Load(topic, group string) error {
	key := fmt.Sprintf("kafka:%s:%s:offsets", topic, group)
	resp, err := om.client.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return fmt.Errorf("load offsets: %w", err)
	}
	if !resp.Found {
		return nil
	}

	var go_ GroupOffsets
	if err := json.Unmarshal(resp.Value, &go_); err != nil {
		return fmt.Errorf("unmarshal offsets: %w", err)
	}

	om.mu.Lock()
	defer om.mu.Unlock()

	localKey := fmt.Sprintf("%s:%s", topic, group)
	om.offsets[localKey] = &go_
	return nil
}

// commitLoop periodically flushes dirty offsets to the swarm.
func (om *OffsetManager) commitLoop() {
	defer close(om.doneCh)
	ticker := time.NewTicker(om.commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			om.flushDirty()
		case <-om.stopCh:
			om.flushDirty()
			return
		}
	}
}

// flushDirty persists all dirty offsets to the swarm.
func (om *OffsetManager) flushDirty() {
	om.mu.Lock()
	dirtyKeys := make([]string, 0, len(om.dirty))
	for k := range om.dirty {
		dirtyKeys = append(dirtyKeys, k)
	}
	// Clear dirty set.
	om.dirty = make(map[string]bool)
	om.mu.Unlock()

	for _, localKey := range dirtyKeys {
		om.mu.RLock()
		go_, ok := om.offsets[localKey]
		if !ok {
			om.mu.RUnlock()
			continue
		}
		data, err := json.Marshal(go_)
		om.mu.RUnlock()

		if err != nil {
			continue
		}

		swarmKey := fmt.Sprintf("kafka:%s:%s:offsets", go_.Topic, go_.Group)
		if _, err := om.client.Store(swarm.StoreRequest{
			Key:     []byte(swarmKey),
			Value:   data,
			Options: swarm.DefaultStoreOptions(),
		}); err != nil {
			log.Printf("offset manager: failed to flush offsets for %s: %v", localKey, err)
		}
	}
}

// Stop stops the async commit loop and flushes remaining dirty offsets.
func (om *OffsetManager) Stop() {
	close(om.stopCh)
	<-om.doneCh
}
