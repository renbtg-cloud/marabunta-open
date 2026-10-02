// Marabunta - Licensed under the MIT License.
// Package storage implements append-only log storage backed by the Marabunta
// Swarm distributed key-value store. Each record is stored at a key derived
// from its topic, partition, and offset.
package storage

import (
	"fmt"
	"sync"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// LogStore implements append-only log storage via swarm key-value operations.
// Key format: kafka:{topic}:{partition}:{offset:020d}
type LogStore struct {
	client *swarm.SwarmClient
	mu     sync.RWMutex
	// Cache of recently written records for fast sequential reads.
	cache map[string][]byte
}

// NewLogStore creates a new LogStore backed by the swarm client.
func NewLogStore(client *swarm.SwarmClient) *LogStore {
	return &LogStore{
		client: client,
		cache:  make(map[string][]byte),
	}
}

// recordKey returns the swarm storage key for a record.
func recordKey(topic string, partition int, offset int64) string {
	return fmt.Sprintf("kafka:%s:%d:%020d", topic, partition, offset)
}

// topicMetaKey returns the swarm storage key for topic metadata.
func topicMetaKey(topic string) string {
	return fmt.Sprintf("kafka:%s:meta", topic)
}

// Append stores a record at the given topic, partition, and offset.
func (ls *LogStore) Append(topic string, partition int, offset int64, record []byte) error {
	key := recordKey(topic, partition, offset)

	_, err := ls.client.Store(swarm.StoreRequest{
		Key:   []byte(key),
		Value: record,
		Options: swarm.StoreOptions{
			Consistency: swarm.ConsistencyEventual,
			Replicas:    3,
			TTLSeconds:  0,
		},
	})
	if err != nil {
		return fmt.Errorf("store record %s: %w", key, err)
	}

	// Cache for fast sequential reads.
	ls.mu.Lock()
	ls.cache[key] = record
	// Evict old entries if cache grows too large.
	if len(ls.cache) > 100000 {
		ls.evictCacheLocked()
	}
	ls.mu.Unlock()

	return nil
}

// Read retrieves a record at the given topic, partition, and offset.
func (ls *LogStore) Read(topic string, partition int, offset int64) ([]byte, error) {
	key := recordKey(topic, partition, offset)

	// Check cache first.
	ls.mu.RLock()
	if cached, ok := ls.cache[key]; ok {
		ls.mu.RUnlock()
		return cached, nil
	}
	ls.mu.RUnlock()

	// Fetch from swarm.
	resp, err := ls.client.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, fmt.Errorf("fetch record %s: %w", key, err)
	}
	if !resp.Found {
		return nil, fmt.Errorf("record not found: %s", key)
	}

	// Cache the fetched record.
	ls.mu.Lock()
	ls.cache[key] = resp.Value
	ls.mu.Unlock()

	return resp.Value, nil
}

// Delete removes a record at the given topic, partition, and offset.
func (ls *LogStore) Delete(topic string, partition int, offset int64) error {
	key := recordKey(topic, partition, offset)

	_, err := ls.client.Delete(swarm.DeleteRequest{
		Key: []byte(key),
	})
	if err != nil {
		return fmt.Errorf("delete record %s: %w", key, err)
	}

	ls.mu.Lock()
	delete(ls.cache, key)
	ls.mu.Unlock()

	return nil
}

// ReadRange retrieves records from startOffset to endOffset (exclusive).
func (ls *LogStore) ReadRange(topic string, partition int, startOffset, endOffset int64) ([][]byte, error) {
	var records [][]byte
	for offset := startOffset; offset < endOffset; offset++ {
		record, err := ls.Read(topic, partition, offset)
		if err != nil {
			break // End of available data.
		}
		records = append(records, record)
	}
	return records, nil
}

// StoreMeta stores topic metadata.
func (ls *LogStore) StoreMeta(topic string, metadata []byte) error {
	key := topicMetaKey(topic)
	_, err := ls.client.Store(swarm.StoreRequest{
		Key:     []byte(key),
		Value:   metadata,
		Options: swarm.DefaultStoreOptions(),
	})
	return err
}

// FetchMeta retrieves topic metadata.
func (ls *LogStore) FetchMeta(topic string) ([]byte, bool, error) {
	key := topicMetaKey(topic)
	resp, err := ls.client.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// evictCacheLocked removes approximately half the cache entries.
// Must be called with ls.mu held for writing.
func (ls *LogStore) evictCacheLocked() {
	count := 0
	target := len(ls.cache) / 2
	for k := range ls.cache {
		delete(ls.cache, k)
		count++
		if count >= target {
			break
		}
	}
}

// CacheSize returns the number of cached records.
func (ls *LogStore) CacheSize() int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return len(ls.cache)
}

// ClearCache removes all cached records.
func (ls *LogStore) ClearCache() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.cache = make(map[string][]byte)
}
