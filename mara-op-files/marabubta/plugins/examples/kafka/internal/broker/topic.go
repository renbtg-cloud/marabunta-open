// Marabunta - Licensed under the MIT License.
package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/marabunta/marabunta-kafka/internal/storage"
)

// TopicConfig holds configuration for a topic.
type TopicConfig struct {
	Name              string            `json:"name"`
	PartitionCount    int               `json:"partition_count"`
	ReplicationFactor int               `json:"replication_factor"`
	RetentionMs       int64             `json:"retention_ms"`
	RetentionBytes    int64             `json:"retention_bytes"`
	CleanupPolicy     string            `json:"cleanup_policy"` // "delete" or "compact"
	Config            map[string]string `json:"config"`
	CreatedAt         time.Time         `json:"created_at"`
}

// Topic represents a Kafka topic with its partitions.
type Topic struct {
	mu         sync.RWMutex
	config     TopicConfig
	partitions []*Partition
	logStore   *storage.LogStore
}

// NewTopic creates a new topic with the given number of partitions.
func NewTopic(name string, partitionCount int, logStore *storage.LogStore) *Topic {
	if partitionCount <= 0 {
		partitionCount = 1
	}

	t := &Topic{
		config: TopicConfig{
			Name:              name,
			PartitionCount:    partitionCount,
			ReplicationFactor: 1,
			RetentionMs:       7 * 24 * 3600 * 1000, // 7 days
			RetentionBytes:    -1,                    // unlimited
			CleanupPolicy:     "delete",
			Config:            make(map[string]string),
			CreatedAt:         time.Now(),
		},
		logStore: logStore,
	}

	t.partitions = make([]*Partition, partitionCount)
	for i := 0; i < partitionCount; i++ {
		t.partitions[i] = NewPartition(name, int32(i), logStore)
	}

	return t
}

// Name returns the topic name.
func (t *Topic) Name() string {
	return t.config.Name
}

// PartitionCount returns the number of partitions.
func (t *Topic) PartitionCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.partitions)
}

// Produce appends records to the specified partition.
func (t *Topic) Produce(partitionID int32, records [][]byte) (int64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return 0, fmt.Errorf("partition %d out of range (topic %s has %d partitions)",
			partitionID, t.config.Name, len(t.partitions))
	}

	return t.partitions[partitionID].Append(records)
}

// Fetch retrieves records from the specified partition.
func (t *Topic) Fetch(partitionID int32, offset int64, maxBytes int32) ([][]byte, int64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return nil, 0, fmt.Errorf("partition %d out of range (topic %s has %d partitions)",
			partitionID, t.config.Name, len(t.partitions))
	}

	return t.partitions[partitionID].Read(offset, maxBytes)
}

// GetPartition returns a partition by ID.
func (t *Topic) GetPartition(id int32) *Partition {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if id < 0 || int(id) >= len(t.partitions) {
		return nil
	}
	return t.partitions[id]
}

// Close cleans up topic resources.
func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Partitions will be garbage collected.
	t.partitions = nil
}

// MarshalConfig returns the topic configuration as JSON.
func (t *Topic) MarshalConfig() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return json.Marshal(t.config)
}

// UpdateConfig updates topic configuration.
func (t *Topic) UpdateConfig(key, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.config.Config == nil {
		t.config.Config = make(map[string]string)
	}
	t.config.Config[key] = value
	log.Printf("topic %s: config %s=%s", t.config.Name, key, value)
}
