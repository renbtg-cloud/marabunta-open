// Marabunta - Licensed under the MIT License.
// Package broker implements the core Kafka broker logic including topic
// management, partition routing, and produce/fetch operations.
package broker

import (
	"fmt"
	"log"
	"sync"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-kafka/internal/retention"
	"github.com/marabunta/marabunta-kafka/internal/storage"
)

// Broker is the core Kafka broker coordinating topics, partitions, and storage.
type Broker struct {
	clusterID string
	nodeID    string
	client    *swarm.SwarmClient
	logStore  *storage.LogStore
	mu        sync.RWMutex
	topics    map[string]*Topic
}

// BrokerInfo describes a broker in the cluster metadata.
type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}

// TopicMeta describes a topic in the cluster metadata.
type TopicMeta struct {
	Name           string
	PartitionCount int32
	Internal       bool
}

// ClusterMetadata describes the cluster for Metadata responses.
type ClusterMetadata struct {
	ClusterID string
	Brokers   []BrokerInfo
	Topics    []TopicMeta
}

// NewBroker creates a new Broker.
func NewBroker(clusterID, nodeID string, client *swarm.SwarmClient, logStore *storage.LogStore) *Broker {
	return &Broker{
		clusterID: clusterID,
		nodeID:    nodeID,
		client:    client,
		logStore:  logStore,
		topics:    make(map[string]*Topic),
	}
}

// NodeID returns this broker's node ID.
func (b *Broker) NodeID() string {
	return b.nodeID
}

// TopicCount returns the number of topics.
func (b *Broker) TopicCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.topics)
}

// Produce appends records to a topic partition. Auto-creates the topic
// if it doesn't exist. Returns the base offset of the appended records.
func (b *Broker) Produce(topicName string, partition int32, records [][]byte) (int64, error) {
	topic := b.getOrCreateTopic(topicName)
	return topic.Produce(partition, records)
}

// Fetch retrieves records from a topic partition starting at the given offset.
// Returns the records, the high watermark (latest offset), and any error.
func (b *Broker) Fetch(topicName string, partition int32, offset int64, maxBytes int32) ([][]byte, int64, error) {
	topic := b.GetTopic(topicName)
	if topic == nil {
		return nil, 0, fmt.Errorf("topic %q not found", topicName)
	}
	return topic.Fetch(partition, offset, maxBytes)
}

// GetMetadata returns cluster metadata for Metadata responses.
func (b *Broker) GetMetadata() *ClusterMetadata {
	b.mu.RLock()
	defer b.mu.RUnlock()

	meta := &ClusterMetadata{
		ClusterID: b.clusterID,
		Brokers: []BrokerInfo{
			{
				NodeID: 0,
				Host:   "localhost",
				Port:   9092,
			},
		},
	}

	for _, topic := range b.topics {
		meta.Topics = append(meta.Topics, TopicMeta{
			Name:           topic.Name(),
			PartitionCount: int32(topic.PartitionCount()),
		})
	}

	return meta
}

// CreateTopic creates a new topic with the given partition count.
func (b *Broker) CreateTopic(name string, partitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	topic := NewTopic(name, partitions, b.logStore)
	b.topics[name] = topic
	log.Printf("broker: created topic %q with %d partitions", name, partitions)
	return nil
}

// DeleteTopic deletes a topic and its data.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic, exists := b.topics[name]
	if !exists {
		return fmt.Errorf("topic %q not found", name)
	}
	topic.Close()
	delete(b.topics, name)
	log.Printf("broker: deleted topic %q", name)
	return nil
}

// GetTopic returns a topic by name, or nil if not found.
func (b *Broker) GetTopic(name string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

// ListTopicsForRetention returns topic metadata needed by the retention engine.
// This satisfies the retention.TopicSource interface.
func (b *Broker) ListTopicsForRetention() []retention.TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []retention.TopicInfo

	for _, topic := range b.topics {
		pCount := topic.PartitionCount()
		hwm := make(map[int]int64)
		for i := 0; i < pCount; i++ {
			p := topic.GetPartition(int32(i))
			if p != nil {
				hwm[i] = p.HighWatermark()
			}
		}
		result = append(result, retention.TopicInfo{
			Name:           topic.Name(),
			PartitionCount: pCount,
			HighWatermarks: hwm,
		})
	}

	return result
}

// getOrCreateTopic returns an existing topic or auto-creates it.
func (b *Broker) getOrCreateTopic(name string) *Topic {
	topic := b.GetTopic(name)
	if topic != nil {
		return topic
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Double-check under write lock.
	topic = b.topics[name]
	if topic != nil {
		return topic
	}

	// Auto-create with default partition count.
	topic = NewTopic(name, 8, b.logStore)
	b.topics[name] = topic
	log.Printf("broker: auto-created topic %q with 8 partitions", name)
	return topic
}
