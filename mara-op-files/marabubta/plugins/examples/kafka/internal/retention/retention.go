// Marabunta - Licensed under the MIT License.
// Package retention implements hourly enforcement of time-based and size-based
// log retention policies (spec K.5).
package retention

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marabunta/marabunta-kafka/internal/storage"
)

// TopicInfo provides the metadata the retention engine needs about a topic.
type TopicInfo struct {
	Name           string
	PartitionCount int
	HighWatermarks map[int]int64 // partition -> high watermark
}

// TopicSource provides the current set of topics and their metadata.
// This is typically implemented by the broker.
type TopicSource interface {
	// ListTopicsForRetention returns the current topic metadata needed for
	// retention enforcement. Implementations must be safe for concurrent use.
	ListTopicsForRetention() []TopicInfo
}

// Engine enforces retention policies on topic logs.
type Engine struct {
	logStore       *storage.LogStore
	retentionHours atomic.Int64
	interval       time.Duration
	topicSource    TopicSource

	// logStartOffsets tracks the lowest non-deleted offset per topic-partition
	// so that subsequent enforcement passes don't re-scan already-deleted offsets.
	mu              sync.Mutex
	logStartOffsets map[string]int64 // key: "topic:partition"
}

// NewEngine creates a new retention engine.
func NewEngine(logStore *storage.LogStore, retentionHours int) *Engine {
	e := &Engine{
		logStore:        logStore,
		interval:        1 * time.Hour,
		logStartOffsets: make(map[string]int64),
	}
	e.retentionHours.Store(int64(retentionHours))
	return e
}

// SetTopicSource sets the provider of topic metadata for retention enforcement.
// If not set, the enforce() loop will log a warning and skip enforcement.
func (e *Engine) SetTopicSource(src TopicSource) {
	e.topicSource = src
}

// Run starts the hourly retention enforcement loop.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	log.Printf("retention: started with %d hour retention, checking every %v",
		e.retentionHours.Load(), e.interval)

	for {
		select {
		case <-ctx.Done():
			log.Println("retention: shutting down")
			return
		case <-ticker.C:
			e.enforce()
		}
	}
}

// enforce runs a single retention pass across all known topics.
func (e *Engine) enforce() {
	if e.topicSource == nil {
		log.Printf("retention: no topic source configured, skipping enforcement")
		return
	}

	topics := e.topicSource.ListTopicsForRetention()
	totalDeleted := 0
	for _, t := range topics {
		deleted := e.EnforceForTopic(t.Name, t.PartitionCount, t.HighWatermarks)
		totalDeleted += deleted
	}

	if totalDeleted > 0 {
		log.Printf("retention: enforcement pass deleted %d total records", totalDeleted)
	}
}

// EnforceForTopic enforces retention for a specific topic.
func (e *Engine) EnforceForTopic(topic string, partitionCount int, highWatermarks map[int]int64) int {
	totalDeleted := 0

	for partition := 0; partition < partitionCount; partition++ {
		hwm, ok := highWatermarks[partition]
		if !ok {
			continue
		}

		deleted := e.enforcePartition(topic, partition, hwm)
		totalDeleted += deleted
	}

	if totalDeleted > 0 {
		log.Printf("retention: deleted %d records from topic %s", totalDeleted, topic)
	}

	return totalDeleted
}

// enforcePartition enforces retention for a single partition.
func (e *Engine) enforcePartition(topic string, partition int, highWatermark int64) int {
	deleted := 0

	// Calculate the estimated log start offset based on retention.
	// In practice, records would have timestamps and we'd delete based on age.
	// For now, we use a simple approach: if the log has more records than
	// what would fit in the retention window, delete the oldest.

	maxRecords := e.retentionHours.Load() * 3600 // rough estimate: 1 record/second
	if highWatermark <= maxRecords {
		return 0
	}

	deleteUpTo := highWatermark - maxRecords

	// Start from the tracked log start offset to avoid re-scanning offsets
	// that were already deleted in a previous pass.
	e.mu.Lock()
	key := logStartKey(topic, partition)
	startOffset := e.logStartOffsets[key]
	e.mu.Unlock()

	for offset := startOffset; offset < deleteUpTo; offset++ {
		if err := e.logStore.Delete(topic, partition, offset); err != nil {
			// Record may already be deleted; advance past it.
			continue
		}
		deleted++
	}

	// Update the tracked start offset.
	e.mu.Lock()
	e.logStartOffsets[key] = deleteUpTo
	e.mu.Unlock()

	return deleted
}

// logStartKey returns a map key for tracking log start offsets.
func logStartKey(topic string, partition int) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}

// RetentionHours returns the configured retention period.
func (e *Engine) RetentionHours() int {
	return int(e.retentionHours.Load())
}

// SetRetentionHours updates the retention period.
func (e *Engine) SetRetentionHours(hours int) {
	e.retentionHours.Store(int64(hours))
	log.Printf("retention: updated to %d hours", hours)
}
