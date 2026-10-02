// Marabunta - Licensed under the MIT License.
package broker

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/marabunta/marabunta-kafka/internal/storage"
)

// Partition represents a single partition of a topic. It tracks the next
// offset and delegates actual storage to the LogStore.
type Partition struct {
	topic      string
	id         int32
	logStore   *storage.LogStore
	nextOffset atomic.Int64
	mu         sync.Mutex // serializes appends
}

// NewPartition creates a new Partition.
func NewPartition(topic string, id int32, logStore *storage.LogStore) *Partition {
	return &Partition{
		topic:    topic,
		id:       id,
		logStore: logStore,
	}
}

// ID returns the partition ID.
func (p *Partition) ID() int32 {
	return p.id
}

// NextOffset returns the next offset that will be assigned.
func (p *Partition) NextOffset() int64 {
	return p.nextOffset.Load()
}

// HighWatermark returns the high watermark (same as next offset for now).
func (p *Partition) HighWatermark() int64 {
	return p.nextOffset.Load()
}

// Append writes records to the partition and returns the base offset.
// Records are serialized appended sequentially, each getting a unique offset.
func (p *Partition) Append(records [][]byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	baseOffset := p.nextOffset.Load()

	for i, record := range records {
		offset := baseOffset + int64(i)
		if err := p.logStore.Append(p.topic, int(p.id), offset, record); err != nil {
			return 0, fmt.Errorf("append offset %d: %w", offset, err)
		}
	}

	p.nextOffset.Store(baseOffset + int64(len(records)))
	return baseOffset, nil
}

// Read retrieves records starting from the given offset up to maxBytes.
func (p *Partition) Read(startOffset int64, maxBytes int32) ([][]byte, int64, error) {
	highWatermark := p.nextOffset.Load()

	if startOffset >= highWatermark {
		return nil, highWatermark, nil
	}

	var records [][]byte
	var totalBytes int32

	for offset := startOffset; offset < highWatermark; offset++ {
		record, err := p.logStore.Read(p.topic, int(p.id), offset)
		if err != nil {
			break // End of available data.
		}

		records = append(records, record)
		totalBytes += int32(len(record))

		if maxBytes > 0 && totalBytes >= maxBytes {
			break
		}
	}

	return records, highWatermark, nil
}

// HashPartition returns the partition ID for a key using FNV-1a hash.
// This implements consistent hash-based partition routing.
func HashPartition(key []byte, partitionCount int) int32 {
	if partitionCount <= 0 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(partitionCount))
}
