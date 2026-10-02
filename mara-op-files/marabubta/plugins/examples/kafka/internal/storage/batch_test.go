// Marabunta - Licensed under the MIT License.
package storage

import (
	"testing"
	"time"
)

func TestBatchWriterPendingCount(t *testing.T) {
	// Create a batch writer with a nil logStore (we won't actually flush).
	bw := &BatchWriter{
		buffers:       make(map[string]*partitionBuffer),
		batchSize:     100, // high threshold so we don't auto-flush
		flushInterval: time.Hour,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	// Manually add records to the buffer.
	bw.mu.Lock()
	bw.buffers["test:0"] = &partitionBuffer{
		topic:     "test",
		partition: 0,
		records: []pendingRecord{
			{offset: 0, data: []byte("hello"), errCh: make(chan error, 1)},
			{offset: 1, data: []byte("world"), errCh: make(chan error, 1)},
		},
	}
	bw.buffers["test:1"] = &partitionBuffer{
		topic:     "test",
		partition: 1,
		records: []pendingRecord{
			{offset: 0, data: []byte("foo"), errCh: make(chan error, 1)},
		},
	}
	bw.mu.Unlock()

	count := bw.PendingCount()
	if count != 3 {
		t.Errorf("expected 3 pending records, got %d", count)
	}
}

func TestPartitionBufferGrouping(t *testing.T) {
	// Verify that records are grouped by topic:partition key.
	bw := &BatchWriter{
		buffers:       make(map[string]*partitionBuffer),
		batchSize:     1000,
		flushInterval: time.Hour,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	// Add records for different partitions.
	for i := 0; i < 5; i++ {
		key := "topic-a:0"
		bw.mu.Lock()
		buf, ok := bw.buffers[key]
		if !ok {
			buf = &partitionBuffer{topic: "topic-a", partition: 0}
			bw.buffers[key] = buf
		}
		buf.records = append(buf.records, pendingRecord{
			offset: int64(i),
			data:   []byte("data"),
			errCh:  make(chan error, 1),
		})
		bw.mu.Unlock()
	}

	for i := 0; i < 3; i++ {
		key := "topic-a:1"
		bw.mu.Lock()
		buf, ok := bw.buffers[key]
		if !ok {
			buf = &partitionBuffer{topic: "topic-a", partition: 1}
			bw.buffers[key] = buf
		}
		buf.records = append(buf.records, pendingRecord{
			offset: int64(i),
			data:   []byte("data"),
			errCh:  make(chan error, 1),
		})
		bw.mu.Unlock()
	}

	if len(bw.buffers) != 2 {
		t.Errorf("expected 2 partition buffers, got %d", len(bw.buffers))
	}

	if len(bw.buffers["topic-a:0"].records) != 5 {
		t.Errorf("expected 5 records in partition 0, got %d", len(bw.buffers["topic-a:0"].records))
	}

	if len(bw.buffers["topic-a:1"].records) != 3 {
		t.Errorf("expected 3 records in partition 1, got %d", len(bw.buffers["topic-a:1"].records))
	}

	total := bw.PendingCount()
	if total != 8 {
		t.Errorf("expected 8 total pending, got %d", total)
	}
}
