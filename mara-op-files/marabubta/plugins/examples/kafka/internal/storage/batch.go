// Marabunta - Licensed under the MIT License.
package storage

import (
	"fmt"
	"sync"
	"time"
)

// BatchWriter groups records by partition and flushes them in batches
// to reduce the number of swarm Store calls (spec K.1).
type BatchWriter struct {
	mu        sync.Mutex
	logStore  *LogStore
	buffers   map[string]*partitionBuffer // key: "topic:partition"
	batchSize int
	flushInterval time.Duration
	stopCh    chan struct{}
	doneCh    chan struct{}
}

type partitionBuffer struct {
	topic     string
	partition int
	records   []pendingRecord
}

type pendingRecord struct {
	offset int64
	data   []byte
	errCh  chan error
}

// NewBatchWriter creates a new BatchWriter that groups writes by partition.
func NewBatchWriter(logStore *LogStore, batchSize int, flushInterval time.Duration) *BatchWriter {
	bw := &BatchWriter{
		logStore:      logStore,
		buffers:       make(map[string]*partitionBuffer),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	go bw.flushLoop()
	return bw
}

// Write queues a record for batch writing. Returns when the batch containing
// this record has been flushed.
func (bw *BatchWriter) Write(topic string, partition int, offset int64, data []byte) error {
	errCh := make(chan error, 1)

	bw.mu.Lock()
	key := fmt.Sprintf("%s:%d", topic, partition)
	buf, ok := bw.buffers[key]
	if !ok {
		buf = &partitionBuffer{
			topic:     topic,
			partition: partition,
		}
		bw.buffers[key] = buf
	}
	buf.records = append(buf.records, pendingRecord{
		offset: offset,
		data:   data,
		errCh:  errCh,
	})

	shouldFlush := len(buf.records) >= bw.batchSize
	bw.mu.Unlock()

	if shouldFlush {
		bw.flushPartition(key)
	}

	return <-errCh
}

// flushLoop periodically flushes all buffered records.
func (bw *BatchWriter) flushLoop() {
	defer close(bw.doneCh)
	ticker := time.NewTicker(bw.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bw.FlushAll()
		case <-bw.stopCh:
			bw.FlushAll()
			return
		}
	}
}

// FlushAll flushes all buffered records across all partitions.
func (bw *BatchWriter) FlushAll() {
	bw.mu.Lock()
	keys := make([]string, 0, len(bw.buffers))
	for k := range bw.buffers {
		keys = append(keys, k)
	}
	bw.mu.Unlock()

	for _, key := range keys {
		bw.flushPartition(key)
	}
}

// flushPartition flushes all buffered records for a single partition.
func (bw *BatchWriter) flushPartition(key string) {
	bw.mu.Lock()
	buf, ok := bw.buffers[key]
	if !ok || len(buf.records) == 0 {
		bw.mu.Unlock()
		return
	}

	// Take the records and capture topic/partition under the lock.
	records := buf.records
	topic := buf.topic
	partition := buf.partition
	buf.records = nil
	bw.mu.Unlock()

	// Write each record to the log store.
	for _, rec := range records {
		err := bw.logStore.Append(topic, partition, rec.offset, rec.data)
		rec.errCh <- err
	}
}

// Stop stops the batch writer and flushes remaining records.
func (bw *BatchWriter) Stop() {
	close(bw.stopCh)
	<-bw.doneCh
}

// PendingCount returns the total number of pending records across all partitions.
func (bw *BatchWriter) PendingCount() int {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	total := 0
	for _, buf := range bw.buffers {
		total += len(buf.records)
	}
	return total
}
