// Marabunta - Licensed under the MIT License.
// Package consumer implements consumer-side logic including background
// prefetching and locality-aware consumption.
package consumer

import (
	"sync"
	"time"

	"github.com/marabunta/marabunta-kafka/internal/storage"
)

// Prefetcher runs a background goroutine that proactively reads ahead in a
// partition's log to reduce fetch latency for consumers (spec K.3).
type Prefetcher struct {
	mu          sync.Mutex
	logStore    *storage.LogStore
	topic       string
	partition   int
	nextOffset  int64
	buffer      [][]byte
	bufferStart int64
	maxBuffer   int
	fetchSize   int
	stopCh      chan struct{}
	doneCh      chan struct{}
}

// NewPrefetcher creates a background prefetcher for a topic-partition.
func NewPrefetcher(logStore *storage.LogStore, topic string, partition int, startOffset int64, maxBuffer int) *Prefetcher {
	pf := &Prefetcher{
		logStore:    logStore,
		topic:       topic,
		partition:   partition,
		nextOffset:  startOffset,
		bufferStart: startOffset,
		maxBuffer:   maxBuffer,
		fetchSize:   100, // fetch 100 records at a time
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	go pf.run()
	return pf
}

// run is the background prefetch loop.
func (pf *Prefetcher) run() {
	defer close(pf.doneCh)

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-pf.stopCh:
			return
		case <-ticker.C:
			pf.tryPrefetch()
		}
	}
}

// tryPrefetch attempts to read ahead if the buffer has room.
func (pf *Prefetcher) tryPrefetch() {
	pf.mu.Lock()
	currentSize := len(pf.buffer)
	if currentSize >= pf.maxBuffer {
		pf.mu.Unlock()
		return
	}

	nextOffset := pf.nextOffset
	pf.mu.Unlock()

	// Read a batch of records.
	var fetched [][]byte
	for i := 0; i < pf.fetchSize; i++ {
		record, err := pf.logStore.Read(pf.topic, pf.partition, nextOffset+int64(i))
		if err != nil {
			break // End of available data.
		}
		fetched = append(fetched, record)
	}

	if len(fetched) == 0 {
		return
	}

	pf.mu.Lock()
	// Only append if we haven't been seeked past these offsets.
	if nextOffset == pf.nextOffset {
		pf.buffer = append(pf.buffer, fetched...)
		pf.nextOffset += int64(len(fetched))
		// Trim if over max.
		if len(pf.buffer) > pf.maxBuffer {
			trimCount := len(pf.buffer) - pf.maxBuffer
			pf.buffer = pf.buffer[trimCount:]
			pf.bufferStart += int64(trimCount)
		}
	}
	pf.mu.Unlock()
}

// Get returns up to maxRecords records starting from the given offset.
// Returns records from the prefetch buffer if available, otherwise nil.
func (pf *Prefetcher) Get(offset int64, maxRecords int) [][]byte {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	if offset < pf.bufferStart || offset >= pf.bufferStart+int64(len(pf.buffer)) {
		return nil
	}

	start := int(offset - pf.bufferStart)
	end := start + maxRecords
	if end > len(pf.buffer) {
		end = len(pf.buffer)
	}

	result := make([][]byte, end-start)
	copy(result, pf.buffer[start:end])
	return result
}

// Seek repositions the prefetcher to start fetching from a new offset.
func (pf *Prefetcher) Seek(offset int64) {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	pf.nextOffset = offset
	pf.bufferStart = offset
	pf.buffer = nil
}

// Stop stops the background prefetch goroutine.
func (pf *Prefetcher) Stop() {
	close(pf.stopCh)
	<-pf.doneCh
}

// BufferSize returns the current number of buffered records.
func (pf *Prefetcher) BufferSize() int {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	return len(pf.buffer)
}
