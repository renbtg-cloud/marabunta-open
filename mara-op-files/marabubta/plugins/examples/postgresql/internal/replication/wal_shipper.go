// Marabunta - Licensed under the MIT License.
package replication

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// WALEntry represents a single write-ahead log entry. Each entry captures
// a SQL statement that was successfully executed on a primary shard, along
// with its arguments. Entries are shipped to replicas via Pub/Sub.
type WALEntry struct {
	LSN       uint64        `json:"lsn"`
	Table     string        `json:"table"`
	ShardID   int           `json:"shard_id"`
	SQL       string        `json:"sql"`
	Args      []interface{} `json:"args,omitempty"`
	Timestamp time.Time     `json:"timestamp"`
	NodeID    string        `json:"node_id"`
}

// WALBatch is a group of WALEntry values shipped together for efficiency.
type WALBatch struct {
	Entries []WALEntry `json:"entries"`
	Table   string     `json:"table"`
	ShardID int        `json:"shard_id"`
}

// WALShipperConfig holds configuration for the WAL shipper.
type WALShipperConfig struct {
	// FlushInterval is the maximum time between flushes (default 100ms).
	FlushInterval time.Duration
	// BatchSize is the maximum number of entries per batch (default 100).
	BatchSize int
	// Instance is the PostgreSQL instance name for topic namespacing.
	Instance string
	// NodeID is this node's swarm ID.
	NodeID string
	// OutboxPath is the path to the SQLite outbox database. When set, WAL
	// entries are persisted to the outbox before publish attempts so that
	// they survive transient failures. A background drain goroutine retries
	// unpublished entries periodically. If empty, outbox is disabled and
	// the shipper behaves as before.
	OutboxPath string
}

// DefaultWALShipperConfig returns a WALShipperConfig with sensible defaults.
func DefaultWALShipperConfig() WALShipperConfig {
	return WALShipperConfig{
		FlushInterval: 100 * time.Millisecond,
		BatchSize:     100,
	}
}

// WALShipper captures write operations from primaries and publishes them as
// WALEntry batches via the swarm's Pub/Sub system. Replicas subscribe to
// the per-shard topics to receive and replay these entries.
//
// Topic format: pg.wal.{instance}.{table}.{shard_id}
type WALShipper struct {
	mu             sync.Mutex
	client         *swarm.SwarmClient
	config         WALShipperConfig
	buffers        map[shardKey][]WALEntry // pending entries per shard
	lsns           map[shardKey]*uint64    // atomic LSN counters per shard
	stopCh         chan struct{}
	stopped        bool
	wg             sync.WaitGroup
	outboxDB       *sql.DB       // optional persistent outbox (nil when disabled)
	outboxDrainStop chan struct{} // signal to stop the drain goroutine
}

// NewWALShipper creates a new WALShipper that publishes entries via the given
// swarm client. The shipper starts a background goroutine that periodically
// flushes buffered entries.
func NewWALShipper(client *swarm.SwarmClient, config WALShipperConfig) *WALShipper {
	if config.FlushInterval <= 0 {
		config.FlushInterval = 100 * time.Millisecond
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	ws := &WALShipper{
		client:          client,
		config:          config,
		buffers:         make(map[shardKey][]WALEntry),
		lsns:            make(map[shardKey]*uint64),
		stopCh:          make(chan struct{}),
		outboxDrainStop: make(chan struct{}),
	}

	// Initialize the persistent outbox when a path is configured.
	if config.OutboxPath != "" {
		if err := os.MkdirAll(filepath.Dir(config.OutboxPath), 0755); err != nil {
			log.Printf("wal_shipper: failed to create outbox directory: %v", err)
		} else {
			dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000", config.OutboxPath)
			db, err := sql.Open("sqlite3", dsn)
			if err != nil {
				log.Printf("wal_shipper: failed to open outbox db: %v", err)
			} else {
				db.SetMaxOpenConns(1)
				if err := db.Ping(); err != nil {
					log.Printf("wal_shipper: failed to ping outbox db: %v", err)
					db.Close()
				} else {
					if err := ws.initOutboxSchema(db); err != nil {
						log.Printf("wal_shipper: failed to init outbox schema: %v", err)
						db.Close()
					} else {
						ws.outboxDB = db
						log.Printf("wal_shipper: outbox enabled at %s", config.OutboxPath)
					}
				}
			}
		}
	}

	ws.wg.Add(1)
	go ws.flushLoop()

	// Start the outbox drain goroutine when outbox is available.
	if ws.outboxDB != nil {
		ws.wg.Add(1)
		go ws.drainOutbox()
	}

	return ws
}

// WALTopic returns the Pub/Sub topic for WAL entries for a given shard.
func WALTopic(instance, table string, shardID int) string {
	return fmt.Sprintf("pg.wal.%s.%s.%d", instance, table, shardID)
}

// PromotedTopic returns the Pub/Sub topic for promotion notifications.
func PromotedTopic(instance, table string, shardID int) string {
	return fmt.Sprintf("pg.promoted.%s.%s.%d", instance, table, shardID)
}

// nextLSN atomically increments and returns the next LSN for a shard.
func (ws *WALShipper) nextLSN(key shardKey) uint64 {
	ws.mu.Lock()
	lsnPtr, ok := ws.lsns[key]
	if !ok {
		var initial uint64
		lsnPtr = &initial
		ws.lsns[key] = lsnPtr
	}
	ws.mu.Unlock()

	return atomic.AddUint64(lsnPtr, 1)
}

// GetLSN returns the current (last shipped) LSN for a shard.
func (ws *WALShipper) GetLSN(table string, shardID int) uint64 {
	key := shardKey{table: table, shardID: shardID}
	ws.mu.Lock()
	lsnPtr, ok := ws.lsns[key]
	ws.mu.Unlock()
	if !ok {
		return 0
	}
	return atomic.LoadUint64(lsnPtr)
}

// SetLSN sets the LSN counter for a shard. This is used during initialization
// to restore the LSN from persistent state.
func (ws *WALShipper) SetLSN(table string, shardID int, lsn uint64) {
	key := shardKey{table: table, shardID: shardID}
	ws.mu.Lock()
	lsnPtr, ok := ws.lsns[key]
	if !ok {
		lsnPtr = new(uint64)
		ws.lsns[key] = lsnPtr
	}
	ws.mu.Unlock()
	atomic.StoreUint64(lsnPtr, lsn)
}

// Ship enqueues a WAL entry for shipping. The entry is buffered and will be
// published when the buffer reaches BatchSize or the FlushInterval elapses.
// The LSN is assigned automatically and monotonically.
//
// When an outbox is configured, entries are persisted to the outbox before
// any publish attempt, ensuring crash-safety. On publish failure the entries
// remain in the outbox and will be retried by the drain goroutine.
func (ws *WALShipper) Ship(entry WALEntry) error {
	key := shardKey{table: entry.Table, shardID: entry.ShardID}

	// Assign LSN.
	entry.LSN = ws.nextLSN(key)
	entry.Timestamp = time.Now().UTC()
	entry.NodeID = ws.config.NodeID

	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.stopped {
		return fmt.Errorf("WAL shipper is stopped")
	}

	ws.buffers[key] = append(ws.buffers[key], entry)

	// Flush if batch size reached.
	if len(ws.buffers[key]) >= ws.config.BatchSize {
		entries := ws.buffers[key]
		ws.buffers[key] = nil
		// Release lock before I/O.
		ws.mu.Unlock()
		var err error
		if ws.outboxDB != nil {
			err = ws.shipWithOutbox(key, entries)
		} else {
			err = ws.publishBatch(key, entries)
		}
		ws.mu.Lock()
		return err
	}

	return nil
}

// Flush immediately publishes all buffered entries for all shards.
func (ws *WALShipper) Flush() error {
	ws.mu.Lock()
	// Snapshot and clear all buffers.
	toFlush := make(map[shardKey][]WALEntry, len(ws.buffers))
	for k, v := range ws.buffers {
		if len(v) > 0 {
			toFlush[k] = v
			ws.buffers[k] = nil
		}
	}
	ws.mu.Unlock()

	var lastErr error
	for key, entries := range toFlush {
		var err error
		if ws.outboxDB != nil {
			err = ws.shipWithOutbox(key, entries)
		} else {
			err = ws.publishBatch(key, entries)
		}
		if err != nil {
			lastErr = err
			log.Printf("wal_shipper: flush error for %s:%d: %v", key.table, key.shardID, err)
		}
	}
	return lastErr
}

// FlushShard immediately publishes all buffered entries for a specific shard.
func (ws *WALShipper) FlushShard(table string, shardID int) error {
	key := shardKey{table: table, shardID: shardID}

	ws.mu.Lock()
	entries := ws.buffers[key]
	ws.buffers[key] = nil
	ws.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	if ws.outboxDB != nil {
		return ws.shipWithOutbox(key, entries)
	}
	return ws.publishBatch(key, entries)
}

// publishBatch publishes a batch of WAL entries to the Pub/Sub topic.
func (ws *WALShipper) publishBatch(key shardKey, entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	batch := WALBatch{
		Entries: entries,
		Table:   key.table,
		ShardID: key.shardID,
	}

	payload, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal WAL batch: %w", err)
	}

	topic := WALTopic(ws.config.Instance, key.table, key.shardID)
	_, err = ws.client.Publish(swarm.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("publish WAL batch to %s: %w", topic, err)
	}

	log.Printf("wal_shipper: shipped %d entries to %s (LSN %d-%d)",
		len(entries), topic, entries[0].LSN, entries[len(entries)-1].LSN)

	return nil
}

// flushLoop periodically flushes buffered entries.
func (ws *WALShipper) flushLoop() {
	defer ws.wg.Done()

	ticker := time.NewTicker(ws.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ws.stopCh:
			// Final flush before exit.
			ws.Flush()
			return
		case <-ticker.C:
			if err := ws.Flush(); err != nil {
				log.Printf("wal_shipper: periodic flush error: %v", err)
			}
		}
	}
}

// Stop gracefully shuts down the WAL shipper, flushing any remaining entries.
func (ws *WALShipper) Stop() {
	ws.mu.Lock()
	if ws.stopped {
		ws.mu.Unlock()
		return
	}
	ws.stopped = true
	ws.mu.Unlock()

	close(ws.stopCh)
	close(ws.outboxDrainStop)
	ws.wg.Wait()

	if ws.outboxDB != nil {
		ws.outboxDB.Close()
	}
}

// PendingCount returns the total number of buffered (unshipped) entries.
func (ws *WALShipper) PendingCount() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	total := 0
	for _, entries := range ws.buffers {
		total += len(entries)
	}
	return total
}

// --- Outbox Methods ---

// initOutboxSchema creates the outbox table if it does not exist.
func (ws *WALShipper) initOutboxSchema(db *sql.DB) error {
	schema := `
		CREATE TABLE IF NOT EXISTS wal_outbox (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			tbl        TEXT    NOT NULL,
			shard_id   INTEGER NOT NULL,
			payload    BLOB   NOT NULL,
			created_at TEXT    NOT NULL,
			published  INTEGER NOT NULL DEFAULT 0
		);
	`
	_, err := db.Exec(schema)
	return err
}

// shipWithOutbox persists WAL entries to the outbox (crash-safe), then
// attempts to publish them. On publish success the rows are marked as
// published. On publish failure nil is returned because the entries are
// safely in the outbox and will be retried by drainOutbox.
func (ws *WALShipper) shipWithOutbox(key shardKey, entries []WALEntry) error {
	if len(entries) == 0 {
		return nil
	}

	batch := WALBatch{
		Entries: entries,
		Table:   key.table,
		ShardID: key.shardID,
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal WAL batch for outbox: %w", err)
	}

	// Step 1: Persist to the outbox (crash-safe).
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := ws.outboxDB.Exec(
		`INSERT INTO wal_outbox (tbl, shard_id, payload, created_at, published) VALUES (?, ?, ?, ?, 0)`,
		key.table, key.shardID, payload, now,
	)
	if err != nil {
		return fmt.Errorf("insert into outbox: %w", err)
	}
	outboxID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get outbox insert id: %w", err)
	}

	// Step 2: Attempt publish.
	topic := WALTopic(ws.config.Instance, key.table, key.shardID)
	_, pubErr := ws.client.Publish(swarm.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})

	if pubErr != nil {
		// Publish failed — entries are safe in the outbox; drain will retry.
		log.Printf("wal_shipper: publish failed for outbox id %d (%s:%d), will retry: %v",
			outboxID, key.table, key.shardID, pubErr)
		return nil
	}

	// Step 3: Mark as published on success.
	if _, err := ws.outboxDB.Exec(`UPDATE wal_outbox SET published = 1 WHERE id = ?`, outboxID); err != nil {
		log.Printf("wal_shipper: failed to mark outbox id %d as published: %v", outboxID, err)
	}

	log.Printf("wal_shipper: shipped %d entries to %s via outbox (LSN %d-%d)",
		len(entries), topic, entries[0].LSN, entries[len(entries)-1].LSN)

	return nil
}

// drainOutbox runs periodically (every 5 seconds) to retry unpublished outbox
// entries and purge old published entries.
func (ws *WALShipper) drainOutbox() {
	defer ws.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ws.outboxDrainStop:
			return
		case <-ws.stopCh:
			return
		case <-ticker.C:
			ws.drainOutboxOnce()
		}
	}
}

// drainOutboxOnce performs a single drain pass: retries unpublished entries
// and purges old published entries.
func (ws *WALShipper) drainOutboxOnce() {
	if ws.outboxDB == nil {
		return
	}

	// Query unpublished entries, oldest first.
	rows, err := ws.outboxDB.Query(
		`SELECT id, tbl, shard_id, payload FROM wal_outbox WHERE published = 0 ORDER BY id ASC LIMIT 100`,
	)
	if err != nil {
		log.Printf("wal_shipper: drain query error: %v", err)
		return
	}
	defer rows.Close()

	var publishedIDs []int64
	for rows.Next() {
		var id int64
		var tbl string
		var shardID int
		var payload []byte

		if err := rows.Scan(&id, &tbl, &shardID, &payload); err != nil {
			log.Printf("wal_shipper: drain scan error: %v", err)
			continue
		}

		topic := WALTopic(ws.config.Instance, tbl, shardID)
		_, pubErr := ws.client.Publish(swarm.PublishRequest{
			Topic:   topic,
			Payload: payload,
		})
		if pubErr != nil {
			log.Printf("wal_shipper: drain retry failed for outbox id %d (%s:%d): %v",
				id, tbl, shardID, pubErr)
			continue
		}

		publishedIDs = append(publishedIDs, id)
		log.Printf("wal_shipper: drain published outbox id %d (%s:%d)", id, tbl, shardID)
	}
	if err := rows.Err(); err != nil {
		log.Printf("wal_shipper: drain rows error: %v", err)
	}

	// Mark successfully published entries.
	for _, id := range publishedIDs {
		if _, err := ws.outboxDB.Exec(`UPDATE wal_outbox SET published = 1 WHERE id = ?`, id); err != nil {
			log.Printf("wal_shipper: drain mark published error for id %d: %v", id, err)
		}
	}

	// Purge old published entries (older than 1 hour).
	cutoff := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339Nano)
	if _, err := ws.outboxDB.Exec(
		`DELETE FROM wal_outbox WHERE published = 1 AND created_at < ?`, cutoff,
	); err != nil {
		log.Printf("wal_shipper: drain purge error: %v", err)
	}
}

// OutboxUnpublishedCount returns the number of unpublished entries in the
// outbox. Returns 0 if the outbox is not enabled.
func (ws *WALShipper) OutboxUnpublishedCount() int {
	if ws.outboxDB == nil {
		return 0
	}
	var count int
	err := ws.outboxDB.QueryRow(`SELECT COUNT(*) FROM wal_outbox WHERE published = 0`).Scan(&count)
	if err != nil {
		return 0
	}
	return count
}
