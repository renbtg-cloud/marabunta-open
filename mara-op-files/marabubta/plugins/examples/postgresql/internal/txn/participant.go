// Marabunta - Licensed under the MIT License.
package txn

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/marabunta/marabunta-postgres/internal/storage"
)

// PreparedTxn represents a transaction that has been prepared on a shard
// but not yet committed or aborted.
type PreparedTxn struct {
	TxnID         string
	SavepointName string
	Table         string
	ShardID       int
	SQL           string
	Args          []interface{}
	PreparedAt    time.Time
}

// TxnParticipant handles the shard-side of 2PC: prepare, commit, and abort.
// Each participant manages prepared transactions for a single shard and
// automatically aborts stale transactions that never received a decision
// (presumed abort protocol).
type TxnParticipant struct {
	shardMgr *storage.ShardManager

	mu       sync.Mutex
	prepared map[string]*PreparedTxn // txnID -> PreparedTxn

	staleTimeout time.Duration // auto-abort after this duration
	stopCh       chan struct{}
	stopped      sync.WaitGroup
}

// NewTxnParticipant creates a new participant for handling shard-side 2PC.
// staleTimeout controls how long a prepared transaction waits for a decision
// before auto-aborting (presumed abort). Pass 0 for the default of 60 seconds.
func NewTxnParticipant(shardMgr *storage.ShardManager, staleTimeout time.Duration) *TxnParticipant {
	if staleTimeout <= 0 {
		staleTimeout = 60 * time.Second
	}
	p := &TxnParticipant{
		shardMgr:     shardMgr,
		prepared:     make(map[string]*PreparedTxn),
		staleTimeout: staleTimeout,
		stopCh:       make(chan struct{}),
	}

	// Start the stale transaction cleanup goroutine.
	p.stopped.Add(1)
	go p.cleanupLoop()

	return p
}

// Stop stops the cleanup goroutine and waits for it to finish.
func (p *TxnParticipant) Stop() {
	close(p.stopCh)
	p.stopped.Wait()
}

// Prepare executes the SQL statement within a savepoint on the target shard.
// If the statement succeeds, the transaction is recorded as prepared and
// awaits a commit or abort decision. Uses SQLite savepoints to allow
// partial rollback within a transaction.
func (p *TxnParticipant) Prepare(ctx context.Context, txnID string, table string, shardID int, sqlStmt string, args []interface{}) error {
	savepointName := fmt.Sprintf("txn_%s", sanitizeTxnID(txnID))

	// Execute the SQL within a savepoint on the target shard.
	// The savepoint acts as the prepare boundary: if the SQL succeeds,
	// we consider this shard "prepared".
	savepointSQL := fmt.Sprintf("SAVEPOINT %s", savepointName)
	if err := p.shardMgr.Exec(ctx, table, shardID, savepointSQL); err != nil {
		return fmt.Errorf("create savepoint on shard %d: %w", shardID, err)
	}

	// Execute the actual SQL statement.
	if err := p.shardMgr.Exec(ctx, table, shardID, sqlStmt); err != nil {
		// Rollback the savepoint on failure.
		rollbackSQL := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName)
		if rbErr := p.shardMgr.Exec(ctx, table, shardID, rollbackSQL); rbErr != nil {
			log.Printf("txn participant: rollback savepoint after prepare failure on shard %d: %v", shardID, rbErr)
		}
		return fmt.Errorf("prepare SQL on shard %d: %w", shardID, err)
	}

	// Record the prepared transaction.
	p.mu.Lock()
	p.prepared[txnID] = &PreparedTxn{
		TxnID:         txnID,
		SavepointName: savepointName,
		Table:         table,
		ShardID:       shardID,
		SQL:           sqlStmt,
		Args:          args,
		PreparedAt:    time.Now(),
	}
	p.mu.Unlock()

	return nil
}

// Commit releases the savepoint for a prepared transaction, making the
// changes permanent on this shard.
func (p *TxnParticipant) Commit(ctx context.Context, txnID string) error {
	p.mu.Lock()
	pt, ok := p.prepared[txnID]
	if !ok {
		p.mu.Unlock()
		// Already committed or aborted -- idempotent.
		return nil
	}
	delete(p.prepared, txnID)
	p.mu.Unlock()

	// Release the savepoint to commit the changes.
	releaseSQL := fmt.Sprintf("RELEASE SAVEPOINT %s", pt.SavepointName)
	if err := p.shardMgr.Exec(ctx, pt.Table, pt.ShardID, releaseSQL); err != nil {
		return fmt.Errorf("commit (release savepoint) on shard %d: %w", pt.ShardID, err)
	}

	return nil
}

// Abort rolls back to the savepoint for a prepared transaction, undoing
// the changes on this shard.
func (p *TxnParticipant) Abort(ctx context.Context, txnID string) error {
	p.mu.Lock()
	pt, ok := p.prepared[txnID]
	if !ok {
		p.mu.Unlock()
		// Already committed or aborted -- idempotent.
		return nil
	}
	delete(p.prepared, txnID)
	p.mu.Unlock()

	// Rollback to the savepoint to undo the changes.
	rollbackSQL := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", pt.SavepointName)
	if err := p.shardMgr.Exec(ctx, pt.Table, pt.ShardID, rollbackSQL); err != nil {
		return fmt.Errorf("abort (rollback savepoint) on shard %d: %w", pt.ShardID, err)
	}

	// Release the savepoint after rollback to clean up.
	releaseSQL := fmt.Sprintf("RELEASE SAVEPOINT %s", pt.SavepointName)
	if err := p.shardMgr.Exec(ctx, pt.Table, pt.ShardID, releaseSQL); err != nil {
		// Non-fatal: the savepoint is already rolled back.
		log.Printf("txn participant: release after rollback on shard %d: %v", pt.ShardID, err)
	}

	return nil
}

// PendingCount returns the number of prepared transactions awaiting a decision.
func (p *TxnParticipant) PendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.prepared)
}

// cleanupLoop periodically scans for stale prepared transactions and
// auto-aborts them (presumed abort protocol). Runs every 10 seconds.
func (p *TxnParticipant) cleanupLoop() {
	defer p.stopped.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.cleanupStale()
		}
	}
}

// cleanupStale aborts all prepared transactions that have exceeded the
// stale timeout. This implements the "presumed abort" protocol: if the
// coordinator never sends a decision, the participant assumes abort.
func (p *TxnParticipant) cleanupStale() {
	p.mu.Lock()
	var stale []*PreparedTxn
	cutoff := time.Now().Add(-p.staleTimeout)
	for _, pt := range p.prepared {
		if pt.PreparedAt.Before(cutoff) {
			stale = append(stale, pt)
		}
	}
	p.mu.Unlock()

	for _, pt := range stale {
		log.Printf("txn participant: auto-aborting stale txn %s (prepared at %s)", pt.TxnID, pt.PreparedAt.Format(time.RFC3339))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := p.Abort(ctx, pt.TxnID); err != nil {
			log.Printf("txn participant: auto-abort failed for %s: %v", pt.TxnID, err)
		}
		cancel()
	}
}

// sanitizeTxnID replaces characters that are not valid in SQLite identifiers
// with underscores. UUIDs contain hyphens which need replacement.
func sanitizeTxnID(txnID string) string {
	result := make([]byte, len(txnID))
	for i := 0; i < len(txnID); i++ {
		c := txnID[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			result[i] = c
		} else {
			result[i] = '_'
		}
	}
	return string(result)
}
