// Marabunta - Licensed under the MIT License.
package txn

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/marabunta/marabunta-postgres/internal/storage"
)

// testSetup creates a temporary directory with a ShardManager and returns
// a cleanup function. Each test gets its own isolated environment.
func testSetup(t *testing.T, shardCount int) (string, *storage.ShardManager, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "txn_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	sm, err := storage.NewShardManager(dir, shardCount)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("create shard manager: %v", err)
	}

	cleanup := func() {
		sm.Close()
		os.RemoveAll(dir)
	}
	return dir, sm, cleanup
}

// createTestTable creates a simple test table on all shards.
func createTestTable(t *testing.T, sm *storage.ShardManager, table string, shardCount int) {
	t.Helper()
	ctx := context.Background()
	ddl := "CREATE TABLE IF NOT EXISTS " + table + " (id TEXT PRIMARY KEY, value TEXT)"
	for i := 0; i < shardCount; i++ {
		if err := sm.Exec(ctx, table, i, ddl); err != nil {
			t.Fatalf("create table on shard %d: %v", i, err)
		}
	}
}

func TestCoordinator_HappyPath(t *testing.T) {
	dir, sm, cleanup := testSetup(t, 4)
	defer cleanup()

	createTestTable(t, sm, "users", 4)

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	txnLog, err := NewTxnLog(filepath.Join(dir, "txn_log.db"))
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}
	defer txnLog.Close()

	coord := NewTxnCoordinator(participant, txnLog)
	ctx := context.Background()

	// Start a distributed transaction.
	txnID, err := coord.BeginDistributed(ctx)
	if err != nil {
		t.Fatalf("begin distributed: %v", err)
	}
	if txnID == "" {
		t.Fatal("expected non-empty txn ID")
	}

	// Prepare on two shards.
	shards := []ShardTarget{
		{Table: "users", ShardID: 0, SQL: "INSERT INTO users (id, value) VALUES ('a1', 'alice')"},
		{Table: "users", ShardID: 1, SQL: "INSERT INTO users (id, value) VALUES ('b1', 'bob')"},
	}

	results, err := coord.Prepare(ctx, txnID, shards)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// All shards should be OK.
	for shardID, r := range results {
		if !r.OK {
			t.Errorf("shard %d prepare failed: %s", shardID, r.Error)
		}
	}

	// Commit.
	if err := coord.Commit(ctx, txnID); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Verify data was committed by querying the shards.
	cols, rows, err := sm.Query(ctx, "users", 0, "SELECT id, value FROM users WHERE id = 'a1'")
	if err != nil {
		t.Fatalf("query shard 0: %v", err)
	}
	if len(cols) == 0 || len(rows) == 0 {
		t.Error("expected data on shard 0 after commit")
	}

	cols, rows, err = sm.Query(ctx, "users", 1, "SELECT id, value FROM users WHERE id = 'b1'")
	if err != nil {
		t.Fatalf("query shard 1: %v", err)
	}
	if len(cols) == 0 || len(rows) == 0 {
		t.Error("expected data on shard 1 after commit")
	}
}

func TestCoordinator_OneShardFails(t *testing.T) {
	dir, sm, cleanup := testSetup(t, 4)
	defer cleanup()

	createTestTable(t, sm, "users", 4)

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	txnLog, err := NewTxnLog(filepath.Join(dir, "txn_log.db"))
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}
	defer txnLog.Close()

	coord := NewTxnCoordinator(participant, txnLog)
	ctx := context.Background()

	// Use ExecuteDistributed which handles the full 2PC cycle.
	// One shard has invalid SQL that will fail.
	shards := []ShardTarget{
		{Table: "users", ShardID: 0, SQL: "INSERT INTO users (id, value) VALUES ('c1', 'charlie')"},
		{Table: "users", ShardID: 1, SQL: "INSERT INTO nonexistent_table (id) VALUES ('fail')"},
	}

	err = coord.ExecuteDistributed(ctx, shards)
	if err == nil {
		t.Fatal("expected error when one shard fails")
	}

	// Verify shard 0 was rolled back (data should NOT be present).
	_, rows, qErr := sm.Query(ctx, "users", 0, "SELECT id FROM users WHERE id = 'c1'")
	if qErr != nil {
		t.Fatalf("query shard 0: %v", qErr)
	}
	if len(rows) > 0 {
		t.Error("expected no data on shard 0 after abort -- atomicity violated")
	}
}

func TestCoordinator_Timeout(t *testing.T) {
	dir, sm, cleanup := testSetup(t, 4)
	defer cleanup()

	createTestTable(t, sm, "users", 4)

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	txnLog, err := NewTxnLog(filepath.Join(dir, "txn_log.db"))
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}
	defer txnLog.Close()

	coord := NewTxnCoordinator(participant, txnLog)
	// Use an already-cancelled context to simulate timeout.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	shards := []ShardTarget{
		{Table: "users", ShardID: 0, SQL: "INSERT INTO users (id, value) VALUES ('t1', 'timeout')"},
	}

	err = coord.ExecuteDistributed(ctx, shards)
	if err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

func TestParticipant_PrepareAndCommit(t *testing.T) {
	_, sm, cleanup := testSetup(t, 2)
	defer cleanup()

	createTestTable(t, sm, "items", 2)

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	ctx := context.Background()
	txnID := "test-prepare-commit-001"

	// Prepare.
	err := participant.Prepare(ctx, txnID, "items", 0, "INSERT INTO items (id, value) VALUES ('p1', 'prepared')", nil)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if participant.PendingCount() != 1 {
		t.Errorf("expected 1 pending txn, got %d", participant.PendingCount())
	}

	// Commit.
	err = participant.Commit(ctx, txnID)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	if participant.PendingCount() != 0 {
		t.Errorf("expected 0 pending txns after commit, got %d", participant.PendingCount())
	}

	// Verify data is visible.
	_, rows, err := sm.Query(ctx, "items", 0, "SELECT id FROM items WHERE id = 'p1'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) == 0 {
		t.Error("expected data after commit")
	}
}

func TestParticipant_PrepareAndAbort(t *testing.T) {
	_, sm, cleanup := testSetup(t, 2)
	defer cleanup()

	createTestTable(t, sm, "items", 2)

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	ctx := context.Background()
	txnID := "test-prepare-abort-001"

	// Prepare.
	err := participant.Prepare(ctx, txnID, "items", 0, "INSERT INTO items (id, value) VALUES ('a1', 'aborted')", nil)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Abort.
	err = participant.Abort(ctx, txnID)
	if err != nil {
		t.Fatalf("abort: %v", err)
	}

	if participant.PendingCount() != 0 {
		t.Errorf("expected 0 pending txns after abort, got %d", participant.PendingCount())
	}

	// Verify data is NOT visible after abort.
	_, rows, err := sm.Query(ctx, "items", 0, "SELECT id FROM items WHERE id = 'a1'")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) > 0 {
		t.Error("expected no data after abort")
	}
}

func TestParticipant_StaleAutoAbort(t *testing.T) {
	_, sm, cleanup := testSetup(t, 2)
	defer cleanup()

	createTestTable(t, sm, "items", 2)

	// Use a very short stale timeout for testing.
	participant := NewTxnParticipant(sm, 100*time.Millisecond)
	defer participant.Stop()

	ctx := context.Background()
	txnID := "test-stale-001"

	// Prepare but never commit/abort.
	err := participant.Prepare(ctx, txnID, "items", 0, "INSERT INTO items (id, value) VALUES ('s1', 'stale')", nil)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if participant.PendingCount() != 1 {
		t.Errorf("expected 1 pending, got %d", participant.PendingCount())
	}

	// Manually trigger cleanup instead of waiting for the ticker.
	time.Sleep(200 * time.Millisecond)
	participant.cleanupStale()

	if participant.PendingCount() != 0 {
		t.Errorf("expected 0 pending after stale cleanup, got %d", participant.PendingCount())
	}
}

func TestTxnLog_PersistAndRecover(t *testing.T) {
	dir, err := os.MkdirTemp("", "txnlog_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logPath := filepath.Join(dir, "txn_log.db")

	// Create log and write some records.
	txnLog, err := NewTxnLog(logPath)
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}

	participants := []ShardTarget{
		{Table: "orders", ShardID: 0, SQL: "INSERT INTO orders VALUES ('1')"},
		{Table: "orders", ShardID: 1, SQL: "INSERT INTO orders VALUES ('2')"},
	}

	// Log a preparing transaction.
	if err := txnLog.LogPrepare("txn-001", participants); err != nil {
		t.Fatalf("log prepare: %v", err)
	}

	// Log a committed transaction.
	if err := txnLog.LogPrepare("txn-002", participants); err != nil {
		t.Fatalf("log prepare txn-002: %v", err)
	}
	if err := txnLog.LogPrepared("txn-002"); err != nil {
		t.Fatalf("log prepared txn-002: %v", err)
	}
	if err := txnLog.LogDecision("txn-002", DecisionCommit); err != nil {
		t.Fatalf("log decision txn-002: %v", err)
	}
	if err := txnLog.LogComplete("txn-002"); err != nil {
		t.Fatalf("log complete txn-002: %v", err)
	}

	// Close and reopen to test persistence.
	txnLog.Close()

	txnLog2, err := NewTxnLog(logPath)
	if err != nil {
		t.Fatalf("reopen txn log: %v", err)
	}
	defer txnLog2.Close()

	pending, err := txnLog2.GetPending()
	if err != nil {
		t.Fatalf("get pending: %v", err)
	}

	// Only txn-001 should be pending (txn-002 is complete).
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending txn, got %d", len(pending))
	}
	if pending[0].TxnID != "txn-001" {
		t.Errorf("expected txn-001, got %s", pending[0].TxnID)
	}
	if pending[0].Phase != PhasePreparing {
		t.Errorf("expected phase %s, got %s", PhasePreparing, pending[0].Phase)
	}
	if len(pending[0].Participants) != 2 {
		t.Errorf("expected 2 participants, got %d", len(pending[0].Participants))
	}
}

func TestRecovery_CrashAfterPrepare(t *testing.T) {
	dir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	sm, err := storage.NewShardManager(filepath.Join(dir, "shards"), 4)
	if err != nil {
		t.Fatalf("create shard manager: %v", err)
	}
	defer sm.Close()

	logPath := filepath.Join(dir, "txn_log.db")
	txnLog, err := NewTxnLog(logPath)
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}

	// Simulate a coordinator that crashed during prepare (no decision logged).
	participants := []ShardTarget{
		{Table: "orders", ShardID: 0, SQL: "INSERT INTO orders VALUES ('crash1')"},
		{Table: "orders", ShardID: 1, SQL: "INSERT INTO orders VALUES ('crash2')"},
	}
	if err := txnLog.LogPrepare("crash-txn-001", participants); err != nil {
		t.Fatalf("log prepare: %v", err)
	}
	txnLog.Close()

	// Reopen log and run recovery.
	txnLog2, err := NewTxnLog(logPath)
	if err != nil {
		t.Fatalf("reopen txn log: %v", err)
	}
	defer txnLog2.Close()

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	recovery := NewTxnRecovery(txnLog2, participant)
	ctx := context.Background()

	result, err := recovery.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Preparing with no decision should be aborted.
	if result.Aborted != 1 {
		t.Errorf("expected 1 aborted, got %d", result.Aborted)
	}
	if result.Committed != 0 {
		t.Errorf("expected 0 committed, got %d", result.Committed)
	}

	// Verify the transaction is now complete in the log.
	pending, err := txnLog2.GetPending()
	if err != nil {
		t.Fatalf("get pending after recovery: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after recovery, got %d", len(pending))
	}
}

func TestRecovery_CrashAfterDecisionCommit(t *testing.T) {
	dir, err := os.MkdirTemp("", "recovery_commit_test_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	sm, err := storage.NewShardManager(filepath.Join(dir, "shards"), 4)
	if err != nil {
		t.Fatalf("create shard manager: %v", err)
	}
	defer sm.Close()

	logPath := filepath.Join(dir, "txn_log.db")
	txnLog, err := NewTxnLog(logPath)
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}

	// Simulate a coordinator that logged commit decision but crashed before completing.
	participants := []ShardTarget{
		{Table: "orders", ShardID: 0, SQL: "INSERT INTO orders VALUES ('commit1')"},
	}
	if err := txnLog.LogPrepare("commit-txn-001", participants); err != nil {
		t.Fatalf("log prepare: %v", err)
	}
	if err := txnLog.LogPrepared("commit-txn-001"); err != nil {
		t.Fatalf("log prepared: %v", err)
	}
	if err := txnLog.LogDecision("commit-txn-001", DecisionCommit); err != nil {
		t.Fatalf("log decision: %v", err)
	}
	txnLog.Close()

	// Reopen log and run recovery.
	txnLog2, err := NewTxnLog(logPath)
	if err != nil {
		t.Fatalf("reopen txn log: %v", err)
	}
	defer txnLog2.Close()

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	recovery := NewTxnRecovery(txnLog2, participant)
	ctx := context.Background()

	result, err := recovery.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	// The committing transaction should be re-committed.
	if result.Committed != 1 {
		t.Errorf("expected 1 committed, got %d", result.Committed)
	}
	if result.Aborted != 0 {
		t.Errorf("expected 0 aborted, got %d", result.Aborted)
	}

	// Verify the transaction is now complete in the log.
	pending, err := txnLog2.GetPending()
	if err != nil {
		t.Fatalf("get pending after recovery: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after recovery, got %d", len(pending))
	}
}

func TestCoordinator_ConcurrentTxns(t *testing.T) {
	dir, sm, cleanup := testSetup(t, 8)
	defer cleanup()

	createTestTable(t, sm, "concurrent", 8)

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	txnLog, err := NewTxnLog(filepath.Join(dir, "txn_log.db"))
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}
	defer txnLog.Close()

	coord := NewTxnCoordinator(participant, txnLog)
	ctx := context.Background()

	// Run 10 concurrent distributed transactions, each targeting different shards.
	const numTxns = 10
	var wg sync.WaitGroup
	errors := make([]error, numTxns)

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			shard1 := idx % 8
			shard2 := (idx + 1) % 8
			if shard1 == shard2 {
				shard2 = (shard2 + 1) % 8
			}

			shards := []ShardTarget{
				{
					Table:   "concurrent",
					ShardID: shard1,
					SQL:     "INSERT INTO concurrent (id, value) VALUES ('" + concurrentID(idx, 1) + "', 'val')",
				},
				{
					Table:   "concurrent",
					ShardID: shard2,
					SQL:     "INSERT INTO concurrent (id, value) VALUES ('" + concurrentID(idx, 2) + "', 'val')",
				},
			}

			errors[idx] = coord.ExecuteDistributed(ctx, shards)
		}(i)
	}

	wg.Wait()

	// All transactions should succeed.
	for i, err := range errors {
		if err != nil {
			t.Errorf("txn %d failed: %v", i, err)
		}
	}

	// Verify no active transactions remain.
	if coord.ActiveCount() != 0 {
		t.Errorf("expected 0 active txns, got %d", coord.ActiveCount())
	}
}

func TestTxnLog_Cleanup(t *testing.T) {
	dir, err := os.MkdirTemp("", "txnlog_cleanup_*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	txnLog, err := NewTxnLog(filepath.Join(dir, "txn_log.db"))
	if err != nil {
		t.Fatalf("create txn log: %v", err)
	}
	defer txnLog.Close()

	participants := []ShardTarget{
		{Table: "t", ShardID: 0, SQL: "SELECT 1"},
	}

	// Create a completed transaction.
	if err := txnLog.LogPrepare("old-txn", participants); err != nil {
		t.Fatalf("log prepare: %v", err)
	}
	if err := txnLog.LogDecision("old-txn", DecisionCommit); err != nil {
		t.Fatalf("log decision: %v", err)
	}
	if err := txnLog.LogComplete("old-txn"); err != nil {
		t.Fatalf("log complete: %v", err)
	}

	// Cleanup with a zero duration should purge everything completed.
	deleted, err := txnLog.Cleanup(0)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}
}

func TestParticipant_CommitIdempotent(t *testing.T) {
	_, sm, cleanup := testSetup(t, 2)
	defer cleanup()

	createTestTable(t, sm, "items", 2)

	participant := NewTxnParticipant(sm, 60*time.Second)
	defer participant.Stop()

	ctx := context.Background()
	txnID := "idempotent-001"

	// Commit without prepare should be a no-op (idempotent).
	err := participant.Commit(ctx, txnID)
	if err != nil {
		t.Errorf("commit of unknown txn should be idempotent, got: %v", err)
	}

	// Abort without prepare should be a no-op (idempotent).
	err = participant.Abort(ctx, txnID)
	if err != nil {
		t.Errorf("abort of unknown txn should be idempotent, got: %v", err)
	}
}

func TestSanitizeTxnID(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"abc123", "abc123"},
		{"txn-001-abc", "txn_001_abc"},
		{"a.b.c", "a_b_c"},
		{"hello world!", "hello_world_"},
	}

	for _, tt := range tests {
		result := sanitizeTxnID(tt.input)
		if result != tt.expected {
			t.Errorf("sanitizeTxnID(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestGenerateTxnID(t *testing.T) {
	// Generate multiple IDs and verify uniqueness and format.
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := generateTxnID()
		if id == "" {
			t.Fatal("generated empty txn ID")
		}
		if ids[id] {
			t.Fatalf("duplicate txn ID generated: %s", id)
		}
		ids[id] = true
	}
}

// concurrentID generates a unique ID for concurrent test transactions.
func concurrentID(txnIdx, rowIdx int) string {
	return "c_" + itoa(txnIdx) + "_" + itoa(rowIdx)
}

// itoa is a simple integer-to-string converter for test IDs.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
