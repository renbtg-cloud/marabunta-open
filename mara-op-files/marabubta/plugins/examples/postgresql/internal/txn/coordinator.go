// Marabunta - Licensed under the MIT License.
package txn

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"
)

// Phase constants for the 2PC state machine.
const (
	PhasePreparing  = "preparing"
	PhasePrepared   = "prepared"
	PhaseCommitting = "committing"
	PhaseCommitted  = "committed"
	PhaseAborting   = "aborting"
	PhaseAborted    = "aborted"
)

// Decision constants.
const (
	DecisionCommit = "commit"
	DecisionAbort  = "abort"
)

// Default timeouts.
const (
	DefaultPrepareTimeout = 10 * time.Second
	DefaultCommitTimeout  = 30 * time.Second
)

// ShardTarget describes a single shard operation within a distributed transaction.
type ShardTarget struct {
	Table   string        `json:"table"`
	ShardID int           `json:"shard_id"`
	SQL     string        `json:"sql"`
	Args    []interface{} `json:"args,omitempty"`
}

// PrepareResult reports whether a shard successfully prepared.
type PrepareResult struct {
	ShardID int    `json:"shard_id"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

// TxnState tracks the full lifecycle of a distributed transaction.
type TxnState struct {
	ID           string
	Participants []ShardTarget
	Phase        string
	Decision     string
	CreatedAt    time.Time
}

// ParticipantAdapter is the interface for 2PC participants (local or remote).
type ParticipantAdapter interface {
	Prepare(ctx context.Context, txnID, table string, shardID int, sql string, args []interface{}) error
	Commit(ctx context.Context, txnID string) error
	Abort(ctx context.Context, txnID string) error
}

// TxnCoordinator manages distributed 2-Phase Commit transactions.
// It orchestrates the prepare and commit/abort phases across multiple
// shard participants, using a write-ahead log to ensure crash safety.
type TxnCoordinator struct {
	participant *TxnParticipant
	txnLog      *TxnLog

	remoteParticipant ParticipantAdapter // for cross-node 2PC operations

	mu     sync.Mutex
	active map[string]*TxnState

	prepareTimeout time.Duration
	commitTimeout  time.Duration
}

// NewTxnCoordinator creates a new 2PC coordinator.
func NewTxnCoordinator(participant *TxnParticipant, txnLog *TxnLog) *TxnCoordinator {
	return &TxnCoordinator{
		participant:    participant,
		txnLog:         txnLog,
		active:         make(map[string]*TxnState),
		prepareTimeout: DefaultPrepareTimeout,
		commitTimeout:  DefaultCommitTimeout,
	}
}

// SetRemoteParticipant sets the remote participant for cross-node 2PC.
func (tc *TxnCoordinator) SetRemoteParticipant(rp ParticipantAdapter) {
	tc.remoteParticipant = rp
}

// BeginDistributed starts a new distributed transaction and returns its ID.
func (tc *TxnCoordinator) BeginDistributed(ctx context.Context) (string, error) {
	txnID := generateTxnID()

	tc.mu.Lock()
	tc.active[txnID] = &TxnState{
		ID:        txnID,
		Phase:     PhasePreparing,
		CreatedAt: time.Now(),
	}
	tc.mu.Unlock()

	return txnID, nil
}

// Prepare sends prepare requests to all participant shards. Each shard executes
// its SQL within a savepoint. Returns a map of shard ID to PrepareResult.
func (tc *TxnCoordinator) Prepare(ctx context.Context, txnID string, shards []ShardTarget) (map[int]PrepareResult, error) {
	tc.mu.Lock()
	state, ok := tc.active[txnID]
	if !ok {
		tc.mu.Unlock()
		return nil, fmt.Errorf("unknown transaction: %s", txnID)
	}
	state.Participants = shards
	state.Phase = PhasePreparing
	tc.mu.Unlock()

	// Write-ahead: log the prepare phase with participants.
	if err := tc.txnLog.LogPrepare(txnID, shards); err != nil {
		return nil, fmt.Errorf("log prepare: %w", err)
	}

	// Apply prepare timeout.
	prepareCtx, cancel := context.WithTimeout(ctx, tc.prepareTimeout)
	defer cancel()

	// Fan out prepare requests to all shards concurrently.
	results := make(map[int]PrepareResult)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, shard := range shards {
		wg.Add(1)
		go func(s ShardTarget) {
			defer wg.Done()

			err := tc.participant.Prepare(prepareCtx, txnID, s.Table, s.ShardID, s.SQL, s.Args)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				results[s.ShardID] = PrepareResult{
					ShardID: s.ShardID,
					OK:      false,
					Error:   err.Error(),
				}
			} else {
				results[s.ShardID] = PrepareResult{
					ShardID: s.ShardID,
					OK:      true,
				}
			}
		}(shard)
	}

	// Wait for all prepare requests to complete.
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// All shards responded.
	case <-prepareCtx.Done():
		// Timeout: mark any missing shards as failed.
		mu.Lock()
		for _, shard := range shards {
			if _, exists := results[shard.ShardID]; !exists {
				results[shard.ShardID] = PrepareResult{
					ShardID: shard.ShardID,
					OK:      false,
					Error:   "prepare timeout",
				}
			}
		}
		mu.Unlock()
	}

	// Update state phase.
	tc.mu.Lock()
	if s, ok := tc.active[txnID]; ok {
		s.Phase = PhasePrepared
	}
	tc.mu.Unlock()

	if err := tc.txnLog.LogPrepared(txnID); err != nil {
		log.Printf("txn coordinator: log prepared failed for %s: %v", txnID, err)
	}

	return results, nil
}

// Commit sends commit to all prepared shards. Must only be called after
// all shards returned OK from Prepare.
func (tc *TxnCoordinator) Commit(ctx context.Context, txnID string) error {
	tc.mu.Lock()
	state, ok := tc.active[txnID]
	if !ok {
		tc.mu.Unlock()
		return fmt.Errorf("unknown transaction: %s", txnID)
	}
	state.Phase = PhaseCommitting
	state.Decision = DecisionCommit
	participants := state.Participants
	tc.mu.Unlock()

	// Write-ahead: log the commit decision BEFORE sending to participants.
	if err := tc.txnLog.LogDecision(txnID, DecisionCommit); err != nil {
		return fmt.Errorf("log commit decision: %w", err)
	}

	// Apply commit timeout.
	commitCtx, cancel := context.WithTimeout(ctx, tc.commitTimeout)
	defer cancel()

	// Send commit to all participants.
	var commitErrors []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, shard := range participants {
		wg.Add(1)
		go func(s ShardTarget) {
			defer wg.Done()
			if err := tc.participant.Commit(commitCtx, txnID); err != nil {
				mu.Lock()
				commitErrors = append(commitErrors, fmt.Sprintf("shard %d: %v", s.ShardID, err))
				mu.Unlock()
				log.Printf("txn coordinator: commit failed on shard %d for %s: %v", s.ShardID, txnID, err)
			}
		}(shard)
	}

	wg.Wait()

	// Mark as complete.
	tc.mu.Lock()
	if s, ok := tc.active[txnID]; ok {
		s.Phase = PhaseCommitted
	}
	delete(tc.active, txnID)
	tc.mu.Unlock()

	if err := tc.txnLog.LogComplete(txnID); err != nil {
		log.Printf("txn coordinator: log complete failed for %s: %v", txnID, err)
	}

	if len(commitErrors) > 0 {
		return fmt.Errorf("commit partially failed: %v", commitErrors)
	}
	return nil
}

// Abort sends abort to all shards. Can be called at any point to roll back.
func (tc *TxnCoordinator) Abort(ctx context.Context, txnID string) error {
	tc.mu.Lock()
	state, ok := tc.active[txnID]
	if !ok {
		tc.mu.Unlock()
		// Transaction might have already been cleaned up -- idempotent.
		return nil
	}
	state.Phase = PhaseAborting
	state.Decision = DecisionAbort
	participants := state.Participants
	tc.mu.Unlock()

	// Write-ahead: log the abort decision BEFORE sending to participants.
	if err := tc.txnLog.LogDecision(txnID, DecisionAbort); err != nil {
		log.Printf("txn coordinator: log abort decision failed for %s: %v", txnID, err)
	}

	// Send abort to all participants.
	var wg sync.WaitGroup
	for _, shard := range participants {
		wg.Add(1)
		go func(s ShardTarget) {
			defer wg.Done()
			abortCtx, cancel := context.WithTimeout(ctx, tc.commitTimeout)
			defer cancel()
			if err := tc.participant.Abort(abortCtx, txnID); err != nil {
				log.Printf("txn coordinator: abort failed on shard %d for %s: %v", s.ShardID, txnID, err)
			}
		}(shard)
	}
	wg.Wait()

	// Mark as complete.
	tc.mu.Lock()
	if s, ok := tc.active[txnID]; ok {
		s.Phase = PhaseAborted
	}
	delete(tc.active, txnID)
	tc.mu.Unlock()

	if err := tc.txnLog.LogComplete(txnID); err != nil {
		log.Printf("txn coordinator: log complete (abort) failed for %s: %v", txnID, err)
	}

	return nil
}

// ExecuteDistributed runs a full 2PC cycle: prepare all shards, then commit
// if all succeeded or abort if any failed. This is the main entry point for
// multi-shard writes.
func (tc *TxnCoordinator) ExecuteDistributed(ctx context.Context, shards []ShardTarget) error {
	txnID, err := tc.BeginDistributed(ctx)
	if err != nil {
		return fmt.Errorf("begin distributed txn: %w", err)
	}

	results, err := tc.Prepare(ctx, txnID, shards)
	if err != nil {
		// Prepare failed entirely -- abort.
		abortErr := tc.Abort(ctx, txnID)
		if abortErr != nil {
			log.Printf("txn coordinator: abort after prepare error for %s: %v", txnID, abortErr)
		}
		return fmt.Errorf("prepare distributed txn: %w", err)
	}

	// Check if all shards prepared successfully.
	allOK := true
	for _, r := range results {
		if !r.OK {
			allOK = false
			break
		}
	}

	if allOK {
		// All shards prepared -- commit.
		if err := tc.Commit(ctx, txnID); err != nil {
			return fmt.Errorf("commit distributed txn %s: %w", txnID, err)
		}
		return nil
	}

	// At least one shard failed -- abort all.
	if err := tc.Abort(ctx, txnID); err != nil {
		return fmt.Errorf("abort distributed txn %s: %w", txnID, err)
	}

	// Build error message from failed shards.
	var failedShards []string
	for shardID, r := range results {
		if !r.OK {
			failedShards = append(failedShards, fmt.Sprintf("shard %d: %s", shardID, r.Error))
		}
	}
	return fmt.Errorf("distributed txn aborted: shards failed to prepare: %v", failedShards)
}

// ActiveCount returns the number of in-flight distributed transactions.
func (tc *TxnCoordinator) ActiveCount() int {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return len(tc.active)
}

// generateTxnID generates a UUID v4 transaction ID without external dependencies.
func generateTxnID() string {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		// Fallback to timestamp-based ID if crypto/rand fails.
		return fmt.Sprintf("txn-%d", time.Now().UnixNano())
	}
	// Set version 4 bits.
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Set variant bits.
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}
