// Marabunta - Licensed under the MIT License.
package txn

import (
	"context"
	"fmt"
	"log"
	"time"
)

// TxnRecovery handles crash recovery for in-flight distributed transactions.
// On startup, it scans the persistent transaction log and resolves any
// transactions that were interrupted:
//
//   - preparing / no decision -> ABORT (coordinator crashed before deciding)
//   - prepared / decision=commit -> re-send COMMIT to all participants
//   - prepared / decision=abort -> re-send ABORT to all participants
//   - committing -> re-send COMMIT (may have partially committed)
//   - aborting -> re-send ABORT (may have partially aborted)
type TxnRecovery struct {
	txnLog      *TxnLog
	participant *TxnParticipant
	timeout     time.Duration
}

// NewTxnRecovery creates a new recovery handler.
func NewTxnRecovery(txnLog *TxnLog, participant *TxnParticipant) *TxnRecovery {
	return &TxnRecovery{
		txnLog:      txnLog,
		participant: participant,
		timeout:     30 * time.Second,
	}
}

// RecoveryResult summarizes the outcome of crash recovery.
type RecoveryResult struct {
	Committed int
	Aborted   int
	Errors    []string
}

// Recover scans the transaction log for pending transactions and resolves them.
// This should be called once on startup, before accepting new transactions.
func (r *TxnRecovery) Recover(ctx context.Context) (*RecoveryResult, error) {
	pending, err := r.txnLog.GetPending()
	if err != nil {
		return nil, fmt.Errorf("get pending txns for recovery: %w", err)
	}

	if len(pending) == 0 {
		log.Println("txn recovery: no pending transactions to recover")
		return &RecoveryResult{}, nil
	}

	log.Printf("txn recovery: found %d pending transactions to recover", len(pending))

	result := &RecoveryResult{}

	for _, rec := range pending {
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("recovery cancelled: %w", ctx.Err())
		default:
		}

		recErr := r.recoverOne(ctx, &rec, result)
		if recErr != nil {
			errMsg := fmt.Sprintf("txn %s: %v", rec.TxnID, recErr)
			result.Errors = append(result.Errors, errMsg)
			log.Printf("txn recovery: %s", errMsg)
		}
	}

	log.Printf("txn recovery: complete — committed=%d, aborted=%d, errors=%d",
		result.Committed, result.Aborted, len(result.Errors))

	return result, nil
}

// recoverOne resolves a single pending transaction based on its phase and decision.
func (r *TxnRecovery) recoverOne(ctx context.Context, rec *TxnRecord, result *RecoveryResult) error {
	recoverCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	switch rec.Phase {
	case PhasePreparing:
		// Coordinator crashed before making a decision. Abort.
		log.Printf("txn recovery: aborting txn %s (crashed during prepare, no decision)", rec.TxnID)
		if err := r.abortParticipants(recoverCtx, rec); err != nil {
			return fmt.Errorf("abort preparing txn: %w", err)
		}
		if err := r.txnLog.LogDecision(rec.TxnID, DecisionAbort); err != nil {
			return fmt.Errorf("log abort decision during recovery: %w", err)
		}
		if err := r.txnLog.LogComplete(rec.TxnID); err != nil {
			return fmt.Errorf("log complete during recovery: %w", err)
		}
		result.Aborted++

	case PhasePrepared:
		// All participants prepared, but decision may not have been acted on.
		switch rec.Decision {
		case DecisionCommit:
			log.Printf("txn recovery: re-committing txn %s (prepared, decision=commit)", rec.TxnID)
			if err := r.commitParticipants(recoverCtx, rec); err != nil {
				return fmt.Errorf("re-commit prepared txn: %w", err)
			}
			if err := r.txnLog.LogComplete(rec.TxnID); err != nil {
				return fmt.Errorf("log complete after re-commit: %w", err)
			}
			result.Committed++

		case DecisionAbort:
			log.Printf("txn recovery: re-aborting txn %s (prepared, decision=abort)", rec.TxnID)
			if err := r.abortParticipants(recoverCtx, rec); err != nil {
				return fmt.Errorf("re-abort prepared txn: %w", err)
			}
			if err := r.txnLog.LogComplete(rec.TxnID); err != nil {
				return fmt.Errorf("log complete after re-abort: %w", err)
			}
			result.Aborted++

		default:
			// No decision recorded for a prepared txn -- abort (presumed abort).
			log.Printf("txn recovery: aborting txn %s (prepared, no decision recorded)", rec.TxnID)
			if err := r.abortParticipants(recoverCtx, rec); err != nil {
				return fmt.Errorf("abort no-decision txn: %w", err)
			}
			if err := r.txnLog.LogDecision(rec.TxnID, DecisionAbort); err != nil {
				return fmt.Errorf("log abort for no-decision: %w", err)
			}
			if err := r.txnLog.LogComplete(rec.TxnID); err != nil {
				return fmt.Errorf("log complete for no-decision: %w", err)
			}
			result.Aborted++
		}

	case PhaseCommitting:
		// Coordinator crashed during commit. Re-send commit to all.
		log.Printf("txn recovery: re-committing txn %s (crashed during commit phase)", rec.TxnID)
		if err := r.commitParticipants(recoverCtx, rec); err != nil {
			return fmt.Errorf("re-commit committing txn: %w", err)
		}
		if err := r.txnLog.LogComplete(rec.TxnID); err != nil {
			return fmt.Errorf("log complete after re-commit: %w", err)
		}
		result.Committed++

	case PhaseAborting:
		// Coordinator crashed during abort. Re-send abort to all.
		log.Printf("txn recovery: re-aborting txn %s (crashed during abort phase)", rec.TxnID)
		if err := r.abortParticipants(recoverCtx, rec); err != nil {
			return fmt.Errorf("re-abort aborting txn: %w", err)
		}
		if err := r.txnLog.LogComplete(rec.TxnID); err != nil {
			return fmt.Errorf("log complete after re-abort: %w", err)
		}
		result.Aborted++

	default:
		log.Printf("txn recovery: unknown phase %q for txn %s, skipping", rec.Phase, rec.TxnID)
	}

	return nil
}

// commitParticipants sends commit to all participants of a transaction.
// Errors are logged but not fatal -- commit is idempotent.
func (r *TxnRecovery) commitParticipants(ctx context.Context, rec *TxnRecord) error {
	var lastErr error
	for _, p := range rec.Participants {
		if err := r.participant.Commit(ctx, rec.TxnID); err != nil {
			log.Printf("txn recovery: commit failed for shard %d in txn %s: %v", p.ShardID, rec.TxnID, err)
			lastErr = err
		}
	}
	return lastErr
}

// abortParticipants sends abort to all participants of a transaction.
// Errors are logged but not fatal -- abort is idempotent.
func (r *TxnRecovery) abortParticipants(ctx context.Context, rec *TxnRecord) error {
	var lastErr error
	for _, p := range rec.Participants {
		if err := r.participant.Abort(ctx, rec.TxnID); err != nil {
			log.Printf("txn recovery: abort failed for shard %d in txn %s: %v", p.ShardID, rec.TxnID, err)
			lastErr = err
		}
	}
	return lastErr
}
