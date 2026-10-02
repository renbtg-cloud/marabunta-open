// Marabunta - Licensed under the MIT License.
// Package txn implements distributed 2-Phase Commit (2PC) transactions
// for the PostgreSQL plugin's multi-shard writes. This file provides
// the persistent transaction log backed by SQLite.
package txn

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TxnRecord represents a single transaction in the log.
type TxnRecord struct {
	TxnID        string        `json:"txn_id"`
	Phase        string        `json:"phase"`
	Participants []ShardTarget `json:"participants"`
	Decision     string        `json:"decision"`
	CreatedAt    time.Time     `json:"created_at"`
	DecidedAt    *time.Time    `json:"decided_at,omitempty"`
}

// TxnLog provides a persistent, write-ahead transaction log backed by SQLite.
// All decisions are written to the log before being acted upon, ensuring
// crash recovery can determine the correct outcome for any in-flight transaction.
type TxnLog struct {
	mu sync.Mutex
	db *sql.DB
}

// NewTxnLog creates a new transaction log at the given path.
// It initializes the database with WAL mode and synchronous=FULL for durability.
func NewTxnLog(path string) (*TxnLog, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=5000", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open txn log db: %w", err)
	}

	db.SetMaxOpenConns(1) // Serialize all writes through one connection.
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping txn log db: %w", err)
	}

	// Ensure WAL mode and synchronous=FULL.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA synchronous=FULL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set synchronous=FULL: %w", err)
	}

	// Create schema if it does not exist.
	schema := `CREATE TABLE IF NOT EXISTS txn_log (
		txn_id       TEXT PRIMARY KEY,
		phase        TEXT NOT NULL,
		participants TEXT NOT NULL,
		decision     TEXT NOT NULL DEFAULT '',
		created_at   TEXT NOT NULL,
		decided_at   TEXT NOT NULL DEFAULT ''
	)`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create txn_log table: %w", err)
	}

	return &TxnLog{db: db}, nil
}

// Close closes the transaction log database.
func (tl *TxnLog) Close() error {
	return tl.db.Close()
}

// LogPrepare records the start of the prepare phase for a transaction.
func (tl *TxnLog) LogPrepare(txnID string, participants []ShardTarget) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	participantsJSON, err := json.Marshal(participants)
	if err != nil {
		return fmt.Errorf("marshal participants: %w", err)
	}

	_, err = tl.db.Exec(
		`INSERT INTO txn_log (txn_id, phase, participants, decision, created_at, decided_at)
		 VALUES (?, ?, ?, '', ?, '')`,
		txnID,
		PhasePreparing,
		string(participantsJSON),
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("log prepare: %w", err)
	}
	return nil
}

// LogPrepared updates the transaction phase to "prepared" after all participants
// have acknowledged the prepare request.
func (tl *TxnLog) LogPrepared(txnID string) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	_, err := tl.db.Exec(
		`UPDATE txn_log SET phase = ? WHERE txn_id = ?`,
		PhasePrepared, txnID,
	)
	if err != nil {
		return fmt.Errorf("log prepared: %w", err)
	}
	return nil
}

// LogDecision records the commit or abort decision for a transaction.
// This MUST be called before sending commit/abort to participants (write-ahead).
func (tl *TxnLog) LogDecision(txnID string, decision string) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	phase := PhaseCommitting
	if decision == DecisionAbort {
		phase = PhaseAborting
	}

	_, err := tl.db.Exec(
		`UPDATE txn_log SET phase = ?, decision = ?, decided_at = ? WHERE txn_id = ?`,
		phase, decision, time.Now().UTC().Format(time.RFC3339Nano), txnID,
	)
	if err != nil {
		return fmt.Errorf("log decision: %w", err)
	}
	return nil
}

// LogComplete marks a transaction as fully resolved. Once complete, the
// record can be cleaned up by Cleanup.
func (tl *TxnLog) LogComplete(txnID string) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	phase := PhaseCommitted
	// Check what decision was made to set the right completed phase.
	var decision string
	err := tl.db.QueryRow(`SELECT decision FROM txn_log WHERE txn_id = ?`, txnID).Scan(&decision)
	if err != nil {
		return fmt.Errorf("lookup decision for complete: %w", err)
	}
	if decision == DecisionAbort {
		phase = PhaseAborted
	}

	_, err = tl.db.Exec(
		`UPDATE txn_log SET phase = ? WHERE txn_id = ?`,
		phase, txnID,
	)
	if err != nil {
		return fmt.Errorf("log complete: %w", err)
	}
	return nil
}

// GetPending returns all non-complete transactions (those still in progress
// or that were interrupted). Used by crash recovery.
func (tl *TxnLog) GetPending() ([]TxnRecord, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	rows, err := tl.db.Query(
		`SELECT txn_id, phase, participants, decision, created_at, decided_at
		 FROM txn_log
		 WHERE phase NOT IN (?, ?)
		 ORDER BY created_at ASC`,
		PhaseCommitted, PhaseAborted,
	)
	if err != nil {
		return nil, fmt.Errorf("query pending txns: %w", err)
	}
	defer rows.Close()

	var records []TxnRecord
	for rows.Next() {
		var r TxnRecord
		var participantsJSON string
		var createdStr, decidedStr string

		if err := rows.Scan(&r.TxnID, &r.Phase, &participantsJSON, &r.Decision, &createdStr, &decidedStr); err != nil {
			return nil, fmt.Errorf("scan pending txn: %w", err)
		}

		if err := json.Unmarshal([]byte(participantsJSON), &r.Participants); err != nil {
			return nil, fmt.Errorf("unmarshal participants for %s: %w", r.TxnID, err)
		}

		if t, err := time.Parse(time.RFC3339Nano, createdStr); err == nil {
			r.CreatedAt = t
		}
		if decidedStr != "" {
			if t, err := time.Parse(time.RFC3339Nano, decidedStr); err == nil {
				r.DecidedAt = &t
			}
		}

		records = append(records, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending txns: %w", err)
	}
	return records, nil
}

// Cleanup purges completed transaction records older than the given duration.
func (tl *TxnLog) Cleanup(olderThan time.Duration) (int64, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	cutoff := time.Now().UTC().Add(-olderThan).Format(time.RFC3339Nano)
	result, err := tl.db.Exec(
		`DELETE FROM txn_log WHERE phase IN (?, ?) AND created_at < ?`,
		PhaseCommitted, PhaseAborted, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup txn log: %w", err)
	}

	deleted, _ := result.RowsAffected()
	return deleted, nil
}
