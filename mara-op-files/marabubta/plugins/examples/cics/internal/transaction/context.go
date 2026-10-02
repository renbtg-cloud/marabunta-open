// Marabunta - Licensed under the MIT License.
package transaction

import (
	"fmt"
	"sync"
	"time"
)

// CICSContext holds the execution context for a CICS transaction,
// including the EIB (Execute Interface Block) and transaction log.
type CICSContext struct {
	mu sync.Mutex

	// EIB (Execute Interface Block) fields.
	EIBTRNID  string    // Transaction identifier
	EIBTRMID  string    // Terminal identifier
	EIBDATE   int32     // Current date (YYYYDDD)
	EIBTIME   int32     // Current time (HHMMSS)
	EIBTASKN  int32     // Task number
	EIBCALEN  int32     // COMMAREA length
	EIBFN     int16     // Function code of last EXEC CICS
	EIBRCODE  int32     // Response code of last EXEC CICS
	EIBDS     string    // Last dataset name
	EIBREQID  string    // Request identifier
	EIBRSRCE  string    // Resource name
	EIBRESP   int32     // RESP value
	EIBRESP2  int32     // RESP2 value

	// Execution metadata.
	RegionID  string
	StartTime time.Time

	// Transaction log for syncpoint/rollback.
	txLog     []TxLogEntry
	committed bool
}

// TxLogEntry records a single operation in the transaction log.
type TxLogEntry struct {
	Timestamp time.Time
	Command   string
	Detail    string
	Undoable  bool
	UndoFunc  func() error
}

// NewContext creates a new CICS execution context.
func NewContext(tranID, termID, regionID string) *CICSContext {
	now := time.Now()
	return &CICSContext{
		EIBTRNID:  tranID,
		EIBTRMID:  termID,
		EIBDATE:   int32(now.Year()*1000 + now.YearDay()),
		EIBTIME:   int32(now.Hour()*10000 + now.Minute()*100 + now.Second()),
		RegionID:  regionID,
		StartTime: now,
	}
}

// LogEntry adds an entry to the transaction log.
func (ctx *CICSContext) LogEntry(command, detail string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	ctx.txLog = append(ctx.txLog, TxLogEntry{
		Timestamp: time.Now(),
		Command:   command,
		Detail:    detail,
	})
}

// LogUndoable adds an undoable entry to the transaction log.
func (ctx *CICSContext) LogUndoable(command, detail string, undoFunc func() error) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	ctx.txLog = append(ctx.txLog, TxLogEntry{
		Timestamp: time.Now(),
		Command:   command,
		Detail:    detail,
		Undoable:  true,
		UndoFunc:  undoFunc,
	})
}

// Commit marks the transaction as committed and clears undo functions.
func (ctx *CICSContext) Commit() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	ctx.committed = true
	// Clear undo functions — committed changes are permanent.
	for i := range ctx.txLog {
		ctx.txLog[i].UndoFunc = nil
	}
}

// Rollback undoes all uncommitted changes by calling undo functions
// in reverse order (spec C.6).
func (ctx *CICSContext) Rollback() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.committed {
		return
	}

	// Undo in reverse order.
	for i := len(ctx.txLog) - 1; i >= 0; i-- {
		entry := ctx.txLog[i]
		if entry.Undoable && entry.UndoFunc != nil {
			if err := entry.UndoFunc(); err != nil {
				// Log the error but continue rolling back.
				ctx.txLog = append(ctx.txLog, TxLogEntry{
					Timestamp: time.Now(),
					Command:   "ROLLBACK_ERROR",
					Detail:    fmt.Sprintf("undo %s failed: %v", entry.Command, err),
				})
			}
		}
	}
}

// SetResp sets the EIBRESP and EIBRESP2 values.
func (ctx *CICSContext) SetResp(resp, resp2 int32) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.EIBRESP = resp
	ctx.EIBRESP2 = resp2
}

// SetDataset sets the EIBDS (last dataset name).
func (ctx *CICSContext) SetDataset(ds string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.EIBDS = ds
}

// TxLogSize returns the number of entries in the transaction log.
func (ctx *CICSContext) TxLogSize() int {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return len(ctx.txLog)
}

// IsCommitted returns whether the transaction has been committed.
func (ctx *CICSContext) IsCommitted() bool {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.committed
}

// CICS response codes.
const (
	RespNormal     int32 = 0
	RespError      int32 = 1
	RespNotFnd     int32 = 13
	RespDupRec     int32 = 14
	RespLenErr     int32 = 22
	RespQIDErr     int32 = 44
	RespItemErr    int32 = 26
	RespInvReq     int32 = 16
	RespPGMIDErr   int32 = 27
	RespDisabled   int32 = 84
	RespNotAuth    int32 = 70
	RespEndFile    int32 = 20
)
