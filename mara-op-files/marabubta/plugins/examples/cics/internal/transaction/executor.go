// Marabunta - Licensed under the MIT License.
// Package transaction implements the EXEC CICS command interpreter and
// transaction execution engine.
package transaction

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-cics/internal/queue"
	"github.com/marabunta/marabunta-cics/internal/vsam"
	"github.com/marabunta/marabunta-cics/internal/wasm"
)

// Executor interprets EXEC CICS commands and dispatches them to the
// appropriate subsystem (VSAM, queues, WASM programs, etc.).
type Executor struct {
	client   *swarm.SwarmClient
	vsamMgr  *vsam.Manager
	tsMgr    *queue.TSManager
	tdMgr    *queue.TDManager
	wasmRT   *wasm.Runtime
	regionID string

	// Transaction definitions: tranID -> program name.
	tranMu   sync.RWMutex
	tranDefs map[string]string
}

// NewExecutor creates a new transaction executor.
func NewExecutor(client *swarm.SwarmClient, vsamMgr *vsam.Manager, tsMgr *queue.TSManager, tdMgr *queue.TDManager, wasmRT *wasm.Runtime, regionID string) *Executor {
	return &Executor{
		client:   client,
		vsamMgr:  vsamMgr,
		tsMgr:    tsMgr,
		tdMgr:    tdMgr,
		wasmRT:   wasmRT,
		regionID: regionID,
		tranDefs: map[string]string{
			"CESN": "DFHSNP",   // Sign-on
			"CESF": "DFHSFP",   // Sign-off
			"CEMT": "DFHEMTP",  // Master terminal
			"CEDA": "DFHEDAP",  // Resource definition
			"CEDF": "DFHEDFP",  // EDF debugging
			"CECI": "DFHECIP",  // Command interpreter
		},
	}
}

// Execute runs a transaction with the given transaction ID, input data,
// and terminal ID. Returns the screen output text.
func (e *Executor) Execute(tranID, data, termID string) (string, error) {
	tranID = strings.ToUpper(strings.TrimSpace(tranID))
	if tranID == "" {
		return "", fmt.Errorf("empty transaction ID")
	}

	log.Printf("CICS: execute tranID=%s termID=%s", tranID, termID)

	// Create execution context.
	ctx := NewContext(tranID, termID, e.regionID)
	ctx.StartTime = time.Now()

	// Look up transaction definition.
	e.tranMu.RLock()
	program, ok := e.tranDefs[tranID]
	e.tranMu.RUnlock()
	if !ok {
		// Try as a direct EXEC CICS command.
		result, err := e.executeCommand(ctx, tranID+" "+data)
		if err != nil {
			return "", fmt.Errorf("transaction %s not defined: %w", tranID, err)
		}
		return result, nil
	}

	// Execute WASM program.
	result, err := e.wasmRT.Execute(program, data, ctx)
	if err != nil {
		return "", fmt.Errorf("program %s failed: %w", program, err)
	}

	return result, nil
}

// executeCommand interprets a single EXEC CICS command.
func (e *Executor) executeCommand(ctx *CICSContext, cmd string) (string, error) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty command")
	}

	verb := strings.ToUpper(parts[0])
	args := make(map[string]string)

	// Parse keyword arguments: KEY(VALUE).
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		if idx := strings.Index(part, "("); idx > 0 {
			key := strings.ToUpper(part[:idx])
			val := strings.TrimRight(part[idx+1:], ")")
			args[key] = val
		}
	}

	switch verb {
	case "READ":
		return e.cmdRead(ctx, args)
	case "WRITE":
		return e.cmdWrite(ctx, args)
	case "REWRITE":
		return e.cmdRewrite(ctx, args)
	case "DELETE":
		return e.cmdDelete(ctx, args)
	case "READQ":
		return e.cmdReadQ(ctx, args)
	case "WRITEQ":
		return e.cmdWriteQ(ctx, args)
	case "DELETEQ":
		return e.cmdDeleteQ(ctx, args)
	case "SEND":
		return e.cmdSend(ctx, args)
	case "RECEIVE":
		return e.cmdReceive(ctx, args)
	case "LINK":
		return e.cmdLink(ctx, args)
	case "XCTL":
		return e.cmdXctl(ctx, args)
	case "RETURN":
		return "TRANSACTION COMPLETE", nil
	case "SYNCPOINT":
		return e.cmdSyncpoint(ctx, args)
	case "INQUIRE":
		return e.cmdInquire(ctx, args)
	default:
		return "", fmt.Errorf("unknown EXEC CICS command: %s", verb)
	}
}

func (e *Executor) cmdRead(ctx *CICSContext, args map[string]string) (string, error) {
	dataset := args["DATASET"]
	if dataset == "" {
		dataset = args["FILE"]
	}
	ridfld := args["RIDFLD"]
	if dataset == "" || ridfld == "" {
		return "", fmt.Errorf("READ requires DATASET and RIDFLD")
	}

	data, err := e.vsamMgr.KSDS().Read(dataset, ridfld)
	if err != nil {
		ctx.LogEntry("READ", fmt.Sprintf("NOTFND DATASET(%s) RIDFLD(%s)", dataset, ridfld))
		return "", fmt.Errorf("READ NOTFND: %w", err)
	}

	ctx.LogEntry("READ", fmt.Sprintf("OK DATASET(%s) RIDFLD(%s) LEN(%d)", dataset, ridfld, len(data)))
	return string(data), nil
}

func (e *Executor) cmdWrite(ctx *CICSContext, args map[string]string) (string, error) {
	dataset := args["DATASET"]
	if dataset == "" {
		dataset = args["FILE"]
	}
	ridfld := args["RIDFLD"]
	from := args["FROM"]
	if dataset == "" || ridfld == "" {
		return "", fmt.Errorf("WRITE requires DATASET and RIDFLD")
	}

	if err := e.vsamMgr.KSDS().Write(dataset, ridfld, []byte(from)); err != nil {
		return "", fmt.Errorf("WRITE failed: %w", err)
	}

	ctx.LogEntry("WRITE", fmt.Sprintf("OK DATASET(%s) RIDFLD(%s)", dataset, ridfld))
	return "WRITE COMPLETE", nil
}

func (e *Executor) cmdRewrite(ctx *CICSContext, args map[string]string) (string, error) {
	dataset := args["DATASET"]
	if dataset == "" {
		dataset = args["FILE"]
	}
	from := args["FROM"]
	ridfld := args["RIDFLD"]
	if dataset == "" || ridfld == "" {
		return "", fmt.Errorf("REWRITE requires DATASET and RIDFLD")
	}

	if err := e.vsamMgr.KSDS().Write(dataset, ridfld, []byte(from)); err != nil {
		return "", fmt.Errorf("REWRITE failed: %w", err)
	}

	ctx.LogEntry("REWRITE", fmt.Sprintf("OK DATASET(%s) RIDFLD(%s)", dataset, ridfld))
	return "REWRITE COMPLETE", nil
}

func (e *Executor) cmdDelete(ctx *CICSContext, args map[string]string) (string, error) {
	dataset := args["DATASET"]
	if dataset == "" {
		dataset = args["FILE"]
	}
	ridfld := args["RIDFLD"]
	if dataset == "" || ridfld == "" {
		return "", fmt.Errorf("DELETE requires DATASET and RIDFLD")
	}

	if err := e.vsamMgr.KSDS().Delete(dataset, ridfld); err != nil {
		return "", fmt.Errorf("DELETE failed: %w", err)
	}

	ctx.LogEntry("DELETE", fmt.Sprintf("OK DATASET(%s) RIDFLD(%s)", dataset, ridfld))
	return "DELETE COMPLETE", nil
}

func (e *Executor) cmdReadQ(ctx *CICSContext, args map[string]string) (string, error) {
	queueName := args["QUEUE"]
	if queueName == "" {
		return "", fmt.Errorf("READQ requires QUEUE")
	}

	if _, ok := args["TD"]; ok {
		data, err := e.tdMgr.Read(queueName)
		if err != nil {
			return "", fmt.Errorf("READQ TD failed: %w", err)
		}
		return string(data), nil
	}

	// Default to TS.
	data, err := e.tsMgr.Read(queueName, 1)
	if err != nil {
		return "", fmt.Errorf("READQ TS failed: %w", err)
	}
	return string(data), nil
}

func (e *Executor) cmdWriteQ(ctx *CICSContext, args map[string]string) (string, error) {
	queueName := args["QUEUE"]
	from := args["FROM"]
	if queueName == "" {
		return "", fmt.Errorf("WRITEQ requires QUEUE")
	}

	if _, ok := args["TD"]; ok {
		if err := e.tdMgr.Write(queueName, []byte(from)); err != nil {
			return "", fmt.Errorf("WRITEQ TD failed: %w", err)
		}
		return "WRITEQ TD COMPLETE", nil
	}

	// Default to TS.
	if err := e.tsMgr.Write(queueName, []byte(from)); err != nil {
		return "", fmt.Errorf("WRITEQ TS failed: %w", err)
	}
	return "WRITEQ TS COMPLETE", nil
}

func (e *Executor) cmdDeleteQ(ctx *CICSContext, args map[string]string) (string, error) {
	queueName := args["QUEUE"]
	if queueName == "" {
		return "", fmt.Errorf("DELETEQ requires QUEUE")
	}

	if _, ok := args["TD"]; ok {
		if err := e.tdMgr.Delete(queueName); err != nil {
			return "", fmt.Errorf("DELETEQ TD failed: %w", err)
		}
		return "DELETEQ TD COMPLETE", nil
	}

	if err := e.tsMgr.Delete(queueName); err != nil {
		return "", fmt.Errorf("DELETEQ TS failed: %w", err)
	}
	return "DELETEQ TS COMPLETE", nil
}

func (e *Executor) cmdSend(ctx *CICSContext, args map[string]string) (string, error) {
	from := args["FROM"]
	if from == "" {
		return "", fmt.Errorf("SEND requires FROM")
	}
	return from, nil
}

func (e *Executor) cmdReceive(ctx *CICSContext, args map[string]string) (string, error) {
	// In a full implementation, this would wait for terminal input.
	return "RECEIVE AWAITING INPUT", nil
}

func (e *Executor) cmdLink(ctx *CICSContext, args map[string]string) (string, error) {
	program := args["PROGRAM"]
	if program == "" {
		return "", fmt.Errorf("LINK requires PROGRAM")
	}

	commarea := args["COMMAREA"]
	result, err := e.wasmRT.Execute(program, commarea, ctx)
	if err != nil {
		return "", fmt.Errorf("LINK %s failed: %w", program, err)
	}
	return result, nil
}

func (e *Executor) cmdXctl(ctx *CICSContext, args map[string]string) (string, error) {
	program := args["PROGRAM"]
	if program == "" {
		return "", fmt.Errorf("XCTL requires PROGRAM")
	}

	commarea := args["COMMAREA"]
	result, err := e.wasmRT.Execute(program, commarea, ctx)
	if err != nil {
		return "", fmt.Errorf("XCTL %s failed: %w", program, err)
	}
	return result, nil
}

func (e *Executor) cmdSyncpoint(ctx *CICSContext, args map[string]string) (string, error) {
	if _, ok := args["ROLLBACK"]; ok {
		ctx.Rollback()
		return "SYNCPOINT ROLLBACK COMPLETE", nil
	}
	ctx.Commit()
	return "SYNCPOINT COMPLETE", nil
}

func (e *Executor) cmdInquire(ctx *CICSContext, args map[string]string) (string, error) {
	what := ""
	for k := range args {
		what = k
		break
	}
	switch what {
	case "SYSTEM":
		return fmt.Sprintf("REGION(%s) STATUS(ACTIVE)", e.regionID), nil
	case "TRANSACTION":
		tranID := args["TRANSACTION"]
		e.tranMu.RLock()
		_, ok := e.tranDefs[tranID]
		e.tranMu.RUnlock()
		if ok {
			return fmt.Sprintf("TRANSACTION(%s) STATUS(ENABLED)", tranID), nil
		}
		return fmt.Sprintf("TRANSACTION(%s) STATUS(NOTFOUND)", tranID), nil
	default:
		return fmt.Sprintf("INQUIRE %s: NOT SUPPORTED", what), nil
	}
}

// DefineTran registers a transaction ID to program mapping.
func (e *Executor) DefineTran(tranID, program string) {
	e.tranMu.Lock()
	defer e.tranMu.Unlock()
	e.tranDefs[strings.ToUpper(tranID)] = program
}
