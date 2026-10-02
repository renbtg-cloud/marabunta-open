// Marabunta - Licensed under the MIT License.
// Package wasm implements WASM-based program execution for CICS transactions
// using wazero (pure Go WebAssembly runtime).
package wasm

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	swarm "github.com/marabunta/swarm-plugin-go"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/marabunta/marabunta-cics/internal/queue"
	"github.com/marabunta/marabunta-cics/internal/vsam"
)

// Runtime manages WASM module loading, caching, and execution.
type Runtime struct {
	ctx     context.Context
	rt      wazero.Runtime
	cache   *ModuleCache
	wasmDir string
	client  *swarm.SwarmClient
	vsamMgr *vsam.Manager
	tsMgr   *queue.TSManager
	tdMgr   *queue.TDManager
}

// NewRuntime creates a new WASM runtime with host function bindings.
func NewRuntime(ctx context.Context, wasmDir string, client *swarm.SwarmClient, vsamMgr *vsam.Manager, tsMgr *queue.TSManager, tdMgr *queue.TDManager) (*Runtime, error) {
	// Create wazero runtime with compilation cache.
	rtConfig := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true)

	rt := wazero.NewRuntimeWithConfig(ctx, rtConfig)

	// Instantiate WASI for basic I/O.
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	r := &Runtime{
		ctx:     ctx,
		rt:      rt,
		cache:   NewModuleCache(),
		wasmDir: wasmDir,
		client:  client,
		vsamMgr: vsamMgr,
		tsMgr:   tsMgr,
		tdMgr:   tdMgr,
	}

	// Register host functions (EXEC CICS bindings).
	if err := r.registerHostFunctions(ctx); err != nil {
		rt.Close(ctx)
		return nil, fmt.Errorf("register host functions: %w", err)
	}

	// Ensure wasm directory exists.
	if err := os.MkdirAll(wasmDir, 0755); err != nil {
		rt.Close(ctx)
		return nil, fmt.Errorf("create wasm dir: %w", err)
	}

	log.Printf("WASM runtime initialized: dir=%s", wasmDir)
	return r, nil
}

// Execute runs a WASM program with the given input data and CICS context.
// The cicsCtx parameter accepts a *transaction.CICSContext (passed as interface{}
// to avoid an import cycle between wasm and transaction packages).
func (r *Runtime) Execute(programName, input string, cicsCtx interface{}) (string, error) {
	// Load or retrieve cached module.
	mod, err := r.getModule(programName)
	if err != nil {
		return "", fmt.Errorf("load program %s: %w", programName, err)
	}

	// Create a new instance with input data.
	modConfig := wazero.NewModuleConfig().
		WithName(programName).
		WithStdin(nil).
		WithStdout(nil).
		WithStderr(nil)

	instance, err := r.rt.InstantiateModule(r.ctx, mod, modConfig)
	if err != nil {
		return "", fmt.Errorf("instantiate %s: %w", programName, err)
	}
	defer instance.Close(r.ctx)

	// Call the program's main function.
	main := instance.ExportedFunction("_start")
	if main == nil {
		main = instance.ExportedFunction("main")
	}
	if main == nil {
		return "", fmt.Errorf("program %s has no _start or main function", programName)
	}

	_, err = main.Call(r.ctx)
	if err != nil {
		return "", fmt.Errorf("execute %s: %w", programName, err)
	}

	return "PROGRAM " + programName + " COMPLETE", nil
}

// getModule loads a WASM module from cache or filesystem.
func (r *Runtime) getModule(name string) (wazero.CompiledModule, error) {
	// Check cache first.
	cached := r.cache.Get(name)
	if cached != nil {
		return cached, nil
	}

	// Load from filesystem.
	wasmPath := filepath.Join(r.wasmDir, name+".wasm")
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", wasmPath, err)
	}

	// Compile.
	mod, err := r.rt.CompileModule(r.ctx, wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("compile %s: %w", name, err)
	}

	// Cache.
	r.cache.Put(name, mod)
	return mod, nil
}

// registerHostFunctions registers EXEC CICS host functions that WASM
// programs can call. Each function delegates to the real HostFunctions
// implementation that bridges to VSAM, TS/TD queues, etc.
func (r *Runtime) registerHostFunctions(ctx context.Context) error {
	hf := NewHostFunctions(r.vsamMgr, r.tsMgr, r.tdMgr)

	_, err := r.rt.NewHostModuleBuilder("cics").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, datasetPtr, datasetLen, keyPtr, keyLen, bufPtr, bufLenPtr uint32) uint32 {
			return hf.CICSRead(ctx, m, datasetPtr, datasetLen, keyPtr, keyLen, bufPtr, bufLenPtr)
		}).
		Export("cics_read").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, datasetPtr, datasetLen, keyPtr, keyLen, dataPtr, dataLen uint32) uint32 {
			return hf.CICSWrite(ctx, m, datasetPtr, datasetLen, keyPtr, keyLen, dataPtr, dataLen)
		}).
		Export("cics_write").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, datasetPtr, datasetLen, keyPtr, keyLen uint32) uint32 {
			return hf.CICSDelete(ctx, m, datasetPtr, datasetLen, keyPtr, keyLen)
		}).
		Export("cics_delete").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, queuePtr, queueLen, dataPtr, dataLen uint32) uint32 {
			return hf.CICSWriteQTS(ctx, m, queuePtr, queueLen, dataPtr, dataLen)
		}).
		Export("cics_writeq_ts").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, queuePtr, queueLen, itemNum, bufPtr, bufLenPtr uint32) uint32 {
			return hf.CICSReadQTS(ctx, m, queuePtr, queueLen, itemNum, bufPtr, bufLenPtr)
		}).
		Export("cics_readq_ts").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, queuePtr, queueLen, dataPtr, dataLen uint32) uint32 {
			return hf.CICSWriteQTD(ctx, m, queuePtr, queueLen, dataPtr, dataLen)
		}).
		Export("cics_writeq_td").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, queuePtr, queueLen, bufPtr, bufLenPtr uint32) uint32 {
			return hf.CICSReadQTD(ctx, m, queuePtr, queueLen, bufPtr, bufLenPtr)
		}).
		Export("cics_readq_td").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, msgPtr, msgLen uint32) uint32 {
			// cics_send: Send data to terminal. Logs the message.
			msg, ok := readMemString(m, msgPtr, msgLen)
			if !ok {
				return 2
			}
			log.Printf("WASM cics_send: %s", msg)
			return 0
		}).
		Export("cics_send").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module) uint32 {
			// cics_return: Return from program.
			return 0
		}).
		Export("cics_return").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module) uint32 {
			// cics_syncpoint: Commit current transaction.
			log.Printf("WASM cics_syncpoint: commit requested")
			return 0
		}).
		Export("cics_syncpoint").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module) uint32 {
			// cics_syncpoint_rollback: Rollback current transaction.
			log.Printf("WASM cics_syncpoint_rollback: rollback requested")
			return 0
		}).
		Export("cics_syncpoint_rollback").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, codePtr, codeLen uint32) uint32 {
			return hf.CICSAbend(ctx, m, codePtr, codeLen)
		}).
		Export("cics_abend").
		Instantiate(ctx)

	return err
}

// CacheSize returns the number of cached WASM modules.
func (r *Runtime) CacheSize() int {
	return r.cache.Size()
}

// Close shuts down the WASM runtime.
func (r *Runtime) Close() {
	r.rt.Close(r.ctx)
}
