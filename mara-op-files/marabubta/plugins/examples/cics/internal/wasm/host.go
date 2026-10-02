// Marabunta - Licensed under the MIT License.
package wasm

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/tetratelabs/wazero/api"

	"github.com/marabunta/marabunta-cics/internal/queue"
	"github.com/marabunta/marabunta-cics/internal/vsam"
)

// HostFunctions provides the EXEC CICS host function implementations
// that are callable from WASM programs. These bridge WASM execution
// to the swarm-backed CICS subsystems.
type HostFunctions struct {
	vsamMgr *vsam.Manager
	tsMgr   *queue.TSManager
	tdMgr   *queue.TDManager
}

// NewHostFunctions creates host function bindings.
func NewHostFunctions(vsamMgr *vsam.Manager, tsMgr *queue.TSManager, tdMgr *queue.TDManager) *HostFunctions {
	return &HostFunctions{
		vsamMgr: vsamMgr,
		tsMgr:   tsMgr,
		tdMgr:   tdMgr,
	}
}

// readMemString reads a string from WASM linear memory.
func readMemString(m api.Module, ptr, length uint32) (string, bool) {
	data, ok := m.Memory().Read(ptr, length)
	if !ok {
		return "", false
	}
	return string(data), true
}

// readMemBytes reads bytes from WASM linear memory.
func readMemBytes(m api.Module, ptr, length uint32) ([]byte, bool) {
	return m.Memory().Read(ptr, length)
}

// writeMemBytes writes bytes to WASM linear memory.
func writeMemBytes(m api.Module, ptr uint32, data []byte) bool {
	return m.Memory().Write(ptr, data)
}

// writeMemUint32 writes a uint32 to WASM linear memory.
func writeMemUint32(m api.Module, ptr, value uint32) bool {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, value)
	return m.Memory().Write(ptr, buf)
}

// CICSRead implements the cics_read host function.
// Parameters: dataset_ptr, dataset_len, key_ptr, key_len, buf_ptr, buf_len_ptr
// Returns: 0=success, 1=NOTFND, 2=error
func (hf *HostFunctions) CICSRead(ctx context.Context, m api.Module, datasetPtr, datasetLen, keyPtr, keyLen, bufPtr, bufLenPtr uint32) uint32 {
	dataset, ok := readMemString(m, datasetPtr, datasetLen)
	if !ok {
		return 2
	}
	key, ok := readMemString(m, keyPtr, keyLen)
	if !ok {
		return 2
	}

	data, err := hf.vsamMgr.KSDS().Read(dataset, key)
	if err != nil {
		log.Printf("WASM cics_read: %v", err)
		return 1 // NOTFND
	}

	if !writeMemBytes(m, bufPtr, data) {
		return 2
	}
	if !writeMemUint32(m, bufLenPtr, uint32(len(data))) {
		return 2
	}
	return 0
}

// CICSWrite implements the cics_write host function.
// Parameters: dataset_ptr, dataset_len, key_ptr, key_len, data_ptr, data_len
// Returns: 0=success, 1=DUPREC, 2=error
func (hf *HostFunctions) CICSWrite(ctx context.Context, m api.Module, datasetPtr, datasetLen, keyPtr, keyLen, dataPtr, dataLen uint32) uint32 {
	dataset, ok := readMemString(m, datasetPtr, datasetLen)
	if !ok {
		return 2
	}
	key, ok := readMemString(m, keyPtr, keyLen)
	if !ok {
		return 2
	}
	data, ok := readMemBytes(m, dataPtr, dataLen)
	if !ok {
		return 2
	}

	if err := hf.vsamMgr.KSDS().Write(dataset, key, data); err != nil {
		log.Printf("WASM cics_write: %v", err)
		return 2
	}
	return 0
}

// CICSDelete implements the cics_delete host function.
// Parameters: dataset_ptr, dataset_len, key_ptr, key_len
// Returns: 0=success, 1=NOTFND, 2=error
func (hf *HostFunctions) CICSDelete(ctx context.Context, m api.Module, datasetPtr, datasetLen, keyPtr, keyLen uint32) uint32 {
	dataset, ok := readMemString(m, datasetPtr, datasetLen)
	if !ok {
		return 2
	}
	key, ok := readMemString(m, keyPtr, keyLen)
	if !ok {
		return 2
	}

	if err := hf.vsamMgr.KSDS().Delete(dataset, key); err != nil {
		log.Printf("WASM cics_delete: %v", err)
		return 1
	}
	return 0
}

// CICSWriteQTS implements the cics_writeq_ts host function.
// Parameters: queue_ptr, queue_len, data_ptr, data_len
// Returns: 0=success, 2=error
func (hf *HostFunctions) CICSWriteQTS(ctx context.Context, m api.Module, queuePtr, queueLen, dataPtr, dataLen uint32) uint32 {
	queueName, ok := readMemString(m, queuePtr, queueLen)
	if !ok {
		return 2
	}
	data, ok := readMemBytes(m, dataPtr, dataLen)
	if !ok {
		return 2
	}

	if err := hf.tsMgr.Write(queueName, data); err != nil {
		log.Printf("WASM cics_writeq_ts: %v", err)
		return 2
	}
	return 0
}

// CICSReadQTS implements the cics_readq_ts host function.
// Parameters: queue_ptr, queue_len, item_num, buf_ptr, buf_len_ptr
// Returns: 0=success, 1=ITEMERR, 2=error
func (hf *HostFunctions) CICSReadQTS(ctx context.Context, m api.Module, queuePtr, queueLen, itemNum, bufPtr, bufLenPtr uint32) uint32 {
	queueName, ok := readMemString(m, queuePtr, queueLen)
	if !ok {
		return 2
	}

	data, err := hf.tsMgr.Read(queueName, int(itemNum))
	if err != nil {
		log.Printf("WASM cics_readq_ts: %v", err)
		return 1
	}

	if !writeMemBytes(m, bufPtr, data) {
		return 2
	}
	if !writeMemUint32(m, bufLenPtr, uint32(len(data))) {
		return 2
	}
	return 0
}

// CICSWriteQTD implements the cics_writeq_td host function.
func (hf *HostFunctions) CICSWriteQTD(ctx context.Context, m api.Module, queuePtr, queueLen, dataPtr, dataLen uint32) uint32 {
	queueName, ok := readMemString(m, queuePtr, queueLen)
	if !ok {
		return 2
	}
	data, ok := readMemBytes(m, dataPtr, dataLen)
	if !ok {
		return 2
	}

	if err := hf.tdMgr.Write(queueName, data); err != nil {
		log.Printf("WASM cics_writeq_td: %v", err)
		return 2
	}
	return 0
}

// CICSReadQTD implements the cics_readq_td host function (destructive read).
func (hf *HostFunctions) CICSReadQTD(ctx context.Context, m api.Module, queuePtr, queueLen, bufPtr, bufLenPtr uint32) uint32 {
	queueName, ok := readMemString(m, queuePtr, queueLen)
	if !ok {
		return 2
	}

	data, err := hf.tdMgr.Read(queueName)
	if err != nil {
		log.Printf("WASM cics_readq_td: %v", err)
		return 1
	}

	if !writeMemBytes(m, bufPtr, data) {
		return 2
	}
	if !writeMemUint32(m, bufLenPtr, uint32(len(data))) {
		return 2
	}
	return 0
}

// CICSAbend implements the cics_abend host function.
func (hf *HostFunctions) CICSAbend(ctx context.Context, m api.Module, codePtr, codeLen uint32) uint32 {
	code, ok := readMemString(m, codePtr, codeLen)
	if !ok {
		code = "????"
	}
	log.Printf("WASM ABEND: %s", code)
	// In CICS, an ABEND terminates the transaction.
	return 0
}

// ErrorResponse formats a CICS error response string.
func ErrorResponse(eibresp int32, command string) string {
	return fmt.Sprintf("EXEC CICS %s EIBRESP=%d", command, eibresp)
}
