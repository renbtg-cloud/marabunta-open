// Marabunta - Licensed under the MIT License.
package vsam

import (
	"fmt"
	"sync/atomic"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// ESDS implements Entry-Sequenced Data Set access.
// Records are appended sequentially and accessed by Relative Byte Address (RBA).
// Key format: cics:vsam:{dataset}:esds:{rba:020d}
type ESDS struct {
	client    *swarm.SwarmClient
	nextRBA   atomic.Int64
}

// NewESDS creates a new ESDS accessor.
func NewESDS(client *swarm.SwarmClient) *ESDS {
	return &ESDS{client: client}
}

// Append adds a record to the end of the dataset and returns the RBA.
func (e *ESDS) Append(dataset string, data []byte) (int64, error) {
	rba := e.nextRBA.Add(1) - 1
	swarmKey := fmt.Sprintf("cics:vsam:%s:esds:%020d", dataset, rba)

	_, err := e.client.Store(swarm.StoreRequest{
		Key:     []byte(swarmKey),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	if err != nil {
		return 0, fmt.Errorf("ESDS append %s: %w", dataset, err)
	}
	return rba, nil
}

// Read retrieves a record by its RBA.
func (e *ESDS) Read(dataset string, rba int64) ([]byte, error) {
	swarmKey := fmt.Sprintf("cics:vsam:%s:esds:%020d", dataset, rba)
	resp, err := e.client.Fetch(swarm.FetchRequest{
		Key:     []byte(swarmKey),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, fmt.Errorf("ESDS read %s rba=%d: %w", dataset, rba, err)
	}
	if !resp.Found {
		return nil, fmt.Errorf("ESDS record not found: %s rba=%d", dataset, rba)
	}
	return resp.Value, nil
}

// ReadRange reads records from startRBA to endRBA (exclusive).
func (e *ESDS) ReadRange(dataset string, startRBA, endRBA int64) ([][]byte, error) {
	var records [][]byte
	for rba := startRBA; rba < endRBA; rba++ {
		data, err := e.Read(dataset, rba)
		if err != nil {
			break // End of available records.
		}
		records = append(records, data)
	}
	return records, nil
}

// Update modifies a record at the given RBA. Note: ESDS records are normally
// not updatable, but CICS allows REWRITE for some operations.
func (e *ESDS) Update(dataset string, rba int64, data []byte) error {
	swarmKey := fmt.Sprintf("cics:vsam:%s:esds:%020d", dataset, rba)
	_, err := e.client.Store(swarm.StoreRequest{
		Key:     []byte(swarmKey),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	if err != nil {
		return fmt.Errorf("ESDS update %s rba=%d: %w", dataset, rba, err)
	}
	return nil
}

// NextRBA returns the next RBA that will be assigned.
func (e *ESDS) NextRBA() int64 {
	return e.nextRBA.Load()
}
