// Marabunta - Licensed under the MIT License.
package vsam

import (
	"fmt"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// RRDS implements Relative Record Data Set access.
// Records are stored in fixed-size slots addressed by slot number.
// Key format: cics:vsam:{dataset}:rrds:{slot:020d}
type RRDS struct {
	client *swarm.SwarmClient
}

// NewRRDS creates a new RRDS accessor.
func NewRRDS(client *swarm.SwarmClient) *RRDS {
	return &RRDS{client: client}
}

// Write stores a record at the given slot number.
func (r *RRDS) Write(dataset string, slot int64, data []byte) error {
	swarmKey := fmt.Sprintf("cics:vsam:%s:rrds:%020d", dataset, slot)
	_, err := r.client.Store(swarm.StoreRequest{
		Key:     []byte(swarmKey),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	if err != nil {
		return fmt.Errorf("RRDS write %s slot=%d: %w", dataset, slot, err)
	}
	return nil
}

// Read retrieves a record from the given slot number.
func (r *RRDS) Read(dataset string, slot int64) ([]byte, error) {
	swarmKey := fmt.Sprintf("cics:vsam:%s:rrds:%020d", dataset, slot)
	resp, err := r.client.Fetch(swarm.FetchRequest{
		Key:     []byte(swarmKey),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, fmt.Errorf("RRDS read %s slot=%d: %w", dataset, slot, err)
	}
	if !resp.Found {
		return nil, fmt.Errorf("RRDS slot empty: %s slot=%d", dataset, slot)
	}
	return resp.Value, nil
}

// Delete clears a slot.
func (r *RRDS) Delete(dataset string, slot int64) error {
	swarmKey := fmt.Sprintf("cics:vsam:%s:rrds:%020d", dataset, slot)
	_, err := r.client.Delete(swarm.DeleteRequest{
		Key: []byte(swarmKey),
	})
	if err != nil {
		return fmt.Errorf("RRDS delete %s slot=%d: %w", dataset, slot, err)
	}
	return nil
}

// Exists checks if a slot has data.
func (r *RRDS) Exists(dataset string, slot int64) (bool, error) {
	swarmKey := fmt.Sprintf("cics:vsam:%s:rrds:%020d", dataset, slot)
	resp, err := r.client.Fetch(swarm.FetchRequest{
		Key:     []byte(swarmKey),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return false, err
	}
	return resp.Found, nil
}

// Update modifies the record at the given slot.
func (r *RRDS) Update(dataset string, slot int64, data []byte) error {
	return r.Write(dataset, slot, data)
}
