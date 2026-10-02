// Marabunta - Licensed under the MIT License.
// Package vsam implements Virtual Storage Access Method (VSAM) file access
// backed by the Marabunta Swarm distributed key-value store.
package vsam

import (
	"fmt"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// KSDS implements Key-Sequenced Data Set access.
// Records are stored with direct O(1) lookup by primary key (spec C.2).
// Key format: cics:vsam:{dataset}:ksds:{key}
type KSDS struct {
	client *swarm.SwarmClient
}

// NewKSDS creates a new KSDS accessor.
func NewKSDS(client *swarm.SwarmClient) *KSDS {
	return &KSDS{client: client}
}

// Read retrieves a record by primary key with O(1) lookup.
func (k *KSDS) Read(dataset, key string) ([]byte, error) {
	swarmKey := fmt.Sprintf("cics:vsam:%s:ksds:%s", dataset, key)
	resp, err := k.client.Fetch(swarm.FetchRequest{
		Key:     []byte(swarmKey),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, fmt.Errorf("KSDS read %s/%s: %w", dataset, key, err)
	}
	if !resp.Found {
		return nil, fmt.Errorf("record not found: %s/%s", dataset, key)
	}
	return resp.Value, nil
}

// Write stores a record by primary key.
func (k *KSDS) Write(dataset, key string, data []byte) error {
	swarmKey := fmt.Sprintf("cics:vsam:%s:ksds:%s", dataset, key)
	_, err := k.client.Store(swarm.StoreRequest{
		Key:     []byte(swarmKey),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	if err != nil {
		return fmt.Errorf("KSDS write %s/%s: %w", dataset, key, err)
	}
	return nil
}

// Delete removes a record by primary key.
func (k *KSDS) Delete(dataset, key string) error {
	swarmKey := fmt.Sprintf("cics:vsam:%s:ksds:%s", dataset, key)
	_, err := k.client.Delete(swarm.DeleteRequest{
		Key: []byte(swarmKey),
	})
	if err != nil {
		return fmt.Errorf("KSDS delete %s/%s: %w", dataset, key, err)
	}
	return nil
}

// Exists checks if a record exists.
func (k *KSDS) Exists(dataset, key string) (bool, error) {
	swarmKey := fmt.Sprintf("cics:vsam:%s:ksds:%s", dataset, key)
	resp, err := k.client.Fetch(swarm.FetchRequest{
		Key:     []byte(swarmKey),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return false, err
	}
	return resp.Found, nil
}

// ReadForUpdate reads a record and marks it for update (locks the record).
// In the swarm-backed implementation, we use optimistic concurrency via
// version numbers rather than pessimistic locking.
func (k *KSDS) ReadForUpdate(dataset, key string) ([]byte, uint64, error) {
	swarmKey := fmt.Sprintf("cics:vsam:%s:ksds:%s", dataset, key)
	resp, err := k.client.Fetch(swarm.FetchRequest{
		Key:     []byte(swarmKey),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, 0, fmt.Errorf("KSDS read-for-update %s/%s: %w", dataset, key, err)
	}
	if !resp.Found {
		return nil, 0, fmt.Errorf("record not found: %s/%s", dataset, key)
	}
	return resp.Value, resp.Version, nil
}
