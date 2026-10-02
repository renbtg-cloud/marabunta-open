// Marabunta - Licensed under the MIT License.
package vsam

import (
	"encoding/json"
	"fmt"
	"sync"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// AIX implements Alternate Index access for VSAM datasets.
// An alternate index maps secondary key values to primary keys,
// enabling lookup by non-primary attributes.
type AIX struct {
	client *swarm.SwarmClient
	mu     sync.RWMutex
	// Local cache of index entries.
	cache map[string][]string // "dataset:indexName:secondaryKey" -> []primaryKey
}

// NewAIX creates a new AIX accessor.
func NewAIX(client *swarm.SwarmClient) *AIX {
	return &AIX{
		client: client,
		cache:  make(map[string][]string),
	}
}

// indexKey returns the swarm key for an AIX entry.
func indexKey(dataset, indexName, secondaryKey string) string {
	return fmt.Sprintf("cics:vsam:%s:aix:%s:%s", dataset, indexName, secondaryKey)
}

// AddEntry adds a secondary key -> primary key mapping to the index.
func (a *AIX) AddEntry(dataset, indexName, secondaryKey, primaryKey string) error {
	// Read existing entries.
	entries, err := a.getEntries(dataset, indexName, secondaryKey)
	if err != nil {
		entries = nil
	}

	// Check for duplicate.
	for _, pk := range entries {
		if pk == primaryKey {
			return nil // Already indexed.
		}
	}

	entries = append(entries, primaryKey)
	return a.putEntries(dataset, indexName, secondaryKey, entries)
}

// RemoveEntry removes a secondary key -> primary key mapping.
func (a *AIX) RemoveEntry(dataset, indexName, secondaryKey, primaryKey string) error {
	entries, err := a.getEntries(dataset, indexName, secondaryKey)
	if err != nil {
		return nil // Nothing to remove.
	}

	var updated []string
	for _, pk := range entries {
		if pk != primaryKey {
			updated = append(updated, pk)
		}
	}

	if len(updated) == 0 {
		// Delete the index entry entirely.
		key := indexKey(dataset, indexName, secondaryKey)
		if _, err := a.client.Delete(swarm.DeleteRequest{Key: []byte(key)}); err != nil {
			return fmt.Errorf("AIX delete %s: %w", key, err)
		}

		a.mu.Lock()
		delete(a.cache, fmt.Sprintf("%s:%s:%s", dataset, indexName, secondaryKey))
		a.mu.Unlock()
		return nil
	}

	return a.putEntries(dataset, indexName, secondaryKey, updated)
}

// Lookup returns the primary keys associated with a secondary key value.
func (a *AIX) Lookup(dataset, indexName, secondaryKey string) ([]string, error) {
	return a.getEntries(dataset, indexName, secondaryKey)
}

// LookupOne returns the first primary key for a secondary key, or error.
func (a *AIX) LookupOne(dataset, indexName, secondaryKey string) (string, error) {
	entries, err := a.getEntries(dataset, indexName, secondaryKey)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("AIX: no primary key found for %s/%s/%s", dataset, indexName, secondaryKey)
	}
	return entries[0], nil
}

// getEntries retrieves the primary keys for a secondary key.
func (a *AIX) getEntries(dataset, indexName, secondaryKey string) ([]string, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", dataset, indexName, secondaryKey)

	// Check cache.
	a.mu.RLock()
	if cached, ok := a.cache[cacheKey]; ok {
		a.mu.RUnlock()
		return cached, nil
	}
	a.mu.RUnlock()

	// Fetch from swarm.
	key := indexKey(dataset, indexName, secondaryKey)
	resp, err := a.client.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, fmt.Errorf("AIX fetch %s: %w", key, err)
	}
	if !resp.Found {
		return nil, fmt.Errorf("AIX: no entries for %s/%s/%s", dataset, indexName, secondaryKey)
	}

	var entries []string
	if err := json.Unmarshal(resp.Value, &entries); err != nil {
		return nil, fmt.Errorf("AIX unmarshal: %w", err)
	}

	// Cache.
	a.mu.Lock()
	a.cache[cacheKey] = entries
	a.mu.Unlock()

	return entries, nil
}

// putEntries stores the primary keys for a secondary key.
func (a *AIX) putEntries(dataset, indexName, secondaryKey string, entries []string) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("AIX marshal: %w", err)
	}

	key := indexKey(dataset, indexName, secondaryKey)
	_, err = a.client.Store(swarm.StoreRequest{
		Key:     []byte(key),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	if err != nil {
		return fmt.Errorf("AIX store %s: %w", key, err)
	}

	// Update cache.
	cacheKey := fmt.Sprintf("%s:%s:%s", dataset, indexName, secondaryKey)
	a.mu.Lock()
	a.cache[cacheKey] = entries
	a.mu.Unlock()

	return nil
}

// Manager provides unified access to all VSAM dataset types.
type Manager struct {
	ksds *KSDS
	esds *ESDS
	rrds *RRDS
	aix  *AIX
}

// NewManager creates a new VSAM Manager.
func NewManager(client *swarm.SwarmClient) *Manager {
	return &Manager{
		ksds: NewKSDS(client),
		esds: NewESDS(client),
		rrds: NewRRDS(client),
		aix:  NewAIX(client),
	}
}

// KSDS returns the Key-Sequenced Data Set accessor.
func (m *Manager) KSDS() *KSDS { return m.ksds }

// ESDS returns the Entry-Sequenced Data Set accessor.
func (m *Manager) ESDS() *ESDS { return m.esds }

// RRDS returns the Relative Record Data Set accessor.
func (m *Manager) RRDS() *RRDS { return m.rrds }

// AIX returns the Alternate Index accessor.
func (m *Manager) AIX() *AIX { return m.aix }
