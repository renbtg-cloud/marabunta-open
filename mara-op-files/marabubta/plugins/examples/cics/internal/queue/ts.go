// Marabunta - Licensed under the MIT License.
// Package queue implements CICS Temporary Storage (TS) and Transient Data (TD)
// queues backed by the Marabunta Swarm.
package queue

import (
	"encoding/json"
	"fmt"
	"sync"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// TSManager manages Temporary Storage queues.
// TS queues are named collections of items that persist until explicitly
// deleted or the region restarts. They are implemented via swarm pub/sub
// for cross-instance visibility (spec C.5).
type TSManager struct {
	client *swarm.SwarmClient
	mu     sync.RWMutex
	queues map[string]*TSQueue
}

// TSQueue represents a single TS queue.
type TSQueue struct {
	Name  string   `json:"name"`
	Items [][]byte `json:"items"`
}

// NewTSManager creates a new TS queue manager.
func NewTSManager(client *swarm.SwarmClient) *TSManager {
	return &TSManager{
		client: client,
		queues: make(map[string]*TSQueue),
	}
}

// Write appends an item to a TS queue.
func (m *TSManager) Write(queueName string, data []byte) error {
	m.mu.Lock()
	q, ok := m.queues[queueName]
	if !ok {
		q = &TSQueue{Name: queueName}
		m.queues[queueName] = q
	}
	q.Items = append(q.Items, data)
	m.mu.Unlock()

	// Persist to swarm.
	if err := m.persist(queueName); err != nil {
		return err
	}
	m.publishChange(queueName, "write")
	return nil
}

// Read retrieves an item from a TS queue by item number (1-based).
func (m *TSManager) Read(queueName string, itemNum int) ([]byte, error) {
	m.mu.RLock()
	q, ok := m.queues[queueName]
	m.mu.RUnlock()

	if !ok {
		// Try loading from swarm.
		if err := m.load(queueName); err != nil {
			return nil, fmt.Errorf("TS queue %s not found", queueName)
		}
		m.mu.RLock()
		q = m.queues[queueName]
		m.mu.RUnlock()
		if q == nil {
			return nil, fmt.Errorf("TS queue %s not found", queueName)
		}
	}

	if itemNum < 1 || itemNum > len(q.Items) {
		return nil, fmt.Errorf("TS queue %s: item %d out of range (1-%d)",
			queueName, itemNum, len(q.Items))
	}

	return q.Items[itemNum-1], nil
}

// ReadAll returns all items in a TS queue.
func (m *TSManager) ReadAll(queueName string) ([][]byte, error) {
	m.mu.RLock()
	q, ok := m.queues[queueName]
	m.mu.RUnlock()

	if !ok {
		if err := m.load(queueName); err != nil {
			return nil, fmt.Errorf("TS queue %s not found", queueName)
		}
		m.mu.RLock()
		q = m.queues[queueName]
		m.mu.RUnlock()
	}

	if q == nil {
		return nil, nil
	}

	items := make([][]byte, len(q.Items))
	copy(items, q.Items)
	return items, nil
}

// Rewrite replaces an item in a TS queue at the given item number.
func (m *TSManager) Rewrite(queueName string, itemNum int, data []byte) error {
	m.mu.Lock()
	q, ok := m.queues[queueName]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("TS queue %s not found", queueName)
	}
	if itemNum < 1 || itemNum > len(q.Items) {
		m.mu.Unlock()
		return fmt.Errorf("TS queue %s: item %d out of range", queueName, itemNum)
	}
	q.Items[itemNum-1] = data
	m.mu.Unlock()

	if err := m.persist(queueName); err != nil {
		return err
	}
	m.publishChange(queueName, "rewrite")
	return nil
}

// Delete removes a TS queue entirely.
func (m *TSManager) Delete(queueName string) error {
	m.mu.Lock()
	delete(m.queues, queueName)
	m.mu.Unlock()

	key := fmt.Sprintf("cics:ts:%s", queueName)
	_, err := m.client.Delete(swarm.DeleteRequest{
		Key: []byte(key),
	})
	if err != nil {
		return err
	}
	m.publishChange(queueName, "delete")
	return nil
}

// NumItems returns the number of items in a TS queue.
func (m *TSManager) NumItems(queueName string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[queueName]
	if !ok {
		return 0
	}
	return len(q.Items)
}

// persist saves a TS queue to the swarm.
func (m *TSManager) persist(queueName string) error {
	m.mu.RLock()
	q := m.queues[queueName]
	m.mu.RUnlock()

	if q == nil {
		return nil
	}

	data, err := json.Marshal(q)
	if err != nil {
		return fmt.Errorf("marshal TS queue: %w", err)
	}

	key := fmt.Sprintf("cics:ts:%s", queueName)
	_, err = m.client.Store(swarm.StoreRequest{
		Key:     []byte(key),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	return err
}

// load retrieves a TS queue from the swarm.
func (m *TSManager) load(queueName string) error {
	key := fmt.Sprintf("cics:ts:%s", queueName)
	resp, err := m.client.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return err
	}
	if !resp.Found {
		return fmt.Errorf("not found")
	}

	var q TSQueue
	if err := json.Unmarshal(resp.Value, &q); err != nil {
		return err
	}

	m.mu.Lock()
	m.queues[queueName] = &q
	m.mu.Unlock()

	return nil
}

// publishChange publishes a TS queue change via swarm pub/sub (spec C.5).
func (m *TSManager) publishChange(queueName, action string) {
	payload, _ := json.Marshal(map[string]string{
		"queue":  queueName,
		"action": action,
	})
	m.client.Publish(swarm.PublishRequest{
		Topic:   "cics:ts:changes",
		Payload: payload,
	})
}
