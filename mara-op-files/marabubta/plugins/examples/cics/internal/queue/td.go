// Marabunta - Licensed under the MIT License.
package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// TDManager manages Transient Data queues.
// TD queues can have trigger handlers that fire when items are written.
type TDManager struct {
	client   *swarm.SwarmClient
	mu       sync.RWMutex
	queues   map[string]*TDQueue
	triggers map[string]TDTrigger
}

// TDQueue represents a single TD queue.
type TDQueue struct {
	Name  string   `json:"name"`
	Items [][]byte `json:"items"`
	Type  string   `json:"type"` // "intrapartition" or "extrapartition"
}

// TDTrigger defines a trigger handler for a TD queue.
type TDTrigger struct {
	QueueName   string
	TriggerLevel int    // Fire when this many items are queued
	TransID     string // Transaction to start
	TermID      string // Terminal ID for the transaction
}

// NewTDManager creates a new TD queue manager.
func NewTDManager(client *swarm.SwarmClient) *TDManager {
	return &TDManager{
		client:   client,
		queues:   make(map[string]*TDQueue),
		triggers: make(map[string]TDTrigger),
	}
}

// Write appends an item to a TD queue. If a trigger is defined and the
// trigger level is reached, the associated transaction is started.
func (m *TDManager) Write(queueName string, data []byte) error {
	m.mu.Lock()
	q, ok := m.queues[queueName]
	if !ok {
		q = &TDQueue{
			Name: queueName,
			Type: "intrapartition",
		}
		m.queues[queueName] = q
	}
	q.Items = append(q.Items, data)
	currentLen := len(q.Items)
	m.mu.Unlock()

	// Check trigger.
	m.mu.RLock()
	trigger, hasTrigger := m.triggers[queueName]
	m.mu.RUnlock()

	if hasTrigger && currentLen >= trigger.TriggerLevel {
		m.fireTrigger(trigger)
	}

	// Persist to swarm.
	return m.persist(queueName)
}

// Read retrieves and removes the first item from a TD queue (destructive read).
func (m *TDManager) Read(queueName string) ([]byte, error) {
	m.mu.Lock()
	q, ok := m.queues[queueName]
	if !ok {
		m.mu.Unlock()
		// Try loading from swarm.
		if err := m.load(queueName); err != nil {
			return nil, fmt.Errorf("TD queue %s not found", queueName)
		}
		m.mu.Lock()
		q = m.queues[queueName]
	}

	if q == nil || len(q.Items) == 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("TD queue %s is empty", queueName)
	}

	// Destructive read: remove first item.
	item := q.Items[0]
	q.Items = q.Items[1:]
	m.mu.Unlock()

	// Persist the updated queue.
	if err := m.persist(queueName); err != nil {
		return item, fmt.Errorf("TD persist after read: %w", err)
	}
	return item, nil
}

// Peek returns the first item without removing it.
func (m *TDManager) Peek(queueName string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	q, ok := m.queues[queueName]
	if !ok || len(q.Items) == 0 {
		return nil, fmt.Errorf("TD queue %s is empty or not found", queueName)
	}
	return q.Items[0], nil
}

// Delete removes a TD queue entirely.
func (m *TDManager) Delete(queueName string) error {
	m.mu.Lock()
	delete(m.queues, queueName)
	m.mu.Unlock()

	key := fmt.Sprintf("cics:td:%s", queueName)
	_, err := m.client.Delete(swarm.DeleteRequest{
		Key: []byte(key),
	})
	return err
}

// DefineTrigger sets up a trigger for a TD queue.
func (m *TDManager) DefineTrigger(trigger TDTrigger) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.triggers[trigger.QueueName] = trigger
}

// RemoveTrigger removes a trigger from a TD queue.
func (m *TDManager) RemoveTrigger(queueName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.triggers, queueName)
}

// NumItems returns the number of items in a TD queue.
func (m *TDManager) NumItems(queueName string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[queueName]
	if !ok {
		return 0
	}
	return len(q.Items)
}

// fireTrigger initiates the triggered transaction via swarm pub/sub.
func (m *TDManager) fireTrigger(trigger TDTrigger) {
	log.Printf("CICS TD trigger: queue=%s level=%d trans=%s",
		trigger.QueueName, trigger.TriggerLevel, trigger.TransID)

	payload, _ := json.Marshal(map[string]string{
		"queue":    trigger.QueueName,
		"trans_id": trigger.TransID,
		"term_id":  trigger.TermID,
		"action":   "trigger",
	})
	m.client.Publish(swarm.PublishRequest{
		Topic:   "cics:td:triggers",
		Payload: payload,
	})
}

// persist saves a TD queue to the swarm.
func (m *TDManager) persist(queueName string) error {
	m.mu.RLock()
	q := m.queues[queueName]
	m.mu.RUnlock()

	if q == nil {
		return nil
	}

	data, err := json.Marshal(q)
	if err != nil {
		return fmt.Errorf("marshal TD queue: %w", err)
	}

	key := fmt.Sprintf("cics:td:%s", queueName)
	_, err = m.client.Store(swarm.StoreRequest{
		Key:     []byte(key),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	return err
}

// load retrieves a TD queue from the swarm.
func (m *TDManager) load(queueName string) error {
	key := fmt.Sprintf("cics:td:%s", queueName)
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

	var q TDQueue
	if err := json.Unmarshal(resp.Value, &q); err != nil {
		return err
	}

	m.mu.Lock()
	m.queues[queueName] = &q
	m.mu.Unlock()
	return nil
}
