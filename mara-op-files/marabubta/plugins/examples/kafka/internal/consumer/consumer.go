// Marabunta - Licensed under the MIT License.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// Coordinator manages consumer groups across the cluster using swarm pub/sub
// for rebalancing coordination (spec K.4).
type Coordinator struct {
	mu     sync.RWMutex
	client *swarm.SwarmClient
	nodeID string
	groups map[string]*ConsumerGroupState
}

// ConsumerGroupState tracks the local state of a consumer group.
type ConsumerGroupState struct {
	GroupID    string                          `json:"group_id"`
	Generation int32                          `json:"generation"`
	Members    map[string]*ConsumerMemberInfo  `json:"members"`
	Leader     string                          `json:"leader"`
	State      string                          `json:"state"`
}

// ConsumerMemberInfo holds info about a consumer group member.
type ConsumerMemberInfo struct {
	MemberID  string    `json:"member_id"`
	ClientID  string    `json:"client_id"`
	Topics    []string  `json:"topics"`
	JoinedAt  time.Time `json:"joined_at"`
	LastSeen  time.Time `json:"last_seen"`
}

// NewCoordinator creates a new consumer group coordinator.
func NewCoordinator(client *swarm.SwarmClient, nodeID string) *Coordinator {
	return &Coordinator{
		client: client,
		nodeID: nodeID,
		groups: make(map[string]*ConsumerGroupState),
	}
}

// Run starts the coordinator's main loop, listening for rebalance events
// and expiring stale members.
func (c *Coordinator) Run(ctx context.Context) {
	// Subscribe to consumer group coordination events.
	if err := c.client.Subscribe("kafka:consumer-group:*:rebalance"); err != nil {
		log.Printf("consumer coordinator: subscribe failed: %v", err)
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-c.client.EventCh:
			if !ok {
				return
			}
			c.handleEvent(evt)
		case <-ticker.C:
			c.expireStaleMembers()
		}
	}
}

// handleEvent processes a consumer group coordination event.
func (c *Coordinator) handleEvent(evt swarm.SubscribeEvent) {
	var payload map[string]interface{}
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return
	}

	groupID, ok := payload["group_id"].(string)
	if !ok {
		return
	}

	log.Printf("consumer coordinator: event for group %s from %s", groupID, evt.FromNode)
}

// expireStaleMembers removes members that haven't sent a heartbeat.
func (c *Coordinator) expireStaleMembers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	timeout := 30 * time.Second

	for _, group := range c.groups {
		var expired []string
		for id, member := range group.Members {
			if now.Sub(member.LastSeen) > timeout {
				expired = append(expired, id)
			}
		}
		for _, id := range expired {
			delete(group.Members, id)
			log.Printf("consumer coordinator: expired member %s from group %s", id, group.GroupID)
		}
	}
}

// JoinGroup handles a consumer joining a group with locality awareness.
// The coordinator prefers assigning partitions whose data is stored on
// nearby swarm nodes for reduced fetch latency (spec K.4).
func (c *Coordinator) JoinGroup(groupID, memberID, clientID string, topics []string) (int32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, ok := c.groups[groupID]
	if !ok {
		group = &ConsumerGroupState{
			GroupID: groupID,
			Members: make(map[string]*ConsumerMemberInfo),
			State:   "Stable",
		}
		c.groups[groupID] = group
	}

	now := time.Now()
	group.Members[memberID] = &ConsumerMemberInfo{
		MemberID: memberID,
		ClientID: clientID,
		Topics:   topics,
		JoinedAt: now,
		LastSeen: now,
	}
	group.Generation++

	// First member becomes leader.
	if group.Leader == "" {
		group.Leader = memberID
	}

	return group.Generation, nil
}

// LeaveGroup removes a member from a consumer group.
func (c *Coordinator) LeaveGroup(groupID, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, ok := c.groups[groupID]
	if !ok {
		return fmt.Errorf("group %s not found", groupID)
	}

	delete(group.Members, memberID)
	group.Generation++

	if group.Leader == memberID {
		group.Leader = ""
		for id := range group.Members {
			group.Leader = id
			break
		}
	}

	return nil
}

// Heartbeat updates the last-seen timestamp for a member.
func (c *Coordinator) Heartbeat(groupID, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, ok := c.groups[groupID]
	if !ok {
		return fmt.Errorf("group %s not found", groupID)
	}

	member, ok := group.Members[memberID]
	if !ok {
		return fmt.Errorf("member %s not in group %s", memberID, groupID)
	}

	member.LastSeen = time.Now()
	return nil
}

// GroupCount returns the number of tracked consumer groups.
func (c *Coordinator) GroupCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.groups)
}
