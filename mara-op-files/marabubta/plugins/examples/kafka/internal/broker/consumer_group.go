// Marabunta - Licensed under the MIT License.
package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// ConsumerGroup tracks consumer group membership and partition assignments.
// Rebalancing is coordinated via swarm pub/sub.
type ConsumerGroup struct {
	mu          sync.RWMutex
	groupID     string
	generation  int32
	members     map[string]*GroupMember // memberID -> member
	assignments map[string][]TopicPartition // memberID -> assigned partitions
	client      *swarm.SwarmClient
}

// GroupMember represents a member of a consumer group.
type GroupMember struct {
	MemberID  string    `json:"member_id"`
	ClientID  string    `json:"client_id"`
	Host      string    `json:"host"`
	JoinedAt  time.Time `json:"joined_at"`
	LastSeen  time.Time `json:"last_seen"`
	Topics    []string  `json:"topics"`
}

// TopicPartition identifies a specific topic-partition.
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// GroupState represents the state of a consumer group.
type GroupState string

const (
	GroupStateEmpty             GroupState = "Empty"
	GroupStatePreparingRebalance GroupState = "PreparingRebalance"
	GroupStateCompletingRebalance GroupState = "CompletingRebalance"
	GroupStateStable            GroupState = "Stable"
	GroupStateDead              GroupState = "Dead"
)

// ConsumerGroupManager manages multiple consumer groups.
type ConsumerGroupManager struct {
	mu     sync.RWMutex
	groups map[string]*ConsumerGroup
	client *swarm.SwarmClient
	nodeID string
}

// NewConsumerGroupManager creates a new manager.
func NewConsumerGroupManager(client *swarm.SwarmClient, nodeID string) *ConsumerGroupManager {
	return &ConsumerGroupManager{
		groups: make(map[string]*ConsumerGroup),
		client: client,
		nodeID: nodeID,
	}
}

// GetOrCreateGroup returns an existing group or creates a new one.
func (m *ConsumerGroupManager) GetOrCreateGroup(groupID string) *ConsumerGroup {
	m.mu.RLock()
	g, ok := m.groups[groupID]
	m.mu.RUnlock()
	if ok {
		return g
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	g, ok = m.groups[groupID]
	if ok {
		return g
	}

	g = &ConsumerGroup{
		groupID:     groupID,
		members:     make(map[string]*GroupMember),
		assignments: make(map[string][]TopicPartition),
		client:      m.client,
	}
	m.groups[groupID] = g
	return g
}

// JoinGroup adds a member to the group and triggers rebalancing.
func (g *ConsumerGroup) JoinGroup(memberID, clientID, host string, topics []string) (int32, error) {
	var generation int32
	var rebalancePayload []byte

	func() {
		g.mu.Lock()
		defer g.mu.Unlock()

		now := time.Now()
		member := &GroupMember{
			MemberID: memberID,
			ClientID: clientID,
			Host:     host,
			JoinedAt: now,
			LastSeen: now,
			Topics:   topics,
		}
		g.members[memberID] = member
		g.generation++
		generation = g.generation

		log.Printf("consumer group %s: member %s joined (gen %d, members: %d)",
			g.groupID, memberID, g.generation, len(g.members))

		rebalancePayload = g.buildRebalancePayload()
	}()

	// Publish outside the lock to avoid holding the group lock during network I/O.
	if rebalancePayload != nil {
		g.doPublishRebalance(rebalancePayload)
	}

	return generation, nil
}

// LeaveGroup removes a member from the group.
func (g *ConsumerGroup) LeaveGroup(memberID string) {
	var rebalancePayload []byte

	func() {
		g.mu.Lock()
		defer g.mu.Unlock()

		delete(g.members, memberID)
		delete(g.assignments, memberID)
		g.generation++

		log.Printf("consumer group %s: member %s left (gen %d, members: %d)",
			g.groupID, memberID, g.generation, len(g.members))

		rebalancePayload = g.buildRebalancePayload()
	}()

	if rebalancePayload != nil {
		g.doPublishRebalance(rebalancePayload)
	}
}

// Heartbeat updates the last-seen time for a member.
func (g *ConsumerGroup) Heartbeat(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	member, ok := g.members[memberID]
	if !ok {
		return fmt.Errorf("member %s not in group %s", memberID, g.groupID)
	}
	member.LastSeen = time.Now()
	return nil
}

// SyncGroup assigns partitions to a member (leader sends assignments).
func (g *ConsumerGroup) SyncGroup(memberID string, assignments map[string][]TopicPartition) ([]TopicPartition, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// If assignments provided (from leader), apply them.
	if len(assignments) > 0 {
		g.assignments = assignments
	}

	// Return this member's assignment.
	return g.assignments[memberID], nil
}

// Rebalance performs a range-based partition assignment across all members.
func (g *ConsumerGroup) Rebalance(topicPartitions map[string]int32) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.members) == 0 {
		g.assignments = make(map[string][]TopicPartition)
		return
	}

	// Collect all partitions.
	var allPartitions []TopicPartition
	for topic, count := range topicPartitions {
		for i := int32(0); i < count; i++ {
			allPartitions = append(allPartitions, TopicPartition{Topic: topic, Partition: i})
		}
	}

	// Range assignment: distribute partitions evenly.
	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}

	newAssignments := make(map[string][]TopicPartition)
	for i, tp := range allPartitions {
		memberIdx := i % len(memberIDs)
		member := memberIDs[memberIdx]
		newAssignments[member] = append(newAssignments[member], tp)
	}

	g.assignments = newAssignments
	g.generation++
}

// buildRebalancePayload builds the JSON payload for a rebalance event.
// Must be called with g.mu held.
func (g *ConsumerGroup) buildRebalancePayload() []byte {
	payload, err := json.Marshal(map[string]interface{}{
		"group_id":   g.groupID,
		"generation": g.generation,
		"members":    len(g.members),
	})
	if err != nil {
		return nil
	}
	return payload
}

// doPublishRebalance publishes a rebalance event via swarm pub/sub.
// Must be called WITHOUT g.mu held to avoid holding the lock during I/O.
func (g *ConsumerGroup) doPublishRebalance(payload []byte) {
	topic := fmt.Sprintf("kafka:consumer-group:%s:rebalance", g.groupID)
	if _, err := g.client.Publish(swarm.PublishRequest{
		Topic:   topic,
		Payload: payload,
	}); err != nil {
		log.Printf("consumer group %s: publish rebalance failed: %v", g.groupID, err)
	}
}

// MemberCount returns the number of members.
func (g *ConsumerGroup) MemberCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.members)
}

// ExpireMembers removes members that haven't sent a heartbeat within the timeout.
func (g *ConsumerGroup) ExpireMembers(timeout time.Duration) int {
	var rebalancePayload []byte
	var expiredCount int

	func() {
		g.mu.Lock()
		defer g.mu.Unlock()

		now := time.Now()
		var expired []string
		for id, member := range g.members {
			if now.Sub(member.LastSeen) > timeout {
				expired = append(expired, id)
			}
		}

		for _, id := range expired {
			delete(g.members, id)
			delete(g.assignments, id)
			log.Printf("consumer group %s: expired member %s", g.groupID, id)
		}

		if len(expired) > 0 {
			g.generation++
			rebalancePayload = g.buildRebalancePayload()
		}

		expiredCount = len(expired)
	}()

	if rebalancePayload != nil {
		g.doPublishRebalance(rebalancePayload)
	}

	return expiredCount
}
