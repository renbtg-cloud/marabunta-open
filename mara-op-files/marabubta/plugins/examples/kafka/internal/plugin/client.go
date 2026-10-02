// Marabunta - Licensed under the MIT License.
// Package plugin wraps the shared swarmclient for the Kafka plugin.
package plugin

import (
	"fmt"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// Client wraps SwarmClient with Kafka-specific convenience methods.
type Client struct {
	sc *swarm.SwarmClient
}

// NewClient connects to the swarm host at the given address.
func NewClient(address string) (*Client, error) {
	sc, err := swarm.Connect(address)
	if err != nil {
		return nil, fmt.Errorf("kafka plugin connect: %w", err)
	}
	return &Client{sc: sc}, nil
}

// Close shuts down the underlying swarm connection.
func (c *Client) Close() error {
	return c.sc.Close()
}

// SwarmClient returns the underlying SwarmClient for direct access.
func (c *Client) SwarmClient() *swarm.SwarmClient {
	return c.sc
}

// Register registers the plugin with the swarm.
func (c *Client) Register(req swarm.RegisterRequest) (*swarm.RegisterResponse, error) {
	return c.sc.Register(req)
}

// StartEventLoop delegates to the underlying SwarmClient.
func (c *Client) StartEventLoop(handler func(msg *swarm.WireMessage) *swarm.WireMessage) {
	c.sc.StartEventLoop(handler)
}

// RecordKey returns the swarm storage key for a Kafka record.
// Format: kafka:{topic}:{partition}:{offset:020d}
func RecordKey(topic string, partition int, offset int64) string {
	return fmt.Sprintf("kafka:%s:%d:%020d", topic, partition, offset)
}

// TopicMetaKey returns the swarm storage key for topic metadata.
// Format: kafka:{topic}:meta
func TopicMetaKey(topic string) string {
	return fmt.Sprintf("kafka:%s:meta", topic)
}

// ConsumerOffsetKey returns the swarm storage key for consumer group offsets.
// Format: kafka:{topic}:{group}:offsets
func ConsumerOffsetKey(topic, group string) string {
	return fmt.Sprintf("kafka:%s:%s:offsets", topic, group)
}
