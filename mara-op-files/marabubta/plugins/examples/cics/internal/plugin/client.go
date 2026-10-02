// Marabunta - Licensed under the MIT License.
// Package plugin wraps the shared swarmclient for the CICS plugin.
package plugin

import (
	"fmt"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// Client wraps SwarmClient with CICS-specific convenience methods.
type Client struct {
	sc *swarm.SwarmClient
}

// NewClient connects to the swarm host at the given address.
func NewClient(address string) (*Client, error) {
	sc, err := swarm.Connect(address)
	if err != nil {
		return nil, fmt.Errorf("cics plugin connect: %w", err)
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

// VSAM key helpers.

// KSDSKey returns the swarm key for a KSDS record.
func KSDSKey(dataset, key string) string {
	return fmt.Sprintf("cics:vsam:%s:ksds:%s", dataset, key)
}

// ESDSKey returns the swarm key for an ESDS record.
func ESDSKey(dataset string, rba int64) string {
	return fmt.Sprintf("cics:vsam:%s:esds:%020d", dataset, rba)
}

// RRDSKey returns the swarm key for an RRDS record.
func RRDSKey(dataset string, slot int64) string {
	return fmt.Sprintf("cics:vsam:%s:rrds:%020d", dataset, slot)
}

// TSQueueKey returns the swarm key for a TS queue.
func TSQueueKey(queueName string) string {
	return fmt.Sprintf("cics:ts:%s", queueName)
}

// TDQueueKey returns the swarm key for a TD queue.
func TDQueueKey(queueName string) string {
	return fmt.Sprintf("cics:td:%s", queueName)
}
