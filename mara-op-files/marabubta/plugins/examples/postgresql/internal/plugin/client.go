// Marabunta - Licensed under the MIT License.
// Package plugin wraps the shared swarmclient for use by the PostgreSQL plugin.
package plugin

import (
	"fmt"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// Client wraps SwarmClient with PostgreSQL-specific convenience methods.
type Client struct {
	sc       *swarm.SwarmClient
	instance string
}

// NewClient connects to the swarm host at the given address.
func NewClient(address string) (*Client, error) {
	sc, err := swarm.Connect(address)
	if err != nil {
		return nil, fmt.Errorf("plugin client connect: %w", err)
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

// Register registers the plugin with the swarm and stores the instance name.
func (c *Client) Register(req swarm.RegisterRequest) (*swarm.RegisterResponse, error) {
	resp, err := c.sc.Register(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// SetInstance configures the instance name used for key formatting.
func (c *Client) SetInstance(instance string) {
	c.instance = instance
}

// StartEventLoop delegates to the underlying SwarmClient.
func (c *Client) StartEventLoop(handler func(msg *swarm.WireMessage) *swarm.WireMessage) {
	c.sc.StartEventLoop(handler)
}

// ShardDataKey returns the swarm storage key for a shard's data.
// Format: pg:{instance}:{table}:shard_{id}:data
func (c *Client) ShardDataKey(instance, table string, shardID int) string {
	return fmt.Sprintf("pg:%s:%s:shard_%d:data", instance, table, shardID)
}

// CatalogKey returns the swarm storage key for the catalog.
// Format: pg:{instance}:catalog
func (c *Client) CatalogKey(instance string) string {
	return fmt.Sprintf("pg:%s:catalog", instance)
}

// StoreShardData stores shard data in the swarm.
func (c *Client) StoreShardData(instance, table string, shardID int, data []byte) error {
	key := c.ShardDataKey(instance, table, shardID)
	_, err := c.sc.Store(swarm.StoreRequest{
		Key:     []byte(key),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	return err
}

// FetchShardData retrieves shard data from the swarm.
func (c *Client) FetchShardData(instance, table string, shardID int) ([]byte, bool, error) {
	key := c.ShardDataKey(instance, table, shardID)
	resp, err := c.sc.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// StoreCatalog stores the catalog data in the swarm.
func (c *Client) StoreCatalog(instance string, data []byte) error {
	key := c.CatalogKey(instance)
	_, err := c.sc.Store(swarm.StoreRequest{
		Key:     []byte(key),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	return err
}

// FetchCatalog retrieves catalog data from the swarm.
func (c *Client) FetchCatalog(instance string) ([]byte, bool, error) {
	key := c.CatalogKey(instance)
	resp, err := c.sc.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// PublishCatalogChange publishes a catalog change event.
func (c *Client) PublishCatalogChange(instance string, payload []byte) error {
	topic := fmt.Sprintf("pg:%s:catalog:changes", instance)
	_, err := c.sc.Publish(swarm.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
	return err
}

// SubscribeCatalogChanges subscribes to catalog change events.
func (c *Client) SubscribeCatalogChanges(instance string) error {
	topic := fmt.Sprintf("pg:%s:catalog:changes", instance)
	return c.sc.Subscribe(topic)
}
