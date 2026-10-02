// Marabunta - Licensed under the MIT License.
package catalog

import (
	"context"
	"encoding/json"
	"log"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// CatalogChangeEvent describes a catalog change received via Pub/Sub.
type CatalogChangeEvent struct {
	Action   string `json:"action"`
	Table    string `json:"table"`
	DDL      string `json:"ddl"`
	Instance string `json:"instance"`
}

// StartSync begins listening for catalog change events from other instances.
// It subscribes to the catalog changes topic and applies incoming changes.
// The goroutine exits when ctx is cancelled.
func (c *Catalog) StartSync(ctx context.Context) {
	topic := "pg:" + c.instance + ":catalog:changes"

	if err := c.client.Subscribe(topic); err != nil {
		log.Printf("catalog sync: subscribe failed: %v", err)
		return
	}

	log.Printf("catalog sync: subscribed to %s", topic)

	// Periodic full sync as a safety net.
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("catalog sync: shutting down")
			return

		case evt, ok := <-c.client.EventCh:
			if !ok {
				log.Println("catalog sync: event channel closed")
				return
			}
			c.handleChangeEvent(evt)

		case <-ticker.C:
			c.periodicSync()
		}
	}
}

// handleChangeEvent processes a single catalog change event.
func (c *Catalog) handleChangeEvent(evt swarm.SubscribeEvent) {
	var change CatalogChangeEvent
	if err := json.Unmarshal(evt.Payload, &change); err != nil {
		log.Printf("catalog sync: unmarshal event: %v", err)
		return
	}

	// Skip events from other instances (different namespace).
	if change.Instance != c.instance {
		return
	}

	log.Printf("catalog sync: received %s on table %s from node %s",
		change.Action, change.Table, evt.FromNode)

	switch change.Action {
	case "create_table":
		c.RegisterTable(change.Table, change.DDL)
	case "drop_table":
		c.UnregisterTable(change.Table)
	case "create_index":
		c.RegisterIndex(change.Table, change.DDL)
	case "alter_table":
		// For ALTER TABLE, re-register with new DDL.
		c.RegisterTable(change.Table, change.DDL)
	default:
		log.Printf("catalog sync: unknown action %q", change.Action)
	}
}

// periodicSync performs a full catalog reload from the swarm as a safety net
// to catch any missed events.
func (c *Catalog) periodicSync() {
	if err := c.Load(); err != nil {
		log.Printf("catalog sync: periodic load failed: %v", err)
	}
}

// PublishTableCreated publishes a table creation event.
func (c *Catalog) PublishTableCreated(table, ddl string) {
	c.publishChange("create_table", table, ddl)
}

// PublishTableDropped publishes a table drop event.
func (c *Catalog) PublishTableDropped(table string) {
	c.publishChange("drop_table", table, "")
}

// PublishIndexCreated publishes an index creation event.
func (c *Catalog) PublishIndexCreated(table, ddl string) {
	c.publishChange("create_index", table, ddl)
}

// PublishTableAltered publishes a table alteration event.
func (c *Catalog) PublishTableAltered(table, ddl string) {
	c.publishChange("alter_table", table, ddl)
}
