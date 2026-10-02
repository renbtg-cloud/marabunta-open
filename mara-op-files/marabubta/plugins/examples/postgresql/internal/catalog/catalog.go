// Marabunta - Licensed under the MIT License.
// Package catalog tracks schema, tables, indexes, and shard key mappings
// for the distributed PostgreSQL plugin. It persists to the swarm via
// Store/Fetch and synchronizes across instances via Pub/Sub.
package catalog

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// TableInfo describes a table in the catalog.
type TableInfo struct {
	Name       string   `json:"name"`
	DDL        string   `json:"ddl"`
	ShardKey   string   `json:"shard_key"`
	Columns    []string `json:"columns"`
	Indexes    []string `json:"indexes"`
	RowEstimate int64   `json:"row_estimate"`
}

// CatalogData is the persisted catalog state.
type CatalogData struct {
	Instance string               `json:"instance"`
	Tables   map[string]*TableInfo `json:"tables"`
	Version  int64                `json:"version"`
}

// Catalog provides thread-safe access to the schema catalog.
type Catalog struct {
	mu       sync.RWMutex
	instance string
	client   *swarm.SwarmClient
	data     CatalogData
}

// NewCatalog creates a new Catalog.
func NewCatalog(instance string, client *swarm.SwarmClient) *Catalog {
	return &Catalog{
		instance: instance,
		client:   client,
		data: CatalogData{
			Instance: instance,
			Tables:   make(map[string]*TableInfo),
		},
	}
}

// Load fetches the catalog from the swarm.
func (c *Catalog) Load() error {
	key := fmt.Sprintf("pg:%s:catalog", c.instance)
	resp, err := c.client.Fetch(swarm.FetchRequest{
		Key:     []byte(key),
		Options: swarm.DefaultFetchOptions(),
	})
	if err != nil {
		return fmt.Errorf("fetch catalog: %w", err)
	}
	if !resp.Found {
		return nil // No existing catalog — start fresh.
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := json.Unmarshal(resp.Value, &c.data); err != nil {
		return fmt.Errorf("unmarshal catalog: %w", err)
	}
	return nil
}

// Save persists the catalog to the swarm.
func (c *Catalog) Save() error {
	c.mu.RLock()
	data, err := json.Marshal(c.data)
	c.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("marshal catalog: %w", err)
	}

	key := fmt.Sprintf("pg:%s:catalog", c.instance)
	_, err = c.client.Store(swarm.StoreRequest{
		Key:     []byte(key),
		Value:   data,
		Options: swarm.DefaultStoreOptions(),
	})
	return err
}

// RegisterTable adds or updates a table in the catalog.
func (c *Catalog) RegisterTable(name, ddl string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := &TableInfo{
		Name: name,
		DDL:  ddl,
	}

	// Extract column names from DDL (simplified).
	info.Columns = extractColumns(ddl)

	// Use first column as shard key by default.
	if len(info.Columns) > 0 {
		info.ShardKey = info.Columns[0]
	}

	c.data.Tables[name] = info
	c.data.Version++

	go c.publishChange("create_table", name, ddl)
}

// UnregisterTable removes a table from the catalog.
func (c *Catalog) UnregisterTable(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data.Tables, name)
	c.data.Version++

	go c.publishChange("drop_table", name, "")
}

// RegisterIndex adds an index DDL to the table's catalog entry.
func (c *Catalog) RegisterIndex(table, ddl string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, ok := c.data.Tables[table]
	if !ok {
		info = &TableInfo{Name: table}
		c.data.Tables[table] = info
	}
	info.Indexes = append(info.Indexes, ddl)
	c.data.Version++

	go c.publishChange("create_index", table, ddl)
}

// GetShardKey returns the shard key column for a table, or empty string.
func (c *Catalog) GetShardKey(table string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.data.Tables[table]
	if !ok {
		return ""
	}
	return info.ShardKey
}

// SetShardKey sets the shard key column for a table.
func (c *Catalog) SetShardKey(table, column string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, ok := c.data.Tables[table]
	if !ok {
		info = &TableInfo{Name: table}
		c.data.Tables[table] = info
	}
	info.ShardKey = column
}

// GetColumnIndex returns the zero-based index of a column in a table,
// or -1 if not found.
func (c *Catalog) GetColumnIndex(table, column string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.data.Tables[table]
	if !ok {
		return -1
	}
	for i, col := range info.Columns {
		if col == column {
			return i
		}
	}
	return -1
}

// GetTableRowEstimate returns the estimated row count for a table.
func (c *Catalog) GetTableRowEstimate(table string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.data.Tables[table]
	if !ok {
		return 0
	}
	return info.RowEstimate
}

// UpdateRowEstimate updates the estimated row count for a table.
func (c *Catalog) UpdateRowEstimate(table string, estimate int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, ok := c.data.Tables[table]
	if !ok {
		return
	}
	info.RowEstimate = estimate
}

// GetTable returns the TableInfo for a table, or nil.
func (c *Catalog) GetTable(table string) *TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.data.Tables[table]
	if !ok {
		return nil
	}
	// Return a copy to avoid data races.
	copy := *info
	copy.Columns = append([]string{}, info.Columns...)
	copy.Indexes = append([]string{}, info.Indexes...)
	return &copy
}

// ListTables returns the names of all tables in the catalog.
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var tables []string
	for name := range c.data.Tables {
		tables = append(tables, name)
	}
	return tables
}

// Version returns the current catalog version.
func (c *Catalog) Version() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.data.Version
}

// extractColumns extracts column names from a CREATE TABLE DDL statement.
// This is a simplified parser — production would use pg_query_go.
func extractColumns(ddl string) []string {
	var columns []string

	// Find content between first ( and last ).
	start := -1
	end := -1
	for i, ch := range ddl {
		if ch == '(' && start < 0 {
			start = i + 1
		}
		if ch == ')' {
			end = i
		}
	}
	if start < 0 || end < 0 || end <= start {
		return nil
	}

	// Split by commas, extract first word of each as column name.
	content := ddl[start:end]
	parts := splitByComma(content)
	for _, part := range parts {
		part = trimSpace(part)
		if part == "" {
			continue
		}
		// Skip constraints (PRIMARY KEY, FOREIGN KEY, CHECK, UNIQUE, CONSTRAINT).
		upper := toUpper(part)
		if hasPrefix(upper, "PRIMARY KEY") || hasPrefix(upper, "FOREIGN KEY") ||
			hasPrefix(upper, "CHECK") || hasPrefix(upper, "UNIQUE") ||
			hasPrefix(upper, "CONSTRAINT") {
			continue
		}
		// First word is the column name.
		fields := splitFields(part)
		if len(fields) > 0 {
			columns = append(columns, fields[0])
		}
	}
	return columns
}

// publishChange publishes a catalog change event to the swarm.
func (c *Catalog) publishChange(action, table, ddl string) {
	payload, err := json.Marshal(map[string]string{
		"action":   action,
		"table":    table,
		"ddl":      ddl,
		"instance": c.instance,
	})
	if err != nil {
		log.Printf("catalog: marshal change: %v", err)
		return
	}

	topic := fmt.Sprintf("pg:%s:catalog:changes", c.instance)
	_, err = c.client.Publish(swarm.PublishRequest{
		Topic:   topic,
		Payload: payload,
	})
	if err != nil {
		log.Printf("catalog: publish change: %v", err)
	}
}

// Helper functions to avoid importing strings package for simple operations.
// These are intentionally minimal to keep the file self-contained.

func splitByComma(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

func toUpper(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		} else {
			b[i] = c
		}
	}
	return string(b)
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func splitFields(s string) []string {
	var fields []string
	start := -1
	for i := 0; i < len(s); i++ {
		isSpace := s[i] == ' ' || s[i] == '\t' || s[i] == '\n'
		if !isSpace && start < 0 {
			start = i
		} else if isSpace && start >= 0 {
			fields = append(fields, s[start:i])
			start = -1
		}
	}
	if start >= 0 {
		fields = append(fields, s[start:])
	}
	return fields
}
