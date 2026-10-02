// Marabunta - Licensed under the MIT License.
package plpgsql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// FunctionCatalog stores registered PL/pgSQL functions and procedures. It is
// safe for concurrent access. Functions are indexed by lowercase name and can
// be overloaded by argument count.
type FunctionCatalog struct {
	mu    sync.RWMutex
	funcs map[string][]*FunctionDef // lowercase name -> overloads
}

// NewFunctionCatalog creates an empty function catalog.
func NewFunctionCatalog() *FunctionCatalog {
	return &FunctionCatalog{
		funcs: make(map[string][]*FunctionDef),
	}
}

// Register adds a function or procedure definition to the catalog. If a
// function with the same name and argument count already exists, it is
// rejected unless the definition has Replace set to true (CREATE OR REPLACE).
func (c *FunctionCatalog) Register(def *FunctionDef) error {
	if def == nil {
		return fmt.Errorf("cannot register nil function definition")
	}
	if def.Name == "" {
		return fmt.Errorf("function name must not be empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	name := normalize(def.Name)
	argCount := len(def.Params)

	overloads := c.funcs[name]
	for i, existing := range overloads {
		if len(existing.Params) == argCount {
			if !def.Replace {
				return fmt.Errorf("function %s with %d arguments already exists (use OR REPLACE to overwrite)", def.Name, argCount)
			}
			// Replace the existing definition.
			overloads[i] = def
			return nil
		}
	}

	c.funcs[name] = append(c.funcs[name], def)
	return nil
}

// Lookup retrieves the first function definition matching the given name.
// Returns nil if no function with that name exists. When multiple overloads
// exist, the first registered overload is returned. For overload-aware
// lookup, use LookupByArity instead.
func (c *FunctionCatalog) Lookup(name string) *FunctionDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	overloads := c.funcs[normalize(name)]
	if len(overloads) == 0 {
		return nil
	}
	return overloads[0]
}

// LookupByArity finds the function/procedure with the given name and argument
// count. If an exact match is not found, it checks for overloads with default
// parameter values that could accept the given count. Returns an error if no
// match is found.
func (c *FunctionCatalog) LookupByArity(name string, argCount int) (*FunctionDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	normalized := normalize(name)
	overloads, ok := c.funcs[normalized]
	if !ok || len(overloads) == 0 {
		return nil, fmt.Errorf("function %q does not exist", name)
	}

	// Exact match on argument count.
	for _, def := range overloads {
		if len(def.Params) == argCount {
			return def, nil
		}
	}

	// Check overloads with defaults that could fill the gap.
	for _, def := range overloads {
		minArgs := 0
		maxArgs := len(def.Params)
		for _, param := range def.Params {
			if param.DefaultExpr == "" {
				minArgs++
			}
		}
		if argCount >= minArgs && argCount <= maxArgs {
			return def, nil
		}
	}

	return nil, fmt.Errorf("function %q with %d arguments does not exist", name, argCount)
}

// MustLookup retrieves a function definition, returning an error if not found.
func (c *FunctionCatalog) MustLookup(name string) (*FunctionDef, error) {
	def := c.Lookup(name)
	if def == nil {
		return nil, fmt.Errorf("function %q does not exist", name)
	}
	return def, nil
}

// Drop removes a function from the catalog. If argTypes is nil, all overloads
// with the given name are removed. Otherwise, only the overload matching the
// argument count (len(argTypes)) is removed.
func (c *FunctionCatalog) Drop(name string, argTypes []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	normalized := normalize(name)
	overloads, ok := c.funcs[normalized]
	if !ok || len(overloads) == 0 {
		return fmt.Errorf("function %q does not exist", name)
	}

	if argTypes == nil {
		// Drop all overloads.
		delete(c.funcs, normalized)
		return nil
	}

	// Drop the overload matching the argument count.
	argCount := len(argTypes)
	found := false
	var remaining []*FunctionDef
	for _, def := range overloads {
		if len(def.Params) == argCount {
			found = true
			continue
		}
		remaining = append(remaining, def)
	}

	if !found {
		return fmt.Errorf("function %q with %d arguments does not exist", name, argCount)
	}

	if len(remaining) == 0 {
		delete(c.funcs, normalized)
	} else {
		c.funcs[normalized] = remaining
	}

	return nil
}

// Remove deletes all overloads of a function by name.
func (c *FunctionCatalog) Remove(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.funcs, normalize(name))
}

// List returns all registered function definitions, sorted by name then
// argument count.
func (c *FunctionCatalog) List() []*FunctionDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var all []*FunctionDef
	for _, overloads := range c.funcs {
		all = append(all, overloads...)
	}

	sort.Slice(all, func(i, j int) bool {
		ni := normalize(all[i].Name)
		nj := normalize(all[j].Name)
		if ni != nj {
			return ni < nj
		}
		return len(all[i].Params) < len(all[j].Params)
	})

	return all
}

// ListNames returns the names of all registered functions (unique, sorted).
func (c *FunctionCatalog) ListNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.funcs))
	for name := range c.funcs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Has returns true if a function with the given name exists (any overload).
func (c *FunctionCatalog) Has(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	overloads := c.funcs[normalize(name)]
	return len(overloads) > 0
}

// --------------------------------------------------------------------------
// Persistence
// --------------------------------------------------------------------------

// persistedFuncDef is a JSON-friendly version of FunctionDef for storage.
// The Body field is not persisted — it is reparsed from BodySource on load.
type persistedFuncDef struct {
	Name         string     `json:"name"`
	Params       []ParamDef `json:"params"`
	ReturnType   string     `json:"return_type"`
	ReturnsSetOf bool       `json:"returns_set_of"`
	Language     string     `json:"language"`
	Volatility   string     `json:"volatility"`
	IsProc       bool       `json:"is_proc"`
	Replace      bool       `json:"replace"`
	BodySource   string     `json:"body_source"`
}

// Persist saves all registered functions to a SQLite database table. The
// schema is created if it does not exist. Existing rows are replaced
// atomically within a transaction.
func (c *FunctionCatalog) Persist(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("database connection is nil")
	}

	// Create table if not exists.
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS function_catalog (
		name TEXT NOT NULL,
		arg_count INTEGER NOT NULL,
		definition TEXT NOT NULL,
		created_at TEXT NOT NULL DEFAULT (datetime('now')),
		PRIMARY KEY (name, arg_count)
	)`)
	if err != nil {
		return fmt.Errorf("create function_catalog table: %w", err)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Clear existing entries and repopulate.
	if _, err := tx.Exec("DELETE FROM function_catalog"); err != nil {
		return fmt.Errorf("clear function_catalog: %w", err)
	}

	stmt, err := tx.Prepare("INSERT INTO function_catalog (name, arg_count, definition) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, overloads := range c.funcs {
		for _, def := range overloads {
			pd := persistedFuncDef{
				Name:         def.Name,
				Params:       def.Params,
				ReturnType:   def.ReturnType,
				ReturnsSetOf: def.ReturnsSetOf,
				Language:     def.Language,
				Volatility:   def.Volatility,
				IsProc:       def.IsProc,
				Replace:      def.Replace,
				BodySource:   def.BodySource,
			}
			defJSON, jsonErr := json.Marshal(pd)
			if jsonErr != nil {
				return fmt.Errorf("marshal function %s: %w", def.Name, jsonErr)
			}
			if _, execErr := stmt.Exec(normalize(def.Name), len(def.Params), string(defJSON)); execErr != nil {
				return fmt.Errorf("insert function %s: %w", def.Name, execErr)
			}
		}
	}

	return tx.Commit()
}

// Load restores the function catalog from a SQLite database table. Existing
// entries in memory are cleared first. Each function body is reparsed from
// its stored source text.
func (c *FunctionCatalog) Load(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("database connection is nil")
	}

	// Check if table exists.
	var tableExists int
	err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='function_catalog'").Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("check function_catalog table: %w", err)
	}
	if tableExists == 0 {
		return nil // No catalog table yet — nothing to load.
	}

	rows, err := db.Query("SELECT name, arg_count, definition FROM function_catalog")
	if err != nil {
		return fmt.Errorf("query function_catalog: %w", err)
	}
	defer rows.Close()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear current catalog.
	c.funcs = make(map[string][]*FunctionDef)

	for rows.Next() {
		var name string
		var argCount int
		var defJSON string

		if scanErr := rows.Scan(&name, &argCount, &defJSON); scanErr != nil {
			return fmt.Errorf("scan function_catalog row: %w", scanErr)
		}

		var pd persistedFuncDef
		if jsonErr := json.Unmarshal([]byte(defJSON), &pd); jsonErr != nil {
			return fmt.Errorf("unmarshal function %s: %w", name, jsonErr)
		}

		def := &FunctionDef{
			Name:         pd.Name,
			Params:       pd.Params,
			ReturnType:   pd.ReturnType,
			ReturnsSetOf: pd.ReturnsSetOf,
			Language:     pd.Language,
			Volatility:   pd.Volatility,
			IsProc:       pd.IsProc,
			Replace:      pd.Replace,
			BodySource:   pd.BodySource,
		}

		// Reparse the body from source.
		if pd.BodySource != "" {
			tokens, tokErr := Tokenize(pd.BodySource)
			if tokErr == nil {
				cleanTokens := FilterTokens(tokens, TokCOMMENT, TokNEWLINE)
				bodyParser := NewParser(cleanTokens)
				def.Body = bodyParser.parseBlock()
				// Ignore parse errors on load — function is still cataloged.
			}
		}

		normalized := normalize(def.Name)
		c.funcs[normalized] = append(c.funcs[normalized], def)
	}

	return rows.Err()
}

// normalize lowercases and trims a function name for case-insensitive lookup.
func normalize(name string) string {
	out := make([]byte, len(name))
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if ch >= 'A' && ch <= 'Z' {
			out[i] = ch + 32
		} else {
			out[i] = ch
		}
	}
	// Trim leading/trailing whitespace.
	start := 0
	end := len(out)
	for start < end && (out[start] == ' ' || out[start] == '\t') {
		start++
	}
	for end > start && (out[end-1] == ' ' || out[end-1] == '\t') {
		end--
	}
	return string(out[start:end])
}
