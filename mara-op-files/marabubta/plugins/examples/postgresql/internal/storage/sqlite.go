// Marabunta - Licensed under the MIT License.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// ConnPool manages a pool of SQLite database connections with health checking
// and automatic reconnection. This complements the sql.DB built-in pool with
// additional monitoring and metrics.
type ConnPool struct {
	mu          sync.Mutex
	path        string
	maxConns    int
	db          *sql.DB
	queryCount  int64
	errorCount  int64
	lastError   time.Time
	created     time.Time
}

// NewConnPool creates a new connection pool for the given SQLite database path.
func NewConnPool(path string, maxConns int) (*ConnPool, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_cache_size=-8000", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	// Enable WAL mode for better concurrent read/write performance.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}

	// Set page size for better I/O performance.
	if _, err := db.Exec("PRAGMA page_size=4096"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set page size: %w", err)
	}

	return &ConnPool{
		path:     path,
		maxConns: maxConns,
		db:       db,
		created:  time.Now(),
	}, nil
}

// Query executes a query and returns columns and rows.
func (cp *ConnPool) Query(ctx context.Context, query string, args ...interface{}) ([]string, [][]string, error) {
	cp.mu.Lock()
	cp.queryCount++
	cp.mu.Unlock()

	rows, err := cp.db.QueryContext(ctx, query, args...)
	if err != nil {
		cp.mu.Lock()
		cp.errorCount++
		cp.lastError = time.Now()
		cp.mu.Unlock()
		return nil, nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var result [][]string
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}

		row := make([]string, len(cols))
		for i, v := range values {
			if v == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		result = append(result, row)
	}

	return cols, result, rows.Err()
}

// Exec executes a non-query statement.
func (cp *ConnPool) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	cp.mu.Lock()
	cp.queryCount++
	cp.mu.Unlock()

	result, err := cp.db.ExecContext(ctx, query, args...)
	if err != nil {
		cp.mu.Lock()
		cp.errorCount++
		cp.lastError = time.Now()
		cp.mu.Unlock()
	}
	return result, err
}

// Stats returns connection pool statistics.
func (cp *ConnPool) Stats() ConnPoolStats {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	dbStats := cp.db.Stats()
	return ConnPoolStats{
		Path:          cp.path,
		MaxConns:      cp.maxConns,
		OpenConns:     dbStats.OpenConnections,
		InUse:         dbStats.InUse,
		Idle:          dbStats.Idle,
		QueryCount:    cp.queryCount,
		ErrorCount:    cp.errorCount,
		LastError:     cp.lastError,
		UptimeSeconds: int64(time.Since(cp.created).Seconds()),
	}
}

// ConnPoolStats contains connection pool statistics.
type ConnPoolStats struct {
	Path          string    `json:"path"`
	MaxConns      int       `json:"max_conns"`
	OpenConns     int       `json:"open_conns"`
	InUse         int       `json:"in_use"`
	Idle          int       `json:"idle"`
	QueryCount    int64     `json:"query_count"`
	ErrorCount    int64     `json:"error_count"`
	LastError     time.Time `json:"last_error,omitempty"`
	UptimeSeconds int64     `json:"uptime_seconds"`
}

// Close closes the connection pool.
func (cp *ConnPool) Close() error {
	return cp.db.Close()
}

// HealthCheck verifies the database is responsive.
func (cp *ConnPool) HealthCheck(ctx context.Context) error {
	return cp.db.PingContext(ctx)
}
