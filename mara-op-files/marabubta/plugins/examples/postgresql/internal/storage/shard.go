// Marabunta - Licensed under the MIT License.
// Package storage manages SQLite-backed shards for the distributed PostgreSQL plugin.
// Each shard is a separate SQLite database file. The ShardManager handles
// creation, opening, closing, and connection pooling (P.12.9).
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

// WriteHookFunc is called after each successful write operation (Exec, ExecReturn)
// with the SQL statement that was executed. This is used by the replication layer
// to capture writes for WAL shipping.
type WriteHookFunc func(table string, shardID int, sql string)

// ShardManager manages a collection of SQLite shard databases.
type ShardManager struct {
	dataDir    string
	shardCount int
	mu         sync.RWMutex
	pools      map[string]*SQLitePool // key: "table:shardID"
	writeHook  WriteHookFunc
}

// NewShardManager creates a new ShardManager with the given data directory
// and shard count. It creates the data directory if it does not exist.
func NewShardManager(dataDir string, shardCount int) (*ShardManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	return &ShardManager{
		dataDir:    dataDir,
		shardCount: shardCount,
		pools:      make(map[string]*SQLitePool),
	}, nil
}

// ShardCount returns the total number of shards.
func (sm *ShardManager) ShardCount() int {
	return sm.shardCount
}

// SetWriteHook registers a callback that is invoked after each successful
// write operation (Exec, ExecReturn, BatchExec) with the SQL statement and
// shard coordinates. This is used by the replication layer to capture writes
// for WAL shipping to replicas.
func (sm *ShardManager) SetWriteHook(hook WriteHookFunc) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.writeHook = hook
}

// GetShardPath returns the filesystem path for a shard's SQLite database.
func (sm *ShardManager) GetShardPath(table string, shardID int) string {
	return sm.shardPath(table, shardID)
}

// Close closes all open shard connections.
func (sm *ShardManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, pool := range sm.pools {
		pool.Close()
	}
	sm.pools = make(map[string]*SQLitePool)
}

// getPool returns (or creates) a connection pool for the given table and shard.
func (sm *ShardManager) getPool(table string, shardID int) (*SQLitePool, error) {
	key := fmt.Sprintf("%s:%d", table, shardID)

	sm.mu.RLock()
	pool, ok := sm.pools[key]
	sm.mu.RUnlock()
	if ok {
		return pool, nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Double-check under write lock.
	pool, ok = sm.pools[key]
	if ok {
		return pool, nil
	}

	dbPath := sm.shardPath(table, shardID)

	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("create shard dir: %w", err)
	}

	pool, err := NewSQLitePool(dbPath, 4) // 4 connections per shard
	if err != nil {
		return nil, fmt.Errorf("open shard %s:%d: %w", table, shardID, err)
	}
	sm.pools[key] = pool
	return pool, nil
}

// shardPath returns the filesystem path for a shard's SQLite database.
func (sm *ShardManager) shardPath(table string, shardID int) string {
	return filepath.Join(sm.dataDir, table, fmt.Sprintf("shard_%d.db", shardID))
}

// Query executes a SELECT query on a specific shard and returns column names
// and row data. Implements P.12.2 (predicate pushdown) by passing the full
// SQL directly to SQLite.
func (sm *ShardManager) Query(ctx context.Context, table string, shardID int, query string) ([]string, [][]byte, error) {
	pool, err := sm.getPool(table, shardID)
	if err != nil {
		return nil, nil, err
	}

	conn, err := pool.Get(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get connection: %w", err)
	}
	defer pool.Put(conn)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("query shard %d: %w", shardID, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("columns: %w", err)
	}

	var resultRows [][]byte
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, fmt.Errorf("scan: %w", err)
		}

		for _, v := range values {
			switch val := v.(type) {
			case []byte:
				resultRows = append(resultRows, val)
			case string:
				resultRows = append(resultRows, []byte(val))
			case nil:
				resultRows = append(resultRows, nil)
			default:
				resultRows = append(resultRows, []byte(fmt.Sprintf("%v", val)))
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("rows iteration: %w", err)
	}

	return cols, resultRows, nil
}

// Exec executes a non-query SQL statement (INSERT, UPDATE, DELETE, DDL)
// on a specific shard.
func (sm *ShardManager) Exec(ctx context.Context, table string, shardID int, query string) error {
	pool, err := sm.getPool(table, shardID)
	if err != nil {
		return err
	}

	conn, err := pool.Get(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}
	defer pool.Put(conn)

	if _, err := conn.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("exec shard %d: %w", shardID, err)
	}

	// Notify the write hook after successful execution.
	sm.mu.RLock()
	hook := sm.writeHook
	sm.mu.RUnlock()
	if hook != nil {
		hook(table, shardID, query)
	}

	return nil
}

// ExecReturn executes a non-query SQL statement and returns the number of
// affected rows.
func (sm *ShardManager) ExecReturn(ctx context.Context, table string, shardID int, query string) (int64, error) {
	pool, err := sm.getPool(table, shardID)
	if err != nil {
		return 0, err
	}

	conn, err := pool.Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("get connection: %w", err)
	}
	defer pool.Put(conn)

	result, err := conn.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("exec shard %d: %w", shardID, err)
	}
	affected, _ := result.RowsAffected()

	// Notify the write hook after successful execution.
	sm.mu.RLock()
	hook := sm.writeHook
	sm.mu.RUnlock()
	if hook != nil {
		hook(table, shardID, query)
	}

	return affected, nil
}

// BatchExec executes multiple SQL statements on a single shard within
// a transaction. Used for multi-row INSERT grouping (P.12.8).
func (sm *ShardManager) BatchExec(ctx context.Context, table string, shardID int, statements []string) error {
	pool, err := sm.getPool(table, shardID)
	if err != nil {
		return err
	}

	conn, err := pool.Get(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}
	defer pool.Put(conn)

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			tx.Rollback()
			return fmt.Errorf("batch exec: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Notify the write hook for each statement after successful commit.
	sm.mu.RLock()
	hook := sm.writeHook
	sm.mu.RUnlock()
	if hook != nil {
		for _, stmt := range statements {
			hook(table, shardID, stmt)
		}
	}

	return nil
}

// DropShard removes a shard's database file.
func (sm *ShardManager) DropShard(table string, shardID int) error {
	key := fmt.Sprintf("%s:%d", table, shardID)

	sm.mu.Lock()
	pool, ok := sm.pools[key]
	if ok {
		pool.Close()
		delete(sm.pools, key)
	}
	sm.mu.Unlock()

	dbPath := sm.shardPath(table, shardID)
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove shard file: %w", err)
	}
	return nil
}

// ShardExists checks if a shard's database file exists.
func (sm *ShardManager) ShardExists(table string, shardID int) bool {
	dbPath := sm.shardPath(table, shardID)
	_, err := os.Stat(dbPath)
	return err == nil
}

// ShardSize returns the size of a shard's database file in bytes.
func (sm *ShardManager) ShardSize(table string, shardID int) (int64, error) {
	dbPath := sm.shardPath(table, shardID)
	info, err := os.Stat(dbPath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// EnsureAllShards creates shard database files for a table if they don't exist.
func (sm *ShardManager) EnsureAllShards(table string) error {
	for i := 0; i < sm.shardCount; i++ {
		if _, err := sm.getPool(table, i); err != nil {
			return fmt.Errorf("ensure shard %d: %w", i, err)
		}
	}
	return nil
}

// DataDir returns the data directory path used by the shard manager.
func (sm *ShardManager) DataDir() string {
	return sm.dataDir
}

// Savepoint creates a named savepoint on a specific shard.
func (sm *ShardManager) Savepoint(ctx context.Context, table string, shardID int, name string) error {
	return sm.Exec(ctx, table, shardID, fmt.Sprintf("SAVEPOINT %s", name))
}

// ReleaseSavepoint releases (commits) a named savepoint on a specific shard.
func (sm *ShardManager) ReleaseSavepoint(ctx context.Context, table string, shardID int, name string) error {
	return sm.Exec(ctx, table, shardID, fmt.Sprintf("RELEASE SAVEPOINT %s", name))
}

// RollbackToSavepoint rolls back to a named savepoint on a specific shard.
func (sm *ShardManager) RollbackToSavepoint(ctx context.Context, table string, shardID int, name string) error {
	return sm.Exec(ctx, table, shardID, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name))
}

// BeginTx starts a new database transaction on a specific shard.
func (sm *ShardManager) BeginTx(ctx context.Context, table string, shardID int) (*sql.Tx, error) {
	pool, err := sm.getPool(table, shardID)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx on shard %d: %w", shardID, err)
	}
	return tx, nil
}

// SQLitePool provides a pool of reusable *sql.DB connections to a SQLite database.
type SQLitePool struct {
	db   *sql.DB
	path string
}

// NewSQLitePool creates a connection pool for a SQLite database (P.12.9).
func NewSQLitePool(path string, maxConns int) (*SQLitePool, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)

	// Verify connectivity.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite %s: %w", path, err)
	}

	return &SQLitePool{db: db, path: path}, nil
}

// Get returns a connection from the pool.
func (p *SQLitePool) Get(ctx context.Context) (*sql.DB, error) {
	if err := p.db.PingContext(ctx); err != nil {
		return nil, err
	}
	return p.db, nil
}

// Put returns a connection to the pool (no-op for sql.DB which manages its own pool).
func (p *SQLitePool) Put(db *sql.DB) {
	// sql.DB manages its own connection pool internally.
}

// Close closes all connections in the pool.
func (p *SQLitePool) Close() {
	p.db.Close()
}
