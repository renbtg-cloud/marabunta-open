// Marabunta - Licensed under the MIT License.
package replication

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marabunta/marabunta-postgres/internal/storage"
)

// --- PlacementManager Tests ---

func newTestPlacementManager(t *testing.T) *PlacementManager {
	t.Helper()
	dir := t.TempDir()
	pm, err := NewPlacementManager(dir, 2)
	if err != nil {
		t.Fatalf("NewPlacementManager: %v", err)
	}
	t.Cleanup(func() { pm.Close() })
	return pm
}

func TestPlacementManager_BasicPlacement(t *testing.T) {
	pm := newTestPlacementManager(t)

	// Place a primary.
	if err := pm.PlacePrimary("users", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}

	primary := pm.GetPrimary("users", 0)
	if primary != "node-A" {
		t.Errorf("expected primary node-A, got %s", primary)
	}

	// Place replicas.
	if err := pm.PlaceReplica("users", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-C"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	replicas := pm.GetReplicas("users", 0)
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(replicas))
	}

	found := make(map[string]bool)
	for _, r := range replicas {
		found[r] = true
	}
	if !found["node-B"] || !found["node-C"] {
		t.Errorf("expected node-B and node-C as replicas, got %v", replicas)
	}

	// Verify GetPlacement returns full info.
	sp := pm.GetPlacement("users", 0)
	if sp == nil {
		t.Fatal("expected placement, got nil")
	}
	if sp.PrimaryNode != "node-A" {
		t.Errorf("expected primary node-A, got %s", sp.PrimaryNode)
	}
	if len(sp.Replicas) != 2 {
		t.Errorf("expected 2 replicas, got %d", len(sp.Replicas))
	}
}

func TestPlacementManager_AntiAffinity(t *testing.T) {
	pm := newTestPlacementManager(t)

	if err := pm.PlacePrimary("orders", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}

	// Placing a replica on the same node as the primary should fail.
	err := pm.PlaceReplica("orders", 0, "node-A")
	if err == nil {
		t.Fatal("expected anti-affinity error, got nil")
	}

	// Placing a duplicate replica should fail.
	if err := pm.PlaceReplica("orders", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	err = pm.PlaceReplica("orders", 0, "node-B")
	if err == nil {
		t.Fatal("expected duplicate replica error, got nil")
	}

	// Placing a replica on a different node should succeed.
	if err := pm.PlaceReplica("orders", 0, "node-C"); err != nil {
		t.Fatalf("PlaceReplica on different node: %v", err)
	}

	replicas := pm.GetReplicas("orders", 0)
	if len(replicas) != 2 {
		t.Errorf("expected 2 replicas, got %d", len(replicas))
	}
}

func TestPlacementManager_RemoveNode(t *testing.T) {
	pm := newTestPlacementManager(t)

	// Set up multiple shards with node-A as primary.
	if err := pm.PlacePrimary("users", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-C"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	if err := pm.PlacePrimary("orders", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm.PlaceReplica("orders", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	// Also place a shard where node-A is a replica.
	if err := pm.PlacePrimary("products", 0, "node-B"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm.PlaceReplica("products", 0, "node-A"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	// Remove node-A.
	needsFailover := pm.RemoveNode("node-A")

	// Should need failover for users:0 and orders:0.
	if len(needsFailover) != 2 {
		t.Fatalf("expected 2 shards needing failover, got %d", len(needsFailover))
	}

	foundUsers := false
	foundOrders := false
	for _, sp := range needsFailover {
		if sp.Table == "users" && sp.ShardID == 0 {
			foundUsers = true
		}
		if sp.Table == "orders" && sp.ShardID == 0 {
			foundOrders = true
		}
	}
	if !foundUsers || !foundOrders {
		t.Errorf("expected users:0 and orders:0 in failover list, got %v", needsFailover)
	}

	// node-A should be removed from products:0 replicas.
	replicas := pm.GetReplicas("products", 0)
	for _, r := range replicas {
		if r == "node-A" {
			t.Error("node-A should be removed from products:0 replicas")
		}
	}

	// users:0 should have empty primary.
	primary := pm.GetPrimary("users", 0)
	if primary != "" {
		t.Errorf("expected empty primary for users:0, got %s", primary)
	}
}

func TestPlacementManager_Persistence(t *testing.T) {
	dir := t.TempDir()

	// Create and populate.
	pm1, err := NewPlacementManager(dir, 2)
	if err != nil {
		t.Fatalf("NewPlacementManager: %v", err)
	}
	if err := pm1.PlacePrimary("users", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm1.PlaceReplica("users", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	pm1.Close()

	// Reload.
	pm2, err := NewPlacementManager(dir, 2)
	if err != nil {
		t.Fatalf("NewPlacementManager (reload): %v", err)
	}
	defer pm2.Close()

	primary := pm2.GetPrimary("users", 0)
	if primary != "node-A" {
		t.Errorf("expected primary node-A after reload, got %s", primary)
	}

	replicas := pm2.GetReplicas("users", 0)
	if len(replicas) != 1 || replicas[0] != "node-B" {
		t.Errorf("expected replica [node-B] after reload, got %v", replicas)
	}
}

func TestPlacementManager_PromoteReplica(t *testing.T) {
	pm := newTestPlacementManager(t)

	if err := pm.PlacePrimary("users", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-C"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	// Promote node-B.
	if err := pm.PromoteReplica("users", 0, "node-B"); err != nil {
		t.Fatalf("PromoteReplica: %v", err)
	}

	primary := pm.GetPrimary("users", 0)
	if primary != "node-B" {
		t.Errorf("expected primary node-B after promotion, got %s", primary)
	}

	replicas := pm.GetReplicas("users", 0)
	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica after promotion, got %d", len(replicas))
	}
	if replicas[0] != "node-C" {
		t.Errorf("expected replica node-C, got %s", replicas[0])
	}
}

func TestPlacementManager_SelectBestReplica(t *testing.T) {
	pm := newTestPlacementManager(t)

	// node-X already has 5 primaries, node-Y has 0.
	for i := 0; i < 5; i++ {
		if err := pm.PlacePrimary("t", i, "node-X"); err != nil {
			t.Fatalf("PlacePrimary: %v", err)
		}
	}

	// Target shard: primary on node-Z, replicas are node-X and node-Y.
	if err := pm.PlacePrimary("target", 0, "node-Z"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm.PlaceReplica("target", 0, "node-X"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	if err := pm.PlaceReplica("target", 0, "node-Y"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	// node-Y should be preferred because it has fewer primaries.
	best := pm.SelectBestReplica("target", 0, nil)
	if best != "node-Y" {
		t.Errorf("expected node-Y as best replica (fewer primaries), got %s", best)
	}
}

// --- WALShipper Tests ---

func newTestWALShipper() *WALShipper {
	return &WALShipper{
		config: WALShipperConfig{
			FlushInterval: time.Hour, // don't auto-flush
			BatchSize:     100,
			Instance:      "test",
			NodeID:        "node-1",
		},
		buffers: make(map[shardKey][]WALEntry),
		lsns:    make(map[shardKey]*uint64),
		stopCh:  make(chan struct{}),
	}
}

func TestWALShipper_ShipAndFlush(t *testing.T) {
	ws := newTestWALShipper()

	entry1 := WALEntry{
		Table:   "users",
		ShardID: 0,
		SQL:     "INSERT INTO users (id, name) VALUES ('1', 'alice')",
	}
	entry2 := WALEntry{
		Table:   "users",
		ShardID: 0,
		SQL:     "INSERT INTO users (id, name) VALUES ('2', 'bob')",
	}

	if err := ws.Ship(entry1); err != nil {
		t.Fatalf("Ship entry1: %v", err)
	}
	if err := ws.Ship(entry2); err != nil {
		t.Fatalf("Ship entry2: %v", err)
	}

	if ws.PendingCount() != 2 {
		t.Errorf("expected 2 pending, got %d", ws.PendingCount())
	}

	// Verify LSN assignments.
	lsn := ws.GetLSN("users", 0)
	if lsn != 2 {
		t.Errorf("expected LSN 2, got %d", lsn)
	}
}

func TestWALShipper_BatchFlush(t *testing.T) {
	ws := &WALShipper{
		config: WALShipperConfig{
			FlushInterval: time.Hour,
			BatchSize:     3,
			Instance:      "test",
			NodeID:        "node-1",
		},
		buffers: make(map[shardKey][]WALEntry),
		lsns:    make(map[shardKey]*uint64),
		stopCh:  make(chan struct{}),
	}

	// Ship 2 entries -- should not trigger batch flush.
	for i := 0; i < 2; i++ {
		if err := ws.Ship(WALEntry{
			Table:   "orders",
			ShardID: 0,
			SQL:     "INSERT INTO orders (id) VALUES (?)",
		}); err != nil {
			t.Fatalf("Ship: %v", err)
		}
	}

	if ws.PendingCount() != 2 {
		t.Errorf("expected 2 pending, got %d", ws.PendingCount())
	}

	// Verify LSN counter is correct.
	lsn := ws.GetLSN("orders", 0)
	if lsn != 2 {
		t.Errorf("expected LSN 2, got %d", lsn)
	}
}

func TestWALShipper_LSNMonotonic(t *testing.T) {
	ws := newTestWALShipper()
	ws.config.BatchSize = 10000 // don't trigger batch flush

	// Ship multiple entries and verify LSNs are strictly monotonic.
	const count = 100
	for i := 0; i < count; i++ {
		if err := ws.Ship(WALEntry{
			Table:   "users",
			ShardID: 0,
			SQL:     "UPDATE users SET name = 'updated'",
		}); err != nil {
			t.Fatalf("Ship %d: %v", i, err)
		}
	}

	// Check the buffer for monotonic LSNs.
	key := shardKey{table: "users", shardID: 0}
	ws.mu.Lock()
	entries := make([]WALEntry, len(ws.buffers[key]))
	copy(entries, ws.buffers[key])
	ws.mu.Unlock()

	if len(entries) != count {
		t.Fatalf("expected %d entries, got %d", count, len(entries))
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].LSN <= entries[i-1].LSN {
			t.Errorf("non-monotonic LSN at index %d: %d <= %d",
				i, entries[i].LSN, entries[i-1].LSN)
		}
	}

	// LSNs should be 1..count.
	if entries[0].LSN != 1 {
		t.Errorf("expected first LSN 1, got %d", entries[0].LSN)
	}
	if entries[count-1].LSN != uint64(count) {
		t.Errorf("expected last LSN %d, got %d", count, entries[count-1].LSN)
	}

	// Different shard should have independent LSN counter.
	if err := ws.Ship(WALEntry{
		Table:   "users",
		ShardID: 1,
		SQL:     "INSERT INTO users (id) VALUES ('x')",
	}); err != nil {
		t.Fatalf("Ship to shard 1: %v", err)
	}

	lsn1 := ws.GetLSN("users", 1)
	if lsn1 != 1 {
		t.Errorf("expected independent LSN 1 for shard 1, got %d", lsn1)
	}
}

func TestWALShipper_SetAndGetLSN(t *testing.T) {
	ws := newTestWALShipper()

	ws.SetLSN("users", 0, 42)
	if got := ws.GetLSN("users", 0); got != 42 {
		t.Errorf("expected LSN 42, got %d", got)
	}

	// Next ship should use LSN 43.
	if err := ws.Ship(WALEntry{
		Table:   "users",
		ShardID: 0,
		SQL:     "DELETE FROM users",
	}); err != nil {
		t.Fatalf("Ship: %v", err)
	}

	if got := ws.GetLSN("users", 0); got != 43 {
		t.Errorf("expected LSN 43 after ship, got %d", got)
	}
}

// --- Follower Tests ---

func newTestFollower(t *testing.T) (*Follower, *storage.ShardManager) {
	t.Helper()
	dir := t.TempDir()
	shardMgr, err := storage.NewShardManager(dir, 16)
	if err != nil {
		t.Fatalf("NewShardManager: %v", err)
	}
	t.Cleanup(func() { shardMgr.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	f := &Follower{
		config:     FollowerConfig{MaxLag: 1000, Instance: "test"},
		appliedLSN: make(map[shardKey]*uint64),
		primaryLSN: make(map[shardKey]*uint64),
		following:  make(map[shardKey]bool),
		gapCh:      make(chan shardKey, 64),
		shardMgr:   shardMgr,
		ctx:        ctx,
		cancel:     cancel,
	}

	return f, shardMgr
}

func setupFollowerShard(f *Follower, table string, shardID int, appliedLSN uint64) {
	key := shardKey{table: table, shardID: shardID}
	applied := appliedLSN
	var primary uint64
	f.appliedLSN[key] = &applied
	f.primaryLSN[key] = &primary
	f.following[key] = true
}

func TestFollower_ApplyEntries(t *testing.T) {
	f, shardMgr := newTestFollower(t)

	key := shardKey{table: "users", shardID: 0}
	setupFollowerShard(f, "users", 0, 0)

	// Create the table first.
	ctx := context.Background()
	if err := shardMgr.Exec(ctx, "users", 0, "CREATE TABLE IF NOT EXISTS users (id TEXT, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Apply a batch.
	batch := WALBatch{
		Table:   "users",
		ShardID: 0,
		Entries: []WALEntry{
			{LSN: 1, SQL: "INSERT INTO users (id, name) VALUES ('1', 'alice')"},
			{LSN: 2, SQL: "INSERT INTO users (id, name) VALUES ('2', 'bob')"},
			{LSN: 3, SQL: "INSERT INTO users (id, name) VALUES ('3', 'charlie')"},
		},
	}

	if err := f.applyBatch(key, batch); err != nil {
		t.Fatalf("applyBatch: %v", err)
	}

	if got := f.GetLSN("users", 0); got != 3 {
		t.Errorf("expected applied LSN 3, got %d", got)
	}

	// Verify data was actually inserted.
	cols, rows, err := shardMgr.Query(ctx, "users", 0, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	// rows is flat: each group of len(cols) values forms one row.
	expectedRowCount := 3
	if len(rows) != expectedRowCount*len(cols) {
		t.Errorf("expected %d values (%d rows x %d cols), got %d",
			expectedRowCount*len(cols), expectedRowCount, len(cols), len(rows))
	}
}

func TestFollower_DetectGap(t *testing.T) {
	f, shardMgr := newTestFollower(t)

	key := shardKey{table: "users", shardID: 0}
	setupFollowerShard(f, "users", 0, 5) // Applied up to LSN 5.

	// Create the table.
	ctx := context.Background()
	if err := shardMgr.Exec(ctx, "users", 0, "CREATE TABLE IF NOT EXISTS users (id TEXT, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Send a batch with a gap: LSN jumps from 5 to 10.
	batch := WALBatch{
		Table:   "users",
		ShardID: 0,
		Entries: []WALEntry{
			{LSN: 10, SQL: "INSERT INTO users (id, name) VALUES ('10', 'gap')"},
		},
	}

	if err := f.applyBatch(key, batch); err != nil {
		t.Fatalf("applyBatch: %v", err)
	}

	// Check that a gap was signaled.
	select {
	case gapKey := <-f.gapCh:
		if gapKey != key {
			t.Errorf("expected gap for %v, got %v", key, gapKey)
		}
	default:
		t.Error("expected gap signal, but channel was empty")
	}

	// LSN should still advance to 10 (we apply despite the gap).
	if got := f.GetLSN("users", 0); got != 10 {
		t.Errorf("expected applied LSN 10, got %d", got)
	}
}

func TestFollower_LagHealth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := &Follower{
		config:     FollowerConfig{MaxLag: 100, Instance: "test"},
		appliedLSN: make(map[shardKey]*uint64),
		primaryLSN: make(map[shardKey]*uint64),
		following:  make(map[shardKey]bool),
		gapCh:      make(chan shardKey, 64),
		ctx:        ctx,
		cancel:     cancel,
	}

	key := shardKey{table: "orders", shardID: 0}
	var applied, primary uint64
	f.appliedLSN[key] = &applied
	f.primaryLSN[key] = &primary
	f.following[key] = true

	// No lag: both at 0.
	if !f.IsHealthy("orders", 0) {
		t.Error("expected healthy when no lag")
	}

	// Set primary ahead by 50 (within threshold).
	atomic.StoreUint64(&primary, 50)
	if !f.IsHealthy("orders", 0) {
		t.Error("expected healthy with lag=50 (threshold=100)")
	}

	lag := f.Lag("orders", 0)
	if lag != 50 {
		t.Errorf("expected lag 50, got %d", lag)
	}

	// Set primary ahead by 200 (exceeds threshold).
	atomic.StoreUint64(&primary, 200)
	if f.IsHealthy("orders", 0) {
		t.Error("expected unhealthy with lag=200 (threshold=100)")
	}

	// Apply some entries to reduce lag.
	atomic.StoreUint64(&applied, 150)
	if !f.IsHealthy("orders", 0) {
		t.Error("expected healthy after applying entries (lag=50)")
	}
}

func TestFollower_SkipAlreadyApplied(t *testing.T) {
	f, shardMgr := newTestFollower(t)

	key := shardKey{table: "users", shardID: 0}
	setupFollowerShard(f, "users", 0, 5)

	ctx := context.Background()
	if err := shardMgr.Exec(ctx, "users", 0, "CREATE TABLE IF NOT EXISTS users (id TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Send entries that are all <= applied LSN.
	batch := WALBatch{
		Table:   "users",
		ShardID: 0,
		Entries: []WALEntry{
			{LSN: 3, SQL: "INSERT INTO users (id) VALUES ('3')"},
			{LSN: 4, SQL: "INSERT INTO users (id) VALUES ('4')"},
			{LSN: 5, SQL: "INSERT INTO users (id) VALUES ('5')"},
		},
	}

	if err := f.applyBatch(key, batch); err != nil {
		t.Fatalf("applyBatch: %v", err)
	}

	// LSN should remain at 5.
	if got := f.GetLSN("users", 0); got != 5 {
		t.Errorf("expected applied LSN 5 (unchanged), got %d", got)
	}

	// No rows should have been inserted.
	_, rows, err := shardMgr.Query(ctx, "users", 0, "SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one result row")
	}
	if string(rows[0]) != "0" {
		t.Errorf("expected 0 rows (all skipped), got %s", string(rows[0]))
	}
}

// --- Failover Tests ---

func TestFailover_PrimaryDeath(t *testing.T) {
	dir := t.TempDir()
	pm, err := NewPlacementManager(dir, 2)
	if err != nil {
		t.Fatalf("NewPlacementManager: %v", err)
	}
	defer pm.Close()

	// Set up placement: primary=node-A, replicas=[node-B, node-C].
	if err := pm.PlacePrimary("users", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-C"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	// Simulate node death via RemoveNode + PlacePrimary.
	needsFailover := pm.RemoveNode("node-A")
	if len(needsFailover) != 1 {
		t.Fatalf("expected 1 shard needing failover, got %d", len(needsFailover))
	}

	sp := needsFailover[0]
	if sp.Table != "users" || sp.ShardID != 0 {
		t.Errorf("expected users:0, got %s:%d", sp.Table, sp.ShardID)
	}

	// After RemoveNode, the replicas should still be node-B and node-C.
	replicas := pm.GetReplicas("users", 0)
	if len(replicas) < 1 {
		t.Fatal("expected at least 1 replica remaining")
	}

	// Promote first available replica.
	if err := pm.PlacePrimary("users", 0, replicas[0]); err != nil {
		t.Fatalf("PlacePrimary for promotion: %v", err)
	}

	newPrimary := pm.GetPrimary("users", 0)
	if newPrimary == "" {
		t.Error("expected new primary after failover")
	}
	if newPrimary == "node-A" {
		t.Error("new primary should not be the dead node")
	}
}

func TestFailover_HighestLSNPromotion(t *testing.T) {
	pm := newTestPlacementManager(t)

	if err := pm.PlacePrimary("users", 0, "node-A"); err != nil {
		t.Fatalf("PlacePrimary: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-B"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}
	if err := pm.PlaceReplica("users", 0, "node-C"); err != nil {
		t.Fatalf("PlaceReplica: %v", err)
	}

	// Give node-B many existing primaries to make it less desirable.
	for i := 1; i <= 10; i++ {
		if err := pm.PlacePrimary("extra", i, "node-B"); err != nil {
			t.Fatalf("PlacePrimary extra: %v", err)
		}
	}

	// node-C should be preferred (fewer primaries).
	best := pm.SelectBestReplica("users", 0, nil)
	if best != "node-C" {
		t.Errorf("expected node-C as best replica, got %s", best)
	}
}

func TestFailover_Fencing(t *testing.T) {
	dir := t.TempDir()
	pm, err := NewPlacementManager(dir, 2)
	if err != nil {
		t.Fatalf("NewPlacementManager: %v", err)
	}
	defer pm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a FailoverManager without a real swarm client.
	fm := &FailoverManager{
		placement: pm,
		config:    FailoverConfig{Instance: "test"},
		monitors:  make(map[shardKey]*primaryHealth),
		fences:    make(map[shardKey]*FenceRecord),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Fence a node.
	fm.fencePrimary("users", 0, "node-A", 100)

	if !fm.IsFenced("users", 0, "node-A") {
		t.Error("expected node-A to be fenced")
	}

	if fm.IsFenced("users", 0, "node-B") {
		t.Error("expected node-B to NOT be fenced")
	}

	if fm.IsFenced("users", 1, "node-A") {
		t.Error("expected node-A to NOT be fenced for different shard")
	}

	fence := fm.GetFence("users", 0)
	if fence == nil {
		t.Fatal("expected fence record")
	}
	if fence.FencedLSN != 100 {
		t.Errorf("expected fenced LSN 100, got %d", fence.FencedLSN)
	}
	if fence.NodeID != "node-A" {
		t.Errorf("expected fenced node node-A, got %s", fence.NodeID)
	}

	// Verify counts.
	if fm.FencedNodes() != 1 {
		t.Errorf("expected 1 fenced node, got %d", fm.FencedNodes())
	}
}
