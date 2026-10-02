// Marabunta - Licensed under the MIT License.
package transaction

import (
	"fmt"
	"log"
	"sync"

	swarm "github.com/marabunta/swarm-plugin-go"
)

// SyncpointManager coordinates syncpoint (commit) and rollback operations
// with strong consistency via the swarm (spec C.6).
type SyncpointManager struct {
	mu     sync.Mutex
	client *swarm.SwarmClient
}

// NewSyncpointManager creates a new SyncpointManager.
func NewSyncpointManager(client *swarm.SwarmClient) *SyncpointManager {
	return &SyncpointManager{client: client}
}

// Syncpoint commits all pending changes for the given context.
// Uses strong consistency to ensure all replicas acknowledge the commit.
func (sm *SyncpointManager) Syncpoint(ctx *CICSContext) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Store a commit marker in the swarm with strong consistency.
	commitKey := fmt.Sprintf("cics:txlog:%s:%s:%d",
		ctx.RegionID, ctx.EIBTRNID, ctx.EIBTIME)

	_, err := sm.client.Store(swarm.StoreRequest{
		Key:   []byte(commitKey),
		Value: []byte("COMMITTED"),
		Options: swarm.StoreOptions{
			Consistency: swarm.ConsistencyStrong,
			Replicas:    3,
			TTLSeconds:  3600, // 1 hour retention for commit markers
		},
	})
	if err != nil {
		return fmt.Errorf("syncpoint commit: %w", err)
	}

	ctx.Commit()
	log.Printf("CICS syncpoint: committed transaction %s", ctx.EIBTRNID)
	return nil
}

// Rollback undoes all changes for the given context.
func (sm *SyncpointManager) Rollback(ctx *CICSContext) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	ctx.Rollback()
	log.Printf("CICS syncpoint: rolled back transaction %s", ctx.EIBTRNID)
	return nil
}

// CheckCommitStatus checks if a transaction was previously committed.
func (sm *SyncpointManager) CheckCommitStatus(regionID, tranID string, eibTime int32) (bool, error) {
	commitKey := fmt.Sprintf("cics:txlog:%s:%s:%d", regionID, tranID, eibTime)

	resp, err := sm.client.Fetch(swarm.FetchRequest{
		Key: []byte(commitKey),
		Options: swarm.FetchOptions{
			Consistency: swarm.ConsistencyStrong,
		},
	})
	if err != nil {
		return false, err
	}

	return resp.Found && string(resp.Value) == "COMMITTED", nil
}
