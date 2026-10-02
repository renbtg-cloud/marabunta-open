// Marabunta - Licensed under the MIT License.
package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-postgres/internal/planner"
)

// ScatterExecutor handles queries that must fan out to multiple shards
// across the swarm and merge results back.
type ScatterExecutor struct {
	client   *swarm.SwarmClient
	instance string
}

// NewScatterExecutor creates a new ScatterExecutor.
func NewScatterExecutor(client *swarm.SwarmClient, instance string) *ScatterExecutor {
	return &ScatterExecutor{
		client:   client,
		instance: instance,
	}
}

// ScatterQuery describes a SQL query to execute on remote shards.
type ScatterQuery struct {
	SQL      string `json:"sql"`
	Table    string `json:"table"`
	ShardID  int    `json:"shard_id"`
	Instance string `json:"instance"`
	TxnOp    string `json:"txn_op,omitempty"`  // "prepare"|"commit"|"abort"|"snapshot"|"lsn_query"
	TxnID    string `json:"txn_id,omitempty"`  // transaction ID for 2PC operations
}

// ScatterQueryResult is the response from a remote shard.
type ScatterQueryResult struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
	Error   string     `json:"error"`
	Affected int64     `json:"affected"`
}

// FanOut distributes a query to remote shards via the swarm Scatter API
// and gathers results. Implements P.12.10 with context-based timeout/cancel.
func (se *ScatterExecutor) FanOut(ctx context.Context, plan *planner.Plan, shardIDs []int) ([]ScatterQueryResult, error) {
	sql := planner.BuildShardSQL(plan)
	table := ""
	if plan.Statement != nil && len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}

	// Build scatter units, one per shard.
	units := make([]swarm.ScatterUnit, len(shardIDs))
	for i, sid := range shardIDs {
		query := ScatterQuery{
			SQL:      sql,
			Table:    table,
			ShardID:  sid,
			Instance: se.instance,
		}
		payload, err := json.Marshal(query)
		if err != nil {
			return nil, fmt.Errorf("marshal scatter query: %w", err)
		}
		units[i] = swarm.ScatterUnit{
			Payload:        payload,
			RequiredTraits: []string{"CanStoreState"},
		}
	}

	// Compute timeout from context deadline.
	timeoutMS := uint32(30000) // default 30s
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 {
			timeoutMS = uint32(remaining.Milliseconds())
		} else {
			return nil, fmt.Errorf("scatter: context deadline already passed")
		}
	}

	resp, err := se.client.Scatter(swarm.ScatterRequest{
		Units: units,
		Hints: swarm.ScatterHints{
			Locality:    swarm.LocalityPreferLocal,
			Consistency: swarm.ConsistencyEventual,
			Priority:    swarm.PriorityNormal,
			TimeoutMS:   timeoutMS,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("scatter query: %w", err)
	}

	// Parse results.
	results := make([]ScatterQueryResult, len(resp.Results))
	for i, r := range resp.Results {
		if !r.Success {
			results[i] = ScatterQueryResult{Error: r.Error}
			continue
		}
		if err := json.Unmarshal(r.Response, &results[i]); err != nil {
			results[i] = ScatterQueryResult{Error: fmt.Sprintf("unmarshal result: %v", err)}
		}
	}

	return results, nil
}

// FanOutLocal distributes a query to local shards in parallel.
// This is used when all target shards are on the local node.
func (se *ScatterExecutor) FanOutLocal(ctx context.Context, plan *planner.Plan, shardIDs []int, queryFunc func(ctx context.Context, table string, shardID int, sql string) ([]string, [][]byte, error)) ([]ScatterQueryResult, error) {
	sql := planner.BuildShardSQL(plan)
	table := ""
	if plan.Statement != nil && len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}

	results := make([]ScatterQueryResult, len(shardIDs))
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, sid := range shardIDs {
		wg.Add(1)
		go func(idx, shardID int) {
			defer wg.Done()

			cols, rows, err := queryFunc(ctx, table, shardID, sql)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				results[idx] = ScatterQueryResult{Error: err.Error()}
				return
			}

			// Convert flat [][]byte to [][]string rows.
			// rows is a flat slice where every len(cols) values form one row.
			var strRows [][]string
			numCols := len(cols)
			if numCols > 0 {
				for r := 0; r+numCols <= len(rows); r += numCols {
					strRow := make([]string, numCols)
					for c := 0; c < numCols; c++ {
						if rows[r+c] != nil {
							strRow[c] = string(rows[r+c])
						}
					}
					strRows = append(strRows, strRow)
				}
			}

			results[idx] = ScatterQueryResult{
				Columns: cols,
				Rows:    strRows,
			}
		}(i, sid)
	}

	// Wait with context cancellation.
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-ctx.Done():
		return nil, fmt.Errorf("fan-out cancelled: %w", ctx.Err())
	}

	// Check for errors.
	var errors []string
	for _, r := range results {
		if r.Error != "" {
			errors = append(errors, r.Error)
		}
	}
	if len(errors) > 0 {
		log.Printf("scatter: %d/%d shards returned errors", len(errors), len(shardIDs))
	}

	return results, nil
}
