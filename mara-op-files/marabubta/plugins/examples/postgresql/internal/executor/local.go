// Marabunta - Licensed under the MIT License.
package executor

import (
	"context"
	"fmt"

	"github.com/marabunta/marabunta-postgres/internal/parser"
	"github.com/marabunta/marabunta-postgres/internal/planner"
	"github.com/marabunta/marabunta-postgres/internal/storage"
	"github.com/marabunta/marabunta-postgres/internal/types"
)

// LocalExecutor handles queries that can be served from a single local shard.
type LocalExecutor struct {
	shardMgr *storage.ShardManager
}

// NewLocalExecutor creates a new LocalExecutor.
func NewLocalExecutor(shardMgr *storage.ShardManager) *LocalExecutor {
	return &LocalExecutor{shardMgr: shardMgr}
}

// Execute runs a plan on a single local shard.
func (le *LocalExecutor) Execute(ctx context.Context, plan *planner.Plan) (*Result, error) {
	if len(plan.Shards) == 0 {
		return nil, fmt.Errorf("local executor: no shard specified")
	}

	shardID := plan.Shards[0]
	table := ""
	if plan.Statement != nil && len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}

	sql := planner.BuildShardSQL(plan)

	// Check for DML vs query.
	switch plan.Statement.Type {
	case parser.StmtInsert, parser.StmtUpdate, parser.StmtDelete:
		if err := le.shardMgr.Exec(ctx, table, shardID, sql); err != nil {
			return nil, fmt.Errorf("local exec shard %d: %w", shardID, err)
		}
		return &Result{
			Tag: buildTag(plan.Statement, 1),
		}, nil

	default:
		cols, rows, err := le.shardMgr.Query(ctx, table, shardID, sql)
		if err != nil {
			return nil, fmt.Errorf("local query shard %d: %w", shardID, err)
		}

		var colDefs []types.ColumnDef
		for _, c := range cols {
			colDefs = append(colDefs, types.ColumnDef{
				Name:     c,
				OID:      25,
				TypeSize: -1,
				TypeMod:  -1,
				Format:   0,
			})
		}

		rowCount := 0
		if len(cols) > 0 {
			rowCount = len(rows) / len(cols)
		}

		return &Result{
			Columns: colDefs,
			Rows:    rows,
			Tag:     buildTag(plan.Statement, rowCount),
		}, nil
	}
}
