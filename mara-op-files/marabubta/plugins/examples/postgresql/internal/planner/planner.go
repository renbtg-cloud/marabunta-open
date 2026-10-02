// Marabunta - Licensed under the MIT License.
// Package planner determines the distributed execution strategy for parsed SQL.
// It takes a Statement from the parser and produces a Plan that the executor
// can dispatch. The planner handles shard pruning, predicate pushdown,
// aggregation pushdown, LIMIT pushdown, GROUP BY pushdown, and join strategies.
package planner

import (
	"fmt"

	"github.com/marabunta/marabunta-postgres/internal/catalog"
	"github.com/marabunta/marabunta-postgres/internal/parser"
)

// PlanType identifies the execution strategy.
type PlanType int

const (
	// PlanSingleShard routes to exactly one shard.
	PlanSingleShard PlanType = iota
	// PlanScatter fans out to multiple shards and gathers results.
	PlanScatter
	// PlanDDL executes DDL across all shards.
	PlanDDL
	// PlanLocal executes locally (SET, SHOW, etc.).
	PlanLocal
	// PlanExplain returns the plan without executing.
	PlanExplain
	// PlanJoin executes a join across shards.
	PlanJoin
	// PlanCreateFunction registers a PL/pgSQL function or procedure.
	PlanCreateFunction
	// PlanCall invokes a PL/pgSQL function or procedure.
	PlanCall
	// PlanDO executes an anonymous PL/pgSQL block.
	PlanDO
)

// JoinStrategy determines how a distributed join is executed.
type JoinStrategy int

const (
	// JoinColocated both tables share the same shard key — execute locally per shard.
	JoinColocated JoinStrategy = iota
	// JoinBroadcast one table is small enough to broadcast to all shards.
	JoinBroadcast
	// JoinShuffle both tables are large — redistribute by join key.
	JoinShuffle
)

// Plan describes the execution strategy for a parsed statement.
type Plan struct {
	Type      PlanType
	Statement *parser.Statement
	Shards    []int // target shard IDs (empty = all shards)
	Strategy  JoinStrategy
	SubPlans  []*Plan // for joins with multiple stages

	// Pushdown flags.
	PushdownPredicate bool
	PushdownAgg       bool
	PushdownLimit     bool
	PushdownGroupBy   bool
	PushdownOrderBy   bool

	// Aggregation merge info.
	MergeAggregates []MergeAgg

	// LIMIT merge info.
	MergeLimit  int64
	MergeOffset int64

	// EXPLAIN info.
	ExplainPlan *Plan

	// For DDL: the raw DDL to scatter.
	DDL string

	// For local: response tag.
	LocalTag   string
	LocalValue string
}

// MergeAgg describes how to merge partial aggregates from shards.
type MergeAgg struct {
	Func      parser.AggFunc
	Column    parser.ColumnRef
	Alias     string
	NeedCount bool // for AVG: also need COUNT
	NeedSum   bool // for AVG: also need SUM
}

// Planner produces execution plans from parsed statements.
type Planner struct {
	catalog    *catalog.Catalog
	shardCount int
}

// New creates a new Planner.
func New(cat *catalog.Catalog, shardCount int) *Planner {
	return &Planner{
		catalog:    cat,
		shardCount: shardCount,
	}
}

// Plan produces an execution plan for a parsed statement.
func (p *Planner) Plan(stmt *parser.Statement) (*Plan, error) {
	switch stmt.Type {
	case parser.StmtSelect:
		return p.planSelect(stmt)
	case parser.StmtInsert:
		return p.planInsert(stmt)
	case parser.StmtUpdate, parser.StmtDelete:
		return p.planModify(stmt)
	case parser.StmtCreateTable, parser.StmtDropTable, parser.StmtAlterTable:
		return p.planDDL(stmt)
	case parser.StmtCreateIndex:
		return p.planCreateIndex(stmt)
	case parser.StmtExplain:
		return p.planExplain(stmt)
	case parser.StmtSet:
		return &Plan{
			Type:      PlanLocal,
			Statement: stmt,
			LocalTag:  "SET",
		}, nil
	case parser.StmtShow:
		return &Plan{
			Type:      PlanLocal,
			Statement: stmt,
			LocalTag:  "SHOW",
		}, nil
	case parser.StmtBegin, parser.StmtCommit, parser.StmtRollback:
		return &Plan{
			Type:      PlanLocal,
			Statement: stmt,
			LocalTag:  stmt.RawSQL,
		}, nil
	case parser.StmtCreateFunction:
		return &Plan{
			Type:      PlanCreateFunction,
			Statement: stmt,
			LocalTag:  "CREATE FUNCTION",
		}, nil
	case parser.StmtCall:
		return &Plan{
			Type:      PlanCall,
			Statement: stmt,
			LocalTag:  "CALL",
		}, nil
	case parser.StmtDO:
		return &Plan{
			Type:      PlanDO,
			Statement: stmt,
			LocalTag:  "DO",
		}, nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %d", stmt.Type)
	}
}

// planSelect routes SELECT queries based on shard key presence and join analysis.
func (p *Planner) planSelect(stmt *parser.Statement) (*Plan, error) {
	plan := &Plan{
		Type:      PlanScatter,
		Statement: stmt,
	}

	// Check for joins first.
	if len(stmt.JoinClauses) > 0 {
		return p.planJoin(stmt)
	}

	// Try shard pruning on WHERE clause.
	if stmt.Where != nil && len(stmt.Tables) > 0 {
		shards := p.pruneShards(stmt.Tables[0], stmt.Where)
		if len(shards) > 0 && len(shards) < p.shardCount {
			plan.Shards = shards
			if len(shards) == 1 {
				plan.Type = PlanSingleShard
			}
		}
	}

	// Predicate pushdown — always push the full WHERE to shards (P.12.2).
	if stmt.Where != nil {
		plan.PushdownPredicate = true
	}

	// Aggregation pushdown (P.12.4).
	if len(stmt.Aggregates) > 0 {
		plan.PushdownAgg = true
		for _, agg := range stmt.Aggregates {
			ma := MergeAgg{
				Func:   agg.Func,
				Column: agg.Column,
				Alias:  agg.Alias,
			}
			if agg.Func == parser.AggAvg {
				// AVG requires both SUM and COUNT from each shard.
				ma.NeedSum = true
				ma.NeedCount = true
			}
			plan.MergeAggregates = append(plan.MergeAggregates, ma)
		}
	}

	// GROUP BY pushdown (P.12.6).
	if len(stmt.GroupBy) > 0 {
		plan.PushdownGroupBy = true
	}

	// LIMIT pushdown (P.12.5).
	if stmt.HasLimit {
		plan.PushdownLimit = true
		plan.MergeLimit = stmt.Limit
		plan.MergeOffset = stmt.Offset
	}

	// ORDER BY pushdown.
	if len(stmt.OrderBy) > 0 {
		plan.PushdownOrderBy = true
	}

	return plan, nil
}

// planInsert routes INSERT statements. Multi-row INSERTs are grouped by shard (P.12.8).
func (p *Planner) planInsert(stmt *parser.Statement) (*Plan, error) {
	if len(stmt.Tables) == 0 {
		return nil, fmt.Errorf("INSERT: no target table")
	}
	table := stmt.Tables[0]

	// If single row and we can determine shard from the shard key value.
	if len(stmt.InsertRows) == 1 {
		shardKeyCol := p.catalog.GetShardKey(table)
		if shardKeyCol != "" {
			colIdx := p.catalog.GetColumnIndex(table, shardKeyCol)
			if colIdx >= 0 && colIdx < len(stmt.InsertRows[0]) {
				shardID := p.hashToShard(stmt.InsertRows[0][colIdx])
				return &Plan{
					Type:      PlanSingleShard,
					Statement: stmt,
					Shards:    []int{shardID},
				}, nil
			}
		}
	}

	// Multi-row INSERT: scatter to all affected shards (P.12.8).
	if len(stmt.InsertRows) > 1 {
		shardKeyCol := p.catalog.GetShardKey(table)
		if shardKeyCol != "" {
			colIdx := p.catalog.GetColumnIndex(table, shardKeyCol)
			if colIdx >= 0 {
				shardSet := make(map[int]bool)
				for _, row := range stmt.InsertRows {
					if colIdx < len(row) {
						shardSet[p.hashToShard(row[colIdx])] = true
					}
				}
				var shards []int
				for s := range shardSet {
					shards = append(shards, s)
				}
				return &Plan{
					Type:      PlanScatter,
					Statement: stmt,
					Shards:    shards,
				}, nil
			}
		}
	}

	// Fallback: scatter to all shards.
	return &Plan{
		Type:      PlanScatter,
		Statement: stmt,
	}, nil
}

// planModify routes UPDATE/DELETE statements with shard pruning.
func (p *Planner) planModify(stmt *parser.Statement) (*Plan, error) {
	plan := &Plan{
		Type:      PlanScatter,
		Statement: stmt,
	}

	if stmt.Where != nil && len(stmt.Tables) > 0 {
		shards := p.pruneShards(stmt.Tables[0], stmt.Where)
		if len(shards) > 0 && len(shards) < p.shardCount {
			plan.Shards = shards
			if len(shards) == 1 {
				plan.Type = PlanSingleShard
			}
		}
		plan.PushdownPredicate = true
	}

	return plan, nil
}

// planDDL routes DDL to all shards.
func (p *Planner) planDDL(stmt *parser.Statement) (*Plan, error) {
	return &Plan{
		Type:      PlanDDL,
		Statement: stmt,
		DDL:       stmt.DDL,
	}, nil
}

// planCreateIndex scatters CREATE INDEX to all shards (P.12.3).
func (p *Planner) planCreateIndex(stmt *parser.Statement) (*Plan, error) {
	return &Plan{
		Type:      PlanDDL,
		Statement: stmt,
		DDL:       stmt.DDL,
	}, nil
}

// planExplain wraps the inner plan with EXPLAIN metadata (P.12.11).
func (p *Planner) planExplain(stmt *parser.Statement) (*Plan, error) {
	if stmt.ExplainStmt == nil {
		return nil, fmt.Errorf("EXPLAIN: no inner statement")
	}
	innerPlan, err := p.Plan(stmt.ExplainStmt)
	if err != nil {
		return nil, fmt.Errorf("EXPLAIN inner plan: %w", err)
	}
	return &Plan{
		Type:        PlanExplain,
		Statement:   stmt,
		ExplainPlan: innerPlan,
	}, nil
}

// planJoin determines the join strategy (P.12.7).
func (p *Planner) planJoin(stmt *parser.Statement) (*Plan, error) {
	plan := &Plan{
		Type:      PlanJoin,
		Statement: stmt,
	}

	if len(stmt.JoinClauses) == 0 {
		return nil, fmt.Errorf("JOIN plan requested but no join clauses")
	}

	jc := stmt.JoinClauses[0]
	leftTable := ""
	if len(stmt.Tables) > 0 {
		leftTable = stmt.Tables[0]
	}
	rightTable := jc.Table

	// Check if tables are co-located (same shard key).
	leftKey := p.catalog.GetShardKey(leftTable)
	rightKey := p.catalog.GetShardKey(rightTable)

	if leftKey != "" && rightKey != "" && p.isJoinOnShardKeys(jc, leftTable, rightTable, leftKey, rightKey) {
		plan.Strategy = JoinColocated
	} else {
		// Check table sizes for broadcast vs shuffle.
		leftSize := p.catalog.GetTableRowEstimate(leftTable)
		rightSize := p.catalog.GetTableRowEstimate(rightTable)
		broadcastThreshold := int64(10000)

		if rightSize < broadcastThreshold {
			plan.Strategy = JoinBroadcast
		} else if leftSize < broadcastThreshold {
			plan.Strategy = JoinBroadcast
		} else {
			plan.Strategy = JoinShuffle
		}
	}

	// Predicate pushdown for joins too.
	if stmt.Where != nil {
		plan.PushdownPredicate = true
	}

	return plan, nil
}

// isJoinOnShardKeys checks if the join ON clause references the shard keys of both tables.
func (p *Planner) isJoinOnShardKeys(jc parser.JoinClause, leftTable, rightTable, leftKey, rightKey string) bool {
	if jc.On == nil {
		return false
	}
	if jc.On.Op != "=" {
		return false
	}
	col1 := jc.On.Column
	val := jc.On.Value

	// Check if one side is leftTable.shardKey and the other is rightTable.shardKey.
	if (col1.Column == leftKey && val == rightKey) ||
		(col1.Column == rightKey && val == leftKey) {
		return true
	}
	if (col1.Table == leftTable && col1.Column == leftKey) ||
		(col1.Table == rightTable && col1.Column == rightKey) {
		return true
	}
	return false
}
