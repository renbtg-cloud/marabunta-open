// Marabunta - Licensed under the MIT License.
package planner

import (
	"fmt"
	"strings"

	"github.com/marabunta/marabunta-postgres/internal/parser"
	"github.com/marabunta/marabunta-postgres/internal/types"
)

// ExplainResult produces a human-readable EXPLAIN output for a plan (P.12.11).
func ExplainResult(plan *Plan, shardCount int) [][]byte {
	if plan == nil {
		return nil
	}

	var lines []string

	target := plan.ExplainPlan
	if target == nil {
		target = plan
	}

	// Strategy line.
	switch target.Type {
	case PlanSingleShard:
		if len(target.Shards) > 0 {
			lines = append(lines, fmt.Sprintf("Strategy: SingleShard (shard %d)", target.Shards[0]))
		} else {
			lines = append(lines, "Strategy: SingleShard (shard unknown)")
		}
	case PlanScatter:
		shardInfo := "all"
		if len(target.Shards) > 0 {
			shardInfo = fmt.Sprintf("%d of %d", len(target.Shards), shardCount)
		}
		lines = append(lines, fmt.Sprintf("Strategy: Scatter (%s shards)", shardInfo))
	case PlanDDL:
		lines = append(lines, "Strategy: DDL (all shards)")
	case PlanJoin:
		strategyName := "Unknown"
		switch target.Strategy {
		case JoinColocated:
			strategyName = "Colocated"
		case JoinBroadcast:
			strategyName = "Broadcast"
		case JoinShuffle:
			strategyName = "Shuffle"
		}
		lines = append(lines, fmt.Sprintf("Strategy: Join (%s)", strategyName))
	case PlanLocal:
		lines = append(lines, "Strategy: Local")
	case PlanCreateFunction:
		lines = append(lines, "Strategy: CreateFunction (local)")
	case PlanCall:
		lines = append(lines, "Strategy: Call (local)")
	case PlanDO:
		lines = append(lines, "Strategy: DO (local)")
	}

	// Shard count.
	if len(target.Shards) > 0 {
		lines = append(lines, fmt.Sprintf("Shards accessed: %d", len(target.Shards)))
	} else if target.Type == PlanScatter || target.Type == PlanDDL {
		lines = append(lines, fmt.Sprintf("Shards accessed: %d (all)", shardCount))
	}

	// Pushdown info.
	var pushdowns []string
	if target.PushdownPredicate {
		pushdowns = append(pushdowns, "predicate")
	}
	if target.PushdownAgg {
		pushdowns = append(pushdowns, "aggregation")
	}
	if target.PushdownLimit {
		pushdowns = append(pushdowns, fmt.Sprintf("limit(%d+%d)", target.MergeLimit, target.MergeOffset))
	}
	if target.PushdownGroupBy {
		pushdowns = append(pushdowns, "group_by")
	}
	if target.PushdownOrderBy {
		pushdowns = append(pushdowns, "order_by")
	}
	if len(pushdowns) > 0 {
		lines = append(lines, fmt.Sprintf("Pushdown: %s", strings.Join(pushdowns, ", ")))
	}

	// Aggregation merge.
	if len(target.MergeAggregates) > 0 {
		var aggs []string
		for _, ma := range target.MergeAggregates {
			name := aggFuncName(ma.Func)
			if ma.NeedSum && ma.NeedCount {
				aggs = append(aggs, fmt.Sprintf("%s(%s) via SUM+COUNT", name, ma.Column.Column))
			} else {
				aggs = append(aggs, fmt.Sprintf("%s(%s)", name, ma.Column.Column))
			}
		}
		lines = append(lines, fmt.Sprintf("Merge aggregates: %s", strings.Join(aggs, ", ")))
	}

	// Join strategy advisory.
	if target.Type == PlanJoin && target.Strategy == JoinShuffle {
		lines = append(lines, "Advisory: Shuffle join may be slow for large tables. Consider co-locating on join key.")
	}

	// Convert to wire format.
	var rows [][]byte
	for _, line := range lines {
		rows = append(rows, []byte(line))
	}
	return rows
}

// ExplainColumns returns the column definitions for EXPLAIN output.
func ExplainColumns() []types.ColumnDef {
	return []types.ColumnDef{
		{
			Name:     "QUERY PLAN",
			OID:      25, // text
			TypeSize: -1,
			TypeMod:  -1,
			Format:   0,
		},
	}
}

// BuildShardSQL rewrites a SQL statement for execution on a specific shard.
// For scatter queries, this includes the full WHERE clause (P.12.2),
// LIMIT pushdown (P.12.5), and GROUP BY pushdown (P.12.6).
func BuildShardSQL(plan *Plan) string {
	if plan.Statement == nil {
		return ""
	}

	sql := plan.Statement.RawSQL

	// For aggregation pushdown with AVG, rewrite AVG(col) to SUM(col), COUNT(col).
	if plan.PushdownAgg {
		for _, ma := range plan.MergeAggregates {
			if ma.NeedSum && ma.NeedCount {
				avgExpr := fmt.Sprintf("AVG(%s)", ma.Column.Column)
				replacement := fmt.Sprintf("SUM(%s) AS _sum_%s, COUNT(%s) AS _count_%s",
					ma.Column.Column, ma.Column.Column,
					ma.Column.Column, ma.Column.Column)
				sql = strings.Replace(sql, avgExpr, replacement, 1)
				// Case-insensitive replacement.
				sql = strings.Replace(sql, strings.ToLower(avgExpr), replacement, 1)
			}
		}
	}

	// For LIMIT pushdown: push LIMIT (limit+offset) to each shard.
	// The coordinator will apply the final LIMIT+OFFSET on merged results.
	if plan.PushdownLimit && plan.MergeLimit > 0 {
		totalNeeded := plan.MergeLimit + plan.MergeOffset
		upperSQL := strings.ToUpper(sql)
		if !strings.Contains(upperSQL, "LIMIT") {
			sql = fmt.Sprintf("%s LIMIT %d", sql, totalNeeded)
		}
	}

	return sql
}

// aggFuncName returns the SQL name of an aggregate function.
func aggFuncName(f parser.AggFunc) string {
	switch f {
	case parser.AggCount:
		return "COUNT"
	case parser.AggSum:
		return "SUM"
	case parser.AggMin:
		return "MIN"
	case parser.AggMax:
		return "MAX"
	case parser.AggAvg:
		return "AVG"
	default:
		return "UNKNOWN"
	}
}
