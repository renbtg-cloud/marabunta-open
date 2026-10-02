// Marabunta - Licensed under the MIT License.
// Package executor dispatches query plans to local SQLite shards or scatters
// them across the swarm. It handles result merging for scatter queries,
// aggregation merge for pushdown, and LIMIT/OFFSET merge for top-K queries.
package executor

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	swarm "github.com/marabunta/swarm-plugin-go"

	"github.com/marabunta/marabunta-postgres/internal/catalog"
	"github.com/marabunta/marabunta-postgres/internal/parser"
	"github.com/marabunta/marabunta-postgres/internal/planner"
	"github.com/marabunta/marabunta-postgres/internal/plpgsql"
	"github.com/marabunta/marabunta-postgres/internal/storage"
	"github.com/marabunta/marabunta-postgres/internal/txn"
	"github.com/marabunta/marabunta-postgres/internal/types"
)

// Result holds the output of executing a plan.
type Result struct {
	Columns []types.ColumnDef
	Rows    [][]byte // each row is a slice of column values (text-encoded)
	Tag     string   // CommandComplete tag
}

// Executor dispatches plans to local shards or scatters them across the swarm.
type Executor struct {
	client           *swarm.SwarmClient
	shardMgr         *storage.ShardManager
	catalog          *catalog.Catalog
	instance         string
	txnCoordinator   *txn.TxnCoordinator
	replicaShardMgr  *storage.ShardManager // optional: separate shard manager for reading from replicas
	readFromReplica  bool                  // if true, route SELECTs to replica shard manager
	funcCatalog      *plpgsql.FunctionCatalog // PL/pgSQL function/procedure catalog
	scatterExec      *ScatterExecutor         // swarm-based scatter for distributed queries
	placementMgr     PlacementQuerier         // shard-to-node placement info
}

// New creates a new Executor.
func New(client *swarm.SwarmClient, shardMgr *storage.ShardManager, cat *catalog.Catalog, instance string) *Executor {
	return &Executor{
		client:   client,
		shardMgr: shardMgr,
		catalog:  cat,
		instance: instance,
	}
}

// SetTxnCoordinator sets the 2PC coordinator for multi-shard write atomicity.
// When set, multi-shard writes (scatter INSERT/UPDATE/DELETE) will use 2PC
// instead of sequential execution.
func (e *Executor) SetTxnCoordinator(coord *txn.TxnCoordinator) {
	e.txnCoordinator = coord
}

// SetReplicaShardMgr sets a separate shard manager used for reading from
// replicas. When readFromReplica is true and this is set, SELECT queries
// will be routed to the replica shard manager.
func (e *Executor) SetReplicaShardMgr(mgr *storage.ShardManager) {
	e.replicaShardMgr = mgr
}

// SetReadFromReplica enables or disables reading from replicas for SELECT
// queries. When enabled and a replicaShardMgr is available, SELECTs are
// routed to the replica instead of the primary.
func (e *Executor) SetReadFromReplica(enabled bool) {
	e.readFromReplica = enabled
}

// shardMgrForRead returns the appropriate shard manager for read operations.
// If read-from-replica is enabled and a replica manager is available, it
// returns the replica manager; otherwise it returns the primary manager.
func (e *Executor) shardMgrForRead() *storage.ShardManager {
	if e.readFromReplica && e.replicaShardMgr != nil {
		return e.replicaShardMgr
	}
	return e.shardMgr
}

// SetFunctionCatalog sets the PL/pgSQL function catalog used for
// CREATE FUNCTION, CALL, and DO block execution.
func (e *Executor) SetFunctionCatalog(fc *plpgsql.FunctionCatalog) {
	e.funcCatalog = fc
}

// FunctionCatalog returns the PL/pgSQL function catalog.
func (e *Executor) FunctionCatalog() *plpgsql.FunctionCatalog {
	return e.funcCatalog
}

// PlacementQuerier is the interface for querying shard placement.
// It is satisfied by replication.PlacementManager.
type PlacementQuerier interface {
	GetPrimary(table string, shardID int) string
	GetReplicas(table string, shardID int) []string
}

// SetScatterExecutor sets the scatter executor for distributed query fan-out.
func (e *Executor) SetScatterExecutor(se *ScatterExecutor) {
	e.scatterExec = se
}

// SetPlacementManager sets the placement manager for local/remote shard routing.
func (e *Executor) SetPlacementManager(pm PlacementQuerier) {
	e.placementMgr = pm
}

// --------------------------------------------------------------------------
// SQLExecutor interface implementation — used by the PL/pgSQL interpreter
// to execute SQL queries against the shard layer.
// --------------------------------------------------------------------------

// sqlExecResult wraps RowsAffected for the plpgsql.SQLResult interface.
type sqlExecResult struct {
	affected int64
}

func (r *sqlExecResult) RowsAffected() (int64, error) {
	return r.affected, nil
}

// ExecSQL executes a non-query SQL statement across the first available shard.
// This satisfies the plpgsql.SQLExecutor interface.
func (e *Executor) ExecSQL(ctx context.Context, sql string, args ...interface{}) (plpgsql.SQLResult, error) {
	// For PL/pgSQL, execute on shard 0 of the internal "__plpgsql" virtual table.
	// This provides a local SQLite context for procedural SQL operations.
	table := "__plpgsql"
	shardID := 0

	// Attempt to extract table name from the SQL for proper shard routing.
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upperSQL, "INSERT INTO ") || strings.HasPrefix(upperSQL, "UPDATE ") || strings.HasPrefix(upperSQL, "DELETE FROM ") {
		// Parse a quick Statement for routing.
		stmts, err := (&parser.Parser{}).Parse(sql)
		if err == nil && len(stmts) > 0 && len(stmts[0].Tables) > 0 {
			table = stmts[0].Tables[0]
		}
	}

	affected, err := e.shardMgr.ExecReturn(ctx, table, shardID, sql)
	if err != nil {
		return nil, fmt.Errorf("plpgsql ExecSQL: %w", err)
	}
	return &sqlExecResult{affected: affected}, nil
}

// QuerySQL executes a SQL query and returns results as maps. This satisfies
// the plpgsql.SQLExecutor interface.
func (e *Executor) QuerySQL(ctx context.Context, sql string, args ...interface{}) ([]map[string]interface{}, error) {
	// Route to the appropriate shard. For PL/pgSQL, use shard 0 by default.
	table := "__plpgsql"
	shardID := 0

	// Try to extract the table name for proper routing.
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))
	if strings.HasPrefix(upperSQL, "SELECT") || strings.HasPrefix(upperSQL, "WITH") {
		stmts, err := (&parser.Parser{}).Parse(sql)
		if err == nil && len(stmts) > 0 && len(stmts[0].Tables) > 0 {
			table = stmts[0].Tables[0]
		}
	}

	mgr := e.shardMgrForRead()
	cols, rows, err := mgr.Query(ctx, table, shardID, sql)
	if err != nil {
		return nil, fmt.Errorf("plpgsql QuerySQL: %w", err)
	}

	// Convert flat [][]byte results into maps.
	if len(cols) == 0 {
		return nil, nil
	}

	numCols := len(cols)
	numRows := len(rows) / numCols
	result := make([]map[string]interface{}, 0, numRows)

	for i := 0; i < numRows; i++ {
		row := make(map[string]interface{})
		for j, col := range cols {
			idx := i*numCols + j
			if idx < len(rows) {
				if rows[idx] == nil {
					row[col] = nil
				} else {
					row[col] = string(rows[idx])
				}
			}
		}
		result = append(result, row)
	}

	return result, nil
}

// --------------------------------------------------------------------------
// PL/pgSQL execution entry points
// --------------------------------------------------------------------------

// ExecutePLpgSQL executes a PL/pgSQL function with the given arguments and
// returns the result along with any NOTICE messages generated during execution.
func (e *Executor) ExecutePLpgSQL(ctx context.Context, funcDef *plpgsql.FunctionDef, args []plpgsql.PLValue) (*Result, []plpgsql.NoticeMessage, error) {
	if e.funcCatalog == nil {
		return nil, nil, fmt.Errorf("PL/pgSQL function catalog not initialized")
	}

	interp := plpgsql.NewInterpreter(e.funcCatalog, e)
	result, err := interp.ExecuteFunction(ctx, funcDef, args)
	if err != nil {
		return nil, interp.Notices, fmt.Errorf("PL/pgSQL execution error: %w", err)
	}

	// Convert the PLValue result to an executor.Result.
	execResult := &Result{}

	if funcDef.ReturnsSetOf {
		// SETOF result — result.Value is []map[string]interface{}.
		if resultRows, ok := result.Value.([]map[string]interface{}); ok && len(resultRows) > 0 {
			// Build columns from first row's keys.
			var colNames []string
			for k := range resultRows[0] {
				colNames = append(colNames, k)
			}
			sort.Strings(colNames)

			var colDefs []types.ColumnDef
			for _, name := range colNames {
				colDefs = append(colDefs, types.ColumnDef{
					Name: name, OID: 25, TypeSize: -1, TypeMod: -1, Format: 0,
				})
			}

			var dataRows [][]byte
			for _, row := range resultRows {
				for _, col := range colNames {
					if v, ok := row[col]; ok && v != nil {
						dataRows = append(dataRows, []byte(fmt.Sprintf("%v", v)))
					} else {
						dataRows = append(dataRows, nil)
					}
				}
			}

			execResult.Columns = colDefs
			execResult.Rows = dataRows
			execResult.Tag = fmt.Sprintf("SELECT %d", len(resultRows))
		} else {
			execResult.Tag = "SELECT 0"
		}
	} else if funcDef.ReturnType != "" && funcDef.ReturnType != "void" {
		// Scalar result.
		colDefs := []types.ColumnDef{
			{Name: funcDef.Name, OID: 25, TypeSize: -1, TypeMod: -1, Format: 0},
		}
		var dataRows [][]byte
		if result.IsNull {
			dataRows = append(dataRows, nil)
		} else {
			dataRows = append(dataRows, []byte(result.String()))
		}
		execResult.Columns = colDefs
		execResult.Rows = dataRows
		execResult.Tag = "SELECT 1"
	} else {
		// Void / procedure — no result set.
		execResult.Tag = "CALL"
	}

	return execResult, interp.Notices, nil
}

// ExecuteDO executes an anonymous DO $$ ... $$ block and returns any
// NOTICE messages generated during execution.
func (e *Executor) ExecuteDO(ctx context.Context, body string) ([]plpgsql.NoticeMessage, error) {
	if e.funcCatalog == nil {
		return nil, fmt.Errorf("PL/pgSQL function catalog not initialized")
	}

	block, err := plpgsql.ParseDO(body)
	if err != nil {
		return nil, fmt.Errorf("parse DO block: %w", err)
	}

	interp := plpgsql.NewInterpreter(e.funcCatalog, e)
	if err := interp.ExecuteDOBlock(ctx, block); err != nil {
		return interp.Notices, fmt.Errorf("DO block execution error: %w", err)
	}

	return interp.Notices, nil
}

// ExecuteCall executes a CALL to a named procedure/function with the given
// string arguments. Arguments are evaluated as PL/pgSQL expressions.
func (e *Executor) ExecuteCall(ctx context.Context, funcName string, argExprs []string) (*Result, []plpgsql.NoticeMessage, error) {
	if e.funcCatalog == nil {
		return nil, nil, fmt.Errorf("PL/pgSQL function catalog not initialized")
	}

	// Look up the function in the catalog.
	funcDef, err := e.funcCatalog.LookupByArity(funcName, len(argExprs))
	if err != nil {
		return nil, nil, fmt.Errorf("CALL %s: %w", funcName, err)
	}

	// Convert string argument expressions to PLValues using a temporary
	// interpreter for expression evaluation.
	interp := plpgsql.NewInterpreter(e.funcCatalog, e)
	args := make([]plpgsql.PLValue, len(argExprs))
	scope := plpgsql.NewScope()
	for i, argExpr := range argExprs {
		// Evaluate each argument as a PL/pgSQL expression. This handles
		// literals (42, 'hello', TRUE, NULL) and simple expressions.
		val, evalErr := interp.EvalExprPublic(ctx, scope, argExpr)
		if evalErr != nil {
			return nil, nil, fmt.Errorf("CALL %s arg %d: %w", funcName, i+1, evalErr)
		}
		args[i] = val
	}

	return e.ExecutePLpgSQL(ctx, funcDef, args)
}

// ExecuteCreateFunction parses and registers a CREATE FUNCTION/PROCEDURE
// statement in the function catalog.
func (e *Executor) ExecuteCreateFunction(ctx context.Context, createSQL string) (*Result, error) {
	if e.funcCatalog == nil {
		return nil, fmt.Errorf("PL/pgSQL function catalog not initialized")
	}

	funcDef, err := plpgsql.ParseFunctionDef(createSQL)
	if err != nil {
		return nil, fmt.Errorf("parse CREATE FUNCTION: %w", err)
	}

	if err := e.funcCatalog.Register(funcDef); err != nil {
		return nil, fmt.Errorf("register function %q: %w", funcDef.Name, err)
	}

	tag := "CREATE FUNCTION"
	if funcDef.IsProc {
		tag = "CREATE PROCEDURE"
	}

	log.Printf("plpgsql: registered %s %s (%d params)", tag, funcDef.Name, len(funcDef.Params))
	return &Result{Tag: tag}, nil
}

// Execute dispatches a plan and returns the result.
func (e *Executor) Execute(ctx context.Context, plan *planner.Plan) (*Result, error) {
	// Enforce timeout from context (P.12.10).
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("query timeout exceeded")
		}
	}

	switch plan.Type {
	case planner.PlanSingleShard:
		return e.executeSingleShard(ctx, plan)
	case planner.PlanScatter:
		return e.executeScatter(ctx, plan)
	case planner.PlanDDL:
		return e.executeDDL(ctx, plan)
	case planner.PlanLocal:
		return e.executeLocal(ctx, plan)
	case planner.PlanExplain:
		return e.executeExplain(ctx, plan)
	case planner.PlanJoin:
		return e.executeJoin(ctx, plan)
	default:
		return nil, fmt.Errorf("unsupported plan type: %d", plan.Type)
	}
}

// executeSingleShard executes the query on exactly one shard.
func (e *Executor) executeSingleShard(ctx context.Context, plan *planner.Plan) (*Result, error) {
	if len(plan.Shards) == 0 {
		return nil, fmt.Errorf("single shard plan has no target shard")
	}
	shardID := plan.Shards[0]
	table := ""
	if len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}

	return e.executeOnShard(ctx, shardID, table, plan)
}

// executeOnShard executes a plan on a specific local shard.
func (e *Executor) executeOnShard(ctx context.Context, shardID int, table string, plan *planner.Plan) (*Result, error) {
	sql := planner.BuildShardSQL(plan)

	// Route SELECT queries to replica if configured.
	mgr := e.shardMgr
	if plan.Statement != nil && plan.Statement.Type == parser.StmtSelect {
		mgr = e.shardMgrForRead()
	}

	cols, rows, err := mgr.Query(ctx, table, shardID, sql)
	if err != nil {
		return nil, fmt.Errorf("shard %d query: %w", shardID, err)
	}

	// Build column definitions.
	var colDefs []types.ColumnDef
	for _, c := range cols {
		colDefs = append(colDefs, types.ColumnDef{
			Name:     c,
			OID:      25, // text
			TypeSize: -1,
			TypeMod:  -1,
			Format:   0,
		})
	}

	// rows is a flat [][]byte of column values across all rows.
	// Each group of len(cols) values forms one row.
	dataRows := rows
	rowCount := 0
	if len(cols) > 0 {
		rowCount = len(rows) / len(cols)
	}

	tag := buildTag(plan.Statement, rowCount)
	return &Result{
		Columns: colDefs,
		Rows:    dataRows,
		Tag:     tag,
	}, nil
}

// shardQueryResult holds the result of querying a single shard.
type shardQueryResult struct {
	cols []string
	rows [][]byte
	err  error
}

// isMultiShardWrite returns true if the plan is a scatter write (INSERT/UPDATE/DELETE)
// targeting more than one shard.
func (e *Executor) isMultiShardWrite(plan *planner.Plan) bool {
	if plan.Statement == nil {
		return false
	}
	switch plan.Statement.Type {
	case parser.StmtInsert, parser.StmtUpdate, parser.StmtDelete:
		// Multi-shard if more than one shard is targeted or all shards
		// (empty Shards means all).
		shardCount := len(plan.Shards)
		if shardCount == 0 {
			shardCount = e.shardMgr.ShardCount()
		}
		return shardCount > 1
	default:
		return false
	}
}

// executeScatter fans out the query to multiple shards and merges results.
// For multi-shard writes, it uses 2PC (when a TxnCoordinator is available)
// to ensure atomicity across shards.
func (e *Executor) executeScatter(ctx context.Context, plan *planner.Plan) (*Result, error) {
	shards := plan.Shards
	if len(shards) == 0 {
		// All shards.
		shards = allShardIDs(e.shardMgr.ShardCount())
	}

	table := ""
	if len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}

	sqlStr := planner.BuildShardSQL(plan)

	// Multi-shard write → 2PC path.
	if e.isMultiShardWrite(plan) && e.txnCoordinator != nil {
		return e.executeScatterWith2PC(ctx, plan, shards, table, sqlStr)
	}

	// Distributed path (ScatterExecutor available).
	if e.scatterExec != nil {
		return e.executeScatterViaSwarm(ctx, plan, shards)
	}

	// Fallback: local-only (existing goroutine logic).
	return e.executeScatterLocal(ctx, plan, shards)
}

// executeScatterViaSwarm dispatches scatter queries via the swarm, splitting
// shards into local and remote sets based on placement info.
func (e *Executor) executeScatterViaSwarm(ctx context.Context, plan *planner.Plan, shards []int) (*Result, error) {
	local, remote := e.partitionShards(plan, shards)

	var allResults []ScatterQueryResult

	// Local shards — direct shard manager query.
	if len(local) > 0 {
		localRes, err := e.scatterExec.FanOutLocal(ctx, plan, local, func(ctx context.Context, table string, shardID int, sql string) ([]string, [][]byte, error) {
			mgr := e.shardMgr
			if plan.Statement != nil && plan.Statement.Type == parser.StmtSelect {
				mgr = e.shardMgrForRead()
			}
			return mgr.Query(ctx, table, shardID, sql)
		})
		if err != nil {
			return nil, fmt.Errorf("local scatter: %w", err)
		}
		allResults = append(allResults, localRes...)
	}

	// Remote shards — swarm scatter via ScatterExecutor.FanOut().
	if len(remote) > 0 {
		remoteRes, err := e.scatterExec.FanOut(ctx, plan, remote)
		if err != nil {
			return nil, fmt.Errorf("remote scatter: %w", err)
		}
		allResults = append(allResults, remoteRes...)
	}

	// Merge results using pushdown-aware merge.
	return e.mergeScatterResults(ctx, plan, allResults)
}

// executeScatterLocal is the fallback local-only scatter path (no swarm).
func (e *Executor) executeScatterLocal(ctx context.Context, plan *planner.Plan, shards []int) (*Result, error) {
	table := ""
	if len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}
	sqlStr := planner.BuildShardSQL(plan)

	// Fan out to shards (P.12.10: context propagation for cancellation).
	results := make([]shardQueryResult, len(shards))
	done := make(chan int, len(shards))

	for i, shardID := range shards {
		go func(idx, sid int) {
			cols, rows, err := e.shardMgr.Query(ctx, table, sid, sqlStr)
			results[idx] = shardQueryResult{cols: cols, rows: rows, err: err}
			done <- idx
		}(i, shardID)
	}

	// Gather results.
	for range shards {
		select {
		case <-done:
		case <-ctx.Done():
			return nil, fmt.Errorf("scatter timeout: %w", ctx.Err())
		}
	}

	// Convert to ScatterQueryResult for unified merge path.
	var scatterResults []ScatterQueryResult
	for _, r := range results {
		if r.err != nil {
			log.Printf("scatter: shard error: %v", r.err)
			scatterResults = append(scatterResults, ScatterQueryResult{Error: r.err.Error()})
			continue
		}
		// Convert flat [][]byte to [][]string rows.
		var strRows [][]string
		numCols := len(r.cols)
		if numCols > 0 {
			for ri := 0; ri+numCols <= len(r.rows); ri += numCols {
				strRow := make([]string, numCols)
				for c := 0; c < numCols; c++ {
					if r.rows[ri+c] != nil {
						strRow[c] = string(r.rows[ri+c])
					}
				}
				strRows = append(strRows, strRow)
			}
		}
		scatterResults = append(scatterResults, ScatterQueryResult{Columns: r.cols, Rows: strRows})
	}

	return e.mergeScatterResults(ctx, plan, scatterResults)
}

// partitionShards splits shard IDs into local and remote sets based on
// placement data. Without a PlacementManager, all shards are treated as local.
func (e *Executor) partitionShards(plan *planner.Plan, shards []int) (local, remote []int) {
	if e.placementMgr == nil {
		return shards, nil // all local until placement is wired
	}
	table := ""
	if plan.Statement != nil && len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}
	localNode := e.client.NodeID()
	for _, sid := range shards {
		primary := e.placementMgr.GetPrimary(table, sid)
		if primary == localNode || primary == "" {
			local = append(local, sid)
		} else {
			remote = append(remote, sid)
		}
	}
	return local, remote
}

// allShardIDs returns a slice [0, 1, ..., n-1].
func allShardIDs(n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	return ids
}

// executeScatterWith2PC executes a multi-shard write using 2-Phase Commit
// for atomicity. If any shard fails to prepare, all shards are rolled back.
func (e *Executor) executeScatterWith2PC(ctx context.Context, plan *planner.Plan, shards []int, table, sqlStr string) (*Result, error) {
	// Build shard targets for the 2PC coordinator.
	targets := make([]txn.ShardTarget, len(shards))
	for i, shardID := range shards {
		targets[i] = txn.ShardTarget{
			Table:   table,
			ShardID: shardID,
			SQL:     sqlStr,
		}
	}

	// Execute the full 2PC cycle: prepare -> commit/abort.
	if err := e.txnCoordinator.ExecuteDistributed(ctx, targets); err != nil {
		return nil, fmt.Errorf("2PC scatter write: %w", err)
	}

	tag := buildTag(plan.Statement, len(shards))
	return &Result{Tag: tag}, nil
}

// executeDDL scatters DDL across all shards and updates the catalog.
func (e *Executor) executeDDL(ctx context.Context, plan *planner.Plan) (*Result, error) {
	ddl := plan.DDL
	table := ""
	if len(plan.Statement.Tables) > 0 {
		table = plan.Statement.Tables[0]
	}

	// Execute on all local shards.
	shardCount := e.shardMgr.ShardCount()
	var lastErr error
	successCount := 0
	for i := 0; i < shardCount; i++ {
		if err := e.shardMgr.Exec(ctx, table, i, ddl); err != nil {
			lastErr = err
			log.Printf("DDL shard %d error: %v", i, err)
		} else {
			successCount++
		}
	}

	if successCount == 0 && lastErr != nil {
		return nil, fmt.Errorf("DDL failed on all shards: %w", lastErr)
	}

	// Update catalog.
	switch plan.Statement.Type {
	case parser.StmtCreateTable:
		e.catalog.RegisterTable(table, ddl)
	case parser.StmtDropTable:
		e.catalog.UnregisterTable(table)
	case parser.StmtCreateIndex:
		e.catalog.RegisterIndex(table, ddl)
	}

	tag := "OK"
	switch plan.Statement.Type {
	case parser.StmtCreateTable:
		tag = "CREATE TABLE"
	case parser.StmtDropTable:
		tag = "DROP TABLE"
	case parser.StmtCreateIndex:
		tag = "CREATE INDEX"
	case parser.StmtAlterTable:
		tag = "ALTER TABLE"
	}

	return &Result{Tag: tag}, nil
}

// executeLocal handles local-only operations (SET, SHOW, BEGIN, etc.).
func (e *Executor) executeLocal(_ context.Context, plan *planner.Plan) (*Result, error) {
	return &Result{Tag: plan.LocalTag}, nil
}

// executeExplain returns the query plan as rows (P.12.11).
func (e *Executor) executeExplain(_ context.Context, plan *planner.Plan) (*Result, error) {
	rows := planner.ExplainResult(plan, e.shardMgr.ShardCount())
	cols := planner.ExplainColumns()

	// Wrap each row in the DataRow format expected by the wire layer.
	var dataRows [][]byte
	for _, row := range rows {
		dataRows = append(dataRows, row)
	}

	return &Result{
		Columns: cols,
		Rows:    dataRows,
		Tag:     fmt.Sprintf("EXPLAIN %d", len(rows)),
	}, nil
}

// executeJoin handles distributed joins (P.12.7).
func (e *Executor) executeJoin(ctx context.Context, plan *planner.Plan) (*Result, error) {
	switch plan.Strategy {
	case planner.JoinColocated:
		// Co-located join: execute the full query on each shard and merge.
		return e.executeScatter(ctx, plan)

	case planner.JoinBroadcast:
		// Broadcast join: scatter small table, join locally on each shard.
		return e.executeScatter(ctx, plan)

	case planner.JoinShuffle:
		// Shuffle join: redistribute both tables by join key, then join.
		// This is the most expensive strategy.
		return e.executeScatter(ctx, plan)

	default:
		return nil, fmt.Errorf("unknown join strategy: %d", plan.Strategy)
	}
}

// MergeAggregateResults merges partial aggregate results from multiple shards.
// Handles COUNT (sum of counts), SUM (sum of sums), MIN (min of mins),
// MAX (max of maxes), AVG (total sum / total count) (P.12.4).
//
// For non-AVG aggregates, each partial row has one column per aggregate at
// the corresponding offset. For AVG, shards return two columns (_sum, _count)
// instead of one, so the column offset advances by 2 for each AVG aggregate.
func MergeAggregateResults(mergeAggs []planner.MergeAgg, partialRows [][]string) []string {
	if len(partialRows) == 0 || len(mergeAggs) == 0 {
		return nil
	}

	result := make([]string, len(mergeAggs))

	// colOffset tracks the current column position in the partial rows.
	// Non-AVG aggregates occupy 1 column; AVG occupies 2 (_sum, _count).
	colOffset := 0

	for i, ma := range mergeAggs {
		switch ma.Func {
		case parser.AggCount:
			total := int64(0)
			for _, row := range partialRows {
				if colOffset < len(row) {
					n, _ := strconv.ParseInt(row[colOffset], 10, 64)
					total += n
				}
			}
			result[i] = strconv.FormatInt(total, 10)
			colOffset++

		case parser.AggSum:
			total := 0.0
			for _, row := range partialRows {
				if colOffset < len(row) {
					n, _ := strconv.ParseFloat(row[colOffset], 64)
					total += n
				}
			}
			result[i] = strconv.FormatFloat(total, 'f', -1, 64)
			colOffset++

		case parser.AggMin:
			var minVal *float64
			for _, row := range partialRows {
				if colOffset < len(row) {
					n, _ := strconv.ParseFloat(row[colOffset], 64)
					if minVal == nil || n < *minVal {
						minVal = &n
					}
				}
			}
			if minVal != nil {
				result[i] = strconv.FormatFloat(*minVal, 'f', -1, 64)
			}
			colOffset++

		case parser.AggMax:
			var maxVal *float64
			for _, row := range partialRows {
				if colOffset < len(row) {
					n, _ := strconv.ParseFloat(row[colOffset], 64)
					if maxVal == nil || n > *maxVal {
						maxVal = &n
					}
				}
			}
			if maxVal != nil {
				result[i] = strconv.FormatFloat(*maxVal, 'f', -1, 64)
			}
			colOffset++

		case parser.AggAvg:
			// AVG is computed from SUM/COUNT pushed to shards.
			// Shards return two columns: _sum at colOffset, _count at colOffset+1.
			totalSum := 0.0
			totalCount := int64(0)
			for _, row := range partialRows {
				if colOffset+1 < len(row) {
					s, _ := strconv.ParseFloat(row[colOffset], 64)
					c, _ := strconv.ParseInt(row[colOffset+1], 10, 64)
					totalSum += s
					totalCount += c
				}
			}
			if totalCount > 0 {
				avg := totalSum / float64(totalCount)
				result[i] = strconv.FormatFloat(avg, 'f', -1, 64)
			} else {
				result[i] = "0"
			}
			colOffset += 2 // AVG occupies 2 columns in partial results
		}
	}
	return result
}

// MergeLimitOffset applies the final LIMIT+OFFSET on merged, sorted results (P.12.5).
// colNames maps column names to their indices in the row slices, enabling
// ORDER BY resolution by name. Pass nil to fall back to positional ordering
// (column 0, 1, 2, ...).
func MergeLimitOffset(rows [][]string, orderCols []parser.OrderByClause, limit, offset int64) [][]string {
	// Sort if ORDER BY is specified.
	if len(orderCols) > 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			for ocIdx, oc := range orderCols {
				colIdx := ocIdx // Use positional index by default.
				if colIdx >= len(rows[i]) || colIdx >= len(rows[j]) {
					continue
				}
				cmp := strings.Compare(rows[i][colIdx], rows[j][colIdx])
				if cmp != 0 {
					if oc.Desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})
	}

	// Apply OFFSET.
	if offset > 0 {
		if offset >= int64(len(rows)) {
			return nil
		}
		rows = rows[offset:]
	}

	// Apply LIMIT.
	if limit > 0 && limit < int64(len(rows)) {
		rows = rows[:limit]
	}

	return rows
}

// MergeGroupBy merges partial GROUP BY results by grouping key (P.12.6).
// Each partial row layout: [group_col_0, group_col_1, ..., agg_val_0, agg_val_1, ...]
// groupCols are the indices of the group key columns. Aggregate values start
// after the last group column.
func MergeGroupBy(partials [][]string, groupCols []int, aggCols []planner.MergeAgg) [][]string {
	if len(partials) == 0 {
		return nil
	}

	// Build a set of group column indices for fast lookup.
	groupColSet := make(map[int]bool, len(groupCols))
	for _, gi := range groupCols {
		groupColSet[gi] = true
	}

	// Group by key.
	groups := make(map[string][][]string)
	for _, row := range partials {
		var keyParts []string
		for _, gi := range groupCols {
			if gi < len(row) {
				keyParts = append(keyParts, row[gi])
			}
		}
		key := strings.Join(keyParts, "\x00")
		groups[key] = append(groups[key], row)
	}

	// Merge each group.
	var result [][]string
	for _, groupRows := range groups {
		if len(groupRows) == 0 {
			continue
		}

		// Extract only aggregate columns (non-group columns) for merging.
		var aggOnlyRows [][]string
		for _, gr := range groupRows {
			var aggRow []string
			for ci, val := range gr {
				if !groupColSet[ci] {
					aggRow = append(aggRow, val)
				}
			}
			aggOnlyRows = append(aggOnlyRows, aggRow)
		}

		merged := MergeAggregateResults(aggCols, aggOnlyRows)
		// Prepend group key columns.
		row := make([]string, 0, len(groupCols)+len(merged))
		for _, gi := range groupCols {
			if gi < len(groupRows[0]) {
				row = append(row, groupRows[0][gi])
			}
		}
		row = append(row, merged...)
		result = append(result, row)
	}

	return result
}

// mergeScatterResults applies pushdown-aware merging on scatter results.
// It calls MergeGroupBy, MergeAggregateResults, and MergeLimitOffset based
// on the plan's pushdown flags, then converts back to an executor.Result.
func (e *Executor) mergeScatterResults(_ context.Context, plan *planner.Plan, results []ScatterQueryResult) (*Result, error) {
	// 1. Collect columns + rows from successful shards.
	var colNames []string
	var allRows [][]string
	for _, r := range results {
		if r.Error != "" {
			log.Printf("scatter: shard error: %s", r.Error)
			continue
		}
		if colNames == nil && len(r.Columns) > 0 {
			colNames = r.Columns
		}
		allRows = append(allRows, r.Rows...)
	}

	merged := allRows

	// 2. GROUP BY merge (must precede pure-aggregate merge).
	if plan.PushdownGroupBy && plan.Statement != nil && len(plan.Statement.GroupBy) > 0 &&
		len(plan.MergeAggregates) > 0 {
		groupCols := groupByIndices(colNames, plan.Statement.GroupBy)
		merged = MergeGroupBy(merged, groupCols, plan.MergeAggregates)

	} else if plan.PushdownAgg && len(plan.MergeAggregates) > 0 {
		// 3. Pure aggregate merge (no GROUP BY).
		row := MergeAggregateResults(plan.MergeAggregates, merged)
		if row != nil {
			merged = [][]string{row}
		}
	}

	// 4. LIMIT / OFFSET merge (always last).
	if plan.PushdownLimit {
		merged = MergeLimitOffset(
			merged, plan.Statement.OrderBy,
			plan.MergeLimit, plan.MergeOffset,
		)
	}

	// 5. Convert [][]string back to flat [][]byte + ColumnDefs.
	return buildResultFromStrings(colNames, merged, plan.Statement), nil
}

// groupByIndices maps GROUP BY column references to their indices in the
// result set column list.
func groupByIndices(colNames []string, groupBy []parser.ColumnRef) []int {
	nameIndex := make(map[string]int, len(colNames))
	for i, name := range colNames {
		nameIndex[strings.ToLower(name)] = i
	}
	var indices []int
	for _, col := range groupBy {
		name := strings.ToLower(col.Column)
		if idx, ok := nameIndex[name]; ok {
			indices = append(indices, idx)
		}
	}
	return indices
}

// buildResultFromStrings converts column names and string rows into an
// executor.Result with proper ColumnDefs and flat [][]byte layout.
func buildResultFromStrings(colNames []string, rows [][]string, stmt *parser.Statement) *Result {
	var colDefs []types.ColumnDef
	for _, name := range colNames {
		colDefs = append(colDefs, types.ColumnDef{
			Name: name, OID: 25, TypeSize: -1, TypeMod: -1, Format: 0,
		})
	}

	var dataRows [][]byte
	for _, row := range rows {
		for _, val := range row {
			if val == "" {
				dataRows = append(dataRows, nil)
			} else {
				dataRows = append(dataRows, []byte(val))
			}
		}
	}

	tag := buildTag(stmt, len(rows))
	return &Result{
		Columns: colDefs,
		Rows:    dataRows,
		Tag:     tag,
	}
}

// buildTag constructs a CommandComplete tag string.
func buildTag(stmt *parser.Statement, rowCount int) string {
	switch stmt.Type {
	case parser.StmtSelect:
		return fmt.Sprintf("SELECT %d", rowCount)
	case parser.StmtInsert:
		return fmt.Sprintf("INSERT 0 %d", rowCount)
	case parser.StmtUpdate:
		return fmt.Sprintf("UPDATE %d", rowCount)
	case parser.StmtDelete:
		return fmt.Sprintf("DELETE %d", rowCount)
	default:
		return "OK"
	}
}
