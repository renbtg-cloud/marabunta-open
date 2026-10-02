// Marabunta - Licensed under the MIT License.
// Package parser provides SQL parsing for the distributed PostgreSQL plugin.
// It wraps pg_query_go to parse PostgreSQL-compatible SQL into an AST, then
// converts it to an internal Statement representation suitable for planning.
package parser

import (
	"fmt"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

// StatementType identifies the kind of SQL statement.
type StatementType int

const (
	StmtSelect StatementType = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtCreateTable
	StmtDropTable
	StmtCreateIndex
	StmtAlterTable
	StmtExplain
	StmtBegin
	StmtCommit
	StmtRollback
	StmtSet
	StmtShow
	StmtUnknown
	StmtCreateFunction  // CREATE FUNCTION or CREATE PROCEDURE
	StmtCall            // CALL procedure_name(args)
	StmtDO              // DO $$ ... $$
)

// AggFunc identifies aggregate functions for pushdown.
type AggFunc int

const (
	AggNone AggFunc = iota
	AggCount
	AggSum
	AggMin
	AggMax
	AggAvg
)

// SetClause represents a single column=expression pair in an UPDATE SET clause.
type SetClause struct {
	Column string
	Value  string
}

// Statement is the internal representation of a parsed SQL statement.
type Statement struct {
	Type        StatementType
	RawSQL      string
	Tables      []string
	Columns     []ColumnRef
	Where       *WhereClause
	GroupBy     []ColumnRef
	OrderBy     []OrderByClause
	Limit       int64
	Offset      int64
	HasLimit    bool
	Aggregates  []AggregateRef
	JoinClauses []JoinClause
	InsertRows  [][]string
	DDL         string
	SetKey      string
	SetValue    string
	ExplainStmt *Statement

	// PL/pgSQL fields — populated for StmtCreateFunction, StmtCall, StmtDO.
	FuncName     string   // function/procedure name (for CALL / CREATE FUNCTION)
	FuncArgs     []string // argument expressions (for CALL)
	FuncBody     string   // raw body text between dollar quotes (for CREATE FUNCTION / DO)
	IsProc       bool     // true for CREATE PROCEDURE / CALL
	CreateSQL    string   // full CREATE FUNCTION/PROCEDURE SQL (for parsing by plpgsql)

	// New fields for previously-missing features.
	Having       *WhereClause  // HAVING clause
	Distinct     bool          // DISTINCT present
	SetOp        string        // UNION / INTERSECT / EXCEPT
	CTEs         []string      // CTE names from WITH clause
	Returning    []ColumnRef   // RETURNING clause columns
	OnConflict   bool          // INSERT ... ON CONFLICT present
	InsertSelect bool          // INSERT ... SELECT (no VALUES)
	SetClauses   []SetClause   // UPDATE ... SET column=value pairs
}

// ColumnRef references a column, optionally qualified by table.
type ColumnRef struct {
	Table  string
	Column string
	Alias  string
}

// WhereClause represents the WHERE condition tree.
type WhereClause struct {
	Op       string        // "=", "<>", "<", ">", "<=", ">=", "IN", "AND", "OR", "NOT", "LIKE", "IS NULL", "IS NOT NULL", "BETWEEN"
	Column   ColumnRef
	Value    string        // literal value for simple comparisons
	Values   []string      // values for IN clause
	Children []*WhereClause // children for AND/OR/NOT
}

// OrderByClause specifies column ordering.
type OrderByClause struct {
	Column ColumnRef
	Desc   bool
}

// AggregateRef references an aggregate function call.
type AggregateRef struct {
	Func   AggFunc
	Column ColumnRef
	Alias  string
}

// JoinType identifies the type of join.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
	JoinCross
)

// JoinClause describes a join between two tables.
type JoinClause struct {
	Type      JoinType
	Table     string
	Alias     string
	On        *WhereClause
	LeftCol   ColumnRef
	RightCol  ColumnRef
}

// Parser parses SQL strings into internal Statement representations.
type Parser struct{}

// New creates a new Parser.
func New() *Parser {
	return &Parser{}
}

// Parse parses a SQL string that may contain multiple semicolon-separated
// statements. Returns a slice of Statement for each statement found.
func (p *Parser) Parse(sql string) ([]*Statement, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, nil
	}

	// Use pg_query_go to parse the SQL into a real PostgreSQL AST.
	tree, err := pgquery.Parse(sql)
	if err != nil {
		// If pg_query_go fails (e.g. on truly malformed input), fall back to
		// the legacy string-based parser for robustness.
		return p.parseLegacy(sql)
	}

	if tree == nil || len(tree.Stmts) == 0 {
		return nil, nil
	}

	var stmts []*Statement
	for _, rawStmt := range tree.Stmts {
		if rawStmt.Stmt == nil {
			continue
		}
		// Extract the raw SQL substring for this statement.
		stmtSQL := extractStmtSQL(sql, rawStmt)

		stmt, convErr := p.convertNode(rawStmt.Stmt, stmtSQL)
		if convErr != nil {
			return nil, fmt.Errorf("parse %q: %w", truncate(stmtSQL, 80), convErr)
		}
		stmts = append(stmts, stmt)
	}
	return stmts, nil
}

// extractStmtSQL extracts the SQL text for a single statement from the
// full query string using the RawStmt location and length fields.
func extractStmtSQL(fullSQL string, rawStmt *pgquery.RawStmt) string {
	start := int(rawStmt.StmtLocation)
	length := int(rawStmt.StmtLen)

	if start < 0 || start >= len(fullSQL) {
		return fullSQL
	}
	if length <= 0 {
		// Length 0 means until end of string (last statement).
		return strings.TrimSpace(fullSQL[start:])
	}
	end := start + length
	if end > len(fullSQL) {
		end = len(fullSQL)
	}
	return strings.TrimSpace(fullSQL[start:end])
}

// convertNode converts a pg_query_go Node into an internal Statement.
func (p *Parser) convertNode(node *pgquery.Node, rawSQL string) (*Statement, error) {
	stmt := &Statement{RawSQL: rawSQL}

	switch {
	case node.GetSelectStmt() != nil:
		return p.convertSelect(node.GetSelectStmt(), stmt)

	case node.GetInsertStmt() != nil:
		return p.convertInsert(node.GetInsertStmt(), stmt)

	case node.GetUpdateStmt() != nil:
		return p.convertUpdate(node.GetUpdateStmt(), stmt)

	case node.GetDeleteStmt() != nil:
		return p.convertDelete(node.GetDeleteStmt(), stmt)

	case node.GetCreateStmt() != nil:
		return p.convertCreateTable(node.GetCreateStmt(), stmt)

	case node.GetDropStmt() != nil:
		return p.convertDrop(node.GetDropStmt(), stmt)

	case node.GetIndexStmt() != nil:
		return p.convertCreateIndex(node.GetIndexStmt(), stmt)

	case node.GetAlterTableStmt() != nil:
		return p.convertAlterTable(node.GetAlterTableStmt(), stmt)

	case node.GetExplainStmt() != nil:
		return p.convertExplain(node.GetExplainStmt(), stmt)

	case node.GetTransactionStmt() != nil:
		return p.convertTransaction(node.GetTransactionStmt(), stmt)

	case node.GetVariableSetStmt() != nil:
		return p.convertVariableSet(node.GetVariableSetStmt(), stmt)

	case node.GetVariableShowStmt() != nil:
		stmt.Type = StmtShow
		return stmt, nil

	case node.GetCreateFunctionStmt() != nil:
		return p.convertCreateFunction(node.GetCreateFunctionStmt(), stmt)

	case node.GetDoStmt() != nil:
		return p.convertDO(node.GetDoStmt(), stmt)

	case node.GetCallStmt() != nil:
		return p.convertCall(node.GetCallStmt(), stmt)

	default:
		stmt.Type = StmtUnknown
		return stmt, nil
	}
}

// --------------------------------------------------------------------------
// SELECT conversion
// --------------------------------------------------------------------------

func (p *Parser) convertSelect(sel *pgquery.SelectStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtSelect

	// Handle set operations (UNION / INTERSECT / EXCEPT).
	if sel.Op != pgquery.SetOperation_SETOP_NONE {
		return p.convertSetOp(sel, stmt)
	}

	// WITH clause (CTEs).
	if sel.WithClause != nil {
		for _, cte := range sel.WithClause.Ctes {
			if cw := cte.GetCommonTableExpr(); cw != nil {
				stmt.CTEs = append(stmt.CTEs, cw.Ctename)
			}
		}
	}

	// DISTINCT.
	if len(sel.DistinctClause) > 0 {
		stmt.Distinct = true
	}

	// Target list (SELECT columns and aggregates).
	for _, target := range sel.TargetList {
		rt := target.GetResTarget()
		if rt == nil {
			continue
		}
		alias := rt.Name

		// Check if this is an aggregate function call.
		if fc := rt.Val.GetFuncCall(); fc != nil {
			if agg := p.detectAggregate(fc, alias); agg != nil {
				stmt.Aggregates = append(stmt.Aggregates, *agg)
				continue
			}
		}

		// Regular column reference.
		col := p.nodeToColumnRef(rt.Val)
		col.Alias = alias
		stmt.Columns = append(stmt.Columns, col)
	}

	// FROM clause — handles tables and joins.
	for _, fromNode := range sel.FromClause {
		p.extractFromClause(fromNode, stmt)
	}

	// WHERE clause.
	if sel.WhereClause != nil {
		stmt.Where = p.convertWhereNode(sel.WhereClause)
	}

	// GROUP BY.
	for _, groupNode := range sel.GroupClause {
		stmt.GroupBy = append(stmt.GroupBy, p.nodeToColumnRef(groupNode))
	}

	// HAVING.
	if sel.HavingClause != nil {
		stmt.Having = p.convertWhereNode(sel.HavingClause)
	}

	// ORDER BY (SortClause).
	for _, sortNode := range sel.SortClause {
		sb := sortNode.GetSortBy()
		if sb == nil {
			continue
		}
		desc := sb.SortbyDir == pgquery.SortByDir_SORTBY_DESC
		stmt.OrderBy = append(stmt.OrderBy, OrderByClause{
			Column: p.nodeToColumnRef(sb.Node),
			Desc:   desc,
		})
	}

	// LIMIT.
	if sel.LimitCount != nil {
		if n := p.nodeToInt64(sel.LimitCount); n >= 0 {
			stmt.Limit = n
			stmt.HasLimit = true
		}
	}

	// OFFSET.
	if sel.LimitOffset != nil {
		if n := p.nodeToInt64(sel.LimitOffset); n >= 0 {
			stmt.Offset = n
		}
	}

	return stmt, nil
}

// convertSetOp handles UNION / INTERSECT / EXCEPT.
func (p *Parser) convertSetOp(sel *pgquery.SelectStmt, stmt *Statement) (*Statement, error) {
	switch sel.Op {
	case pgquery.SetOperation_SETOP_UNION:
		stmt.SetOp = "UNION"
	case pgquery.SetOperation_SETOP_INTERSECT:
		stmt.SetOp = "INTERSECT"
	case pgquery.SetOperation_SETOP_EXCEPT:
		stmt.SetOp = "EXCEPT"
	}

	// Process left side to populate tables and columns.
	if sel.Larg != nil {
		leftStmt := &Statement{RawSQL: stmt.RawSQL}
		leftStmt.Type = StmtSelect
		p.convertSelect(sel.Larg, leftStmt)
		stmt.Tables = leftStmt.Tables
		stmt.Columns = leftStmt.Columns
		stmt.Aggregates = leftStmt.Aggregates
		stmt.Where = leftStmt.Where
	}

	// ORDER BY and LIMIT can appear on the overall set operation.
	for _, sortNode := range sel.SortClause {
		sb := sortNode.GetSortBy()
		if sb == nil {
			continue
		}
		desc := sb.SortbyDir == pgquery.SortByDir_SORTBY_DESC
		stmt.OrderBy = append(stmt.OrderBy, OrderByClause{
			Column: p.nodeToColumnRef(sb.Node),
			Desc:   desc,
		})
	}
	if sel.LimitCount != nil {
		if n := p.nodeToInt64(sel.LimitCount); n >= 0 {
			stmt.Limit = n
			stmt.HasLimit = true
		}
	}
	if sel.LimitOffset != nil {
		if n := p.nodeToInt64(sel.LimitOffset); n >= 0 {
			stmt.Offset = n
		}
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// INSERT conversion
// --------------------------------------------------------------------------

func (p *Parser) convertInsert(ins *pgquery.InsertStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtInsert

	// Target table.
	if ins.Relation != nil {
		tableName := ins.Relation.Relname
		if ins.Relation.Schemaname != "" {
			tableName = ins.Relation.Schemaname + "." + tableName
		}
		stmt.Tables = append(stmt.Tables, tableName)
	}

	// ON CONFLICT.
	if ins.OnConflictClause != nil {
		stmt.OnConflict = true
	}

	// RETURNING.
	for _, ret := range ins.ReturningList {
		rt := ret.GetResTarget()
		if rt != nil {
			col := p.nodeToColumnRef(rt.Val)
			col.Alias = rt.Name
			stmt.Returning = append(stmt.Returning, col)
		}
	}

	// WITH clause (CTEs).
	if ins.WithClause != nil {
		for _, cte := range ins.WithClause.Ctes {
			if cw := cte.GetCommonTableExpr(); cw != nil {
				stmt.CTEs = append(stmt.CTEs, cw.Ctename)
			}
		}
	}

	// The SelectStmt inside INSERT can be either VALUES or a SELECT.
	if ins.SelectStmt != nil {
		selStmt := ins.SelectStmt.GetSelectStmt()
		if selStmt != nil {
			if len(selStmt.ValuesLists) > 0 {
				// INSERT ... VALUES (...)
				for _, valList := range selStmt.ValuesLists {
					listNode := valList.GetList()
					if listNode == nil {
						continue
					}
					var row []string
					for _, item := range listNode.Items {
						row = append(row, p.nodeToValueString(item))
					}
					stmt.InsertRows = append(stmt.InsertRows, row)
				}
			} else if len(selStmt.TargetList) > 0 || len(selStmt.FromClause) > 0 {
				// INSERT ... SELECT ...
				stmt.InsertSelect = true
			}
		}
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// UPDATE conversion
// --------------------------------------------------------------------------

func (p *Parser) convertUpdate(upd *pgquery.UpdateStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtUpdate

	// Target table.
	if upd.Relation != nil {
		tableName := upd.Relation.Relname
		if upd.Relation.Schemaname != "" {
			tableName = upd.Relation.Schemaname + "." + tableName
		}
		stmt.Tables = append(stmt.Tables, tableName)
	}

	// SET clauses.
	for _, target := range upd.TargetList {
		rt := target.GetResTarget()
		if rt == nil {
			continue
		}
		sc := SetClause{
			Column: rt.Name,
			Value:  p.nodeToValueString(rt.Val),
		}
		stmt.SetClauses = append(stmt.SetClauses, sc)
	}

	// WHERE clause.
	if upd.WhereClause != nil {
		stmt.Where = p.convertWhereNode(upd.WhereClause)
	}

	// RETURNING.
	for _, ret := range upd.ReturningList {
		rt := ret.GetResTarget()
		if rt != nil {
			col := p.nodeToColumnRef(rt.Val)
			col.Alias = rt.Name
			stmt.Returning = append(stmt.Returning, col)
		}
	}

	// WITH clause.
	if upd.WithClause != nil {
		for _, cte := range upd.WithClause.Ctes {
			if cw := cte.GetCommonTableExpr(); cw != nil {
				stmt.CTEs = append(stmt.CTEs, cw.Ctename)
			}
		}
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// DELETE conversion
// --------------------------------------------------------------------------

func (p *Parser) convertDelete(del *pgquery.DeleteStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtDelete

	// Target table.
	if del.Relation != nil {
		tableName := del.Relation.Relname
		if del.Relation.Schemaname != "" {
			tableName = del.Relation.Schemaname + "." + tableName
		}
		stmt.Tables = append(stmt.Tables, tableName)
	}

	// WHERE clause.
	if del.WhereClause != nil {
		stmt.Where = p.convertWhereNode(del.WhereClause)
	}

	// RETURNING.
	for _, ret := range del.ReturningList {
		rt := ret.GetResTarget()
		if rt != nil {
			col := p.nodeToColumnRef(rt.Val)
			col.Alias = rt.Name
			stmt.Returning = append(stmt.Returning, col)
		}
	}

	// WITH clause.
	if del.WithClause != nil {
		for _, cte := range del.WithClause.Ctes {
			if cw := cte.GetCommonTableExpr(); cw != nil {
				stmt.CTEs = append(stmt.CTEs, cw.Ctename)
			}
		}
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// DDL conversions
// --------------------------------------------------------------------------

func (p *Parser) convertCreateTable(cs *pgquery.CreateStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtCreateTable
	stmt.DDL = stmt.RawSQL
	if cs.Relation != nil {
		tableName := cs.Relation.Relname
		if cs.Relation.Schemaname != "" {
			tableName = cs.Relation.Schemaname + "." + tableName
		}
		stmt.Tables = append(stmt.Tables, tableName)
	}
	return stmt, nil
}

func (p *Parser) convertDrop(ds *pgquery.DropStmt, stmt *Statement) (*Statement, error) {
	stmt.DDL = stmt.RawSQL

	switch ds.RemoveType {
	case pgquery.ObjectType_OBJECT_TABLE:
		stmt.Type = StmtDropTable
	case pgquery.ObjectType_OBJECT_INDEX:
		stmt.Type = StmtCreateIndex // Reuse for DROP INDEX
		stmt.DDL = stmt.RawSQL
		return stmt, nil
	default:
		stmt.Type = StmtUnknown
		return stmt, nil
	}

	// Extract table names from the objects list.
	for _, obj := range ds.Objects {
		if listNode := obj.GetList(); listNode != nil {
			var parts []string
			for _, item := range listNode.Items {
				if s := item.GetString_(); s != nil {
					parts = append(parts, s.Sval)
				}
			}
			if len(parts) > 0 {
				stmt.Tables = append(stmt.Tables, strings.Join(parts, "."))
			}
		}
	}

	return stmt, nil
}

func (p *Parser) convertCreateIndex(is *pgquery.IndexStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtCreateIndex
	stmt.DDL = stmt.RawSQL
	if is.Relation != nil {
		tableName := is.Relation.Relname
		if is.Relation.Schemaname != "" {
			tableName = is.Relation.Schemaname + "." + tableName
		}
		stmt.Tables = append(stmt.Tables, tableName)
	}
	return stmt, nil
}

func (p *Parser) convertAlterTable(at *pgquery.AlterTableStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtAlterTable
	stmt.DDL = stmt.RawSQL
	if at.Relation != nil {
		tableName := at.Relation.Relname
		if at.Relation.Schemaname != "" {
			tableName = at.Relation.Schemaname + "." + tableName
		}
		stmt.Tables = append(stmt.Tables, tableName)
	}
	return stmt, nil
}

// --------------------------------------------------------------------------
// EXPLAIN conversion
// --------------------------------------------------------------------------

func (p *Parser) convertExplain(es *pgquery.ExplainStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtExplain

	if es.Query != nil {
		innerStmt, err := p.convertNode(es.Query, stmt.RawSQL)
		if err != nil {
			return nil, fmt.Errorf("explain inner: %w", err)
		}
		stmt.ExplainStmt = innerStmt
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// Transaction conversion
// --------------------------------------------------------------------------

func (p *Parser) convertTransaction(ts *pgquery.TransactionStmt, stmt *Statement) (*Statement, error) {
	switch ts.Kind {
	case pgquery.TransactionStmtKind_TRANS_STMT_BEGIN, pgquery.TransactionStmtKind_TRANS_STMT_START:
		stmt.Type = StmtBegin
	case pgquery.TransactionStmtKind_TRANS_STMT_COMMIT:
		stmt.Type = StmtCommit
	case pgquery.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		stmt.Type = StmtRollback
	default:
		stmt.Type = StmtUnknown
	}
	return stmt, nil
}

// --------------------------------------------------------------------------
// SET / SHOW conversion
// --------------------------------------------------------------------------

func (p *Parser) convertVariableSet(vs *pgquery.VariableSetStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtSet
	stmt.SetKey = vs.Name

	if len(vs.Args) > 0 {
		// Extract the first argument value.
		stmt.SetValue = p.nodeToValueString(vs.Args[0])
		// Strip surrounding quotes if present.
		stmt.SetValue = strings.Trim(stmt.SetValue, "'\"")
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// PL/pgSQL conversions
// --------------------------------------------------------------------------

func (p *Parser) convertCreateFunction(cf *pgquery.CreateFunctionStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtCreateFunction
	stmt.CreateSQL = stmt.RawSQL

	// Determine name.
	var nameParts []string
	for _, nameNode := range cf.Funcname {
		if s := nameNode.GetString_(); s != nil {
			nameParts = append(nameParts, s.Sval)
		}
	}
	stmt.FuncName = strings.Join(nameParts, ".")

	// Determine if procedure.
	stmt.IsProc = cf.IsProcedure

	// Extract dollar-quoted body from the raw SQL.
	stmt.FuncBody = extractDollarBody(stmt.RawSQL)

	return stmt, nil
}

func (p *Parser) convertDO(ds *pgquery.DoStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtDO

	// Extract the body from the raw SQL's dollar-quoted string.
	stmt.FuncBody = extractDollarBody(stmt.RawSQL)

	return stmt, nil
}

func (p *Parser) convertCall(cs *pgquery.CallStmt, stmt *Statement) (*Statement, error) {
	stmt.Type = StmtCall
	stmt.IsProc = true

	if cs.Funccall != nil {
		fc := cs.Funccall
		// Extract function name.
		var nameParts []string
		for _, nameNode := range fc.Funcname {
			if s := nameNode.GetString_(); s != nil {
				nameParts = append(nameParts, s.Sval)
			}
		}
		stmt.FuncName = strings.Join(nameParts, ".")

		// Extract arguments as string expressions.
		for _, arg := range fc.Args {
			stmt.FuncArgs = append(stmt.FuncArgs, p.nodeToValueString(arg))
		}
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// FROM clause extraction (handles tables, aliases, and JOINs)
// --------------------------------------------------------------------------

func (p *Parser) extractFromClause(node *pgquery.Node, stmt *Statement) {
	if node == nil {
		return
	}

	// Simple table reference.
	if rv := node.GetRangeVar(); rv != nil {
		tableName := rv.Relname
		if rv.Schemaname != "" {
			tableName = rv.Schemaname + "." + tableName
		}
		stmt.Tables = append(stmt.Tables, tableName)
		return
	}

	// Subquery in FROM.
	if rs := node.GetRangeSubselect(); rs != nil {
		// We don't deeply parse subqueries for table extraction, but note it.
		return
	}

	// JOIN expression.
	if je := node.GetJoinExpr(); je != nil {
		p.extractJoinExpr(je, stmt)
		return
	}
}

func (p *Parser) extractJoinExpr(je *pgquery.JoinExpr, stmt *Statement) {
	// Recursively extract left side.
	if je.Larg != nil {
		p.extractFromClause(je.Larg, stmt)
	}

	// Extract right side table.
	rightTable := ""
	rightAlias := ""
	if je.Rarg != nil {
		if rv := je.Rarg.GetRangeVar(); rv != nil {
			rightTable = rv.Relname
			if rv.Schemaname != "" {
				rightTable = rv.Schemaname + "." + rightTable
			}
			if rv.Alias != nil {
				rightAlias = rv.Alias.Aliasname
			}
			stmt.Tables = append(stmt.Tables, rightTable)
		} else if innerJoin := je.Rarg.GetJoinExpr(); innerJoin != nil {
			// Nested join on the right side.
			p.extractJoinExpr(innerJoin, stmt)
		}
	}

	// Map pg_query join type to our JoinType.
	jt := JoinInner
	switch je.Jointype {
	case pgquery.JoinType_JOIN_INNER:
		jt = JoinInner
	case pgquery.JoinType_JOIN_LEFT:
		jt = JoinLeft
	case pgquery.JoinType_JOIN_RIGHT:
		jt = JoinRight
	case pgquery.JoinType_JOIN_FULL:
		jt = JoinFull
	}
	// CROSS JOIN is represented as JOIN_INNER with no quals.
	if je.Quals == nil && je.Jointype == pgquery.JoinType_JOIN_INNER {
		jt = JoinCross
	}

	jc := JoinClause{
		Type:  jt,
		Table: rightTable,
		Alias: rightAlias,
	}

	// Parse ON clause.
	if je.Quals != nil {
		jc.On = p.convertWhereNode(je.Quals)
	}

	stmt.JoinClauses = append(stmt.JoinClauses, jc)
}

// --------------------------------------------------------------------------
// WHERE / expression conversion
// --------------------------------------------------------------------------

func (p *Parser) convertWhereNode(node *pgquery.Node) *WhereClause {
	if node == nil {
		return nil
	}

	// Boolean expression (AND / OR / NOT).
	if be := node.GetBoolExpr(); be != nil {
		return p.convertBoolExpr(be)
	}

	// Comparison expression (a op b).
	if ae := node.GetAExpr(); ae != nil {
		return p.convertAExpr(ae)
	}

	// NULL test (IS NULL / IS NOT NULL).
	if nt := node.GetNullTest(); nt != nil {
		return p.convertNullTest(nt)
	}

	// Sublink (EXISTS, IN subquery, etc.).
	if sl := node.GetSubLink(); sl != nil {
		return p.convertSubLink(sl)
	}

	return nil
}

func (p *Parser) convertBoolExpr(be *pgquery.BoolExpr) *WhereClause {
	var op string
	switch be.Boolop {
	case pgquery.BoolExprType_AND_EXPR:
		op = "AND"
	case pgquery.BoolExprType_OR_EXPR:
		op = "OR"
	case pgquery.BoolExprType_NOT_EXPR:
		op = "NOT"
	default:
		return nil
	}

	wc := &WhereClause{Op: op}
	for _, arg := range be.Args {
		child := p.convertWhereNode(arg)
		if child != nil {
			wc.Children = append(wc.Children, child)
		}
	}
	return wc
}

func (p *Parser) convertAExpr(ae *pgquery.A_Expr) *WhereClause {
	wc := &WhereClause{}

	// Determine the operator.
	opName := ""
	if len(ae.Name) > 0 {
		if s := ae.Name[0].GetString_(); s != nil {
			opName = s.Sval
		}
	}

	switch ae.Kind {
	case pgquery.A_Expr_Kind_AEXPR_OP:
		// Standard comparison operator.
		wc.Op = normalizeOp(opName)
		wc.Column = p.nodeToColumnRef(ae.Lexpr)
		wc.Value = p.nodeToValueString(ae.Rexpr)

	case pgquery.A_Expr_Kind_AEXPR_IN:
		// IN expression.
		wc.Op = "IN"
		wc.Column = p.nodeToColumnRef(ae.Lexpr)
		// Right side is a list of values.
		if listNode := ae.Rexpr.GetList(); listNode != nil {
			for _, item := range listNode.Items {
				wc.Values = append(wc.Values, p.nodeToValueString(item))
			}
		}

	case pgquery.A_Expr_Kind_AEXPR_LIKE:
		wc.Op = "LIKE"
		wc.Column = p.nodeToColumnRef(ae.Lexpr)
		wc.Value = p.nodeToValueString(ae.Rexpr)

	case pgquery.A_Expr_Kind_AEXPR_BETWEEN:
		wc.Op = "BETWEEN"
		wc.Column = p.nodeToColumnRef(ae.Lexpr)
		// BETWEEN is represented with a list of [low, high].
		if listNode := ae.Rexpr.GetList(); listNode != nil && len(listNode.Items) == 2 {
			wc.Values = []string{
				p.nodeToValueString(listNode.Items[0]),
				p.nodeToValueString(listNode.Items[1]),
			}
		}

	default:
		// Fallback: treat as a simple op.
		wc.Op = normalizeOp(opName)
		wc.Column = p.nodeToColumnRef(ae.Lexpr)
		wc.Value = p.nodeToValueString(ae.Rexpr)
	}

	return wc
}

func (p *Parser) convertNullTest(nt *pgquery.NullTest) *WhereClause {
	wc := &WhereClause{}
	wc.Column = p.nodeToColumnRef(nt.Arg)

	switch nt.Nulltesttype {
	case pgquery.NullTestType_IS_NULL:
		wc.Op = "IS NULL"
	case pgquery.NullTestType_IS_NOT_NULL:
		wc.Op = "IS NOT NULL"
	}

	return wc
}

func (p *Parser) convertSubLink(sl *pgquery.SubLink) *WhereClause {
	wc := &WhereClause{}

	switch sl.SubLinkType {
	case pgquery.SubLinkType_EXISTS_SUBLINK:
		wc.Op = "EXISTS"
	case pgquery.SubLinkType_ANY_SUBLINK:
		wc.Op = "IN"
		if sl.Testexpr != nil {
			wc.Column = p.nodeToColumnRef(sl.Testexpr)
		}
	default:
		wc.Op = "SUBQUERY"
	}

	return wc
}

// --------------------------------------------------------------------------
// Aggregate detection
// --------------------------------------------------------------------------

// aggFuncNames maps lowercase function names to AggFunc constants.
var aggFuncNames = map[string]AggFunc{
	"count": AggCount,
	"sum":   AggSum,
	"min":   AggMin,
	"max":   AggMax,
	"avg":   AggAvg,
}

func (p *Parser) detectAggregate(fc *pgquery.FuncCall, alias string) *AggregateRef {
	if fc == nil || len(fc.Funcname) == 0 {
		return nil
	}

	// Extract function name (last part if schema-qualified).
	funcName := ""
	for _, nameNode := range fc.Funcname {
		if s := nameNode.GetString_(); s != nil {
			funcName = s.Sval
		}
	}

	aggFunc, ok := aggFuncNames[strings.ToLower(funcName)]
	if !ok {
		return nil
	}

	ref := &AggregateRef{
		Func:  aggFunc,
		Alias: alias,
	}

	// Extract the column argument.
	if fc.AggStar {
		ref.Column = ColumnRef{Column: "*"}
	} else if len(fc.Args) > 0 {
		ref.Column = p.nodeToColumnRef(fc.Args[0])
	}

	return ref
}

// --------------------------------------------------------------------------
// Node-to-value conversion helpers
// --------------------------------------------------------------------------

// nodeToColumnRef converts an AST Node to a ColumnRef.
func (p *Parser) nodeToColumnRef(node *pgquery.Node) ColumnRef {
	if node == nil {
		return ColumnRef{}
	}

	// ColumnRef node.
	if cr := node.GetColumnRef(); cr != nil {
		var parts []string
		for _, field := range cr.Fields {
			if s := field.GetString_(); s != nil {
				parts = append(parts, s.Sval)
			} else if field.GetAStar() != nil {
				parts = append(parts, "*")
			}
		}
		switch len(parts) {
		case 1:
			return ColumnRef{Column: parts[0]}
		case 2:
			return ColumnRef{Table: parts[0], Column: parts[1]}
		default:
			return ColumnRef{Column: strings.Join(parts, ".")}
		}
	}

	// A_Const — string, integer, float, etc.
	if ac := node.GetAConst(); ac != nil {
		return ColumnRef{Column: aConstToString(ac)}
	}

	// FuncCall — used when a function call appears in column position.
	if fc := node.GetFuncCall(); fc != nil {
		var nameParts []string
		for _, n := range fc.Funcname {
			if s := n.GetString_(); s != nil {
				nameParts = append(nameParts, s.Sval)
			}
		}
		return ColumnRef{Column: strings.Join(nameParts, ".")}
	}

	// TypeCast — column::type.
	if tc := node.GetTypeCast(); tc != nil {
		return p.nodeToColumnRef(tc.Arg)
	}

	return ColumnRef{}
}

// nodeToValueString converts an AST Node into a string value (for literals).
func (p *Parser) nodeToValueString(node *pgquery.Node) string {
	if node == nil {
		return ""
	}

	// A_Const — string, integer, float, etc.
	if ac := node.GetAConst(); ac != nil {
		return aConstToString(ac)
	}

	// ColumnRef — treat as a column name string.
	if cr := node.GetColumnRef(); cr != nil {
		ref := p.nodeToColumnRef(node)
		if ref.Table != "" {
			return ref.Table + "." + ref.Column
		}
		return ref.Column
	}

	// TypeCast — unwrap.
	if tc := node.GetTypeCast(); tc != nil {
		return p.nodeToValueString(tc.Arg)
	}

	// FuncCall — render as funcname(args...).
	if fc := node.GetFuncCall(); fc != nil {
		var nameParts []string
		for _, n := range fc.Funcname {
			if s := n.GetString_(); s != nil {
				nameParts = append(nameParts, s.Sval)
			}
		}
		name := strings.Join(nameParts, ".")
		var args []string
		for _, arg := range fc.Args {
			args = append(args, p.nodeToValueString(arg))
		}
		return name + "(" + strings.Join(args, ", ") + ")"
	}

	// ParamRef ($1, $2, etc.).
	if pr := node.GetParamRef(); pr != nil {
		return fmt.Sprintf("$%d", pr.Number)
	}

	// List.
	if listNode := node.GetList(); listNode != nil {
		var items []string
		for _, item := range listNode.Items {
			items = append(items, p.nodeToValueString(item))
		}
		return strings.Join(items, ", ")
	}

	// SubLink (subquery).
	if node.GetSubLink() != nil {
		return "(subquery)"
	}

	return ""
}

// aConstToString converts an A_Const node to its string representation.
func aConstToString(ac *pgquery.A_Const) string {
	if ac.GetIval() != nil {
		return fmt.Sprintf("%d", ac.GetIval().Ival)
	}
	if ac.GetFval() != nil {
		return ac.GetFval().Fval
	}
	if ac.GetSval() != nil {
		return ac.GetSval().Sval
	}
	if ac.GetBoolval() != nil {
		if ac.GetBoolval().Boolval {
			return "true"
		}
		return "false"
	}
	if ac.Isnull {
		return "NULL"
	}
	return ""
}

// nodeToInt64 tries to extract an integer value from a Node.
func (p *Parser) nodeToInt64(node *pgquery.Node) int64 {
	if node == nil {
		return -1
	}
	if ac := node.GetAConst(); ac != nil {
		if ac.GetIval() != nil {
			return int64(ac.GetIval().Ival)
		}
	}
	return -1
}

// normalizeOp normalizes SQL operators.
func normalizeOp(op string) string {
	switch op {
	case "!=":
		return "<>"
	case "~~":
		return "LIKE"
	case "!~~":
		return "NOT LIKE"
	default:
		return op
	}
}

// --------------------------------------------------------------------------
// Legacy fallback parser (for statements that pg_query_go cannot parse)
// --------------------------------------------------------------------------

func (p *Parser) parseLegacy(sql string) ([]*Statement, error) {
	parts := splitStatements(sql)
	var stmts []*Statement

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		stmt, err := p.parseLegacyOne(part)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", truncate(part, 80), err)
		}
		stmts = append(stmts, stmt)
	}
	return stmts, nil
}

func (p *Parser) parseLegacyOne(sql string) (*Statement, error) {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	stmt := &Statement{RawSQL: sql}

	switch {
	case strings.HasPrefix(upper, "SELECT"):
		stmt.Type = StmtSelect
		if err := p.parseLegacySelect(sql, stmt); err != nil {
			return nil, err
		}
	case strings.HasPrefix(upper, "INSERT"):
		stmt.Type = StmtInsert
		if err := p.parseLegacyInsert(sql, stmt); err != nil {
			return nil, err
		}
	case strings.HasPrefix(upper, "UPDATE"):
		stmt.Type = StmtUpdate
		p.parseLegacySimpleTableRef(sql, stmt)
	case strings.HasPrefix(upper, "DELETE"):
		stmt.Type = StmtDelete
		p.parseLegacySimpleTableRef(sql, stmt)
	case isCreateFunction(upper):
		stmt.Type = StmtCreateFunction
		p.parseLegacyCreateFunction(sql, stmt)
	case strings.HasPrefix(upper, "CREATE TABLE"):
		stmt.Type = StmtCreateTable
		stmt.DDL = sql
		p.parseLegacyDDLTable(sql, stmt)
	case strings.HasPrefix(upper, "DROP TABLE"):
		stmt.Type = StmtDropTable
		stmt.DDL = sql
		p.parseLegacyDDLTable(sql, stmt)
	case strings.HasPrefix(upper, "CREATE INDEX"):
		stmt.Type = StmtCreateIndex
		stmt.DDL = sql
		p.parseLegacyCreateIndex(sql, stmt)
	case strings.HasPrefix(upper, "ALTER TABLE"):
		stmt.Type = StmtAlterTable
		stmt.DDL = sql
		p.parseLegacyDDLTable(sql, stmt)
	case strings.HasPrefix(upper, "EXPLAIN"):
		stmt.Type = StmtExplain
		inner := strings.TrimSpace(sql[7:])
		innerStmt, err := p.parseLegacyOne(inner)
		if err != nil {
			return nil, fmt.Errorf("explain inner: %w", err)
		}
		stmt.ExplainStmt = innerStmt
	case strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "START TRANSACTION"):
		stmt.Type = StmtBegin
	case strings.HasPrefix(upper, "COMMIT"):
		stmt.Type = StmtCommit
	case strings.HasPrefix(upper, "ROLLBACK"):
		stmt.Type = StmtRollback
	case strings.HasPrefix(upper, "SET"):
		stmt.Type = StmtSet
		p.parseLegacySet(sql, stmt)
	case strings.HasPrefix(upper, "SHOW"):
		stmt.Type = StmtShow
	case strings.HasPrefix(upper, "DO ") || strings.HasPrefix(upper, "DO$"):
		stmt.Type = StmtDO
		p.parseLegacyDO(sql, stmt)
	case strings.HasPrefix(upper, "CALL "):
		stmt.Type = StmtCall
		p.parseLegacyCall(sql, stmt)
	default:
		stmt.Type = StmtUnknown
	}

	return stmt, nil
}

// --------------------------------------------------------------------------
// Legacy helper methods (preserved for fallback)
// --------------------------------------------------------------------------

func (p *Parser) parseLegacySelect(sql string, stmt *Statement) error {
	upper := strings.ToUpper(sql)

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx >= 0 {
		afterFrom := sql[fromIdx+6:]
		tablePart := extractUntilKeyword(afterFrom, []string{"WHERE", "GROUP", "ORDER", "LIMIT", "JOIN", "LEFT", "RIGHT", "INNER", "FULL", "CROSS", "HAVING", "UNION"})
		tables := strings.Split(tablePart, ",")
		for _, t := range tables {
			t = strings.TrimSpace(t)
			if t != "" {
				parts := strings.Fields(t)
				stmt.Tables = append(stmt.Tables, parts[0])
			}
		}
	}

	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx >= 0 {
		afterWhere := sql[whereIdx+7:]
		wherePart := extractUntilKeyword(afterWhere, []string{"GROUP BY", "ORDER BY", "LIMIT", "HAVING", "UNION"})
		stmt.Where = parseLegacyWhereClause(wherePart)
	}

	groupIdx := strings.Index(upper, " GROUP BY ")
	if groupIdx >= 0 {
		afterGroup := sql[groupIdx+10:]
		groupPart := extractUntilKeyword(afterGroup, []string{"ORDER BY", "LIMIT", "HAVING", "UNION"})
		cols := strings.Split(groupPart, ",")
		for _, c := range cols {
			c = strings.TrimSpace(c)
			if c != "" {
				stmt.GroupBy = append(stmt.GroupBy, parseLegacyColumnRef(c))
			}
		}
	}

	orderIdx := strings.Index(upper, " ORDER BY ")
	if orderIdx >= 0 {
		afterOrder := sql[orderIdx+10:]
		orderPart := extractUntilKeyword(afterOrder, []string{"LIMIT", "OFFSET", "UNION"})
		parts := strings.Split(orderPart, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			desc := false
			upperPart := strings.ToUpper(part)
			if strings.HasSuffix(upperPart, " DESC") {
				desc = true
				part = strings.TrimSpace(part[:len(part)-5])
			} else if strings.HasSuffix(upperPart, " ASC") {
				part = strings.TrimSpace(part[:len(part)-4])
			}
			stmt.OrderBy = append(stmt.OrderBy, OrderByClause{
				Column: parseLegacyColumnRef(part),
				Desc:   desc,
			})
		}
	}

	limitIdx := strings.Index(upper, " LIMIT ")
	if limitIdx >= 0 {
		afterLimit := strings.TrimSpace(sql[limitIdx+7:])
		limitPart := extractUntilKeyword(afterLimit, []string{"OFFSET", "UNION"})
		n, err := parseInt64(strings.TrimSpace(limitPart))
		if err == nil {
			stmt.Limit = n
			stmt.HasLimit = true
		}
	}

	offsetIdx := strings.Index(upper, " OFFSET ")
	if offsetIdx >= 0 {
		afterOffset := strings.TrimSpace(sql[offsetIdx+8:])
		offsetPart := extractUntilKeyword(afterOffset, []string{"UNION"})
		n, err := parseInt64(strings.TrimSpace(offsetPart))
		if err == nil {
			stmt.Offset = n
		}
	}

	if fromIdx >= 0 {
		colPart := sql[7:fromIdx]
		p.parseLegacyAggregates(colPart, stmt)
	}

	p.parseLegacyJoins(sql, stmt)
	return nil
}

func (p *Parser) parseLegacyInsert(sql string, stmt *Statement) error {
	upper := strings.ToUpper(sql)

	intoIdx := strings.Index(upper, "INTO ")
	if intoIdx >= 0 {
		afterInto := strings.TrimSpace(sql[intoIdx+5:])
		parts := strings.Fields(afterInto)
		if len(parts) > 0 {
			tableName := strings.TrimRight(parts[0], "(")
			stmt.Tables = append(stmt.Tables, tableName)
		}
	}

	valIdx := strings.Index(upper, "VALUES")
	if valIdx >= 0 {
		afterVals := strings.TrimSpace(sql[valIdx+6:])
		stmt.InsertRows = parseValuesList(afterVals)
	}

	return nil
}

func (p *Parser) parseLegacyAggregates(colPart string, stmt *Statement) {
	upper := strings.ToUpper(colPart)
	aggFuncs := map[string]AggFunc{
		"COUNT": AggCount,
		"SUM":   AggSum,
		"MIN":   AggMin,
		"MAX":   AggMax,
		"AVG":   AggAvg,
	}

	for name, agg := range aggFuncs {
		idx := strings.Index(upper, name+"(")
		for idx >= 0 {
			start := idx + len(name) + 1
			end := strings.Index(upper[start:], ")")
			if end >= 0 {
				inner := strings.TrimSpace(colPart[start : start+end])
				ref := AggregateRef{
					Func:   agg,
					Column: parseLegacyColumnRef(inner),
				}
				stmt.Aggregates = append(stmt.Aggregates, ref)
			}
			remaining := upper[idx+1:]
			nextIdx := strings.Index(remaining, name+"(")
			if nextIdx < 0 {
				break
			}
			idx = idx + 1 + nextIdx
		}
	}
}

func (p *Parser) parseLegacyJoins(sql string, stmt *Statement) {
	upper := strings.ToUpper(sql)

	joinKeywords := []struct {
		keyword string
		jtype   JoinType
	}{
		{"LEFT JOIN ", JoinLeft},
		{"RIGHT JOIN ", JoinRight},
		{"FULL JOIN ", JoinFull},
		{"CROSS JOIN ", JoinCross},
		{"INNER JOIN ", JoinInner},
		{"JOIN ", JoinInner},
	}

	for _, jk := range joinKeywords {
		idx := strings.Index(upper, jk.keyword)
		for idx >= 0 {
			afterJoin := sql[idx+len(jk.keyword):]
			tablePart := extractUntilKeyword(afterJoin, []string{"ON ", "WHERE ", "GROUP ", "ORDER ", "LIMIT ", "JOIN "})
			tablePart = strings.TrimSpace(tablePart)

			parts := strings.Fields(tablePart)
			jc := JoinClause{
				Type: jk.jtype,
			}
			if len(parts) > 0 {
				jc.Table = parts[0]
			}
			if len(parts) > 1 && strings.ToUpper(parts[1]) != "ON" {
				jc.Alias = parts[1]
			}

			onIdx := strings.Index(strings.ToUpper(afterJoin), "ON ")
			if onIdx >= 0 {
				afterOn := afterJoin[onIdx+3:]
				onPart := extractUntilKeyword(afterOn, []string{"WHERE ", "GROUP ", "ORDER ", "LIMIT ", "JOIN ", "LEFT ", "RIGHT ", "INNER ", "FULL ", "CROSS "})
				jc.On = parseLegacyWhereClause(onPart)
			}

			stmt.JoinClauses = append(stmt.JoinClauses, jc)
			stmt.Tables = append(stmt.Tables, jc.Table)

			remaining := upper[idx+len(jk.keyword):]
			nextIdx := strings.Index(remaining, jk.keyword)
			if nextIdx < 0 {
				break
			}
			idx = idx + len(jk.keyword) + nextIdx
		}
	}
}

func (p *Parser) parseLegacySimpleTableRef(sql string, stmt *Statement) {
	upper := strings.ToUpper(sql)

	var afterKeyword string
	if strings.HasPrefix(upper, "UPDATE ") {
		afterKeyword = strings.TrimSpace(sql[7:])
	} else if idx := strings.Index(upper, "FROM "); idx >= 0 {
		afterKeyword = strings.TrimSpace(sql[idx+5:])
	}

	if afterKeyword != "" {
		parts := strings.Fields(afterKeyword)
		if len(parts) > 0 {
			stmt.Tables = append(stmt.Tables, parts[0])
		}
	}

	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx >= 0 {
		afterWhere := sql[whereIdx+7:]
		wherePart := extractUntilKeyword(afterWhere, []string{"ORDER BY", "LIMIT", "RETURNING"})
		stmt.Where = parseLegacyWhereClause(wherePart)
	}
}

func (p *Parser) parseLegacyDDLTable(sql string, stmt *Statement) {
	upper := strings.ToUpper(sql)
	var search string
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		search = sql[12:]
	case strings.HasPrefix(upper, "DROP TABLE"):
		search = sql[10:]
	case strings.HasPrefix(upper, "ALTER TABLE"):
		search = sql[11:]
	default:
		return
	}
	search = strings.TrimSpace(search)
	searchUpper := strings.ToUpper(search)
	if strings.HasPrefix(searchUpper, "IF NOT EXISTS ") {
		search = strings.TrimSpace(search[14:])
	} else if strings.HasPrefix(searchUpper, "IF EXISTS ") {
		search = strings.TrimSpace(search[10:])
	}
	parts := strings.Fields(search)
	if len(parts) > 0 {
		tableName := strings.TrimRight(parts[0], "(")
		stmt.Tables = append(stmt.Tables, tableName)
	}
}

func (p *Parser) parseLegacyCreateIndex(sql string, stmt *Statement) {
	upper := strings.ToUpper(sql)
	onIdx := strings.Index(upper, " ON ")
	if onIdx >= 0 {
		afterOn := strings.TrimSpace(sql[onIdx+4:])
		parts := strings.Fields(afterOn)
		if len(parts) > 0 {
			tableName := strings.TrimRight(parts[0], "(")
			stmt.Tables = append(stmt.Tables, tableName)
		}
	}
}

func (p *Parser) parseLegacySet(sql string, stmt *Statement) {
	after := strings.TrimSpace(sql[3:])
	parts := strings.SplitN(after, "=", 2)
	if len(parts) == 2 {
		stmt.SetKey = strings.TrimSpace(parts[0])
		stmt.SetValue = strings.Trim(strings.TrimSpace(parts[1]), "'\"")
	} else {
		fields := strings.Fields(after)
		if len(fields) >= 3 && strings.ToUpper(fields[1]) == "TO" {
			stmt.SetKey = fields[0]
			stmt.SetValue = strings.Trim(fields[2], "'\"")
		}
	}
}

func (p *Parser) parseLegacyCreateFunction(sql string, stmt *Statement) {
	stmt.CreateSQL = sql
	upper := strings.ToUpper(sql)

	stmt.IsProc = strings.Contains(upper, " PROCEDURE ")

	after := sql
	if idx := indexAfterKeyword(upper, "FUNCTION"); idx >= 0 {
		after = strings.TrimSpace(sql[idx:])
	} else if idx := indexAfterKeyword(upper, "PROCEDURE"); idx >= 0 {
		after = strings.TrimSpace(sql[idx:])
	}

	name := extractIdentifier(after)
	stmt.FuncName = name
	stmt.FuncBody = extractDollarBody(sql)
}

func (p *Parser) parseLegacyDO(sql string, stmt *Statement) {
	stmt.FuncBody = extractDollarBody(sql)
}

func (p *Parser) parseLegacyCall(sql string, stmt *Statement) {
	stmt.IsProc = true
	trimmed := strings.TrimSpace(sql)
	after := strings.TrimSpace(trimmed[5:])

	parenIdx := strings.Index(after, "(")
	if parenIdx < 0 {
		stmt.FuncName = strings.TrimSpace(after)
		return
	}

	stmt.FuncName = strings.TrimSpace(after[:parenIdx])

	argStr := after[parenIdx+1:]
	closeIdx := findMatchingParen(argStr)
	if closeIdx >= 0 {
		argStr = argStr[:closeIdx]
	}
	argStr = strings.TrimSpace(argStr)
	if argStr == "" {
		return
	}

	stmt.FuncArgs = splitCallArgs(argStr)
}

// --------------------------------------------------------------------------
// Legacy WHERE clause parser
// --------------------------------------------------------------------------

func parseLegacyWhereClause(s string) *WhereClause {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	upper := strings.ToUpper(s)

	andIdx := findTopLevelKeyword(upper, " AND ")
	if andIdx >= 0 {
		left := parseLegacyWhereClause(s[:andIdx])
		right := parseLegacyWhereClause(s[andIdx+5:])
		if left != nil && right != nil {
			return &WhereClause{
				Op:       "AND",
				Children: []*WhereClause{left, right},
			}
		}
	}

	orIdx := findTopLevelKeyword(upper, " OR ")
	if orIdx >= 0 {
		left := parseLegacyWhereClause(s[:orIdx])
		right := parseLegacyWhereClause(s[orIdx+4:])
		if left != nil && right != nil {
			return &WhereClause{
				Op:       "OR",
				Children: []*WhereClause{left, right},
			}
		}
	}

	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		return parseLegacyWhereClause(s[1 : len(s)-1])
	}

	inIdx := strings.Index(upper, " IN (")
	if inIdx >= 0 {
		col := strings.TrimSpace(s[:inIdx])
		valStart := inIdx + 5
		valEnd := strings.Index(s[valStart:], ")")
		if valEnd >= 0 {
			valStr := s[valStart : valStart+valEnd]
			values := splitValues(valStr)
			return &WhereClause{
				Op:     "IN",
				Column: parseLegacyColumnRef(col),
				Values: values,
			}
		}
	}

	ops := []string{"<>", "!=", "<=", ">=", "=", "<", ">", " LIKE "}
	for _, op := range ops {
		var idx int
		if op == " LIKE " {
			idx = strings.Index(upper, op)
		} else {
			idx = strings.Index(s, op)
		}
		if idx >= 0 {
			col := strings.TrimSpace(s[:idx])
			val := strings.TrimSpace(s[idx+len(op):])
			val = strings.Trim(val, "'\"")
			normalizedOp := strings.TrimSpace(op)
			if normalizedOp == "!=" {
				normalizedOp = "<>"
			}
			return &WhereClause{
				Op:     normalizedOp,
				Column: parseLegacyColumnRef(col),
				Value:  val,
			}
		}
	}

	if strings.HasSuffix(upper, " IS NULL") {
		col := strings.TrimSpace(s[:len(s)-8])
		return &WhereClause{
			Op:     "IS NULL",
			Column: parseLegacyColumnRef(col),
		}
	}
	if strings.HasSuffix(upper, " IS NOT NULL") {
		col := strings.TrimSpace(s[:len(s)-12])
		return &WhereClause{
			Op:     "IS NOT NULL",
			Column: parseLegacyColumnRef(col),
		}
	}

	return nil
}

func parseLegacyColumnRef(s string) ColumnRef {
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, ".", 2)
	if len(parts) == 2 {
		return ColumnRef{Table: parts[0], Column: parts[1]}
	}
	return ColumnRef{Column: s}
}

// --------------------------------------------------------------------------
// Shared helpers
// --------------------------------------------------------------------------

func findTopLevelKeyword(s, keyword string) int {
	depth := 0
	for i := 0; i < len(s)-len(keyword)+1; i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && s[i:i+len(keyword)] == keyword {
				return i
			}
		}
	}
	return -1
}

func extractUntilKeyword(s string, keywords []string) string {
	upper := strings.ToUpper(s)
	minIdx := len(s)
	for _, kw := range keywords {
		idx := findTopLevelKeyword(upper, " "+kw)
		if idx >= 0 && idx < minIdx {
			minIdx = idx
		}
		if strings.HasPrefix(upper, kw) {
			minIdx = 0
		}
	}
	return strings.TrimSpace(s[:minIdx])
}

func splitValues(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "'\"")
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func parseValuesList(s string) [][]string {
	var rows [][]string
	s = strings.TrimSpace(s)

	for len(s) > 0 {
		if s[0] != '(' {
			break
		}
		end := strings.Index(s, ")")
		if end < 0 {
			break
		}
		inner := s[1:end]
		rows = append(rows, splitValues(inner))
		s = strings.TrimSpace(s[end+1:])
		if len(s) > 0 && s[0] == ',' {
			s = strings.TrimSpace(s[1:])
		}
	}
	return rows
}

func splitStatements(sql string) []string {
	var parts []string
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	dollarTag := ""

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if dollarTag != "" {
			if ch == '$' && i+len(dollarTag) <= len(sql) && sql[i:i+len(dollarTag)] == dollarTag {
				current.WriteString(dollarTag)
				i += len(dollarTag) - 1
				dollarTag = ""
				continue
			}
			current.WriteByte(ch)
			continue
		}

		if ch == '$' && !inSingleQuote && !inDoubleQuote {
			tag := scanDollarTag(sql, i)
			if tag != "" {
				current.WriteString(tag)
				i += len(tag) - 1
				dollarTag = tag
				continue
			}
		}

		switch {
		case ch == '\'' && !inDoubleQuote:
			inSingleQuote = !inSingleQuote
			current.WriteByte(ch)
		case ch == '"' && !inSingleQuote:
			inDoubleQuote = !inDoubleQuote
			current.WriteByte(ch)
		case ch == ';' && !inSingleQuote && !inDoubleQuote:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

func scanDollarTag(s string, i int) string {
	if i >= len(s) || s[i] != '$' {
		return ""
	}
	if i+1 < len(s) && s[i+1] == '$' {
		return "$$"
	}
	j := i + 1
	if j >= len(s) {
		return ""
	}
	ch := s[j]
	if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
		return ""
	}
	j++
	for j < len(s) {
		ch = s[j]
		if ch == '$' {
			return s[i : j+1]
		}
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
			return ""
		}
		j++
	}
	return ""
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// --------------------------------------------------------------------------
// PL/pgSQL statement detection and parsing (legacy helpers)
// --------------------------------------------------------------------------

func isCreateFunction(upper string) bool {
	if strings.HasPrefix(upper, "CREATE FUNCTION") || strings.HasPrefix(upper, "CREATE PROCEDURE") {
		return true
	}
	if strings.HasPrefix(upper, "CREATE OR REPLACE FUNCTION") || strings.HasPrefix(upper, "CREATE OR REPLACE PROCEDURE") {
		return true
	}
	return false
}

func indexAfterKeyword(upper, keyword string) int {
	idx := strings.Index(upper, keyword)
	if idx < 0 {
		return -1
	}
	return idx + len(keyword)
}

func extractIdentifier(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' {
			b.WriteByte(ch)
		} else {
			break
		}
	}
	return b.String()
}

func extractDollarBody(sql string) string {
	for i := 0; i < len(sql); i++ {
		if sql[i] != '$' {
			continue
		}
		tag := scanDollarTag(sql, i)
		if tag == "" {
			continue
		}
		bodyStart := i + len(tag)
		closeIdx := strings.Index(sql[bodyStart:], tag)
		if closeIdx >= 0 {
			return sql[bodyStart : bodyStart+closeIdx]
		}
	}
	return ""
}

func findMatchingParen(s string) int {
	depth := 0
	inSingle := false
	dollarTag := ""

	for i := 0; i < len(s); i++ {
		ch := s[i]

		if dollarTag != "" {
			if ch == '$' && i+len(dollarTag) <= len(s) && s[i:i+len(dollarTag)] == dollarTag {
				i += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}
		if ch == '$' && !inSingle {
			tag := scanDollarTag(s, i)
			if tag != "" {
				i += len(tag) - 1
				dollarTag = tag
				continue
			}
		}

		if ch == '\'' {
			inSingle = !inSingle
			continue
		}
		if inSingle {
			continue
		}

		if ch == '(' {
			depth++
		} else if ch == ')' {
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}

func splitCallArgs(s string) []string {
	var args []string
	depth := 0
	inSingle := false
	dollarTag := ""
	start := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]

		if dollarTag != "" {
			if ch == '$' && i+len(dollarTag) <= len(s) && s[i:i+len(dollarTag)] == dollarTag {
				i += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}
		if ch == '$' && !inSingle {
			tag := scanDollarTag(s, i)
			if tag != "" {
				i += len(tag) - 1
				dollarTag = tag
				continue
			}
		}

		if ch == '\'' {
			inSingle = !inSingle
			continue
		}
		if inSingle {
			continue
		}

		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		} else if ch == ',' && depth == 0 {
			args = append(args, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	last := strings.TrimSpace(s[start:])
	if last != "" {
		args = append(args, last)
	}
	return args
}
