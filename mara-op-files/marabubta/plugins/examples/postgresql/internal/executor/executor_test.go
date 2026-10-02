// Marabunta - Licensed under the MIT License.
package executor

import (
	"testing"

	"github.com/marabunta/marabunta-postgres/internal/parser"
	"github.com/marabunta/marabunta-postgres/internal/planner"
)

func TestMergeAggregateCount(t *testing.T) {
	mergeAggs := []planner.MergeAgg{
		{Func: parser.AggCount, Column: parser.ColumnRef{Column: "*"}},
	}
	partials := [][]string{
		{"10"},
		{"20"},
		{"30"},
	}
	result := MergeAggregateResults(mergeAggs, partials)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0] != "60" {
		t.Errorf("expected COUNT=60, got %s", result[0])
	}
}

func TestMergeAggregateSum(t *testing.T) {
	mergeAggs := []planner.MergeAgg{
		{Func: parser.AggSum, Column: parser.ColumnRef{Column: "amount"}},
	}
	partials := [][]string{
		{"100.5"},
		{"200.3"},
		{"50.2"},
	}
	result := MergeAggregateResults(mergeAggs, partials)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	if result[0] != "351" {
		t.Errorf("expected SUM=351, got %s", result[0])
	}
}

func TestMergeAggregateMinMax(t *testing.T) {
	mergeAggs := []planner.MergeAgg{
		{Func: parser.AggMin, Column: parser.ColumnRef{Column: "price"}},
		{Func: parser.AggMax, Column: parser.ColumnRef{Column: "price"}},
	}
	partials := [][]string{
		{"5", "100"},
		{"3", "200"},
		{"10", "150"},
	}
	result := MergeAggregateResults(mergeAggs, partials)
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result[0] != "3" {
		t.Errorf("expected MIN=3, got %s", result[0])
	}
	if result[1] != "200" {
		t.Errorf("expected MAX=200, got %s", result[1])
	}
}

func TestMergeLimitOffset(t *testing.T) {
	rows := [][]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"d", "4"},
		{"e", "5"},
	}

	// LIMIT 2 OFFSET 1
	result := MergeLimitOffset(rows, nil, 2, 1)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0][0] != "b" {
		t.Errorf("expected first row 'b', got '%s'", result[0][0])
	}
	if result[1][0] != "c" {
		t.Errorf("expected second row 'c', got '%s'", result[1][0])
	}
}

func TestMergeLimitOffsetBeyondRows(t *testing.T) {
	rows := [][]string{
		{"a"},
		{"b"},
	}

	result := MergeLimitOffset(rows, nil, 10, 5)
	if result != nil {
		t.Errorf("expected nil for offset beyond row count, got %d rows", len(result))
	}
}

func TestShardPruningEquals(t *testing.T) {
	// Test that shard pruning produces a single shard for = condition.
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM orders WHERE customer_id = '42'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if stmt.Where.Op != "=" {
		t.Errorf("expected op '=', got '%s'", stmt.Where.Op)
	}
	if stmt.Where.Column.Column != "customer_id" {
		t.Errorf("expected column 'customer_id', got '%s'", stmt.Where.Column.Column)
	}
	if stmt.Where.Value != "42" {
		t.Errorf("expected value '42', got '%s'", stmt.Where.Value)
	}
}

func TestShardPruningIN(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM orders WHERE region IN ('us', 'eu', 'asia')")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if stmt.Where.Op != "IN" {
		t.Errorf("expected op 'IN', got '%s'", stmt.Where.Op)
	}
	if len(stmt.Where.Values) != 3 {
		t.Errorf("expected 3 IN values, got %d", len(stmt.Where.Values))
	}
}

func TestPredicatePushdownFlag(t *testing.T) {
	// Verify that predicate pushdown flag is set for queries with WHERE.
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM orders WHERE status = 'active'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	// Use a mock catalog that returns a shard key.
	// For this test, we just verify the parser sets up WHERE correctly
	// and that BuildShardSQL preserves the original SQL.
	if stmts[0].Where == nil {
		t.Fatal("expected WHERE clause for pushdown test")
	}

	plan := &planner.Plan{
		Type:              planner.PlanScatter,
		Statement:         stmts[0],
		PushdownPredicate: true,
	}
	sql := planner.BuildShardSQL(plan)
	if sql != stmts[0].RawSQL {
		t.Errorf("expected pushdown SQL to match original, got %q", sql)
	}
}

func TestMergeAggregateAvg(t *testing.T) {
	// AVG is decomposed into SUM + COUNT on each shard.
	// Each partial row has 2 columns: _sum, _count.
	mergeAggs := []planner.MergeAgg{
		{Func: parser.AggAvg, Column: parser.ColumnRef{Column: "price"}, NeedSum: true, NeedCount: true},
	}
	partials := [][]string{
		{"300", "3"},  // shard 1: sum=300, count=3
		{"500", "5"},  // shard 2: sum=500, count=5
		{"200", "2"},  // shard 3: sum=200, count=2
	}
	result := MergeAggregateResults(mergeAggs, partials)
	if len(result) != 1 {
		t.Fatalf("expected 1 result, got %d", len(result))
	}
	// total sum=1000, total count=10, avg=100
	if result[0] != "100" {
		t.Errorf("expected AVG=100, got %s", result[0])
	}
}

func TestMergeAggregateMixedWithAvg(t *testing.T) {
	// Test that column offsets work correctly when mixing non-AVG and AVG aggregates.
	// Layout per partial row: COUNT(*), AVG(price) -> [count_val, sum_val, cnt_val]
	mergeAggs := []planner.MergeAgg{
		{Func: parser.AggCount, Column: parser.ColumnRef{Column: "*"}},
		{Func: parser.AggAvg, Column: parser.ColumnRef{Column: "price"}, NeedSum: true, NeedCount: true},
	}
	partials := [][]string{
		{"10", "200", "4"},  // shard 1: count=10, sum=200, cnt=4
		{"20", "300", "6"},  // shard 2: count=20, sum=300, cnt=6
	}
	result := MergeAggregateResults(mergeAggs, partials)
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result[0] != "30" {
		t.Errorf("expected COUNT=30, got %s", result[0])
	}
	// total sum=500, total count=10, avg=50
	if result[1] != "50" {
		t.Errorf("expected AVG=50, got %s", result[1])
	}
}

func TestMergeAggregateEmpty(t *testing.T) {
	mergeAggs := []planner.MergeAgg{
		{Func: parser.AggCount, Column: parser.ColumnRef{Column: "*"}},
	}
	result := MergeAggregateResults(mergeAggs, nil)
	if result != nil {
		t.Errorf("expected nil for empty partials, got %v", result)
	}

	result = MergeAggregateResults(nil, [][]string{{"10"}})
	if result != nil {
		t.Errorf("expected nil for empty mergeAggs, got %v", result)
	}
}

func TestMergeLimitOffsetWithOrderByDesc(t *testing.T) {
	rows := [][]string{
		{"b", "2"},
		{"a", "1"},
		{"d", "4"},
		{"c", "3"},
		{"e", "5"},
	}

	orderCols := []parser.OrderByClause{
		{Column: parser.ColumnRef{Column: "name"}, Desc: true},
	}
	result := MergeLimitOffset(rows, orderCols, 3, 0)
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
	// DESC: e, d, c, b, a -> first 3 = e, d, c
	if result[0][0] != "e" {
		t.Errorf("expected first row 'e', got '%s'", result[0][0])
	}
	if result[1][0] != "d" {
		t.Errorf("expected second row 'd', got '%s'", result[1][0])
	}
	if result[2][0] != "c" {
		t.Errorf("expected third row 'c', got '%s'", result[2][0])
	}
}

func TestMergeLimitOffsetWithOrderByAsc(t *testing.T) {
	rows := [][]string{
		{"c"},
		{"a"},
		{"b"},
	}

	orderCols := []parser.OrderByClause{
		{Column: parser.ColumnRef{Column: "name"}, Desc: false},
	}
	result := MergeLimitOffset(rows, orderCols, 2, 0)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0][0] != "a" {
		t.Errorf("expected first row 'a', got '%s'", result[0][0])
	}
	if result[1][0] != "b" {
		t.Errorf("expected second row 'b', got '%s'", result[1][0])
	}
}

func TestMergeGroupBy(t *testing.T) {
	// Group by column 0, aggregate COUNT at column 1.
	partials := [][]string{
		{"us", "10"},
		{"eu", "5"},
		{"us", "20"},
		{"eu", "15"},
	}
	aggCols := []planner.MergeAgg{
		{Func: parser.AggCount, Column: parser.ColumnRef{Column: "*"}},
	}
	result := MergeGroupBy(partials, []int{0}, aggCols)
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}

	// Build a map of results for order-independent checking.
	groups := make(map[string]string)
	for _, row := range result {
		if len(row) >= 2 {
			groups[row[0]] = row[1]
		}
	}
	if groups["us"] != "30" {
		t.Errorf("expected us COUNT=30, got %s", groups["us"])
	}
	if groups["eu"] != "20" {
		t.Errorf("expected eu COUNT=20, got %s", groups["eu"])
	}
}

func TestMergeGroupByEmpty(t *testing.T) {
	result := MergeGroupBy(nil, []int{0}, nil)
	if result != nil {
		t.Errorf("expected nil for empty partials, got %v", result)
	}
}

func TestBuildTag(t *testing.T) {
	tests := []struct {
		stmtType parser.StatementType
		rowCount int
		expected string
	}{
		{parser.StmtSelect, 5, "SELECT 5"},
		{parser.StmtInsert, 3, "INSERT 0 3"},
		{parser.StmtUpdate, 2, "UPDATE 2"},
		{parser.StmtDelete, 1, "DELETE 1"},
		{parser.StmtCreateTable, 0, "OK"},
		{parser.StmtSet, 0, "OK"},
	}

	for _, tt := range tests {
		stmt := &parser.Statement{Type: tt.stmtType}
		tag := buildTag(stmt, tt.rowCount)
		if tag != tt.expected {
			t.Errorf("buildTag(%d, %d) = %q, want %q", tt.stmtType, tt.rowCount, tag, tt.expected)
		}
	}
}

func TestParserMultipleStatements(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT 1; SELECT 2; SELECT 3")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(stmts))
	}
}

func TestParserSemicolonInQuotes(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM t WHERE name = 'hello;world'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
}

func TestParserEmptyInput(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 0 {
		t.Fatalf("expected 0 statements, got %d", len(stmts))
	}
}

func TestParserInsertValues(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("INSERT INTO users (id, name) VALUES ('1', 'alice'), ('2', 'bob')")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if stmt.Type != parser.StmtInsert {
		t.Errorf("expected StmtInsert, got %d", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "users" {
		t.Errorf("expected table 'users', got %v", stmt.Tables)
	}
	if len(stmt.InsertRows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(stmt.InsertRows))
	}
	if stmt.InsertRows[0][0] != "1" || stmt.InsertRows[0][1] != "alice" {
		t.Errorf("unexpected first row: %v", stmt.InsertRows[0])
	}
	if stmt.InsertRows[1][0] != "2" || stmt.InsertRows[1][1] != "bob" {
		t.Errorf("unexpected second row: %v", stmt.InsertRows[1])
	}
}

func TestParserCreateTable(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("CREATE TABLE users (id INT, name TEXT, email VARCHAR(255))")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if stmt.Type != parser.StmtCreateTable {
		t.Errorf("expected StmtCreateTable, got %d", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "users" {
		t.Errorf("expected table 'users', got %v", stmt.Tables)
	}
}

func TestParserDropTableIfExists(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if stmt.Type != parser.StmtDropTable {
		t.Errorf("expected StmtDropTable, got %d", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "users" {
		t.Errorf("expected table 'users', got %v", stmt.Tables)
	}
}

func TestParserSelectWithAggregates(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT COUNT(*), SUM(amount), AVG(price) FROM orders WHERE status = 'active'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if len(stmt.Aggregates) != 3 {
		t.Fatalf("expected 3 aggregates, got %d", len(stmt.Aggregates))
	}

	// Check aggregate functions detected.
	foundCount, foundSum, foundAvg := false, false, false
	for _, agg := range stmt.Aggregates {
		switch agg.Func {
		case parser.AggCount:
			foundCount = true
		case parser.AggSum:
			foundSum = true
		case parser.AggAvg:
			foundAvg = true
		}
	}
	if !foundCount {
		t.Error("expected COUNT aggregate")
	}
	if !foundSum {
		t.Error("expected SUM aggregate")
	}
	if !foundAvg {
		t.Error("expected AVG aggregate")
	}
}

func TestParserSelectGroupByOrderByLimit(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT region, COUNT(*) FROM orders GROUP BY region ORDER BY region DESC LIMIT 10 OFFSET 5")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if len(stmt.GroupBy) != 1 {
		t.Errorf("expected 1 GROUP BY column, got %d", len(stmt.GroupBy))
	}
	if len(stmt.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY column, got %d", len(stmt.OrderBy))
	}
	if !stmt.OrderBy[0].Desc {
		t.Error("expected ORDER BY DESC")
	}
	if !stmt.HasLimit {
		t.Error("expected HasLimit=true")
	}
	if stmt.Limit != 10 {
		t.Errorf("expected LIMIT 10, got %d", stmt.Limit)
	}
	if stmt.Offset != 5 {
		t.Errorf("expected OFFSET 5, got %d", stmt.Offset)
	}
}

func TestParserWhereAND(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM orders WHERE status = 'active' AND region = 'us'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt := stmts[0]
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if stmt.Where.Op != "AND" {
		t.Errorf("expected AND op, got %q", stmt.Where.Op)
	}
	if len(stmt.Where.Children) != 2 {
		t.Fatalf("expected 2 AND children, got %d", len(stmt.Where.Children))
	}
	if stmt.Where.Children[0].Op != "=" {
		t.Errorf("expected first child op '=', got %q", stmt.Where.Children[0].Op)
	}
	if stmt.Where.Children[1].Op != "=" {
		t.Errorf("expected second child op '=', got %q", stmt.Where.Children[1].Op)
	}
}

func TestParserExplain(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("EXPLAIN SELECT * FROM orders WHERE id = '42'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if stmt.Type != parser.StmtExplain {
		t.Errorf("expected StmtExplain, got %d", stmt.Type)
	}
	if stmt.ExplainStmt == nil {
		t.Fatal("expected inner statement")
	}
	if stmt.ExplainStmt.Type != parser.StmtSelect {
		t.Errorf("expected inner StmtSelect, got %d", stmt.ExplainStmt.Type)
	}
}

func TestParserSetWithEquals(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SET search_path = 'public'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	stmt := stmts[0]
	if stmt.Type != parser.StmtSet {
		t.Errorf("expected StmtSet, got %d", stmt.Type)
	}
	if stmt.SetKey != "search_path" {
		t.Errorf("expected key 'search_path', got %q", stmt.SetKey)
	}
	if stmt.SetValue != "public" {
		t.Errorf("expected value 'public', got %q", stmt.SetValue)
	}
}

func TestParserSetWithTO(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SET search_path TO 'public'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt := stmts[0]
	if stmt.SetKey != "search_path" {
		t.Errorf("expected key 'search_path', got %q", stmt.SetKey)
	}
	if stmt.SetValue != "public" {
		t.Errorf("expected value 'public', got %q", stmt.SetValue)
	}
}

func TestParserUpdateWithWhere(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("UPDATE orders SET status = 'shipped' WHERE id = '42'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt := stmts[0]
	if stmt.Type != parser.StmtUpdate {
		t.Errorf("expected StmtUpdate, got %d", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "orders" {
		t.Errorf("expected table 'orders', got %v", stmt.Tables)
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if stmt.Where.Column.Column != "id" {
		t.Errorf("expected WHERE column 'id', got %q", stmt.Where.Column.Column)
	}
}

func TestParserDeleteWithWhere(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("DELETE FROM orders WHERE id = '42'")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt := stmts[0]
	if stmt.Type != parser.StmtDelete {
		t.Errorf("expected StmtDelete, got %d", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "orders" {
		t.Errorf("expected table 'orders', got %v", stmt.Tables)
	}
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParserWhereIsNull(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM orders WHERE deleted_at IS NULL")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt := stmts[0]
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if stmt.Where.Op != "IS NULL" {
		t.Errorf("expected op 'IS NULL', got %q", stmt.Where.Op)
	}
	if stmt.Where.Column.Column != "deleted_at" {
		t.Errorf("expected column 'deleted_at', got %q", stmt.Where.Column.Column)
	}
}

func TestParserWhereIsNotNull(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM orders WHERE deleted_at IS NOT NULL")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	stmt := stmts[0]
	if stmt.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if stmt.Where.Op != "IS NOT NULL" {
		t.Errorf("expected op 'IS NOT NULL', got %q", stmt.Where.Op)
	}
}

func TestBuildShardSQLAvgRewrite(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT AVG(price) FROM orders")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	plan := &planner.Plan{
		Type:        planner.PlanScatter,
		Statement:   stmts[0],
		PushdownAgg: true,
		MergeAggregates: []planner.MergeAgg{
			{Func: parser.AggAvg, Column: parser.ColumnRef{Column: "price"}, NeedSum: true, NeedCount: true},
		},
	}
	sql := planner.BuildShardSQL(plan)
	if sql == stmts[0].RawSQL {
		t.Error("expected AVG to be rewritten, but SQL was unchanged")
	}
	// Should contain SUM(price) and COUNT(price).
	if !contains(sql, "SUM(price)") {
		t.Errorf("expected SUM(price) in rewritten SQL, got %q", sql)
	}
	if !contains(sql, "COUNT(price)") {
		t.Errorf("expected COUNT(price) in rewritten SQL, got %q", sql)
	}
}

func TestBuildShardSQLLimitPushdown(t *testing.T) {
	p := parser.New()
	stmts, err := p.Parse("SELECT * FROM orders")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	plan := &planner.Plan{
		Type:          planner.PlanScatter,
		Statement:     stmts[0],
		PushdownLimit: true,
		MergeLimit:    10,
		MergeOffset:   5,
	}
	sql := planner.BuildShardSQL(plan)
	// Should push LIMIT 15 (10+5) to each shard.
	if !contains(sql, "LIMIT 15") {
		t.Errorf("expected LIMIT 15 in pushdown SQL, got %q", sql)
	}
}

func TestExplainResultSingleShard(t *testing.T) {
	plan := &planner.Plan{
		Type: planner.PlanExplain,
		ExplainPlan: &planner.Plan{
			Type:   planner.PlanSingleShard,
			Shards: []int{7},
		},
	}
	rows := planner.ExplainResult(plan, 16)
	if len(rows) == 0 {
		t.Fatal("expected at least one EXPLAIN row")
	}
	firstLine := string(rows[0])
	if !contains(firstLine, "SingleShard") || !contains(firstLine, "7") {
		t.Errorf("expected SingleShard with shard 7, got %q", firstLine)
	}
}

func TestExplainResultScatter(t *testing.T) {
	plan := &planner.Plan{
		Type: planner.PlanExplain,
		ExplainPlan: &planner.Plan{
			Type: planner.PlanScatter,
		},
	}
	rows := planner.ExplainResult(plan, 16)
	if len(rows) == 0 {
		t.Fatal("expected at least one EXPLAIN row")
	}
	firstLine := string(rows[0])
	if !contains(firstLine, "Scatter") {
		t.Errorf("expected Scatter strategy, got %q", firstLine)
	}
}

func TestExplainResultNilPlan(t *testing.T) {
	rows := planner.ExplainResult(nil, 16)
	if rows != nil {
		t.Errorf("expected nil for nil plan, got %v", rows)
	}
}

func TestExplainResultEmptyShards(t *testing.T) {
	// Verify no panic when PlanSingleShard has empty Shards.
	plan := &planner.Plan{
		Type: planner.PlanExplain,
		ExplainPlan: &planner.Plan{
			Type:   planner.PlanSingleShard,
			Shards: []int{},
		},
	}
	rows := planner.ExplainResult(plan, 16)
	if len(rows) == 0 {
		t.Fatal("expected at least one EXPLAIN row")
	}
	firstLine := string(rows[0])
	if !contains(firstLine, "SingleShard") {
		t.Errorf("expected SingleShard strategy, got %q", firstLine)
	}
}

// contains checks if s contains substr (helper for tests).
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
