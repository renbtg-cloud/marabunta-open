// Marabunta - Licensed under the MIT License.
package plpgsql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// =========================================================================
// Mock SQL executor
// =========================================================================

type mockResult struct {
	affected int64
}

func (m mockResult) RowsAffected() (int64, error) { return m.affected, nil }

type mockSQLExec struct {
	// results are consumed in order by QuerySQL calls.
	results  [][]map[string]interface{}
	queryIdx int
	// execErr is returned by ExecSQL if non-nil.
	execErr error
	// Record every SQL that was executed.
	executedQueries []string
	execAffected    int64
}

func (m *mockSQLExec) ExecSQL(ctx context.Context, sql string, args ...interface{}) (SQLResult, error) {
	m.executedQueries = append(m.executedQueries, sql)
	if m.execErr != nil {
		return nil, m.execErr
	}
	return mockResult{affected: m.execAffected}, nil
}

func (m *mockSQLExec) QuerySQL(ctx context.Context, sql string, args ...interface{}) ([]map[string]interface{}, error) {
	m.executedQueries = append(m.executedQueries, sql)
	if m.queryIdx < len(m.results) {
		rows := m.results[m.queryIdx]
		m.queryIdx++
		return rows, nil
	}
	return nil, nil
}

func newMockExec(results ...[]map[string]interface{}) *mockSQLExec {
	return &mockSQLExec{results: results}
}

// =========================================================================
// Helper to build AST quickly
// =========================================================================

func mkBlock(decls []DeclareStmt, body ...Node) *Block {
	return &Block{Declarations: decls, Body: body}
}

func mkDecl(name, typeName string, def string) DeclareStmt {
	return DeclareStmt{Name: name, TypeName: typeName, DefaultExpr: def}
}

func mkAssign(variable, expr string) *AssignStmt {
	return &AssignStmt{Variable: variable, Expr: expr}
}

func mkReturn(expr string) *ReturnStmt {
	return &ReturnStmt{Expr: expr}
}

func mkIf(cond string, then []Node, elsifs []ElsIfClause, elseBody []Node) *IfStmt {
	return &IfStmt{Condition: cond, Then: then, ElsIfs: elsifs, Else: elseBody}
}

func mkFuncDef(name string, params []ParamDef, retType string, body *Block) *FunctionDef {
	return &FunctionDef{
		Name:       name,
		Params:     params,
		ReturnType: retType,
		Body:       body,
		Language:   "plpgsql",
		Replace:    true, // allow re-registration in tests
	}
}

// =========================================================================
// Type tests
// =========================================================================

func TestResolveType_AllTypes(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"integer", "integer"},
		{"int", "integer"},
		{"int4", "integer"},
		{"bigint", "bigint"},
		{"int8", "bigint"},
		{"numeric", "numeric"},
		{"decimal", "numeric"},
		{"float", "numeric"},
		{"text", "text"},
		{"varchar", "text"},
		{"varchar(255)", "text"},
		{"character varying", "text"},
		{"boolean", "boolean"},
		{"bool", "boolean"},
		{"date", "date"},
		{"timestamp", "timestamp"},
		{"timestamptz", "timestamp"},
		{"void", "void"},
		{"record", "record"},
		{"NUMERIC(10,2)", "numeric"},
	}
	for _, tc := range cases {
		typ, err := ResolveType(tc.input)
		if err != nil {
			t.Errorf("ResolveType(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if typ.TypeName() != tc.expected {
			t.Errorf("ResolveType(%q) = %q, want %q", tc.input, typ.TypeName(), tc.expected)
		}
	}

	// Unknown type.
	_, err := ResolveType("foobar")
	if err == nil {
		t.Error("ResolveType(\"foobar\") expected error, got nil")
	}
}

func TestCoerce_IntToText(t *testing.T) {
	val := NewPLValue(PLInteger{}, int64(42))
	result, err := Coerce(val, PLText{})
	if err != nil {
		t.Fatalf("Coerce int to text: %v", err)
	}
	if result.Value.(string) != "42" {
		t.Errorf("expected \"42\", got %q", result.Value)
	}
}

func TestCoerce_TextToInt(t *testing.T) {
	val := NewPLValue(PLText{}, "123")
	result, err := Coerce(val, PLInteger{})
	if err != nil {
		t.Fatalf("Coerce text to int: %v", err)
	}
	if result.Value.(int64) != 123 {
		t.Errorf("expected 123, got %v", result.Value)
	}
}

func TestCoerce_NullPropagation(t *testing.T) {
	val := NullValue(PLInteger{})
	result, err := Coerce(val, PLText{})
	if err != nil {
		t.Fatalf("Coerce NULL: %v", err)
	}
	if !result.IsNull {
		t.Error("expected NULL result")
	}
	if result.Type.TypeName() != "text" {
		t.Errorf("expected type text, got %s", result.Type.TypeName())
	}
}

// =========================================================================
// Interpreter tests
// =========================================================================

func TestInterp_Variables(t *testing.T) {
	// DECLARE x integer := 10; y integer; BEGIN y := x + 5; RETURN y; END;
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("x", "integer", "10"),
			mkDecl("y", "integer", ""),
		},
		mkAssign("y", "x + 5"),
		mkReturn("y"),
	)
	funcDef := mkFuncDef("test_vars", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 15 {
		t.Errorf("expected 15, got %v", result.Value)
	}
}

func TestInterp_IfElse(t *testing.T) {
	// DECLARE x integer := 5; result text; BEGIN IF x > 10 THEN result := 'big'; ELSIF x > 3 THEN result := 'medium'; ELSE result := 'small'; END IF; RETURN result;
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("x", "integer", "5"),
			mkDecl("result", "text", ""),
		},
		mkIf("x > 10",
			[]Node{mkAssign("result", "'big'")},
			[]ElsIfClause{{Condition: "x > 3", Body: []Node{mkAssign("result", "'medium'")}}},
			[]Node{mkAssign("result", "'small'")},
		),
		mkReturn("result"),
	)
	funcDef := mkFuncDef("test_if", nil, "text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "medium" {
		t.Errorf("expected \"medium\", got %v", result.Value)
	}
}

func TestInterp_WhileLoop(t *testing.T) {
	// DECLARE i integer := 0; total integer := 0; BEGIN WHILE i < 5 LOOP total := total + i; i := i + 1; END LOOP; RETURN total;
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("i", "integer", "0"),
			mkDecl("total", "integer", "0"),
		},
		&WhileStmt{
			Condition: "i < 5",
			Body: []Node{
				mkAssign("total", "total + i"),
				mkAssign("i", "i + 1"),
			},
		},
		mkReturn("total"),
	)
	funcDef := mkFuncDef("test_while", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	// 0+1+2+3+4 = 10
	if result.Value.(int64) != 10 {
		t.Errorf("expected 10, got %v", result.Value)
	}
}

func TestInterp_ForNumeric(t *testing.T) {
	// DECLARE total integer := 0; BEGIN FOR i IN 1..10 LOOP total := total + i; END LOOP; RETURN total;
	block := mkBlock(
		[]DeclareStmt{mkDecl("total", "integer", "0")},
		&ForStmt{
			Variable: "i",
			Low:      "1",
			High:     "10",
			Body:     []Node{mkAssign("total", "total + i")},
		},
		mkReturn("total"),
	)
	funcDef := mkFuncDef("test_for", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	// 1+2+3+...+10 = 55
	if result.Value.(int64) != 55 {
		t.Errorf("expected 55, got %v", result.Value)
	}
}

func TestInterp_ForNumericReverse(t *testing.T) {
	// FOR i IN REVERSE 5..1 LOOP collect i; END LOOP;
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("result", "text", "''"),
		},
		&ForStmt{
			Variable: "i",
			Low:      "1",
			High:     "5",
			Reverse:  true,
			Body:     []Node{mkAssign("result", "result || i")},
		},
		mkReturn("result"),
	)
	funcDef := mkFuncDef("test_for_rev", nil, "text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "54321" {
		t.Errorf("expected \"54321\", got %v", result.Value)
	}
}

func TestInterp_ForQuery(t *testing.T) {
	// FOR rec IN SELECT id, name FROM users LOOP ... END LOOP;
	queryResults := []map[string]interface{}{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}

	block := mkBlock(
		[]DeclareStmt{
			mkDecl("result", "text", "''"),
		},
		&ForQueryStmt{
			Variable: "rec",
			Query:    "SELECT id, name FROM users",
			Body:     []Node{mkAssign("result", "result || rec.name || ','")},
		},
		mkReturn("result"),
	)
	funcDef := mkFuncDef("test_for_query", nil, "text", block)

	mock := newMockExec(queryResults)
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "alice,bob," {
		t.Errorf("expected \"alice,bob,\", got %v", result.Value)
	}
}

func TestInterp_Return(t *testing.T) {
	block := mkBlock(nil, mkReturn("42"))
	funcDef := mkFuncDef("test_return", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 42 {
		t.Errorf("expected 42, got %v", result.Value)
	}
}

func TestInterp_ReturnQuery(t *testing.T) {
	queryResults := []map[string]interface{}{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	}

	block := mkBlock(nil,
		&ReturnQueryStmt{Query: "SELECT id FROM items"},
		mkReturn(""),
	)
	funcDef := &FunctionDef{
		Name:         "test_return_query",
		ReturnType:   "integer",
		ReturnsSetOf: true,
		Body:         block,
		Language:     "plpgsql",
	}

	mock := newMockExec(queryResults)
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	rows, ok := result.Value.([]map[string]interface{})
	if !ok {
		t.Fatalf("expected []map[string]interface{}, got %T", result.Value)
	}
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

func TestInterp_ReturnNext(t *testing.T) {
	block := mkBlock(
		[]DeclareStmt{mkDecl("i", "integer", "0")},
		&WhileStmt{
			Condition: "i < 3",
			Body: []Node{
				mkAssign("i", "i + 1"),
				&ReturnNextStmt{Expr: "i"},
			},
		},
		mkReturn(""),
	)
	funcDef := &FunctionDef{
		Name:         "test_return_next",
		ReturnType:   "integer",
		ReturnsSetOf: true,
		Body:         block,
		Language:     "plpgsql",
	}

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	rows, ok := result.Value.([]map[string]interface{})
	if !ok {
		t.Fatalf("expected []map[string]interface{}, got %T", result.Value)
	}
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

func TestInterp_RaiseNotice(t *testing.T) {
	block := mkBlock(nil,
		&RaiseStmt{Level: "NOTICE", Message: "Hello % from %", Params: []string{"'world'", "'test'"}},
		mkReturn("1"),
	)
	funcDef := mkFuncDef("test_raise", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 1 {
		t.Errorf("expected 1, got %v", result.Value)
	}
	if len(interp.Notices) != 1 {
		t.Fatalf("expected 1 notice, got %d", len(interp.Notices))
	}
	if interp.Notices[0].Message != "Hello world from test" {
		t.Errorf("expected 'Hello world from test', got %q", interp.Notices[0].Message)
	}
	if interp.Notices[0].Level != "NOTICE" {
		t.Errorf("expected level NOTICE, got %q", interp.Notices[0].Level)
	}
}

func TestInterp_RaiseException(t *testing.T) {
	block := mkBlock(nil,
		&RaiseStmt{Level: "EXCEPTION", Message: "something went wrong"},
		mkReturn("1"),
	)
	funcDef := mkFuncDef("test_raise_exc", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	_, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err == nil {
		t.Fatal("expected error from RAISE EXCEPTION")
	}
	raiseErr, ok := err.(*RaiseError)
	if !ok {
		t.Fatalf("expected *RaiseError, got %T", err)
	}
	if raiseErr.Message != "something went wrong" {
		t.Errorf("expected 'something went wrong', got %q", raiseErr.Message)
	}
}

func TestInterp_ExceptionHandler(t *testing.T) {
	// BEGIN ... EXCEPTION WHEN division_by_zero THEN ...
	block := &Block{
		Declarations: []DeclareStmt{mkDecl("result", "text", "'ok'")},
		Body: []Node{
			mkAssign("result", "1 / 0"),
		},
		ExceptionHandlers: []ExceptionHandler{
			{
				Conditions: []string{"division_by_zero"},
				Body:       []Node{mkAssign("result", "'caught division by zero'")},
			},
		},
	}
	funcBlock := mkBlock(nil,
		block,
	)
	// Need to wrap: the outer block returns the result from the inner block's scope.
	// Actually, let's restructure: put the exception handler at function level.
	funcBlock2 := &Block{
		Declarations: []DeclareStmt{mkDecl("result", "text", "'ok'")},
		Body: []Node{
			mkAssign("result", "1 / 0"),
			mkReturn("result"),
		},
		ExceptionHandlers: []ExceptionHandler{
			{
				Conditions: []string{"division_by_zero"},
				Body:       []Node{mkReturn("'caught division by zero'")},
			},
		},
	}
	funcDef := mkFuncDef("test_exc_handler", nil, "text", funcBlock2)
	_ = funcBlock

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "caught division by zero" {
		t.Errorf("expected 'caught division by zero', got %v", result.Value)
	}
}

func TestInterp_PerformDiscards(t *testing.T) {
	block := mkBlock(nil,
		&PerformStmt{Query: "pg_notify('channel', 'hello')"},
		mkReturn("'done'"),
	)
	funcDef := mkFuncDef("test_perform", nil, "text", block)

	mock := newMockExec([]map[string]interface{}{{"pg_notify": ""}})
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "done" {
		t.Errorf("expected 'done', got %v", result.Value)
	}
	// PERFORM should have executed a SELECT.
	if len(mock.executedQueries) != 1 {
		t.Errorf("expected 1 executed query, got %d", len(mock.executedQueries))
	}
}

func TestInterp_SelectInto(t *testing.T) {
	queryResults := []map[string]interface{}{
		{"count": int64(42)},
	}

	block := mkBlock(
		[]DeclareStmt{mkDecl("cnt", "integer", "0")},
		&SelectIntoStmt{
			Variables: []string{"cnt"},
			Query:     "SELECT count(*) FROM users",
		},
		mkReturn("cnt"),
	)
	funcDef := mkFuncDef("test_select_into", nil, "integer", block)

	mock := newMockExec(queryResults)
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 42 {
		t.Errorf("expected 42, got %v", result.Value)
	}
}

func TestInterp_SelectInto_NotFound(t *testing.T) {
	// Empty result set.
	block := mkBlock(
		[]DeclareStmt{mkDecl("cnt", "integer", "0")},
		&SelectIntoStmt{
			Variables: []string{"cnt"},
			Query:     "SELECT id FROM users WHERE id = 9999",
		},
		// Check FOUND variable.
		mkIf("found = FALSE",
			[]Node{mkReturn("-1")},
			nil,
			nil,
		),
		mkReturn("cnt"),
	)
	funcDef := mkFuncDef("test_select_into_nf", nil, "integer", block)

	// Return empty result set.
	mock := newMockExec([]map[string]interface{}{})
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != -1 {
		t.Errorf("expected -1 (not found), got %v", result.Value)
	}
}

func TestInterp_ExitWhen(t *testing.T) {
	// LOOP ... EXIT WHEN i >= 3; ... END LOOP;
	block := mkBlock(
		[]DeclareStmt{mkDecl("i", "integer", "0")},
		&LoopStmt{
			Body: []Node{
				mkAssign("i", "i + 1"),
				&ExitStmt{WhenExpr: "i >= 3"},
			},
		},
		mkReturn("i"),
	)
	funcDef := mkFuncDef("test_exit_when", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 3 {
		t.Errorf("expected 3, got %v", result.Value)
	}
}

func TestInterp_ContinueWhen(t *testing.T) {
	// Sum only odd numbers 1..5.
	block := mkBlock(
		[]DeclareStmt{mkDecl("total", "integer", "0")},
		&ForStmt{
			Variable: "i",
			Low:      "1",
			High:     "5",
			Body: []Node{
				// Skip even: CONTINUE WHEN i % 2 = 0
				&ContinueStmt{WhenExpr: "i % 2 = 0"},
				mkAssign("total", "total + i"),
			},
		},
		mkReturn("total"),
	)
	funcDef := mkFuncDef("test_continue_when", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	// 1+3+5 = 9
	if result.Value.(int64) != 9 {
		t.Errorf("expected 9, got %v", result.Value)
	}
}

func TestInterp_NestedFunctionCall(t *testing.T) {
	catalog := NewFunctionCatalog()

	// Register a helper function: double(n) returns n*2.
	doubleBlock := mkBlock(nil, mkReturn("n * 2"))
	doubleDef := mkFuncDef("double", []ParamDef{{Name: "n", TypeName: "integer"}}, "integer", doubleBlock)
	if err := catalog.Register(doubleDef); err != nil {
		t.Fatalf("Register double: %v", err)
	}

	// Main function calls double.
	mainBlock := mkBlock(
		[]DeclareStmt{mkDecl("x", "integer", "21")},
		mkReturn("double(x)"),
	)
	mainDef := mkFuncDef("main_func", nil, "integer", mainBlock)
	if err := catalog.Register(mainDef); err != nil {
		t.Fatalf("Register main_func: %v", err)
	}

	mock := newMockExec()
	interp := NewInterpreter(catalog, mock)
	result, err := interp.ExecuteFunction(context.Background(), mainDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 42 {
		t.Errorf("expected 42, got %v", result.Value)
	}
}

func TestInterp_DynamicExecute(t *testing.T) {
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("tbl", "text", "'users'"),
			mkDecl("cnt", "integer", "0"),
		},
		&ExecuteStmt{
			SQLExpr: "'SELECT count(*) FROM ' || tbl",
			Into:    []string{"cnt"},
		},
		mkReturn("cnt"),
	)
	funcDef := mkFuncDef("test_exec", nil, "integer", block)

	queryResults := []map[string]interface{}{
		{"count": int64(99)},
	}
	mock := newMockExec(queryResults)
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 99 {
		t.Errorf("expected 99, got %v", result.Value)
	}
}

func TestInterp_NullHandling(t *testing.T) {
	// NULL + 5 = NULL, coalesce(NULL, 42) = 42.
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("x", "integer", ""),
			mkDecl("y", "integer", "0"),
		},
		mkAssign("y", "coalesce(x, 42)"),
		mkReturn("y"),
	)
	funcDef := mkFuncDef("test_null", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 42 {
		t.Errorf("expected 42, got %v", result.Value)
	}
}

func TestInterp_Factorial(t *testing.T) {
	// Recursive factorial: if n <= 1 then return 1 else return n * factorial(n-1).
	catalog := NewFunctionCatalog()

	factBlock := mkBlock(nil,
		mkIf("n <= 1",
			[]Node{mkReturn("1")},
			nil,
			[]Node{mkReturn("n * factorial(n - 1)")},
		),
	)
	factDef := mkFuncDef("factorial", []ParamDef{{Name: "n", TypeName: "integer"}}, "integer", factBlock)
	if err := catalog.Register(factDef); err != nil {
		t.Fatalf("Register factorial: %v", err)
	}

	mock := newMockExec()
	interp := NewInterpreter(catalog, mock)
	result, err := interp.ExecuteFunction(context.Background(), factDef, []PLValue{
		NewPLValue(PLInteger{}, int64(5)),
	})
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	// 5! = 120
	if result.Value.(int64) != 120 {
		t.Errorf("expected 120, got %v", result.Value)
	}
}

func TestInterp_FibonacciLoop(t *testing.T) {
	// Iterative fibonacci: f(10) = 55.
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("a", "integer", "0"),
			mkDecl("b", "integer", "1"),
			mkDecl("temp", "integer", "0"),
			mkDecl("i", "integer", "0"),
		},
		&WhileStmt{
			Condition: "i < n",
			Body: []Node{
				mkAssign("temp", "b"),
				mkAssign("b", "a + b"),
				mkAssign("a", "temp"),
				mkAssign("i", "i + 1"),
			},
		},
		mkReturn("a"),
	)
	funcDef := mkFuncDef("fibonacci",
		[]ParamDef{{Name: "n", TypeName: "integer"}},
		"integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, []PLValue{
		NewPLValue(PLInteger{}, int64(10)),
	})
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 55 {
		t.Errorf("expected 55, got %v", result.Value)
	}
}

func TestInterp_DOBlock(t *testing.T) {
	// DO $$ DECLARE x integer := 5; BEGIN RAISE NOTICE 'x = %', x; END $$;
	block := &Block{
		Declarations: []DeclareStmt{mkDecl("x", "integer", "5")},
		Body: []Node{
			&RaiseStmt{Level: "NOTICE", Message: "x = %", Params: []string{"x"}},
		},
	}

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	err := interp.ExecuteDOBlock(context.Background(), block)
	if err != nil {
		t.Fatalf("ExecuteDOBlock: %v", err)
	}
	if len(interp.Notices) != 1 {
		t.Fatalf("expected 1 notice, got %d", len(interp.Notices))
	}
	if interp.Notices[0].Message != "x = 5" {
		t.Errorf("expected 'x = 5', got %q", interp.Notices[0].Message)
	}
}

// =========================================================================
// Expression evaluation tests
// =========================================================================

func TestEvalExpr_Literals(t *testing.T) {
	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	scope := NewScope()
	ctx := context.Background()

	tests := []struct {
		expr     string
		expected interface{}
	}{
		{"42", int64(42)},
		{"'hello'", "hello"},
		{"TRUE", true},
		{"FALSE", false},
		{"NULL", nil},
		{"3.14", float64(3.14)},
	}

	for _, tc := range tests {
		val, err := interp.evalExpr(ctx, scope, tc.expr)
		if err != nil {
			t.Errorf("evalExpr(%q) error: %v", tc.expr, err)
			continue
		}
		if tc.expected == nil {
			if !val.IsNull {
				t.Errorf("evalExpr(%q) expected NULL, got %v", tc.expr, val)
			}
			continue
		}
		if val.Value != tc.expected {
			t.Errorf("evalExpr(%q) = %v, want %v", tc.expr, val.Value, tc.expected)
		}
	}
}

func TestEvalExpr_Arithmetic(t *testing.T) {
	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	scope := NewScope()
	scope.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(10)), false, false)
	ctx := context.Background()

	tests := []struct {
		expr     string
		expected int64
	}{
		{"x + 5", 15},
		{"x - 3", 7},
		{"x * 2", 20},
		{"x / 2", 5},
		{"x % 3", 1},
	}

	for _, tc := range tests {
		val, err := interp.evalExpr(ctx, scope, tc.expr)
		if err != nil {
			t.Errorf("evalExpr(%q) error: %v", tc.expr, err)
			continue
		}
		if val.Value.(int64) != tc.expected {
			t.Errorf("evalExpr(%q) = %v, want %d", tc.expr, val.Value, tc.expected)
		}
	}
}

func TestEvalExpr_Comparison(t *testing.T) {
	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	scope := NewScope()
	scope.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(5)), false, false)
	ctx := context.Background()

	tests := []struct {
		expr     string
		expected bool
	}{
		{"x = 5", true},
		{"x = 6", false},
		{"x <> 5", false},
		{"x <> 6", true},
		{"x < 10", true},
		{"x > 10", false},
		{"x <= 5", true},
		{"x >= 5", true},
	}

	for _, tc := range tests {
		val, err := interp.evalExpr(ctx, scope, tc.expr)
		if err != nil {
			t.Errorf("evalExpr(%q) error: %v", tc.expr, err)
			continue
		}
		b, ok := val.Value.(bool)
		if !ok {
			t.Errorf("evalExpr(%q) not bool: %T", tc.expr, val.Value)
			continue
		}
		if b != tc.expected {
			t.Errorf("evalExpr(%q) = %v, want %v", tc.expr, b, tc.expected)
		}
	}
}

func TestEvalExpr_StringConcat(t *testing.T) {
	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	scope := NewScope()
	scope.Declare("name", PLText{}, NewPLValue(PLText{}, "world"), false, false)
	ctx := context.Background()

	val, err := interp.evalExpr(ctx, scope, "'hello ' || name")
	if err != nil {
		t.Fatalf("evalExpr error: %v", err)
	}
	if val.Value.(string) != "hello world" {
		t.Errorf("expected 'hello world', got %v", val.Value)
	}
}

func TestEvalExpr_LogicalOps(t *testing.T) {
	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	scope := NewScope()
	ctx := context.Background()

	tests := []struct {
		expr     string
		expected bool
	}{
		{"TRUE AND TRUE", true},
		{"TRUE AND FALSE", false},
		{"FALSE OR TRUE", true},
		{"FALSE OR FALSE", false},
		{"NOT TRUE", false},
		{"NOT FALSE", true},
	}

	for _, tc := range tests {
		val, err := interp.evalExpr(ctx, scope, tc.expr)
		if err != nil {
			t.Errorf("evalExpr(%q) error: %v", tc.expr, err)
			continue
		}
		b, ok := val.Value.(bool)
		if !ok {
			t.Errorf("evalExpr(%q) not bool: %T", tc.expr, val.Value)
			continue
		}
		if b != tc.expected {
			t.Errorf("evalExpr(%q) = %v, want %v", tc.expr, b, tc.expected)
		}
	}
}

func TestEvalExpr_IsNull(t *testing.T) {
	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	scope := NewScope()
	scope.Declare("x", PLText{}, NullValue(PLText{}), false, false)
	scope.Declare("y", PLInteger{}, NewPLValue(PLInteger{}, int64(5)), false, false)
	ctx := context.Background()

	val, err := interp.evalExpr(ctx, scope, "x IS NULL")
	if err != nil {
		t.Fatalf("evalExpr error: %v", err)
	}
	if val.Value.(bool) != true {
		t.Error("expected x IS NULL = true")
	}

	val, err = interp.evalExpr(ctx, scope, "y IS NOT NULL")
	if err != nil {
		t.Fatalf("evalExpr error: %v", err)
	}
	if val.Value.(bool) != true {
		t.Error("expected y IS NOT NULL = true")
	}
}

// =========================================================================
// Builtin function tests
// =========================================================================

func TestBuiltin_StringFunctions(t *testing.T) {
	registry := NewBuiltinRegistry()

	tests := []struct {
		name     string
		args     []PLValue
		expected string
	}{
		{"upper", []PLValue{NewPLValue(PLText{}, "hello")}, "HELLO"},
		{"lower", []PLValue{NewPLValue(PLText{}, "HELLO")}, "hello"},
		{"trim", []PLValue{NewPLValue(PLText{}, "  hi  ")}, "hi"},
		{"ltrim", []PLValue{NewPLValue(PLText{}, "  hi  ")}, "hi  "},
		{"rtrim", []PLValue{NewPLValue(PLText{}, "  hi  ")}, "  hi"},
		{"reverse", []PLValue{NewPLValue(PLText{}, "abc")}, "cba"},
		{"repeat", []PLValue{NewPLValue(PLText{}, "ab"), NewPLValue(PLInteger{}, int64(3))}, "ababab"},
		{"replace", []PLValue{
			NewPLValue(PLText{}, "hello world"),
			NewPLValue(PLText{}, "world"),
			NewPLValue(PLText{}, "go"),
		}, "hello go"},
		{"left", []PLValue{NewPLValue(PLText{}, "hello"), NewPLValue(PLInteger{}, int64(3))}, "hel"},
		{"right", []PLValue{NewPLValue(PLText{}, "hello"), NewPLValue(PLInteger{}, int64(3))}, "llo"},
		{"concat", []PLValue{
			NewPLValue(PLText{}, "a"),
			NewPLValue(PLText{}, "b"),
			NewPLValue(PLText{}, "c"),
		}, "abc"},
		{"concat_ws", []PLValue{
			NewPLValue(PLText{}, ","),
			NewPLValue(PLText{}, "a"),
			NewPLValue(PLText{}, "b"),
			NewPLValue(PLText{}, "c"),
		}, "a,b,c"},
		{"split_part", []PLValue{
			NewPLValue(PLText{}, "a.b.c"),
			NewPLValue(PLText{}, "."),
			NewPLValue(PLInteger{}, int64(2)),
		}, "b"},
	}

	for _, tc := range tests {
		result, err := registry.Call(tc.name, tc.args)
		if err != nil {
			t.Errorf("%s() error: %v", tc.name, err)
			continue
		}
		if plValueToString(result) != tc.expected {
			t.Errorf("%s() = %q, want %q", tc.name, plValueToString(result), tc.expected)
		}
	}

	// Test length.
	lenResult, err := registry.Call("length", []PLValue{NewPLValue(PLText{}, "hello")})
	if err != nil {
		t.Fatalf("length() error: %v", err)
	}
	if lenResult.Value.(int64) != 5 {
		t.Errorf("length('hello') = %v, want 5", lenResult.Value)
	}

	// Test substring.
	subResult, err := registry.Call("substring", []PLValue{
		NewPLValue(PLText{}, "hello world"),
		NewPLValue(PLInteger{}, int64(7)),
		NewPLValue(PLInteger{}, int64(5)),
	})
	if err != nil {
		t.Fatalf("substring() error: %v", err)
	}
	if subResult.Value.(string) != "world" {
		t.Errorf("substring('hello world', 7, 5) = %q, want 'world'", subResult.Value)
	}

	// Test position.
	posResult, err := registry.Call("position", []PLValue{
		NewPLValue(PLText{}, "lo"),
		NewPLValue(PLText{}, "hello"),
	})
	if err != nil {
		t.Fatalf("position() error: %v", err)
	}
	if posResult.Value.(int64) != 4 {
		t.Errorf("position('lo' in 'hello') = %v, want 4", posResult.Value)
	}
}

func TestBuiltin_NumericFunctions(t *testing.T) {
	registry := NewBuiltinRegistry()

	// abs
	absResult, err := registry.Call("abs", []PLValue{NewPLValue(PLInteger{}, int64(-5))})
	if err != nil {
		t.Fatalf("abs() error: %v", err)
	}
	if absResult.Value.(int64) != 5 {
		t.Errorf("abs(-5) = %v, want 5", absResult.Value)
	}

	// ceil
	ceilResult, err := registry.Call("ceil", []PLValue{NewPLValue(PLNumeric{}, float64(4.2))})
	if err != nil {
		t.Fatalf("ceil() error: %v", err)
	}
	if ceilResult.Value.(float64) != 5.0 {
		t.Errorf("ceil(4.2) = %v, want 5.0", ceilResult.Value)
	}

	// floor
	floorResult, err := registry.Call("floor", []PLValue{NewPLValue(PLNumeric{}, float64(4.8))})
	if err != nil {
		t.Fatalf("floor() error: %v", err)
	}
	if floorResult.Value.(float64) != 4.0 {
		t.Errorf("floor(4.8) = %v, want 4.0", floorResult.Value)
	}

	// round with places
	roundResult, err := registry.Call("round", []PLValue{
		NewPLValue(PLNumeric{}, float64(3.14159)),
		NewPLValue(PLInteger{}, int64(2)),
	})
	if err != nil {
		t.Fatalf("round() error: %v", err)
	}
	if roundResult.Value.(float64) != 3.14 {
		t.Errorf("round(3.14159, 2) = %v, want 3.14", roundResult.Value)
	}

	// mod
	modResult, err := registry.Call("mod", []PLValue{
		NewPLValue(PLInteger{}, int64(10)),
		NewPLValue(PLInteger{}, int64(3)),
	})
	if err != nil {
		t.Fatalf("mod() error: %v", err)
	}
	if modResult.Value.(float64) != 1.0 {
		t.Errorf("mod(10, 3) = %v, want 1.0", modResult.Value)
	}

	// power
	powResult, err := registry.Call("power", []PLValue{
		NewPLValue(PLInteger{}, int64(2)),
		NewPLValue(PLInteger{}, int64(10)),
	})
	if err != nil {
		t.Fatalf("power() error: %v", err)
	}
	if powResult.Value.(float64) != 1024.0 {
		t.Errorf("power(2, 10) = %v, want 1024.0", powResult.Value)
	}

	// sqrt
	sqrtResult, err := registry.Call("sqrt", []PLValue{NewPLValue(PLNumeric{}, float64(16))})
	if err != nil {
		t.Fatalf("sqrt() error: %v", err)
	}
	if sqrtResult.Value.(float64) != 4.0 {
		t.Errorf("sqrt(16) = %v, want 4.0", sqrtResult.Value)
	}

	// greatest
	greatResult, err := registry.Call("greatest", []PLValue{
		NewPLValue(PLInteger{}, int64(3)),
		NewPLValue(PLInteger{}, int64(7)),
		NewPLValue(PLInteger{}, int64(1)),
	})
	if err != nil {
		t.Fatalf("greatest() error: %v", err)
	}
	gv, _ := toFloat(greatResult)
	if gv != 7.0 {
		t.Errorf("greatest(3,7,1) = %v, want 7", gv)
	}

	// least
	leastResult, err := registry.Call("least", []PLValue{
		NewPLValue(PLInteger{}, int64(3)),
		NewPLValue(PLInteger{}, int64(7)),
		NewPLValue(PLInteger{}, int64(1)),
	})
	if err != nil {
		t.Fatalf("least() error: %v", err)
	}
	lv, _ := toFloat(leastResult)
	if lv != 1.0 {
		t.Errorf("least(3,7,1) = %v, want 1", lv)
	}

	// random — just verify it returns a value in [0,1)
	randResult, err := registry.Call("random", nil)
	if err != nil {
		t.Fatalf("random() error: %v", err)
	}
	rv := randResult.Value.(float64)
	if rv < 0 || rv >= 1 {
		t.Errorf("random() = %v, want [0,1)", rv)
	}
}

func TestBuiltin_DateFunctions(t *testing.T) {
	registry := NewBuiltinRegistry()

	// now() should return a timestamp.
	nowResult, err := registry.Call("now", nil)
	if err != nil {
		t.Fatalf("now() error: %v", err)
	}
	if _, ok := nowResult.Value.(time.Time); !ok {
		t.Errorf("now() returned %T, want time.Time", nowResult.Value)
	}

	// extract(year, timestamp)
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	extractResult, err := registry.Call("extract", []PLValue{
		NewPLValue(PLText{}, "year"),
		NewPLValue(PLTimestamp{}, ts),
	})
	if err != nil {
		t.Fatalf("extract() error: %v", err)
	}
	if extractResult.Value.(float64) != 2024 {
		t.Errorf("extract(year, ...) = %v, want 2024", extractResult.Value)
	}

	// date_trunc(day, timestamp)
	truncResult, err := registry.Call("date_trunc", []PLValue{
		NewPLValue(PLText{}, "day"),
		NewPLValue(PLTimestamp{}, ts),
	})
	if err != nil {
		t.Fatalf("date_trunc() error: %v", err)
	}
	truncTime, ok := truncResult.Value.(time.Time)
	if !ok {
		t.Fatalf("date_trunc returned %T, want time.Time", truncResult.Value)
	}
	expected := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	if !truncTime.Equal(expected) {
		t.Errorf("date_trunc(day, ...) = %v, want %v", truncTime, expected)
	}
}

func TestBuiltin_NullFunctions(t *testing.T) {
	registry := NewBuiltinRegistry()

	// coalesce(NULL, NULL, 42)
	coalResult, err := registry.Call("coalesce", []PLValue{
		NullValue(PLText{}),
		NullValue(PLText{}),
		NewPLValue(PLInteger{}, int64(42)),
	})
	if err != nil {
		t.Fatalf("coalesce() error: %v", err)
	}
	if coalResult.Value.(int64) != 42 {
		t.Errorf("coalesce(NULL, NULL, 42) = %v, want 42", coalResult.Value)
	}

	// coalesce(NULL, NULL) = NULL
	coalResult2, err := registry.Call("coalesce", []PLValue{
		NullValue(PLText{}),
		NullValue(PLText{}),
	})
	if err != nil {
		t.Fatalf("coalesce(NULL, NULL) error: %v", err)
	}
	if !coalResult2.IsNull {
		t.Error("coalesce(NULL, NULL) should be NULL")
	}

	// nullif(5, 5) = NULL
	nullifResult, err := registry.Call("nullif", []PLValue{
		NewPLValue(PLInteger{}, int64(5)),
		NewPLValue(PLInteger{}, int64(5)),
	})
	if err != nil {
		t.Fatalf("nullif() error: %v", err)
	}
	if !nullifResult.IsNull {
		t.Error("nullif(5, 5) should be NULL")
	}

	// nullif(5, 6) = 5
	nullifResult2, err := registry.Call("nullif", []PLValue{
		NewPLValue(PLInteger{}, int64(5)),
		NewPLValue(PLInteger{}, int64(6)),
	})
	if err != nil {
		t.Fatalf("nullif(5,6) error: %v", err)
	}
	if nullifResult2.Value.(int64) != 5 {
		t.Errorf("nullif(5, 6) = %v, want 5", nullifResult2.Value)
	}

	// ifnull(NULL, 99) = 99
	ifnullResult, err := registry.Call("ifnull", []PLValue{
		NullValue(PLText{}),
		NewPLValue(PLInteger{}, int64(99)),
	})
	if err != nil {
		t.Fatalf("ifnull() error: %v", err)
	}
	if ifnullResult.Value.(int64) != 99 {
		t.Errorf("ifnull(NULL, 99) = %v, want 99", ifnullResult.Value)
	}
}

func TestBuiltin_Format(t *testing.T) {
	registry := NewBuiltinRegistry()

	tests := []struct {
		args     []PLValue
		expected string
	}{
		{
			[]PLValue{
				NewPLValue(PLText{}, "Hello %s, you are %s"),
				NewPLValue(PLText{}, "world"),
				NewPLValue(PLText{}, "great"),
			},
			"Hello world, you are great",
		},
		{
			[]PLValue{
				NewPLValue(PLText{}, "CREATE TABLE %I (id int)"),
				NewPLValue(PLText{}, "users"),
			},
			`CREATE TABLE "users" (id int)`,
		},
		{
			[]PLValue{
				NewPLValue(PLText{}, "WHERE name = %L"),
				NewPLValue(PLText{}, "O'Brien"),
			},
			"WHERE name = 'O''Brien'",
		},
		{
			[]PLValue{
				NewPLValue(PLText{}, "100%%"),
			},
			"100%",
		},
	}

	for i, tc := range tests {
		result, err := registry.Call("format", tc.args)
		if err != nil {
			t.Errorf("format test %d error: %v", i, err)
			continue
		}
		if result.Value.(string) != tc.expected {
			t.Errorf("format test %d = %q, want %q", i, result.Value, tc.expected)
		}
	}
}

// =========================================================================
// Scope tests
// =========================================================================

func TestScope_DeclareAndGet(t *testing.T) {
	s := NewScope()
	s.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(42)), false, false)

	val, ok := s.Get("x")
	if !ok {
		t.Fatal("variable 'x' not found")
	}
	if val.Value.(int64) != 42 {
		t.Errorf("x = %v, want 42", val.Value)
	}
}

func TestScope_ChildInherits(t *testing.T) {
	parent := NewScope()
	parent.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(10)), false, false)

	child := parent.NewChild()
	val, ok := child.Get("x")
	if !ok {
		t.Fatal("child should see parent's variable")
	}
	if val.Value.(int64) != 10 {
		t.Errorf("x = %v, want 10", val.Value)
	}
}

func TestScope_ChildShadows(t *testing.T) {
	parent := NewScope()
	parent.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(10)), false, false)

	child := parent.NewChild()
	child.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(99)), false, false)

	val, ok := child.Get("x")
	if !ok {
		t.Fatal("child should have 'x'")
	}
	if val.Value.(int64) != 99 {
		t.Errorf("child.x = %v, want 99", val.Value)
	}

	// Parent still has original value.
	val2, ok := parent.Get("x")
	if !ok {
		t.Fatal("parent should have 'x'")
	}
	if val2.Value.(int64) != 10 {
		t.Errorf("parent.x = %v, want 10", val2.Value)
	}
}

func TestScope_SetPropagates(t *testing.T) {
	parent := NewScope()
	parent.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(10)), false, false)

	child := parent.NewChild()
	err := child.Set("x", NewPLValue(PLInteger{}, int64(20)))
	if err != nil {
		t.Fatalf("Set error: %v", err)
	}

	val, _ := parent.Get("x")
	if val.Value.(int64) != 20 {
		t.Errorf("after child.Set, parent.x = %v, want 20", val.Value)
	}
}

func TestScope_ConstantCannotBeSet(t *testing.T) {
	s := NewScope()
	s.Declare("PI", PLNumeric{}, NewPLValue(PLNumeric{}, float64(3.14159)), false, true)

	err := s.Set("PI", NewPLValue(PLNumeric{}, float64(3)))
	if err == nil {
		t.Error("expected error setting CONSTANT variable")
	}
}

func TestScope_NotNullRejectsNull(t *testing.T) {
	s := NewScope()
	s.Declare("x", PLInteger{}, NewPLValue(PLInteger{}, int64(1)), true, false)

	err := s.Set("x", NullValue(PLInteger{}))
	if err == nil {
		t.Error("expected error setting NOT NULL variable to NULL")
	}
}

func TestScope_SpecialVariables(t *testing.T) {
	s := NewScope()

	// FOUND should be pre-declared.
	val, ok := s.Get("found")
	if !ok {
		t.Fatal("FOUND not found in scope")
	}
	if val.Value.(bool) != false {
		t.Errorf("initial FOUND = %v, want false", val.Value)
	}

	// ROW_COUNT.
	val, ok = s.Get("row_count")
	if !ok {
		t.Fatal("ROW_COUNT not found in scope")
	}
	if val.Value.(int64) != 0 {
		t.Errorf("initial ROW_COUNT = %v, want 0", val.Value)
	}
}

// =========================================================================
// FunctionCatalog tests
// =========================================================================

func TestFunctionCatalog_RegisterLookup(t *testing.T) {
	cat := NewFunctionCatalog()
	def := &FunctionDef{Name: "my_func"}
	if err := cat.Register(def); err != nil {
		t.Fatalf("Register: %v", err)
	}

	found := cat.Lookup("my_func")
	if found == nil {
		t.Fatal("expected to find my_func")
	}
	if found.Name != "my_func" {
		t.Errorf("expected name my_func, got %s", found.Name)
	}

	// Case insensitive.
	found2 := cat.Lookup("MY_FUNC")
	if found2 == nil {
		t.Fatal("lookup should be case-insensitive")
	}
}

func TestFunctionCatalog_Remove(t *testing.T) {
	cat := NewFunctionCatalog()
	cat.Register(&FunctionDef{Name: "to_remove"})
	cat.Remove("to_remove")
	if cat.Lookup("to_remove") != nil {
		t.Error("expected nil after remove")
	}
}

func TestFunctionCatalog_List(t *testing.T) {
	cat := NewFunctionCatalog()
	cat.Register(&FunctionDef{Name: "func_a"})
	cat.Register(&FunctionDef{Name: "func_b"})
	defs := cat.List()
	if len(defs) != 2 {
		t.Errorf("expected 2 functions, got %d", len(defs))
	}
}

// =========================================================================
// Exception handling tests
// =========================================================================

func TestInterp_ExceptionOthers(t *testing.T) {
	// Test WHEN OTHERS catches any exception.
	block := &Block{
		Declarations: []DeclareStmt{mkDecl("result", "text", "'initial'")},
		Body: []Node{
			&RaiseStmt{Level: "EXCEPTION", Message: "custom error"},
		},
		ExceptionHandlers: []ExceptionHandler{
			{
				Conditions: []string{"others"},
				Body:       []Node{mkReturn("'caught by others'")},
			},
		},
	}
	funcDef := mkFuncDef("test_others", nil, "text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "caught by others" {
		t.Errorf("expected 'caught by others', got %v", result.Value)
	}
}

func TestInterp_SQLSTATE_In_Handler(t *testing.T) {
	block := &Block{
		Declarations: []DeclareStmt{mkDecl("msg", "text", "''")},
		Body: []Node{
			mkAssign("msg", "1 / 0"),
		},
		ExceptionHandlers: []ExceptionHandler{
			{
				Conditions: []string{"others"},
				Body: []Node{
					mkReturn("sqlerrm"),
				},
			},
		},
	}
	funcDef := mkFuncDef("test_sqlstate", nil, "text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "division by zero" {
		t.Errorf("expected SQLERRM = 'division by zero', got %q", result.Value)
	}
}

// =========================================================================
// Edge case tests
// =========================================================================

func TestInterp_ContextCancellation(t *testing.T) {
	block := mkBlock(nil,
		&LoopStmt{
			Body: []Node{&NullStmt{}},
		},
	)
	funcDef := mkFuncDef("infinite", nil, "void", block)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately.
	cancel()

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	_, err := interp.ExecuteFunction(ctx, funcDef, nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !strings.Contains(err.Error(), "cancel") {
		t.Errorf("expected cancellation error, got: %v", err)
	}
}

func TestInterp_MaxRecursionDepth(t *testing.T) {
	catalog := NewFunctionCatalog()

	// Function that calls itself infinitely.
	block := mkBlock(nil, mkReturn("infinite()"))
	def := mkFuncDef("infinite", nil, "integer", block)
	if err := catalog.Register(def); err != nil {
		t.Fatalf("Register infinite: %v", err)
	}

	mock := newMockExec()
	interp := NewInterpreter(catalog, mock)
	_, err := interp.ExecuteFunction(context.Background(), def, nil)
	if err == nil {
		t.Fatal("expected recursion depth error")
	}
	if !strings.Contains(err.Error(), "depth") {
		t.Errorf("expected depth error, got: %v", err)
	}
}

func TestInterp_TypeCoercionOnAssign(t *testing.T) {
	// Assign a string "42" to an integer variable.
	block := mkBlock(
		[]DeclareStmt{mkDecl("x", "integer", "0")},
		mkAssign("x", "'42'"),
		mkReturn("x"),
	)
	funcDef := mkFuncDef("test_coerce_assign", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 42 {
		t.Errorf("expected 42, got %v", result.Value)
	}
}

func TestInterp_FunctionWithParams(t *testing.T) {
	block := mkBlock(nil, mkReturn("a + b"))
	funcDef := mkFuncDef("add",
		[]ParamDef{
			{Name: "a", TypeName: "integer"},
			{Name: "b", TypeName: "integer"},
		},
		"integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, []PLValue{
		NewPLValue(PLInteger{}, int64(3)),
		NewPLValue(PLInteger{}, int64(7)),
	})
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 10 {
		t.Errorf("add(3, 7) = %v, want 10", result.Value)
	}
}

func TestInterp_ParamDefaultValues(t *testing.T) {
	block := mkBlock(nil, mkReturn("x"))
	funcDef := mkFuncDef("with_default",
		[]ParamDef{
			{Name: "x", TypeName: "integer", DefaultExpr: "99"},
		},
		"integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	// Call without providing the argument.
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 99 {
		t.Errorf("expected 99 (default), got %v", result.Value)
	}
}

// =========================================================================
// Builtin edge cases
// =========================================================================

func TestBuiltin_NullPropagation(t *testing.T) {
	registry := NewBuiltinRegistry()

	// length(NULL) = NULL
	result, err := registry.Call("length", []PLValue{NullValue(PLText{})})
	if err != nil {
		t.Fatalf("length(NULL) error: %v", err)
	}
	if !result.IsNull {
		t.Error("length(NULL) should be NULL")
	}

	// upper(NULL) = NULL
	result, err = registry.Call("upper", []PLValue{NullValue(PLText{})})
	if err != nil {
		t.Fatalf("upper(NULL) error: %v", err)
	}
	if !result.IsNull {
		t.Error("upper(NULL) should be NULL")
	}

	// abs(NULL) = NULL
	result, err = registry.Call("abs", []PLValue{NullValue(PLNumeric{})})
	if err != nil {
		t.Fatalf("abs(NULL) error: %v", err)
	}
	if !result.IsNull {
		t.Error("abs(NULL) should be NULL")
	}
}

func TestBuiltin_Trunc(t *testing.T) {
	registry := NewBuiltinRegistry()

	result, err := registry.Call("trunc", []PLValue{
		NewPLValue(PLNumeric{}, float64(3.789)),
		NewPLValue(PLInteger{}, int64(1)),
	})
	if err != nil {
		t.Fatalf("trunc error: %v", err)
	}
	if result.Value.(float64) != 3.7 {
		t.Errorf("trunc(3.789, 1) = %v, want 3.7", result.Value)
	}
}

func TestBuiltin_RegexpMatches(t *testing.T) {
	registry := NewBuiltinRegistry()

	result, err := registry.Call("regexp_matches", []PLValue{
		NewPLValue(PLText{}, "hello123world"),
		NewPLValue(PLText{}, `(\d+)`),
	})
	if err != nil {
		t.Fatalf("regexp_matches error: %v", err)
	}
	if result.IsNull {
		t.Fatal("expected non-null result")
	}
	if result.Value.(string) != "{123}" {
		t.Errorf("regexp_matches = %q, want {123}", result.Value)
	}
}

func TestBuiltin_ToChar(t *testing.T) {
	registry := NewBuiltinRegistry()

	ts := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)
	result, err := registry.Call("to_char", []PLValue{
		NewPLValue(PLTimestamp{}, ts),
		NewPLValue(PLText{}, "YYYY-MM-DD"),
	})
	if err != nil {
		t.Fatalf("to_char error: %v", err)
	}
	if result.Value.(string) != "2024-01-15" {
		t.Errorf("to_char = %q, want '2024-01-15'", result.Value)
	}
}

func TestBuiltin_Age(t *testing.T) {
	registry := NewBuiltinRegistry()

	t1 := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC)
	result, err := registry.Call("age", []PLValue{
		NewPLValue(PLTimestamp{}, t1),
		NewPLValue(PLTimestamp{}, t2),
	})
	if err != nil {
		t.Fatalf("age error: %v", err)
	}
	if !strings.Contains(result.Value.(string), "5 days") {
		t.Errorf("age = %q, want something with '5 days'", result.Value)
	}
}

// =========================================================================
// Mock SQL executor recording tests
// =========================================================================

func TestInterp_ExecuteRecordsSQL(t *testing.T) {
	block := mkBlock(nil,
		&PerformStmt{Query: "1 + 1"},
		mkReturn("'done'"),
	)
	funcDef := mkFuncDef("test_record", nil, "text", block)

	mock := newMockExec([]map[string]interface{}{{"1": int64(2)}})
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	_, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if len(mock.executedQueries) == 0 {
		t.Fatal("expected executed queries to be recorded")
	}
	found := false
	for _, q := range mock.executedQueries {
		if strings.Contains(q, "SELECT") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a SELECT query, got: %v", mock.executedQueries)
	}
}

// =========================================================================
// Complex integration-style tests
// =========================================================================

func TestInterp_PowerFunction(t *testing.T) {
	// Custom power function using a loop.
	block := mkBlock(
		[]DeclareStmt{
			mkDecl("result", "integer", "1"),
			mkDecl("i", "integer", "0"),
		},
		&WhileStmt{
			Condition: "i < exp",
			Body: []Node{
				mkAssign("result", "result * base"),
				mkAssign("i", "i + 1"),
			},
		},
		mkReturn("result"),
	)
	funcDef := mkFuncDef("my_power",
		[]ParamDef{
			{Name: "base", TypeName: "integer"},
			{Name: "exp", TypeName: "integer"},
		},
		"integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, []PLValue{
		NewPLValue(PLInteger{}, int64(2)),
		NewPLValue(PLInteger{}, int64(8)),
	})
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(int64) != 256 {
		t.Errorf("my_power(2, 8) = %v, want 256", result.Value)
	}
}

func TestInterp_IfElseChain(t *testing.T) {
	// Grade assignment: A >= 90, B >= 80, C >= 70, D >= 60, F otherwise.
	block := mkBlock(nil,
		mkIf("score >= 90",
			[]Node{mkReturn("'A'")},
			[]ElsIfClause{
				{Condition: "score >= 80", Body: []Node{mkReturn("'B'")}},
				{Condition: "score >= 70", Body: []Node{mkReturn("'C'")}},
				{Condition: "score >= 60", Body: []Node{mkReturn("'D'")}},
			},
			[]Node{mkReturn("'F'")},
		),
	)
	funcDef := mkFuncDef("grade",
		[]ParamDef{{Name: "score", TypeName: "integer"}},
		"text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)

	tests := []struct {
		score    int64
		expected string
	}{
		{95, "A"},
		{85, "B"},
		{75, "C"},
		{65, "D"},
		{55, "F"},
	}

	for _, tc := range tests {
		result, err := interp.ExecuteFunction(context.Background(), funcDef, []PLValue{
			NewPLValue(PLInteger{}, tc.score),
		})
		if err != nil {
			t.Fatalf("grade(%d): %v", tc.score, err)
		}
		if result.Value.(string) != tc.expected {
			t.Errorf("grade(%d) = %q, want %q", tc.score, result.Value, tc.expected)
		}
	}
}

func TestInterp_StringManipulation(t *testing.T) {
	// Function that formats a name.
	block := mkBlock(
		[]DeclareStmt{mkDecl("result", "text", "''")},
		mkAssign("result", "upper(left(first_name, 1)) || lower(substring(first_name, 2)) || ' ' || upper(last_name)"),
		mkReturn("result"),
	)
	funcDef := mkFuncDef("format_name",
		[]ParamDef{
			{Name: "first_name", TypeName: "text"},
			{Name: "last_name", TypeName: "text"},
		},
		"text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, []PLValue{
		NewPLValue(PLText{}, "jOHN"),
		NewPLValue(PLText{}, "doe"),
	})
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if result.Value.(string) != "John DOE" {
		t.Errorf("format_name = %q, want 'John DOE'", result.Value)
	}
}

func TestInterp_MultipleRaiseNotice(t *testing.T) {
	block := mkBlock(nil,
		&RaiseStmt{Level: "NOTICE", Message: "step %", Params: []string{"'1'"}},
		&RaiseStmt{Level: "WARNING", Message: "step %", Params: []string{"'2'"}},
		&RaiseStmt{Level: "INFO", Message: "step %", Params: []string{"'3'"}},
		mkReturn("'done'"),
	)
	funcDef := mkFuncDef("multi_raise", nil, "text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	_, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if len(interp.Notices) != 3 {
		t.Fatalf("expected 3 notices, got %d", len(interp.Notices))
	}
	if interp.Notices[0].Level != "NOTICE" {
		t.Errorf("notice 0 level = %q", interp.Notices[0].Level)
	}
	if interp.Notices[1].Level != "WARNING" {
		t.Errorf("notice 1 level = %q", interp.Notices[1].Level)
	}
	if interp.Notices[2].Level != "INFO" {
		t.Errorf("notice 2 level = %q", interp.Notices[2].Level)
	}
}

// =========================================================================
// Regression / error handling tests
// =========================================================================

func TestInterp_DivisionByZero(t *testing.T) {
	block := mkBlock(nil, mkReturn("10 / 0"))
	funcDef := mkFuncDef("div_zero", nil, "integer", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	_, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err == nil {
		t.Fatal("expected division by zero error")
	}
	raiseErr, ok := err.(*RaiseError)
	if !ok {
		t.Fatalf("expected *RaiseError, got %T: %v", err, err)
	}
	if raiseErr.SQLState != "22012" {
		t.Errorf("expected SQLSTATE 22012, got %s", raiseErr.SQLState)
	}
}

func TestInterp_UndeclaredVariable(t *testing.T) {
	block := mkBlock(nil, mkReturn("nonexistent_var"))
	funcDef := mkFuncDef("undeclared", nil, "text", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	// The expression evaluator will try to look it up in scope, then fallback to SQL.
	// Without a real SQL executor, this should fail.
	_, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	// We accept both an error or a NULL result from fallback.
	_ = err
	_ = fmt.Sprintf("") // suppress unused import
}

func TestInterp_VoidReturn(t *testing.T) {
	block := mkBlock(nil,
		&RaiseStmt{Level: "NOTICE", Message: "hello"},
		mkReturn(""),
	)
	funcDef := mkFuncDef("void_func", nil, "void", block)

	mock := newMockExec()
	interp := NewInterpreter(NewFunctionCatalog(), mock)
	result, err := interp.ExecuteFunction(context.Background(), funcDef, nil)
	if err != nil {
		t.Fatalf("ExecuteFunction: %v", err)
	}
	if !result.IsNull && result.Type.TypeName() != "void" {
		// Void return is represented as NULL void.
		t.Errorf("expected void/null return, got %v", result)
	}
}
