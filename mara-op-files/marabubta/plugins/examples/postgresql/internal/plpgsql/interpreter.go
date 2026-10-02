// Marabunta - Licensed under the MIT License.
package plpgsql

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// SQL executor interface
// ---------------------------------------------------------------------------

// SQLExecutor is the interface used by the interpreter to execute SQL
// queries against the database. The Executor in the executor package
// implements this interface.
type SQLExecutor interface {
	// ExecSQL executes a non-query SQL statement.
	ExecSQL(ctx context.Context, sql string, args ...interface{}) (SQLResult, error)
	// QuerySQL executes a query and returns the result rows as maps.
	QuerySQL(ctx context.Context, sql string, args ...interface{}) ([]map[string]interface{}, error)
}

// SQLResult is a minimal interface matching database/sql.Result.
type SQLResult interface {
	RowsAffected() (int64, error)
}

// ---------------------------------------------------------------------------
// Notice messages
// ---------------------------------------------------------------------------

// NoticeMessage is a RAISE NOTICE/INFO/WARNING message collected during execution.
type NoticeMessage struct {
	Level   string
	Message string
}

// ---------------------------------------------------------------------------
// Control flow
// ---------------------------------------------------------------------------

// ControlFlow signals a non-normal transfer of control.
type ControlFlow int

const (
	FlowNormal   ControlFlow = iota // normal sequential execution
	FlowReturn                      // RETURN statement
	FlowExit                        // EXIT statement
	FlowContinue                    // CONTINUE statement
)

// ---------------------------------------------------------------------------
// Scope — variable scope stack
// ---------------------------------------------------------------------------

// Variable holds a single runtime variable.
type Variable struct {
	Name     string
	Type     PLType
	Value    PLValue
	NotNull  bool
	Constant bool
}

// Scope is a lexical variable scope.
type Scope struct {
	parent    *Scope
	variables map[string]*Variable
}

// NewScope creates a root scope with special variables.
func NewScope() *Scope {
	s := &Scope{variables: make(map[string]*Variable)}
	// Pre-declare special variables.
	s.variables["found"] = &Variable{Name: "FOUND", Type: PLBoolean{}, Value: NewPLValue(PLBoolean{}, false)}
	s.variables["row_count"] = &Variable{Name: "ROW_COUNT", Type: PLBigint{}, Value: NewPLValue(PLBigint{}, int64(0))}
	s.variables["sqlstate"] = &Variable{Name: "SQLSTATE", Type: PLText{}, Value: NewPLValue(PLText{}, "00000")}
	s.variables["sqlerrm"] = &Variable{Name: "SQLERRM", Type: PLText{}, Value: NewPLValue(PLText{}, "")}
	return s
}

// NewChild creates a child scope that inherits from this one.
func (s *Scope) NewChild() *Scope {
	return &Scope{parent: s, variables: make(map[string]*Variable)}
}

// Declare adds a variable to this scope.
func (s *Scope) Declare(name string, typ PLType, defaultVal PLValue, notNull, constant bool) {
	key := strings.ToLower(name)
	s.variables[key] = &Variable{
		Name:     name,
		Type:     typ,
		Value:    defaultVal,
		NotNull:  notNull,
		Constant: constant,
	}
}

// Get retrieves a variable value, walking up the scope chain.
func (s *Scope) Get(name string) (PLValue, bool) {
	key := strings.ToLower(name)
	for cur := s; cur != nil; cur = cur.parent {
		if v, ok := cur.variables[key]; ok {
			return v.Value, true
		}
	}
	return PLValue{}, false
}

// Set assigns a value to a variable, walking up the scope chain.
func (s *Scope) Set(name string, val PLValue) error {
	key := strings.ToLower(name)
	for cur := s; cur != nil; cur = cur.parent {
		if v, ok := cur.variables[key]; ok {
			if v.Constant {
				return fmt.Errorf("variable %q is declared CONSTANT", name)
			}
			if v.NotNull && val.IsNull {
				return fmt.Errorf("variable %q is NOT NULL but got NULL", name)
			}
			// Coerce to declared type if needed.
			if val.Type != nil && v.Type != nil && val.Type.TypeName() != v.Type.TypeName() {
				coerced, err := Coerce(val, v.Type)
				if err != nil {
					return fmt.Errorf("assigning to %q: %w", name, err)
				}
				val = coerced
			}
			v.Value = val
			return nil
		}
	}
	return fmt.Errorf("variable %q not found", name)
}

// GetVariable retrieves the full Variable struct, walking up the scope chain.
func (s *Scope) GetVariable(name string) (*Variable, bool) {
	key := strings.ToLower(name)
	for cur := s; cur != nil; cur = cur.parent {
		if v, ok := cur.variables[key]; ok {
			return v, true
		}
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// PLpgSQL exception
// ---------------------------------------------------------------------------

// RaiseError is an error produced by RAISE EXCEPTION or internal errors.
type RaiseError struct {
	Level    string // "EXCEPTION"
	Message  string
	SQLState string // 5-char code
	Detail   string
	Hint     string
}

func (e *RaiseError) Error() string { return e.Message }

// conditionToSQLState maps PG condition names to 5-char SQLSTATE codes.
var conditionToSQLState = map[string]string{
	"division_by_zero":   "22012",
	"no_data_found":      "P0002",
	"too_many_rows":      "P0003",
	"unique_violation":   "23505",
	"not_null_violation": "23502",
	"check_violation":    "23514",
	"raise_exception":    "P0001",
	"others":             "",
}

// ---------------------------------------------------------------------------
// Interpreter
// ---------------------------------------------------------------------------

// Interpreter executes PL/pgSQL function bodies.
type Interpreter struct {
	catalog  *FunctionCatalog
	builtins *BuiltinRegistry
	sqlExec  SQLExecutor
	Notices  []NoticeMessage

	// For RETURNS SETOF functions:
	resultRows []map[string]interface{}

	// Maximum recursion depth to avoid stack overflow.
	maxDepth int
	curDepth int
}

// NewInterpreter creates a new interpreter.
func NewInterpreter(catalog *FunctionCatalog, sqlExec SQLExecutor) *Interpreter {
	return &Interpreter{
		catalog:  catalog,
		builtins: NewBuiltinRegistry(),
		sqlExec:  sqlExec,
		maxDepth: 100,
	}
}

// ExecuteFunction executes a PL/pgSQL function with the given arguments.
func (interp *Interpreter) ExecuteFunction(ctx context.Context, funcDef *FunctionDef, args []PLValue) (PLValue, error) {
	interp.curDepth++
	if interp.curDepth > interp.maxDepth {
		return PLValue{}, fmt.Errorf("maximum function call depth (%d) exceeded", interp.maxDepth)
	}
	defer func() { interp.curDepth-- }()

	if funcDef.Body == nil {
		return PLValue{}, fmt.Errorf("function %q has no body", funcDef.Name)
	}

	// Create root scope and bind parameters.
	scope := NewScope()
	for i, param := range funcDef.Params {
		typ, err := ResolveType(param.TypeName)
		if err != nil {
			typ = PLText{} // fallback
		}
		var val PLValue
		if i < len(args) {
			val = args[i]
			// Coerce argument to parameter type.
			if val.Type != nil && val.Type.TypeName() != typ.TypeName() {
				coerced, cerr := Coerce(val, typ)
				if cerr == nil {
					val = coerced
				}
			}
		} else if param.DefaultExpr != "" {
			v, err := interp.evalExpr(ctx, scope, param.DefaultExpr)
			if err != nil {
				return PLValue{}, fmt.Errorf("evaluating default for param %q: %w", param.Name, err)
			}
			val = v
		} else {
			val = NullValue(typ)
		}
		name := param.Name
		if name == "" {
			name = fmt.Sprintf("$%d", i+1)
		}
		scope.Declare(name, typ, val, false, false)
	}

	// Reset result rows for SETOF functions.
	interp.resultRows = nil

	result, err, flow := interp.executeBlock(ctx, funcDef.Body, scope)
	if err != nil {
		return PLValue{}, err
	}

	// If function returns SETOF, return the collected rows.
	if funcDef.ReturnsSetOf {
		return NewPLValue(PLRecord{}, interp.resultRows), nil
	}

	if flow == FlowReturn {
		// Coerce return value to the declared return type.
		if funcDef.ReturnType != "" && funcDef.ReturnType != "void" {
			retType, terr := ResolveType(funcDef.ReturnType)
			if terr == nil && result.Type != nil && result.Type.TypeName() != retType.TypeName() {
				coerced, cerr := Coerce(result, retType)
				if cerr == nil {
					result = coerced
				}
			}
		}
		return result, nil
	}

	// Function ended without explicit RETURN — return void or zero.
	if funcDef.ReturnType == "" || funcDef.ReturnType == "void" {
		return NullValue(PLVoid{}), nil
	}
	retType, _ := ResolveType(funcDef.ReturnType)
	if retType != nil {
		return retType.Zero(), nil
	}
	return NullValue(PLVoid{}), nil
}

// EvalExprPublic evaluates a PL/pgSQL expression string and returns a PLValue.
// This is the public entry point for expression evaluation, used by the
// executor for CALL argument evaluation.
func (interp *Interpreter) EvalExprPublic(ctx context.Context, scope *Scope, expr string) (PLValue, error) {
	return interp.evalExpr(ctx, scope, expr)
}

// ExecuteDOBlock executes an anonymous DO $$ ... $$ block.
func (interp *Interpreter) ExecuteDOBlock(ctx context.Context, block *Block) error {
	scope := NewScope()
	_, err, _ := interp.executeBlock(ctx, block, scope)
	return err
}

// ---------------------------------------------------------------------------
// Block execution
// ---------------------------------------------------------------------------

func (interp *Interpreter) executeBlock(ctx context.Context, block *Block, parentScope *Scope) (PLValue, error, ControlFlow) {
	scope := parentScope.NewChild()

	// Process declarations.
	for _, decl := range block.Declarations {
		if err := interp.execDeclare(ctx, scope, &decl); err != nil {
			return PLValue{}, err, FlowNormal
		}
	}

	// Execute body with exception handling.
	if len(block.ExceptionHandlers) > 0 {
		return interp.execBlockWithExceptions(ctx, block, scope)
	}

	return interp.execStatements(ctx, scope, block.Body)
}

func (interp *Interpreter) execBlockWithExceptions(ctx context.Context, block *Block, scope *Scope) (PLValue, error, ControlFlow) {
	result, err, flow := interp.execStatements(ctx, scope, block.Body)
	if err == nil {
		return result, nil, flow
	}

	// Check if it's a RaiseError we can handle.
	raiseErr, ok := err.(*RaiseError)
	if !ok {
		return PLValue{}, err, FlowNormal
	}

	// Try to match an exception handler.
	for _, handler := range block.ExceptionHandlers {
		if matchesCondition(handler.Conditions, raiseErr) {
			// Set SQLSTATE and SQLERRM in handler scope.
			scope.Set("sqlstate", NewPLValue(PLText{}, raiseErr.SQLState))
			scope.Set("sqlerrm", NewPLValue(PLText{}, raiseErr.Message))
			return interp.execStatements(ctx, scope, handler.Body)
		}
	}

	// No handler matched — propagate the error.
	return PLValue{}, err, FlowNormal
}

func matchesCondition(conditions []string, err *RaiseError) bool {
	for _, cond := range conditions {
		cond = strings.ToLower(strings.TrimSpace(cond))
		if cond == "others" {
			return true
		}
		if code, ok := conditionToSQLState[cond]; ok {
			if code == err.SQLState {
				return true
			}
		}
		// Also match by SQLSTATE directly.
		if cond == strings.ToLower(err.SQLState) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Statement list execution
// ---------------------------------------------------------------------------

func (interp *Interpreter) execStatements(ctx context.Context, scope *Scope, stmts []Node) (PLValue, error, ControlFlow) {
	var result PLValue
	for _, stmt := range stmts {
		select {
		case <-ctx.Done():
			return PLValue{}, ctx.Err(), FlowNormal
		default:
		}

		val, err, flow := interp.execStatement(ctx, scope, stmt)
		if err != nil {
			return PLValue{}, err, FlowNormal
		}
		if flow != FlowNormal {
			return val, nil, flow
		}
		result = val
	}
	return result, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// Statement dispatch
// ---------------------------------------------------------------------------

func (interp *Interpreter) execStatement(ctx context.Context, scope *Scope, node Node) (PLValue, error, ControlFlow) {
	switch stmt := node.(type) {
	case *AssignStmt:
		return interp.execAssign(ctx, scope, stmt)
	case *IfStmt:
		return interp.execIf(ctx, scope, stmt)
	case *LoopStmt:
		return interp.execLoop(ctx, scope, stmt)
	case *WhileStmt:
		return interp.execWhile(ctx, scope, stmt)
	case *ForStmt:
		return interp.execFor(ctx, scope, stmt)
	case *ForQueryStmt:
		return interp.execForQuery(ctx, scope, stmt)
	case *ReturnStmt:
		return interp.execReturn(ctx, scope, stmt)
	case *ReturnQueryStmt:
		return interp.execReturnQuery(ctx, scope, stmt)
	case *ReturnNextStmt:
		return interp.execReturnNext(ctx, scope, stmt)
	case *RaiseStmt:
		return interp.execRaise(ctx, scope, stmt)
	case *PerformStmt:
		return interp.execPerform(ctx, scope, stmt)
	case *SelectIntoStmt:
		return interp.execSelectInto(ctx, scope, stmt)
	case *ExecuteStmt:
		return interp.execExecute(ctx, scope, stmt)
	case *ExitStmt:
		return interp.execExit(ctx, scope, stmt)
	case *ContinueStmt:
		return interp.execContinueStmt(ctx, scope, stmt)
	case *NullStmt:
		return PLValue{}, nil, FlowNormal
	case *CallStmt:
		return interp.execCall(ctx, scope, stmt)
	case *Block:
		return interp.executeBlock(ctx, stmt, scope)
	default:
		return PLValue{}, fmt.Errorf("unknown statement type: %T", node), FlowNormal
	}
}

// ---------------------------------------------------------------------------
// Declaration
// ---------------------------------------------------------------------------

func (interp *Interpreter) execDeclare(ctx context.Context, scope *Scope, decl *DeclareStmt) error {
	typ, err := ResolveType(decl.TypeName)
	if err != nil {
		typ = PLText{} // fallback
	}

	var val PLValue
	if decl.DefaultExpr != "" {
		v, err := interp.evalExpr(ctx, scope, decl.DefaultExpr)
		if err != nil {
			return fmt.Errorf("declaration default for %q: %w", decl.Name, err)
		}
		// Coerce to declared type.
		if v.Type != nil && v.Type.TypeName() != typ.TypeName() {
			coerced, cerr := Coerce(v, typ)
			if cerr != nil {
				return fmt.Errorf("coercing default for %q: %w", decl.Name, cerr)
			}
			val = coerced
		} else {
			val = v
		}
	} else {
		val = NullValue(typ)
	}

	if decl.NotNull && val.IsNull {
		return fmt.Errorf("NOT NULL variable %q has no default", decl.Name)
	}

	scope.Declare(decl.Name, typ, val, decl.NotNull, decl.Constant)
	return nil
}

// ---------------------------------------------------------------------------
// Assignment
// ---------------------------------------------------------------------------

func (interp *Interpreter) execAssign(ctx context.Context, scope *Scope, stmt *AssignStmt) (PLValue, error, ControlFlow) {
	val, err := interp.evalExpr(ctx, scope, stmt.Expr)
	if err != nil {
		return PLValue{}, fmt.Errorf("assignment to %q: %w", stmt.Variable, err), FlowNormal
	}
	if err := scope.Set(stmt.Variable, val); err != nil {
		return PLValue{}, err, FlowNormal
	}
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// IF
// ---------------------------------------------------------------------------

func (interp *Interpreter) execIf(ctx context.Context, scope *Scope, stmt *IfStmt) (PLValue, error, ControlFlow) {
	cond, err := interp.evalBool(ctx, scope, stmt.Condition)
	if err != nil {
		return PLValue{}, fmt.Errorf("IF condition: %w", err), FlowNormal
	}
	if cond {
		return interp.execStatements(ctx, scope, stmt.Then)
	}

	// Check ELSIF branches.
	for _, elsif := range stmt.ElsIfs {
		c, err := interp.evalBool(ctx, scope, elsif.Condition)
		if err != nil {
			return PLValue{}, fmt.Errorf("ELSIF condition: %w", err), FlowNormal
		}
		if c {
			return interp.execStatements(ctx, scope, elsif.Body)
		}
	}

	// ELSE branch.
	if len(stmt.Else) > 0 {
		return interp.execStatements(ctx, scope, stmt.Else)
	}

	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// LOOP
// ---------------------------------------------------------------------------

func (interp *Interpreter) execLoop(ctx context.Context, scope *Scope, stmt *LoopStmt) (PLValue, error, ControlFlow) {
	const maxIter = 1000000
	for i := 0; i < maxIter; i++ {
		select {
		case <-ctx.Done():
			return PLValue{}, ctx.Err(), FlowNormal
		default:
		}

		val, err, flow := interp.execStatements(ctx, scope, stmt.Body)
		if err != nil {
			return PLValue{}, err, FlowNormal
		}
		switch flow {
		case FlowReturn:
			return val, nil, FlowReturn
		case FlowExit:
			return val, nil, FlowNormal
		case FlowContinue:
			continue
		}
	}
	return PLValue{}, fmt.Errorf("LOOP exceeded maximum iterations (%d)", maxIter), FlowNormal
}

// ---------------------------------------------------------------------------
// WHILE
// ---------------------------------------------------------------------------

func (interp *Interpreter) execWhile(ctx context.Context, scope *Scope, stmt *WhileStmt) (PLValue, error, ControlFlow) {
	const maxIter = 1000000
	for i := 0; i < maxIter; i++ {
		select {
		case <-ctx.Done():
			return PLValue{}, ctx.Err(), FlowNormal
		default:
		}

		cond, err := interp.evalBool(ctx, scope, stmt.Condition)
		if err != nil {
			return PLValue{}, fmt.Errorf("WHILE condition: %w", err), FlowNormal
		}
		if !cond {
			break
		}

		val, err2, flow := interp.execStatements(ctx, scope, stmt.Body)
		if err2 != nil {
			return PLValue{}, err2, FlowNormal
		}
		switch flow {
		case FlowReturn:
			return val, nil, FlowReturn
		case FlowExit:
			return val, nil, FlowNormal
		case FlowContinue:
			continue
		}
	}
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// FOR (numeric)
// ---------------------------------------------------------------------------

func (interp *Interpreter) execFor(ctx context.Context, scope *Scope, stmt *ForStmt) (PLValue, error, ControlFlow) {
	low, err := interp.evalInt(ctx, scope, stmt.Low)
	if err != nil {
		return PLValue{}, fmt.Errorf("FOR lower bound: %w", err), FlowNormal
	}
	high, err := interp.evalInt(ctx, scope, stmt.High)
	if err != nil {
		return PLValue{}, fmt.Errorf("FOR upper bound: %w", err), FlowNormal
	}

	step := int64(1)
	if stmt.Reverse {
		step = -1
	}

	// Declare loop variable.
	childScope := scope.NewChild()
	childScope.Declare(stmt.Variable, PLInteger{}, NewPLValue(PLInteger{}, low), false, false)

	if stmt.Reverse {
		for i := high; i >= low; i += step {
			select {
			case <-ctx.Done():
				return PLValue{}, ctx.Err(), FlowNormal
			default:
			}

			childScope.Set(stmt.Variable, NewPLValue(PLInteger{}, i))
			val, err, flow := interp.execStatements(ctx, childScope, stmt.Body)
			if err != nil {
				return PLValue{}, err, FlowNormal
			}
			switch flow {
			case FlowReturn:
				return val, nil, FlowReturn
			case FlowExit:
				return val, nil, FlowNormal
			case FlowContinue:
				continue
			}
		}
	} else {
		for i := low; i <= high; i += step {
			select {
			case <-ctx.Done():
				return PLValue{}, ctx.Err(), FlowNormal
			default:
			}

			childScope.Set(stmt.Variable, NewPLValue(PLInteger{}, i))
			val, err, flow := interp.execStatements(ctx, childScope, stmt.Body)
			if err != nil {
				return PLValue{}, err, FlowNormal
			}
			switch flow {
			case FlowReturn:
				return val, nil, FlowReturn
			case FlowExit:
				return val, nil, FlowNormal
			case FlowContinue:
				continue
			}
		}
	}
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// FOR QUERY
// ---------------------------------------------------------------------------

func (interp *Interpreter) execForQuery(ctx context.Context, scope *Scope, stmt *ForQueryStmt) (PLValue, error, ControlFlow) {
	// Substitute variables in the query.
	query := interp.substituteVars(scope, stmt.Query)

	rows, err := interp.sqlExec.QuerySQL(ctx, query)
	if err != nil {
		return PLValue{}, fmt.Errorf("FOR query: %w", err), FlowNormal
	}

	// Update FOUND.
	scope.Set("found", NewPLValue(PLBoolean{}, len(rows) > 0))
	scope.Set("row_count", NewPLValue(PLBigint{}, int64(len(rows))))

	// Declare the record variable.
	childScope := scope.NewChild()
	childScope.Declare(stmt.Variable, PLRecord{}, NewPLValue(PLRecord{}, map[string]PLValue{}), false, false)

	for _, row := range rows {
		select {
		case <-ctx.Done():
			return PLValue{}, ctx.Err(), FlowNormal
		default:
		}

		// Convert row map to PLValue map.
		rec := make(map[string]PLValue)
		for k, v := range row {
			rec[strings.ToLower(k)] = goValueToPLValue(v)
		}
		childScope.Set(stmt.Variable, NewPLValue(PLRecord{}, rec))

		val, err, flow := interp.execStatements(ctx, childScope, stmt.Body)
		if err != nil {
			return PLValue{}, err, FlowNormal
		}
		switch flow {
		case FlowReturn:
			return val, nil, FlowReturn
		case FlowExit:
			return val, nil, FlowNormal
		case FlowContinue:
			continue
		}
	}
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// RETURN
// ---------------------------------------------------------------------------

func (interp *Interpreter) execReturn(ctx context.Context, scope *Scope, stmt *ReturnStmt) (PLValue, error, ControlFlow) {
	if stmt.Expr == "" {
		return NullValue(PLVoid{}), nil, FlowReturn
	}
	val, err := interp.evalExpr(ctx, scope, stmt.Expr)
	if err != nil {
		return PLValue{}, fmt.Errorf("RETURN expression: %w", err), FlowNormal
	}
	return val, nil, FlowReturn
}

// ---------------------------------------------------------------------------
// RETURN QUERY
// ---------------------------------------------------------------------------

func (interp *Interpreter) execReturnQuery(ctx context.Context, scope *Scope, stmt *ReturnQueryStmt) (PLValue, error, ControlFlow) {
	query := interp.substituteVars(scope, stmt.Query)
	rows, err := interp.sqlExec.QuerySQL(ctx, query)
	if err != nil {
		return PLValue{}, fmt.Errorf("RETURN QUERY: %w", err), FlowNormal
	}
	interp.resultRows = append(interp.resultRows, rows...)
	scope.Set("found", NewPLValue(PLBoolean{}, len(rows) > 0))
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// RETURN NEXT
// ---------------------------------------------------------------------------

func (interp *Interpreter) execReturnNext(ctx context.Context, scope *Scope, stmt *ReturnNextStmt) (PLValue, error, ControlFlow) {
	val, err := interp.evalExpr(ctx, scope, stmt.Expr)
	if err != nil {
		return PLValue{}, fmt.Errorf("RETURN NEXT: %w", err), FlowNormal
	}

	row := make(map[string]interface{})
	// If val is a record, flatten it.
	if rec, ok := val.Value.(map[string]PLValue); ok {
		for k, v := range rec {
			if v.IsNull {
				row[k] = nil
			} else {
				row[k] = v.Value
			}
		}
	} else {
		row["value"] = val.Value
	}
	interp.resultRows = append(interp.resultRows, row)
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// RAISE
// ---------------------------------------------------------------------------

func (interp *Interpreter) execRaise(ctx context.Context, scope *Scope, stmt *RaiseStmt) (PLValue, error, ControlFlow) {
	level := strings.ToUpper(stmt.Level)
	if level == "" {
		level = "EXCEPTION"
	}

	// Format message: replace % with parameter values.
	msg := stmt.Message
	for _, param := range stmt.Params {
		val, err := interp.evalExpr(ctx, scope, param)
		if err != nil {
			return PLValue{}, fmt.Errorf("RAISE param: %w", err), FlowNormal
		}
		// Replace first % with the value.
		idx := strings.Index(msg, "%")
		if idx >= 0 {
			msg = msg[:idx] + val.String() + msg[idx+1:]
		}
	}

	if level == "EXCEPTION" {
		sqlstate := "P0001"
		return PLValue{}, &RaiseError{
			Level:    level,
			Message:  msg,
			SQLState: sqlstate,
		}, FlowNormal
	}

	// For NOTICE, WARNING, INFO, LOG, DEBUG — collect and continue.
	interp.Notices = append(interp.Notices, NoticeMessage{Level: level, Message: msg})
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// PERFORM
// ---------------------------------------------------------------------------

func (interp *Interpreter) execPerform(ctx context.Context, scope *Scope, stmt *PerformStmt) (PLValue, error, ControlFlow) {
	query := interp.substituteVars(scope, stmt.Query)
	// PERFORM is equivalent to SELECT but discards results.
	_, err := interp.sqlExec.QuerySQL(ctx, "SELECT "+query)
	if err != nil {
		return PLValue{}, fmt.Errorf("PERFORM: %w", err), FlowNormal
	}
	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// SELECT INTO
// ---------------------------------------------------------------------------

func (interp *Interpreter) execSelectInto(ctx context.Context, scope *Scope, stmt *SelectIntoStmt) (PLValue, error, ControlFlow) {
	query := interp.substituteVars(scope, stmt.Query)
	rows, err := interp.sqlExec.QuerySQL(ctx, query)
	if err != nil {
		return PLValue{}, fmt.Errorf("SELECT INTO: %w", err), FlowNormal
	}

	if len(rows) == 0 {
		scope.Set("found", NewPLValue(PLBoolean{}, false))
		// Set target variables to NULL.
		for _, varName := range stmt.Variables {
			scope.Set(varName, NullValue(PLText{}))
		}
		if stmt.Strict {
			return PLValue{}, &RaiseError{
				Level:    "EXCEPTION",
				Message:  "query returned no rows",
				SQLState: "P0002",
			}, FlowNormal
		}
		return PLValue{}, nil, FlowNormal
	}

	if stmt.Strict && len(rows) > 1 {
		return PLValue{}, &RaiseError{
			Level:    "EXCEPTION",
			Message:  "query returned more than one row",
			SQLState: "P0003",
		}, FlowNormal
	}

	scope.Set("found", NewPLValue(PLBoolean{}, true))
	scope.Set("row_count", NewPLValue(PLBigint{}, int64(len(rows))))

	row := rows[0]

	// If there's only one target variable and it's a record, assign the whole row.
	if len(stmt.Variables) == 1 {
		if v, ok := scope.GetVariable(stmt.Variables[0]); ok && v.Type.TypeName() == "record" {
			rec := make(map[string]PLValue)
			for k, val := range row {
				rec[strings.ToLower(k)] = goValueToPLValue(val)
			}
			scope.Set(stmt.Variables[0], NewPLValue(PLRecord{}, rec))
			return PLValue{}, nil, FlowNormal
		}
	}

	// Assign columns to variables by position.
	// Build an ordered list of column keys from the row map.
	colIdx := 0
	for _, varName := range stmt.Variables {
		if colIdx >= len(row) {
			break
		}
		// Find the colIdx-th entry in the row.
		i := 0
		for _, v := range row {
			if i == colIdx {
				scope.Set(varName, goValueToPLValue(v))
				break
			}
			i++
		}
		colIdx++
	}

	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// EXECUTE (dynamic SQL)
// ---------------------------------------------------------------------------

func (interp *Interpreter) execExecute(ctx context.Context, scope *Scope, stmt *ExecuteStmt) (PLValue, error, ControlFlow) {
	// Evaluate the SQL expression.
	sqlVal, err := interp.evalExpr(ctx, scope, stmt.SQLExpr)
	if err != nil {
		return PLValue{}, fmt.Errorf("EXECUTE expression: %w", err), FlowNormal
	}

	sqlStr := sqlVal.String()

	// Evaluate USING parameters and substitute $1, $2, etc.
	if len(stmt.Using) > 0 {
		for i, paramExpr := range stmt.Using {
			val, err := interp.evalExpr(ctx, scope, paramExpr)
			if err != nil {
				return PLValue{}, fmt.Errorf("EXECUTE USING param %d: %w", i+1, err), FlowNormal
			}
			placeholder := fmt.Sprintf("$%d", i+1)
			replacement := val.String()
			if val.IsNull {
				replacement = "NULL"
			} else if val.Type != nil && val.Type.TypeName() == "text" {
				replacement = "'" + strings.ReplaceAll(replacement, "'", "''") + "'"
			}
			sqlStr = strings.ReplaceAll(sqlStr, placeholder, replacement)
		}
	}

	// Check if it looks like a query.
	upperSQL := strings.ToUpper(strings.TrimSpace(sqlStr))
	if strings.HasPrefix(upperSQL, "SELECT") || strings.HasPrefix(upperSQL, "WITH") {
		rows, err := interp.sqlExec.QuerySQL(ctx, sqlStr)
		if err != nil {
			return PLValue{}, fmt.Errorf("EXECUTE query: %w", err), FlowNormal
		}
		scope.Set("found", NewPLValue(PLBoolean{}, len(rows) > 0))
		scope.Set("row_count", NewPLValue(PLBigint{}, int64(len(rows))))

		// INTO clause.
		if len(stmt.Into) > 0 && len(rows) > 0 {
			row := rows[0]
			colIdx := 0
			for _, varName := range stmt.Into {
				i := 0
				for _, v := range row {
					if i == colIdx {
						scope.Set(varName, goValueToPLValue(v))
						break
					}
					i++
				}
				colIdx++
			}
		}
	} else {
		result, err := interp.sqlExec.ExecSQL(ctx, sqlStr)
		if err != nil {
			return PLValue{}, fmt.Errorf("EXECUTE: %w", err), FlowNormal
		}
		affected, _ := result.RowsAffected()
		scope.Set("row_count", NewPLValue(PLBigint{}, affected))
		scope.Set("found", NewPLValue(PLBoolean{}, affected > 0))
	}

	return PLValue{}, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// EXIT
// ---------------------------------------------------------------------------

func (interp *Interpreter) execExit(ctx context.Context, scope *Scope, stmt *ExitStmt) (PLValue, error, ControlFlow) {
	if stmt.WhenExpr != "" {
		cond, err := interp.evalBool(ctx, scope, stmt.WhenExpr)
		if err != nil {
			return PLValue{}, fmt.Errorf("EXIT WHEN: %w", err), FlowNormal
		}
		if !cond {
			return PLValue{}, nil, FlowNormal
		}
	}
	return PLValue{}, nil, FlowExit
}

// ---------------------------------------------------------------------------
// CONTINUE
// ---------------------------------------------------------------------------

func (interp *Interpreter) execContinueStmt(ctx context.Context, scope *Scope, stmt *ContinueStmt) (PLValue, error, ControlFlow) {
	if stmt.WhenExpr != "" {
		cond, err := interp.evalBool(ctx, scope, stmt.WhenExpr)
		if err != nil {
			return PLValue{}, fmt.Errorf("CONTINUE WHEN: %w", err), FlowNormal
		}
		if !cond {
			return PLValue{}, nil, FlowNormal
		}
	}
	return PLValue{}, nil, FlowContinue
}

// ---------------------------------------------------------------------------
// CALL
// ---------------------------------------------------------------------------

func (interp *Interpreter) execCall(ctx context.Context, scope *Scope, stmt *CallStmt) (PLValue, error, ControlFlow) {
	funcDef := interp.catalog.Lookup(stmt.FuncName)
	if funcDef == nil {
		return PLValue{}, fmt.Errorf("function or procedure %q not found", stmt.FuncName), FlowNormal
	}

	// Evaluate arguments.
	args := make([]PLValue, len(stmt.Args))
	for i, argExpr := range stmt.Args {
		val, err := interp.evalExpr(ctx, scope, argExpr)
		if err != nil {
			return PLValue{}, fmt.Errorf("CALL arg %d: %w", i+1, err), FlowNormal
		}
		args[i] = val
	}

	result, err := interp.ExecuteFunction(ctx, funcDef, args)
	if err != nil {
		return PLValue{}, err, FlowNormal
	}
	return result, nil, FlowNormal
}

// ---------------------------------------------------------------------------
// Expression evaluation
// ---------------------------------------------------------------------------

// evalBool evaluates an expression and coerces it to a boolean.
func (interp *Interpreter) evalBool(ctx context.Context, scope *Scope, expr string) (bool, error) {
	val, err := interp.evalExpr(ctx, scope, expr)
	if err != nil {
		return false, err
	}
	if val.IsNull {
		return false, nil // NULL is treated as false in conditionals
	}
	b, err := Coerce(val, PLBoolean{})
	if err != nil {
		return false, err
	}
	if bv, ok := b.Value.(bool); ok {
		return bv, nil
	}
	return false, nil
}

// evalInt evaluates an expression and coerces it to int64.
func (interp *Interpreter) evalInt(ctx context.Context, scope *Scope, expr string) (int64, error) {
	val, err := interp.evalExpr(ctx, scope, expr)
	if err != nil {
		return 0, err
	}
	switch v := val.Value.(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		n, e := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if e != nil {
			f, ef := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if ef != nil {
				return 0, fmt.Errorf("cannot convert %q to integer", v)
			}
			return int64(f), nil
		}
		return n, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to integer", v)
	}
}

// evalExpr evaluates a PL/pgSQL expression string and returns a PLValue.
// It handles common patterns directly and falls back to SQL for complex
// expressions.
func (interp *Interpreter) evalExpr(ctx context.Context, scope *Scope, expr string) (PLValue, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return NullValue(PLVoid{}), nil
	}

	// NULL literal.
	if strings.ToUpper(expr) == "NULL" {
		return NullValue(PLText{}), nil
	}

	// Boolean literals.
	upper := strings.ToUpper(expr)
	if upper == "TRUE" {
		return NewPLValue(PLBoolean{}, true), nil
	}
	if upper == "FALSE" {
		return NewPLValue(PLBoolean{}, false), nil
	}

	// Integer literal.
	if n, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return NewPLValue(PLInteger{}, n), nil
	}

	// Numeric literal with decimal point.
	if f, err := strconv.ParseFloat(expr, 64); err == nil && strings.Contains(expr, ".") {
		return NewPLValue(PLNumeric{}, f), nil
	}

	// String literal (single-quoted).
	if len(expr) >= 2 && expr[0] == '\'' && expr[len(expr)-1] == '\'' {
		inner := expr[1 : len(expr)-1]
		inner = strings.ReplaceAll(inner, "''", "'")
		return NewPLValue(PLText{}, inner), nil
	}

	// NOT expression.
	if strings.HasPrefix(upper, "NOT ") {
		inner := strings.TrimSpace(expr[4:])
		val, err := interp.evalExpr(ctx, scope, inner)
		if err != nil {
			return PLValue{}, err
		}
		if val.IsNull {
			return NullValue(PLBoolean{}), nil
		}
		bv, err := Coerce(val, PLBoolean{})
		if err != nil {
			return PLValue{}, err
		}
		if b, ok := bv.Value.(bool); ok {
			return NewPLValue(PLBoolean{}, !b), nil
		}
		return NullValue(PLBoolean{}), nil
	}

	// Record field access: rec.field
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 {
		varName := expr[:dotIdx]
		fieldName := expr[dotIdx+1:]
		if val, ok := scope.Get(varName); ok {
			if rec, ok := val.Value.(map[string]PLValue); ok {
				if fv, ok := rec[strings.ToLower(fieldName)]; ok {
					return fv, nil
				}
				return NullValue(PLText{}), nil
			}
		}
	}

	// IS NULL / IS NOT NULL.
	if strings.HasSuffix(upper, " IS NULL") {
		innerExpr := strings.TrimSpace(expr[:len(expr)-8])
		val, err := interp.evalExpr(ctx, scope, innerExpr)
		if err != nil {
			return PLValue{}, err
		}
		return NewPLValue(PLBoolean{}, val.IsNull), nil
	}
	if strings.HasSuffix(upper, " IS NOT NULL") {
		innerExpr := strings.TrimSpace(expr[:len(expr)-12])
		val, err := interp.evalExpr(ctx, scope, innerExpr)
		if err != nil {
			return PLValue{}, err
		}
		return NewPLValue(PLBoolean{}, !val.IsNull), nil
	}

	// Logical operators: AND, OR (handle before comparison to get precedence right).
	if idx := findTopLevelOp(upper, " AND "); idx >= 0 {
		left, err := interp.evalExpr(ctx, scope, expr[:idx])
		if err != nil {
			return PLValue{}, err
		}
		right, err := interp.evalExpr(ctx, scope, expr[idx+5:])
		if err != nil {
			return PLValue{}, err
		}
		lb, _ := toBool(left)
		rb, _ := toBool(right)
		return NewPLValue(PLBoolean{}, lb && rb), nil
	}
	if idx := findTopLevelOp(upper, " OR "); idx >= 0 {
		left, err := interp.evalExpr(ctx, scope, expr[:idx])
		if err != nil {
			return PLValue{}, err
		}
		right, err := interp.evalExpr(ctx, scope, expr[idx+4:])
		if err != nil {
			return PLValue{}, err
		}
		lb, _ := toBool(left)
		rb, _ := toBool(right)
		return NewPLValue(PLBoolean{}, lb || rb), nil
	}

	// Comparison operators: =, <>, !=, <, >, <=, >=
	for _, op := range []string{"<>", "!=", "<=", ">=", "<", ">", "="} {
		if idx := findTopLevelOp(expr, " "+op+" "); idx >= 0 {
			left, err := interp.evalExpr(ctx, scope, expr[:idx])
			if err != nil {
				return PLValue{}, err
			}
			right, err := interp.evalExpr(ctx, scope, expr[idx+len(op)+2:])
			if err != nil {
				return PLValue{}, err
			}
			// NULL propagation for comparison.
			if left.IsNull || right.IsNull {
				return NullValue(PLBoolean{}), nil
			}
			result := compareValues(left, right, op)
			return NewPLValue(PLBoolean{}, result), nil
		}
	}

	// String concatenation: ||
	if idx := findTopLevelOp(expr, " || "); idx >= 0 {
		left, err := interp.evalExpr(ctx, scope, expr[:idx])
		if err != nil {
			return PLValue{}, err
		}
		right, err := interp.evalExpr(ctx, scope, expr[idx+4:])
		if err != nil {
			return PLValue{}, err
		}
		if left.IsNull || right.IsNull {
			return NullValue(PLText{}), nil
		}
		ls := plValueToString(left)
		rs := plValueToString(right)
		return NewPLValue(PLText{}, ls+rs), nil
	}
	// Also handle || without spaces.
	if idx := findTopLevelOp(expr, "||"); idx >= 0 && idx > 0 {
		left, err := interp.evalExpr(ctx, scope, expr[:idx])
		if err != nil {
			return PLValue{}, err
		}
		right, err := interp.evalExpr(ctx, scope, expr[idx+2:])
		if err != nil {
			return PLValue{}, err
		}
		if left.IsNull || right.IsNull {
			return NullValue(PLText{}), nil
		}
		ls := plValueToString(left)
		rs := plValueToString(right)
		return NewPLValue(PLText{}, ls+rs), nil
	}

	// Arithmetic: +, -, *, /, %
	// Check for + and - last since they can appear in numeric literals.
	for _, op := range []string{"+", "-"} {
		idx := findTopLevelArithOp(expr, op)
		if idx > 0 { // idx > 0 to avoid unary minus at start
			left, err := interp.evalExpr(ctx, scope, expr[:idx])
			if err != nil {
				return PLValue{}, err
			}
			right, err := interp.evalExpr(ctx, scope, expr[idx+1:])
			if err != nil {
				return PLValue{}, err
			}
			return arithmeticOp(left, right, op)
		}
	}
	for _, op := range []string{"*", "/", "%"} {
		if idx := findTopLevelOp(expr, op); idx > 0 {
			left, err := interp.evalExpr(ctx, scope, expr[:idx])
			if err != nil {
				return PLValue{}, err
			}
			right, err := interp.evalExpr(ctx, scope, expr[idx+1:])
			if err != nil {
				return PLValue{}, err
			}
			return arithmeticOp(left, right, op)
		}
	}

	// Parenthesized expression.
	if expr[0] == '(' && expr[len(expr)-1] == ')' {
		return interp.evalExpr(ctx, scope, expr[1:len(expr)-1])
	}

	// Function call: funcname(args...)
	if parenIdx := strings.Index(expr, "("); parenIdx > 0 && expr[len(expr)-1] == ')' {
		funcName := strings.TrimSpace(expr[:parenIdx])
		argsStr := expr[parenIdx+1 : len(expr)-1]
		// Check for built-in functions.
		if interp.builtins.Has(strings.ToLower(funcName)) {
			args, err := interp.evalArgList(ctx, scope, argsStr)
			if err != nil {
				return PLValue{}, fmt.Errorf("args for %s: %w", funcName, err)
			}
			return interp.builtins.Call(strings.ToLower(funcName), args)
		}
		// Check the function catalog.
		if funcDef := interp.catalog.Lookup(funcName); funcDef != nil {
			args, err := interp.evalArgList(ctx, scope, argsStr)
			if err != nil {
				return PLValue{}, fmt.Errorf("args for %s: %w", funcName, err)
			}
			return interp.ExecuteFunction(ctx, funcDef, args)
		}
	}

	// Variable reference.
	if val, ok := scope.Get(expr); ok {
		return val, nil
	}

	// CASE WHEN ... THEN ... ELSE ... END.
	if strings.HasPrefix(upper, "CASE") && strings.HasSuffix(upper, "END") {
		return interp.evalCase(ctx, scope, expr)
	}

	// Fallback: execute as SQL expression: SELECT (expr).
	if interp.sqlExec != nil {
		sqlExpr := interp.substituteVars(scope, expr)
		rows, err := interp.sqlExec.QuerySQL(ctx, "SELECT ("+sqlExpr+")")
		if err != nil {
			return PLValue{}, fmt.Errorf("expression evaluation %q: %w", expr, err)
		}
		if len(rows) > 0 {
			for _, v := range rows[0] {
				return goValueToPLValue(v), nil
			}
		}
		return NullValue(PLText{}), nil
	}

	return PLValue{}, fmt.Errorf("cannot evaluate expression: %q", expr)
}

// evalCase handles CASE WHEN ... THEN ... ELSE ... END.
func (interp *Interpreter) evalCase(ctx context.Context, scope *Scope, expr string) (PLValue, error) {
	// Strip CASE and END.
	inner := strings.TrimSpace(expr[4 : len(expr)-3])
	upper := strings.ToUpper(inner)

	// Simple search: WHEN cond THEN result [WHEN ...] [ELSE result]
	for {
		whenIdx := strings.Index(strings.ToUpper(inner), "WHEN ")
		if whenIdx < 0 {
			break
		}
		afterWhen := inner[whenIdx+5:]
		thenIdx := strings.Index(strings.ToUpper(afterWhen), " THEN ")
		if thenIdx < 0 {
			break
		}
		condExpr := strings.TrimSpace(afterWhen[:thenIdx])
		afterThen := afterWhen[thenIdx+6:]

		// Find the end of the THEN expression (next WHEN or ELSE).
		upperAfter := strings.ToUpper(afterThen)
		nextWhen := strings.Index(upperAfter, "WHEN ")
		nextElse := strings.Index(upperAfter, "ELSE ")
		endIdx := len(afterThen)
		if nextWhen >= 0 && nextWhen < endIdx {
			endIdx = nextWhen
		}
		if nextElse >= 0 && nextElse < endIdx {
			endIdx = nextElse
		}
		thenExpr := strings.TrimSpace(afterThen[:endIdx])

		cond, err := interp.evalBool(ctx, scope, condExpr)
		if err != nil {
			return PLValue{}, err
		}
		if cond {
			return interp.evalExpr(ctx, scope, thenExpr)
		}

		inner = afterThen[endIdx:]
	}

	// Check for ELSE.
	_ = upper
	elseIdx := strings.Index(strings.ToUpper(inner), "ELSE ")
	if elseIdx >= 0 {
		elseExpr := strings.TrimSpace(inner[elseIdx+5:])
		return interp.evalExpr(ctx, scope, elseExpr)
	}

	return NullValue(PLText{}), nil
}

// evalArgList splits a comma-separated argument string and evaluates each.
func (interp *Interpreter) evalArgList(ctx context.Context, scope *Scope, argsStr string) ([]PLValue, error) {
	argsStr = strings.TrimSpace(argsStr)
	if argsStr == "" {
		return nil, nil
	}

	// Split by commas respecting parentheses and quotes.
	parts := splitArgList(argsStr)
	result := make([]PLValue, len(parts))
	for i, part := range parts {
		val, err := interp.evalExpr(ctx, scope, strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Variable substitution in SQL
// ---------------------------------------------------------------------------

// substituteVars replaces PL/pgSQL variable references in SQL text with their
// values. It uses a simple heuristic: if a word matches a known variable name,
// it is replaced with the literal value.
func (interp *Interpreter) substituteVars(scope *Scope, sql string) string {
	// Find all variable names in scope.
	var varNames []string
	for cur := scope; cur != nil; cur = cur.parent {
		for name := range cur.variables {
			varNames = append(varNames, name)
		}
	}
	if len(varNames) == 0 {
		return sql
	}

	// Sort by length descending so longer names match first.
	for i := 0; i < len(varNames); i++ {
		for j := i + 1; j < len(varNames); j++ {
			if len(varNames[j]) > len(varNames[i]) {
				varNames[i], varNames[j] = varNames[j], varNames[i]
			}
		}
	}

	result := sql
	for _, name := range varNames {
		val, ok := scope.Get(name)
		if !ok {
			continue
		}
		// Skip special variables.
		lower := strings.ToLower(name)
		if lower == "found" || lower == "row_count" || lower == "sqlstate" || lower == "sqlerrm" {
			continue
		}
		// Replace word boundaries.
		pattern := `\b` + regexp.QuoteMeta(name) + `\b`
		re, err := regexp.Compile("(?i)" + pattern)
		if err != nil {
			continue
		}
		replacement := plValueToSQLLiteral(val)
		result = re.ReplaceAllString(result, replacement)
	}
	return result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// goValueToPLValue converts a Go interface{} from SQL results to PLValue.
func goValueToPLValue(v interface{}) PLValue {
	if v == nil {
		return NullValue(PLText{})
	}
	switch val := v.(type) {
	case int64:
		return NewPLValue(PLInteger{}, val)
	case int:
		return NewPLValue(PLInteger{}, int64(val))
	case float64:
		return NewPLValue(PLNumeric{}, val)
	case string:
		// Try to parse as number.
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return NewPLValue(PLInteger{}, n)
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil && strings.Contains(val, ".") {
			return NewPLValue(PLNumeric{}, f)
		}
		return NewPLValue(PLText{}, val)
	case bool:
		return NewPLValue(PLBoolean{}, val)
	case time.Time:
		return NewPLValue(PLTimestamp{}, val)
	case []byte:
		s := string(val)
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return NewPLValue(PLInteger{}, n)
		}
		return NewPLValue(PLText{}, s)
	default:
		return NewPLValue(PLText{}, fmt.Sprintf("%v", val))
	}
}

// plValueToString converts a PLValue to a Go string.
func plValueToString(v PLValue) string {
	if v.IsNull {
		return ""
	}
	switch val := v.Value.(type) {
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		if val == math.Trunc(val) && !math.IsInf(val, 0) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", val)
	}
}

// plValueToSQLLiteral converts a PLValue to a SQL literal string for substitution.
func plValueToSQLLiteral(v PLValue) string {
	if v.IsNull {
		return "NULL"
	}
	switch val := v.Value.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("'%v'", val)
	}
}

// toBool converts a PLValue to a Go bool.
func toBool(v PLValue) (bool, error) {
	if v.IsNull {
		return false, nil
	}
	switch val := v.Value.(type) {
	case bool:
		return val, nil
	case int64:
		return val != 0, nil
	case float64:
		return val != 0, nil
	case string:
		lower := strings.ToLower(strings.TrimSpace(val))
		switch lower {
		case "true", "t", "yes", "y", "1", "on":
			return true, nil
		case "false", "f", "no", "n", "0", "off", "":
			return false, nil
		}
		return false, fmt.Errorf("cannot convert %q to boolean", val)
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", val)
	}
}

// compareValues compares two PLValues using the given operator.
func compareValues(left, right PLValue, op string) bool {
	// Try numeric comparison first.
	lf, lok := toFloat(left)
	rf, rok := toFloat(right)
	if lok && rok {
		switch op {
		case "=":
			return lf == rf
		case "<>", "!=":
			return lf != rf
		case "<":
			return lf < rf
		case ">":
			return lf > rf
		case "<=":
			return lf <= rf
		case ">=":
			return lf >= rf
		}
	}

	// Fall back to string comparison.
	ls := plValueToString(left)
	rs := plValueToString(right)
	switch op {
	case "=":
		return ls == rs
	case "<>", "!=":
		return ls != rs
	case "<":
		return ls < rs
	case ">":
		return ls > rs
	case "<=":
		return ls <= rs
	case ">=":
		return ls >= rs
	}
	return false
}

// toFloat tries to convert a PLValue to float64.
func toFloat(v PLValue) (float64, bool) {
	if v.IsNull {
		return 0, false
	}
	switch val := v.Value.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// arithmeticOp performs arithmetic on two PLValues.
func arithmeticOp(left, right PLValue, op string) (PLValue, error) {
	if left.IsNull || right.IsNull {
		return NullValue(PLNumeric{}), nil
	}

	lf, lok := toFloat(left)
	rf, rok := toFloat(right)
	if !lok || !rok {
		return PLValue{}, fmt.Errorf("cannot perform arithmetic on non-numeric values")
	}

	var result float64
	switch op {
	case "+":
		result = lf + rf
	case "-":
		result = lf - rf
	case "*":
		result = lf * rf
	case "/":
		if rf == 0 {
			return PLValue{}, &RaiseError{
				Level:    "EXCEPTION",
				Message:  "division by zero",
				SQLState: "22012",
			}
		}
		result = lf / rf
	case "%":
		if rf == 0 {
			return PLValue{}, &RaiseError{
				Level:    "EXCEPTION",
				Message:  "division by zero",
				SQLState: "22012",
			}
		}
		result = math.Mod(lf, rf)
	default:
		return PLValue{}, fmt.Errorf("unknown operator: %s", op)
	}

	// Return integer if both inputs were integers and result is whole.
	_, leftIsInt := left.Value.(int64)
	_, rightIsInt := right.Value.(int64)
	if leftIsInt && rightIsInt && result == math.Trunc(result) {
		return NewPLValue(PLInteger{}, int64(result)), nil
	}

	return NewPLValue(PLNumeric{}, result), nil
}

// findTopLevelOp finds a binary operator in an expression string, respecting
// parentheses and string literals. Returns the index of the operator or -1.
func findTopLevelOp(expr, op string) int {
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false

	// Scan right-to-left for left-associative operators.
	for i := len(expr) - len(op); i >= 0; i-- {
		ch := expr[i]
		// Track quotes — scanning right to left, we toggle on quote chars.
		if ch == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
		}
		if ch == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
		}
		if inSingleQuote || inDoubleQuote {
			continue
		}
		if ch == ')' {
			depth++
		} else if ch == '(' {
			depth--
		}
		if depth == 0 && i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
			return i
		}
	}
	return -1
}

// findTopLevelArithOp finds + or - that acts as a binary (not unary) operator.
func findTopLevelArithOp(expr, op string) int {
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false

	// Scan right-to-left for left-associative operators.
	for i := len(expr) - 1; i >= 1; i-- {
		ch := expr[i]
		if ch == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
		}
		if ch == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
		}
		if inSingleQuote || inDoubleQuote {
			continue
		}
		if ch == ')' {
			depth++
		} else if ch == '(' {
			depth--
		}
		if depth == 0 && string(ch) == op {
			// Make sure this is a binary operator (preceded by a value, not another operator).
			prevChar := expr[i-1]
			if prevChar == ' ' || prevChar == ')' || (prevChar >= '0' && prevChar <= '9') || (prevChar >= 'a' && prevChar <= 'z') || (prevChar >= 'A' && prevChar <= 'Z') || prevChar == '_' || prevChar == '\'' {
				return i
			}
		}
	}
	return -1
}

// splitArgList splits a comma-separated argument list respecting parentheses and quotes.
func splitArgList(s string) []string {
	var parts []string
	depth := 0
	inSingle := false
	inDouble := false
	start := 0

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
		}
		if !inSingle && !inDouble {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			} else if ch == ',' && depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}
