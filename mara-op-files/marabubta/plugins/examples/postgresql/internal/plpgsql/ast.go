// Marabunta - Licensed under the MIT License.
package plpgsql

// Position records a location in the source text for error reporting.
type Position struct {
	Line int
	Col  int
}

// Node is the interface implemented by all PL/pgSQL AST nodes.
type Node interface {
	// NodeType returns a short string identifying the node kind (e.g. "IfStmt").
	NodeType() string
	// Pos returns the source position of the node.
	Pos() Position
}

// --------------------------------------------------------------------------
// Function / Procedure definition
// --------------------------------------------------------------------------

// FunctionDef represents a CREATE FUNCTION or CREATE PROCEDURE definition.
type FunctionDef struct {
	Name         string     // function or procedure name
	Params       []ParamDef // parameter list
	ReturnType   string     // return type name ("void" for procedures)
	ReturnsSetOf bool       // RETURNS SETOF <type>
	Body         *Block     // parsed PL/pgSQL body
	Language     string     // "plpgsql" (only language supported)
	Volatility   string     // "volatile", "stable", "immutable"
	IsProc       bool       // true for procedures, false for functions
	Replace      bool       // true if CREATE OR REPLACE
	BodySource   string     // original unparsed body text
}

// ParamDef describes a single function/procedure parameter.
type ParamDef struct {
	Name        string // parameter name (may be empty for positional)
	TypeName    string // type name as written in source
	Mode        string // "IN", "OUT", "INOUT" (default "IN")
	DefaultExpr string // default value expression (empty if none)
}

// --------------------------------------------------------------------------
// Block
// --------------------------------------------------------------------------

// Block is the top-level structure of a PL/pgSQL body: DECLARE ... BEGIN ... END.
type Block struct {
	Label             string             // optional block label
	Declarations      []DeclareStmt      // variable declarations
	Body              []Node             // statements between BEGIN and END
	ExceptionHandlers []ExceptionHandler // EXCEPTION WHEN handlers
	P                 Position
}

func (b *Block) NodeType() string { return "Block" }
func (b *Block) Pos() Position    { return b.P }

// --------------------------------------------------------------------------
// Declaration
// --------------------------------------------------------------------------

// DeclareStmt declares a local variable.
type DeclareStmt struct {
	Name        string // variable name
	TypeName    string // type name
	NotNull     bool   // NOT NULL constraint
	DefaultExpr string // default/initial value expression
	Constant    bool   // CONSTANT — value cannot be reassigned
	P           Position
}

func (d *DeclareStmt) NodeType() string { return "DeclareStmt" }
func (d *DeclareStmt) Pos() Position    { return d.P }

// --------------------------------------------------------------------------
// Statements
// --------------------------------------------------------------------------

// AssignStmt represents variable := expression.
type AssignStmt struct {
	Variable string // target variable name
	Expr     string // right-hand side expression text
	P        Position
}

func (a *AssignStmt) NodeType() string { return "AssignStmt" }
func (a *AssignStmt) Pos() Position    { return a.P }

// IfStmt represents IF ... THEN ... [ELSIF ... THEN ...] [ELSE ...] END IF.
type IfStmt struct {
	Condition string       // condition expression text
	Then      []Node       // statements in the THEN branch
	ElsIfs    []ElsIfClause // optional ELSIF branches
	Else      []Node       // optional ELSE branch
	P         Position
}

func (i *IfStmt) NodeType() string { return "IfStmt" }
func (i *IfStmt) Pos() Position    { return i.P }

// ElsIfClause represents a single ELSIF branch.
type ElsIfClause struct {
	Condition string // condition expression text
	Body      []Node // statements in this branch
}

// LoopStmt represents an unconditional LOOP ... END LOOP.
type LoopStmt struct {
	Label string // optional loop label
	Body  []Node
	P     Position
}

func (l *LoopStmt) NodeType() string { return "LoopStmt" }
func (l *LoopStmt) Pos() Position    { return l.P }

// WhileStmt represents WHILE condition LOOP ... END LOOP.
type WhileStmt struct {
	Label     string
	Condition string // condition expression text
	Body      []Node
	P         Position
}

func (w *WhileStmt) NodeType() string { return "WhileStmt" }
func (w *WhileStmt) Pos() Position    { return w.P }

// ForStmt represents FOR var IN [REVERSE] low..high LOOP ... END LOOP.
type ForStmt struct {
	Label    string
	Variable string // loop variable name
	Low      string // lower bound expression
	High     string // upper bound expression
	Reverse  bool   // iterate in reverse
	Body     []Node
	P        Position
}

func (f *ForStmt) NodeType() string { return "ForStmt" }
func (f *ForStmt) Pos() Position    { return f.P }

// ForQueryStmt represents FOR record IN query LOOP ... END LOOP.
type ForQueryStmt struct {
	Label    string
	Variable string // loop variable name (record)
	Query    string // SQL query text
	Body     []Node
	P        Position
}

func (f *ForQueryStmt) NodeType() string { return "ForQueryStmt" }
func (f *ForQueryStmt) Pos() Position    { return f.P }

// ReturnStmt represents RETURN expression.
type ReturnStmt struct {
	Expr string // expression text (empty for bare RETURN in procedures)
	P    Position
}

func (r *ReturnStmt) NodeType() string { return "ReturnStmt" }
func (r *ReturnStmt) Pos() Position    { return r.P }

// ReturnQueryStmt represents RETURN QUERY sql_query.
type ReturnQueryStmt struct {
	Query string // SQL query text
	P     Position
}

func (r *ReturnQueryStmt) NodeType() string { return "ReturnQueryStmt" }
func (r *ReturnQueryStmt) Pos() Position    { return r.P }

// ReturnNextStmt represents RETURN NEXT expression.
type ReturnNextStmt struct {
	Expr string // expression text
	P    Position
}

func (r *ReturnNextStmt) NodeType() string { return "ReturnNextStmt" }
func (r *ReturnNextStmt) Pos() Position    { return r.P }

// RaiseStmt represents RAISE level 'message', param1, param2, ...
type RaiseStmt struct {
	Level   string   // DEBUG, LOG, INFO, NOTICE, WARNING, EXCEPTION
	Message string   // format string
	Params  []string // substitution parameters
	P       Position
}

func (r *RaiseStmt) NodeType() string { return "RaiseStmt" }
func (r *RaiseStmt) Pos() Position    { return r.P }

// PerformStmt represents PERFORM query (executes SQL and discards results).
type PerformStmt struct {
	Query string // SQL query text
	P     Position
}

func (p *PerformStmt) NodeType() string { return "PerformStmt" }
func (p *PerformStmt) Pos() Position    { return p.P }

// SelectIntoStmt represents SELECT ... INTO var1, var2, ... FROM ...
type SelectIntoStmt struct {
	Variables []string // target variable names
	Query     string   // full SELECT query text (without INTO clause)
	Strict    bool     // STRICT — raise error if not exactly one row
	P         Position
}

func (s *SelectIntoStmt) NodeType() string { return "SelectIntoStmt" }
func (s *SelectIntoStmt) Pos() Position    { return s.P }

// ExecuteStmt represents EXECUTE sql_expression [INTO var] [USING param, ...].
type ExecuteStmt struct {
	SQLExpr string   // SQL expression to evaluate and execute
	Into    []string // optional INTO target variables
	Using   []string // optional USING parameter expressions
	P       Position
}

func (e *ExecuteStmt) NodeType() string { return "ExecuteStmt" }
func (e *ExecuteStmt) Pos() Position    { return e.P }

// ExitStmt represents EXIT [label] [WHEN condition].
type ExitStmt struct {
	Label    string // optional target label
	WhenExpr string // optional WHEN condition
	P        Position
}

func (e *ExitStmt) NodeType() string { return "ExitStmt" }
func (e *ExitStmt) Pos() Position    { return e.P }

// ContinueStmt represents CONTINUE [label] [WHEN condition].
type ContinueStmt struct {
	Label    string
	WhenExpr string
	P        Position
}

func (c *ContinueStmt) NodeType() string { return "ContinueStmt" }
func (c *ContinueStmt) Pos() Position    { return c.P }

// NullStmt represents the NULL; no-op statement.
type NullStmt struct {
	P Position
}

func (n *NullStmt) NodeType() string { return "NullStmt" }
func (n *NullStmt) Pos() Position    { return n.P }

// CallStmt represents CALL procedure_name(args).
type CallStmt struct {
	FuncName string   // procedure/function name
	Args     []string // argument expression texts
	P        Position
}

func (c *CallStmt) NodeType() string { return "CallStmt" }
func (c *CallStmt) Pos() Position    { return c.P }

// ExceptionHandler represents a single WHEN condition THEN handler in an
// EXCEPTION block.
type ExceptionHandler struct {
	Conditions []string // condition names (e.g. "no_data_found", "others")
	Body       []Node   // handler statements
}
