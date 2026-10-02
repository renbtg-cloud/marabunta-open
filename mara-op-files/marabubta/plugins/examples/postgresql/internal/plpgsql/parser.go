// Marabunta - Licensed under the MIT License.
package plpgsql

import (
	"fmt"
	"strings"
)

// ParseError records a single parse error with its source position.
type ParseError struct {
	Message string
	Pos     Position
}

// Error implements the error interface.
func (e *ParseError) Error() string {
	return fmt.Sprintf("line %d, col %d: %s", e.Pos.Line, e.Pos.Col, e.Message)
}

// Parser is a recursive-descent parser for PL/pgSQL blocks and statements.
type Parser struct {
	tokens []Token
	pos    int
	errors []ParseError
}

// NewParser creates a parser from a token slice. Comments and newlines should
// already be filtered out before calling this.
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

// Errors returns all parse errors accumulated during parsing.
func (p *Parser) Errors() []ParseError {
	return p.errors
}

// --------------------------------------------------------------------------
// Public entry points
// --------------------------------------------------------------------------

// ParseBlock parses a PL/pgSQL body into a Block AST. The token stream should
// start at or before DECLARE/BEGIN and end at the matching END.
func ParseBlock(tokens []Token) (*Block, error) {
	clean := FilterTokens(tokens, TokCOMMENT, TokNEWLINE)
	parser := NewParser(clean)
	block := parser.parseBlock()
	if len(parser.errors) > 0 {
		msgs := make([]string, len(parser.errors))
		for i, e := range parser.errors {
			msgs[i] = e.Error()
		}
		return nil, fmt.Errorf("parse errors:\n%s", strings.Join(msgs, "\n"))
	}
	return block, nil
}

// ParseFunctionDef parses a CREATE [OR REPLACE] FUNCTION/PROCEDURE statement.
// The input is the full CREATE statement text. It extracts the signature and
// parses the dollar-quoted body into a Block AST.
func ParseFunctionDef(createStmt string) (*FunctionDef, error) {
	tokens, err := Tokenize(createStmt)
	if err != nil {
		return nil, fmt.Errorf("tokenize CREATE FUNCTION: %w", err)
	}
	clean := FilterTokens(tokens, TokCOMMENT, TokNEWLINE)
	parser := NewParser(clean)
	def, parseErr := parser.parseFunctionDef()
	if parseErr != nil {
		return nil, parseErr
	}
	if len(parser.errors) > 0 {
		msgs := make([]string, len(parser.errors))
		for i, e := range parser.errors {
			msgs[i] = e.Error()
		}
		return nil, fmt.Errorf("parse errors:\n%s", strings.Join(msgs, "\n"))
	}
	return def, nil
}

// ParseDO parses a DO $$ ... $$ block. The input is the body text between the
// dollar quotes (without the delimiters).
func ParseDO(body string) (*Block, error) {
	tokens, err := Tokenize(body)
	if err != nil {
		return nil, fmt.Errorf("tokenize DO block: %w", err)
	}
	return ParseBlock(tokens)
}

// --------------------------------------------------------------------------
// Token helpers
// --------------------------------------------------------------------------

// peek returns the current token without consuming it.
func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokEOF, Pos: Position{}}
	}
	return p.tokens[p.pos]
}

// peekAt returns the token at position pos+offset without consuming.
func (p *Parser) peekAt(offset int) Token {
	idx := p.pos + offset
	if idx >= len(p.tokens) {
		return Token{Type: TokEOF, Pos: Position{}}
	}
	return p.tokens[idx]
}

// advance consumes and returns the current token.
func (p *Parser) advance() Token {
	tok := p.peek()
	if tok.Type != TokEOF {
		p.pos++
	}
	return tok
}

// expect consumes a token of the expected type. If the current token does not
// match, it records an error and returns a zero token.
func (p *Parser) expect(typ TokenType) Token {
	tok := p.peek()
	if tok.Type == typ {
		return p.advance()
	}
	p.addError(tok.Pos, "expected %s, got %s (%q)", typ.String(), tok.Type.String(), tok.Value)
	return tok
}

// match returns true and advances if the current token matches the given type.
func (p *Parser) match(typ TokenType) bool {
	if p.peek().Type == typ {
		p.advance()
		return true
	}
	return false
}

// matchKeyword returns true and advances if the current token is a keyword with
// the given uppercase value.
func (p *Parser) matchKeyword(kw string) bool {
	tok := p.peek()
	if strings.ToUpper(tok.Value) == kw {
		p.advance()
		return true
	}
	return false
}

// isKeywordValue checks if the current token has the given keyword value
// (case-insensitive) without consuming it.
func (p *Parser) isKeywordValue(kw string) bool {
	return strings.ToUpper(p.peek().Value) == kw
}

// addError records a parse error at the given position.
func (p *Parser) addError(pos Position, format string, args ...interface{}) {
	p.errors = append(p.errors, ParseError{
		Message: fmt.Sprintf(format, args...),
		Pos:     pos,
	})
}

// skipToSemicolon advances the parser past the next semicolon for error recovery.
func (p *Parser) skipToSemicolon() {
	for p.peek().Type != TokSEMICOLON && p.peek().Type != TokEOF {
		p.advance()
	}
	if p.peek().Type == TokSEMICOLON {
		p.advance()
	}
}

// --------------------------------------------------------------------------
// Block parsing
// --------------------------------------------------------------------------

// parseBlock parses [<<label>>] [DECLARE ...] BEGIN ... END [label];
func (p *Parser) parseBlock() *Block {
	block := &Block{P: p.peek().Pos}

	// Optional label: <<label>>
	if p.peek().Type == TokLT && p.peekAt(1).Type == TokLT {
		p.advance() // <
		p.advance() // <
		if p.peek().Type == TokIDENT {
			block.Label = p.advance().Value
		}
		if p.peek().Type == TokGT {
			p.advance() // >
		}
		if p.peek().Type == TokGT {
			p.advance() // >
		}
	}

	// Optional DECLARE section.
	if p.peek().Type == TokDECLARE {
		p.advance()
		block.Declarations = p.parseDeclarations()
	}

	// BEGIN ... END
	p.expect(TokBEGIN)
	block.Body = p.parseStatements()

	// Optional EXCEPTION block.
	if p.peek().Type == TokEXCEPTION {
		p.advance()
		block.ExceptionHandlers = p.parseExceptionBlock()
	}

	p.expect(TokEND)

	// Optional label after END.
	if p.peek().Type == TokIDENT && p.peek().Value == block.Label {
		p.advance()
	}

	// Optional trailing semicolon.
	p.match(TokSEMICOLON)

	return block
}

// --------------------------------------------------------------------------
// Declarations
// --------------------------------------------------------------------------

// parseDeclarations parses the DECLARE section until BEGIN is found.
func (p *Parser) parseDeclarations() []DeclareStmt {
	var decls []DeclareStmt
	for p.peek().Type != TokBEGIN && p.peek().Type != TokEOF {
		decl := p.parseOneDeclaration()
		if decl != nil {
			decls = append(decls, *decl)
		}
	}
	return decls
}

// parseOneDeclaration parses: name [CONSTANT] type [NOT NULL] [{DEFAULT | :=} expr];
func (p *Parser) parseOneDeclaration() *DeclareStmt {
	if p.peek().Type == TokBEGIN || p.peek().Type == TokEOF {
		return nil
	}

	decl := &DeclareStmt{P: p.peek().Pos}

	// Variable name.
	nameTok := p.advance()
	decl.Name = nameTok.Value

	// Optional CONSTANT.
	if p.peek().Type == TokCONSTANT {
		p.advance()
		decl.Constant = true
	}

	// Type name — may be multi-word (e.g., "character varying", "double precision").
	decl.TypeName = p.parseTypeName()

	// Optional NOT NULL.
	if p.peek().Type == TokNOT {
		p.advance()
		if p.peek().Type == TokNULL {
			p.advance()
			decl.NotNull = true
		}
	}

	// Optional DEFAULT or :=.
	if p.peek().Type == TokDEFAULT || p.peek().Type == TokASSIGN {
		p.advance()
		decl.DefaultExpr = p.parseExpressionUntilSemicolon()
	}

	p.expect(TokSEMICOLON)
	return decl
}

// parseTypeName reads a type name, handling multi-word types and parenthesized
// precision/scale specifiers.
func (p *Parser) parseTypeName() string {
	var parts []string
	tok := p.advance()
	parts = append(parts, tok.Value)

	// Handle multi-word types: keep consuming identifiers/keywords that form a type.
	for {
		next := p.peek()
		upper := strings.ToUpper(next.Value)
		// Common multi-word type continuations.
		if upper == "VARYING" || upper == "PRECISION" || upper == "WITHOUT" ||
			upper == "WITH" || upper == "TIME" || upper == "ZONE" {
			parts = append(parts, p.advance().Value)
			continue
		}
		break
	}

	// Handle (precision) or (precision, scale).
	if p.peek().Type == TokLPAREN {
		p.advance()
		var inner []string
		for p.peek().Type != TokRPAREN && p.peek().Type != TokEOF {
			inner = append(inner, p.advance().Value)
		}
		p.expect(TokRPAREN)
		return strings.Join(parts, " ") + "(" + strings.Join(inner, "") + ")"
	}

	return strings.Join(parts, " ")
}

// --------------------------------------------------------------------------
// Statement dispatch
// --------------------------------------------------------------------------

// parseStatements parses statements until END, EXCEPTION, ELSE, ELSIF, or EOF.
func (p *Parser) parseStatements() []Node {
	var stmts []Node
	for {
		tok := p.peek()
		if tok.Type == TokEOF || tok.Type == TokEND ||
			tok.Type == TokEXCEPTION || tok.Type == TokELSE ||
			tok.Type == TokELSIF {
			break
		}

		stmt := p.parseOneStatement()
		if stmt != nil {
			stmts = append(stmts, stmt)
		}
	}
	return stmts
}

// parseOneStatement dispatches to the appropriate parse method based on the
// current token.
func (p *Parser) parseOneStatement() Node {
	tok := p.peek()

	switch tok.Type {
	case TokIF:
		return p.parseIf()
	case TokLOOP:
		return p.parseLoop("")
	case TokWHILE:
		return p.parseWhile("")
	case TokFOR:
		return p.parseFor("")
	case TokRETURN:
		return p.parseReturn()
	case TokRAISE:
		return p.parseRaise()
	case TokPERFORM:
		return p.parsePerform()
	case TokEXECUTE:
		return p.parseExecute()
	case TokEXIT:
		return p.parseExit()
	case TokCONTINUE:
		return p.parseContinue()
	case TokNULL:
		return p.parseNull()
	case TokCALL:
		return p.parseCall()
	case TokBEGIN:
		// Nested block.
		return p.parseBlock()
	case TokDECLARE:
		// Nested block starting with DECLARE.
		return p.parseBlock()
	default:
		// Check for label: <<label>> followed by LOOP/WHILE/FOR.
		if tok.Type == TokLT && p.peekAt(1).Type == TokLT {
			label := p.parseLabelPrefix()
			next := p.peek()
			switch next.Type {
			case TokLOOP:
				return p.parseLoop(label)
			case TokWHILE:
				return p.parseWhile(label)
			case TokFOR:
				return p.parseFor(label)
			default:
				// Label on a block.
				block := p.parseBlock()
				block.Label = label
				return block
			}
		}

		// SELECT INTO detection.
		if tok.Type == TokIDENT && strings.ToUpper(tok.Value) == "SELECT" {
			return p.parseSelectInto()
		}

		// Assignment or function call: ident := expr; or ident(args);
		if tok.Type == TokIDENT || tok.Type == TokFOUND {
			return p.parseAssignOrCall()
		}

		// Unknown — skip to next semicolon for recovery.
		p.addError(tok.Pos, "unexpected token: %s (%q)", tok.Type.String(), tok.Value)
		p.skipToSemicolon()
		return nil
	}
}

// parseLabelPrefix parses <<label>> and returns the label name.
func (p *Parser) parseLabelPrefix() string {
	p.advance() // <
	p.advance() // <
	label := ""
	if p.peek().Type == TokIDENT {
		label = p.advance().Value
	}
	if p.peek().Type == TokGT {
		p.advance()
	}
	if p.peek().Type == TokGT {
		p.advance()
	}
	return label
}

// --------------------------------------------------------------------------
// IF
// --------------------------------------------------------------------------

func (p *Parser) parseIf() *IfStmt {
	pos := p.peek().Pos
	p.expect(TokIF)

	stmt := &IfStmt{P: pos}
	stmt.Condition = p.parseExpressionUntil(TokTHEN)
	p.expect(TokTHEN)
	stmt.Then = p.parseStatements()

	// ELSIF chains.
	for p.peek().Type == TokELSIF {
		p.advance()
		clause := ElsIfClause{}
		clause.Condition = p.parseExpressionUntil(TokTHEN)
		p.expect(TokTHEN)
		clause.Body = p.parseStatements()
		stmt.ElsIfs = append(stmt.ElsIfs, clause)
	}

	// ELSE.
	if p.peek().Type == TokELSE {
		p.advance()
		stmt.Else = p.parseStatements()
	}

	p.expect(TokEND)
	// Expect IF after END.
	if p.peek().Type == TokIF {
		p.advance()
	}
	p.expect(TokSEMICOLON)

	return stmt
}

// --------------------------------------------------------------------------
// LOOP / WHILE / FOR
// --------------------------------------------------------------------------

func (p *Parser) parseLoop(label string) *LoopStmt {
	pos := p.peek().Pos
	p.expect(TokLOOP)

	stmt := &LoopStmt{Label: label, P: pos}
	stmt.Body = p.parseStatements()

	p.expect(TokEND)
	if p.peek().Type == TokLOOP {
		p.advance()
	}
	// Optional label after END LOOP.
	if label != "" && p.peek().Type == TokIDENT && p.peek().Value == label {
		p.advance()
	}
	p.expect(TokSEMICOLON)

	return stmt
}

func (p *Parser) parseWhile(label string) *WhileStmt {
	pos := p.peek().Pos
	p.expect(TokWHILE)

	stmt := &WhileStmt{Label: label, P: pos}
	stmt.Condition = p.parseExpressionUntil(TokLOOP)
	p.expect(TokLOOP)
	stmt.Body = p.parseStatements()

	p.expect(TokEND)
	if p.peek().Type == TokLOOP {
		p.advance()
	}
	if label != "" && p.peek().Type == TokIDENT && p.peek().Value == label {
		p.advance()
	}
	p.expect(TokSEMICOLON)

	return stmt
}

// parseFor parses FOR which can be either numeric range or query iteration.
func (p *Parser) parseFor(label string) Node {
	pos := p.peek().Pos
	p.expect(TokFOR)

	varName := p.advance().Value

	p.expect(TokIN)

	// Check for REVERSE.
	reverse := false
	if p.peek().Type == TokREVERSE {
		p.advance()
		reverse = true
	}

	// Now determine if this is a numeric range (expr..expr) or a query FOR.
	// We collect tokens until we hit LOOP, looking for '..' to decide.
	saved := p.pos
	isRange := false
	depth := 0
	for i := p.pos; i < len(p.tokens); i++ {
		t := p.tokens[i]
		if t.Type == TokLPAREN {
			depth++
		} else if t.Type == TokRPAREN {
			depth--
		} else if t.Type == TokDOTDOT && depth == 0 {
			isRange = true
			break
		} else if t.Type == TokLOOP && depth == 0 {
			break
		}
	}
	p.pos = saved

	if isRange {
		// Numeric range: FOR var IN [REVERSE] low .. high LOOP
		low := p.parseExpressionUntil(TokDOTDOT)
		p.expect(TokDOTDOT)
		high := p.parseExpressionUntil(TokLOOP)
		p.expect(TokLOOP)

		stmt := &ForStmt{
			Label:    label,
			Variable: varName,
			Low:      low,
			High:     high,
			Reverse:  reverse,
			P:        pos,
		}
		stmt.Body = p.parseStatements()

		p.expect(TokEND)
		if p.peek().Type == TokLOOP {
			p.advance()
		}
		if label != "" && p.peek().Type == TokIDENT && p.peek().Value == label {
			p.advance()
		}
		p.expect(TokSEMICOLON)
		return stmt
	}

	// Query FOR: FOR record IN query LOOP
	query := p.parseSQLUntil(TokLOOP)
	p.expect(TokLOOP)

	stmt := &ForQueryStmt{
		Label:    label,
		Variable: varName,
		Query:    query,
		P:        pos,
	}
	stmt.Body = p.parseStatements()

	p.expect(TokEND)
	if p.peek().Type == TokLOOP {
		p.advance()
	}
	if label != "" && p.peek().Type == TokIDENT && p.peek().Value == label {
		p.advance()
	}
	p.expect(TokSEMICOLON)
	return stmt
}

// --------------------------------------------------------------------------
// RETURN
// --------------------------------------------------------------------------

func (p *Parser) parseReturn() Node {
	pos := p.peek().Pos
	p.expect(TokRETURN)

	// RETURN QUERY ...
	if p.peek().Type == TokQUERY {
		p.advance()
		query := p.parseSQLUntilSemicolon()
		p.expect(TokSEMICOLON)
		return &ReturnQueryStmt{Query: query, P: pos}
	}

	// RETURN NEXT ...
	if p.peek().Type == TokNEXT {
		p.advance()
		expr := p.parseExpressionUntilSemicolon()
		p.expect(TokSEMICOLON)
		return &ReturnNextStmt{Expr: expr, P: pos}
	}

	// RETURN [expr];
	expr := ""
	if p.peek().Type != TokSEMICOLON {
		expr = p.parseExpressionUntilSemicolon()
	}
	p.expect(TokSEMICOLON)
	return &ReturnStmt{Expr: expr, P: pos}
}

// --------------------------------------------------------------------------
// RAISE
// --------------------------------------------------------------------------

func (p *Parser) parseRaise() *RaiseStmt {
	pos := p.peek().Pos
	p.expect(TokRAISE)

	stmt := &RaiseStmt{P: pos}

	// Optional level: DEBUG|LOG|INFO|NOTICE|WARNING|EXCEPTION.
	tok := p.peek()
	switch tok.Type {
	case TokDEBUG, TokLOG, TokINFO, TokNOTICE, TokWARNING, TokEXCEPTION:
		stmt.Level = strings.ToUpper(p.advance().Value)
	default:
		// Default level is EXCEPTION.
		stmt.Level = "EXCEPTION"
	}

	// Optional message (string literal).
	if p.peek().Type == TokSTRING_LIT {
		stmt.Message = p.advance().Value
	} else if p.peek().Type != TokSEMICOLON {
		// Message might be an expression.
		stmt.Message = p.parseExpressionUntilSemicolon()
		p.expect(TokSEMICOLON)
		return stmt
	}

	// Optional parameters: , param1, param2, ...
	for p.peek().Type == TokCOMMA {
		p.advance() // consume comma
		param := p.parseExpressionUntilCommaOrSemicolon()
		stmt.Params = append(stmt.Params, param)
	}

	// Optional USING keyword for named parameters (USING MESSAGE = expr).
	if p.peek().Type == TokUSING {
		p.advance()
		// Collect the rest until semicolon as additional info.
		for p.peek().Type != TokSEMICOLON && p.peek().Type != TokEOF {
			p.advance()
		}
	}

	p.expect(TokSEMICOLON)
	return stmt
}

// --------------------------------------------------------------------------
// PERFORM
// --------------------------------------------------------------------------

func (p *Parser) parsePerform() *PerformStmt {
	pos := p.peek().Pos
	p.expect(TokPERFORM)
	query := p.parseSQLUntilSemicolon()
	p.expect(TokSEMICOLON)
	return &PerformStmt{Query: query, P: pos}
}

// --------------------------------------------------------------------------
// SELECT INTO
// --------------------------------------------------------------------------

func (p *Parser) parseSelectInto() *SelectIntoStmt {
	pos := p.peek().Pos

	// Collect all tokens until semicolon to reconstruct the statement.
	// We need to find INTO and STRICT within the SELECT.
	var allTokens []Token
	for p.peek().Type != TokSEMICOLON && p.peek().Type != TokEOF {
		allTokens = append(allTokens, p.advance())
	}
	p.expect(TokSEMICOLON)

	stmt := &SelectIntoStmt{P: pos}

	// Find the INTO clause position.
	intoIdx := -1
	for i, t := range allTokens {
		if t.Type == TokINTO {
			intoIdx = i
			break
		}
	}

	if intoIdx < 0 {
		// No INTO found — this is a regular SQL statement captured as SELECT INTO
		// because the caller detected SELECT. Reconstruct as query.
		stmt.Query = tokensToString(allTokens)
		return stmt
	}

	// Check for STRICT before INTO.
	strictIdx := -1
	if intoIdx > 0 && allTokens[intoIdx-1].Type == TokSTRICT {
		stmt.Strict = true
		strictIdx = intoIdx - 1
	}

	// Check for STRICT after INTO and variables.
	// Find variables between INTO and FROM.
	varStart := intoIdx + 1
	varEnd := len(allTokens)
	for i := varStart; i < len(allTokens); i++ {
		upper := strings.ToUpper(allTokens[i].Value)
		if upper == "FROM" || upper == "WHERE" || upper == "GROUP" ||
			upper == "ORDER" || upper == "LIMIT" || upper == "HAVING" {
			varEnd = i
			break
		}
		if allTokens[i].Type == TokSTRICT {
			stmt.Strict = true
			// Remove STRICT from the variable list processing.
			varEnd = i
			break
		}
	}

	// Parse variable names from INTO clause (comma-separated).
	var varNames []string
	for i := varStart; i < varEnd; i++ {
		t := allTokens[i]
		if t.Type == TokCOMMA {
			continue
		}
		if t.Type == TokIDENT || t.Type == TokFOUND {
			varNames = append(varNames, t.Value)
		}
	}
	stmt.Variables = varNames

	// Reconstruct the SELECT query without the INTO clause.
	var queryParts []Token
	skipEnd := varEnd
	if stmt.Strict && varEnd < len(allTokens) && allTokens[varEnd].Type == TokSTRICT {
		skipEnd = varEnd + 1
	}
	skipStart := intoIdx
	if strictIdx >= 0 {
		skipStart = strictIdx
	}
	queryParts = append(queryParts, allTokens[:skipStart]...)
	if skipEnd < len(allTokens) {
		queryParts = append(queryParts, allTokens[skipEnd:]...)
	}
	stmt.Query = tokensToString(queryParts)

	return stmt
}

// --------------------------------------------------------------------------
// EXECUTE
// --------------------------------------------------------------------------

func (p *Parser) parseExecute() *ExecuteStmt {
	pos := p.peek().Pos
	p.expect(TokEXECUTE)

	stmt := &ExecuteStmt{P: pos}

	// SQL expression to execute — collect until INTO, USING, or semicolon.
	stmt.SQLExpr = p.parseExpressionUntilAny(TokINTO, TokUSING, TokSEMICOLON)

	// Optional INTO.
	if p.peek().Type == TokINTO {
		p.advance()
		// Check for STRICT.
		if p.peek().Type == TokSTRICT {
			p.advance()
		}
		for {
			name := p.advance().Value
			stmt.Into = append(stmt.Into, name)
			if p.peek().Type != TokCOMMA {
				break
			}
			p.advance() // consume comma
		}
	}

	// Optional USING.
	if p.peek().Type == TokUSING {
		p.advance()
		for {
			param := p.parseExpressionUntilCommaOrSemicolon()
			stmt.Using = append(stmt.Using, param)
			if p.peek().Type != TokCOMMA {
				break
			}
			p.advance() // consume comma
		}
	}

	p.expect(TokSEMICOLON)
	return stmt
}

// --------------------------------------------------------------------------
// EXIT / CONTINUE
// --------------------------------------------------------------------------

func (p *Parser) parseExit() *ExitStmt {
	pos := p.peek().Pos
	p.expect(TokEXIT)

	stmt := &ExitStmt{P: pos}

	// Optional label.
	if p.peek().Type == TokIDENT {
		stmt.Label = p.advance().Value
	}

	// Optional WHEN condition.
	if p.peek().Type == TokWHEN {
		p.advance()
		stmt.WhenExpr = p.parseExpressionUntilSemicolon()
	}

	p.expect(TokSEMICOLON)
	return stmt
}

func (p *Parser) parseContinue() *ContinueStmt {
	pos := p.peek().Pos
	p.expect(TokCONTINUE)

	stmt := &ContinueStmt{P: pos}

	// Optional label.
	if p.peek().Type == TokIDENT {
		stmt.Label = p.advance().Value
	}

	// Optional WHEN condition.
	if p.peek().Type == TokWHEN {
		p.advance()
		stmt.WhenExpr = p.parseExpressionUntilSemicolon()
	}

	p.expect(TokSEMICOLON)
	return stmt
}

// --------------------------------------------------------------------------
// NULL / CALL
// --------------------------------------------------------------------------

func (p *Parser) parseNull() *NullStmt {
	pos := p.peek().Pos
	p.expect(TokNULL)
	p.expect(TokSEMICOLON)
	return &NullStmt{P: pos}
}

func (p *Parser) parseCall() *CallStmt {
	pos := p.peek().Pos
	p.expect(TokCALL)

	stmt := &CallStmt{P: pos}

	// Function name.
	if p.peek().Type == TokIDENT {
		stmt.FuncName = p.advance().Value
	} else {
		stmt.FuncName = p.advance().Value
	}

	// Arguments in parentheses.
	if p.peek().Type == TokLPAREN {
		p.advance()
		if p.peek().Type != TokRPAREN {
			for {
				arg := p.parseExpressionUntilCommaOrRParen()
				stmt.Args = append(stmt.Args, arg)
				if p.peek().Type != TokCOMMA {
					break
				}
				p.advance() // consume comma
			}
		}
		p.expect(TokRPAREN)
	}

	p.expect(TokSEMICOLON)
	return stmt
}

// --------------------------------------------------------------------------
// Assignment or function call
// --------------------------------------------------------------------------

// parseAssignOrCall handles: variable := expr; or function_call(args);
func (p *Parser) parseAssignOrCall() Node {
	pos := p.peek().Pos
	name := p.advance().Value

	// Check for dotted name (record.field := ...).
	for p.peek().Type == TokDOT {
		p.advance()
		name += "." + p.advance().Value
	}

	// Assignment: name := expr;
	if p.peek().Type == TokASSIGN {
		p.advance()
		expr := p.parseExpressionUntilSemicolon()
		p.expect(TokSEMICOLON)
		return &AssignStmt{Variable: name, Expr: expr, P: pos}
	}

	// Function call: name(args);
	if p.peek().Type == TokLPAREN {
		p.advance()
		var args []string
		if p.peek().Type != TokRPAREN {
			for {
				arg := p.parseExpressionUntilCommaOrRParen()
				args = append(args, arg)
				if p.peek().Type != TokCOMMA {
					break
				}
				p.advance() // consume comma
			}
		}
		p.expect(TokRPAREN)
		p.expect(TokSEMICOLON)
		return &CallStmt{FuncName: name, Args: args, P: pos}
	}

	// Bare assignment with = (some dialects).
	if p.peek().Type == TokEQ {
		p.advance()
		expr := p.parseExpressionUntilSemicolon()
		p.expect(TokSEMICOLON)
		return &AssignStmt{Variable: name, Expr: expr, P: pos}
	}

	// Unknown — treat as error and skip.
	p.addError(pos, "expected := or ( after %q", name)
	p.skipToSemicolon()
	return nil
}

// --------------------------------------------------------------------------
// Exception block
// --------------------------------------------------------------------------

// parseExceptionBlock parses WHEN condition [OR condition] THEN statements ...
func (p *Parser) parseExceptionBlock() []ExceptionHandler {
	var handlers []ExceptionHandler

	for p.peek().Type == TokWHEN {
		p.advance() // consume WHEN

		handler := ExceptionHandler{}

		// Condition names separated by OR.
		for {
			cond := p.advance().Value
			handler.Conditions = append(handler.Conditions, strings.ToLower(cond))
			if p.peek().Type == TokOR {
				p.advance()
				continue
			}
			break
		}

		p.expect(TokTHEN)
		handler.Body = p.parseStatements()
		handlers = append(handlers, handler)
	}

	return handlers
}

// --------------------------------------------------------------------------
// CREATE FUNCTION / CREATE PROCEDURE
// --------------------------------------------------------------------------

// parseFunctionDef parses CREATE [OR REPLACE] FUNCTION/PROCEDURE ...
func (p *Parser) parseFunctionDef() (*FunctionDef, error) {
	def := &FunctionDef{
		Language:   "plpgsql",
		Volatility: "volatile",
	}

	// CREATE
	if p.peek().Type != TokCREATE {
		return nil, fmt.Errorf("expected CREATE, got %s", p.peek().Value)
	}
	p.advance()

	// Optional OR REPLACE.
	if p.peek().Type == TokOR {
		p.advance()
		if p.peek().Type == TokREPLACE {
			p.advance()
			def.Replace = true
		}
	}

	// FUNCTION or PROCEDURE.
	switch p.peek().Type {
	case TokFUNCTION:
		p.advance()
		def.IsProc = false
	case TokPROCEDURE:
		p.advance()
		def.IsProc = true
	default:
		return nil, fmt.Errorf("expected FUNCTION or PROCEDURE, got %s", p.peek().Value)
	}

	// Function name — may be schema-qualified.
	name := p.advance().Value
	if p.peek().Type == TokDOT {
		p.advance()
		name += "." + p.advance().Value
	}
	def.Name = name

	// Parameter list.
	p.expect(TokLPAREN)
	if p.peek().Type != TokRPAREN {
		def.Params = p.parseParamDefs()
	}
	p.expect(TokRPAREN)

	// Parse trailing clauses: RETURNS, LANGUAGE, AS, IMMUTABLE/STABLE/VOLATILE.
	for p.peek().Type != TokEOF {
		tok := p.peek()

		switch tok.Type {
		case TokRETURNS:
			p.advance()
			if p.peek().Type == TokSETOF {
				p.advance()
				def.ReturnsSetOf = true
			}
			def.ReturnType = p.parseTypeName()

		case TokLANGUAGE:
			p.advance()
			def.Language = strings.ToLower(p.advance().Value)

		case TokIMMUTABLE:
			p.advance()
			def.Volatility = "immutable"

		case TokSTABLE:
			p.advance()
			def.Volatility = "stable"

		case TokVOLATILE:
			p.advance()
			def.Volatility = "volatile"

		case TokAS:
			p.advance()
			// The body is in a dollar-quoted string.
			if p.peek().Type == TokDOLLAR_STRING {
				bodyText := p.advance().Value
				def.BodySource = bodyText
				bodyTokens, err := Tokenize(bodyText)
				if err != nil {
					return nil, fmt.Errorf("tokenize function body: %w", err)
				}
				cleanTokens := FilterTokens(bodyTokens, TokCOMMENT, TokNEWLINE)
				bodyParser := NewParser(cleanTokens)
				def.Body = bodyParser.parseBlock()
				if len(bodyParser.errors) > 0 {
					msgs := make([]string, len(bodyParser.errors))
					for i, e := range bodyParser.errors {
						msgs[i] = e.Error()
					}
					return nil, fmt.Errorf("parse function body:\n%s", strings.Join(msgs, "\n"))
				}
			} else if p.peek().Type == TokSTRING_LIT {
				bodyText := p.advance().Value
				def.BodySource = bodyText
				bodyTokens, err := Tokenize(bodyText)
				if err != nil {
					return nil, fmt.Errorf("tokenize function body: %w", err)
				}
				cleanTokens := FilterTokens(bodyTokens, TokCOMMENT, TokNEWLINE)
				bodyParser := NewParser(cleanTokens)
				def.Body = bodyParser.parseBlock()
				if len(bodyParser.errors) > 0 {
					msgs := make([]string, len(bodyParser.errors))
					for i, e := range bodyParser.errors {
						msgs[i] = e.Error()
					}
					return nil, fmt.Errorf("parse function body:\n%s", strings.Join(msgs, "\n"))
				}
			}

		default:
			// Unknown clause — skip.
			p.advance()
		}
	}

	// Default return type for procedures.
	if def.IsProc && def.ReturnType == "" {
		def.ReturnType = "void"
	}

	return def, nil
}

// parseParamDefs parses a comma-separated parameter list.
func (p *Parser) parseParamDefs() []ParamDef {
	var params []ParamDef

	for {
		param := ParamDef{Mode: "IN"}

		// Optional mode: IN, OUT, INOUT.
		upper := strings.ToUpper(p.peek().Value)
		if upper == "IN" || upper == "OUT" || upper == "INOUT" {
			param.Mode = upper
			p.advance()
			// Check for INOUT as two separate tokens: IN OUT.
			if param.Mode == "IN" && strings.ToUpper(p.peek().Value) == "OUT" {
				param.Mode = "INOUT"
				p.advance()
			}
		}

		// Name and type. Ambiguity: is the next token a name or a type?
		// If the token after next is a type-like token, then current is a name.
		// Otherwise, current is the type (anonymous parameter).
		first := p.peek()
		second := p.peekAt(1)

		if first.Type == TokIDENT && (second.Type == TokIDENT || isTypeKeyword(second)) {
			// name type
			param.Name = p.advance().Value
			param.TypeName = p.parseTypeName()
		} else {
			// just type (no name)
			param.TypeName = p.parseTypeName()
		}

		// Optional DEFAULT.
		if p.peek().Type == TokDEFAULT || p.peek().Type == TokASSIGN {
			p.advance()
			param.DefaultExpr = p.parseExpressionUntilCommaOrRParen()
		}

		params = append(params, param)

		if p.peek().Type != TokCOMMA {
			break
		}
		p.advance() // consume comma
	}

	return params
}

// isTypeKeyword returns true if the token looks like a type name keyword.
func isTypeKeyword(tok Token) bool {
	upper := strings.ToUpper(tok.Value)
	switch upper {
	case "INTEGER", "INT", "INT4", "BIGINT", "INT8", "NUMERIC", "DECIMAL",
		"REAL", "FLOAT", "FLOAT4", "FLOAT8", "TEXT", "VARCHAR", "CHAR",
		"CHARACTER", "BOOLEAN", "BOOL", "DATE", "TIMESTAMP", "VOID",
		"RECORD", "SERIAL", "BIGSERIAL", "SETOF", "NAME":
		return true
	}
	return false
}

// --------------------------------------------------------------------------
// Expression parsing helpers
// --------------------------------------------------------------------------

// parseExpressionUntil collects token text until the stop token type is seen
// at the top level (not inside parentheses). Returns the expression as a string.
func (p *Parser) parseExpressionUntil(stop TokenType) string {
	var parts []string
	depth := 0

	for p.peek().Type != TokEOF {
		tok := p.peek()
		if tok.Type == stop && depth == 0 {
			break
		}
		if tok.Type == TokLPAREN {
			depth++
		} else if tok.Type == TokRPAREN {
			depth--
		}
		parts = append(parts, p.advance().Value)
	}

	return strings.TrimSpace(strings.Join(parts, " "))
}

// parseExpressionUntilSemicolon collects token text until a semicolon at the
// top level.
func (p *Parser) parseExpressionUntilSemicolon() string {
	return p.parseExpressionUntil(TokSEMICOLON)
}

// parseExpressionUntilCommaOrSemicolon collects until comma or semicolon at
// the top level.
func (p *Parser) parseExpressionUntilCommaOrSemicolon() string {
	var parts []string
	depth := 0

	for p.peek().Type != TokEOF {
		tok := p.peek()
		if depth == 0 && (tok.Type == TokCOMMA || tok.Type == TokSEMICOLON) {
			break
		}
		if tok.Type == TokLPAREN {
			depth++
		} else if tok.Type == TokRPAREN {
			depth--
		}
		parts = append(parts, p.advance().Value)
	}

	return strings.TrimSpace(strings.Join(parts, " "))
}

// parseExpressionUntilCommaOrRParen collects until comma or closing paren at
// the top level.
func (p *Parser) parseExpressionUntilCommaOrRParen() string {
	var parts []string
	depth := 0

	for p.peek().Type != TokEOF {
		tok := p.peek()
		if depth == 0 && (tok.Type == TokCOMMA || tok.Type == TokRPAREN) {
			break
		}
		if tok.Type == TokLPAREN {
			depth++
		} else if tok.Type == TokRPAREN {
			if depth == 0 {
				break
			}
			depth--
		}
		parts = append(parts, p.advance().Value)
	}

	return strings.TrimSpace(strings.Join(parts, " "))
}

// parseExpressionUntilAny collects until any of the stop types at top level.
func (p *Parser) parseExpressionUntilAny(stops ...TokenType) string {
	stopSet := make(map[TokenType]bool, len(stops))
	for _, s := range stops {
		stopSet[s] = true
	}

	var parts []string
	depth := 0

	for p.peek().Type != TokEOF {
		tok := p.peek()
		if depth == 0 && stopSet[tok.Type] {
			break
		}
		if tok.Type == TokLPAREN {
			depth++
		} else if tok.Type == TokRPAREN {
			depth--
		}
		parts = append(parts, p.advance().Value)
	}

	return strings.TrimSpace(strings.Join(parts, " "))
}

// parseSQLUntil collects raw SQL text until the stop token at the top level.
func (p *Parser) parseSQLUntil(stop TokenType) string {
	return p.parseExpressionUntil(stop)
}

// parseSQLUntilSemicolon collects raw SQL text until a semicolon.
func (p *Parser) parseSQLUntilSemicolon() string {
	return p.parseExpressionUntilSemicolon()
}

// --------------------------------------------------------------------------
// Token reconstruction helpers
// --------------------------------------------------------------------------

// tokensToString reconstructs source text from a token slice.
func tokensToString(tokens []Token) string {
	var parts []string
	for _, t := range tokens {
		parts = append(parts, t.Value)
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}
