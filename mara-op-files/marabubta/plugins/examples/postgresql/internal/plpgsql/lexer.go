// Marabunta - Licensed under the MIT License.
package plpgsql

import (
	"fmt"
	"strings"
)

// TokenType identifies the kind of lexical token.
type TokenType int

const (
	// Keywords
	TokDECLARE TokenType = iota + 1
	TokBEGIN
	TokEND
	TokIF
	TokTHEN
	TokELSIF
	TokELSE
	TokLOOP
	TokWHILE
	TokFOR
	TokIN
	TokREVERSE
	TokRETURN
	TokQUERY
	TokNEXT
	TokRAISE
	TokPERFORM
	TokEXECUTE
	TokINTO
	TokUSING
	TokSTRICT
	TokEXIT
	TokWHEN
	TokCONTINUE
	TokNULL
	TokNOT
	TokDEFAULT
	TokCONSTANT
	TokEXCEPTION
	TokOTHERS
	TokFOUND
	TokDEBUG
	TokLOG
	TokINFO
	TokNOTICE
	TokWARNING
	TokCALL
	TokLANGUAGE
	TokRETURNS
	TokSETOF
	TokAS
	TokDO
	TokCREATE
	TokFUNCTION
	TokPROCEDURE
	TokOR
	TokREPLACE
	TokIMMUTABLE
	TokSTABLE
	TokVOLATILE

	// Operators
	TokASSIGN    // :=
	TokSEMICOLON // ;
	TokLPAREN    // (
	TokRPAREN    // )
	TokCOMMA     // ,
	TokDOT       // .
	TokDOTDOT    // ..
	TokCOLON     // :
	TokLBRACKET  // [
	TokRBRACKET  // ]
	TokCONCAT    // ||
	TokPLUS      // +
	TokMINUS     // -
	TokSTAR      // *
	TokSLASH     // /
	TokPERCENT   // %
	TokEQ        // =
	TokNEQ       // <> or !=
	TokLT        // <
	TokGT        // >
	TokLTE       // <=
	TokGTE       // >=
	TokAND       // AND
	TokOR_KW     // OR (as operator, not CREATE OR REPLACE)
	TokNOT_KW    // NOT (as operator)

	// Literals
	TokINTEGER_LIT  // 42
	TokNUMERIC_LIT  // 3.14
	TokSTRING_LIT   // 'hello'
	TokDOLLAR_STRING // $$body$$ or $tag$body$tag$
	TokBOOLEAN_LIT  // TRUE / FALSE
	TokNULL_LIT     // NULL

	// Other
	TokIDENT       // identifier
	TokSQL_FRAGMENT // captured SQL
	TokEOF
	TokNEWLINE
	TokCOMMENT
)

// tokenTypeNames maps token types to string names for debugging.
var tokenTypeNames = map[TokenType]string{
	TokDECLARE: "DECLARE", TokBEGIN: "BEGIN", TokEND: "END",
	TokIF: "IF", TokTHEN: "THEN", TokELSIF: "ELSIF", TokELSE: "ELSE",
	TokLOOP: "LOOP", TokWHILE: "WHILE", TokFOR: "FOR", TokIN: "IN",
	TokREVERSE: "REVERSE", TokRETURN: "RETURN", TokQUERY: "QUERY",
	TokNEXT: "NEXT", TokRAISE: "RAISE", TokPERFORM: "PERFORM",
	TokEXECUTE: "EXECUTE", TokINTO: "INTO", TokUSING: "USING",
	TokSTRICT: "STRICT", TokEXIT: "EXIT", TokWHEN: "WHEN",
	TokCONTINUE: "CONTINUE", TokNULL: "NULL", TokNOT: "NOT",
	TokDEFAULT: "DEFAULT", TokCONSTANT: "CONSTANT",
	TokEXCEPTION: "EXCEPTION", TokOTHERS: "OTHERS", TokFOUND: "FOUND",
	TokDEBUG: "DEBUG", TokLOG: "LOG", TokINFO: "INFO",
	TokNOTICE: "NOTICE", TokWARNING: "WARNING", TokCALL: "CALL",
	TokLANGUAGE: "LANGUAGE", TokRETURNS: "RETURNS", TokSETOF: "SETOF",
	TokAS: "AS", TokDO: "DO", TokCREATE: "CREATE",
	TokFUNCTION: "FUNCTION", TokPROCEDURE: "PROCEDURE",
	TokOR: "OR", TokREPLACE: "REPLACE",
	TokIMMUTABLE: "IMMUTABLE", TokSTABLE: "STABLE", TokVOLATILE: "VOLATILE",
	TokASSIGN: ":=", TokSEMICOLON: ";", TokLPAREN: "(", TokRPAREN: ")",
	TokCOMMA: ",", TokDOT: ".", TokDOTDOT: "..", TokCOLON: ":",
	TokLBRACKET: "[", TokRBRACKET: "]", TokCONCAT: "||",
	TokPLUS: "+", TokMINUS: "-", TokSTAR: "*", TokSLASH: "/",
	TokPERCENT: "%", TokEQ: "=", TokNEQ: "<>", TokLT: "<", TokGT: ">",
	TokLTE: "<=", TokGTE: ">=", TokAND: "AND", TokOR_KW: "OR_KW",
	TokNOT_KW: "NOT_KW",
	TokINTEGER_LIT: "INTEGER_LIT", TokNUMERIC_LIT: "NUMERIC_LIT",
	TokSTRING_LIT: "STRING_LIT", TokDOLLAR_STRING: "DOLLAR_STRING",
	TokBOOLEAN_LIT: "BOOLEAN_LIT", TokNULL_LIT: "NULL_LIT",
	TokIDENT: "IDENT", TokSQL_FRAGMENT: "SQL_FRAGMENT",
	TokEOF: "EOF", TokNEWLINE: "NEWLINE", TokCOMMENT: "COMMENT",
}

// String returns the human-readable name of a token type.
func (t TokenType) String() string {
	if name, ok := tokenTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("TokenType(%d)", int(t))
}

// Token represents a single lexical token produced by the lexer.
type Token struct {
	Type  TokenType
	Value string
	Pos   Position
}

// String returns a debug representation of the token.
func (t Token) String() string {
	return fmt.Sprintf("%s(%q)@%d:%d", t.Type.String(), t.Value, t.Pos.Line, t.Pos.Col)
}

// keywords maps uppercase keyword strings to their token types.
var keywords = map[string]TokenType{
	"DECLARE":   TokDECLARE,
	"BEGIN":     TokBEGIN,
	"END":       TokEND,
	"IF":        TokIF,
	"THEN":      TokTHEN,
	"ELSIF":     TokELSIF,
	"ELSEIF":    TokELSIF, // alias
	"ELSE":      TokELSE,
	"LOOP":      TokLOOP,
	"WHILE":     TokWHILE,
	"FOR":       TokFOR,
	"IN":        TokIN,
	"REVERSE":   TokREVERSE,
	"RETURN":    TokRETURN,
	"QUERY":     TokQUERY,
	"NEXT":      TokNEXT,
	"RAISE":     TokRAISE,
	"PERFORM":   TokPERFORM,
	"EXECUTE":   TokEXECUTE,
	"INTO":      TokINTO,
	"USING":     TokUSING,
	"STRICT":    TokSTRICT,
	"EXIT":      TokEXIT,
	"WHEN":      TokWHEN,
	"CONTINUE":  TokCONTINUE,
	"NULL":      TokNULL,
	"NOT":       TokNOT,
	"DEFAULT":   TokDEFAULT,
	"CONSTANT":  TokCONSTANT,
	"EXCEPTION": TokEXCEPTION,
	"OTHERS":    TokOTHERS,
	"FOUND":     TokFOUND,
	"DEBUG":     TokDEBUG,
	"LOG":       TokLOG,
	"INFO":      TokINFO,
	"NOTICE":    TokNOTICE,
	"WARNING":   TokWARNING,
	"CALL":      TokCALL,
	"LANGUAGE":  TokLANGUAGE,
	"RETURNS":   TokRETURNS,
	"SETOF":     TokSETOF,
	"AS":        TokAS,
	"DO":        TokDO,
	"CREATE":    TokCREATE,
	"FUNCTION":  TokFUNCTION,
	"PROCEDURE": TokPROCEDURE,
	"OR":        TokOR,
	"REPLACE":   TokREPLACE,
	"AND":       TokAND,
	"TRUE":      TokBOOLEAN_LIT,
	"FALSE":     TokBOOLEAN_LIT,
	"IMMUTABLE": TokIMMUTABLE,
	"STABLE":    TokSTABLE,
	"VOLATILE":  TokVOLATILE,
}

// Lexer tokenizes PL/pgSQL source text.
type Lexer struct {
	input  string
	pos    int
	line   int
	col    int
	tokens []Token
}

// NewLexer creates a new Lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Tokenize is the main entry point. It scans the entire input and returns all
// tokens. Comments and newlines are included in the token stream but can be
// filtered by the parser.
func Tokenize(input string) ([]Token, error) {
	l := NewLexer(input)
	if err := l.scan(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

// scan drives the lexer through the input, appending tokens until EOF.
func (l *Lexer) scan() error {
	for l.pos < len(l.input) {
		// Skip whitespace (but track newlines).
		if l.skipWhitespace() {
			continue
		}

		ch := l.input[l.pos]

		// Single-line comment: -- ...
		if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			l.scanLineComment()
			continue
		}

		// Multi-line comment: /* ... */
		if ch == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '*' {
			if err := l.scanBlockComment(); err != nil {
				return err
			}
			continue
		}

		// Dollar-quoted string: $$...$$ or $tag$...$tag$
		if ch == '$' {
			if tag, ok := l.peekDollarTag(); ok {
				if err := l.scanDollarString(tag); err != nil {
					return err
				}
				continue
			}
		}

		// String literal: 'text' or E'escape\ntext'
		if ch == '\'' || (ch == 'E' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'') {
			if err := l.scanStringLiteral(); err != nil {
				return err
			}
			continue
		}

		// Numeric literal.
		if ch >= '0' && ch <= '9' {
			l.scanNumber()
			continue
		}

		// Identifier or keyword.
		if isIdentStart(ch) {
			l.scanIdentOrKeyword()
			continue
		}

		// Operators and punctuation.
		if err := l.scanOperator(); err != nil {
			return err
		}
	}

	l.tokens = append(l.tokens, Token{Type: TokEOF, Value: "", Pos: Position{Line: l.line, Col: l.col}})
	return nil
}

// skipWhitespace advances past spaces, tabs, and newlines. Returns true if
// any whitespace was consumed.
func (l *Lexer) skipWhitespace() bool {
	consumed := false
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\n' {
			l.pos++
			l.line++
			l.col = 1
			consumed = true
		} else if ch == '\r' {
			l.pos++
			if l.pos < len(l.input) && l.input[l.pos] == '\n' {
				l.pos++
			}
			l.line++
			l.col = 1
			consumed = true
		} else if ch == ' ' || ch == '\t' {
			l.pos++
			l.col++
			consumed = true
		} else {
			break
		}
	}
	return consumed
}

// scanLineComment consumes -- through end of line.
func (l *Lexer) scanLineComment() {
	startPos := Position{Line: l.line, Col: l.col}
	start := l.pos
	for l.pos < len(l.input) && l.input[l.pos] != '\n' {
		l.pos++
		l.col++
	}
	l.tokens = append(l.tokens, Token{Type: TokCOMMENT, Value: l.input[start:l.pos], Pos: startPos})
}

// scanBlockComment consumes /* ... */ including nested comments.
func (l *Lexer) scanBlockComment() error {
	startPos := Position{Line: l.line, Col: l.col}
	start := l.pos
	l.pos += 2 // skip /*
	l.col += 2
	depth := 1

	for l.pos < len(l.input) && depth > 0 {
		if l.input[l.pos] == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '*' {
			depth++
			l.pos += 2
			l.col += 2
		} else if l.input[l.pos] == '*' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '/' {
			depth--
			l.pos += 2
			l.col += 2
		} else if l.input[l.pos] == '\n' {
			l.pos++
			l.line++
			l.col = 1
		} else {
			l.pos++
			l.col++
		}
	}

	if depth > 0 {
		return fmt.Errorf("unterminated block comment at line %d, col %d", startPos.Line, startPos.Col)
	}

	l.tokens = append(l.tokens, Token{Type: TokCOMMENT, Value: l.input[start:l.pos], Pos: startPos})
	return nil
}

// peekDollarTag checks if the current position starts a dollar-quote tag.
// Returns (tag, true) if found, where tag includes the $ delimiters.
func (l *Lexer) peekDollarTag() (string, bool) {
	if l.pos >= len(l.input) || l.input[l.pos] != '$' {
		return "", false
	}

	// $$ case.
	if l.pos+1 < len(l.input) && l.input[l.pos+1] == '$' {
		return "$$", true
	}

	// $tag$ case — tag must be an identifier.
	i := l.pos + 1
	if i >= len(l.input) || !isIdentStart(l.input[i]) {
		return "", false
	}
	for i < len(l.input) && isIdentContinue(l.input[i]) {
		i++
	}
	if i >= len(l.input) || l.input[i] != '$' {
		return "", false
	}
	tag := l.input[l.pos : i+1]
	return tag, true
}

// scanDollarString consumes a dollar-quoted string with the given tag.
func (l *Lexer) scanDollarString(tag string) error {
	startPos := Position{Line: l.line, Col: l.col}
	l.pos += len(tag)
	l.col += len(tag)

	bodyStart := l.pos
	for {
		if l.pos >= len(l.input) {
			return fmt.Errorf("unterminated dollar-quoted string at line %d, col %d", startPos.Line, startPos.Col)
		}
		if l.input[l.pos] == '$' && l.pos+len(tag) <= len(l.input) && l.input[l.pos:l.pos+len(tag)] == tag {
			body := l.input[bodyStart:l.pos]
			l.pos += len(tag)
			l.col += len(tag)
			l.tokens = append(l.tokens, Token{Type: TokDOLLAR_STRING, Value: body, Pos: startPos})
			return nil
		}
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

// scanStringLiteral consumes a single-quoted string literal, handling '' escapes
// and E'' escape syntax.
func (l *Lexer) scanStringLiteral() error {
	startPos := Position{Line: l.line, Col: l.col}

	// Handle E'...' prefix.
	if l.input[l.pos] == 'E' {
		l.pos++
		l.col++
	}

	l.pos++ // skip opening quote
	l.col++

	var buf strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			// Check for escaped quote ('').
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				buf.WriteByte('\'')
				l.pos += 2
				l.col += 2
				continue
			}
			// End of string.
			l.pos++
			l.col++
			l.tokens = append(l.tokens, Token{Type: TokSTRING_LIT, Value: buf.String(), Pos: startPos})
			return nil
		}
		if ch == '\\' && l.pos+1 < len(l.input) {
			// Backslash escapes (in E'' strings).
			next := l.input[l.pos+1]
			switch next {
			case 'n':
				buf.WriteByte('\n')
			case 't':
				buf.WriteByte('\t')
			case 'r':
				buf.WriteByte('\r')
			case '\\':
				buf.WriteByte('\\')
			case '\'':
				buf.WriteByte('\'')
			default:
				buf.WriteByte('\\')
				buf.WriteByte(next)
			}
			l.pos += 2
			l.col += 2
			continue
		}
		if ch == '\n' {
			buf.WriteByte(ch)
			l.pos++
			l.line++
			l.col = 1
			continue
		}
		buf.WriteByte(ch)
		l.pos++
		l.col++
	}

	return fmt.Errorf("unterminated string literal at line %d, col %d", startPos.Line, startPos.Col)
}

// scanNumber consumes an integer or numeric literal.
func (l *Lexer) scanNumber() {
	startPos := Position{Line: l.line, Col: l.col}
	start := l.pos
	isFloat := false

	for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
		l.pos++
		l.col++
	}

	// Check for decimal point.
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		// Make sure it's not '..' (range operator).
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '.' {
			// It's a '..' — don't consume the dot.
			l.tokens = append(l.tokens, Token{Type: TokINTEGER_LIT, Value: l.input[start:l.pos], Pos: startPos})
			return
		}
		isFloat = true
		l.pos++
		l.col++
		for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
			l.pos++
			l.col++
		}
	}

	// Check for scientific notation.
	if l.pos < len(l.input) && (l.input[l.pos] == 'e' || l.input[l.pos] == 'E') {
		isFloat = true
		l.pos++
		l.col++
		if l.pos < len(l.input) && (l.input[l.pos] == '+' || l.input[l.pos] == '-') {
			l.pos++
			l.col++
		}
		for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
			l.pos++
			l.col++
		}
	}

	tokType := TokINTEGER_LIT
	if isFloat {
		tokType = TokNUMERIC_LIT
	}
	l.tokens = append(l.tokens, Token{Type: tokType, Value: l.input[start:l.pos], Pos: startPos})
}

// scanIdentOrKeyword consumes an identifier and checks if it is a keyword.
func (l *Lexer) scanIdentOrKeyword() {
	startPos := Position{Line: l.line, Col: l.col}
	start := l.pos

	for l.pos < len(l.input) && isIdentContinue(l.input[l.pos]) {
		l.pos++
		l.col++
	}

	word := l.input[start:l.pos]
	upper := strings.ToUpper(word)

	if tokType, ok := keywords[upper]; ok {
		// Special handling: NULL can be a keyword or a literal depending on context.
		if upper == "NULL" {
			l.tokens = append(l.tokens, Token{Type: TokNULL, Value: upper, Pos: startPos})
		} else {
			l.tokens = append(l.tokens, Token{Type: tokType, Value: upper, Pos: startPos})
		}
	} else {
		l.tokens = append(l.tokens, Token{Type: TokIDENT, Value: word, Pos: startPos})
	}
}

// scanOperator consumes a single- or multi-character operator/punctuation.
func (l *Lexer) scanOperator() error {
	startPos := Position{Line: l.line, Col: l.col}
	ch := l.input[l.pos]

	// Two-character operators.
	if l.pos+1 < len(l.input) {
		two := l.input[l.pos : l.pos+2]
		switch two {
		case ":=":
			l.emit(TokASSIGN, two, startPos, 2)
			return nil
		case "..":
			l.emit(TokDOTDOT, two, startPos, 2)
			return nil
		case "||":
			l.emit(TokCONCAT, two, startPos, 2)
			return nil
		case "<>":
			l.emit(TokNEQ, two, startPos, 2)
			return nil
		case "!=":
			l.emit(TokNEQ, two, startPos, 2)
			return nil
		case "<=":
			l.emit(TokLTE, two, startPos, 2)
			return nil
		case ">=":
			l.emit(TokGTE, two, startPos, 2)
			return nil
		}
	}

	// Single-character operators.
	switch ch {
	case ';':
		l.emit(TokSEMICOLON, ";", startPos, 1)
	case '(':
		l.emit(TokLPAREN, "(", startPos, 1)
	case ')':
		l.emit(TokRPAREN, ")", startPos, 1)
	case ',':
		l.emit(TokCOMMA, ",", startPos, 1)
	case '.':
		l.emit(TokDOT, ".", startPos, 1)
	case ':':
		l.emit(TokCOLON, ":", startPos, 1)
	case '[':
		l.emit(TokLBRACKET, "[", startPos, 1)
	case ']':
		l.emit(TokRBRACKET, "]", startPos, 1)
	case '+':
		l.emit(TokPLUS, "+", startPos, 1)
	case '-':
		l.emit(TokMINUS, "-", startPos, 1)
	case '*':
		l.emit(TokSTAR, "*", startPos, 1)
	case '/':
		l.emit(TokSLASH, "/", startPos, 1)
	case '%':
		l.emit(TokPERCENT, "%", startPos, 1)
	case '=':
		l.emit(TokEQ, "=", startPos, 1)
	case '<':
		l.emit(TokLT, "<", startPos, 1)
	case '>':
		l.emit(TokGT, ">", startPos, 1)
	case '|':
		l.emit(TokCONCAT, "|", startPos, 1) // single pipe, unusual but handle gracefully
	default:
		return fmt.Errorf("unexpected character %q at line %d, col %d", string(ch), l.line, l.col)
	}
	return nil
}

// emit appends a token and advances the position.
func (l *Lexer) emit(typ TokenType, value string, pos Position, width int) {
	l.tokens = append(l.tokens, Token{Type: typ, Value: value, Pos: pos})
	l.pos += width
	l.col += width
}

// isIdentStart returns true if ch can start an identifier (letter or underscore).
func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

// isIdentContinue returns true if ch can continue an identifier.
func isIdentContinue(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9') || ch == '$'
}

// IsKeyword returns true if the given word (case-insensitive) is a PL/pgSQL keyword.
func IsKeyword(word string) bool {
	_, ok := keywords[strings.ToUpper(word)]
	return ok
}

// FilterTokens returns a new slice with tokens of the specified types removed.
// This is useful for stripping comments and newlines before parsing.
func FilterTokens(tokens []Token, remove ...TokenType) []Token {
	removeSet := make(map[TokenType]bool, len(remove))
	for _, t := range remove {
		removeSet[t] = true
	}
	var result []Token
	for _, tok := range tokens {
		if !removeSet[tok.Type] {
			result = append(result, tok)
		}
	}
	return result
}

