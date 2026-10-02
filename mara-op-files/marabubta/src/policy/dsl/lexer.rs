// Marabunta - Licensed under the MIT License.
//! DSL Lexer (Tokenizer)
//!
//! Converts policy DSL source text into a stream of tokens for parsing.

use super::error::{DslError, DslErrorKind, DslResult, SourcePosition, Span};

/// Token types for the policy DSL
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Literals
    /// String literal "..."
    String(String),
    /// Integer literal
    Integer(i64),
    /// Float literal
    Float(f64),
    /// Boolean literal (true/false)
    Boolean(bool),

    // Identifiers and keywords
    /// Identifier (variable, property name)
    Identifier(String),

    // Keywords
    /// "AND" keyword
    And,
    /// "OR" keyword
    Or,
    /// "NOT" keyword
    Not,
    /// "true" keyword
    True,
    /// "false" keyword
    False,
    /// "prefer" keyword
    Prefer,
    /// "require" keyword
    Require,
    /// "exclude" keyword
    Exclude,
    /// "affinity" keyword
    Affinity,
    /// "anti_affinity" keyword
    AntiAffinity,
    /// "with" keyword
    With,
    /// "at" keyword
    At,
    /// "weight" keyword
    Weight,
    /// "policy" keyword
    Policy,
    /// "when" keyword
    When,
    /// "then" keyword
    Then,
    /// "in" keyword
    In,
    /// "matches" keyword
    Matches,
    /// "contains" keyword
    Contains,
    /// "has" keyword
    Has,
    /// "all" keyword
    All,
    /// "any" keyword
    Any,
    /// "none" keyword
    None,
    /// "between" keyword
    Between,
    /// "scope" keyword
    Scope,

    // Object prefixes (semantic keywords)
    /// "job" keyword
    Job,
    /// "node" keyword
    Node,
    /// "submitter" keyword
    Submitter,
    /// "resource" keyword
    Resource,
    /// "time" keyword
    Time,
    /// "tag" keyword
    Tag,
    /// "group" keyword
    Group,

    // Operators
    /// "=>" (implication, condition => effect)
    Arrow,
    /// "=" (equals)
    Equals,
    /// "!=" (not equals)
    NotEquals,
    /// ">" (greater than)
    GreaterThan,
    /// ">=" (greater than or equal)
    GreaterThanOrEqual,
    /// "<" (less than)
    LessThan,
    /// "<=" (less than or equal)
    LessThanOrEqual,
    /// "~" (regex match)
    Tilde,
    /// "+" (plus)
    Plus,
    /// "-" (minus)
    Minus,
    /// "*" (multiply)
    Star,
    /// "/" (divide)
    Slash,
    /// "%" (modulo)
    Percent,

    // Delimiters
    /// "(" left parenthesis
    LeftParen,
    /// ")" right parenthesis
    RightParen,
    /// "{" left brace
    LeftBrace,
    /// "}" right brace
    RightBrace,
    /// "[" left bracket
    LeftBracket,
    /// "]" right bracket
    RightBracket,
    /// "." dot
    Dot,
    /// "," comma
    Comma,
    /// ":" colon
    Colon,
    /// ";" semicolon
    Semicolon,

    // Special
    /// End of file
    Eof,
    /// Newline (for line-aware parsing)
    Newline,
    /// Comment (for preserving comments)
    Comment(String),
}

impl TokenKind {
    /// Get a human-readable name for this token kind
    pub fn name(&self) -> &'static str {
        match self {
            TokenKind::String(_) => "string",
            TokenKind::Integer(_) => "integer",
            TokenKind::Float(_) => "float",
            TokenKind::Boolean(_) => "boolean",
            TokenKind::Identifier(_) => "identifier",
            TokenKind::And => "'AND'",
            TokenKind::Or => "'OR'",
            TokenKind::Not => "'NOT'",
            TokenKind::True => "'true'",
            TokenKind::False => "'false'",
            TokenKind::Prefer => "'prefer'",
            TokenKind::Require => "'require'",
            TokenKind::Exclude => "'exclude'",
            TokenKind::Affinity => "'affinity'",
            TokenKind::AntiAffinity => "'anti_affinity'",
            TokenKind::With => "'with'",
            TokenKind::At => "'at'",
            TokenKind::Weight => "'weight'",
            TokenKind::Policy => "'policy'",
            TokenKind::When => "'when'",
            TokenKind::Then => "'then'",
            TokenKind::In => "'in'",
            TokenKind::Matches => "'matches'",
            TokenKind::Contains => "'contains'",
            TokenKind::Has => "'has'",
            TokenKind::All => "'all'",
            TokenKind::Any => "'any'",
            TokenKind::None => "'none'",
            TokenKind::Between => "'between'",
            TokenKind::Scope => "'scope'",
            TokenKind::Job => "'job'",
            TokenKind::Node => "'node'",
            TokenKind::Submitter => "'submitter'",
            TokenKind::Resource => "'resource'",
            TokenKind::Time => "'time'",
            TokenKind::Tag => "'tag'",
            TokenKind::Group => "'group'",
            TokenKind::Arrow => "'=>'",
            TokenKind::Equals => "'='",
            TokenKind::NotEquals => "'!='",
            TokenKind::GreaterThan => "'>'",
            TokenKind::GreaterThanOrEqual => "'>='",
            TokenKind::LessThan => "'<'",
            TokenKind::LessThanOrEqual => "'<='",
            TokenKind::Tilde => "'~'",
            TokenKind::Plus => "'+'",
            TokenKind::Minus => "'-'",
            TokenKind::Star => "'*'",
            TokenKind::Slash => "'/'",
            TokenKind::Percent => "'%'",
            TokenKind::LeftParen => "'('",
            TokenKind::RightParen => "')'",
            TokenKind::LeftBrace => "'{'",
            TokenKind::RightBrace => "'}'",
            TokenKind::LeftBracket => "'['",
            TokenKind::RightBracket => "']'",
            TokenKind::Dot => "'.'",
            TokenKind::Comma => "','",
            TokenKind::Colon => "':'",
            TokenKind::Semicolon => "';'",
            TokenKind::Eof => "end of file",
            TokenKind::Newline => "newline",
            TokenKind::Comment(_) => "comment",
        }
    }
}

/// A token with its location in source
#[derive(Debug, Clone)]
pub struct Token {
    /// The token kind
    pub kind: TokenKind,
    /// Span in source
    pub span: Span,
    /// The raw text that produced this token
    pub lexeme: String,
}

impl Token {
    /// Create a new token
    pub fn new(kind: TokenKind, span: Span, lexeme: String) -> Self {
        Self { kind, span, lexeme }
    }

    /// Check if this is an EOF token
    pub fn is_eof(&self) -> bool {
        matches!(self.kind, TokenKind::Eof)
    }
}

/// The lexer for the policy DSL
pub struct Lexer<'a> {
    /// Source text
    source: &'a str,
    /// Current position in source
    position: usize,
    /// Current line number (1-indexed)
    line: usize,
    /// Current column number (1-indexed)
    column: usize,
    /// Iterator over characters
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
    /// Collected tokens
    tokens: Vec<Token>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given source
    pub fn new(source: &'a str) -> Self {
        Self {
            source,
            position: 0,
            line: 1,
            column: 1,
            chars: source.char_indices().peekable(),
            tokens: Vec::new(),
        }
    }

    /// Tokenize the entire source
    pub fn tokenize(mut self) -> DslResult<Vec<Token>> {
        while !self.is_at_end() {
            self.skip_whitespace();
            if self.is_at_end() {
                break;
            }
            self.scan_token()?;
        }

        // Add EOF token
        let eof_pos = SourcePosition::new(self.line, self.column, self.position);
        self.tokens.push(Token::new(
            TokenKind::Eof,
            Span::point(eof_pos),
            String::new(),
        ));

        Ok(self.tokens)
    }

    fn is_at_end(&mut self) -> bool {
        self.chars.peek().is_none()
    }

    fn current_position(&self) -> SourcePosition {
        SourcePosition::new(self.line, self.column, self.position)
    }

    fn advance(&mut self) -> Option<char> {
        if let Some((pos, ch)) = self.chars.next() {
            self.position = pos + ch.len_utf8();
            if ch == '\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
            Some(ch)
        } else {
            None
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.chars.peek().map(|(_, ch)| *ch)
    }

    fn peek_next(&self) -> Option<char> {
        let mut iter = self.chars.clone();
        iter.next();
        iter.next().map(|(_, ch)| ch)
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            match ch {
                ' ' | '\t' | '\r' => {
                    self.advance();
                }
                '\n' => {
                    // Optionally track newlines for line-sensitive parsing
                    self.advance();
                }
                '#' => {
                    // Comment - skip to end of line
                    self.skip_line_comment();
                }
                _ => break,
            }
        }
    }

    fn skip_line_comment(&mut self) {
        let start = self.current_position();
        let mut comment = String::new();

        self.advance(); // consume '#'
        while let Some(ch) = self.peek() {
            if ch == '\n' {
                break;
            }
            comment.push(ch);
            self.advance();
        }

        // Optionally preserve comments
        let end = self.current_position();
        self.tokens.push(Token::new(
            TokenKind::Comment(comment.trim().to_string()),
            Span::new(start, end),
            format!("#{}", comment),
        ));
    }

    fn scan_token(&mut self) -> DslResult<()> {
        let start = self.current_position();
        let ch = self.advance().unwrap();

        let kind = match ch {
            // Single character tokens
            '(' => TokenKind::LeftParen,
            ')' => TokenKind::RightParen,
            '{' => TokenKind::LeftBrace,
            '}' => TokenKind::RightBrace,
            '[' => TokenKind::LeftBracket,
            ']' => TokenKind::RightBracket,
            '.' => TokenKind::Dot,
            ',' => TokenKind::Comma,
            ':' => TokenKind::Colon,
            ';' => TokenKind::Semicolon,
            '+' => TokenKind::Plus,
            '-' => TokenKind::Minus,
            '*' => TokenKind::Star,
            '/' => TokenKind::Slash,
            '%' => TokenKind::Percent,
            '~' => TokenKind::Tilde,

            // Two character tokens
            '=' => {
                if self.peek() == Some('>') {
                    self.advance();
                    TokenKind::Arrow
                } else {
                    TokenKind::Equals
                }
            }
            '!' => {
                if self.peek() == Some('=') {
                    self.advance();
                    TokenKind::NotEquals
                } else {
                    return Err(DslError::new(DslErrorKind::UnexpectedCharacter('!'))
                        .with_span(Span::point(start))
                        .with_hint("did you mean '!=' (not equals)?"));
                }
            }
            '>' => {
                if self.peek() == Some('=') {
                    self.advance();
                    TokenKind::GreaterThanOrEqual
                } else {
                    TokenKind::GreaterThan
                }
            }
            '<' => {
                if self.peek() == Some('=') {
                    self.advance();
                    TokenKind::LessThanOrEqual
                } else {
                    TokenKind::LessThan
                }
            }

            // String literals
            '"' => return self.scan_string(start),

            // Numbers
            '0'..='9' => return self.scan_number(start, ch),

            // Identifiers and keywords
            'a'..='z' | 'A'..='Z' | '_' => return self.scan_identifier(start, ch),

            _ => {
                return Err(DslError::new(DslErrorKind::UnexpectedCharacter(ch))
                    .with_span(Span::point(start)));
            }
        };

        let end = self.current_position();
        let lexeme = self.source[start.offset..end.offset].to_string();
        self.tokens
            .push(Token::new(kind, Span::new(start, end), lexeme));

        Ok(())
    }

    fn scan_string(&mut self, start: SourcePosition) -> DslResult<()> {
        let mut value = String::new();

        loop {
            match self.peek() {
                None | Some('\n') => {
                    return Err(DslError::new(DslErrorKind::UnterminatedString)
                        .with_span(Span::new(start, self.current_position()))
                        .with_hint("strings must be terminated with a closing '\"'"));
                }
                Some('"') => {
                    self.advance();
                    break;
                }
                Some('\\') => {
                    self.advance();
                    match self.peek() {
                        Some('n') => {
                            self.advance();
                            value.push('\n');
                        }
                        Some('t') => {
                            self.advance();
                            value.push('\t');
                        }
                        Some('r') => {
                            self.advance();
                            value.push('\r');
                        }
                        Some('\\') => {
                            self.advance();
                            value.push('\\');
                        }
                        Some('"') => {
                            self.advance();
                            value.push('"');
                        }
                        Some(ch) => {
                            return Err(DslError::new(DslErrorKind::InvalidEscapeSequence(
                                ch.to_string(),
                            ))
                            .with_span(Span::point(self.current_position()))
                            .with_hint("valid escape sequences are: \\n, \\t, \\r, \\\\, \\\""));
                        }
                        None => {
                            return Err(DslError::new(DslErrorKind::UnterminatedString)
                                .with_span(Span::new(start, self.current_position())));
                        }
                    }
                }
                Some(ch) => {
                    self.advance();
                    value.push(ch);
                }
            }
        }

        let end = self.current_position();
        let lexeme = self.source[start.offset..end.offset].to_string();
        self.tokens.push(Token::new(
            TokenKind::String(value),
            Span::new(start, end),
            lexeme,
        ));

        Ok(())
    }

    fn scan_number(&mut self, start: SourcePosition, first_digit: char) -> DslResult<()> {
        let mut num_str = String::new();
        num_str.push(first_digit);

        // Collect integer part
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                num_str.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        // Check for decimal point
        let mut is_float = false;
        if self.peek() == Some('.') && self.peek_next().is_some_and(|c| c.is_ascii_digit()) {
            is_float = true;
            num_str.push('.');
            self.advance();

            while let Some(ch) = self.peek() {
                if ch.is_ascii_digit() {
                    num_str.push(ch);
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let end = self.current_position();
        let lexeme = self.source[start.offset..end.offset].to_string();

        let kind = if is_float {
            let value: f64 = num_str.parse().map_err(|_| {
                DslError::new(DslErrorKind::InvalidNumber(num_str.clone()))
                    .with_span(Span::new(start, end))
            })?;
            TokenKind::Float(value)
        } else {
            let value: i64 = num_str.parse().map_err(|_| {
                DslError::new(DslErrorKind::InvalidNumber(num_str.clone()))
                    .with_span(Span::new(start, end))
            })?;
            TokenKind::Integer(value)
        };

        self.tokens
            .push(Token::new(kind, Span::new(start, end), lexeme));
        Ok(())
    }

    fn scan_identifier(&mut self, start: SourcePosition, first_char: char) -> DslResult<()> {
        let mut ident = String::new();
        ident.push(first_char);

        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                ident.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        let end = self.current_position();
        let lexeme = self.source[start.offset..end.offset].to_string();

        // Check for keywords
        let kind = match ident.as_str() {
            // Logical operators
            "AND" | "and" => TokenKind::And,
            "OR" | "or" => TokenKind::Or,
            "NOT" | "not" => TokenKind::Not,

            // Boolean literals
            "true" => TokenKind::True,
            "false" => TokenKind::False,

            // Effect keywords
            "prefer" => TokenKind::Prefer,
            "require" => TokenKind::Require,
            "exclude" => TokenKind::Exclude,
            "affinity" => TokenKind::Affinity,
            "anti_affinity" => TokenKind::AntiAffinity,

            // Other keywords
            "with" => TokenKind::With,
            "at" => TokenKind::At,
            "weight" => TokenKind::Weight,
            "policy" => TokenKind::Policy,
            "when" => TokenKind::When,
            "then" => TokenKind::Then,
            "in" => TokenKind::In,
            "matches" => TokenKind::Matches,
            "contains" => TokenKind::Contains,
            "has" => TokenKind::Has,
            "all" => TokenKind::All,
            "any" => TokenKind::Any,
            "none" => TokenKind::None,
            "between" => TokenKind::Between,
            "scope" => TokenKind::Scope,

            // Object types
            "job" => TokenKind::Job,
            "node" => TokenKind::Node,
            "submitter" => TokenKind::Submitter,
            "resource" => TokenKind::Resource,
            "time" => TokenKind::Time,
            "tag" => TokenKind::Tag,
            "group" => TokenKind::Group,

            // Regular identifier
            _ => TokenKind::Identifier(ident),
        };

        self.tokens
            .push(Token::new(kind, Span::new(start, end), lexeme));
        Ok(())
    }
}

/// Tokenize source code
pub fn tokenize(source: &str) -> DslResult<Vec<Token>> {
    Lexer::new(source).tokenize()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenize_kinds(source: &str) -> Vec<TokenKind> {
        tokenize(source)
            .unwrap()
            .into_iter()
            .filter(|t| !matches!(t.kind, TokenKind::Comment(_)))
            .map(|t| t.kind)
            .collect()
    }

    #[test]
    fn test_simple_tokens() {
        let tokens = tokenize_kinds("( ) { } [ ] . , : ;");
        assert_eq!(
            tokens,
            vec![
                TokenKind::LeftParen,
                TokenKind::RightParen,
                TokenKind::LeftBrace,
                TokenKind::RightBrace,
                TokenKind::LeftBracket,
                TokenKind::RightBracket,
                TokenKind::Dot,
                TokenKind::Comma,
                TokenKind::Colon,
                TokenKind::Semicolon,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_operators() {
        let tokens = tokenize_kinds("= != > >= < <= => ~ + - * / %");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Equals,
                TokenKind::NotEquals,
                TokenKind::GreaterThan,
                TokenKind::GreaterThanOrEqual,
                TokenKind::LessThan,
                TokenKind::LessThanOrEqual,
                TokenKind::Arrow,
                TokenKind::Tilde,
                TokenKind::Plus,
                TokenKind::Minus,
                TokenKind::Star,
                TokenKind::Slash,
                TokenKind::Percent,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_string_literal() {
        let tokens = tokenize_kinds(r#""hello world""#);
        assert_eq!(
            tokens,
            vec![TokenKind::String("hello world".to_string()), TokenKind::Eof]
        );
    }

    #[test]
    fn test_string_escapes() {
        let tokens = tokenize_kinds(r#""hello\nworld\ttab\\slash""#);
        assert_eq!(
            tokens,
            vec![
                TokenKind::String("hello\nworld\ttab\\slash".to_string()),
                TokenKind::Eof
            ]
        );
    }

    #[test]
    fn test_numbers() {
        let tokens = tokenize_kinds("42 3.14 0 100");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Integer(42),
                TokenKind::Float(3.14),
                TokenKind::Integer(0),
                TokenKind::Integer(100),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_keywords() {
        let tokens = tokenize_kinds("AND OR NOT true false prefer require exclude");
        assert_eq!(
            tokens,
            vec![
                TokenKind::And,
                TokenKind::Or,
                TokenKind::Not,
                TokenKind::True,
                TokenKind::False,
                TokenKind::Prefer,
                TokenKind::Require,
                TokenKind::Exclude,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_object_keywords() {
        let tokens = tokenize_kinds("job node submitter resource time tag group");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Job,
                TokenKind::Node,
                TokenKind::Submitter,
                TokenKind::Resource,
                TokenKind::Time,
                TokenKind::Tag,
                TokenKind::Group,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_identifiers() {
        let tokens = tokenize_kinds("my_var someVar123 _private");
        assert_eq!(
            tokens,
            vec![
                TokenKind::Identifier("my_var".to_string()),
                TokenKind::Identifier("someVar123".to_string()),
                TokenKind::Identifier("_private".to_string()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_full_policy_expression() {
        let source = r#"job.priority > 5 AND node.region = "us-east" => prefer node.gpu = true"#;
        let tokens = tokenize_kinds(source);

        assert_eq!(
            tokens,
            vec![
                TokenKind::Job,
                TokenKind::Dot,
                TokenKind::Identifier("priority".to_string()),
                TokenKind::GreaterThan,
                TokenKind::Integer(5),
                TokenKind::And,
                TokenKind::Node,
                TokenKind::Dot,
                TokenKind::Identifier("region".to_string()),
                TokenKind::Equals,
                TokenKind::String("us-east".to_string()),
                TokenKind::Arrow,
                TokenKind::Prefer,
                TokenKind::Node,
                TokenKind::Dot,
                TokenKind::Identifier("gpu".to_string()),
                TokenKind::Equals,
                TokenKind::True,
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn test_comments() {
        let tokens = tokenize("# this is a comment\njob.priority > 5").unwrap();
        // Should have comment, then expression tokens
        assert!(matches!(tokens[0].kind, TokenKind::Comment(_)));
        assert!(matches!(tokens[1].kind, TokenKind::Job));
    }

    #[test]
    fn test_unterminated_string_error() {
        let result = tokenize(r#""unterminated"#);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind, DslErrorKind::UnterminatedString));
    }

    #[test]
    fn test_unexpected_character_error() {
        let result = tokenize("@invalid");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err.kind, DslErrorKind::UnexpectedCharacter('@')));
    }

    #[test]
    fn test_token_positions() {
        let tokens = tokenize("job.priority").unwrap();
        // "job" at position 0
        assert_eq!(tokens[0].span.start.column, 1);
        assert_eq!(tokens[0].span.start.offset, 0);
        // "." at position 3
        assert_eq!(tokens[1].span.start.column, 4);
        assert_eq!(tokens[1].span.start.offset, 3);
    }
}
