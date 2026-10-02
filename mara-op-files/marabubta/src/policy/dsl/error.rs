// Marabunta - Licensed under the MIT License.
//! DSL Error Types
//!
//! Provides detailed error reporting with line and column information
//! for policy DSL parsing and compilation errors.

use std::fmt;

/// Position in source code
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourcePosition {
    /// Line number (1-indexed)
    pub line: usize,
    /// Column number (1-indexed)
    pub column: usize,
    /// Byte offset from start of source
    pub offset: usize,
}

impl SourcePosition {
    /// Create a new source position
    pub fn new(line: usize, column: usize, offset: usize) -> Self {
        Self {
            line,
            column,
            offset,
        }
    }

    /// Start of file position
    pub fn start() -> Self {
        Self {
            line: 1,
            column: 1,
            offset: 0,
        }
    }
}

impl fmt::Display for SourcePosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

/// A span of source code
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Start position
    pub start: SourcePosition,
    /// End position
    pub end: SourcePosition,
}

impl Span {
    /// Create a new span
    pub fn new(start: SourcePosition, end: SourcePosition) -> Self {
        Self { start, end }
    }

    /// Create a span from a single position (zero-width)
    pub fn point(pos: SourcePosition) -> Self {
        Self {
            start: pos,
            end: pos,
        }
    }

    /// Merge two spans into one that covers both
    pub fn merge(self, other: Span) -> Self {
        Self {
            start: if self.start.offset < other.start.offset {
                self.start
            } else {
                other.start
            },
            end: if self.end.offset > other.end.offset {
                self.end
            } else {
                other.end
            },
        }
    }
}

impl fmt::Display for Span {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start.line == self.end.line {
            write!(
                f,
                "line {}, columns {}-{}",
                self.start.line, self.start.column, self.end.column
            )
        } else {
            write!(
                f,
                "{}:{} to {}:{}",
                self.start.line, self.start.column, self.end.line, self.end.column
            )
        }
    }
}

/// DSL error kinds
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DslErrorKind {
    // Lexer errors
    /// Unexpected character in input
    UnexpectedCharacter(char),
    /// Unterminated string literal
    UnterminatedString,
    /// Invalid escape sequence
    InvalidEscapeSequence(String),
    /// Invalid number format
    InvalidNumber(String),

    // Parser errors
    /// Unexpected token
    UnexpectedToken { expected: String, found: String },
    /// Unexpected end of input
    UnexpectedEof { expected: String },
    /// Invalid syntax
    InvalidSyntax(String),
    /// Missing required component
    MissingRequired(String),

    // Semantic errors
    /// Undefined variable
    UndefinedVariable(String),
    /// Type mismatch
    TypeMismatch { expected: String, found: String },
    /// Invalid operator for types
    InvalidOperator {
        operator: String,
        left_type: String,
        right_type: String,
    },
    /// Duplicate definition
    DuplicateDefinition(String),
    /// Invalid property access
    InvalidProperty {
        object_type: String,
        property: String,
    },

    // Compilation errors
    /// Unsupported feature
    UnsupportedFeature(String),
    /// Invalid policy structure
    InvalidPolicyStructure(String),
}

impl fmt::Display for DslErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DslErrorKind::UnexpectedCharacter(c) => write!(f, "unexpected character '{}'", c),
            DslErrorKind::UnterminatedString => write!(f, "unterminated string literal"),
            DslErrorKind::InvalidEscapeSequence(s) => {
                write!(f, "invalid escape sequence '\\{}'", s)
            }
            DslErrorKind::InvalidNumber(s) => write!(f, "invalid number '{}'", s),
            DslErrorKind::UnexpectedToken { expected, found } => {
                write!(f, "expected {}, found {}", expected, found)
            }
            DslErrorKind::UnexpectedEof { expected } => {
                write!(f, "unexpected end of input, expected {}", expected)
            }
            DslErrorKind::InvalidSyntax(msg) => write!(f, "invalid syntax: {}", msg),
            DslErrorKind::MissingRequired(what) => write!(f, "missing required {}", what),
            DslErrorKind::UndefinedVariable(name) => write!(f, "undefined variable '{}'", name),
            DslErrorKind::TypeMismatch { expected, found } => {
                write!(f, "type mismatch: expected {}, found {}", expected, found)
            }
            DslErrorKind::InvalidOperator {
                operator,
                left_type,
                right_type,
            } => write!(
                f,
                "operator '{}' cannot be applied to {} and {}",
                operator, left_type, right_type
            ),
            DslErrorKind::DuplicateDefinition(name) => {
                write!(f, "duplicate definition of '{}'", name)
            }
            DslErrorKind::InvalidProperty {
                object_type,
                property,
            } => {
                write!(f, "'{}' has no property '{}'", object_type, property)
            }
            DslErrorKind::UnsupportedFeature(feature) => {
                write!(f, "unsupported feature: {}", feature)
            }
            DslErrorKind::InvalidPolicyStructure(msg) => {
                write!(f, "invalid policy structure: {}", msg)
            }
        }
    }
}

/// A DSL error with location information
#[derive(Debug, Clone)]
pub struct DslError {
    /// The error kind
    pub kind: DslErrorKind,
    /// Location in source
    pub span: Option<Span>,
    /// The source line (for error display)
    pub source_line: Option<String>,
    /// Additional context/hint
    pub hint: Option<String>,
}

impl DslError {
    /// Create a new DSL error
    pub fn new(kind: DslErrorKind) -> Self {
        Self {
            kind,
            span: None,
            source_line: None,
            hint: None,
        }
    }

    /// Add span information
    pub fn with_span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }

    /// Add source line
    pub fn with_source_line(mut self, line: String) -> Self {
        self.source_line = Some(line);
        self
    }

    /// Add hint
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Format error with source context
    pub fn format_with_source(&self, source: &str) -> String {
        let mut output = String::new();

        // Error message
        output.push_str(&format!("error: {}\n", self.kind));

        // Location
        if let Some(span) = &self.span {
            output.push_str(&format!("  --> {}\n", span.start));

            // Source context
            let lines: Vec<&str> = source.lines().collect();
            if span.start.line > 0 && span.start.line <= lines.len() {
                let line_num = span.start.line;
                let line = lines[line_num - 1];

                // Line number gutter
                let gutter_width = format!("{}", line_num).len();
                output.push_str(&format!("{:>width$} |\n", "", width = gutter_width));
                output.push_str(&format!(
                    "{:>width$} | {}\n",
                    line_num,
                    line,
                    width = gutter_width
                ));

                // Error indicator
                let col_start = span.start.column.saturating_sub(1);
                let col_end = if span.start.line == span.end.line {
                    span.end.column.saturating_sub(1).max(col_start + 1)
                } else {
                    line.len()
                };

                let indicator_len = col_end.saturating_sub(col_start).max(1);
                output.push_str(&format!(
                    "{:>width$} | {:>indent$}{}\n",
                    "",
                    "",
                    "^".repeat(indicator_len),
                    width = gutter_width,
                    indent = col_start
                ));
            }
        }

        // Hint
        if let Some(hint) = &self.hint {
            output.push_str(&format!("  = hint: {}\n", hint));
        }

        output
    }
}

impl fmt::Display for DslError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(span) = &self.span {
            write!(f, " at {}", span)?;
        }
        Ok(())
    }
}

impl std::error::Error for DslError {}

/// Result type for DSL operations
pub type DslResult<T> = Result<T, DslError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_position_display() {
        let pos = SourcePosition::new(5, 10, 100);
        assert_eq!(format!("{}", pos), "line 5, column 10");
    }

    #[test]
    fn test_span_display_same_line() {
        let span = Span::new(
            SourcePosition::new(5, 10, 100),
            SourcePosition::new(5, 15, 105),
        );
        assert_eq!(format!("{}", span), "line 5, columns 10-15");
    }

    #[test]
    fn test_span_display_multi_line() {
        let span = Span::new(
            SourcePosition::new(5, 10, 100),
            SourcePosition::new(7, 5, 150),
        );
        assert_eq!(format!("{}", span), "5:10 to 7:5");
    }

    #[test]
    fn test_error_format_with_source() {
        let error = DslError::new(DslErrorKind::UnexpectedToken {
            expected: "'=>'".to_string(),
            found: "'AND'".to_string(),
        })
        .with_span(Span::new(
            SourcePosition::new(2, 5, 25),
            SourcePosition::new(2, 8, 28),
        ))
        .with_hint("use '=>' to separate condition from effect");

        let source = "# Example policy\njob.priority > 5 AND node.region = \"us-east\"";
        let formatted = error.format_with_source(source);

        assert!(formatted.contains("error:"));
        assert!(formatted.contains("expected"));
        assert!(formatted.contains("hint:"));
    }

    #[test]
    fn test_span_merge() {
        let span1 = Span::new(SourcePosition::new(1, 5, 5), SourcePosition::new(1, 10, 10));
        let span2 = Span::new(
            SourcePosition::new(1, 15, 15),
            SourcePosition::new(1, 20, 20),
        );

        let merged = span1.merge(span2);
        assert_eq!(merged.start.column, 5);
        assert_eq!(merged.end.column, 20);
    }
}
