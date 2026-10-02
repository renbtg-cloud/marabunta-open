// Marabunta - Licensed under the MIT License.
//! Table formatting for CLI list outputs
//!
//! Provides flexible table rendering for displaying jobs, tasks, workers, and nodes
//! with support for colors, alignment, and truncation.
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::cli::table::{Table, Column, Alignment};
//!
//! let mut table = Table::new(vec![
//!     Column::new("ID").width(12),
//!     Column::new("Status").width(10),
//!     Column::new("Progress").width(8).align(Alignment::Right),
//! ]);
//!
//! table.add_row(vec!["job-123".into(), "Running".into(), "45%".into()]);
//! table.print();
//! ```

use console::style;
use std::cmp::max;
use std::io::{self, Write};

// ─────────────────────────────────────────────────────────────────────────────
// ALIGNMENT
// ─────────────────────────────────────────────────────────────────────────────

/// Text alignment within a column
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Alignment {
    /// Left-aligned (default)
    #[default]
    Left,
    /// Right-aligned
    Right,
    /// Centered
    Center,
}

// ─────────────────────────────────────────────────────────────────────────────
// COLUMN
// ─────────────────────────────────────────────────────────────────────────────

/// Column definition for a table
#[derive(Debug, Clone)]
pub struct Column {
    /// Column header
    pub header: String,
    /// Minimum width (0 = auto)
    pub min_width: usize,
    /// Maximum width (0 = unlimited)
    pub max_width: usize,
    /// Text alignment
    pub alignment: Alignment,
    /// Whether to hide this column
    pub hidden: bool,
    /// Whether to truncate long values
    pub truncate: bool,
}

impl Column {
    /// Create a new column with the given header
    pub fn new(header: impl Into<String>) -> Self {
        Self {
            header: header.into(),
            min_width: 0,
            max_width: 0,
            alignment: Alignment::Left,
            hidden: false,
            truncate: true,
        }
    }

    /// Set minimum/fixed width
    pub fn width(mut self, width: usize) -> Self {
        self.min_width = width;
        self
    }

    /// Set maximum width
    pub fn max_width(mut self, max_width: usize) -> Self {
        self.max_width = max_width;
        self
    }

    /// Set alignment
    pub fn align(mut self, alignment: Alignment) -> Self {
        self.alignment = alignment;
        self
    }

    /// Hide this column
    pub fn hidden(mut self, hidden: bool) -> Self {
        self.hidden = hidden;
        self
    }

    /// Set truncation behavior
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TABLE STYLE
// ─────────────────────────────────────────────────────────────────────────────

/// Table display style
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableStyle {
    /// Simple ASCII borders
    #[default]
    Simple,
    /// Unicode box-drawing characters
    Unicode,
    /// No borders, just spacing
    Minimal,
    /// Markdown-compatible
    Markdown,
    /// Compact with no header separator
    Compact,
}

// ─────────────────────────────────────────────────────────────────────────────
// TABLE
// ─────────────────────────────────────────────────────────────────────────────

/// A formatted table for CLI output
#[derive(Debug, Clone)]
pub struct Table {
    /// Column definitions
    columns: Vec<Column>,
    /// Row data
    rows: Vec<Vec<String>>,
    /// Table style
    style: TableStyle,
    /// Whether to use colors
    use_colors: bool,
    /// Column separator
    separator: String,
    /// Indentation
    indent: usize,
    /// Show header row
    show_header: bool,
    /// Show row numbers
    show_row_numbers: bool,
}

impl Table {
    /// Create a new table with the given columns
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            style: TableStyle::Simple,
            use_colors: true,
            separator: "  ".to_string(),
            indent: 2,
            show_header: true,
            show_row_numbers: false,
        }
    }

    /// Set table style
    pub fn style(mut self, style: TableStyle) -> Self {
        self.style = style;
        self
    }

    /// Enable/disable colors
    pub fn colors(mut self, use_colors: bool) -> Self {
        self.use_colors = use_colors;
        self
    }

    /// Set column separator
    pub fn separator(mut self, separator: impl Into<String>) -> Self {
        self.separator = separator.into();
        self
    }

    /// Set indentation
    pub fn indent(mut self, indent: usize) -> Self {
        self.indent = indent;
        self
    }

    /// Show/hide header
    pub fn header(mut self, show: bool) -> Self {
        self.show_header = show;
        self
    }

    /// Show/hide row numbers
    pub fn row_numbers(mut self, show: bool) -> Self {
        self.show_row_numbers = show;
        self
    }

    /// Add a row of data
    pub fn add_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    /// Add multiple rows
    pub fn add_rows(&mut self, rows: Vec<Vec<String>>) {
        self.rows.extend(rows);
    }

    /// Clear all rows
    pub fn clear(&mut self) {
        self.rows.clear();
    }

    /// Get number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if table is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Calculate actual column widths
    fn calculate_widths(&self) -> Vec<usize> {
        let mut widths: Vec<usize> = self.columns.iter()
            .map(|c| if c.hidden { 0 } else { c.header.len() })
            .collect();

        // Find max width for each column based on data
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() && !self.columns[i].hidden {
                    // Strip ANSI codes for width calculation
                    let cell_width = strip_ansi_codes(cell).len();
                    widths[i] = max(widths[i], cell_width);
                }
            }
        }

        // Apply column constraints
        for (i, col) in self.columns.iter().enumerate() {
            if col.hidden {
                widths[i] = 0;
            } else {
                if col.min_width > 0 {
                    widths[i] = max(widths[i], col.min_width);
                }
                if col.max_width > 0 && widths[i] > col.max_width {
                    widths[i] = col.max_width;
                }
            }
        }

        widths
    }

    /// Format a cell value with alignment and width
    fn format_cell(&self, value: &str, width: usize, alignment: Alignment, truncate: bool) -> String {
        let stripped = strip_ansi_codes(value);
        let value_width = stripped.len();

        // Handle truncation
        let display_value = if truncate && value_width > width {
            // Try to preserve ANSI codes at the start
            let ansi_prefix = extract_ansi_prefix(value);
            let ansi_suffix = "\x1b[0m";

            let truncated = if stripped.len() > width - 2 {
                format!("{}...", &stripped[..width - 3])
            } else {
                stripped.to_string()
            };

            if !ansi_prefix.is_empty() {
                format!("{}{}{}", ansi_prefix, truncated, ansi_suffix)
            } else {
                truncated
            }
        } else {
            value.to_string()
        };

        let display_width = strip_ansi_codes(&display_value).len();
        let padding = width.saturating_sub(display_width);

        match alignment {
            Alignment::Left => format!("{}{}", display_value, " ".repeat(padding)),
            Alignment::Right => format!("{}{}", " ".repeat(padding), display_value),
            Alignment::Center => {
                let left_pad = padding / 2;
                let right_pad = padding - left_pad;
                format!("{}{}{}", " ".repeat(left_pad), display_value, " ".repeat(right_pad))
            }
        }
    }

    /// Print the table to stdout
    pub fn print(&self) {
        self.print_to(&mut io::stdout()).expect("Failed to write to stdout");
    }

    /// Print the table to a writer
    pub fn print_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let widths = self.calculate_widths();
        let indent = " ".repeat(self.indent);

        // Print header
        if self.show_header {
            write!(writer, "{}", indent)?;

            if self.show_row_numbers {
                write!(writer, "{}", style("#").dim())?;
                write!(writer, "{}", self.separator)?;
            }

            for (i, col) in self.columns.iter().enumerate() {
                if col.hidden {
                    continue;
                }
                let header = self.format_cell(&col.header, widths[i], col.alignment, false);
                if self.use_colors {
                    write!(writer, "{}", style(header).dim())?;
                } else {
                    write!(writer, "{}", header)?;
                }
                if i < self.columns.len() - 1 {
                    write!(writer, "{}", self.separator)?;
                }
            }
            writeln!(writer)?;

            // Print separator line
            match self.style {
                TableStyle::Simple => {
                    let total_width: usize = widths.iter().sum::<usize>()
                        + (self.columns.iter().filter(|c| !c.hidden).count() - 1) * self.separator.len();
                    writeln!(writer, "{}{}", indent, "-".repeat(total_width))?;
                }
                TableStyle::Unicode => {
                    write!(writer, "{}", indent)?;
                    for (i, col) in self.columns.iter().enumerate() {
                        if col.hidden {
                            continue;
                        }
                        write!(writer, "{}", "─".repeat(widths[i]))?;
                        if i < self.columns.len() - 1 {
                            write!(writer, "─┼─")?;
                        }
                    }
                    writeln!(writer)?;
                }
                TableStyle::Markdown => {
                    write!(writer, "{}|", indent)?;
                    for (i, col) in self.columns.iter().enumerate() {
                        if col.hidden {
                            continue;
                        }
                        let sep = match col.alignment {
                            Alignment::Left => format!(":{}", "-".repeat(widths[i] - 1)),
                            Alignment::Right => format!("{}:", "-".repeat(widths[i] - 1)),
                            Alignment::Center => format!(":{}:", "-".repeat(widths[i] - 2)),
                        };
                        write!(writer, "{}|", sep)?;
                    }
                    writeln!(writer)?;
                }
                TableStyle::Minimal | TableStyle::Compact => {}
            }
        }

        // Print rows
        for (row_idx, row) in self.rows.iter().enumerate() {
            write!(writer, "{}", indent)?;

            if self.show_row_numbers {
                write!(writer, "{}", style(format!("{}", row_idx + 1)).dim())?;
                write!(writer, "{}", self.separator)?;
            }

            for (i, col) in self.columns.iter().enumerate() {
                if col.hidden {
                    continue;
                }
                let cell = row.get(i).map(|s| s.as_str()).unwrap_or("");
                let formatted = self.format_cell(cell, widths[i], col.alignment, col.truncate);
                write!(writer, "{}", formatted)?;
                if i < self.columns.len() - 1 {
                    write!(writer, "{}", self.separator)?;
                }
            }
            writeln!(writer)?;
        }

        Ok(())
    }

    /// Render the table to a string
    pub fn to_string(&self) -> String {
        let mut buffer = Vec::new();
        self.print_to(&mut buffer).expect("Failed to write to buffer");
        String::from_utf8(buffer).expect("Invalid UTF-8")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPER FUNCTIONS
// ─────────────────────────────────────────────────────────────────────────────

/// Strip ANSI escape codes from a string
fn strip_ansi_codes(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip until we find the end of the escape sequence
            if chars.peek() == Some(&'[') {
                chars.next(); // consume '['
                // Skip until we find a letter (the command)
                while let Some(&c) = chars.peek() {
                    chars.next();
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// Extract ANSI prefix from a string (for preserving colors during truncation)
fn extract_ansi_prefix(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(&c) = chars.peek() {
        if c == '\x1b' {
            result.push(chars.next().unwrap());
            if chars.peek() == Some(&'[') {
                result.push(chars.next().unwrap());
                while let Some(&c) = chars.peek() {
                    result.push(chars.next().unwrap());
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            break;
        }
    }

    result
}

// ─────────────────────────────────────────────────────────────────────────────
// PREBUILT TABLES
// ─────────────────────────────────────────────────────────────────────────────

/// Create a table for displaying jobs
pub fn jobs_table() -> Table {
    Table::new(vec![
        Column::new("Job ID").width(14),
        Column::new("Name").width(20),
        Column::new("Status").width(10),
        Column::new("Progress").width(10).align(Alignment::Right),
        Column::new("Tasks").width(12).align(Alignment::Right),
        Column::new("Started").width(20),
    ])
}

/// Create a table for displaying tasks
pub fn tasks_table() -> Table {
    Table::new(vec![
        Column::new("Task ID").width(16),
        Column::new("Name").width(16),
        Column::new("Status").width(10),
        Column::new("Duration").width(10).align(Alignment::Right),
        Column::new("Worker").width(14),
        Column::new("Depends On").width(20),
    ])
}

/// Create a table for displaying nodes
pub fn nodes_table() -> Table {
    Table::new(vec![
        Column::new("Node ID").width(14),
        Column::new("Type").width(12),
        Column::new("Status").width(10),
        Column::new("Cores").width(6).align(Alignment::Right),
        Column::new("Memory").width(8).align(Alignment::Right),
        Column::new("Load").width(6).align(Alignment::Right),
        Column::new("Tasks").width(6).align(Alignment::Right),
        Column::new("Runtimes").width(20),
    ])
}

/// Create a table for displaying workers
pub fn workers_table() -> Table {
    Table::new(vec![
        Column::new("Worker ID").width(16),
        Column::new("Status").width(10),
        Column::new("Node").width(14),
        Column::new("Tasks").width(8).align(Alignment::Right),
        Column::new("CPU").width(6).align(Alignment::Right),
        Column::new("Memory").width(8).align(Alignment::Right),
        Column::new("Uptime").width(10),
    ])
}

/// Create a table for displaying tokens/transactions
pub fn tokens_table() -> Table {
    Table::new(vec![
        Column::new("Timestamp").width(20),
        Column::new("Type").width(10),
        Column::new("Amount").width(12).align(Alignment::Right),
        Column::new("Description").width(30),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_ansi_codes() {
        assert_eq!(strip_ansi_codes("\x1b[32mHello\x1b[0m"), "Hello");
        assert_eq!(strip_ansi_codes("No codes"), "No codes");
        assert_eq!(strip_ansi_codes("\x1b[1;31mBold Red\x1b[0m"), "Bold Red");
    }

    #[test]
    fn test_table_basic() {
        let mut table = Table::new(vec![
            Column::new("Name").width(10),
            Column::new("Value").width(8).align(Alignment::Right),
        ]);

        table.add_row(vec!["foo".into(), "123".into()]);
        table.add_row(vec!["bar".into(), "456".into()]);

        assert_eq!(table.len(), 2);

        let output = table.to_string();
        assert!(output.contains("Name"));
        assert!(output.contains("foo"));
        assert!(output.contains("456"));
    }

    #[test]
    fn test_column_alignment() {
        let table = Table::new(vec![
            Column::new("Left").width(10).align(Alignment::Left),
            Column::new("Right").width(10).align(Alignment::Right),
            Column::new("Center").width(10).align(Alignment::Center),
        ]);

        // Just verify it doesn't panic
        table.calculate_widths();
    }

    #[test]
    fn test_truncation() {
        let mut table = Table::new(vec![
            Column::new("ID").width(8).max_width(8).truncate(true),
        ]);

        table.add_row(vec!["very-long-identifier-here".into()]);

        let output = table.to_string();
        assert!(output.contains("..."));
    }
}
