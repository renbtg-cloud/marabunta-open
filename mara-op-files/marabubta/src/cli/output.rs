// Marabunta - Licensed under the MIT License.
//! Output formatting with JSON mode support for scripting
//!
//! Provides unified output handling that can render data as human-readable text,
//! JSON, CSV, or YAML based on the configured output format.
//!
//! # Example
//!
//! ```rust,ignore
//! use marabunta_compute::cli::output::{Output, Outputter};
//!
//! let outputter = Outputter::new(OutputFormat::Json);
//!
//! // Outputs JSON when format is Json, human-readable otherwise
//! outputter.output(&job_status)?;
//! ```

use serde::Serialize;
use std::io::{self, Write};

use crate::cli::types::OutputFormat;

// ─────────────────────────────────────────────────────────────────────────────
// OUTPUT TRAIT
// ─────────────────────────────────────────────────────────────────────────────

/// Trait for types that can be output in multiple formats
pub trait Output: Serialize {
    /// Render as human-readable output
    fn to_human(&self) -> String;

    /// Render as CSV (with header on first call)
    fn to_csv(&self, include_header: bool) -> String {
        // Default implementation for types that don't support CSV
        let _ = include_header;
        self.to_human()
    }

    /// Render as YAML
    fn to_yaml(&self) -> Result<String, serde_json::Error> {
        // Default to JSON-like YAML (proper YAML would need serde_yaml)
        let json = serde_json::to_value(self)?;
        Ok(json_to_yaml(&json, 0))
    }
}

/// Convert JSON value to YAML-like string
fn json_to_yaml(value: &serde_json::Value, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            if s.contains('\n') || s.contains(':') || s.contains('#') {
                format!("|\n{}{}", prefix, s.replace('\n', &format!("\n{}", prefix)))
            } else {
                s.clone()
            }
        }
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                "[]".to_string()
            } else {
                let items: Vec<String> = arr
                    .iter()
                    .map(|v| format!("{}- {}", prefix, json_to_yaml(v, indent + 1)))
                    .collect();
                format!("\n{}", items.join("\n"))
            }
        }
        serde_json::Value::Object(obj) => {
            if obj.is_empty() {
                "{}".to_string()
            } else {
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| {
                        let val = json_to_yaml(v, indent + 1);
                        if val.starts_with('\n') {
                            format!("{}{}:{}", prefix, k, val)
                        } else {
                            format!("{}{}: {}", prefix, k, val)
                        }
                    })
                    .collect();
                if indent == 0 {
                    items.join("\n")
                } else {
                    format!("\n{}", items.join("\n"))
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTPUTTER
// ─────────────────────────────────────────────────────────────────────────────

/// Handles output formatting based on the configured format
pub struct Outputter {
    format: OutputFormat,
    /// Whether we've output a CSV header yet
    csv_header_written: bool,
    /// Use colors (for human output)
    use_colors: bool,
    /// Output to stderr instead of stdout
    use_stderr: bool,
    /// Quiet mode (minimal output)
    quiet: bool,
}

impl Outputter {
    /// Create a new outputter with the specified format
    pub fn new(format: OutputFormat) -> Self {
        Self {
            format,
            csv_header_written: false,
            use_colors: true,
            use_stderr: false,
            quiet: false,
        }
    }

    /// Set color usage
    pub fn colors(mut self, use_colors: bool) -> Self {
        self.use_colors = use_colors;
        self
    }

    /// Output to stderr
    pub fn stderr(mut self) -> Self {
        self.use_stderr = true;
        self
    }

    /// Enable quiet mode
    pub fn quiet(mut self, quiet: bool) -> Self {
        self.quiet = quiet;
        self
    }

    /// Get the output format
    pub fn format(&self) -> OutputFormat {
        self.format
    }

    /// Check if in JSON mode
    pub fn is_json(&self) -> bool {
        self.format == OutputFormat::Json
    }

    /// Check if in human mode
    pub fn is_human(&self) -> bool {
        self.format == OutputFormat::Human
    }

    /// Output a value in the configured format
    pub fn output<T: Output>(&mut self, value: &T) -> io::Result<()> {
        let text = match self.format {
            OutputFormat::Human => value.to_human(),
            OutputFormat::Json => {
                serde_json::to_string_pretty(value).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, e)
                })?
            }
            OutputFormat::Csv => {
                let include_header = !self.csv_header_written;
                self.csv_header_written = true;
                value.to_csv(include_header)
            }
            OutputFormat::Yaml => {
                value.to_yaml().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, e)
                })?
            }
        };

        self.write(&text)
    }

    /// Output raw JSON (for types that implement Serialize but not Output)
    pub fn output_json<T: Serialize>(&self, value: &T) -> io::Result<()> {
        let text = serde_json::to_string_pretty(value).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })?;
        self.write(&text)
    }

    /// Output a message (only in human mode, unless forced)
    pub fn message(&self, msg: &str) -> io::Result<()> {
        if !self.quiet && self.format == OutputFormat::Human {
            self.write(msg)
        } else {
            Ok(())
        }
    }

    /// Output a message in any mode
    pub fn message_always(&self, msg: &str) -> io::Result<()> {
        if !self.quiet {
            self.write(msg)
        } else {
            Ok(())
        }
    }

    /// Output an error message
    pub fn error(&self, msg: &str) -> io::Result<()> {
        if self.format == OutputFormat::Json {
            let err = serde_json::json!({
                "error": msg
            });
            writeln!(io::stderr(), "{}", serde_json::to_string_pretty(&err).unwrap())
        } else {
            writeln!(io::stderr(), "Error: {}", msg)
        }
    }

    /// Output a success message (only in human mode)
    pub fn success(&self, msg: &str) -> io::Result<()> {
        if !self.quiet && self.format == OutputFormat::Human {
            use console::style;
            self.write(&format!("{} {}", style("SUCCESS").green().bold(), msg))
        } else {
            Ok(())
        }
    }

    /// Output a warning message
    pub fn warning(&self, msg: &str) -> io::Result<()> {
        if !self.quiet {
            if self.format == OutputFormat::Json {
                let warn = serde_json::json!({
                    "warning": msg
                });
                writeln!(io::stderr(), "{}", serde_json::to_string_pretty(&warn).unwrap())
            } else {
                use console::style;
                writeln!(io::stderr(), "{} {}", style("WARNING").yellow().bold(), msg)
            }
        } else {
            Ok(())
        }
    }

    /// Write to the configured output stream
    fn write(&self, text: &str) -> io::Result<()> {
        if self.use_stderr {
            writeln!(io::stderr(), "{}", text)
        } else {
            writeln!(io::stdout(), "{}", text)
        }
    }
}

impl Default for Outputter {
    fn default() -> Self {
        Self::new(OutputFormat::Human)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON OUTPUT HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/// Builder for JSON output with consistent structure
#[derive(Debug, Clone)]
pub struct JsonOutput {
    data: serde_json::Map<String, serde_json::Value>,
}

impl JsonOutput {
    /// Create a new JSON output builder
    pub fn new() -> Self {
        Self {
            data: serde_json::Map::new(),
        }
    }

    /// Add a field
    pub fn field(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        self.data.insert(
            key.into(),
            serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
        );
        self
    }

    /// Add an optional field (omitted if None)
    pub fn field_opt<T: Serialize>(mut self, key: impl Into<String>, value: Option<T>) -> Self {
        if let Some(v) = value {
            self.data.insert(
                key.into(),
                serde_json::to_value(v).unwrap_or(serde_json::Value::Null),
            );
        }
        self
    }

    /// Add a nested object
    pub fn object(mut self, key: impl Into<String>, builder: JsonOutput) -> Self {
        self.data.insert(key.into(), serde_json::Value::Object(builder.data));
        self
    }

    /// Add an array
    pub fn array<T: Serialize>(mut self, key: impl Into<String>, items: impl IntoIterator<Item = T>) -> Self {
        let arr: Vec<serde_json::Value> = items
            .into_iter()
            .filter_map(|item| serde_json::to_value(item).ok())
            .collect();
        self.data.insert(key.into(), serde_json::Value::Array(arr));
        self
    }

    /// Build the JSON value
    pub fn build(self) -> serde_json::Value {
        serde_json::Value::Object(self.data)
    }

    /// Build and print to stdout
    pub fn print(self) -> io::Result<()> {
        let json = self.build();
        println!("{}", serde_json::to_string_pretty(&json).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })?);
        Ok(())
    }

    /// Build and return as string
    pub fn to_string(self) -> Result<String, serde_json::Error> {
        let json = self.build();
        serde_json::to_string_pretty(&json)
    }
}

impl Default for JsonOutput {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// STANDARD OUTPUT IMPLEMENTATIONS
// ─────────────────────────────────────────────────────────────────────────────

/// Standard success response
#[derive(Debug, Clone, Serialize)]
pub struct SuccessResponse {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl SuccessResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            id: None,
            details: None,
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn with_details(mut self, details: impl Serialize) -> Self {
        self.details = serde_json::to_value(details).ok();
        self
    }
}

impl Output for SuccessResponse {
    fn to_human(&self) -> String {
        use console::style;
        let mut output = format!("{} {}", style("SUCCESS").green().bold(), self.message);
        if let Some(id) = &self.id {
            output.push_str(&format!("\n  ID: {}", style(id).cyan()));
        }
        output
    }
}

/// Standard error response
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub success: bool,
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            success: false,
            error: error.into(),
            code: None,
            details: None,
        }
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn with_details(mut self, details: impl Serialize) -> Self {
        self.details = serde_json::to_value(details).ok();
        self
    }
}

impl Output for ErrorResponse {
    fn to_human(&self) -> String {
        use console::style;
        let mut output = format!("{} {}", style("ERROR").red().bold(), self.error);
        if let Some(code) = &self.code {
            output.push_str(&format!(" [{}]", code));
        }
        output
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Serialize)]
    struct TestData {
        name: String,
        value: i32,
    }

    impl Output for TestData {
        fn to_human(&self) -> String {
            format!("Name: {}, Value: {}", self.name, self.value)
        }

        fn to_csv(&self, include_header: bool) -> String {
            let mut output = String::new();
            if include_header {
                output.push_str("name,value\n");
            }
            output.push_str(&format!("{},{}\n", self.name, self.value));
            output
        }
    }

    #[test]
    fn test_outputter_human() {
        let mut outputter = Outputter::new(OutputFormat::Human);
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        // Just verify it doesn't panic
        let _ = outputter.output(&data);
    }

    #[test]
    fn test_outputter_json() {
        let outputter = Outputter::new(OutputFormat::Json);
        assert!(outputter.is_json());
        assert!(!outputter.is_human());
    }

    #[test]
    fn test_json_output_builder() {
        let json = JsonOutput::new()
            .field("name", "test")
            .field("value", 42)
            .field_opt::<String>("optional", None)
            .array("items", vec![1, 2, 3])
            .build();

        assert!(json.is_object());
        assert_eq!(json["name"], "test");
        assert_eq!(json["value"], 42);
        assert!(json.get("optional").is_none());
    }

    #[test]
    fn test_success_response() {
        let response = SuccessResponse::new("Operation completed")
            .with_id("job-123");

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["success"], true);
        assert_eq!(json["message"], "Operation completed");
        assert_eq!(json["id"], "job-123");
    }

    #[test]
    fn test_error_response() {
        let response = ErrorResponse::new("Something went wrong")
            .with_code("E001");

        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["success"], false);
        assert_eq!(json["error"], "Something went wrong");
        assert_eq!(json["code"], "E001");
    }

    #[test]
    fn test_json_to_yaml() {
        let json = serde_json::json!({
            "name": "test",
            "value": 42,
            "items": [1, 2, 3]
        });

        let yaml = json_to_yaml(&json, 0);
        assert!(yaml.contains("name: test"));
        assert!(yaml.contains("value: 42"));
    }
}
