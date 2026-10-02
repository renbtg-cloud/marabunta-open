// Marabunta - Licensed under the MIT License.
//! Configuration errors
//!
//! Error types for configuration loading and validation.
//! Integrates with the unified Marabunta error system for consistent
//! error codes, context, and suggestions.

use std::path::PathBuf;
use thiserror::Error;

use crate::error::{ErrorCode, MarabuntaError};

/// Format multiple errors into a single string
fn format_errors(errors: &[ConfigError]) -> String {
    errors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Errors that can occur during configuration operations
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Configuration file not found
    #[error("[E001] Configuration file not found: {0}")]
    FileNotFound(PathBuf),

    /// Failed to read configuration file
    #[error("[E004] Failed to read configuration file '{path}': {source}")]
    ReadError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Failed to parse TOML configuration
    #[error("[E002] Failed to parse configuration: {0}")]
    ParseError(#[from] toml::de::Error),

    /// Failed to serialize configuration
    #[error("[E902] Failed to serialize configuration: {0}")]
    SerializeError(#[from] toml::ser::Error),

    /// Failed to write configuration file
    #[error("[E005] Failed to write configuration file '{path}': {source}")]
    WriteError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Invalid configuration value
    #[error("[E003] Invalid configuration: {field}: {message}")]
    ValidationError { field: String, message: String },

    /// Invalid environment variable value
    #[error("[E006] Invalid environment variable '{var}': {message}")]
    InvalidEnvVar { var: String, message: String },

    /// Multiple validation errors
    #[error("[E003] Configuration validation failed with {} errors: {}", .0.len(), format_errors(.0))]
    MultipleErrors(Vec<ConfigError>),

    /// Missing required field
    #[error("[E007] Missing required configuration field: {0}")]
    MissingField(String),

    /// Invalid port number
    #[error("[E003] Invalid port number '{port}' for {field}")]
    InvalidPort { field: String, port: String },

    /// Invalid address format
    #[error("[E003] Invalid address format '{address}' for {field}: {message}")]
    InvalidAddress {
        field: String,
        address: String,
        message: String,
    },

    /// Invalid duration format
    #[error("[E003] Invalid duration format '{value}' for {field}: {message}")]
    InvalidDuration {
        field: String,
        value: String,
        message: String,
    },

    /// Invalid path
    #[error("[E003] Invalid path for {field}: {message}")]
    InvalidPath { field: String, message: String },

    /// Security configuration error
    #[error("[E003] Security configuration error: {0}")]
    SecurityError(String),

    /// No configuration source found
    #[error("[E001] No configuration found. Checked: {}", .0.join(", "))]
    NoConfigFound(Vec<String>),
}

impl ConfigError {
    /// Create a validation error
    pub fn validation(field: impl Into<String>, message: impl Into<String>) -> Self {
        ConfigError::ValidationError {
            field: field.into(),
            message: message.into(),
        }
    }

    /// Create an invalid address error
    pub fn invalid_address(
        field: impl Into<String>,
        address: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        ConfigError::InvalidAddress {
            field: field.into(),
            address: address.into(),
            message: message.into(),
        }
    }

    /// Create an invalid duration error
    pub fn invalid_duration(
        field: impl Into<String>,
        value: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        ConfigError::InvalidDuration {
            field: field.into(),
            value: value.into(),
            message: message.into(),
        }
    }

    /// Get the error code for this error
    pub fn error_code(&self) -> ErrorCode {
        match self {
            ConfigError::FileNotFound(_) | ConfigError::NoConfigFound(_) => {
                ErrorCode::CONFIG_NOT_FOUND
            }
            ConfigError::ParseError(_) => ErrorCode::CONFIG_PARSE_ERROR,
            ConfigError::ValidationError { .. }
            | ConfigError::MultipleErrors(_)
            | ConfigError::InvalidPort { .. }
            | ConfigError::InvalidAddress { .. }
            | ConfigError::InvalidDuration { .. }
            | ConfigError::InvalidPath { .. }
            | ConfigError::SecurityError(_) => ErrorCode::CONFIG_VALIDATION_ERROR,
            ConfigError::ReadError { .. } => ErrorCode::CONFIG_FILE_READ_ERROR,
            ConfigError::WriteError { .. } => ErrorCode::CONFIG_FILE_WRITE_ERROR,
            ConfigError::InvalidEnvVar { .. } => ErrorCode::CONFIG_ENV_VAR_ERROR,
            ConfigError::MissingField(_) => ErrorCode::CONFIG_MISSING_FIELD,
            ConfigError::SerializeError(_) => ErrorCode::SERIALIZATION_ERROR,
        }
    }

    /// Get suggestions for resolving this error
    pub fn suggestions(&self) -> Vec<&'static str> {
        match self {
            ConfigError::FileNotFound(_) | ConfigError::NoConfigFound(_) => vec![
                "Create a configuration file at one of the searched locations",
                "Run 'marabunta config init' to generate a default configuration",
                "Set MARABUNTA_CONFIG_PATH environment variable to specify config location",
            ],
            ConfigError::ParseError(_) => vec![
                "Check the configuration file for TOML syntax errors",
                "Validate TOML syntax at https://www.toml-lint.com/",
                "Look for missing quotes, brackets, or invalid values",
            ],
            ConfigError::ValidationError { field, .. } => match field.as_str() {
                f if f.contains("port") => vec![
                    "Ports must be numbers between 1 and 65535",
                    "Avoid well-known ports (1-1023) unless running as root",
                ],
                f if f.contains("address") => vec![
                    "Use format 'host:port' (e.g., '0.0.0.0:7000')",
                    "For IPv6, use brackets: '[::1]:7000'",
                ],
                f if f.contains("timeout") || f.contains("interval") => vec![
                    "Use humantime format: '30s', '5m', '1h'",
                    "Common values: connect_timeout='10s', heartbeat_interval='5s'",
                ],
                _ => vec!["Review the configuration documentation for valid values"],
            },
            ConfigError::ReadError { .. } => vec![
                "Check that the file exists and is readable",
                "Verify file permissions (chmod 644 config.toml)",
            ],
            ConfigError::WriteError { .. } => vec![
                "Check that the directory exists",
                "Verify write permissions on the directory",
            ],
            ConfigError::InvalidEnvVar { var, .. } => {
                let suggestions: Vec<&'static str> = if var.contains("PORT") {
                    vec!["Ports must be numbers between 1 and 65535"]
                } else if var.contains("ADDRESS") {
                    vec!["Use format 'host:port' (e.g., '0.0.0.0:7000')"]
                } else if var.contains("TIMEOUT") {
                    vec!["Use humantime format: '30s', '5m', '1h'"]
                } else if var.contains("TLS_ENABLED") {
                    vec!["Use 'true' or 'false'"]
                } else {
                    vec!["Review the expected format for this variable"]
                };
                suggestions
            }
            ConfigError::MissingField(field) => {
                let suggestions: Vec<&'static str> = match field.as_str() {
                    "node.name" => vec!["Add 'name = \"my-node\"' under [node] section"],
                    "master.coordinator_addresses" => vec![
                        "Add coordinator URLs: coordinator_addresses = [\"http://coord1:7000\"]",
                    ],
                    "worker.master_addresses" => {
                        vec!["Add master URLs: master_addresses = [\"http://master1:7100\"]"]
                    }
                    _ => vec!["Add the missing field to your configuration file"],
                };
                suggestions
            }
            ConfigError::InvalidPort { .. } => vec![
                "Ports must be numbers between 1 and 65535",
                "Ensure the port is not already in use",
            ],
            ConfigError::InvalidAddress { .. } => vec![
                "Use format 'host:port' (e.g., '0.0.0.0:7000')",
                "For IPv6, use brackets: '[::1]:7000'",
            ],
            ConfigError::InvalidDuration { .. } => vec![
                "Use humantime format: '30s', '5m', '1h', '1d'",
                "Examples: '10s' (10 seconds), '5m' (5 minutes), '2h30m' (2.5 hours)",
            ],
            ConfigError::InvalidPath { .. } => vec![
                "Ensure the path uses forward slashes or proper escaping",
                "Verify the directory exists or can be created",
            ],
            ConfigError::SecurityError(_) => vec![
                "When TLS is enabled, both cert_path and key_path are required",
                "Ensure certificate files exist and are readable",
            ],
            ConfigError::MultipleErrors(_) => vec![
                "Fix each validation error listed above",
                "Run 'marabunta config validate' to check your configuration",
            ],
            ConfigError::SerializeError(_) => {
                vec!["This is likely an internal error - please report it"]
            }
        }
    }

    /// Format the error with colored output for CLI display
    pub fn format_cli(&self, use_color: bool) -> String {
        let mut output = String::new();

        // Error header
        if use_color {
            output.push_str("\x1b[1;31m");
        }
        output.push_str(&format!("error[{}]", self.error_code()));
        if use_color {
            output.push_str("\x1b[0m");
        }
        output.push_str(": ");

        // Error message (without the error code prefix that's already in Display)
        if use_color {
            output.push_str("\x1b[1m");
        }
        // Extract message without [Exxx] prefix
        let msg = self.to_string();
        let msg = if msg.starts_with('[') {
            msg.split(']')
                .skip(1)
                .collect::<Vec<_>>()
                .join("]")
                .trim_start()
                .to_string()
        } else {
            msg
        };
        output.push_str(&msg);
        if use_color {
            output.push_str("\x1b[0m");
        }
        output.push('\n');

        // Suggestions
        let suggestions = self.suggestions();
        if !suggestions.is_empty() {
            output.push('\n');
            if use_color {
                output.push_str("\x1b[1;32m");
            }
            output.push_str("help");
            if use_color {
                output.push_str("\x1b[0m");
            }
            output.push_str(": ");

            for (i, suggestion) in suggestions.iter().enumerate() {
                if i > 0 {
                    output.push_str("\n      ");
                }
                output.push_str(suggestion);
            }
            output.push('\n');
        }

        // Documentation link
        if use_color {
            output.push_str("\x1b[2m");
        }
        output.push_str(&format!(
            "\nFor more info, see https://marabunta-compute.io/docs/errors/{}\n",
            self.error_code()
        ));
        if use_color {
            output.push_str("\x1b[0m");
        }

        output
    }
}

/// Convert ConfigError to MarabuntaError
impl From<ConfigError> for MarabuntaError {
    fn from(err: ConfigError) -> Self {
        match err {
            ConfigError::FileNotFound(path) => {
                MarabuntaError::config_not_found(&[path.display().to_string()])
            }
            ConfigError::NoConfigFound(paths) => MarabuntaError::config_not_found(&paths),
            ConfigError::ReadError { path, source } => {
                MarabuntaError::config_file_read(path.display().to_string(), source)
            }
            ConfigError::WriteError { path, source } => {
                MarabuntaError::config_file_write(path.display().to_string(), source)
            }
            ConfigError::ParseError(e) => {
                let message = e.message().to_string();
                let line = e.span().map(|s| s.start);
                MarabuntaError::config_parse_error("<config>", line, message)
            }
            ConfigError::ValidationError { field, message } => {
                MarabuntaError::config_validation_error(&field, "<value>", &message)
            }
            ConfigError::InvalidEnvVar { var, message } => {
                MarabuntaError::config_env_var_error(&var, "<value>", &message)
            }
            ConfigError::MissingField(field) => MarabuntaError::config_missing_field(field),
            ConfigError::InvalidPort { field, port } => MarabuntaError::config_validation_error(
                &field,
                &port,
                "must be a valid port number (1-65535)",
            ),
            ConfigError::InvalidAddress {
                field,
                address,
                message,
            } => MarabuntaError::config_validation_error(&field, &address, &message),
            ConfigError::InvalidDuration {
                field,
                value,
                message,
            } => MarabuntaError::config_validation_error(&field, &value, &message),
            ConfigError::InvalidPath { field, message } => {
                MarabuntaError::config_validation_error(&field, "<path>", &message)
            }
            ConfigError::SecurityError(message) => {
                MarabuntaError::config_validation_error("security", "<config>", &message)
            }
            ConfigError::MultipleErrors(errors) => {
                let messages: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
                MarabuntaError::new(
                    ErrorCode::CONFIG_VALIDATION_ERROR,
                    format!("{} configuration errors", errors.len()),
                )
                .with_field("Errors", messages.join("\n"))
            }
            ConfigError::SerializeError(e) => MarabuntaError::serialization("TOML", e),
        }
    }
}

/// Result type for configuration operations
pub type ConfigResult<T> = Result<T, ConfigError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_error_display() {
        let err = ConfigError::validation("network.bind_address", "must be a valid socket address");
        assert!(err.to_string().contains("network.bind_address"));
        assert!(err.to_string().contains("valid socket address"));
        assert!(err.to_string().contains("[E003]"));
    }

    #[test]
    fn test_invalid_address_error() {
        let err = ConfigError::invalid_address(
            "network.bind_address",
            "invalid:address",
            "could not parse",
        );
        let msg = err.to_string();
        assert!(msg.contains("network.bind_address"));
        assert!(msg.contains("invalid:address"));
    }

    #[test]
    fn test_no_config_found_error() {
        let err = ConfigError::NoConfigFound(vec![
            "./marabunta.toml".to_string(),
            "~/.config/marabunta/config.toml".to_string(),
        ]);
        let msg = err.to_string();
        assert!(msg.contains("./marabunta.toml"));
        assert!(msg.contains("~/.config/marabunta/config.toml"));
        assert!(msg.contains("[E001]"));
    }

    #[test]
    fn test_error_code() {
        assert_eq!(
            ConfigError::FileNotFound(PathBuf::from("test")).error_code(),
            ErrorCode::CONFIG_NOT_FOUND
        );
        assert_eq!(
            ConfigError::validation("field", "msg").error_code(),
            ErrorCode::CONFIG_VALIDATION_ERROR
        );
    }

    #[test]
    fn test_suggestions() {
        let err = ConfigError::FileNotFound(PathBuf::from("test"));
        let suggestions = err.suggestions();
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.contains("marabunta config init")));
    }

    #[test]
    fn test_field_specific_suggestions() {
        let err = ConfigError::validation("network.bind_address", "invalid");
        let suggestions = err.suggestions();
        assert!(suggestions.iter().any(|s| s.contains("host:port")));
    }

    #[test]
    fn test_conversion_to_marabunta_error() {
        let config_err = ConfigError::FileNotFound(PathBuf::from("/etc/marabunta/config.toml"));
        let marabunta_err: MarabuntaError = config_err.into();

        assert_eq!(marabunta_err.code, ErrorCode::CONFIG_NOT_FOUND);
        assert!(!marabunta_err.context.suggestions.is_empty());
    }

    #[test]
    fn test_cli_format() {
        let err = ConfigError::validation("worker.cpu_limit", "must be between 0.0 and 1.0");
        let formatted = err.format_cli(false);

        assert!(formatted.contains("error[E003]"));
        assert!(formatted.contains("worker.cpu_limit"));
        assert!(formatted.contains("help"));
        assert!(formatted.contains("docs/errors/E003"));
    }
}
