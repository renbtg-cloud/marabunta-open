// Marabunta - Licensed under the MIT License.
//! CLI configuration management
//!
//! Handles loading, saving, and interactive setup of CLI configuration.
//! Configuration is stored in ~/.marabunta/config.toml

use clap::{Args, Subcommand};
use dialoguer::{theme::ColorfulTheme, Confirm, Input, Select};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

use crate::cli::types::{CliError, OutputFormat};

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG ARGS
// ─────────────────────────────────────────────────────────────────────────────

/// Arguments for the config command
#[derive(Args)]
pub struct ConfigArgs {
    /// Config subcommand
    #[command(subcommand)]
    pub command: Option<ConfigCommand>,
}

/// Config subcommands
#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
    /// Get a configuration value
    Get {
        /// Configuration key
        key: String,
    },
    /// List all configuration values
    List,
    /// Interactive setup wizard
    Init,
    /// Reset configuration to defaults
    Reset,
    /// Show configuration file path
    Path,
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG ERROR
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration-specific errors
#[derive(Debug)]
pub enum ConfigError {
    /// IO error
    Io(std::io::Error),
    /// Parse error
    Parse(String),
    /// Serialization error
    Serialize(String),
    /// Invalid key
    InvalidKey(String),
    /// Missing required value
    MissingValue(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(err) => write!(f, "IO error: {}", err),
            ConfigError::Parse(msg) => write!(f, "Parse error: {}", msg),
            ConfigError::Serialize(msg) => write!(f, "Serialization error: {}", msg),
            ConfigError::InvalidKey(key) => write!(f, "Invalid configuration key: {}", key),
            ConfigError::MissingValue(key) => write!(f, "Missing required value: {}", key),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        ConfigError::Io(err)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(err: toml::de::Error) -> Self {
        ConfigError::Parse(err.to_string())
    }
}

impl From<toml::ser::Error> for ConfigError {
    fn from(err: toml::ser::Error) -> Self {
        ConfigError::Serialize(err.to_string())
    }
}

impl From<ConfigError> for CliError {
    fn from(err: ConfigError) -> Self {
        CliError::Config(err.to_string())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG STRUCT
// ─────────────────────────────────────────────────────────────────────────────

/// CLI configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Coordinator URL
    #[serde(default = "default_coordinator_url")]
    pub coordinator_url: String,

    /// Organization/API key (optional)
    #[serde(default)]
    pub org_key: Option<String>,

    /// Default runtime for job submission
    #[serde(default)]
    pub default_runtime: Option<String>,

    /// Default timeout for tasks
    #[serde(default = "default_timeout")]
    pub default_timeout: String,

    /// Default output format
    #[serde(default)]
    pub output_format: OutputFormat,

    /// Enable color output
    #[serde(default = "default_true")]
    pub color: bool,

    /// Default region preference
    #[serde(default)]
    pub default_region: Option<String>,

    /// Auto-confirm prompts
    #[serde(default)]
    pub auto_confirm: bool,

    /// Maximum concurrent uploads
    #[serde(default = "default_max_uploads")]
    pub max_concurrent_uploads: u32,

    /// HTTP request timeout in seconds
    #[serde(default = "default_http_timeout")]
    pub http_timeout_secs: u64,

    /// Enable verbose output
    #[serde(default)]
    pub verbose: bool,
}

fn default_coordinator_url() -> String {
    "http://localhost:8080".to_string()
}

fn default_timeout() -> String {
    "5m".to_string()
}

fn default_true() -> bool {
    true
}

fn default_max_uploads() -> u32 {
    4
}

fn default_http_timeout() -> u64 {
    30
}

impl Default for Config {
    fn default() -> Self {
        Self {
            coordinator_url: default_coordinator_url(),
            org_key: None,
            default_runtime: None,
            default_timeout: default_timeout(),
            output_format: OutputFormat::Human,
            color: true,
            default_region: None,
            auto_confirm: false,
            max_concurrent_uploads: default_max_uploads(),
            http_timeout_secs: default_http_timeout(),
            verbose: false,
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn load() -> Result<Self, ConfigError> {
        let path = Self::config_path();
        if path.exists() {
            let content = std::fs::read_to_string(&path)?;
            Ok(toml::from_str(&content)?)
        } else {
            Ok(Self::default())
        }
    }

    /// Save configuration to file
    pub fn save(&self) -> Result<(), ConfigError> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Get configuration file path
    pub fn config_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".marabunta")
            .join("config.toml")
    }

    /// Get configuration directory path
    pub fn config_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".marabunta")
    }

    /// Set a configuration value by key
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), ConfigError> {
        match key {
            "coordinator_url" | "coordinator" | "url" => {
                self.coordinator_url = value.to_string();
            }
            "org_key" | "key" | "api_key" => {
                self.org_key = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                };
            }
            "default_runtime" | "runtime" => {
                self.default_runtime = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                };
            }
            "default_timeout" | "timeout" => {
                self.default_timeout = value.to_string();
            }
            "output_format" | "format" => {
                self.output_format = match value.to_lowercase().as_str() {
                    "human" => OutputFormat::Human,
                    "json" => OutputFormat::Json,
                    "csv" => OutputFormat::Csv,
                    "yaml" => OutputFormat::Yaml,
                    _ => {
                        return Err(ConfigError::InvalidKey(format!(
                            "Invalid format: {}",
                            value
                        )))
                    }
                };
            }
            "color" => {
                self.color = value
                    .parse()
                    .map_err(|_| ConfigError::InvalidKey(format!("Invalid boolean: {}", value)))?;
            }
            "default_region" | "region" => {
                self.default_region = if value.is_empty() {
                    None
                } else {
                    Some(value.to_string())
                };
            }
            "auto_confirm" => {
                self.auto_confirm = value
                    .parse()
                    .map_err(|_| ConfigError::InvalidKey(format!("Invalid boolean: {}", value)))?;
            }
            "max_concurrent_uploads" | "max_uploads" => {
                self.max_concurrent_uploads = value
                    .parse()
                    .map_err(|_| ConfigError::InvalidKey(format!("Invalid number: {}", value)))?;
            }
            "http_timeout_secs" | "http_timeout" => {
                self.http_timeout_secs = value
                    .parse()
                    .map_err(|_| ConfigError::InvalidKey(format!("Invalid number: {}", value)))?;
            }
            "verbose" => {
                self.verbose = value
                    .parse()
                    .map_err(|_| ConfigError::InvalidKey(format!("Invalid boolean: {}", value)))?;
            }
            _ => return Err(ConfigError::InvalidKey(key.to_string())),
        }
        Ok(())
    }

    /// Get a configuration value by key
    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "coordinator_url" | "coordinator" | "url" => Some(self.coordinator_url.clone()),
            "org_key" | "key" | "api_key" => self.org_key.clone(),
            "default_runtime" | "runtime" => self.default_runtime.clone(),
            "default_timeout" | "timeout" => Some(self.default_timeout.clone()),
            "output_format" | "format" => Some(self.output_format.to_string()),
            "color" => Some(self.color.to_string()),
            "default_region" | "region" => self.default_region.clone(),
            "auto_confirm" => Some(self.auto_confirm.to_string()),
            "max_concurrent_uploads" | "max_uploads" => {
                Some(self.max_concurrent_uploads.to_string())
            }
            "http_timeout_secs" | "http_timeout" => Some(self.http_timeout_secs.to_string()),
            "verbose" => Some(self.verbose.to_string()),
            _ => None,
        }
    }

    /// Merge with command-line overrides
    pub fn with_overrides(
        mut self,
        coordinator: Option<String>,
        format: Option<OutputFormat>,
    ) -> Self {
        if let Some(url) = coordinator {
            self.coordinator_url = url;
        }
        if let Some(fmt) = format {
            self.output_format = fmt;
        }
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTE CONFIG
// ─────────────────────────────────────────────────────────────────────────────

/// Execute the config command
pub async fn execute_config(args: ConfigArgs) -> Result<(), CliError> {
    match args.command {
        Some(ConfigCommand::Init) => execute_init().await,
        Some(ConfigCommand::Set { key, value }) => execute_set(&key, &value),
        Some(ConfigCommand::Get { key }) => execute_get(&key),
        Some(ConfigCommand::List) | None => execute_list(),
        Some(ConfigCommand::Reset) => execute_reset(),
        Some(ConfigCommand::Path) => execute_path(),
    }
}

/// Interactive setup wizard
async fn execute_init() -> Result<(), CliError> {
    use console::style;

    println!();
    println!("{} Marabunta Compute CLI Setup", style("").yellow());
    println!("{}", style("─".repeat(50)).dim());
    println!();

    let theme = ColorfulTheme::default();

    // Coordinator URL
    let coordinator: String = Input::with_theme(&theme)
        .with_prompt("Coordinator URL")
        .default("http://localhost:8080".to_string())
        .interact_text()
        .map_err(|e| CliError::Config(e.to_string()))?;

    // Organization key (optional)
    let use_org_key = Confirm::with_theme(&theme)
        .with_prompt("Do you have an organization/API key?")
        .default(false)
        .interact()
        .map_err(|e| CliError::Config(e.to_string()))?;

    let org_key = if use_org_key {
        let key: String = Input::with_theme(&theme)
            .with_prompt("Organization key")
            .interact_text()
            .map_err(|e| CliError::Config(e.to_string()))?;
        Some(key)
    } else {
        None
    };

    // Default runtime
    let runtimes = vec!["Auto-detect", "python3", "wasm", "lua", "native"];
    let runtime_idx = Select::with_theme(&theme)
        .with_prompt("Default runtime")
        .items(&runtimes)
        .default(0)
        .interact()
        .map_err(|e| CliError::Config(e.to_string()))?;

    let default_runtime = if runtime_idx == 0 {
        None
    } else {
        Some(runtimes[runtime_idx].to_string())
    };

    // Output format
    let formats = vec!["human", "json", "csv", "yaml"];
    let format_idx = Select::with_theme(&theme)
        .with_prompt("Default output format")
        .items(&formats)
        .default(0)
        .interact()
        .map_err(|e| CliError::Config(e.to_string()))?;

    let output_format = match formats[format_idx] {
        "json" => OutputFormat::Json,
        "csv" => OutputFormat::Csv,
        "yaml" => OutputFormat::Yaml,
        _ => OutputFormat::Human,
    };

    // Default timeout
    let timeout: String = Input::with_theme(&theme)
        .with_prompt("Default task timeout")
        .default("5m".to_string())
        .interact_text()
        .map_err(|e| CliError::Config(e.to_string()))?;

    // Create config
    let config = Config {
        coordinator_url: coordinator,
        org_key,
        default_runtime,
        default_timeout: timeout,
        output_format,
        ..Config::default()
    };

    // Save
    config.save()?;

    println!();
    println!(
        "{} Configuration saved to {}",
        style("SUCCESS").green().bold(),
        Config::config_path().display()
    );
    println!();

    // Show saved config
    println!("{}", toml::to_string_pretty(&config)?);

    Ok(())
}

/// Set a configuration value
fn execute_set(key: &str, value: &str) -> Result<(), CliError> {
    use console::style;

    let mut config = Config::load()?;
    config.set(key, value)?;
    config.save()?;

    println!(
        "{} {} = {}",
        style("Set").green(),
        style(key).cyan(),
        style(value).yellow()
    );

    Ok(())
}

/// Get a configuration value
fn execute_get(key: &str) -> Result<(), CliError> {
    let config = Config::load()?;

    if let Some(value) = config.get(key) {
        println!("{}", value);
    } else {
        return Err(CliError::Config(format!("Unknown key: {}", key)));
    }

    Ok(())
}

/// List all configuration values
fn execute_list() -> Result<(), CliError> {
    use console::style;

    let config = Config::load()?;

    println!();
    println!("{} Current Configuration", style("Marabunta").yellow().bold());
    println!("{}", style("─".repeat(50)).dim());
    println!();
    println!("{}", toml::to_string_pretty(&config)?);
    println!("{}", style("─".repeat(50)).dim());
    println!(
        "Config file: {}",
        style(Config::config_path().display()).dim()
    );

    Ok(())
}

/// Reset configuration to defaults
fn execute_reset() -> Result<(), CliError> {
    use console::style;
    use dialoguer::{theme::ColorfulTheme, Confirm};

    let theme = ColorfulTheme::default();

    let confirm = Confirm::with_theme(&theme)
        .with_prompt("Reset configuration to defaults?")
        .default(false)
        .interact()
        .map_err(|e| CliError::Config(e.to_string()))?;

    if confirm {
        let config = Config::default();
        config.save()?;

        println!(
            "{} Configuration reset to defaults",
            style("SUCCESS").green().bold()
        );
    } else {
        println!("Cancelled");
    }

    Ok(())
}

/// Show configuration file path
fn execute_path() -> Result<(), CliError> {
    println!("{}", Config::config_path().display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.coordinator_url, "http://localhost:8080");
        assert_eq!(config.default_timeout, "5m");
        assert!(config.color);
    }

    #[test]
    fn test_config_set_get() {
        let mut config = Config::default();

        config
            .set("coordinator_url", "http://example.com:9000")
            .unwrap();
        assert_eq!(
            config.get("coordinator_url"),
            Some("http://example.com:9000".to_string())
        );

        config.set("color", "false").unwrap();
        assert_eq!(config.get("color"), Some("false".to_string()));

        config.set("format", "json").unwrap();
        assert_eq!(config.output_format, OutputFormat::Json);
    }

    #[test]
    fn test_config_invalid_key() {
        let mut config = Config::default();
        assert!(config.set("invalid_key", "value").is_err());
    }
}
