#![allow(dead_code)]
// Marabunta - Licensed under the MIT License.

//! swarmctl -- Management CLI for the Marabunta Swarm.
//!
//! A comprehensive command-line tool for operating, monitoring, and managing
//! Marabunta swarm clusters. Supports interactive TUI mode, shell completions,
//! and multiple output formats (text, JSON, YAML, CSV).
//!
//! # Configuration
//!
//! Config file at `~/.swarmctl.toml`:
//! ```toml
//! url = "http://localhost:8080"
//! token = "secret"
//! default_output = "text"
//! color = true
//! ```

use std::collections::HashMap;
use std::io::{self, Write as IoWrite};
use std::path::PathBuf;
use std::time::Duration;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use console::{style, Term};
use dialoguer::Confirm;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_API_URL: &str = "http://localhost:8080";
const DEFAULT_CONFIG_FILENAME: &str = ".swarmctl.toml";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const USER_AGENT: &str = "swarmctl";

// ============================================================================
// Config file (~/.swarmctl.toml)
// ============================================================================

/// Persistent configuration loaded from `~/.swarmctl.toml`.
#[derive(Debug, Deserialize, Serialize, Default)]
struct SwarmCtlConfig {
    /// Base URL of the swarm API.
    #[serde(default)]
    url: Option<String>,
    /// Authentication token.
    #[serde(default)]
    token: Option<String>,
    /// Default output format (text, json, yaml, csv).
    #[serde(default)]
    default_output: Option<String>,
    /// Whether to use colored output.
    #[serde(default)]
    color: Option<bool>,
}

impl SwarmCtlConfig {
    /// Load config from a file path; returns default if the file does not exist.
    fn load(path: &PathBuf) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }
}

// ============================================================================
// Output format
// ============================================================================

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
enum OutputFormat {
    Text,
    Json,
    Yaml,
    Csv,
}

/// Shell type for completions generation.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ShellType {
    Bash,
    Zsh,
    Fish,
    Powershell,
    Elvish,
}

impl OutputFormat {
    fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => Self::Json,
            "yaml" | "yml" => Self::Yaml,
            "csv" => Self::Csv,
            _ => Self::Text,
        }
    }
}

// ============================================================================
// CLI definition (clap)
// ============================================================================

/// swarmctl -- Management CLI for the Marabunta Swarm
#[derive(Parser)]
#[command(name = "swarmctl", version = VERSION, about = "Management CLI for the Marabunta Swarm")]
struct Cli {
    /// API endpoint URL
    #[arg(long, global = true, env = "SWARMCTL_URL")]
    url: Option<String>,

    /// Authentication token
    #[arg(long, global = true, env = "SWARMCTL_TOKEN")]
    token: Option<String>,

    /// Output format: text, json, yaml, csv
    #[arg(long, global = true, value_enum)]
    output: Option<OutputFormat>,

    /// Enable verbose/debug logging
    #[arg(long, global = true)]
    verbose: bool,

    /// Suppress non-essential output (for scripts)
    #[arg(long, global = true)]
    quiet: bool,

    /// Disable colored output
    #[arg(long, global = true)]
    no_color: bool,

    /// Path to config file (default: ~/.swarmctl.toml)
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Dashboard: psyche + nodes + jobs + alerts summary
    Status,

    /// Swarm psyche (emotional state) management
    #[command(subcommand)]
    Psyche(PsycheCommands),

    /// Node management
    #[command(subcommand)]
    Nodes(NodeCommands),

    /// Job management
    #[command(subcommand)]
    Jobs(JobCommands),

    /// Fleet operations (drain, cordon, rolling updates)
    #[command(subcommand)]
    Fleet(FleetCommands),

    /// Event bus operations
    #[command(subcommand)]
    Events(EventCommands),

    /// Alert management
    #[command(subcommand)]
    Alerts(AlertCommands),

    /// SLA monitoring
    #[command(subcommand)]
    Sla(SlaCommands),

    /// Capacity planning
    #[command(subcommand)]
    Capacity(CapacityCommands),

    /// Vision management
    #[command(subcommand)]
    Visions(VisionCommands),

    /// Multi-swarm constellation management
    #[command(subcommand)]
    Constellation(ConstellationCommands),

    /// Audit log operations
    #[command(subcommand)]
    Audit(AuditCommands),

    /// Raw Prometheus metrics
    Metrics,

    /// Interactive TUI dashboard
    Tui,

    /// Generate shell completions
    Completions {
        /// Shell type (bash, zsh, fish, powershell, elvish)
        #[arg(value_enum)]
        shell: ShellType,
    },

    /// Version information
    Version,
}

// -- Psyche subcommands --
#[derive(Subcommand)]
enum PsycheCommands {
    /// Current psyche state with ASCII bars
    Status,
    /// Detailed facet breakdown
    Facets,
    /// Facet history chart (ASCII sparklines)
    History {
        /// Hours of history to show
        #[arg(long, default_value = "24")]
        hours: u32,
    },
    /// Predicted future state
    Forecast {
        /// Minutes to forecast
        #[arg(long, default_value = "60")]
        minutes: u32,
    },
    /// Archetype management
    #[command(subcommand)]
    Archetypes(ArchetypeCommands),
}

#[derive(Subcommand)]
enum ArchetypeCommands {
    /// List all archetypes with match status
    List,
    /// Add custom archetype from TOML file
    Add {
        /// Path to archetype TOML file
        file: PathBuf,
    },
    /// Remove custom archetype by name
    Remove {
        /// Archetype name
        name: String,
    },
}

// -- Node subcommands --
#[derive(Subcommand)]
enum NodeCommands {
    /// List nodes with status, load, traits, region, uptime
    List {
        /// Filter by status (alive, suspect, dead, draining, cordoned, quarantined)
        #[arg(long)]
        status: Option<String>,
    },
    /// Show detailed node information
    Show {
        /// Node ID (short or full UUID)
        id: String,
    },
    /// Drain a node (finish current work, stop accepting new)
    Drain {
        /// Node ID
        id: String,
        /// Drain timeout in seconds
        #[arg(long, default_value = "300")]
        timeout: u64,
        /// Reason for draining
        #[arg(long)]
        reason: Option<String>,
    },
    /// Cordon a node (stop accepting new work)
    Cordon {
        /// Node ID
        id: String,
        /// Reason for cordoning
        #[arg(long)]
        reason: Option<String>,
    },
    /// Uncordon a node (resume accepting work)
    Uncordon {
        /// Node ID
        id: String,
    },
    /// Quarantine a node (isolate from swarm)
    Quarantine {
        /// Node ID
        id: String,
        /// Reason for quarantining
        #[arg(long)]
        reason: Option<String>,
    },
    /// Remove quarantine from a node
    Unquarantine {
        /// Node ID
        id: String,
    },
    /// Show or set node tags
    #[command(subcommand)]
    Tags(TagCommands),
    /// Show health check results
    Health {
        /// Node ID (omit for all nodes)
        id: Option<String>,
    },
}

#[derive(Subcommand)]
enum TagCommands {
    /// Show tags for a node
    Show {
        /// Node ID
        id: String,
    },
    /// Set a tag on a node (key=value)
    Set {
        /// Node ID
        id: String,
        /// Tag in key=value format
        tag: String,
    },
}

// -- Job subcommands --
#[derive(Subcommand)]
enum JobCommands {
    /// List jobs
    List {
        /// Filter by status
        #[arg(long)]
        status: Option<String>,
    },
    /// Show detailed job information
    Show {
        /// Job ID
        id: String,
    },
    /// Root-cause analysis tree for a job
    Diagnose {
        /// Job ID
        id: String,
    },
    /// Cancel a job
    Cancel {
        /// Job ID
        id: String,
    },
}

// -- Fleet subcommands --
#[derive(Subcommand)]
enum FleetCommands {
    /// Fleet summary and node counts by state
    Status,
    /// Rolling update management
    #[command(subcommand)]
    Update(UpdateCommands),
}

#[derive(Subcommand)]
enum UpdateCommands {
    /// Show rolling update status with progress bar
    Status,
    /// Start a rolling update
    Start {
        /// Target version
        version: String,
        /// Canary percentage (0.0 - 1.0)
        #[arg(long, default_value = "0.05")]
        canary_pct: f64,
        /// Comma-separated target regions
        #[arg(long)]
        regions: Option<String>,
        /// Dry run (show plan without executing)
        #[arg(long)]
        dry_run: bool,
    },
    /// Pause the rolling update
    Pause {
        /// Reason for pausing
        #[arg(long)]
        reason: Option<String>,
    },
    /// Resume the rolling update
    Resume,
    /// Rollback the rolling update
    Rollback {
        /// Reason for rollback
        #[arg(long)]
        reason: Option<String>,
    },
    /// Cancel the rolling update
    Cancel,
    /// Show past update history
    History,
}

// -- Event subcommands --
#[derive(Subcommand)]
enum EventCommands {
    /// List recent events
    List {
        /// Filter by domain
        #[arg(long)]
        domain: Option<String>,
        /// Filter by severity
        #[arg(long)]
        severity: Option<String>,
        /// Maximum events to show
        #[arg(long, default_value = "50")]
        limit: usize,
    },
    /// Live event stream (SSE, Ctrl+C to stop)
    Stream {
        /// Filter by domain
        #[arg(long)]
        domain: Option<String>,
        /// Filter by severity
        #[arg(long)]
        severity: Option<String>,
    },
    /// Event bus statistics
    Stats,
    /// Show a single event
    Show {
        /// Event ID
        id: String,
    },
}

// -- Alert subcommands --
#[derive(Subcommand)]
enum AlertCommands {
    /// List active alerts
    List,
    /// Alert rule management
    #[command(subcommand)]
    Rules(AlertRuleCommands),
    /// Acknowledge an alert
    Ack {
        /// Alert name
        name: String,
    },
    /// Silence an alert
    Silence {
        /// Alert name
        name: String,
        /// Silence duration in seconds
        #[arg(long, default_value = "3600")]
        duration: u64,
        /// Reason for silencing
        #[arg(long)]
        reason: Option<String>,
    },
    /// List active silence windows
    Silences,
    /// Alert summary counts by severity
    Summary,
}

#[derive(Subcommand)]
enum AlertRuleCommands {
    /// List alert rules
    List,
    /// Add alert rule from JSON/TOML file
    Add {
        /// Path to rule file
        file: PathBuf,
    },
    /// Remove an alert rule
    Remove {
        /// Rule name
        name: String,
    },
}

// -- SLA subcommands --
#[derive(Subcommand)]
enum SlaCommands {
    /// All SLA statuses with compliance bars
    Status,
    /// Detailed SLA status
    Show {
        /// SLA name
        name: String,
    },
    /// SLA report
    Report {
        /// SLA name
        name: String,
        /// Hours of data to include
        #[arg(long, default_value = "24")]
        hours: u32,
    },
    /// Add SLA definition from JSON/TOML file
    Add {
        /// Path to SLA definition file
        file: PathBuf,
    },
}

// -- Capacity subcommands --
#[derive(Subcommand)]
enum CapacityCommands {
    /// Current capacity with utilization bars
    Status,
    /// Resource forecasts
    Forecast,
    /// Identified bottlenecks
    Bottlenecks,
    /// What-if scenario from JSON file
    Whatif {
        /// Path to scenario JSON file
        file: PathBuf,
    },
    /// Right-sizing recommendations
    Rightsizing,
}

// -- Vision subcommands --
#[derive(Subcommand)]
enum VisionCommands {
    /// List available visions
    List,
    /// Show vision details
    Show {
        /// Vision name
        name: String,
    },
    /// Add custom vision from TOML file
    Add {
        /// Path to vision TOML file
        file: PathBuf,
    },
}

// -- Constellation subcommands --
#[derive(Subcommand)]
enum ConstellationCommands {
    /// Multi-swarm overview
    Status,
    /// List known swarms
    Swarms,
    /// List membranes
    Membranes,
    /// Membrane detail and crossing stats
    Membrane {
        /// Membrane ID
        id: String,
    },
    /// List treaties
    Treaties,
    /// Active lending sessions
    Lending,
}

// -- Audit subcommands --
#[derive(Subcommand)]
enum AuditCommands {
    /// Show audit log
    Log {
        /// Filter by actor
        #[arg(long)]
        actor: Option<String>,
        /// Filter by action
        #[arg(long)]
        action: Option<String>,
        /// Maximum entries to show
        #[arg(long, default_value = "50")]
        limit: usize,
    },
    /// Verify audit chain integrity
    Verify,
    /// Export audit log
    Export {
        /// Export format (json, csv)
        #[arg(long, default_value = "json")]
        format: String,
    },
}

// ============================================================================
// SwarmCtlClient -- inline HTTP client
// ============================================================================

/// Lightweight HTTP client for communicating with the swarm API.
struct SwarmCtlClient {
    base_url: String,
    token: Option<String>,
    http: reqwest::Client,
    output: OutputFormat,
    verbose: bool,
    quiet: bool,
    color: bool,
    term: Term,
}

/// Unified error type for the CLI.
#[derive(Debug)]
enum CtlError {
    Http(reqwest::Error),
    Api { status: StatusCode, message: String },
    Io(io::Error),
    Json(serde_json::Error),
    Other(String),
}

impl std::fmt::Display for CtlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CtlError::Http(e) => write!(f, "HTTP error: {}", e),
            CtlError::Api { status, message } => {
                write!(f, "API error ({}): {}", status, message)
            }
            CtlError::Io(e) => write!(f, "I/O error: {}", e),
            CtlError::Json(e) => write!(f, "JSON error: {}", e),
            CtlError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for CtlError {}

impl From<reqwest::Error> for CtlError {
    fn from(e: reqwest::Error) -> Self {
        CtlError::Http(e)
    }
}

impl From<io::Error> for CtlError {
    fn from(e: io::Error) -> Self {
        CtlError::Io(e)
    }
}

impl From<serde_json::Error> for CtlError {
    fn from(e: serde_json::Error) -> Self {
        CtlError::Json(e)
    }
}

type CtlResult<T> = Result<T, CtlError>;

impl SwarmCtlClient {
    /// Create a new client with the resolved configuration.
    fn new(
        base_url: String,
        token: Option<String>,
        output: OutputFormat,
        verbose: bool,
        quiet: bool,
        color: bool,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(USER_AGENT)
            .build()
            .expect("failed to build HTTP client");

        Self {
            base_url,
            token,
            http,
            output,
            verbose,
            quiet,
            color,
            term: Term::stdout(),
        }
    }

    /// Build a full URL from a path.
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }

    /// Perform a GET request and return the response body as JSON [`Value`].
    async fn get(&self, path: &str) -> CtlResult<Value> {
        let url = self.url(path);
        if self.verbose {
            eprintln!("[DEBUG] GET {}", url);
        }

        let mut req = self.http.get(&url);
        if let Some(ref tok) = self.token {
            req = req.bearer_auth(tok);
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;

        if self.verbose {
            eprintln!("[DEBUG] Response {}: {} bytes", status, body.len());
        }

        if !status.is_success() {
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or(body);
            return Err(CtlError::Api { status, message });
        }

        let value: Value = serde_json::from_str(&body)?;
        Ok(value)
    }

    /// Perform a GET request and return the raw response text.
    async fn get_raw(&self, path: &str) -> CtlResult<String> {
        let url = self.url(path);
        if self.verbose {
            eprintln!("[DEBUG] GET {}", url);
        }

        let mut req = self.http.get(&url);
        if let Some(ref tok) = self.token {
            req = req.bearer_auth(tok);
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await?;

        if !status.is_success() {
            let message = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or(body);
            return Err(CtlError::Api { status, message });
        }

        Ok(body)
    }

    /// Perform a POST request with a JSON body.
    async fn post(&self, path: &str, body: &Value) -> CtlResult<Value> {
        let url = self.url(path);
        if self.verbose {
            eprintln!("[DEBUG] POST {}", url);
        }

        let mut req = self.http.post(&url).json(body);
        if let Some(ref tok) = self.token {
            req = req.bearer_auth(tok);
        }

        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if self.verbose {
            eprintln!("[DEBUG] Response {}: {} bytes", status, text.len());
        }

        if !status.is_success() {
            let message = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or(text);
            return Err(CtlError::Api { status, message });
        }

        if text.is_empty() {
            Ok(Value::Null)
        } else {
            let value: Value = serde_json::from_str(&text)?;
            Ok(value)
        }
    }

    /// Perform a PUT request with a JSON body.
    async fn put(&self, path: &str, body: &Value) -> CtlResult<Value> {
        let url = self.url(path);
        if self.verbose {
            eprintln!("[DEBUG] PUT {}", url);
        }

        let mut req = self.http.put(&url).json(body);
        if let Some(ref tok) = self.token {
            req = req.bearer_auth(tok);
        }

        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            let message = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or(text);
            return Err(CtlError::Api { status, message });
        }

        if text.is_empty() {
            Ok(Value::Null)
        } else {
            let value: Value = serde_json::from_str(&text)?;
            Ok(value)
        }
    }

    /// Perform a DELETE request.
    async fn delete(&self, path: &str) -> CtlResult<Value> {
        let url = self.url(path);
        if self.verbose {
            eprintln!("[DEBUG] DELETE {}", url);
        }

        let mut req = self.http.delete(&url);
        if let Some(ref tok) = self.token {
            req = req.bearer_auth(tok);
        }

        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            let message = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or(text);
            return Err(CtlError::Api { status, message });
        }

        if text.is_empty() {
            Ok(Value::Null)
        } else {
            let value: Value = serde_json::from_str(&text)?;
            Ok(value)
        }
    }

    // -- Output helpers --

    /// Print a JSON value in the configured output format.
    fn print_value(&self, value: &Value) {
        match self.output {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(value).unwrap_or_default());
            }
            OutputFormat::Yaml => {
                println!("{}", serde_yaml::to_string(value).unwrap_or_default());
            }
            OutputFormat::Csv => {
                self.print_value_csv(value);
            }
            OutputFormat::Text => {
                // Text output is handled by individual command handlers.
                println!("{}", serde_json::to_string_pretty(value).unwrap_or_default());
            }
        }
    }

    /// Print a JSON value as CSV (best-effort for arrays of objects).
    fn print_value_csv(&self, value: &Value) {
        match value {
            Value::Array(arr) => {
                if arr.is_empty() {
                    return;
                }
                // Collect headers from the first object.
                if let Some(Value::Object(first)) = arr.first() {
                    let headers: Vec<&String> = first.keys().collect();
                    println!("{}", headers.iter().map(|h| h.as_str()).collect::<Vec<_>>().join(","));
                    for item in arr {
                        if let Value::Object(obj) = item {
                            let row: Vec<String> = headers
                                .iter()
                                .map(|h| {
                                    obj.get(*h)
                                        .map(|v| match v {
                                            Value::String(s) => {
                                                if s.contains(',') || s.contains('"') {
                                                    format!("\"{}\"", s.replace('"', "\"\""))
                                                } else {
                                                    s.clone()
                                                }
                                            }
                                            other => other.to_string(),
                                        })
                                        .unwrap_or_default()
                                })
                                .collect();
                            println!("{}", row.join(","));
                        }
                    }
                }
            }
            _ => {
                println!("{}", serde_json::to_string(value).unwrap_or_default());
            }
        }
    }

    /// Print informational text (suppressed in quiet mode).
    fn info(&self, msg: &str) {
        if !self.quiet {
            if self.color {
                eprintln!("{}", style(msg).dim());
            } else {
                eprintln!("{}", msg);
            }
        }
    }

    /// Print a success message.
    fn success(&self, msg: &str) {
        if !self.quiet {
            if self.color {
                println!("{} {}", style("[ok]").green().bold(), msg);
            } else {
                println!("[ok] {}", msg);
            }
        }
    }

    /// Print a warning message.
    fn warn(&self, msg: &str) {
        if self.color {
            eprintln!("{} {}", style("[!]").yellow().bold(), msg);
        } else {
            eprintln!("[!] {}", msg);
        }
    }

    /// Print an error message.
    fn error(&self, msg: &str) {
        if self.color {
            eprintln!("{} {}", style("[x]").red().bold(), msg);
        } else {
            eprintln!("[x] {}", msg);
        }
    }

    /// Ask for interactive confirmation. Returns false in quiet mode.
    fn confirm(&self, message: &str) -> bool {
        if self.quiet {
            return false;
        }
        Confirm::new()
            .with_prompt(message)
            .default(false)
            .interact()
            .unwrap_or(false)
    }
}

// ============================================================================
// Formatting helpers
// ============================================================================

/// Format a duration in human-readable form (e.g., "2h 14m", "47s").
fn format_duration_human(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}

/// Format bytes in human-readable form (e.g., "1.2 GB", "847 KB").
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.0} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a percentage as a colored bar: [########..] 80%
fn format_bar(value: f64, width: usize, color: bool) -> String {
    let clamped = value.clamp(0.0, 100.0);
    let filled = ((clamped / 100.0) * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    let bar = format!(
        "[{}{}] {:.0}%",
        "#".repeat(filled),
        ".".repeat(empty),
        clamped
    );
    if !color {
        return bar;
    }
    if clamped >= 90.0 {
        style(bar).red().to_string()
    } else if clamped >= 70.0 {
        style(bar).yellow().to_string()
    } else {
        style(bar).green().to_string()
    }
}

/// Format a severity icon with color.
fn severity_icon(severity: &str, color: bool) -> String {
    let s = severity.to_lowercase();
    if !color {
        return match s.as_str() {
            "critical" | "error" => "[!!]".to_string(),
            "warning" | "warn" => "[!]".to_string(),
            "info" => "[i]".to_string(),
            _ => "[.]".to_string(),
        };
    }
    match s.as_str() {
        "critical" | "error" => style("[!!]").red().bold().to_string(),
        "warning" | "warn" => style("[!]").yellow().bold().to_string(),
        "info" => style("[i]").blue().to_string(),
        _ => style("[.]").dim().to_string(),
    }
}

/// Format a node status string with color.
fn format_status(status: &str, color: bool) -> String {
    if !color {
        return status.to_string();
    }
    match status.to_lowercase().as_str() {
        "alive" | "active" | "healthy" | "compliant" | "running" => {
            style(status).green().to_string()
        }
        "suspect" | "draining" | "warning" | "updating" | "pending" | "cordoned" => {
            style(status).yellow().to_string()
        }
        "dead" | "quarantined" | "error" | "failed" | "breached" => {
            style(status).red().to_string()
        }
        _ => style(status).dim().to_string(),
    }
}

/// Truncate a string to a maximum length, adding "..." if truncated.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else if max <= 3 {
        s[..max].to_string()
    } else {
        format!("{}...", &s[..max - 3])
    }
}

/// Pad a string to a minimum width (left-aligned).
fn pad_right(s: &str, width: usize) -> String {
    if s.len() >= width {
        s.to_string()
    } else {
        format!("{}{}", s, " ".repeat(width - s.len()))
    }
}

/// Print a table from rows of column values with a header row.
fn print_table(headers: &[&str], rows: &[Vec<String>], color: bool) {
    if rows.is_empty() && headers.is_empty() {
        return;
    }

    // Calculate column widths.
    let num_cols = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < num_cols {
                // Strip ANSI escape codes for width calculation.
                let plain_len = strip_ansi(cell).len();
                if plain_len > widths[i] {
                    widths[i] = plain_len;
                }
            }
        }
    }

    // Print header.
    let header_line: String = headers
        .iter()
        .enumerate()
        .map(|(i, h)| pad_right(h, widths[i]))
        .collect::<Vec<_>>()
        .join("  ");
    if color {
        println!("{}", style(&header_line).bold().underlined());
    } else {
        println!("{}", header_line);
        println!(
            "{}",
            widths
                .iter()
                .map(|w| "-".repeat(*w))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }

    // Print rows.
    for row in rows {
        let line: String = row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                if i < num_cols {
                    let plain_len = strip_ansi(cell).len();
                    let padding = widths[i].saturating_sub(plain_len);
                    format!("{}{}", cell, " ".repeat(padding))
                } else {
                    cell.clone()
                }
            })
            .collect::<Vec<_>>()
            .join("  ");
        println!("{}", line);
    }
}

/// Strip ANSI escape codes from a string (for width calculation).
fn strip_ansi(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut in_escape = false;
    for ch in s.chars() {
        if in_escape {
            if ch.is_ascii_alphabetic() {
                in_escape = false;
            }
        } else if ch == '\x1b' {
            in_escape = true;
        } else {
            result.push(ch);
        }
    }
    result
}

/// Extract a string field from a JSON value.
fn json_str(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("-")
        .to_string()
}

/// Extract an f64 field from a JSON value.
fn json_f64(v: &Value, key: &str) -> f64 {
    v.get(key).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

/// Extract a u64 field from a JSON value.
fn json_u64(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(|v| v.as_u64()).unwrap_or(0)
}

/// Extract an i64 field from a JSON value.
fn json_i64(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(|v| v.as_i64()).unwrap_or(0)
}

/// Extract a bool from a JSON value.
fn json_bool(v: &Value, key: &str) -> bool {
    v.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

/// Parse a relative timestamp (ISO 8601 or seconds) into "Xm ago" style.
fn format_relative_time(timestamp_str: &str) -> String {
    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(timestamp_str) {
        let now = chrono::Utc::now();
        let diff = now.signed_duration_since(ts);
        let secs = diff.num_seconds();
        if secs < 0 {
            format!("in {}", format_duration_human((-secs) as u64))
        } else {
            format!("{} ago", format_duration_human(secs as u64))
        }
    } else {
        timestamp_str.to_string()
    }
}

// ============================================================================
// main()
// ============================================================================

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Resolve config file path.
    let config_path = cli.config.clone().unwrap_or_else(|| {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(DEFAULT_CONFIG_FILENAME)
    });

    // Load config.
    let config = SwarmCtlConfig::load(&config_path);

    // Resolve effective values (CLI flag > env > config > default).
    let base_url = cli
        .url
        .clone()
        .or(config.url)
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    let token = cli.token.clone().or(config.token);

    let output = cli.output.unwrap_or_else(|| {
        config
            .default_output
            .as_deref()
            .map(OutputFormat::from_str_loose)
            .unwrap_or(OutputFormat::Text)
    });

    let color = if cli.no_color {
        false
    } else {
        config.color.unwrap_or(true)
    };

    let client = SwarmCtlClient::new(base_url, token, output, cli.verbose, cli.quiet, color);

    let result = match cli.command {
        Commands::Status => cmd_status(&client).await,
        Commands::Psyche(sub) => cmd_psyche(&client, sub).await,
        Commands::Nodes(sub) => cmd_nodes(&client, sub).await,
        Commands::Jobs(sub) => cmd_jobs(&client, sub).await,
        Commands::Fleet(sub) => cmd_fleet(&client, sub).await,
        Commands::Events(sub) => cmd_events(&client, sub).await,
        Commands::Alerts(sub) => cmd_alerts(&client, sub).await,
        Commands::Sla(sub) => cmd_sla(&client, sub).await,
        Commands::Capacity(sub) => cmd_capacity(&client, sub).await,
        Commands::Visions(sub) => cmd_visions(&client, sub).await,
        Commands::Constellation(sub) => cmd_constellation(&client, sub).await,
        Commands::Audit(sub) => cmd_audit(&client, sub).await,
        Commands::Metrics => cmd_metrics(&client).await,
        Commands::Tui => cmd_tui(&client).await,
        Commands::Completions { shell } => {
            cmd_completions(shell, &client);
            Ok(())
        }
        Commands::Version => {
            cmd_version(&client);
            Ok(())
        }
    };

    if let Err(e) = result {
        client.error(&e.to_string());
        std::process::exit(1);
    }
}

// ============================================================================
// Command: status (dashboard)
// ============================================================================

async fn cmd_status(client: &SwarmCtlClient) -> CtlResult<()> {
    // Gather data from multiple endpoints in parallel.
    let (health_res, nodes_res, jobs_res) = tokio::join!(
        client.get("/api/v1/health"),
        client.get("/api/v1/nodes"),
        client.get("/api/v1/jobs"),
    );

    let health = health_res.unwrap_or(Value::Null);
    let nodes = nodes_res.unwrap_or(Value::Array(vec![]));
    let jobs = jobs_res.unwrap_or(Value::Array(vec![]));

    if client.output != OutputFormat::Text {
        let combined = serde_json::json!({
            "health": health,
            "nodes": nodes,
            "jobs": jobs,
        });
        client.print_value(&combined);
        return Ok(());
    }

    // -- Text output: dashboard --
    if client.color {
        println!(
            "{}",
            style("=== Swarm Dashboard ===").bold().cyan()
        );
    } else {
        println!("=== Swarm Dashboard ===");
    }
    println!();

    // Health summary.
    let status_str = json_str(&health, "status");
    println!(
        "  Health:  {}",
        format_status(&status_str, client.color)
    );
    let uptime = json_u64(&health, "uptime_secs");
    if uptime > 0 {
        println!("  Uptime:  {}", format_duration_human(uptime));
    }
    println!();

    // Node counts.
    let node_arr = nodes.as_array().cloned().unwrap_or_default();
    let total = node_arr.len();
    let alive = node_arr
        .iter()
        .filter(|n| json_str(n, "status").to_lowercase() == "alive")
        .count();
    let suspect = node_arr
        .iter()
        .filter(|n| json_str(n, "status").to_lowercase() == "suspect")
        .count();
    let dead = node_arr
        .iter()
        .filter(|n| json_str(n, "status").to_lowercase() == "dead")
        .count();
    let draining = node_arr
        .iter()
        .filter(|n| json_str(n, "status").to_lowercase() == "draining")
        .count();
    let quarantined = node_arr
        .iter()
        .filter(|n| json_str(n, "status").to_lowercase() == "quarantined")
        .count();

    println!("  Nodes:   {} total", total);
    println!(
        "           {} {} | {} {} | {} {} | {} {} | {} {}",
        alive,
        format_status("alive", client.color),
        suspect,
        format_status("suspect", client.color),
        dead,
        format_status("dead", client.color),
        draining,
        format_status("draining", client.color),
        quarantined,
        format_status("quarantined", client.color),
    );
    println!();

    // Job counts.
    let job_arr = jobs.as_array().cloned().unwrap_or_default();
    let total_jobs = job_arr.len();
    let running_jobs = job_arr
        .iter()
        .filter(|j| {
            let s = json_str(j, "status").to_lowercase();
            s == "running" || s == "executing"
        })
        .count();
    let completed_jobs = job_arr
        .iter()
        .filter(|j| json_str(j, "status").to_lowercase() == "completed")
        .count();
    let failed_jobs = job_arr
        .iter()
        .filter(|j| json_str(j, "status").to_lowercase() == "failed")
        .count();

    println!("  Jobs:    {} total", total_jobs);
    println!(
        "           {} {} | {} {} | {} {}",
        running_jobs,
        format_status("running", client.color),
        completed_jobs,
        format_status("completed", client.color),
        failed_jobs,
        format_status("failed", client.color),
    );
    println!();

    Ok(())
}

// ============================================================================
// Command: psyche
// ============================================================================

async fn cmd_psyche(client: &SwarmCtlClient, sub: PsycheCommands) -> CtlResult<()> {
    match sub {
        PsycheCommands::Status => cmd_psyche_status(client).await,
        PsycheCommands::Facets => cmd_psyche_facets(client).await,
        PsycheCommands::History { hours } => cmd_psyche_history(client, hours).await,
        PsycheCommands::Forecast { minutes } => cmd_psyche_forecast(client, minutes).await,
        PsycheCommands::Archetypes(sub) => cmd_psyche_archetypes(client, sub).await,
    }
}

async fn cmd_psyche_status(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/psyche/status").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Swarm Psyche ===").bold().cyan());
    } else {
        println!("=== Swarm Psyche ===");
    }
    println!();

    // Archetype.
    let archetype = json_str(&data, "archetype");
    if archetype != "-" {
        println!(
            "  Archetype: {}",
            if client.color {
                style(&archetype).magenta().bold().to_string()
            } else {
                archetype.clone()
            }
        );
        println!();
    }

    // Facets.
    let facets = data
        .get("facets")
        .and_then(|f| f.as_object())
        .cloned()
        .unwrap_or_default();

    let facet_names = [
        "exertion",
        "vitality",
        "resilience",
        "cohesion",
        "curiosity",
        "anxiety",
        "focus",
        "satisfaction",
    ];

    for name in &facet_names {
        if let Some(val) = facets.get(*name).and_then(|v| v.as_f64()) {
            let label = format!("{:<14}", capitalize(name));
            let bar = format_bar(val, 20, client.color);
            let descriptor = psyche_descriptor(name, val);
            println!("  {} {} {}", label, bar, descriptor);
        }
    }

    // Also show any extra facets not in the standard list.
    for (key, val) in &facets {
        if !facet_names.contains(&key.as_str()) {
            if let Some(v) = val.as_f64() {
                let label = format!("{:<14}", capitalize(key));
                let bar = format_bar(v, 20, client.color);
                println!("  {} {}", label, bar);
            }
        }
    }

    println!();
    Ok(())
}

/// Return a human-readable descriptor for a psyche facet value.
fn psyche_descriptor(facet: &str, value: f64) -> String {
    let v = value as u32;
    match facet {
        "exertion" => match v {
            0..=20 => "Idle".to_string(),
            21..=40 => "Light".to_string(),
            41..=60 => "Moderate".to_string(),
            61..=80 => "Heavy".to_string(),
            _ => "Maxed".to_string(),
        },
        "vitality" => match v {
            0..=20 => "Critical".to_string(),
            21..=40 => "Declining".to_string(),
            41..=60 => "Stable".to_string(),
            61..=80 => "Healthy".to_string(),
            _ => "Thriving".to_string(),
        },
        "resilience" => match v {
            0..=20 => "Fragile".to_string(),
            21..=40 => "Brittle".to_string(),
            41..=60 => "Moderate".to_string(),
            61..=80 => "Robust".to_string(),
            _ => "Antifragile".to_string(),
        },
        "cohesion" => match v {
            0..=20 => "Fractured".to_string(),
            21..=40 => "Loose".to_string(),
            41..=60 => "Moderate".to_string(),
            61..=80 => "Tight".to_string(),
            _ => "Unified".to_string(),
        },
        "curiosity" => match v {
            0..=20 => "Dormant".to_string(),
            21..=40 => "Passive".to_string(),
            41..=60 => "Interested".to_string(),
            61..=80 => "Exploring".to_string(),
            _ => "Discovering".to_string(),
        },
        "anxiety" => match v {
            0..=20 => "Calm".to_string(),
            21..=40 => "Watchful".to_string(),
            41..=60 => "Concerned".to_string(),
            61..=80 => "Anxious".to_string(),
            _ => "Panicking".to_string(),
        },
        "focus" => match v {
            0..=20 => "Scattered".to_string(),
            21..=40 => "Distracted".to_string(),
            41..=60 => "Moderate".to_string(),
            61..=80 => "Focused".to_string(),
            _ => "Laser".to_string(),
        },
        "satisfaction" => match v {
            0..=20 => "Frustrated".to_string(),
            21..=40 => "Dissatisfied".to_string(),
            41..=60 => "Neutral".to_string(),
            61..=80 => "Content".to_string(),
            _ => "Delighted".to_string(),
        },
        _ => String::new(),
    }
}

/// Capitalize the first letter of a string.
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => format!("{}{}", c.to_uppercase(), chars.as_str()),
        None => String::new(),
    }
}

async fn cmd_psyche_facets(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/psyche/facets").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Psyche Facet Breakdown ===").bold().cyan());
    } else {
        println!("=== Psyche Facet Breakdown ===");
    }
    println!();

    let facets = data.as_array().cloned().unwrap_or_else(|| {
        // If it's an object, convert to array of facet entries.
        if let Some(obj) = data.as_object() {
            obj.iter()
                .map(|(k, v)| {
                    serde_json::json!({
                        "name": k,
                        "value": v.get("value").unwrap_or(v),
                        "trend": v.get("trend").unwrap_or(&Value::Null),
                        "drivers": v.get("drivers").unwrap_or(&Value::Array(vec![])),
                    })
                })
                .collect()
        } else {
            vec![]
        }
    });

    for facet in &facets {
        let name = json_str(facet, "name");
        let value = json_f64(facet, "value");
        let trend = json_str(facet, "trend");
        let drivers = facet
            .get("drivers")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();

        println!(
            "  {} ({})",
            if client.color {
                style(capitalize(&name)).bold().to_string()
            } else {
                capitalize(&name)
            },
            format_bar(value, 15, client.color)
        );

        if trend != "-" {
            let trend_icon = match trend.as_str() {
                "rising" | "up" => "^",
                "falling" | "down" => "v",
                "stable" | "flat" => "=",
                _ => "?",
            };
            println!("    Trend: {} {}", trend_icon, trend);
        }

        if !drivers.is_empty() {
            println!("    Drivers:");
            for driver in &drivers {
                let driver_name = json_str(driver, "name");
                let driver_impact = json_f64(driver, "impact");
                let sign = if driver_impact >= 0.0 { "+" } else { "" };
                println!("      - {} ({}{:.1})", driver_name, sign, driver_impact);
            }
        }
        println!();
    }

    Ok(())
}

async fn cmd_psyche_history(client: &SwarmCtlClient, hours: u32) -> CtlResult<()> {
    let data = client
        .get(&format!("/api/v1/psyche/history?hours={}", hours))
        .await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!(
            "{}",
            style(format!("=== Psyche History (last {}h) ===", hours))
                .bold()
                .cyan()
        );
    } else {
        println!("=== Psyche History (last {}h) ===", hours);
    }
    println!();

    // Expect an object mapping facet names to arrays of data points.
    let facets = data.as_object().cloned().unwrap_or_default();

    for (facet_name, points) in &facets {
        let arr = points.as_array().cloned().unwrap_or_default();
        if arr.is_empty() {
            continue;
        }

        // Build sparkline from values.
        let values: Vec<f64> = arr
            .iter()
            .filter_map(|p| p.get("value").and_then(|v| v.as_f64()))
            .collect();

        let sparkline = build_sparkline(&values, 40);
        let latest = values.last().copied().unwrap_or(0.0);

        println!(
            "  {:<14} {} ({:.0}%)",
            capitalize(facet_name),
            sparkline,
            latest
        );
    }

    println!();
    println!("  Sparkline legend: _ . - ~ = * # @ (low to high)");
    println!();

    Ok(())
}

/// Build an ASCII sparkline from a series of values.
fn build_sparkline(values: &[f64], width: usize) -> String {
    if values.is_empty() {
        return " ".repeat(width);
    }

    let chars = ['_', '.', '-', '~', '=', '*', '#', '@'];
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let range = if (max - min).abs() < 0.001 {
        1.0
    } else {
        max - min
    };

    // Downsample or pad to fit width.
    let sampled = if values.len() > width {
        let step = values.len() as f64 / width as f64;
        (0..width)
            .map(|i| {
                let idx = (i as f64 * step) as usize;
                values[idx.min(values.len() - 1)]
            })
            .collect::<Vec<_>>()
    } else {
        values.to_vec()
    };

    sampled
        .iter()
        .map(|v| {
            let normalized = ((v - min) / range * (chars.len() - 1) as f64).round() as usize;
            chars[normalized.min(chars.len() - 1)]
        })
        .collect()
}

async fn cmd_psyche_forecast(client: &SwarmCtlClient, minutes: u32) -> CtlResult<()> {
    let data = client
        .get(&format!("/api/v1/psyche/forecast?minutes={}", minutes))
        .await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!(
            "{}",
            style(format!("=== Psyche Forecast (next {}m) ===", minutes))
                .bold()
                .cyan()
        );
    } else {
        println!("=== Psyche Forecast (next {}m) ===", minutes);
    }
    println!();

    let predictions = data.as_array().cloned().unwrap_or_else(|| {
        if let Some(obj) = data.as_object() {
            obj.iter()
                .map(|(k, v)| {
                    serde_json::json!({
                        "facet": k,
                        "current": v.get("current").unwrap_or(&Value::Null),
                        "predicted": v.get("predicted").unwrap_or(&Value::Null),
                        "confidence": v.get("confidence").unwrap_or(&Value::Null),
                    })
                })
                .collect()
        } else {
            vec![]
        }
    });

    let headers = ["Facet", "Current", "Predicted", "Change", "Confidence"];
    let rows: Vec<Vec<String>> = predictions
        .iter()
        .map(|p| {
            let facet = json_str(p, "facet");
            let current = json_f64(p, "current");
            let predicted = json_f64(p, "predicted");
            let change = predicted - current;
            let confidence = json_f64(p, "confidence");
            let change_str = if change >= 0.0 {
                format!("+{:.1}", change)
            } else {
                format!("{:.1}", change)
            };
            vec![
                capitalize(&facet),
                format!("{:.0}%", current),
                format!("{:.0}%", predicted),
                if client.color {
                    if change > 5.0 {
                        style(change_str).green().to_string()
                    } else if change < -5.0 {
                        style(change_str).red().to_string()
                    } else {
                        style(change_str).dim().to_string()
                    }
                } else {
                    change_str
                },
                format!("{:.0}%", confidence),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_psyche_archetypes(
    client: &SwarmCtlClient,
    sub: ArchetypeCommands,
) -> CtlResult<()> {
    match sub {
        ArchetypeCommands::List => {
            let data = client.get("/api/v1/psyche/archetypes").await?;

            if client.output != OutputFormat::Text {
                client.print_value(&data);
                return Ok(());
            }

            if client.color {
                println!("{}", style("=== Archetypes ===").bold().cyan());
            } else {
                println!("=== Archetypes ===");
            }
            println!();

            let archetypes = data.as_array().cloned().unwrap_or_default();
            let headers = ["Name", "Active", "Match %", "Description"];
            let rows: Vec<Vec<String>> = archetypes
                .iter()
                .map(|a| {
                    let name = json_str(a, "name");
                    let active = json_bool(a, "active");
                    let match_pct = json_f64(a, "match_pct");
                    let desc = json_str(a, "description");
                    vec![
                        name,
                        if active {
                            format_status("active", client.color)
                        } else {
                            format_status("inactive", client.color)
                        },
                        format!("{:.0}%", match_pct),
                        truncate(&desc, 50),
                    ]
                })
                .collect();

            print_table(&headers, &rows, client.color);
            println!();
            Ok(())
        }
        ArchetypeCommands::Add { file } => {
            let contents = std::fs::read_to_string(&file).map_err(|e| {
                CtlError::Other(format!("Failed to read {}: {}", file.display(), e))
            })?;
            let value: Value = toml::from_str(&contents).map_err(|e| {
                CtlError::Other(format!("Failed to parse TOML: {}", e))
            })?;
            let result = client.post("/api/v1/psyche/archetypes", &value).await?;
            client.success("Archetype added successfully");
            if client.output != OutputFormat::Text {
                client.print_value(&result);
            }
            Ok(())
        }
        ArchetypeCommands::Remove { name } => {
            if !client.confirm(&format!("Remove archetype '{}'?", name)) {
                client.info("Cancelled.");
                return Ok(());
            }
            client
                .delete(&format!("/api/v1/psyche/archetypes/{}", name))
                .await?;
            client.success(&format!("Archetype '{}' removed", name));
            Ok(())
        }
    }
}

// ============================================================================
// Command: nodes
// ============================================================================

async fn cmd_nodes(client: &SwarmCtlClient, sub: NodeCommands) -> CtlResult<()> {
    match sub {
        NodeCommands::List { status } => cmd_nodes_list(client, status).await,
        NodeCommands::Show { id } => cmd_nodes_show(client, &id).await,
        NodeCommands::Drain {
            id,
            timeout,
            reason,
        } => cmd_nodes_drain(client, &id, timeout, reason).await,
        NodeCommands::Cordon { id, reason } => cmd_nodes_cordon(client, &id, reason).await,
        NodeCommands::Uncordon { id } => cmd_nodes_uncordon(client, &id).await,
        NodeCommands::Quarantine { id, reason } => {
            cmd_nodes_quarantine(client, &id, reason).await
        }
        NodeCommands::Unquarantine { id } => cmd_nodes_unquarantine(client, &id).await,
        NodeCommands::Tags(sub) => cmd_nodes_tags(client, sub).await,
        NodeCommands::Health { id } => cmd_nodes_health(client, id).await,
    }
}

async fn cmd_nodes_list(client: &SwarmCtlClient, status_filter: Option<String>) -> CtlResult<()> {
    let mut path = "/api/v1/nodes".to_string();
    if let Some(ref s) = status_filter {
        path = format!("{}?status={}", path, s);
    }

    let data = client.get(&path).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let nodes = data.as_array().cloned().unwrap_or_default();

    if nodes.is_empty() {
        client.info("No nodes found.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Nodes ===").bold().cyan());
    } else {
        println!("=== Nodes ===");
    }
    println!();

    let headers = ["ID", "Status", "Load", "Traits", "Region", "Uptime"];
    let rows: Vec<Vec<String>> = nodes
        .iter()
        .map(|n| {
            let id = json_str(n, "id");
            let node_id = truncate(&id, 16);
            let status = json_str(n, "status");
            let load = json_f64(n, "load");
            let traits_val = n.get("traits");
            let traits_str = match traits_val {
                Some(Value::Array(arr)) => arr
                    .iter()
                    .filter_map(|t| t.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                Some(Value::String(s)) => s.clone(),
                _ => "-".to_string(),
            };
            let region = json_str(n, "region");
            let uptime = json_u64(n, "uptime_secs");
            let uptime_str = if uptime > 0 {
                format_duration_human(uptime)
            } else {
                // Try last_seen as fallback.
                let ls = json_str(n, "last_seen");
                if ls != "-" {
                    format_relative_time(&ls)
                } else {
                    "-".to_string()
                }
            };

            vec![
                node_id,
                format_status(&status, client.color),
                format!("{:.0}%", load * 100.0),
                truncate(&traits_str, 30),
                if region == "-" {
                    "-".to_string()
                } else {
                    region
                },
                uptime_str,
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();
    println!("  Total: {} nodes", nodes.len());
    println!();

    Ok(())
}

async fn cmd_nodes_show(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    let data = client.get(&format!("/api/v1/nodes/{}", id)).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Node Detail ===").bold().cyan());
    } else {
        println!("=== Node Detail ===");
    }
    println!();

    let fields: &[(&str, &str)] = &[
        ("ID", "id"),
        ("Status", "status"),
        ("Address", "address"),
        ("Region", "region"),
        ("Node Type", "node_type"),
        ("Version", "version"),
    ];

    for (label, key) in fields {
        let val = json_str(&data, key);
        if val != "-" {
            if *key == "status" {
                println!("  {:<16} {}", label, format_status(&val, client.color));
            } else {
                println!("  {:<16} {}", label, val);
            }
        }
    }

    // Load.
    let load = json_f64(&data, "load");
    println!(
        "  {:<16} {}",
        "Load",
        format_bar(load * 100.0, 15, client.color)
    );

    // Resources.
    if let Some(resources) = data.get("resources").and_then(|r| r.as_object()) {
        println!();
        println!("  Resources:");
        if let Some(cpu) = resources.get("cpu_cores").and_then(|v| v.as_u64()) {
            println!("    CPU:     {} cores", cpu);
        }
        if let Some(mem) = resources.get("memory_mb").and_then(|v| v.as_u64()) {
            println!("    Memory:  {}", format_bytes(mem * 1024 * 1024));
        }
        if let Some(disk) = resources.get("disk_mb").and_then(|v| v.as_u64()) {
            println!("    Disk:    {}", format_bytes(disk * 1024 * 1024));
        }
    }

    // Traits.
    if let Some(Value::Array(traits)) = data.get("traits") {
        println!();
        println!(
            "  Traits:        {}",
            traits
                .iter()
                .filter_map(|t| t.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Tags.
    if let Some(Value::Object(tags)) = data.get("tags") {
        if !tags.is_empty() {
            println!();
            println!("  Tags:");
            for (k, v) in tags {
                println!("    {}={}", k, v);
            }
        }
    }

    // Uptime.
    let uptime = json_u64(&data, "uptime_secs");
    if uptime > 0 {
        println!();
        println!("  Uptime:        {}", format_duration_human(uptime));
    }

    // Last seen.
    let last_seen = json_str(&data, "last_seen");
    if last_seen != "-" {
        println!(
            "  Last Seen:     {}",
            format_relative_time(&last_seen)
        );
    }

    println!();
    Ok(())
}

async fn cmd_nodes_drain(
    client: &SwarmCtlClient,
    id: &str,
    timeout: u64,
    reason: Option<String>,
) -> CtlResult<()> {
    let reason_str = reason.as_deref().unwrap_or("operator-initiated drain");

    if client.color {
        println!();
        println!(
            "  {} You are about to drain node {}.",
            style("WARNING:").yellow().bold(),
            style(id).bold()
        );
    } else {
        println!();
        println!("  WARNING: You are about to drain node {}.", id);
    }
    println!("  Reason: \"{}\"", reason_str);
    println!("  Timeout: {}s", timeout);
    println!(
        "  This will stop accepting new work and wait for current work to finish."
    );
    println!();

    if !client.confirm("  Continue?") {
        client.info("Cancelled.");
        return Ok(());
    }

    let body = serde_json::json!({
        "command": "drain",
        "node_id": id,
        "timeout_secs": timeout,
        "reason": reason_str,
    });

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .expect("valid template"),
    );
    pb.set_message(format!("Draining node {}...", id));
    pb.enable_steady_tick(Duration::from_millis(100));

    let result = client.post("/api/v1/fleet/command", &body).await;
    pb.finish_and_clear();

    match result {
        Ok(_) => client.success(&format!("Node {} is now draining", id)),
        Err(e) => return Err(e),
    }

    Ok(())
}

async fn cmd_nodes_cordon(
    client: &SwarmCtlClient,
    id: &str,
    reason: Option<String>,
) -> CtlResult<()> {
    let reason_str = reason.as_deref().unwrap_or("operator-initiated cordon");

    println!();
    println!("  Cordoning node {}", id);
    println!("  Reason: \"{}\"", reason_str);
    println!("  This will stop the node from accepting new work.");
    println!();

    if !client.confirm("  Continue?") {
        client.info("Cancelled.");
        return Ok(());
    }

    let body = serde_json::json!({
        "command": "cordon",
        "node_id": id,
        "reason": reason_str,
    });

    client.post("/api/v1/fleet/command", &body).await?;
    client.success(&format!("Node {} is now cordoned", id));
    Ok(())
}

async fn cmd_nodes_uncordon(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    let body = serde_json::json!({
        "command": "uncordon",
        "node_id": id,
    });

    client.post("/api/v1/fleet/command", &body).await?;
    client.success(&format!("Node {} uncordoned", id));
    Ok(())
}

async fn cmd_nodes_quarantine(
    client: &SwarmCtlClient,
    id: &str,
    reason: Option<String>,
) -> CtlResult<()> {
    let reason_str = reason
        .as_deref()
        .unwrap_or("operator-initiated quarantine");

    if client.color {
        println!();
        println!(
            "  {} You are about to quarantine node {}.",
            style("WARNING:").red().bold(),
            style(id).bold()
        );
    } else {
        println!();
        println!("  WARNING: You are about to quarantine node {}.", id);
    }
    println!("  Reason: \"{}\"", reason_str);
    println!("  This will immediately stop all work on this node.");
    println!();

    if !client.confirm("  Continue?") {
        client.info("Cancelled.");
        return Ok(());
    }

    let body = serde_json::json!({
        "command": "quarantine",
        "node_id": id,
        "reason": reason_str,
    });

    client.post("/api/v1/fleet/command", &body).await?;
    client.success(&format!("Node {} is now quarantined", id));
    Ok(())
}

async fn cmd_nodes_unquarantine(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    let body = serde_json::json!({
        "command": "unquarantine",
        "node_id": id,
    });

    client.post("/api/v1/fleet/command", &body).await?;
    client.success(&format!("Node {} unquarantined", id));
    Ok(())
}

async fn cmd_nodes_tags(client: &SwarmCtlClient, sub: TagCommands) -> CtlResult<()> {
    match sub {
        TagCommands::Show { id } => {
            let data = client
                .get(&format!("/api/v1/fleet/nodes/{}/tags", id))
                .await?;

            if client.output != OutputFormat::Text {
                client.print_value(&data);
                return Ok(());
            }

            let tags = data.as_object().cloned().unwrap_or_default();
            if tags.is_empty() {
                client.info(&format!("No tags on node {}", id));
                return Ok(());
            }

            println!("Tags for node {}:", id);
            for (k, v) in &tags {
                println!("  {} = {}", k, v);
            }
            println!();
            Ok(())
        }
        TagCommands::Set { id, tag } => {
            let parts: Vec<&str> = tag.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(CtlError::Other(
                    "Tag must be in key=value format".to_string(),
                ));
            }
            let body = serde_json::json!({
                "key": parts[0],
                "value": parts[1],
            });
            client
                .post(&format!("/api/v1/fleet/nodes/{}/tags", id), &body)
                .await?;
            client.success(&format!("Tag set on node {}: {}", id, tag));
            Ok(())
        }
    }
}

async fn cmd_nodes_health(client: &SwarmCtlClient, id: Option<String>) -> CtlResult<()> {
    let path = match &id {
        Some(node_id) => format!("/api/v1/health/nodes/{}", node_id),
        None => "/api/v1/health/nodes".to_string(),
    };

    let data = client.get(&path).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Health Check Results ===").bold().cyan());
    } else {
        println!("=== Health Check Results ===");
    }
    println!();

    let checks = data.as_array().cloned().unwrap_or_else(|| {
        // Single node detail -- wrap in array.
        if data.is_object() {
            vec![data.clone()]
        } else {
            vec![]
        }
    });

    if checks.is_empty() {
        client.info("No health check data available.");
        return Ok(());
    }

    let headers = ["Node", "Probe", "Status", "Latency", "Last Check"];
    let rows: Vec<Vec<String>> = checks
        .iter()
        .flat_map(|entry| {
            let node_id = json_str(entry, "node_id");
            let probes = entry
                .get("probes")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default();

            if probes.is_empty() {
                // Entry might be a flat probe result itself.
                let probe_name = json_str(entry, "probe");
                let probe_status = json_str(entry, "status");
                let latency = json_f64(entry, "latency_ms");
                let last_check = json_str(entry, "last_check");
                vec![vec![
                    truncate(&node_id, 16),
                    probe_name,
                    format_status(&probe_status, client.color),
                    format!("{:.1}ms", latency),
                    if last_check != "-" {
                        format_relative_time(&last_check)
                    } else {
                        "-".to_string()
                    },
                ]]
            } else {
                probes
                    .iter()
                    .map(|probe| {
                        let probe_name = json_str(probe, "name");
                        let probe_status = json_str(probe, "status");
                        let latency = json_f64(probe, "latency_ms");
                        let last_check = json_str(probe, "last_check");
                        vec![
                            truncate(&node_id, 16),
                            probe_name,
                            format_status(&probe_status, client.color),
                            format!("{:.1}ms", latency),
                            if last_check != "-" {
                                format_relative_time(&last_check)
                            } else {
                                "-".to_string()
                            },
                        ]
                    })
                    .collect()
            }
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

// ============================================================================
// Command: jobs
// ============================================================================

async fn cmd_jobs(client: &SwarmCtlClient, sub: JobCommands) -> CtlResult<()> {
    match sub {
        JobCommands::List { status } => cmd_jobs_list(client, status).await,
        JobCommands::Show { id } => cmd_jobs_show(client, &id).await,
        JobCommands::Diagnose { id } => cmd_jobs_diagnose(client, &id).await,
        JobCommands::Cancel { id } => cmd_jobs_cancel(client, &id).await,
    }
}

async fn cmd_jobs_list(client: &SwarmCtlClient, status_filter: Option<String>) -> CtlResult<()> {
    let mut path = "/api/v1/jobs".to_string();
    if let Some(ref s) = status_filter {
        path = format!("{}?status={}", path, s);
    }

    // The /api/v1/jobs endpoint might not support listing; try as-is.
    let data = client.get(&path).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let jobs = data.as_array().cloned().unwrap_or_default();

    if jobs.is_empty() {
        client.info("No jobs found.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Jobs ===").bold().cyan());
    } else {
        println!("=== Jobs ===");
    }
    println!();

    let headers = ["ID", "Name", "Status", "Progress", "Submitter", "Created"];
    let rows: Vec<Vec<String>> = jobs
        .iter()
        .map(|j| {
            let id = json_str(j, "id");
            let name = json_str(j, "name");
            let status = json_str(j, "status");
            let progress = json_f64(j, "progress");
            let submitter = json_str(j, "submitter");
            let created = json_str(j, "created_at");

            vec![
                truncate(&id, 16),
                truncate(&name, 24),
                format_status(&status, client.color),
                format_bar(progress, 10, client.color),
                truncate(&submitter, 16),
                if created != "-" {
                    format_relative_time(&created)
                } else {
                    "-".to_string()
                },
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();
    println!("  Total: {} jobs", jobs.len());
    println!();

    Ok(())
}

async fn cmd_jobs_show(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    let data = client.get(&format!("/api/v1/jobs/{}", id)).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Job Detail ===").bold().cyan());
    } else {
        println!("=== Job Detail ===");
    }
    println!();

    let fields: &[(&str, &str)] = &[
        ("ID", "id"),
        ("Name", "name"),
        ("Status", "status"),
        ("Submitter", "submitter"),
        ("Script Type", "script_type"),
        ("Created", "created_at"),
    ];

    for (label, key) in fields {
        let val = json_str(&data, key);
        if val != "-" {
            if *key == "status" {
                println!("  {:<16} {}", label, format_status(&val, client.color));
            } else if key.ends_with("_at") {
                println!("  {:<16} {}", label, format_relative_time(&val));
            } else {
                println!("  {:<16} {}", label, val);
            }
        }
    }

    // Progress.
    let progress = json_f64(&data, "progress");
    if progress > 0.0 {
        println!(
            "  {:<16} {}",
            "Progress",
            format_bar(progress, 20, client.color)
        );
    }

    // Chunks.
    let total_chunks = json_u64(&data, "total_chunks");
    let completed_chunks = json_u64(&data, "completed_chunks");
    let failed_chunks = json_u64(&data, "failed_chunks");
    if total_chunks > 0 {
        println!();
        println!("  Chunks: {} total, {} completed, {} failed",
            total_chunks, completed_chunks, failed_chunks);
    }

    // Error info.
    let error = json_str(&data, "error");
    if error != "-" {
        println!();
        if client.color {
            println!("  Error: {}", style(&error).red());
        } else {
            println!("  Error: {}", error);
        }
    }

    println!();
    Ok(())
}

async fn cmd_jobs_diagnose(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    let data = client
        .get(&format!("/api/v1/jobs/{}/diagnose", id))
        .await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let job_name = json_str(&data, "name");
    let job_status = json_str(&data, "status");
    let progress = json_f64(&data, "progress");
    let expected = json_f64(&data, "expected_progress");

    if client.color {
        println!(
            "{}",
            style(format!(
                "Diagnosis for job \"{}\" ({})",
                job_name, id
            ))
            .bold()
            .cyan()
        );
    } else {
        println!("Diagnosis for job \"{}\" ({})", job_name, id);
    }

    println!(
        "|-- Status: {} ({:.0}% complete, expected {:.0}%)",
        format_status(&job_status, client.color),
        progress,
        expected
    );

    // Root causes.
    let root_causes = data
        .get("root_causes")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();

    if !root_causes.is_empty() {
        println!("|-- Root Causes:");
        for (i, cause) in root_causes.iter().enumerate() {
            let severity = json_str(cause, "severity");
            let message = json_str(cause, "message");
            let suggestion = json_str(cause, "suggestion");
            let connector = if i < root_causes.len() - 1 {
                "|   |--"
            } else {
                "|   `--"
            };
            let _sev_icon = severity_icon(&severity, client.color);
            let severity_lower = severity.to_lowercase();
            let sev_label = match severity_lower.as_str() {
                "high" | "critical" => "HIGH",
                "medium" | "med" => "MED",
                "low" => "LOW",
                other => other,
            };
            println!("{} [{}] {}", connector, sev_label, message);
            if suggestion != "-" {
                let sub_connector = if i < root_causes.len() - 1 {
                    "|   |   `--"
                } else {
                    "|       `--"
                };
                if client.color {
                    println!(
                        "{} Suggestion: {}",
                        sub_connector,
                        style(&suggestion).dim()
                    );
                } else {
                    println!("{} Suggestion: {}", sub_connector, suggestion);
                }
            }
        }
    }

    // Recommended actions.
    let actions = data
        .get("recommended_actions")
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();

    if !actions.is_empty() {
        println!("`-- Recommended Actions:");
        for (i, action) in actions.iter().enumerate() {
            let action_str = action.as_str().unwrap_or("-");
            println!("    {}. {}", i + 1, action_str);
        }
    }

    println!();
    Ok(())
}

async fn cmd_jobs_cancel(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    println!();
    println!("  Cancelling job {}", id);
    println!("  This will abort all remaining chunks.");
    println!();

    if !client.confirm("  Continue?") {
        client.info("Cancelled.");
        return Ok(());
    }

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.yellow} {msg}")
            .expect("valid template"),
    );
    pb.set_message(format!("Cancelling job {}...", id));
    pb.enable_steady_tick(Duration::from_millis(100));

    let result = client
        .post(&format!("/api/v1/jobs/{}/cancel", id), &Value::Null)
        .await;
    pb.finish_and_clear();

    match result {
        Ok(_) => client.success(&format!("Job {} cancelled", id)),
        Err(e) => return Err(e),
    }

    Ok(())
}

// ============================================================================
// Command: fleet
// ============================================================================

async fn cmd_fleet(client: &SwarmCtlClient, sub: FleetCommands) -> CtlResult<()> {
    match sub {
        FleetCommands::Status => cmd_fleet_status(client).await,
        FleetCommands::Update(sub) => cmd_fleet_update(client, sub).await,
    }
}

async fn cmd_fleet_status(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/fleet/status").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Fleet Status ===").bold().cyan());
    } else {
        println!("=== Fleet Status ===");
    }
    println!();

    // Node counts by state.
    let counts = data
        .get("counts")
        .and_then(|c| c.as_object())
        .cloned()
        .unwrap_or_default();

    let total = json_u64(&data, "total_nodes");
    println!("  Total Nodes: {}", total);
    println!();

    if !counts.is_empty() {
        let headers = ["State", "Count", "Percentage"];
        let rows: Vec<Vec<String>> = counts
            .iter()
            .map(|(state, count)| {
                let c = count.as_u64().unwrap_or(0);
                let pct = if total > 0 {
                    c as f64 / total as f64 * 100.0
                } else {
                    0.0
                };
                vec![
                    format_status(state, client.color),
                    c.to_string(),
                    format_bar(pct, 15, client.color),
                ]
            })
            .collect();

        print_table(&headers, &rows, client.color);
        println!();
    }

    // Rolling update status (if active).
    if let Some(update) = data.get("active_update") {
        if !update.is_null() {
            let version = json_str(update, "target_version");
            let phase = json_str(update, "phase");
            let progress = json_f64(update, "progress");
            let updated = json_u64(update, "nodes_updated");
            let remaining = json_u64(update, "nodes_remaining");

            println!("  Active Rolling Update:");
            println!("    Target:   {}", version);
            println!(
                "    Phase:    {}",
                format_status(&phase, client.color)
            );
            println!(
                "    Progress: {}",
                format_bar(progress, 20, client.color)
            );
            println!("    Updated:  {} / {}", updated, updated + remaining);
            println!();
        }
    }

    // Maintenance windows.
    if let Some(Value::Array(windows)) = data.get("maintenance_windows") {
        if !windows.is_empty() {
            println!("  Maintenance Windows:");
            for w in windows {
                let name = json_str(w, "name");
                let start = json_str(w, "start");
                let end = json_str(w, "end");
                let active = json_bool(w, "active");
                println!(
                    "    {} ({} - {}) {}",
                    name,
                    start,
                    end,
                    if active {
                        format_status("ACTIVE", client.color)
                    } else {
                        "scheduled".to_string()
                    }
                );
            }
            println!();
        }
    }

    Ok(())
}

async fn cmd_fleet_update(client: &SwarmCtlClient, sub: UpdateCommands) -> CtlResult<()> {
    match sub {
        UpdateCommands::Status => cmd_fleet_update_status(client).await,
        UpdateCommands::Start {
            version,
            canary_pct,
            regions,
            dry_run,
        } => cmd_fleet_update_start(client, &version, canary_pct, regions, dry_run).await,
        UpdateCommands::Pause { reason } => cmd_fleet_update_pause(client, reason).await,
        UpdateCommands::Resume => cmd_fleet_update_resume(client).await,
        UpdateCommands::Rollback { reason } => cmd_fleet_update_rollback(client, reason).await,
        UpdateCommands::Cancel => cmd_fleet_update_cancel(client).await,
        UpdateCommands::History => cmd_fleet_update_history(client).await,
    }
}

async fn cmd_fleet_update_status(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/fleet/update/status").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if data.is_null() || json_str(&data, "status") == "none" {
        client.info("No rolling update in progress.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Rolling Update Status ===").bold().cyan());
    } else {
        println!("=== Rolling Update Status ===");
    }
    println!();

    let target = json_str(&data, "target_version");
    let phase = json_str(&data, "phase");
    let progress_pct = json_f64(&data, "progress");
    let nodes_updated = json_u64(&data, "nodes_updated");
    let nodes_total = json_u64(&data, "nodes_total");
    let nodes_failed = json_u64(&data, "nodes_failed");
    let started = json_str(&data, "started_at");
    let canary_pct = json_f64(&data, "canary_pct");

    println!("  Target Version:  {}", target);
    println!("  Phase:           {}", format_status(&phase, client.color));
    println!("  Canary:          {:.0}%", canary_pct * 100.0);
    println!(
        "  Started:         {}",
        if started != "-" {
            format_relative_time(&started)
        } else {
            "-".to_string()
        }
    );
    println!();

    // Progress bar.
    let pb = ProgressBar::new(nodes_total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  [{bar:40.green/dim}] {pos}/{len} nodes ({percent}%)")
            .expect("valid template")
            .progress_chars("##."),
    );
    pb.set_position(nodes_updated);
    pb.finish_and_clear();

    println!(
        "  Progress: {}",
        format_bar(progress_pct, 30, client.color)
    );
    println!(
        "  Nodes:    {} updated, {} remaining, {} failed",
        nodes_updated,
        nodes_total.saturating_sub(nodes_updated),
        nodes_failed
    );

    // Per-region breakdown (if available).
    if let Some(Value::Array(regions)) = data.get("regions") {
        println!();
        println!("  Per-region:");
        for r in regions {
            let region_name = json_str(r, "region");
            let region_progress = json_f64(r, "progress");
            let region_updated = json_u64(r, "updated");
            let region_total = json_u64(r, "total");
            println!(
                "    {:<12} {} ({}/{})",
                region_name,
                format_bar(region_progress, 15, client.color),
                region_updated,
                region_total
            );
        }
    }

    println!();
    Ok(())
}

async fn cmd_fleet_update_start(
    client: &SwarmCtlClient,
    version: &str,
    canary_pct: f64,
    regions: Option<String>,
    dry_run: bool,
) -> CtlResult<()> {
    println!();
    if client.color {
        println!(
            "  {} Rolling update to version {}",
            style("PLAN:").bold(),
            style(version).green().bold()
        );
    } else {
        println!("  PLAN: Rolling update to version {}", version);
    }
    println!("  Canary:  {:.0}%", canary_pct * 100.0);
    if let Some(ref r) = regions {
        println!("  Regions: {}", r);
    } else {
        println!("  Regions: all");
    }
    println!("  Dry run: {}", dry_run);
    println!();

    if !dry_run {
        if !client.confirm("  Start rolling update?") {
            client.info("Cancelled.");
            return Ok(());
        }
    }

    let body = serde_json::json!({
        "target_version": version,
        "canary_pct": canary_pct,
        "regions": regions.as_deref().map(|r| r.split(',').collect::<Vec<_>>()).unwrap_or_default(),
        "dry_run": dry_run,
    });

    let result = client.post("/api/v1/fleet/update/start", &body).await?;

    if dry_run {
        client.info("Dry run complete. No changes applied.");
        if client.output != OutputFormat::Text {
            client.print_value(&result);
        } else if let Some(plan) = result.get("plan") {
            println!();
            println!(
                "  Batches: {}",
                plan.get("batches").and_then(|b| b.as_u64()).unwrap_or(0)
            );
            println!(
                "  Estimated duration: {}",
                format_duration_human(
                    plan.get("estimated_duration_secs")
                        .and_then(|d| d.as_u64())
                        .unwrap_or(0)
                )
            );
        }
    } else {
        client.success(&format!("Rolling update to {} started", version));
    }

    Ok(())
}

async fn cmd_fleet_update_pause(
    client: &SwarmCtlClient,
    reason: Option<String>,
) -> CtlResult<()> {
    let reason_str = reason.as_deref().unwrap_or("operator-initiated pause");

    if !client.confirm("  Pause the rolling update?") {
        client.info("Cancelled.");
        return Ok(());
    }

    let body = serde_json::json!({
        "reason": reason_str,
    });

    client.post("/api/v1/fleet/update/pause", &body).await?;
    client.success("Rolling update paused");
    Ok(())
}

async fn cmd_fleet_update_resume(client: &SwarmCtlClient) -> CtlResult<()> {
    client
        .post("/api/v1/fleet/update/resume", &Value::Null)
        .await?;
    client.success("Rolling update resumed");
    Ok(())
}

async fn cmd_fleet_update_rollback(
    client: &SwarmCtlClient,
    reason: Option<String>,
) -> CtlResult<()> {
    let reason_str = reason.as_deref().unwrap_or("operator-initiated rollback");

    if client.color {
        println!();
        println!(
            "  {} You are about to rollback the rolling update.",
            style("WARNING:").red().bold()
        );
    } else {
        println!();
        println!("  WARNING: You are about to rollback the rolling update.");
    }
    println!("  Reason: \"{}\"", reason_str);
    println!("  All updated nodes will be reverted to the previous version.");
    println!();

    if !client.confirm("  Continue with rollback?") {
        client.info("Cancelled.");
        return Ok(());
    }

    let body = serde_json::json!({
        "reason": reason_str,
    });

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.red} {msg}")
            .expect("valid template"),
    );
    pb.set_message("Rolling back...");
    pb.enable_steady_tick(Duration::from_millis(100));

    let result = client.post("/api/v1/fleet/update/rollback", &body).await;
    pb.finish_and_clear();

    match result {
        Ok(_) => client.success("Rolling update rollback initiated"),
        Err(e) => return Err(e),
    }

    Ok(())
}

async fn cmd_fleet_update_cancel(client: &SwarmCtlClient) -> CtlResult<()> {
    if !client.confirm("  Cancel the rolling update?") {
        client.info("Cancelled.");
        return Ok(());
    }

    client
        .post("/api/v1/fleet/update/cancel", &Value::Null)
        .await?;
    client.success("Rolling update cancelled");
    Ok(())
}

async fn cmd_fleet_update_history(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/fleet/update/history").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let updates = data.as_array().cloned().unwrap_or_default();

    if updates.is_empty() {
        client.info("No update history.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Update History ===").bold().cyan());
    } else {
        println!("=== Update History ===");
    }
    println!();

    let headers = [
        "Version",
        "Status",
        "Started",
        "Duration",
        "Nodes",
        "Failed",
    ];
    let rows: Vec<Vec<String>> = updates
        .iter()
        .map(|u| {
            let version = json_str(u, "target_version");
            let status = json_str(u, "status");
            let started = json_str(u, "started_at");
            let duration = json_u64(u, "duration_secs");
            let nodes = json_u64(u, "nodes_updated");
            let failed = json_u64(u, "nodes_failed");

            vec![
                version,
                format_status(&status, client.color),
                if started != "-" {
                    format_relative_time(&started)
                } else {
                    "-".to_string()
                },
                if duration > 0 {
                    format_duration_human(duration)
                } else {
                    "-".to_string()
                },
                nodes.to_string(),
                if failed > 0 {
                    if client.color {
                        style(failed.to_string()).red().to_string()
                    } else {
                        failed.to_string()
                    }
                } else {
                    "0".to_string()
                },
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

// ============================================================================
// Command: events
// ============================================================================

async fn cmd_events(client: &SwarmCtlClient, sub: EventCommands) -> CtlResult<()> {
    match sub {
        EventCommands::List {
            domain,
            severity,
            limit,
        } => cmd_events_list(client, domain, severity, limit).await,
        EventCommands::Stream { domain, severity } => {
            cmd_events_stream(client, domain, severity).await
        }
        EventCommands::Stats => cmd_events_stats(client).await,
        EventCommands::Show { id } => cmd_events_show(client, &id).await,
    }
}

async fn cmd_events_list(
    client: &SwarmCtlClient,
    domain: Option<String>,
    severity: Option<String>,
    limit: usize,
) -> CtlResult<()> {
    let mut params = vec![format!("limit={}", limit)];
    if let Some(ref d) = domain {
        params.push(format!("domain={}", d));
    }
    if let Some(ref s) = severity {
        params.push(format!("severity={}", s));
    }
    let path = format!("/api/v1/events?{}", params.join("&"));

    let data = client.get(&path).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let events = data.as_array().cloned().unwrap_or_default();

    if events.is_empty() {
        client.info("No events found.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Events ===").bold().cyan());
    } else {
        println!("=== Events ===");
    }
    println!();

    let headers = ["Time", "Sev", "Domain", "Summary"];
    let rows: Vec<Vec<String>> = events
        .iter()
        .map(|e| {
            let timestamp = json_str(e, "timestamp");
            let sev = json_str(e, "severity");
            let dom = json_str(e, "domain");
            let summary = json_str(e, "summary");

            vec![
                if timestamp != "-" {
                    format_relative_time(&timestamp)
                } else {
                    "-".to_string()
                },
                severity_icon(&sev, client.color),
                dom,
                truncate(&summary, 60),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();
    println!("  Showing {} of {} events", events.len(), events.len());
    println!();

    Ok(())
}

async fn cmd_events_stream(
    client: &SwarmCtlClient,
    domain: Option<String>,
    severity: Option<String>,
) -> CtlResult<()> {
    let mut params = Vec::new();
    if let Some(ref d) = domain {
        params.push(format!("domain={}", d));
    }
    if let Some(ref s) = severity {
        params.push(format!("severity={}", s));
    }
    let query = if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    };

    let url = client.url(&format!("/api/v1/events/stream{}", query));

    if !client.quiet {
        if client.color {
            eprintln!(
                "{} Streaming events from {} (press Ctrl+C to stop)",
                style("[i]").blue(),
                url
            );
        } else {
            eprintln!("[i] Streaming events from {} (press Ctrl+C to stop)", url);
        }
    }

    // Poll for events at regular intervals (SSE streaming requires the `stream`
    // feature on reqwest; we use a polling approach instead).
    let poll_interval = Duration::from_secs(2);
    let mut last_event_id: Option<String> = None;

    loop {
        // Check for Ctrl+C via terminal raw mode polling.
        if crossterm::event::poll(Duration::from_millis(50)).unwrap_or(false) {
            if let Ok(crossterm::event::Event::Key(key)) = crossterm::event::read() {
                if key.code == crossterm::event::KeyCode::Char('c')
                    && key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL)
                {
                    break;
                }
                if key.code == crossterm::event::KeyCode::Char('q') {
                    break;
                }
            }
        }

        // Fetch latest events.
        let mut poll_path = format!("/api/v1/events?limit=10");
        if let Some(ref d) = domain {
            poll_path.push_str(&format!("&domain={}", d));
        }
        if let Some(ref s) = severity {
            poll_path.push_str(&format!("&severity={}", s));
        }
        if let Some(ref last_id) = last_event_id {
            poll_path.push_str(&format!("&after={}", last_id));
        }

        if let Ok(data) = client.get(&poll_path).await {
            if let Some(events) = data.as_array() {
                for event in events {
                    let timestamp = json_str(event, "timestamp");
                    let sev = json_str(event, "severity");
                    let dom = json_str(event, "domain");
                    let summary = json_str(event, "summary");
                    let event_id = json_str(event, "id");

                    let time_str = if timestamp != "-" {
                        format_relative_time(&timestamp)
                    } else {
                        "now".to_string()
                    };

                    println!(
                        "{} {} [{}] {}",
                        time_str,
                        severity_icon(&sev, client.color),
                        dom,
                        summary
                    );

                    if event_id != "-" {
                        last_event_id = Some(event_id);
                    }
                }
            }
        }

        tokio::time::sleep(poll_interval).await;
    }

    Ok(())
}

async fn cmd_events_stats(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/events/stats").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Event Bus Stats ===").bold().cyan());
    } else {
        println!("=== Event Bus Stats ===");
    }
    println!();

    let total = json_u64(&data, "total_events");
    let per_second = json_f64(&data, "events_per_second");
    let subscribers = json_u64(&data, "subscribers");
    let buffer_used = json_u64(&data, "buffer_used");
    let buffer_capacity = json_u64(&data, "buffer_capacity");

    println!("  Total Events:    {}", total);
    println!("  Rate:            {:.1} events/sec", per_second);
    println!("  Subscribers:     {}", subscribers);

    if buffer_capacity > 0 {
        let usage_pct = buffer_used as f64 / buffer_capacity as f64 * 100.0;
        println!(
            "  Buffer:          {} / {} ({})",
            buffer_used,
            buffer_capacity,
            format_bar(usage_pct, 15, client.color)
        );
    }

    // Per-domain breakdown.
    if let Some(Value::Object(domains)) = data.get("by_domain") {
        println!();
        println!("  By Domain:");
        let headers = ["Domain", "Count", "Rate"];
        let rows: Vec<Vec<String>> = domains
            .iter()
            .map(|(dom, stats)| {
                let count = stats
                    .get("count")
                    .and_then(|c| c.as_u64())
                    .unwrap_or(0);
                let rate = stats
                    .get("rate")
                    .and_then(|r| r.as_f64())
                    .unwrap_or(0.0);
                vec![dom.clone(), count.to_string(), format!("{:.1}/s", rate)]
            })
            .collect();
        print_table(&headers, &rows, client.color);
    }

    // Per-severity breakdown.
    if let Some(Value::Object(severities)) = data.get("by_severity") {
        println!();
        println!("  By Severity:");
        for (sev, count) in severities {
            let c: u64 = count.as_u64().unwrap_or(0);
            println!(
                "    {} {:<12} {}",
                severity_icon(sev, client.color),
                sev,
                c
            );
        }
    }

    println!();
    Ok(())
}

async fn cmd_events_show(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    let data = client.get(&format!("/api/v1/events/{}", id)).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Event Detail ===").bold().cyan());
    } else {
        println!("=== Event Detail ===");
    }
    println!();

    let fields = [
        ("ID", "id"),
        ("Domain", "domain"),
        ("Severity", "severity"),
        ("Summary", "summary"),
        ("Entity Type", "entity_type"),
        ("Entity ID", "entity_id"),
        ("Timestamp", "timestamp"),
        ("Correlation ID", "correlation_id"),
    ];

    for (label, key) in &fields {
        let val = json_str(&data, key);
        if val != "-" {
            if *key == "severity" {
                println!(
                    "  {:<18} {} {}",
                    label,
                    severity_icon(&val, client.color),
                    val
                );
            } else if *key == "timestamp" {
                println!("  {:<18} {} ({})", label, val, format_relative_time(&val));
            } else {
                println!("  {:<18} {}", label, val);
            }
        }
    }

    // Extra data / metadata.
    if let Some(meta) = data.get("metadata") {
        if !meta.is_null() && meta.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
            println!();
            println!("  Metadata:");
            if let Some(obj) = meta.as_object() {
                for (k, v) in obj {
                    println!("    {}: {}", k, v);
                }
            }
        }
    }

    println!();
    Ok(())
}

// ============================================================================
// Command: alerts
// ============================================================================

async fn cmd_alerts(client: &SwarmCtlClient, sub: AlertCommands) -> CtlResult<()> {
    match sub {
        AlertCommands::List => cmd_alerts_list(client).await,
        AlertCommands::Rules(sub) => cmd_alerts_rules(client, sub).await,
        AlertCommands::Ack { name } => cmd_alerts_ack(client, &name).await,
        AlertCommands::Silence {
            name,
            duration,
            reason,
        } => cmd_alerts_silence(client, &name, duration, reason).await,
        AlertCommands::Silences => cmd_alerts_silences(client).await,
        AlertCommands::Summary => cmd_alerts_summary(client).await,
    }
}

async fn cmd_alerts_list(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/alerts").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let alerts = data.as_array().cloned().unwrap_or_default();

    if alerts.is_empty() {
        client.info("No active alerts.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Active Alerts ===").bold().cyan());
    } else {
        println!("=== Active Alerts ===");
    }
    println!();

    let headers = ["Name", "Severity", "State", "Since", "Summary"];
    let rows: Vec<Vec<String>> = alerts
        .iter()
        .map(|a| {
            let name = json_str(a, "name");
            let sev = json_str(a, "severity");
            let state = json_str(a, "state");
            let since = json_str(a, "fired_at");
            let summary = json_str(a, "summary");

            vec![
                name,
                format!(
                    "{} {}",
                    severity_icon(&sev, client.color),
                    sev
                ),
                format_status(&state, client.color),
                if since != "-" {
                    format_relative_time(&since)
                } else {
                    "-".to_string()
                },
                truncate(&summary, 45),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_alerts_rules(client: &SwarmCtlClient, sub: AlertRuleCommands) -> CtlResult<()> {
    match sub {
        AlertRuleCommands::List => {
            let data = client.get("/api/v1/alerts/rules").await?;

            if client.output != OutputFormat::Text {
                client.print_value(&data);
                return Ok(());
            }

            let rules = data.as_array().cloned().unwrap_or_default();

            if rules.is_empty() {
                client.info("No alert rules configured.");
                return Ok(());
            }

            if client.color {
                println!("{}", style("=== Alert Rules ===").bold().cyan());
            } else {
                println!("=== Alert Rules ===");
            }
            println!();

            let headers = ["Name", "Severity", "Enabled", "For", "Condition"];
            let rows: Vec<Vec<String>> = rules
                .iter()
                .map(|r| {
                    let name = json_str(r, "name");
                    let sev = json_str(r, "severity");
                    let enabled = json_bool(r, "enabled");
                    let for_duration = json_str(r, "for_duration");
                    let condition = json_str(r, "condition_summary");

                    vec![
                        name,
                        format!(
                            "{} {}",
                            severity_icon(&sev, client.color),
                            sev
                        ),
                        if enabled {
                            format_status("enabled", client.color)
                        } else {
                            format_status("disabled", client.color)
                        },
                        if for_duration != "-" {
                            for_duration
                        } else {
                            "instant".to_string()
                        },
                        truncate(&condition, 40),
                    ]
                })
                .collect();

            print_table(&headers, &rows, client.color);
            println!();
            Ok(())
        }
        AlertRuleCommands::Add { file } => {
            let contents = std::fs::read_to_string(&file).map_err(|e| {
                CtlError::Other(format!("Failed to read {}: {}", file.display(), e))
            })?;

            // Try parsing as JSON first, then TOML.
            let value: Value = if file
                .extension()
                .map(|e| e == "toml")
                .unwrap_or(false)
            {
                toml::from_str(&contents).map_err(|e| {
                    CtlError::Other(format!("Failed to parse TOML: {}", e))
                })?
            } else {
                serde_json::from_str(&contents).map_err(|e| {
                    CtlError::Other(format!("Failed to parse JSON: {}", e))
                })?
            };

            client.post("/api/v1/alerts/rules", &value).await?;
            client.success("Alert rule added");
            Ok(())
        }
        AlertRuleCommands::Remove { name } => {
            if !client.confirm(&format!("Remove alert rule '{}'?", name)) {
                client.info("Cancelled.");
                return Ok(());
            }
            client
                .delete(&format!("/api/v1/alerts/rules/{}", name))
                .await?;
            client.success(&format!("Alert rule '{}' removed", name));
            Ok(())
        }
    }
}

async fn cmd_alerts_ack(client: &SwarmCtlClient, name: &str) -> CtlResult<()> {
    let body = serde_json::json!({
        "action": "acknowledge",
    });
    client
        .post(&format!("/api/v1/alerts/{}/ack", name), &body)
        .await?;
    client.success(&format!("Alert '{}' acknowledged", name));
    Ok(())
}

async fn cmd_alerts_silence(
    client: &SwarmCtlClient,
    name: &str,
    duration: u64,
    reason: Option<String>,
) -> CtlResult<()> {
    let reason_str = reason.as_deref().unwrap_or("operator-initiated silence");

    let body = serde_json::json!({
        "alert_name": name,
        "duration_secs": duration,
        "reason": reason_str,
    });

    client.post("/api/v1/alerts/silences", &body).await?;
    client.success(&format!(
        "Alert '{}' silenced for {}",
        name,
        format_duration_human(duration)
    ));
    Ok(())
}

async fn cmd_alerts_silences(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/alerts/silences").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let silences = data.as_array().cloned().unwrap_or_default();

    if silences.is_empty() {
        client.info("No active silence windows.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Silence Windows ===").bold().cyan());
    } else {
        println!("=== Silence Windows ===");
    }
    println!();

    let headers = ["Alert", "Reason", "Created", "Expires"];
    let rows: Vec<Vec<String>> = silences
        .iter()
        .map(|s| {
            let alert = json_str(s, "alert_name");
            let reason = json_str(s, "reason");
            let created = json_str(s, "created_at");
            let expires = json_str(s, "expires_at");

            vec![
                alert,
                truncate(&reason, 30),
                if created != "-" {
                    format_relative_time(&created)
                } else {
                    "-".to_string()
                },
                if expires != "-" {
                    format_relative_time(&expires)
                } else {
                    "-".to_string()
                },
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_alerts_summary(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/alerts/summary").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Alert Summary ===").bold().cyan());
    } else {
        println!("=== Alert Summary ===");
    }
    println!();

    let total = json_u64(&data, "total");
    let firing = json_u64(&data, "firing");
    let pending = json_u64(&data, "pending");
    let resolved = json_u64(&data, "resolved");
    let silenced = json_u64(&data, "silenced");

    println!("  Total:     {}", total);
    println!(
        "  Firing:    {}",
        if firing > 0 && client.color {
            style(firing.to_string()).red().bold().to_string()
        } else {
            firing.to_string()
        }
    );
    println!(
        "  Pending:   {}",
        if pending > 0 && client.color {
            style(pending.to_string()).yellow().to_string()
        } else {
            pending.to_string()
        }
    );
    println!("  Resolved:  {}", resolved);
    println!("  Silenced:  {}", silenced);

    // By severity.
    if let Some(Value::Object(by_sev)) = data.get("by_severity") {
        println!();
        println!("  By Severity:");
        for (sev, count) in by_sev {
            let c = count.as_u64().unwrap_or(0);
            println!(
                "    {} {:<12} {}",
                severity_icon(sev, client.color),
                sev,
                c
            );
        }
    }

    println!();
    Ok(())
}

// ============================================================================
// Command: sla
// ============================================================================

async fn cmd_sla(client: &SwarmCtlClient, sub: SlaCommands) -> CtlResult<()> {
    match sub {
        SlaCommands::Status => cmd_sla_status(client).await,
        SlaCommands::Show { name } => cmd_sla_show(client, &name).await,
        SlaCommands::Report { name, hours } => cmd_sla_report(client, &name, hours).await,
        SlaCommands::Add { file } => cmd_sla_add(client, &file).await,
    }
}

async fn cmd_sla_status(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/sla/status").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let slas = data.as_array().cloned().unwrap_or_default();

    if slas.is_empty() {
        client.info("No SLAs defined.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== SLA Status ===").bold().cyan());
    } else {
        println!("=== SLA Status ===");
    }
    println!();

    let headers = ["Name", "Metric", "Target", "Current", "Compliance", "Budget"];
    let rows: Vec<Vec<String>> = slas
        .iter()
        .map(|s| {
            let name = json_str(s, "name");
            let metric = json_str(s, "metric");
            let target = json_f64(s, "target");
            let current = json_f64(s, "current_value");
            let compliance = json_f64(s, "compliance_pct");
            let budget_remaining = json_f64(s, "error_budget_remaining_pct");

            let compliance_status = if compliance >= 99.0 {
                "compliant"
            } else if compliance >= 95.0 {
                "warning"
            } else {
                "breached"
            };

            vec![
                name,
                metric,
                format!("{:.2}", target),
                format!("{:.2}", current),
                format!(
                    "{} {}",
                    format_bar(compliance, 10, client.color),
                    format_status(compliance_status, client.color)
                ),
                format_bar(budget_remaining, 8, client.color),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_sla_show(client: &SwarmCtlClient, name: &str) -> CtlResult<()> {
    let data = client.get(&format!("/api/v1/sla/{}", name)).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!(
            "{}",
            style(format!("=== SLA: {} ===", name)).bold().cyan()
        );
    } else {
        println!("=== SLA: {} ===", name);
    }
    println!();

    let fields = [
        ("Name", "name"),
        ("Metric", "metric"),
        ("Description", "description"),
    ];

    for (label, key) in &fields {
        let val = json_str(&data, key);
        if val != "-" {
            println!("  {:<22} {}", label, val);
        }
    }

    let target = json_f64(&data, "target");
    let current = json_f64(&data, "current_value");
    let compliance = json_f64(&data, "compliance_pct");
    let budget_total = json_f64(&data, "error_budget_total");
    let budget_used = json_f64(&data, "error_budget_used");
    let budget_remaining = json_f64(&data, "error_budget_remaining_pct");
    let burn_rate = json_f64(&data, "burn_rate");

    println!("  {:<22} {:.4}", "Target", target);
    println!("  {:<22} {:.4}", "Current Value", current);
    println!(
        "  {:<22} {}",
        "Compliance",
        format_bar(compliance, 20, client.color)
    );
    println!();
    println!("  Error Budget:");
    println!("    Total:     {:.4}", budget_total);
    println!("    Used:      {:.4}", budget_used);
    println!(
        "    Remaining: {}",
        format_bar(budget_remaining, 15, client.color)
    );
    println!("    Burn Rate: {:.2}x", burn_rate);

    if burn_rate > 14.4 {
        if client.color {
            println!(
                "    {}",
                style("FAST BURN: Error budget is being consumed rapidly!").red().bold()
            );
        } else {
            println!("    FAST BURN: Error budget is being consumed rapidly!");
        }
    } else if burn_rate > 2.0 {
        if client.color {
            println!(
                "    {}",
                style("SLOW BURN: Error budget consumption above sustainable rate").yellow()
            );
        } else {
            println!("    SLOW BURN: Error budget consumption above sustainable rate");
        }
    }

    // Recent breaches.
    if let Some(Value::Array(breaches)) = data.get("recent_breaches") {
        if !breaches.is_empty() {
            println!();
            println!("  Recent Breaches:");
            for b in breaches.iter().take(5) {
                let started = json_str(b, "started_at");
                let ended = json_str(b, "ended_at");
                let severity = json_str(b, "severity");
                let value = json_f64(b, "measured_value");
                println!(
                    "    {} {} -> {} (value: {:.4}, severity: {})",
                    severity_icon(&severity, client.color),
                    if started != "-" {
                        format_relative_time(&started)
                    } else {
                        "-".to_string()
                    },
                    if ended != "-" {
                        format_relative_time(&ended)
                    } else {
                        "ongoing".to_string()
                    },
                    value,
                    severity
                );
            }
        }
    }

    println!();
    Ok(())
}

async fn cmd_sla_report(client: &SwarmCtlClient, name: &str, hours: u32) -> CtlResult<()> {
    let data = client
        .get(&format!("/api/v1/sla/{}/report?hours={}", name, hours))
        .await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!(
            "{}",
            style(format!("=== SLA Report: {} (last {}h) ===", name, hours))
                .bold()
                .cyan()
        );
    } else {
        println!("=== SLA Report: {} (last {}h) ===", name, hours);
    }
    println!();

    let uptime_pct = json_f64(&data, "uptime_pct");
    let total_breaches = json_u64(&data, "total_breaches");
    let total_breach_duration = json_u64(&data, "total_breach_duration_secs");
    let avg_value = json_f64(&data, "average_value");
    let min_value = json_f64(&data, "min_value");
    let max_value = json_f64(&data, "max_value");

    println!("  Uptime:             {}", format_bar(uptime_pct, 20, client.color));
    println!("  Total Breaches:     {}", total_breaches);
    println!(
        "  Breach Duration:    {}",
        format_duration_human(total_breach_duration)
    );
    println!("  Average Value:      {:.4}", avg_value);
    println!("  Min Value:          {:.4}", min_value);
    println!("  Max Value:          {:.4}", max_value);

    // Time series data (sparkline).
    if let Some(Value::Array(points)) = data.get("data_points") {
        let values: Vec<f64> = points
            .iter()
            .filter_map(|p| p.get("value").and_then(|v| v.as_f64()))
            .collect();

        if !values.is_empty() {
            println!();
            println!(
                "  Trend: {}",
                build_sparkline(&values, 50)
            );
        }
    }

    println!();
    Ok(())
}

async fn cmd_sla_add(client: &SwarmCtlClient, file: &PathBuf) -> CtlResult<()> {
    let contents = std::fs::read_to_string(file).map_err(|e| {
        CtlError::Other(format!("Failed to read {}: {}", file.display(), e))
    })?;

    let value: Value = if file
        .extension()
        .map(|e| e == "toml")
        .unwrap_or(false)
    {
        toml::from_str(&contents)
            .map_err(|e| CtlError::Other(format!("Failed to parse TOML: {}", e)))?
    } else {
        serde_json::from_str(&contents)
            .map_err(|e| CtlError::Other(format!("Failed to parse JSON: {}", e)))?
    };

    client.post("/api/v1/sla", &value).await?;
    client.success("SLA definition added");
    Ok(())
}

// ============================================================================
// Command: capacity
// ============================================================================

async fn cmd_capacity(client: &SwarmCtlClient, sub: CapacityCommands) -> CtlResult<()> {
    match sub {
        CapacityCommands::Status => cmd_capacity_status(client).await,
        CapacityCommands::Forecast => cmd_capacity_forecast(client).await,
        CapacityCommands::Bottlenecks => cmd_capacity_bottlenecks(client).await,
        CapacityCommands::Whatif { file } => cmd_capacity_whatif(client, &file).await,
        CapacityCommands::Rightsizing => cmd_capacity_rightsizing(client).await,
    }
}

async fn cmd_capacity_status(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/capacity/status").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Capacity Status ===").bold().cyan());
    } else {
        println!("=== Capacity Status ===");
    }
    println!();

    // CPU.
    let cpu_total = json_u64(&data, "total_cpu");
    let cpu_used = json_f64(&data, "used_cpu");
    let cpu_pct = if cpu_total > 0 {
        cpu_used / cpu_total as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "  CPU:       {} ({:.1} / {} cores)",
        format_bar(cpu_pct, 25, client.color),
        cpu_used,
        cpu_total
    );

    // Memory.
    let mem_total = json_u64(&data, "total_memory_mb");
    let mem_used = json_u64(&data, "used_memory_mb");
    let mem_pct = if mem_total > 0 {
        mem_used as f64 / mem_total as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "  Memory:    {} ({} / {})",
        format_bar(mem_pct, 25, client.color),
        format_bytes(mem_used * 1024 * 1024),
        format_bytes(mem_total * 1024 * 1024)
    );

    // Disk.
    let disk_total = json_u64(&data, "total_disk_mb");
    let disk_used = json_u64(&data, "used_disk_mb");
    let disk_pct = if disk_total > 0 {
        disk_used as f64 / disk_total as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "  Disk:      {} ({} / {})",
        format_bar(disk_pct, 25, client.color),
        format_bytes(disk_used * 1024 * 1024),
        format_bytes(disk_total * 1024 * 1024)
    );

    // Bandwidth.
    let bw_total = json_f64(&data, "total_bandwidth_mbps");
    let bw_used = json_f64(&data, "used_bandwidth_mbps");
    let bw_pct = if bw_total > 0.0 {
        bw_used / bw_total * 100.0
    } else {
        0.0
    };
    println!(
        "  Bandwidth: {} ({:.0} / {:.0} Mbps)",
        format_bar(bw_pct, 25, client.color),
        bw_used,
        bw_total
    );

    // Node count.
    let node_count = json_u64(&data, "node_count");
    println!();
    println!("  Active Nodes: {}", node_count);

    println!();
    Ok(())
}

async fn cmd_capacity_forecast(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/capacity/forecast").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let forecasts = data.as_array().cloned().unwrap_or_default();

    if forecasts.is_empty() {
        client.info("No forecast data available (insufficient history).");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Capacity Forecast ===").bold().cyan());
    } else {
        println!("=== Capacity Forecast ===");
    }
    println!();

    let headers = [
        "Resource",
        "Current",
        "1h Forecast",
        "4h Forecast",
        "24h Forecast",
        "Trend",
    ];
    let rows: Vec<Vec<String>> = forecasts
        .iter()
        .map(|f| {
            let resource = json_str(f, "resource");
            let current = json_f64(f, "current_pct");
            let h1 = json_f64(f, "forecast_1h_pct");
            let h4 = json_f64(f, "forecast_4h_pct");
            let h24 = json_f64(f, "forecast_24h_pct");
            let trend = json_str(f, "trend");

            let trend_str = match trend.as_str() {
                "rising" | "up" => {
                    if client.color {
                        style("^ rising").red().to_string()
                    } else {
                        "^ rising".to_string()
                    }
                }
                "falling" | "down" => {
                    if client.color {
                        style("v falling").green().to_string()
                    } else {
                        "v falling".to_string()
                    }
                }
                "stable" | "flat" => "= stable".to_string(),
                other => other.to_string(),
            };

            vec![
                resource,
                format_bar(current, 8, client.color),
                format_bar(h1, 8, client.color),
                format_bar(h4, 8, client.color),
                format_bar(h24, 8, client.color),
                trend_str,
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_capacity_bottlenecks(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/capacity/bottlenecks").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let bottlenecks = data.as_array().cloned().unwrap_or_default();

    if bottlenecks.is_empty() {
        client.success("No bottlenecks identified.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Capacity Bottlenecks ===").bold().cyan());
    } else {
        println!("=== Capacity Bottlenecks ===");
    }
    println!();

    let headers = ["Resource", "Severity", "Utilization", "ETA", "Recommendation"];
    let rows: Vec<Vec<String>> = bottlenecks
        .iter()
        .map(|b| {
            let resource = json_str(b, "resource");
            let severity = json_str(b, "severity");
            let utilization = json_f64(b, "utilization_pct");
            let eta = json_str(b, "eta");
            let recommendation = json_str(b, "recommendation");

            vec![
                resource,
                format!(
                    "{} {}",
                    severity_icon(&severity, client.color),
                    severity
                ),
                format_bar(utilization, 10, client.color),
                if eta != "-" { eta } else { "N/A".to_string() },
                truncate(&recommendation, 35),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_capacity_whatif(client: &SwarmCtlClient, file: &PathBuf) -> CtlResult<()> {
    let contents = std::fs::read_to_string(file).map_err(|e| {
        CtlError::Other(format!("Failed to read {}: {}", file.display(), e))
    })?;

    let scenario: Value = serde_json::from_str(&contents).map_err(|e| {
        CtlError::Other(format!("Failed to parse JSON: {}", e))
    })?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.blue} {msg}")
            .expect("valid template"),
    );
    pb.set_message("Running what-if analysis...");
    pb.enable_steady_tick(Duration::from_millis(100));

    let data = client.post("/api/v1/capacity/whatif", &scenario).await?;
    pb.finish_and_clear();

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== What-If Analysis ===").bold().cyan());
    } else {
        println!("=== What-If Analysis ===");
    }
    println!();

    // Show before/after comparison.
    let before = data.get("before").cloned().unwrap_or(Value::Null);
    let after = data.get("after").cloned().unwrap_or(Value::Null);

    let resources = ["cpu", "memory", "disk", "bandwidth"];
    let headers = ["Resource", "Before", "After", "Change"];
    let rows: Vec<Vec<String>> = resources
        .iter()
        .map(|r| {
            let before_val = json_f64(&before, &format!("{}_pct", r));
            let after_val = json_f64(&after, &format!("{}_pct", r));
            let change = after_val - before_val;
            let change_str = if change >= 0.0 {
                format!("+{:.1}%", change)
            } else {
                format!("{:.1}%", change)
            };

            vec![
                capitalize(r),
                format_bar(before_val, 10, client.color),
                format_bar(after_val, 10, client.color),
                if client.color {
                    if change > 5.0 {
                        style(change_str).red().to_string()
                    } else if change < -5.0 {
                        style(change_str).green().to_string()
                    } else {
                        change_str
                    }
                } else {
                    change_str
                },
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);

    // Verdict.
    let verdict = json_str(&data, "verdict");
    let risk = json_str(&data, "risk_level");
    println!();
    println!(
        "  Verdict: {} (risk: {})",
        verdict,
        format_status(&risk, client.color)
    );

    println!();
    Ok(())
}

async fn cmd_capacity_rightsizing(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/capacity/rightsizing").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let recommendations = data.as_array().cloned().unwrap_or_default();

    if recommendations.is_empty() {
        client.success("All nodes are right-sized.");
        return Ok(());
    }

    if client.color {
        println!(
            "{}",
            style("=== Right-Sizing Recommendations ===").bold().cyan()
        );
    } else {
        println!("=== Right-Sizing Recommendations ===");
    }
    println!();

    let headers = ["Node", "Status", "CPU Util", "Mem Util", "Recommendation"];
    let rows: Vec<Vec<String>> = recommendations
        .iter()
        .map(|r| {
            let node = json_str(r, "node_id");
            let status = json_str(r, "status");
            let cpu_util = json_f64(r, "cpu_utilization_pct");
            let mem_util = json_f64(r, "memory_utilization_pct");
            let rec = json_str(r, "recommendation");

            vec![
                truncate(&node, 16),
                format_status(&status, client.color),
                format_bar(cpu_util, 8, client.color),
                format_bar(mem_util, 8, client.color),
                truncate(&rec, 30),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

// ============================================================================
// Command: visions
// ============================================================================

async fn cmd_visions(client: &SwarmCtlClient, sub: VisionCommands) -> CtlResult<()> {
    match sub {
        VisionCommands::List => cmd_visions_list(client).await,
        VisionCommands::Show { name } => cmd_visions_show(client, &name).await,
        VisionCommands::Add { file } => cmd_visions_add(client, &file).await,
    }
}

async fn cmd_visions_list(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/visions").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let visions = data.as_array().cloned().unwrap_or_default();

    if visions.is_empty() {
        client.info("No visions defined.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Visions ===").bold().cyan());
    } else {
        println!("=== Visions ===");
    }
    println!();

    let headers = ["Name", "Status", "Progress", "Description"];
    let rows: Vec<Vec<String>> = visions
        .iter()
        .map(|v| {
            let name = json_str(v, "name");
            let status = json_str(v, "status");
            let progress = json_f64(v, "progress");
            let desc = json_str(v, "description");

            vec![
                name,
                format_status(&status, client.color),
                format_bar(progress, 10, client.color),
                truncate(&desc, 40),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_visions_show(client: &SwarmCtlClient, name: &str) -> CtlResult<()> {
    let data = client.get(&format!("/api/v1/visions/{}", name)).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!(
            "{}",
            style(format!("=== Vision: {} ===", name)).bold().cyan()
        );
    } else {
        println!("=== Vision: {} ===", name);
    }
    println!();

    let fields = [
        ("Name", "name"),
        ("Description", "description"),
        ("Status", "status"),
        ("Created", "created_at"),
    ];

    for (label, key) in &fields {
        let val = json_str(&data, key);
        if val != "-" {
            if *key == "status" {
                println!("  {:<18} {}", label, format_status(&val, client.color));
            } else if key.ends_with("_at") {
                println!("  {:<18} {}", label, format_relative_time(&val));
            } else {
                println!("  {:<18} {}", label, val);
            }
        }
    }

    let progress = json_f64(&data, "progress");
    println!(
        "  {:<18} {}",
        "Progress",
        format_bar(progress, 20, client.color)
    );

    // Goals.
    if let Some(Value::Array(goals)) = data.get("goals") {
        println!();
        println!("  Goals:");
        for goal in goals {
            let goal_name = json_str(goal, "name");
            let goal_met = json_bool(goal, "met");
            let icon = if goal_met {
                if client.color {
                    style("[+]").green().to_string()
                } else {
                    "[+]".to_string()
                }
            } else {
                if client.color {
                    style("[ ]").dim().to_string()
                } else {
                    "[ ]".to_string()
                }
            };
            println!("    {} {}", icon, goal_name);
        }
    }

    println!();
    Ok(())
}

async fn cmd_visions_add(client: &SwarmCtlClient, file: &PathBuf) -> CtlResult<()> {
    let contents = std::fs::read_to_string(file).map_err(|e| {
        CtlError::Other(format!("Failed to read {}: {}", file.display(), e))
    })?;

    let value: Value = toml::from_str(&contents).map_err(|e| {
        CtlError::Other(format!("Failed to parse TOML: {}", e))
    })?;

    client.post("/api/v1/visions", &value).await?;
    client.success("Vision added");
    Ok(())
}

// ============================================================================
// Command: constellation
// ============================================================================

async fn cmd_constellation(client: &SwarmCtlClient, sub: ConstellationCommands) -> CtlResult<()> {
    match sub {
        ConstellationCommands::Status => cmd_constellation_status(client).await,
        ConstellationCommands::Swarms => cmd_constellation_swarms(client).await,
        ConstellationCommands::Membranes => cmd_constellation_membranes(client).await,
        ConstellationCommands::Membrane { id } => {
            cmd_constellation_membrane(client, &id).await
        }
        ConstellationCommands::Treaties => cmd_constellation_treaties(client).await,
        ConstellationCommands::Lending => cmd_constellation_lending(client).await,
    }
}

async fn cmd_constellation_status(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/constellation/status").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Constellation Status ===").bold().cyan());
    } else {
        println!("=== Constellation Status ===");
    }
    println!();

    let local_id = json_str(&data, "local_swarm_id");
    let local_name = json_str(&data, "local_swarm_name");
    let known_swarms = json_u64(&data, "known_swarms");
    let active_treaties = json_u64(&data, "active_treaties");
    let active_membranes = json_u64(&data, "active_membranes");
    let lending_sessions = json_u64(&data, "active_lending_sessions");

    println!("  Local Swarm:     {} ({})", local_name, truncate(&local_id, 12));
    println!("  Known Swarms:    {}", known_swarms);
    println!("  Active Treaties: {}", active_treaties);
    println!("  Membranes:       {}", active_membranes);
    println!("  Lending Sessions:{}", lending_sessions);

    println!();
    Ok(())
}

async fn cmd_constellation_swarms(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/constellation/swarms").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let swarms = data.as_array().cloned().unwrap_or_default();

    if swarms.is_empty() {
        client.info("No remote swarms known.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Known Swarms ===").bold().cyan());
    } else {
        println!("=== Known Swarms ===");
    }
    println!();

    let headers = ["ID", "Name", "Reachability", "Nodes", "Trust", "Last Heartbeat"];
    let rows: Vec<Vec<String>> = swarms
        .iter()
        .map(|s| {
            let id = json_str(s, "id");
            let name = json_str(s, "name");
            let reachability = json_str(s, "reachability");
            let nodes = json_u64(s, "node_count");
            let trust = json_f64(s, "trust_score");
            let last_hb = json_str(s, "last_heartbeat");

            vec![
                truncate(&id, 12),
                truncate(&name, 16),
                format_status(&reachability, client.color),
                nodes.to_string(),
                format_bar(trust * 100.0, 8, client.color),
                if last_hb != "-" {
                    format_relative_time(&last_hb)
                } else {
                    "-".to_string()
                },
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_constellation_membranes(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/constellation/membranes").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let membranes = data.as_array().cloned().unwrap_or_default();

    if membranes.is_empty() {
        client.info("No membranes configured.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Membranes ===").bold().cyan());
    } else {
        println!("=== Membranes ===");
    }
    println!();

    let headers = ["ID", "Remote Swarm", "Status", "Permeability", "Crossings"];
    let rows: Vec<Vec<String>> = membranes
        .iter()
        .map(|m| {
            let id = json_str(m, "id");
            let remote = json_str(m, "remote_swarm");
            let status = json_str(m, "status");
            let permeability = json_str(m, "permeability");
            let crossings = json_u64(m, "total_crossings");

            vec![
                truncate(&id, 12),
                truncate(&remote, 16),
                format_status(&status, client.color),
                permeability,
                crossings.to_string(),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_constellation_membrane(client: &SwarmCtlClient, id: &str) -> CtlResult<()> {
    let data = client
        .get(&format!("/api/v1/constellation/membranes/{}", id))
        .await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    if client.color {
        println!(
            "{}",
            style(format!("=== Membrane: {} ===", id)).bold().cyan()
        );
    } else {
        println!("=== Membrane: {} ===", id);
    }
    println!();

    let fields = [
        ("ID", "id"),
        ("Remote Swarm", "remote_swarm"),
        ("Status", "status"),
        ("Permeability", "permeability"),
        ("Embassy Node", "embassy_node"),
        ("Created", "created_at"),
    ];

    for (label, key) in &fields {
        let val = json_str(&data, key);
        if val != "-" {
            if *key == "status" {
                println!("  {:<18} {}", label, format_status(&val, client.color));
            } else {
                println!("  {:<18} {}", label, val);
            }
        }
    }

    // Crossing stats.
    let total_crossings = json_u64(&data, "total_crossings");
    let allowed = json_u64(&data, "crossings_allowed");
    let denied = json_u64(&data, "crossings_denied");
    let rate = json_f64(&data, "crossing_rate");

    println!();
    println!("  Crossing Stats:");
    println!("    Total:    {}", total_crossings);
    println!("    Allowed:  {}", allowed);
    println!(
        "    Denied:   {}",
        if denied > 0 && client.color {
            style(denied.to_string()).red().to_string()
        } else {
            denied.to_string()
        }
    );
    println!("    Rate:     {:.1}/min", rate);

    // Rules.
    if let Some(Value::Array(rules)) = data.get("rules") {
        println!();
        println!("  Permeability Rules:");
        for rule in rules {
            let rule_name = json_str(rule, "name");
            let allow = json_bool(rule, "allow");
            let category = json_str(rule, "category");
            let icon = if allow {
                if client.color {
                    style("[ALLOW]").green().to_string()
                } else {
                    "[ALLOW]".to_string()
                }
            } else {
                if client.color {
                    style("[DENY]").red().to_string()
                } else {
                    "[DENY]".to_string()
                }
            };
            println!("    {} {} ({})", icon, rule_name, category);
        }
    }

    println!();
    Ok(())
}

async fn cmd_constellation_treaties(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/constellation/treaties").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let treaties = data.as_array().cloned().unwrap_or_default();

    if treaties.is_empty() {
        client.info("No treaties.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Treaties ===").bold().cyan());
    } else {
        println!("=== Treaties ===");
    }
    println!();

    let headers = ["Name", "Remote Swarm", "Status", "Version", "Since"];
    let rows: Vec<Vec<String>> = treaties
        .iter()
        .map(|t| {
            let name = json_str(t, "name");
            let remote = json_str(t, "remote_swarm");
            let status = json_str(t, "status");
            let version = json_u64(t, "version");
            let since = json_str(t, "effective_since");

            vec![
                truncate(&name, 20),
                truncate(&remote, 16),
                format_status(&status, client.color),
                format!("v{}", version),
                if since != "-" {
                    format_relative_time(&since)
                } else {
                    "-".to_string()
                },
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_constellation_lending(client: &SwarmCtlClient) -> CtlResult<()> {
    let data = client.get("/api/v1/constellation/lending").await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let sessions = data.as_array().cloned().unwrap_or_default();

    if sessions.is_empty() {
        client.info("No active lending sessions.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Active Lending Sessions ===").bold().cyan());
    } else {
        println!("=== Active Lending Sessions ===");
    }
    println!();

    let headers = ["Session", "Remote Swarm", "Direction", "Nodes", "Duration", "Cost"];
    let rows: Vec<Vec<String>> = sessions
        .iter()
        .map(|s| {
            let session_id = json_str(s, "id");
            let remote = json_str(s, "remote_swarm");
            let direction = json_str(s, "direction");
            let nodes = json_u64(s, "node_count");
            let duration = json_u64(s, "duration_secs");
            let cost = json_f64(s, "accumulated_cost");

            vec![
                truncate(&session_id, 12),
                truncate(&remote, 16),
                if direction == "lending" {
                    if client.color {
                        style("-> OUT").yellow().to_string()
                    } else {
                        "-> OUT".to_string()
                    }
                } else {
                    if client.color {
                        style("<- IN").green().to_string()
                    } else {
                        "<- IN".to_string()
                    }
                },
                nodes.to_string(),
                format_duration_human(duration),
                format_currency(cost),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

/// Format a currency value in compact form.
fn format_currency(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("${:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("${:.0}K", value / 1_000.0)
    } else {
        format!("${:.2}", value)
    }
}

// ============================================================================
// Command: audit
// ============================================================================

async fn cmd_audit(client: &SwarmCtlClient, sub: AuditCommands) -> CtlResult<()> {
    match sub {
        AuditCommands::Log {
            actor,
            action,
            limit,
        } => cmd_audit_log(client, actor, action, limit).await,
        AuditCommands::Verify => cmd_audit_verify(client).await,
        AuditCommands::Export { format } => cmd_audit_export(client, &format).await,
    }
}

async fn cmd_audit_log(
    client: &SwarmCtlClient,
    actor: Option<String>,
    action: Option<String>,
    limit: usize,
) -> CtlResult<()> {
    let mut params = vec![format!("limit={}", limit)];
    if let Some(ref a) = actor {
        params.push(format!("actor={}", a));
    }
    if let Some(ref a) = action {
        params.push(format!("action={}", a));
    }
    let path = format!("/api/v1/audit/log?{}", params.join("&"));

    let data = client.get(&path).await?;

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let entries = data.as_array().cloned().unwrap_or_default();

    if entries.is_empty() {
        client.info("No audit entries found.");
        return Ok(());
    }

    if client.color {
        println!("{}", style("=== Audit Log ===").bold().cyan());
    } else {
        println!("=== Audit Log ===");
    }
    println!();

    let headers = ["Time", "Actor", "Action", "Target", "Outcome"];
    let rows: Vec<Vec<String>> = entries
        .iter()
        .map(|e| {
            let timestamp = json_str(e, "timestamp");
            let actor_str = e
                .get("actor")
                .and_then(|a| {
                    a.get("id")
                        .and_then(|i| i.as_str())
                        .map(String::from)
                        .or_else(|| a.as_str().map(String::from))
                })
                .unwrap_or_else(|| json_str(e, "actor"));
            let action_str = json_str(e, "action");
            let target_str = e
                .get("target")
                .and_then(|t| {
                    t.get("id")
                        .and_then(|i| i.as_str())
                        .map(String::from)
                        .or_else(|| t.as_str().map(String::from))
                })
                .unwrap_or_else(|| json_str(e, "target"));
            let outcome = json_str(e, "outcome");

            vec![
                if timestamp != "-" {
                    format_relative_time(&timestamp)
                } else {
                    "-".to_string()
                },
                truncate(&actor_str, 16),
                truncate(&action_str, 20),
                truncate(&target_str, 16),
                format_status(&outcome, client.color),
            ]
        })
        .collect();

    print_table(&headers, &rows, client.color);
    println!();

    Ok(())
}

async fn cmd_audit_verify(client: &SwarmCtlClient) -> CtlResult<()> {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.blue} {msg}")
            .expect("valid template"),
    );
    pb.set_message("Verifying audit chain integrity...");
    pb.enable_steady_tick(Duration::from_millis(100));

    let data = client.get("/api/v1/audit/verify").await?;
    pb.finish_and_clear();

    if client.output != OutputFormat::Text {
        client.print_value(&data);
        return Ok(());
    }

    let valid = json_bool(&data, "valid");
    let entries_checked = json_u64(&data, "entries_checked");
    let first_invalid = data
        .get("first_invalid_index")
        .and_then(|v| v.as_u64());

    if valid {
        client.success(&format!(
            "Audit chain integrity verified ({} entries checked)",
            entries_checked
        ));
    } else {
        let idx_str = first_invalid
            .map(|i| format!(" (first invalid at index {})", i))
            .unwrap_or_default();

        if client.color {
            println!(
                "  {} Audit chain integrity FAILED{} ({} entries checked)",
                style("[!!]").red().bold(),
                idx_str,
                entries_checked
            );
        } else {
            println!(
                "  [!!] Audit chain integrity FAILED{} ({} entries checked)",
                idx_str, entries_checked
            );
        }
    }

    println!();
    Ok(())
}

async fn cmd_audit_export(client: &SwarmCtlClient, format: &str) -> CtlResult<()> {
    let path = format!("/api/v1/audit/export?format={}", format);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.blue} {msg}")
            .expect("valid template"),
    );
    pb.set_message("Exporting audit log...");
    pb.enable_steady_tick(Duration::from_millis(100));

    let data = client.get_raw(&path).await?;
    pb.finish_and_clear();

    // Output directly to stdout.
    print!("{}", data);
    io::stdout().flush()?;

    if !client.quiet {
        eprintln!();
        client.info(&format!(
            "Exported {} bytes in {} format",
            data.len(),
            format
        ));
    }

    Ok(())
}

// ============================================================================
// Command: metrics
// ============================================================================

async fn cmd_metrics(client: &SwarmCtlClient) -> CtlResult<()> {
    // Metrics endpoint returns Prometheus text format.
    let data = client.get_raw("/api/v1/metrics").await?;

    if client.output == OutputFormat::Json {
        // Parse Prometheus text into JSON (simplified).
        let metrics = parse_prometheus_to_json(&data);
        client.print_value(&metrics);
        return Ok(());
    }

    // For text/csv/yaml, output raw Prometheus format.
    print!("{}", data);
    io::stdout().flush()?;

    Ok(())
}

/// Parse Prometheus text exposition format into a JSON object (simplified).
fn parse_prometheus_to_json(text: &str) -> Value {
    let mut metrics: HashMap<String, Value> = HashMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Simple parsing: "metric_name{labels} value" or "metric_name value"
        if let Some(space_pos) = line.rfind(' ') {
            let name_part = &line[..space_pos];
            let value_part = &line[space_pos + 1..];

            if let Ok(val) = value_part.parse::<f64>() {
                // Extract metric name (before any '{').
                let metric_name = if let Some(brace_pos) = name_part.find('{') {
                    &name_part[..brace_pos]
                } else {
                    name_part
                };

                metrics.insert(
                    metric_name.to_string(),
                    serde_json::json!(val),
                );
            }
        }
    }

    Value::Object(metrics.into_iter().collect())
}

// ============================================================================
// Command: tui (interactive dashboard)
// ============================================================================

/// TUI application state.
struct TuiApp {
    /// Which panel is focused.
    focused_panel: TuiPanel,
    /// Current center view mode.
    center_view: TuiCenterView,
    /// Cached data for rendering.
    psyche_data: Option<Value>,
    nodes_data: Option<Value>,
    jobs_data: Option<Value>,
    events_data: Option<Value>,
    alerts_data: Option<Value>,
    sla_data: Option<Value>,
    fleet_data: Option<Value>,
    health_data: Option<Value>,
    /// Scroll offset for lists.
    scroll_offset: usize,
    /// Whether the app should quit.
    should_quit: bool,
    /// Status bar message.
    status_message: String,
    /// Last refresh time.
    last_refresh: std::time::Instant,
    /// Refresh interval.
    refresh_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TuiPanel {
    Left,
    Center,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TuiCenterView {
    Nodes,
    Jobs,
    Events,
    Fleet,
}

impl TuiApp {
    fn new() -> Self {
        Self {
            focused_panel: TuiPanel::Center,
            center_view: TuiCenterView::Nodes,
            psyche_data: None,
            nodes_data: None,
            jobs_data: None,
            events_data: None,
            alerts_data: None,
            sla_data: None,
            fleet_data: None,
            health_data: None,
            scroll_offset: 0,
            should_quit: false,
            status_message: "Loading...".to_string(),
            last_refresh: std::time::Instant::now(),
            refresh_interval: Duration::from_secs(5),
        }
    }

    fn next_panel(&mut self) {
        self.focused_panel = match self.focused_panel {
            TuiPanel::Left => TuiPanel::Center,
            TuiPanel::Center => TuiPanel::Right,
            TuiPanel::Right => TuiPanel::Left,
        };
        self.scroll_offset = 0;
    }

    fn next_center_view(&mut self) {
        self.center_view = match self.center_view {
            TuiCenterView::Nodes => TuiCenterView::Jobs,
            TuiCenterView::Jobs => TuiCenterView::Events,
            TuiCenterView::Events => TuiCenterView::Fleet,
            TuiCenterView::Fleet => TuiCenterView::Nodes,
        };
        self.scroll_offset = 0;
    }

    fn scroll_down(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_add(1);
    }

    fn scroll_up(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    fn needs_refresh(&self) -> bool {
        self.last_refresh.elapsed() >= self.refresh_interval
    }

    async fn refresh_data(&mut self, client: &SwarmCtlClient) {
        // Fetch data from API in parallel.
        let (psyche, nodes, alerts, sla, health) = tokio::join!(
            client.get("/api/v1/psyche/status"),
            client.get("/api/v1/nodes"),
            client.get("/api/v1/alerts"),
            client.get("/api/v1/sla/status"),
            client.get("/api/v1/health"),
        );

        self.psyche_data = psyche.ok();
        self.nodes_data = nodes.ok();
        self.alerts_data = alerts.ok();
        self.sla_data = sla.ok();
        self.health_data = health.ok();

        // Fetch view-specific data.
        match self.center_view {
            TuiCenterView::Jobs => {
                self.jobs_data = client.get("/api/v1/jobs").await.ok();
            }
            TuiCenterView::Events => {
                self.events_data = client
                    .get("/api/v1/events?limit=50")
                    .await
                    .ok();
            }
            TuiCenterView::Fleet => {
                self.fleet_data = client.get("/api/v1/fleet/status").await.ok();
            }
            _ => {}
        }

        self.last_refresh = std::time::Instant::now();
        self.status_message = format!(
            "Last refresh: {} | Press 'q' to quit, Tab to switch panels",
            chrono::Local::now().format("%H:%M:%S")
        );
    }
}

async fn cmd_tui(client: &SwarmCtlClient) -> CtlResult<()> {
    use crossterm::{
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    };
    use ratatui::{
        backend::CrosstermBackend,
        Terminal,
    };

    // Setup terminal.
    enable_raw_mode().map_err(|e| CtlError::Other(format!("Failed to enable raw mode: {}", e)))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)
        .map_err(|e| CtlError::Other(format!("Failed to enter alternate screen: {}", e)))?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)
        .map_err(|e| CtlError::Other(format!("Failed to create terminal: {}", e)))?;

    let mut app = TuiApp::new();

    // Initial data load.
    app.refresh_data(client).await;

    let result = run_tui_loop(&mut terminal, &mut app, client).await;

    // Restore terminal.
    disable_raw_mode().ok();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();

    result
}

async fn run_tui_loop<B: ratatui::backend::Backend>(
    terminal: &mut ratatui::Terminal<B>,
    app: &mut TuiApp,
    client: &SwarmCtlClient,
) -> CtlResult<()> {
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use ratatui::layout::{Constraint, Direction, Layout};

    loop {
        if app.should_quit {
            break;
        }

        // Refresh data if needed.
        if app.needs_refresh() {
            app.refresh_data(client).await;
        }

        // Draw UI.
        terminal
            .draw(|f| {
                let size = f.area();

                // Main layout: header, body, footer.
                let main_chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(3),  // Header
                        Constraint::Min(10),    // Body
                        Constraint::Length(3),  // Footer
                    ])
                    .split(size);

                // -- Header --
                draw_tui_header(f, main_chunks[0], app);

                // -- Body: 3-column layout --
                let body_chunks = Layout::default()
                    .direction(Direction::Horizontal)
                    .constraints([
                        Constraint::Percentage(25), // Left
                        Constraint::Percentage(50), // Center
                        Constraint::Percentage(25), // Right
                    ])
                    .split(main_chunks[1]);

                draw_tui_left_panel(f, body_chunks[0], app);
                draw_tui_center_panel(f, body_chunks[1], app);
                draw_tui_right_panel(f, body_chunks[2], app);

                // -- Footer --
                draw_tui_footer(f, main_chunks[2], app);
            })
            .map_err(|e| CtlError::Other(format!("Draw error: {}", e)))?;

        // Handle input (with timeout for refresh).
        if crossterm::event::poll(Duration::from_millis(200))
            .map_err(|e| CtlError::Other(format!("Poll error: {}", e)))?
        {
            if let Event::Key(key) = event::read()
                .map_err(|e| CtlError::Other(format!("Read error: {}", e)))?
            {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            app.should_quit = true;
                        }
                        KeyCode::Tab => {
                            app.next_panel();
                        }
                        KeyCode::Char('1') => {
                            app.center_view = TuiCenterView::Nodes;
                            app.scroll_offset = 0;
                        }
                        KeyCode::Char('2') => {
                            app.center_view = TuiCenterView::Jobs;
                            app.scroll_offset = 0;
                        }
                        KeyCode::Char('3') => {
                            app.center_view = TuiCenterView::Events;
                            app.scroll_offset = 0;
                        }
                        KeyCode::Char('4') => {
                            app.center_view = TuiCenterView::Fleet;
                            app.scroll_offset = 0;
                        }
                        KeyCode::Char('v') => {
                            app.next_center_view();
                        }
                        KeyCode::Char('j') | KeyCode::Down => {
                            app.scroll_down();
                        }
                        KeyCode::Char('k') | KeyCode::Up => {
                            app.scroll_up();
                        }
                        KeyCode::Char('r') => {
                            app.refresh_data(client).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}

fn draw_tui_header(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    app: &TuiApp,
) {
    use ratatui::{
        style::{Color, Modifier, Style as RStyle},
        text::{Line, Span},
        widgets::{Block, Borders, Paragraph},
    };

    let health_status = app
        .health_data
        .as_ref()
        .and_then(|h| h.get("status").and_then(|s| s.as_str()))
        .unwrap_or("unknown");

    let node_count = app
        .nodes_data
        .as_ref()
        .and_then(|n| n.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    let archetype = app
        .psyche_data
        .as_ref()
        .and_then(|p| p.get("archetype").and_then(|a| a.as_str()))
        .unwrap_or("--");

    let uptime = app
        .health_data
        .as_ref()
        .and_then(|h| h.get("uptime_secs").and_then(|u| u.as_u64()))
        .map(format_duration_human)
        .unwrap_or_else(|| "--".to_string());

    let status_color = match health_status {
        "healthy" | "ok" => Color::Green,
        "degraded" | "warning" => Color::Yellow,
        _ => Color::Red,
    };

    let header_line = Line::from(vec![
        Span::styled(" Marabunta Swarm ", RStyle::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(" | "),
        Span::styled(
            format!("Health: {}", health_status),
            RStyle::default().fg(status_color),
        ),
        Span::raw(" | "),
        Span::raw(format!("Nodes: {}", node_count)),
        Span::raw(" | "),
        Span::raw(format!("Uptime: {}", uptime)),
        Span::raw(" | "),
        Span::styled(
            format!("Archetype: {}", archetype),
            RStyle::default().fg(Color::Magenta),
        ),
    ]);

    let header = Paragraph::new(header_line).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" swarmctl ")
            .border_style(RStyle::default().fg(Color::Cyan)),
    );

    f.render_widget(header, area);
}

fn draw_tui_left_panel(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    app: &TuiApp,
) {
    use ratatui::{
        layout::{Constraint, Direction, Layout},
        style::{Color, Modifier, Style as RStyle},
        text::{Line, Span},
        widgets::{Block, Borders, List, ListItem, Paragraph},
    };

    let border_color = if app.focused_panel == TuiPanel::Left {
        Color::Cyan
    } else {
        Color::DarkGray
    };

    // Split left panel into psyche bars and archetype info.
    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(70), // Psyche facets
            Constraint::Percentage(30), // Archetype info
        ])
        .split(area);

    // Psyche facets.
    let facet_names = [
        "exertion",
        "vitality",
        "resilience",
        "cohesion",
        "curiosity",
        "anxiety",
        "focus",
        "satisfaction",
    ];

    let facets = app
        .psyche_data
        .as_ref()
        .and_then(|p| p.get("facets").and_then(|f| f.as_object()))
        .cloned()
        .unwrap_or_default();

    let mut facet_items: Vec<ListItem> = Vec::new();
    for name in &facet_names {
        let value = facets
            .get(*name)
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let bar_width: usize = 10;
        let filled = ((value / 100.0) * bar_width as f64).round() as usize;
        let empty = bar_width.saturating_sub(filled);
        let bar_str = format!(
            "{}{} {:>3.0}%",
            "#".repeat(filled),
            ".".repeat(empty),
            value
        );

        let bar_color = if value >= 80.0 {
            Color::Green
        } else if value >= 50.0 {
            Color::Yellow
        } else {
            Color::Red
        };

        // Special case: anxiety is reversed (low is good).
        let bar_color = if *name == "anxiety" {
            if value <= 30.0 {
                Color::Green
            } else if value <= 60.0 {
                Color::Yellow
            } else {
                Color::Red
            }
        } else {
            bar_color
        };

        let item = ListItem::new(Line::from(vec![
            Span::styled(
                format!("{:<12}", capitalize(name)),
                RStyle::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(bar_str, RStyle::default().fg(bar_color)),
        ]));
        facet_items.push(item);
    }

    let facets_widget = List::new(facet_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Psyche ")
            .border_style(RStyle::default().fg(border_color)),
    );
    f.render_widget(facets_widget, left_chunks[0]);

    // Archetype info.
    let archetype = app
        .psyche_data
        .as_ref()
        .and_then(|p| p.get("archetype").and_then(|a| a.as_str()))
        .unwrap_or("Unknown");

    let arch_paragraph = Paragraph::new(vec![
        Line::from(Span::styled(
            archetype,
            RStyle::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        )),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Archetype ")
            .border_style(RStyle::default().fg(border_color)),
    );
    f.render_widget(arch_paragraph, left_chunks[1]);
}

fn draw_tui_center_panel(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    app: &TuiApp,
) {
    use ratatui::{
        style::{Color, Modifier, Style as RStyle},
        text::{Line, Span},
        widgets::{Block, Borders, List, ListItem, Paragraph},
    };

    let border_color = if app.focused_panel == TuiPanel::Center {
        Color::Cyan
    } else {
        Color::DarkGray
    };

    let view_label = match app.center_view {
        TuiCenterView::Nodes => " Nodes [1] ",
        TuiCenterView::Jobs => " Jobs [2] ",
        TuiCenterView::Events => " Events [3] ",
        TuiCenterView::Fleet => " Fleet [4] ",
    };

    match app.center_view {
        TuiCenterView::Nodes => {
            let nodes = app
                .nodes_data
                .as_ref()
                .and_then(|n| n.as_array())
                .cloned()
                .unwrap_or_default();

            let items: Vec<ListItem> = nodes
                .iter()
                .skip(app.scroll_offset)
                .map(|n| {
                    let id = json_str(n, "id");
                    let status = json_str(n, "status");
                    let load = json_f64(n, "load");

                    let status_color = match status.to_lowercase().as_str() {
                        "alive" => Color::Green,
                        "suspect" | "draining" => Color::Yellow,
                        "dead" | "quarantined" => Color::Red,
                        _ => Color::DarkGray,
                    };

                    let load_bar_width = 8;
                    let filled = ((load * load_bar_width as f64).round() as usize).min(load_bar_width);
                    let empty = load_bar_width.saturating_sub(filled);
                    let load_bar = format!("[{}{}]", "#".repeat(filled), ".".repeat(empty));

                    ListItem::new(Line::from(vec![
                        Span::raw(format!("{:<16} ", truncate(&id, 15))),
                        Span::styled(
                            format!("{:<12}", status),
                            RStyle::default().fg(status_color),
                        ),
                        Span::raw(load_bar),
                    ]))
                })
                .collect();

            let widget = List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(view_label)
                    .border_style(RStyle::default().fg(border_color)),
            );
            f.render_widget(widget, area);
        }
        TuiCenterView::Jobs => {
            let jobs = app
                .jobs_data
                .as_ref()
                .and_then(|j| j.as_array())
                .cloned()
                .unwrap_or_default();

            let items: Vec<ListItem> = jobs
                .iter()
                .skip(app.scroll_offset)
                .map(|j| {
                    let id = json_str(j, "id");
                    let name = json_str(j, "name");
                    let status = json_str(j, "status");
                    let progress = json_f64(j, "progress");

                    let status_color = match status.to_lowercase().as_str() {
                        "running" | "executing" => Color::Green,
                        "pending" => Color::Yellow,
                        "failed" => Color::Red,
                        "completed" => Color::Cyan,
                        _ => Color::DarkGray,
                    };

                    let prog_width: usize = 6;
                    let filled = ((progress / 100.0) * prog_width as f64).round() as usize;
                    let empty = prog_width.saturating_sub(filled);
                    let prog_bar = format!("[{}{}]", "#".repeat(filled), ".".repeat(empty));

                    ListItem::new(Line::from(vec![
                        Span::raw(format!("{:<10} ", truncate(&id, 9))),
                        Span::raw(format!("{:<16} ", truncate(&name, 15))),
                        Span::styled(
                            format!("{:<10}", status),
                            RStyle::default().fg(status_color),
                        ),
                        Span::raw(prog_bar),
                    ]))
                })
                .collect();

            let widget = List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(view_label)
                    .border_style(RStyle::default().fg(border_color)),
            );
            f.render_widget(widget, area);
        }
        TuiCenterView::Events => {
            let events = app
                .events_data
                .as_ref()
                .and_then(|e| e.as_array())
                .cloned()
                .unwrap_or_default();

            let items: Vec<ListItem> = events
                .iter()
                .skip(app.scroll_offset)
                .map(|e| {
                    let sev = json_str(e, "severity");
                    let domain = json_str(e, "domain");
                    let summary = json_str(e, "summary");
                    let timestamp = json_str(e, "timestamp");

                    let sev_color = match sev.to_lowercase().as_str() {
                        "critical" | "error" => Color::Red,
                        "warning" | "warn" => Color::Yellow,
                        "info" => Color::Blue,
                        _ => Color::DarkGray,
                    };

                    let sev_icon = match sev.to_lowercase().as_str() {
                        "critical" | "error" => "!!",
                        "warning" | "warn" => " !",
                        "info" => " i",
                        _ => " .",
                    };

                    let time_str = if timestamp != "-" {
                        format_relative_time(&timestamp)
                    } else {
                        "-".to_string()
                    };

                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("[{}] ", sev_icon),
                            RStyle::default().fg(sev_color),
                        ),
                        Span::styled(
                            format!("{:<8} ", truncate(&time_str, 7)),
                            RStyle::default().fg(Color::DarkGray),
                        ),
                        Span::raw(format!("[{}] ", truncate(&domain, 8))),
                        Span::raw(truncate(&summary, 40)),
                    ]))
                })
                .collect();

            let widget = List::new(items).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(view_label)
                    .border_style(RStyle::default().fg(border_color)),
            );
            f.render_widget(widget, area);
        }
        TuiCenterView::Fleet => {
            let fleet = app
                .fleet_data
                .as_ref()
                .cloned()
                .unwrap_or(Value::Null);

            let mut lines = Vec::new();

            let total = json_u64(&fleet, "total_nodes");
            lines.push(Line::from(vec![
                Span::styled("Total Nodes: ", RStyle::default().add_modifier(Modifier::BOLD)),
                Span::raw(total.to_string()),
            ]));

            if let Some(counts) = fleet.get("counts").and_then(|c| c.as_object()) {
                lines.push(Line::from(""));
                for (state, count) in counts {
                    let c = count.as_u64().unwrap_or(0);
                    let color = match state.to_lowercase().as_str() {
                        "alive" => Color::Green,
                        "suspect" | "draining" | "cordoned" => Color::Yellow,
                        "dead" | "quarantined" => Color::Red,
                        _ => Color::DarkGray,
                    };
                    lines.push(Line::from(vec![
                        Span::styled(
                            format!("  {:<14}", state),
                            RStyle::default().fg(color),
                        ),
                        Span::raw(c.to_string()),
                    ]));
                }
            }

            // Active update.
            if let Some(update) = fleet.get("active_update") {
                if !update.is_null() {
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled(
                        "Rolling Update:",
                        RStyle::default().add_modifier(Modifier::BOLD),
                    )));
                    let version = json_str(update, "target_version");
                    let phase = json_str(update, "phase");
                    let progress = json_f64(update, "progress");
                    lines.push(Line::from(format!("  Target:   {}", version)));
                    lines.push(Line::from(format!("  Phase:    {}", phase)));
                    lines.push(Line::from(format!("  Progress: {:.0}%", progress)));
                }
            }

            let widget = Paragraph::new(lines).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(view_label)
                    .border_style(RStyle::default().fg(border_color)),
            );
            f.render_widget(widget, area);
        }
    }
}

fn draw_tui_right_panel(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    app: &TuiApp,
) {
    use ratatui::{
        layout::{Constraint, Direction, Layout},
        style::{Color, Style as RStyle},
        text::{Line, Span},
        widgets::{Block, Borders, List, ListItem},
    };

    let border_color = if app.focused_panel == TuiPanel::Right {
        Color::Cyan
    } else {
        Color::DarkGray
    };

    // Split right panel: alerts on top, SLA on bottom.
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(60), // Alerts
            Constraint::Percentage(40), // SLA
        ])
        .split(area);

    // Alerts.
    let alerts = app
        .alerts_data
        .as_ref()
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();

    let alert_items: Vec<ListItem> = if alerts.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  No active alerts",
            RStyle::default().fg(Color::DarkGray),
        )))]
    } else {
        alerts
            .iter()
            .map(|a| {
                let name = json_str(a, "name");
                let sev = json_str(a, "severity");

                let sev_color = match sev.to_lowercase().as_str() {
                    "critical" | "error" => Color::Red,
                    "warning" | "warn" => Color::Yellow,
                    _ => Color::Blue,
                };

                let sev_icon = match sev.to_lowercase().as_str() {
                    "critical" | "error" => "!!",
                    "warning" | "warn" => " !",
                    _ => " i",
                };

                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("[{}] ", sev_icon),
                        RStyle::default().fg(sev_color),
                    ),
                    Span::raw(truncate(&name, 20)),
                ]))
            })
            .collect()
    };

    let alerts_widget = List::new(alert_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Alerts ({}) ", alerts.len()))
            .border_style(RStyle::default().fg(border_color)),
    );
    f.render_widget(alerts_widget, right_chunks[0]);

    // SLA status.
    let slas = app
        .sla_data
        .as_ref()
        .and_then(|s| s.as_array())
        .cloned()
        .unwrap_or_default();

    let sla_items: Vec<ListItem> = if slas.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "  No SLAs defined",
            RStyle::default().fg(Color::DarkGray),
        )))]
    } else {
        slas.iter()
            .map(|s| {
                let name = json_str(s, "name");
                let compliance = json_f64(s, "compliance_pct");

                let color = if compliance >= 99.0 {
                    Color::Green
                } else if compliance >= 95.0 {
                    Color::Yellow
                } else {
                    Color::Red
                };

                let bar_width: usize = 8;
                let filled =
                    ((compliance / 100.0) * bar_width as f64).round() as usize;
                let empty = bar_width.saturating_sub(filled);
                let bar = format!("[{}{}]", "#".repeat(filled), ".".repeat(empty));

                ListItem::new(Line::from(vec![
                    Span::raw(format!("{:<10} ", truncate(&name, 9))),
                    Span::styled(bar, RStyle::default().fg(color)),
                    Span::styled(
                        format!(" {:.1}%", compliance),
                        RStyle::default().fg(color),
                    ),
                ]))
            })
            .collect()
    };

    let sla_widget = List::new(sla_items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" SLA ")
            .border_style(RStyle::default().fg(border_color)),
    );
    f.render_widget(sla_widget, right_chunks[1]);
}

fn draw_tui_footer(
    f: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    app: &TuiApp,
) {
    use ratatui::{
        style::{Color, Modifier, Style as RStyle},
        text::{Line, Span},
        widgets::{Block, Borders, Paragraph},
    };

    let help_line = Line::from(vec![
        Span::styled(" q", RStyle::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(":quit "),
        Span::styled("Tab", RStyle::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(":panel "),
        Span::styled("1-4", RStyle::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(":view "),
        Span::styled("j/k", RStyle::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(":scroll "),
        Span::styled("r", RStyle::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw(":refresh "),
        Span::raw("| "),
        Span::styled(&app.status_message, RStyle::default().fg(Color::DarkGray)),
    ]);

    let footer = Paragraph::new(help_line).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(RStyle::default().fg(Color::DarkGray)),
    );

    f.render_widget(footer, area);
}

// ============================================================================
// Command: completions
// ============================================================================

fn cmd_completions(shell: ShellType, _client: &SwarmCtlClient) {
    // Build the list of subcommands from the CLI definition.
    let cmd = Cli::command();
    let subcommands: Vec<String> = cmd
        .get_subcommands()
        .map(|s| s.get_name().to_string())
        .collect();

    let name = "swarmctl";

    match shell {
        ShellType::Bash => {
            println!(r#"# Bash completion for {name}"#);
            println!(r#"_{name}() {{"#);
            println!(r#"    local cur prev commands"#);
            println!(r#"    COMPREPLY=()"#);
            println!(r#"    cur="${{COMP_WORDS[COMP_CWORD]}}""#);
            println!(r#"    prev="${{COMP_WORDS[COMP_CWORD-1]}}""#);
            println!(r#""#);
            println!(
                r#"    commands="{}""#,
                subcommands.join(" ")
            );
            println!(r#""#);
            println!(r#"    if [[ ${{COMP_CWORD}} -eq 1 ]]; then"#);
            println!(r#"        COMPREPLY=( $(compgen -W "${{commands}}" -- "${{cur}}") )"#);
            println!(r#"        return 0"#);
            println!(r#"    fi"#);
            println!(r#""#);
            println!(r#"    case "${{prev}}" in"#);
            println!(r#"        --output)"#);
            println!(r#"            COMPREPLY=( $(compgen -W "text json yaml csv" -- "${{cur}}") )"#);
            println!(r#"            return 0"#);
            println!(r#"            ;;"#);
            println!(r#"    esac"#);
            println!(r#"}}"#);
            println!(r#""#);
            println!(r#"complete -F _{name} {name}"#);
        }
        ShellType::Zsh => {
            println!(r#"#compdef {name}"#);
            println!(r#""#);
            println!(r#"_{name}() {{"#);
            println!(r#"    local -a commands"#);
            println!(r#"    commands=("#);
            for sub in &subcommands {
                println!(r#"        '{}:{}'"#, sub, sub);
            }
            println!(r#"    )"#);
            println!(r#""#);
            println!(r#"    _arguments -C \"#);
            println!(r#"        '--url[API endpoint URL]:url:' \"#);
            println!(r#"        '--token[Auth token]:token:' \"#);
            println!(r#"        '--output[Output format]:format:(text json yaml csv)' \"#);
            println!(r#"        '--verbose[Enable verbose output]' \"#);
            println!(r#"        '--quiet[Suppress output]' \"#);
            println!(r#"        '--no-color[Disable colors]' \"#);
            println!(r#"        '--config[Config file path]:file:_files' \"#);
            println!(r#"        '1:command:->commands' \"#);
            println!(r#"        '*::arg:->args'"#);
            println!(r#""#);
            println!(r#"    case "$state" in"#);
            println!(r#"        commands)"#);
            println!(r#"            _describe 'command' commands"#);
            println!(r#"            ;;"#);
            println!(r#"    esac"#);
            println!(r#"}}"#);
            println!(r#""#);
            println!(r#"_{name}"#);
        }
        ShellType::Fish => {
            println!(r#"# Fish completion for {name}"#);
            for sub in &subcommands {
                println!(
                    r#"complete -c {name} -n '__fish_use_subcommand' -a '{sub}' -d '{sub}'"#,
                );
            }
            println!(
                r#"complete -c {name} -l output -xa 'text json yaml csv' -d 'Output format'"#,
            );
            println!(r#"complete -c {name} -l url -d 'API endpoint URL'"#);
            println!(r#"complete -c {name} -l token -d 'Auth token'"#);
            println!(r#"complete -c {name} -l verbose -d 'Enable verbose output'"#);
            println!(r#"complete -c {name} -l quiet -d 'Suppress output'"#);
            println!(r#"complete -c {name} -l no-color -d 'Disable colors'"#);
            println!(
                r#"complete -c {name} -l config -rF -d 'Config file path'"#,
            );
        }
        ShellType::Powershell => {
            println!(r#"# PowerShell completion for {name}"#);
            println!(r#"Register-ArgumentCompleter -CommandName {name} -ScriptBlock {{"#);
            println!(r#"    param($commandName, $wordToComplete, $cursorPosition)"#);
            println!(r#""#);
            println!(r#"    $commands = @({})"#, subcommands
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", "));
            println!(r#""#);
            println!(r#"    $commands | Where-Object {{ $_ -like "$wordToComplete*" }} | ForEach-Object {{"#);
            println!(r#"        [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)"#);
            println!(r#"    }}"#);
            println!(r#"}}"#);
        }
        ShellType::Elvish => {
            println!(r#"# Elvish completion for {name}"#);
            println!(r#"edit:completion:arg-completer[{name}] = {{|@args|"#);
            println!(r#"    var commands = [{}]"#, subcommands
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(" "));
            println!(r#"    if (eq (count $args) 2) {{"#);
            println!(r#"        each {{|c| put $c}} $commands"#);
            println!(r#"    }}"#);
            println!(r#"}}"#);
        }
    }
}

// ============================================================================
// Command: version
// ============================================================================

fn cmd_version(client: &SwarmCtlClient) {
    if client.output == OutputFormat::Json {
        let version_info = serde_json::json!({
            "name": "swarmctl",
            "version": VERSION,
            "user_agent": USER_AGENT,
        });
        client.print_value(&version_info);
        return;
    }

    if client.color {
        println!(
            "{} {}",
            style("swarmctl").bold().cyan(),
            style(VERSION).bold()
        );
    } else {
        println!("swarmctl {}", VERSION);
    }
    println!("Marabunta Swarm management CLI");
    println!("Part of the Marabunta Compute project");
}
