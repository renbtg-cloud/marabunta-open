// Marabunta - Licensed under the MIT License.
use clap::{Parser, ValueEnum};
use serde::Serialize;

/// Global flags available on every marabunta observe subcommand.
#[derive(Debug, Clone, Parser)]
pub struct GlobalOpts {
    /// API server URL (e.g., http://localhost:3000).
    #[arg(long, env = "MARABUNTA_API_URL", default_value = "http://localhost:3000")]
    pub api_url: String,

    /// Bearer token for API authentication.
    #[arg(long, env = "MARABUNTA_TOKEN")]
    pub token: Option<String>,

    /// Output format.
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,

    /// Disable colored output.
    #[arg(long)]
    pub no_color: bool,

    /// Answer yes to all confirmation prompts (for scripting).
    #[arg(long, short = 'y')]
    pub yes: bool,

    /// Config file path.
    #[arg(long, default_value = "~/.marabunta/config.toml")]
    pub config: String,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
    Jsonl,
}

/// HTTP client wrapper for calling the marabunta API.
pub struct MarabuntaClient {
    base_url: String,
    token: Option<String>,
    client: reqwest::Client,
}

impl MarabuntaClient {
    pub fn new(opts: &GlobalOpts) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client");

        Self {
            base_url: opts.api_url.trim_end_matches('/').to_string(),
            token: opts.token.clone(),
            client,
        }
    }

    /// GET request to the API.
    pub async fn get(&self, path: &str) -> Result<reqwest::Response, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.get(&url);
        if let Some(ref token) = self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        req.send()
            .await
            .map_err(|e| CliError::Network(e.to_string()))
    }

    /// POST request with JSON body.
    pub async fn post(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<reqwest::Response, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.post(&url).json(body);
        if let Some(ref token) = self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        req.send()
            .await
            .map_err(|e| CliError::Network(e.to_string()))
    }

    /// DELETE request.
    pub async fn delete(&self, path: &str) -> Result<reqwest::Response, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.delete(&url);
        if let Some(ref token) = self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        req.send()
            .await
            .map_err(|e| CliError::Network(e.to_string()))
    }

    /// SSE stream request.
    pub async fn sse(&self, path: &str) -> Result<reqwest::Response, CliError> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.get(&url).header("Accept", "text/event-stream");
        if let Some(ref token) = self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        req.send()
            .await
            .map_err(|e| CliError::Network(e.to_string()))
    }
}

/// CLI-specific error type.
pub enum CliError {
    /// Network/connection error.
    Network(String),
    /// API returned an error response.
    Api {
        status: u16,
        body: serde_json::Value,
    },
    /// Local error (config parsing, etc.).
    Local(String),
}

impl CliError {
    /// Map to exit code per OAI spec.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Api { body, .. } => {
                if let Some(code) = body.get("code").and_then(|c| c.as_str()) {
                    match code {
                        "GUARD_VIOLATED" | "VERSION_MISMATCH" | "STALE_OBSERVATION" => {
                            super::errors::EXIT_GUARD_FAILED
                        }
                        "INTERVENTION_CONFLICT" => super::errors::EXIT_CONFLICT,
                        _ => super::errors::EXIT_ERROR,
                    }
                } else {
                    super::errors::EXIT_ERROR
                }
            }
            _ => super::errors::EXIT_ERROR,
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Network(msg) => write!(f, "network error: {}", msg),
            Self::Api { status, body } => {
                if let Some(msg) = body.get("error").and_then(|e| e.as_str()) {
                    write!(f, "API error ({}): {}", status, msg)
                } else {
                    write!(f, "API error ({}): {}", status, body)
                }
            }
            Self::Local(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::fmt::Debug for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

/// Format and print a value according to the output format.
pub fn print_output<T: Serialize>(value: &T, format: OutputFormat) {
    match format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(value).unwrap_or_default()
            );
        }
        OutputFormat::Jsonl => {
            println!("{}", serde_json::to_string(value).unwrap_or_default());
        }
        OutputFormat::Csv => {
            if let Ok(val) = serde_json::to_value(value) {
                if let Some(obj) = val.as_object() {
                    let keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
                    println!("{}", keys.join(","));
                    let vals: Vec<String> = obj
                        .values()
                        .map(format_value)
                        .collect();
                    println!("{}", vals.join(","));
                }
            }
        }
        OutputFormat::Table => {
            if let Ok(val) = serde_json::to_value(value) {
                if let Some(obj) = val.as_object() {
                    let max_key_len = obj.keys().map(|k| k.len()).max().unwrap_or(0);
                    for (k, v) in obj {
                        println!(
                            "{:>width$}: {}",
                            k,
                            format_value(v),
                            width = max_key_len
                        );
                    }
                } else {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(value).unwrap_or_default()
                    );
                }
            }
        }
    }
}

/// Format a list of items as a table.
pub fn print_table<T: Serialize>(items: &[T], columns: &[&str], format: OutputFormat) {
    match format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(items).unwrap_or_default()
            );
        }
        OutputFormat::Jsonl => {
            for item in items {
                println!("{}", serde_json::to_string(item).unwrap_or_default());
            }
        }
        OutputFormat::Csv => {
            println!("{}", columns.join(","));
            for item in items {
                if let Ok(val) = serde_json::to_value(item) {
                    if let Some(obj) = val.as_object() {
                        let row: Vec<String> = columns
                            .iter()
                            .map(|c| {
                                obj.get(*c)
                                    .map(format_value)
                                    .unwrap_or_default()
                            })
                            .collect();
                        println!("{}", row.join(","));
                    }
                }
            }
        }
        OutputFormat::Table => {
            let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
            let rows: Vec<Vec<String>> = items
                .iter()
                .map(|item| {
                    if let Ok(val) = serde_json::to_value(item) {
                        if let Some(obj) = val.as_object() {
                            columns
                                .iter()
                                .enumerate()
                                .map(|(i, c)| {
                                    let s = obj
                                        .get(*c)
                                        .map(format_value)
                                        .unwrap_or_default();
                                    widths[i] = widths[i].max(s.len());
                                    s
                                })
                                .collect()
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    }
                })
                .collect();

            // Header
            let header: String = columns
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{:<width$}", c.to_uppercase(), width = widths[i]))
                .collect::<Vec<_>>()
                .join("  ");
            println!("{}", header);
            println!("{}", "-".repeat(header.len()));

            // Rows
            for row in &rows {
                let line: String = row
                    .iter()
                    .enumerate()
                    .map(|(i, v)| format!("{:<width$}", v, width = widths[i]))
                    .collect::<Vec<_>>()
                    .join("  ");
                println!("{}", line);
            }
        }
    }
}

pub fn format_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "-".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => v.to_string(),
    }
}

/// Ask the user to confirm an action.
pub fn confirm(message: &str, yes: bool) -> bool {
    if yes {
        return true;
    }
    eprint!("{} [y/N] ", message);
    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_err() {
        return false;
    }
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

/// Display an intervention impact assessment and ask for confirmation.
pub fn confirm_intervention(
    action: &str,
    target: &str,
    impact_summary: Option<&str>,
    guard_info: Option<&str>,
    yes: bool,
) -> bool {
    eprintln!("Action: {} on {}", action, target);
    if let Some(impact) = impact_summary {
        eprintln!("Impact: {}", impact);
    }
    if let Some(guard) = guard_info {
        eprintln!("Guard:  {}", guard);
    }
    confirm("Proceed?", yes)
}

/// CLI configuration loaded from ~/.marabunta/config.toml.
#[derive(Debug, Clone, Serialize, serde::Deserialize, Default)]
pub struct CliConfig {
    pub api_url: Option<String>,
    pub token: Option<String>,
    pub format: Option<String>,
    pub no_color: Option<bool>,
}

impl CliConfig {
    /// Load from path. Returns default if file doesn't exist.
    pub fn load(path: &str) -> Self {
        let expanded = expand_tilde(path);
        match std::fs::read_to_string(&expanded) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    /// Apply config defaults to GlobalOpts (opts take precedence).
    pub fn apply_to(&self, opts: &mut GlobalOpts) {
        if opts.token.is_none() {
            opts.token.clone_from(&self.token);
        }
        if let Some(ref url) = self.api_url {
            if opts.api_url == "http://localhost:3000" {
                opts.api_url.clone_from(url);
            }
        }
    }
}

fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}/{}", home, rest);
        }
    }
    path.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_value_types() {
        assert_eq!(format_value(&serde_json::json!("hello")), "hello");
        assert_eq!(format_value(&serde_json::json!(null)), "-");
        assert_eq!(format_value(&serde_json::json!(true)), "true");
        assert_eq!(format_value(&serde_json::json!(42)), "42");
        assert_eq!(format_value(&serde_json::json!(3.14)), "3.14");
    }

    #[test]
    fn test_cli_error_exit_codes() {
        let guard_err = CliError::Api {
            status: 409,
            body: serde_json::json!({"code": "GUARD_VIOLATED"}),
        };
        assert_eq!(guard_err.exit_code(), super::super::errors::EXIT_GUARD_FAILED);

        let conflict_err = CliError::Api {
            status: 409,
            body: serde_json::json!({"code": "INTERVENTION_CONFLICT"}),
        };
        assert_eq!(conflict_err.exit_code(), super::super::errors::EXIT_CONFLICT);

        let network_err = CliError::Network("timeout".to_string());
        assert_eq!(network_err.exit_code(), super::super::errors::EXIT_ERROR);
    }

    #[test]
    fn test_cli_error_display() {
        let err = CliError::Network("connection refused".to_string());
        assert!(err.to_string().contains("connection refused"));

        let err = CliError::Api {
            status: 404,
            body: serde_json::json!({"error": "not found"}),
        };
        assert!(err.to_string().contains("not found"));

        let err = CliError::Local("bad config".to_string());
        assert!(err.to_string().contains("bad config"));
    }

    #[test]
    fn test_confirm_yes_flag() {
        assert!(confirm("test?", true));
    }

    #[test]
    fn test_cli_config_load_default() {
        let config = CliConfig::load("/nonexistent/path/config.toml");
        assert!(config.api_url.is_none());
        assert!(config.token.is_none());
    }

    #[test]
    fn test_cli_config_apply() {
        let config = CliConfig {
            api_url: Some("http://custom:9000".to_string()),
            token: Some("tok123".to_string()),
            format: None,
            no_color: None,
        };
        let mut opts = GlobalOpts {
            api_url: "http://localhost:3000".to_string(),
            token: None,
            format: OutputFormat::Table,
            no_color: false,
            yes: false,
            config: "".to_string(),
        };
        config.apply_to(&mut opts);
        assert_eq!(opts.api_url, "http://custom:9000");
        assert_eq!(opts.token, Some("tok123".to_string()));
    }

    #[test]
    fn test_expand_tilde() {
        // Only test non-home expansion since HOME may vary
        assert_eq!(expand_tilde("/absolute/path"), "/absolute/path");
        assert_eq!(expand_tilde("relative/path"), "relative/path");
    }

    #[test]
    fn test_output_format_json_roundtrip() {
        // Verify JSON format produces valid JSON
        let data = serde_json::json!({"key": "value", "num": 42});
        let json_str = serde_json::to_string_pretty(&data).unwrap_or_default();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");
        assert_eq!(parsed["key"], "value");
    }
}
