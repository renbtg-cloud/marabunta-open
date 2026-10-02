// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Total time budget for the entire switchboard pipeline (ingest → classify → menu → compute).
pub const PIPELINE_BUDGET: Duration = Duration::from_secs(2);

/// Maximum time for ingestion (OCR + parsing).
pub const INGEST_TIMEOUT: Duration = Duration::from_millis(800);

/// Maximum time for classification.
pub const CLASSIFY_TIMEOUT: Duration = Duration::from_millis(200);

/// Maximum time for a single oracle evaluation.
pub const ORACLE_TIMEOUT: Duration = Duration::from_millis(1500);

/// Maximum time for menu generation.
pub const MENU_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum input size in bytes (LaTeX/plain text).
pub const MAX_INPUT_SIZE: usize = 10_000;

/// Maximum image size in bytes (10 MB).
pub const MAX_IMAGE_SIZE: usize = 10 * 1024 * 1024;

/// Session TTL (30 minutes).
pub const SESSION_TTL: Duration = Duration::from_secs(30 * 60);

/// How often to clean up expired sessions.
pub const SESSION_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum concurrent sessions.
pub const MAX_SESSIONS: usize = 10_000;

/// Rate limit: max requests per minute per user.
pub const RATE_LIMIT_PER_MINUTE: u32 = 30;

/// Rate limit: max requests per hour per user.
pub const RATE_LIMIT_PER_HOUR: u32 = 300;

/// SymPy subprocess pool size.
pub const SYMPY_POOL_SIZE: usize = 3;

/// SymPy subprocess idle timeout before recycling.
pub const SYMPY_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum expression nesting depth (prevent stack overflow in parser).
pub const MAX_NESTING_DEPTH: usize = 50;

/// Confidence threshold below which classification is considered uncertain.
pub const CLASSIFICATION_CONFIDENCE_THRESHOLD: f32 = 0.5;

/// Number of menu options to show by default.
pub const DEFAULT_MENU_SIZE: usize = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchboardConfig {
    /// Whether the switchboard is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Mathpix API key for OCR (None = napkin photo disabled).
    #[serde(default)]
    pub mathpix_app_id: Option<String>,
    #[serde(default)]
    pub mathpix_app_key: Option<String>,

    /// Wolfram Alpha App ID (None = Wolfram handler disabled).
    #[serde(default)]
    pub wolfram_app_id: Option<String>,

    /// Pipeline budget override.
    #[serde(default = "default_pipeline_budget", with = "humantime_serde")]
    pub pipeline_budget: Duration,

    /// Oracle timeout override.
    #[serde(default = "default_oracle_timeout", with = "humantime_serde")]
    pub oracle_timeout: Duration,

    /// Maximum concurrent sessions.
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,

    /// Rate limit per minute per user.
    #[serde(default = "default_rate_per_minute")]
    pub rate_limit_per_minute: u32,

    /// Rate limit per hour per user.
    #[serde(default = "default_rate_per_hour")]
    pub rate_limit_per_hour: u32,

    /// SymPy pool size.
    #[serde(default = "default_sympy_pool")]
    pub sympy_pool_size: usize,

    /// Session TTL.
    #[serde(default = "default_session_ttl", with = "humantime_serde")]
    pub session_ttl: Duration,

    /// Default number of menu options to present.
    #[serde(default = "default_menu_size")]
    pub menu_size: usize,

    /// Maximum LaTeX/plaintext input size in bytes.
    #[serde(default = "default_max_input_size")]
    pub max_input_size: usize,
}

impl Default for SwitchboardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mathpix_app_id: None,
            mathpix_app_key: None,
            wolfram_app_id: None,
            pipeline_budget: PIPELINE_BUDGET,
            oracle_timeout: ORACLE_TIMEOUT,
            max_sessions: MAX_SESSIONS,
            rate_limit_per_minute: RATE_LIMIT_PER_MINUTE,
            rate_limit_per_hour: RATE_LIMIT_PER_HOUR,
            sympy_pool_size: SYMPY_POOL_SIZE,
            session_ttl: SESSION_TTL,
            menu_size: DEFAULT_MENU_SIZE,
            max_input_size: MAX_INPUT_SIZE,
        }
    }
}

// Default helper functions for serde
fn default_pipeline_budget() -> Duration {
    PIPELINE_BUDGET
}

fn default_oracle_timeout() -> Duration {
    ORACLE_TIMEOUT
}

fn default_max_sessions() -> usize {
    MAX_SESSIONS
}

fn default_rate_per_minute() -> u32 {
    RATE_LIMIT_PER_MINUTE
}

fn default_rate_per_hour() -> u32 {
    RATE_LIMIT_PER_HOUR
}

fn default_sympy_pool() -> usize {
    SYMPY_POOL_SIZE
}

fn default_session_ttl() -> Duration {
    SESSION_TTL
}

fn default_menu_size() -> usize {
    DEFAULT_MENU_SIZE
}

fn default_max_input_size() -> usize {
    MAX_INPUT_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = SwitchboardConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.pipeline_budget, PIPELINE_BUDGET);
        assert_eq!(config.oracle_timeout, ORACLE_TIMEOUT);
        assert_eq!(config.max_sessions, MAX_SESSIONS);
        assert_eq!(config.rate_limit_per_minute, RATE_LIMIT_PER_MINUTE);
        assert_eq!(config.rate_limit_per_hour, RATE_LIMIT_PER_HOUR);
        assert_eq!(config.sympy_pool_size, SYMPY_POOL_SIZE);
        assert_eq!(config.session_ttl, SESSION_TTL);
        assert_eq!(config.menu_size, DEFAULT_MENU_SIZE);
        assert_eq!(config.max_input_size, MAX_INPUT_SIZE);
        assert!(config.mathpix_app_id.is_none());
        assert!(config.mathpix_app_key.is_none());
        assert!(config.wolfram_app_id.is_none());
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = SwitchboardConfig {
            enabled: true,
            mathpix_app_id: Some("app123".to_string()),
            mathpix_app_key: Some("key456".to_string()),
            wolfram_app_id: Some("wolf789".to_string()),
            pipeline_budget: Duration::from_secs(3),
            oracle_timeout: Duration::from_millis(2000),
            max_sessions: 5000,
            rate_limit_per_minute: 60,
            rate_limit_per_hour: 600,
            sympy_pool_size: 5,
            session_ttl: Duration::from_secs(1800),
            menu_size: 10,
            max_input_size: 20_000,
        };

        let json = serde_json::to_string(&config).unwrap();
        let decoded: SwitchboardConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.enabled, config.enabled);
        assert_eq!(decoded.mathpix_app_id, config.mathpix_app_id);
        assert_eq!(decoded.mathpix_app_key, config.mathpix_app_key);
        assert_eq!(decoded.wolfram_app_id, config.wolfram_app_id);
        assert_eq!(decoded.pipeline_budget, config.pipeline_budget);
        assert_eq!(decoded.oracle_timeout, config.oracle_timeout);
        assert_eq!(decoded.max_sessions, config.max_sessions);
        assert_eq!(decoded.rate_limit_per_minute, config.rate_limit_per_minute);
        assert_eq!(decoded.rate_limit_per_hour, config.rate_limit_per_hour);
        assert_eq!(decoded.sympy_pool_size, config.sympy_pool_size);
        assert_eq!(decoded.session_ttl, config.session_ttl);
        assert_eq!(decoded.menu_size, config.menu_size);
        assert_eq!(decoded.max_input_size, config.max_input_size);
    }

    #[test]
    fn test_toml_deserialization() {
        let toml = r#"
            enabled = true
            mathpix_app_id = "test_app_id"
            mathpix_app_key = "test_app_key"
            wolfram_app_id = "test_wolfram_id"
            pipeline_budget = "3s"
            oracle_timeout = "2s"
            max_sessions = 5000
            rate_limit_per_minute = 60
            rate_limit_per_hour = 600
            sympy_pool_size = 5
            session_ttl = "1800s"
            menu_size = 10
            max_input_size = 20000
        "#;

        let config: SwitchboardConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.mathpix_app_id, Some("test_app_id".to_string()));
        assert_eq!(config.mathpix_app_key, Some("test_app_key".to_string()));
        assert_eq!(config.wolfram_app_id, Some("test_wolfram_id".to_string()));
        assert_eq!(config.pipeline_budget, Duration::from_secs(3));
        assert_eq!(config.oracle_timeout, Duration::from_secs(2));
        assert_eq!(config.max_sessions, 5000);
        assert_eq!(config.rate_limit_per_minute, 60);
        assert_eq!(config.rate_limit_per_hour, 600);
        assert_eq!(config.sympy_pool_size, 5);
        assert_eq!(config.session_ttl, Duration::from_secs(1800));
        assert_eq!(config.menu_size, 10);
        assert_eq!(config.max_input_size, 20000);
    }

    #[test]
    fn test_partial_override() {
        let toml = r#"
            enabled = true
            pipeline_budget = "5s"
        "#;

        let config: SwitchboardConfig = toml::from_str(toml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.pipeline_budget, Duration::from_secs(5));
        // Other fields should use defaults
        assert_eq!(config.oracle_timeout, ORACLE_TIMEOUT);
        assert_eq!(config.max_sessions, MAX_SESSIONS);
        assert_eq!(config.rate_limit_per_minute, RATE_LIMIT_PER_MINUTE);
        assert_eq!(config.rate_limit_per_hour, RATE_LIMIT_PER_HOUR);
        assert_eq!(config.sympy_pool_size, SYMPY_POOL_SIZE);
        assert_eq!(config.session_ttl, SESSION_TTL);
        assert_eq!(config.menu_size, DEFAULT_MENU_SIZE);
        assert_eq!(config.max_input_size, MAX_INPUT_SIZE);
        assert!(config.mathpix_app_id.is_none());
        assert!(config.mathpix_app_key.is_none());
        assert!(config.wolfram_app_id.is_none());
    }

    #[test]
    fn test_timeout_ordering() {
        // Each individual stage timeout must fit within the pipeline budget
        assert!(
            INGEST_TIMEOUT < PIPELINE_BUDGET,
            "Ingest timeout ({:?}) should be less than pipeline budget ({:?})",
            INGEST_TIMEOUT, PIPELINE_BUDGET
        );
        assert!(
            CLASSIFY_TIMEOUT < PIPELINE_BUDGET,
            "Classify timeout ({:?}) should be less than pipeline budget ({:?})",
            CLASSIFY_TIMEOUT, PIPELINE_BUDGET
        );
        assert!(
            ORACLE_TIMEOUT < PIPELINE_BUDGET,
            "Oracle timeout ({:?}) should be less than pipeline budget ({:?})",
            ORACLE_TIMEOUT, PIPELINE_BUDGET
        );
        assert!(
            MENU_TIMEOUT < PIPELINE_BUDGET,
            "Menu timeout ({:?}) should be less than pipeline budget ({:?})",
            MENU_TIMEOUT, PIPELINE_BUDGET
        );
        // Ingest + classify (fast path) should leave room for oracle
        assert!(
            INGEST_TIMEOUT + CLASSIFY_TIMEOUT < PIPELINE_BUDGET,
            "Ingest + classify ({:?}) should leave room for oracle within budget ({:?})",
            INGEST_TIMEOUT + CLASSIFY_TIMEOUT, PIPELINE_BUDGET
        );
    }
}
