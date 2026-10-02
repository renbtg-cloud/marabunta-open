// Marabunta - Licensed under the MIT License.
//! Configuration types for the Marabunta Compute framework
//!
//! This module defines all configuration structures used by various components
//! of the distributed computing system.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Main configuration struct containing all settings for a Marabunta node
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct MarabuntaConfig {
    /// Node identity settings
    pub node: NodeConfig,

    /// Network settings
    pub network: NetworkConfig,

    /// Storage settings
    pub storage: StorageConfig,

    /// Coordinator settings (if running as coordinator)
    pub coordinator: Option<CoordinatorConfig>,

    /// Master settings (if running as master)
    pub master: Option<MasterConfig>,

    /// Worker settings (if running as worker)
    pub worker: Option<WorkerConfig>,

    /// Logging settings
    pub logging: LoggingConfig,

    /// Security settings
    pub security: SecurityConfig,

    /// Auto-group detection settings
    #[serde(default)]
    pub auto_groups: AutoGroupsConfig,

    /// Rate limiting settings
    #[serde(default)]
    pub rate_limiting: RateLimitingConfig,
}

/// Node identity configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier. Auto-generated if not set.
    pub id: Option<String>,
    /// Human-readable node name
    pub name: String,
    /// Geographic or logical region
    pub region: Option<String>,
    /// Tags for node classification and placement decisions
    #[serde(default)]
    pub tags: Vec<String>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: None,
            name: "marabunta-node".to_string(),
            region: None,
            tags: Vec::new(),
        }
    }
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Address to bind to for incoming connections
    pub bind_address: SocketAddr,
    /// Public address for other nodes to connect to (if behind NAT)
    pub public_address: Option<SocketAddr>,
    /// Bootstrap servers for initial cluster discovery
    #[serde(default)]
    pub bootstrap_servers: Vec<String>,
    /// Timeout for establishing connections
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// Timeout for individual requests
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:7000".parse().unwrap(),
            public_address: None,
            bootstrap_servers: Vec::new(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Base directory for all data
    pub data_dir: PathBuf,
    /// Path to the main database file. Defaults to data_dir/marabunta.db
    pub database_path: Option<PathBuf>,
    /// Directory for checkpoint files. Defaults to data_dir/checkpoints
    pub checkpoint_dir: Option<PathBuf>,
    /// Maximum checkpoint file size in megabytes
    #[serde(default = "default_max_checkpoint_size")]
    pub max_checkpoint_size_mb: u64,
}

fn default_max_checkpoint_size() -> u64 {
    1024 // 1GB default
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/marabunta"),
            database_path: None,
            checkpoint_dir: None,
            max_checkpoint_size_mb: default_max_checkpoint_size(),
        }
    }
}

impl StorageConfig {
    /// Get the effective database path
    pub fn effective_database_path(&self) -> PathBuf {
        self.database_path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("marabunta.db"))
    }

    /// Get the effective checkpoint directory
    pub fn effective_checkpoint_dir(&self) -> PathBuf {
        self.checkpoint_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("checkpoints"))
    }
}

/// Coordinator-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    /// Port for client API connections
    pub listen_port: u16,
    /// Port for Raft consensus protocol
    pub raft_port: u16,
    /// Addresses of other coordinator peers for Raft cluster
    #[serde(default)]
    pub peers: Vec<String>,
    /// Timeout before starting a new leader election
    #[serde(with = "humantime_serde")]
    pub election_timeout: Duration,
    /// Interval between leader heartbeats
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            listen_port: 7000,
            raft_port: 7001,
            peers: Vec::new(),
            election_timeout: Duration::from_millis(1000),
            heartbeat_interval: Duration::from_millis(200),
        }
    }
}

/// Master-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterConfig {
    /// Port for worker and client connections
    pub listen_port: u16,
    /// Addresses of coordinator nodes to register with
    #[serde(default)]
    pub coordinator_addresses: Vec<String>,
    /// Maximum number of workers this master can manage
    pub max_workers: usize,
    /// Timeout before considering a worker dead
    #[serde(with = "humantime_serde")]
    pub worker_timeout: Duration,
    /// Interval between scheduling cycles
    #[serde(with = "humantime_serde")]
    pub scheduling_interval: Duration,
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            listen_port: 7100,
            coordinator_addresses: vec!["http://localhost:7000".to_string()],
            max_workers: 1000,
            worker_timeout: Duration::from_secs(30),
            scheduling_interval: Duration::from_millis(100),
        }
    }
}

/// Worker-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Addresses of master nodes to connect to
    #[serde(default)]
    pub master_addresses: Vec<String>,
    /// Maximum number of tasks to execute concurrently
    pub max_concurrent_tasks: usize,
    /// CPU usage limit as a fraction (0.0-1.0). None means no limit.
    pub cpu_limit: Option<f64>,
    /// Memory usage limit in megabytes. None means no limit.
    pub memory_limit_mb: Option<u64>,
    /// Interval between heartbeats to the master
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// Interval between checkpointing task state
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            master_addresses: vec!["http://localhost:7100".to_string()],
            max_concurrent_tasks: num_cpus::get(),
            cpu_limit: None,
            memory_limit_mb: None,
            heartbeat_interval: Duration::from_secs(5),
            checkpoint_interval: Duration::from_secs(30),
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Output format
    #[serde(default)]
    pub format: LogFormat,
    /// Optional log file path. If None, logs to stderr.
    pub file: Option<PathBuf>,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: LogFormat::default(),
            file: None,
        }
    }
}

/// Log output format
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable format with colors
    #[default]
    Pretty,
    /// JSON format for structured logging
    Json,
    /// Compact single-line format
    Compact,
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct SecurityConfig {
    /// TLS configuration
    #[serde(default)]
    pub tls: TlsSecurityConfig,

    /// API authentication configuration
    #[serde(default)]
    pub auth: AuthSecurityConfig,

    /// Node authentication configuration
    #[serde(default)]
    pub node_auth: NodeAuthSecurityConfig,

    /// Legacy: Enable TLS for all connections (use tls.enabled instead)
    #[serde(default)]
    pub tls_enabled: bool,
    /// Legacy: Path to TLS certificate file (use tls.cert_path instead)
    pub cert_path: Option<PathBuf>,
    /// Legacy: Path to TLS private key file (use tls.key_path instead)
    pub key_path: Option<PathBuf>,
    /// Legacy: Path to CA certificate (use tls.ca_path instead)
    pub ca_path: Option<PathBuf>,
    /// Legacy: Simple authentication token (use auth.bootstrap_token instead)
    pub auth_token: Option<String>,
}


impl SecurityConfig {
    /// Get effective TLS configuration (merges legacy fields with new structure)
    pub fn effective_tls(&self) -> TlsSecurityConfig {
        let mut tls = self.tls.clone();

        // Apply legacy settings if new settings are not specified
        if !tls.enabled && self.tls_enabled {
            tls.enabled = true;
            tls.mode = "enabled".to_string();
        }
        if tls.cert_path.is_none() && self.cert_path.is_some() {
            tls.cert_path = self.cert_path.clone();
        }
        if tls.key_path.is_none() && self.key_path.is_some() {
            tls.key_path = self.key_path.clone();
        }
        if tls.ca_path.is_none() && self.ca_path.is_some() {
            tls.ca_path = self.ca_path.clone();
        }

        tls
    }

    /// Get effective auth configuration (merges legacy fields)
    pub fn effective_auth(&self) -> AuthSecurityConfig {
        let mut auth = self.auth.clone();

        // Apply legacy auth_token as bootstrap_token
        if auth.bootstrap_token.is_none() && self.auth_token.is_some() {
            auth.bootstrap_token = self.auth_token.clone();
            auth.enabled = true;
        }

        auth
    }
}

/// TLS security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsSecurityConfig {
    /// Enable TLS for all connections
    #[serde(default)]
    pub enabled: bool,

    /// TLS mode: "disabled", "enabled", or "dev" (accepts self-signed)
    #[serde(default = "default_tls_mode")]
    pub mode: String,

    /// Path to TLS certificate file (PEM format)
    pub cert_path: Option<PathBuf>,

    /// Path to TLS private key file (PEM format)
    pub key_path: Option<PathBuf>,

    /// Path to CA certificate for verifying peer certificates (PEM format)
    pub ca_path: Option<PathBuf>,

    /// Require client certificates (mutual TLS)
    #[serde(default)]
    pub require_client_cert: bool,

    /// Verify hostname in certificates
    #[serde(default = "default_true")]
    pub verify_hostname: bool,

    /// ALPN protocols to advertise
    #[serde(default)]
    pub alpn_protocols: Vec<String>,
}

fn default_tls_mode() -> String {
    "disabled".to_string()
}

impl Default for TlsSecurityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: "disabled".to_string(),
            cert_path: None,
            key_path: None,
            ca_path: None,
            require_client_cert: false,
            verify_hostname: true,
            alpn_protocols: Vec::new(),
        }
    }
}

impl TlsSecurityConfig {
    /// Convert to the security module's TlsConfig
    pub fn to_tls_config(&self) -> crate::security::TlsConfig {
        use crate::security::tls::TlsMode;

        let mode = match self.mode.to_lowercase().as_str() {
            "enabled" | "true" | "on" => TlsMode::Enabled,
            "dev" | "development" | "self-signed" => TlsMode::DevMode,
            _ => TlsMode::Disabled,
        };

        crate::security::TlsConfig {
            mode,
            cert_path: self.cert_path.clone(),
            key_path: self.key_path.clone(),
            ca_path: self.ca_path.clone(),
            require_client_cert: self.require_client_cert,
            verify_hostname: self.verify_hostname,
            alpn_protocols: self.alpn_protocols.clone(),
        }
    }
}

/// API authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSecurityConfig {
    /// Enable API authentication
    #[serde(default)]
    pub enabled: bool,

    /// Require authentication for all API requests
    #[serde(default)]
    pub required: bool,

    /// Default permission for unauthenticated requests (when not required)
    /// Values: "read_only", "user", "operator", "admin"
    #[serde(default = "default_permission")]
    pub default_permission: String,

    /// Token expiration duration (e.g., "24h", "7d")
    #[serde(default = "default_token_expiration")]
    pub token_expiration: String,

    /// Bootstrap admin token (only used on first startup)
    pub bootstrap_token: Option<String>,

    /// Path to token database file
    pub token_db_path: Option<PathBuf>,
}

fn default_permission() -> String {
    "read_only".to_string()
}

fn default_token_expiration() -> String {
    "24h".to_string()
}

impl Default for AuthSecurityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            required: false,
            default_permission: "read_only".to_string(),
            token_expiration: "24h".to_string(),
            bootstrap_token: None,
            token_db_path: None,
        }
    }
}

/// Node authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAuthSecurityConfig {
    /// Enable node authentication
    #[serde(default)]
    pub enabled: bool,

    /// Require mutual TLS for node connections
    #[serde(default)]
    pub require_mtls: bool,

    /// Registration token expiration (e.g., "1h", "30m")
    #[serde(default = "default_registration_expiration")]
    pub registration_token_expiration: String,

    /// Default maximum uses for registration tokens
    #[serde(default = "default_max_uses")]
    pub default_max_uses: u32,

    /// Path to node credentials database
    pub node_db_path: Option<PathBuf>,

    /// Automatically verify nodes on successful mTLS connection
    #[serde(default = "default_true")]
    pub auto_verify_mtls: bool,
}

fn default_registration_expiration() -> String {
    "1h".to_string()
}

fn default_max_uses() -> u32 {
    10
}

impl Default for NodeAuthSecurityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require_mtls: false,
            registration_token_expiration: "1h".to_string(),
            default_max_uses: 10,
            node_db_path: None,
            auto_verify_mtls: true,
        }
    }
}

/// Auto-group detection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoGroupsConfig {
    /// Master enable/disable for all auto-detection
    #[serde(default = "default_auto_groups_enabled")]
    pub enabled: bool,

    /// Interval between detection runs (in seconds)
    #[serde(default = "default_detection_interval")]
    pub detection_interval_secs: u64,

    /// Whether to trigger detection on node join
    #[serde(default = "default_true")]
    pub detect_on_node_join: bool,

    /// Whether to trigger detection on node leave
    #[serde(default = "default_true")]
    pub detect_on_node_leave: bool,

    /// Whether to trigger detection on significant metric changes
    #[serde(default = "default_true")]
    pub detect_on_metric_change: bool,

    /// Threshold for what constitutes a "significant" metric change (percentage)
    #[serde(default = "default_metric_threshold")]
    pub metric_change_threshold: f64,

    /// How long ephemeral groups remain valid without re-detection (in seconds)
    #[serde(default = "default_group_ttl")]
    pub group_ttl_secs: u64,

    /// Whether to automatically clean up expired groups
    #[serde(default = "default_true")]
    pub auto_cleanup: bool,

    /// Prefix for all auto-group IDs
    #[serde(default = "default_group_prefix")]
    pub group_id_prefix: String,

    /// Region detector configuration
    #[serde(default)]
    pub region_detector: DetectorSettings,

    /// Hardware detector configuration
    #[serde(default)]
    pub hardware_detector: DetectorSettings,

    /// Network detector configuration
    #[serde(default)]
    pub network_detector: DetectorSettings,

    /// Load detector configuration
    #[serde(default)]
    pub load_detector: DetectorSettings,
}

fn default_auto_groups_enabled() -> bool {
    true
}

fn default_detection_interval() -> u64 {
    60
}

fn default_true() -> bool {
    true
}

fn default_metric_threshold() -> f64 {
    10.0
}

fn default_group_ttl() -> u64 {
    300
}

fn default_group_prefix() -> String {
    "auto".to_string()
}

impl Default for AutoGroupsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            detection_interval_secs: 60,
            detect_on_node_join: true,
            detect_on_node_leave: true,
            detect_on_metric_change: true,
            metric_change_threshold: 10.0,
            group_ttl_secs: 300,
            auto_cleanup: true,
            group_id_prefix: "auto".to_string(),
            region_detector: DetectorSettings::default(),
            hardware_detector: DetectorSettings::default(),
            network_detector: DetectorSettings::default(),
            load_detector: DetectorSettings::default(),
        }
    }
}

/// Configuration for a specific detector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectorSettings {
    /// Whether this detector is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Minimum number of nodes to form a group
    #[serde(default = "default_min_group_size")]
    pub min_group_size: usize,

    /// Maximum number of groups this detector can create
    pub max_groups: Option<usize>,

    /// Custom naming pattern for groups (supports {detector}, {value} placeholders)
    #[serde(default = "default_naming_pattern")]
    pub naming_pattern: String,

    /// Confidence threshold for including nodes (0.0-1.0)
    #[serde(default = "default_confidence")]
    pub confidence_threshold: f64,
}

fn default_min_group_size() -> usize {
    1
}

fn default_naming_pattern() -> String {
    "auto:{detector}:{value}".to_string()
}

fn default_confidence() -> f64 {
    0.5
}

impl Default for DetectorSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            min_group_size: 1,
            max_groups: None,
            naming_pattern: "auto:{detector}:{value}".to_string(),
            confidence_threshold: 0.5,
        }
    }
}

impl AutoGroupsConfig {
    /// Convert to the placement module's AutoGroupConfig
    pub fn to_auto_group_config(&self) -> crate::placement::AutoGroupConfig {
        use crate::placement::AutoGroupConfig;
        use std::collections::HashMap;

        let mut detectors = HashMap::new();
        detectors.insert(
            "region".to_string(),
            self.region_detector.to_detector_config(),
        );
        detectors.insert(
            "hardware".to_string(),
            self.hardware_detector.to_detector_config(),
        );
        detectors.insert(
            "network".to_string(),
            self.network_detector.to_detector_config(),
        );
        detectors.insert("load".to_string(), self.load_detector.to_detector_config());

        AutoGroupConfig {
            enabled: self.enabled,
            detection_interval_secs: self.detection_interval_secs,
            detect_on_node_join: self.detect_on_node_join,
            detect_on_node_leave: self.detect_on_node_leave,
            detect_on_metric_change: self.detect_on_metric_change,
            metric_change_threshold: self.metric_change_threshold,
            group_ttl_secs: self.group_ttl_secs,
            auto_cleanup: self.auto_cleanup,
            group_id_prefix: self.group_id_prefix.clone(),
            detectors,
        }
    }
}

impl DetectorSettings {
    /// Convert to the placement module's DetectorConfig
    pub fn to_detector_config(&self) -> crate::placement::DetectorConfig {
        crate::placement::DetectorConfig {
            enabled: self.enabled,
            min_group_size: self.min_group_size,
            max_groups: self.max_groups,
            naming_pattern: self.naming_pattern.clone(),
            confidence_threshold: self.confidence_threshold,
        }
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitingConfig {
    /// Enable rate limiting globally
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Default requests per second for all endpoints
    #[serde(default = "default_requests_per_second")]
    pub default_requests_per_second: f64,

    /// Default burst size for all endpoints
    #[serde(default = "default_burst_size")]
    pub default_burst_size: u32,

    /// Per-endpoint rate limit overrides
    #[serde(default)]
    pub endpoint_limits: Vec<EndpointRateLimit>,

    /// Global rate limits (shared across all clients)
    #[serde(default)]
    pub global_limits: Vec<EndpointRateLimit>,

    /// Paths exempt from rate limiting
    #[serde(default = "default_exempt_paths")]
    pub exempt_paths: Vec<String>,

    /// Rate limits by authentication level
    #[serde(default)]
    pub auth_level_limits: AuthLevelLimits,

    /// Enable distributed rate limiting for multi-coordinator deployments
    #[serde(default)]
    pub distributed: bool,

    /// Cleanup interval for expired rate limit buckets
    #[serde(with = "humantime_serde", default = "default_cleanup_interval")]
    pub cleanup_interval: Duration,

    /// Maximum age for inactive buckets before cleanup
    #[serde(with = "humantime_serde", default = "default_bucket_ttl")]
    pub bucket_ttl: Duration,

    /// Include rate limit headers in responses
    #[serde(default = "default_true")]
    pub include_headers: bool,

    /// Custom error message for rate limited responses
    #[serde(default = "default_error_message")]
    pub error_message: String,

    /// Header name for API key authentication
    #[serde(default = "default_api_key_header")]
    pub api_key_header: String,

    /// Trusted proxy headers for IP extraction (e.g., X-Forwarded-For)
    #[serde(default = "default_trusted_proxy_headers")]
    pub trusted_proxy_headers: Vec<String>,
}

fn default_requests_per_second() -> f64 {
    100.0
}

fn default_burst_size() -> u32 {
    200
}

fn default_exempt_paths() -> Vec<String> {
    vec![
        "/health".to_string(),
        "/ready".to_string(),
        "/metrics".to_string(),
    ]
}

fn default_cleanup_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_bucket_ttl() -> Duration {
    Duration::from_secs(3600)
}

fn default_error_message() -> String {
    "Rate limit exceeded. Please try again later.".to_string()
}

fn default_api_key_header() -> String {
    "X-API-Key".to_string()
}

fn default_trusted_proxy_headers() -> Vec<String> {
    vec!["X-Forwarded-For".to_string(), "X-Real-IP".to_string()]
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_requests_per_second: 100.0,
            default_burst_size: 200,
            endpoint_limits: Vec::new(),
            global_limits: Vec::new(),
            exempt_paths: default_exempt_paths(),
            auth_level_limits: AuthLevelLimits::default(),
            distributed: false,
            cleanup_interval: Duration::from_secs(60),
            bucket_ttl: Duration::from_secs(3600),
            include_headers: true,
            error_message: default_error_message(),
            api_key_header: "X-API-Key".to_string(),
            trusted_proxy_headers: default_trusted_proxy_headers(),
        }
    }
}

/// Per-endpoint rate limit configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointRateLimit {
    /// Endpoint path (supports :param placeholders like /api/nodes/:id)
    pub path: String,
    /// Requests per second
    pub requests_per_second: f64,
    /// Burst size
    pub burst_size: u32,
}

/// Rate limits by authentication level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthLevelLimits {
    /// Limits for anonymous (unauthenticated) requests
    #[serde(default = "default_anonymous_limits")]
    pub anonymous: RateLimitValues,
    /// Limits for basic authenticated users
    #[serde(default = "default_authenticated_limits")]
    pub authenticated: RateLimitValues,
    /// Limits for premium/paid tier users
    #[serde(default = "default_premium_limits")]
    pub premium: RateLimitValues,
    /// Limits for admin or internal service accounts
    #[serde(default = "default_admin_limits")]
    pub admin: RateLimitValues,
}

fn default_anonymous_limits() -> RateLimitValues {
    RateLimitValues {
        requests_per_second: 20.0,
        burst_size: 40,
    }
}

fn default_authenticated_limits() -> RateLimitValues {
    RateLimitValues {
        requests_per_second: 100.0,
        burst_size: 200,
    }
}

fn default_premium_limits() -> RateLimitValues {
    RateLimitValues {
        requests_per_second: 500.0,
        burst_size: 1000,
    }
}

fn default_admin_limits() -> RateLimitValues {
    RateLimitValues {
        requests_per_second: 10000.0,
        burst_size: 50000,
    }
}

impl Default for AuthLevelLimits {
    fn default() -> Self {
        Self {
            anonymous: default_anonymous_limits(),
            authenticated: default_authenticated_limits(),
            premium: default_premium_limits(),
            admin: default_admin_limits(),
        }
    }
}

/// Rate limit values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitValues {
    /// Requests per second
    pub requests_per_second: f64,
    /// Burst size
    pub burst_size: u32,
}

impl RateLimitingConfig {
    /// Convert to the ratelimit module's RateLimiterConfig
    pub fn to_rate_limiter_config(&self) -> crate::ratelimit::RateLimiterConfig {
        use crate::ratelimit::{AuthLevel, RateLimit, RateLimiterConfig};
        use std::collections::HashMap;

        let mut endpoint_limits = HashMap::new();
        for limit in &self.endpoint_limits {
            endpoint_limits.insert(
                limit.path.clone(),
                RateLimit::new(limit.requests_per_second, limit.burst_size),
            );
        }

        let mut global_limits = HashMap::new();
        for limit in &self.global_limits {
            global_limits.insert(
                limit.path.clone(),
                RateLimit::new(limit.requests_per_second, limit.burst_size),
            );
        }

        let mut auth_level_limits = HashMap::new();
        auth_level_limits.insert(
            AuthLevel::Anonymous,
            RateLimit::new(
                self.auth_level_limits.anonymous.requests_per_second,
                self.auth_level_limits.anonymous.burst_size,
            ),
        );
        auth_level_limits.insert(
            AuthLevel::Authenticated,
            RateLimit::new(
                self.auth_level_limits.authenticated.requests_per_second,
                self.auth_level_limits.authenticated.burst_size,
            ),
        );
        auth_level_limits.insert(
            AuthLevel::Premium,
            RateLimit::new(
                self.auth_level_limits.premium.requests_per_second,
                self.auth_level_limits.premium.burst_size,
            ),
        );
        auth_level_limits.insert(
            AuthLevel::Admin,
            RateLimit::new(
                self.auth_level_limits.admin.requests_per_second,
                self.auth_level_limits.admin.burst_size,
            ),
        );

        RateLimiterConfig {
            default_limit: RateLimit::new(
                self.default_requests_per_second,
                self.default_burst_size,
            ),
            endpoint_limits,
            global_limits,
            exempt_paths: self.exempt_paths.clone(),
            auth_level_limits,
            distributed: self.distributed,
            cleanup_interval: self.cleanup_interval,
            bucket_ttl: self.bucket_ttl,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_config_default() {
        let config = NodeConfig::default();
        assert!(config.id.is_none());
        assert_eq!(config.name, "marabunta-node");
        assert!(config.region.is_none());
        assert!(config.tags.is_empty());
    }

    #[test]
    fn test_network_config_default() {
        let config = NetworkConfig::default();
        assert_eq!(
            config.bind_address,
            "0.0.0.0:7000".parse::<SocketAddr>().unwrap()
        );
        assert!(config.public_address.is_none());
        assert!(config.bootstrap_servers.is_empty());
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_storage_config_effective_paths() {
        let mut config = StorageConfig::default();
        config.data_dir = PathBuf::from("/data/marabunta");

        assert_eq!(
            config.effective_database_path(),
            PathBuf::from("/data/marabunta/marabunta.db")
        );
        assert_eq!(
            config.effective_checkpoint_dir(),
            PathBuf::from("/data/marabunta/checkpoints")
        );

        // Test with explicit paths
        config.database_path = Some(PathBuf::from("/custom/db.sqlite"));
        config.checkpoint_dir = Some(PathBuf::from("/custom/checkpoints"));

        assert_eq!(
            config.effective_database_path(),
            PathBuf::from("/custom/db.sqlite")
        );
        assert_eq!(
            config.effective_checkpoint_dir(),
            PathBuf::from("/custom/checkpoints")
        );
    }

    #[test]
    fn test_log_format_serialization() {
        let format = LogFormat::Pretty;
        let json = serde_json::to_string(&format).unwrap();
        assert_eq!(json, "\"pretty\"");

        let format: LogFormat = serde_json::from_str("\"json\"").unwrap();
        assert_eq!(format, LogFormat::Json);
    }
}
