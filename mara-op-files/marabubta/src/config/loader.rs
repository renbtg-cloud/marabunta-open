// Marabunta - Licensed under the MIT License.
//! Configuration loading utilities
//!
//! This module provides functions for loading configuration from files,
//! environment variables, and default locations.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::errors::{ConfigError, ConfigResult};
use crate::config::types::*;

/// Default configuration file name
pub const DEFAULT_CONFIG_NAME: &str = "marabunta.toml";

impl MarabuntaConfig {
    /// Load configuration from a TOML file
    ///
    /// # Arguments
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Errors
    /// Returns an error if the file cannot be read or parsed
    pub fn from_file(path: impl AsRef<Path>) -> ConfigResult<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(ConfigError::FileNotFound(path.to_path_buf()));
        }

        let contents = fs::read_to_string(path).map_err(|e| ConfigError::ReadError {
            path: path.to_path_buf(),
            source: e,
        })?;

        let config: MarabuntaConfig = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Load configuration from a file with environment variable overrides
    ///
    /// Environment variables follow the pattern `MARABUNTA_<SECTION>_<FIELD>`.
    /// For example:
    /// - `MARABUNTA_NODE_NAME` overrides `node.name`
    /// - `MARABUNTA_NETWORK_BIND_ADDRESS` overrides `network.bind_address`
    /// - `MARABUNTA_LOGGING_LEVEL` overrides `logging.level`
    ///
    /// # Arguments
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Errors
    /// Returns an error if the file cannot be read/parsed or if env vars are invalid
    pub fn from_file_with_env(path: impl AsRef<Path>) -> ConfigResult<Self> {
        let mut config = Self::from_file(path)?;
        config.apply_env_overrides()?;
        Ok(config)
    }

    /// Load configuration from default locations
    ///
    /// Checks the following locations in order:
    /// 1. `./marabunta.toml` - Current directory
    /// 2. `~/.config/marabunta/config.toml` - User config directory
    /// 3. `/etc/marabunta/config.toml` - System config directory (Unix only)
    ///
    /// Environment variable overrides are applied after loading.
    ///
    /// # Errors
    /// Returns an error if no configuration file is found or if parsing fails
    pub fn load_default() -> ConfigResult<Self> {
        let mut checked_paths = Vec::new();

        // Check current directory
        let cwd_config = PathBuf::from(DEFAULT_CONFIG_NAME);
        checked_paths.push(cwd_config.display().to_string());
        if cwd_config.exists() {
            return Self::from_file_with_env(&cwd_config);
        }

        // Check user config directory
        if let Some(config_dir) = directories::ProjectDirs::from("com", "marabunta", "marabunta-compute") {
            let user_config = config_dir.config_dir().join("config.toml");
            checked_paths.push(user_config.display().to_string());
            if user_config.exists() {
                return Self::from_file_with_env(&user_config);
            }
        }

        // Check system config directory (Unix)
        #[cfg(unix)]
        {
            let system_config = PathBuf::from("/etc/marabunta/config.toml");
            checked_paths.push(system_config.display().to_string());
            if system_config.exists() {
                return Self::from_file_with_env(&system_config);
            }
        }

        // Windows system config
        #[cfg(windows)]
        {
            if let Ok(program_data) = env::var("ProgramData") {
                let system_config = PathBuf::from(program_data).join("marabunta").join("config.toml");
                checked_paths.push(system_config.display().to_string());
                if system_config.exists() {
                    return Self::from_file_with_env(&system_config);
                }
            }
        }

        Err(ConfigError::NoConfigFound(checked_paths))
    }

    /// Create a default configuration for a coordinator node
    pub fn default_coordinator() -> Self {
        Self {
            node: NodeConfig {
                id: None,
                name: "coordinator".to_string(),
                region: Some("default".to_string()),
                tags: vec!["coordinator".to_string()],
            },
            network: NetworkConfig::default(),
            storage: StorageConfig {
                data_dir: PathBuf::from("/var/lib/marabunta/coordinator"),
                ..Default::default()
            },
            coordinator: Some(CoordinatorConfig::default()),
            master: None,
            worker: None,
            logging: LoggingConfig::default(),
            security: SecurityConfig::default(),
            auto_groups: AutoGroupsConfig::default(),
            rate_limiting: RateLimitingConfig::default(),
        }
    }

    /// Create a default configuration for a master node
    pub fn default_master() -> Self {
        Self {
            node: NodeConfig {
                id: None,
                name: "master".to_string(),
                region: Some("default".to_string()),
                tags: vec!["master".to_string()],
            },
            network: NetworkConfig {
                bind_address: "0.0.0.0:7100".parse().unwrap(),
                ..Default::default()
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("/var/lib/marabunta/master"),
                ..Default::default()
            },
            coordinator: None,
            master: Some(MasterConfig::default()),
            worker: None,
            logging: LoggingConfig::default(),
            security: SecurityConfig::default(),
            auto_groups: AutoGroupsConfig::default(),
            rate_limiting: RateLimitingConfig::default(),
        }
    }

    /// Create a default configuration for a worker node
    pub fn default_worker() -> Self {
        Self {
            node: NodeConfig {
                id: None,
                name: "worker".to_string(),
                region: None,
                tags: vec!["worker".to_string()],
            },
            network: NetworkConfig {
                bind_address: "0.0.0.0:7200".parse().unwrap(),
                ..Default::default()
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("/var/lib/marabunta/worker"),
                ..Default::default()
            },
            coordinator: None,
            master: None,
            worker: Some(WorkerConfig::default()),
            logging: LoggingConfig::default(),
            security: SecurityConfig::default(),
            auto_groups: AutoGroupsConfig::default(),
            rate_limiting: RateLimitingConfig::default(),
        }
    }

    /// Validate the configuration
    ///
    /// Checks for:
    /// - Required fields based on node role
    /// - Valid port numbers
    /// - Valid addresses
    /// - Consistent TLS configuration
    ///
    /// # Errors
    /// Returns an error if validation fails
    pub fn validate(&self) -> ConfigResult<()> {
        let mut errors = Vec::new();

        // Node validation
        if self.node.name.is_empty() {
            errors.push(ConfigError::validation("node.name", "cannot be empty"));
        }

        // Storage validation
        if self.storage.max_checkpoint_size_mb == 0 {
            errors.push(ConfigError::validation(
                "storage.max_checkpoint_size_mb",
                "must be greater than 0",
            ));
        }

        // Coordinator validation
        if let Some(ref coord) = self.coordinator {
            if coord.listen_port == coord.raft_port {
                errors.push(ConfigError::validation(
                    "coordinator",
                    "listen_port and raft_port must be different",
                ));
            }

            if coord.election_timeout < coord.heartbeat_interval * 2 {
                errors.push(ConfigError::validation(
                    "coordinator.election_timeout",
                    "should be at least 2x heartbeat_interval",
                ));
            }
        }

        // Master validation
        if let Some(ref master) = self.master {
            if master.coordinator_addresses.is_empty() {
                errors.push(ConfigError::validation(
                    "master.coordinator_addresses",
                    "must have at least one coordinator address",
                ));
            }

            if master.max_workers == 0 {
                errors.push(ConfigError::validation(
                    "master.max_workers",
                    "must be greater than 0",
                ));
            }
        }

        // Worker validation
        if let Some(ref worker) = self.worker {
            if worker.master_addresses.is_empty() {
                errors.push(ConfigError::validation(
                    "worker.master_addresses",
                    "must have at least one master address",
                ));
            }

            if worker.max_concurrent_tasks == 0 {
                errors.push(ConfigError::validation(
                    "worker.max_concurrent_tasks",
                    "must be greater than 0",
                ));
            }

            if let Some(cpu_limit) = worker.cpu_limit {
                if !(0.0..=1.0).contains(&cpu_limit) {
                    errors.push(ConfigError::validation(
                        "worker.cpu_limit",
                        "must be between 0.0 and 1.0",
                    ));
                }
            }
        }

        // Security validation
        if self.security.tls_enabled {
            if self.security.cert_path.is_none() {
                errors.push(ConfigError::validation(
                    "security.cert_path",
                    "required when TLS is enabled",
                ));
            }
            if self.security.key_path.is_none() {
                errors.push(ConfigError::validation(
                    "security.key_path",
                    "required when TLS is enabled",
                ));
            }
        }

        // Logging validation
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.logging.level.to_lowercase().as_str()) {
            errors.push(ConfigError::validation(
                "logging.level",
                format!("must be one of: {}", valid_levels.join(", ")),
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else if errors.len() == 1 {
            Err(errors.remove(0))
        } else {
            Err(ConfigError::MultipleErrors(errors))
        }
    }

    /// Write an example configuration file
    ///
    /// # Arguments
    /// * `path` - Path where the example config should be written
    ///
    /// # Errors
    /// Returns an error if the file cannot be written
    pub fn write_example(path: impl AsRef<Path>) -> ConfigResult<()> {
        let example = EXAMPLE_CONFIG;
        let path = path.as_ref();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| ConfigError::WriteError {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }

        fs::write(path, example).map_err(|e| ConfigError::WriteError {
            path: path.to_path_buf(),
            source: e,
        })?;

        Ok(())
    }

    /// Apply environment variable overrides to the configuration
    fn apply_env_overrides(&mut self) -> ConfigResult<()> {
        // Node overrides
        if let Ok(val) = env::var("MARABUNTA_NODE_ID") {
            self.node.id = Some(val);
        }
        if let Ok(val) = env::var("MARABUNTA_NODE_NAME") {
            self.node.name = val;
        }
        if let Ok(val) = env::var("MARABUNTA_NODE_REGION") {
            self.node.region = Some(val);
        }

        // Network overrides
        if let Ok(val) = env::var("MARABUNTA_NETWORK_BIND_ADDRESS") {
            self.network.bind_address = val.parse().map_err(|_| {
                ConfigError::invalid_address(
                    "MARABUNTA_NETWORK_BIND_ADDRESS",
                    &val,
                    "must be a valid socket address (e.g., 0.0.0.0:7000)",
                )
            })?;
        }
        if let Ok(val) = env::var("MARABUNTA_NETWORK_PUBLIC_ADDRESS") {
            self.network.public_address = Some(val.parse().map_err(|_| {
                ConfigError::invalid_address(
                    "MARABUNTA_NETWORK_PUBLIC_ADDRESS",
                    &val,
                    "must be a valid socket address",
                )
            })?);
        }
        if let Ok(val) = env::var("MARABUNTA_NETWORK_CONNECT_TIMEOUT") {
            self.network.connect_timeout = parse_duration(&val).map_err(|e| {
                ConfigError::invalid_duration("MARABUNTA_NETWORK_CONNECT_TIMEOUT", &val, e)
            })?;
        }
        if let Ok(val) = env::var("MARABUNTA_NETWORK_REQUEST_TIMEOUT") {
            self.network.request_timeout = parse_duration(&val).map_err(|e| {
                ConfigError::invalid_duration("MARABUNTA_NETWORK_REQUEST_TIMEOUT", &val, e)
            })?;
        }

        // Storage overrides
        if let Ok(val) = env::var("MARABUNTA_STORAGE_DATA_DIR") {
            self.storage.data_dir = PathBuf::from(val);
        }
        if let Ok(val) = env::var("MARABUNTA_STORAGE_DATABASE_PATH") {
            self.storage.database_path = Some(PathBuf::from(val));
        }
        if let Ok(val) = env::var("MARABUNTA_STORAGE_CHECKPOINT_DIR") {
            self.storage.checkpoint_dir = Some(PathBuf::from(val));
        }
        if let Ok(val) = env::var("MARABUNTA_STORAGE_MAX_CHECKPOINT_SIZE_MB") {
            self.storage.max_checkpoint_size_mb =
                val.parse().map_err(|_| ConfigError::InvalidEnvVar {
                    var: "MARABUNTA_STORAGE_MAX_CHECKPOINT_SIZE_MB".to_string(),
                    message: "must be a valid positive integer".to_string(),
                })?;
        }

        // Logging overrides
        if let Ok(val) = env::var("MARABUNTA_LOGGING_LEVEL") {
            self.logging.level = val;
        }
        if let Ok(val) = env::var("MARABUNTA_LOGGING_FILE") {
            self.logging.file = Some(PathBuf::from(val));
        }

        // Security overrides
        if let Ok(val) = env::var("MARABUNTA_SECURITY_TLS_ENABLED") {
            self.security.tls_enabled = val.parse().map_err(|_| ConfigError::InvalidEnvVar {
                var: "MARABUNTA_SECURITY_TLS_ENABLED".to_string(),
                message: "must be 'true' or 'false'".to_string(),
            })?;
        }
        if let Ok(val) = env::var("MARABUNTA_SECURITY_AUTH_TOKEN") {
            self.security.auth_token = Some(val);
        }
        if let Ok(val) = env::var("MARABUNTA_SECURITY_CERT_PATH") {
            self.security.cert_path = Some(PathBuf::from(val));
        }
        if let Ok(val) = env::var("MARABUNTA_SECURITY_KEY_PATH") {
            self.security.key_path = Some(PathBuf::from(val));
        }
        if let Ok(val) = env::var("MARABUNTA_SECURITY_CA_PATH") {
            self.security.ca_path = Some(PathBuf::from(val));
        }

        // Coordinator overrides
        if let Some(ref mut coord) = self.coordinator {
            if let Ok(val) = env::var("MARABUNTA_COORDIGLOBAL_ALLIANCE_T1R_LISTEN_PORT") {
                coord.listen_port = val.parse().map_err(|_| ConfigError::InvalidPort {
                    field: "MARABUNTA_COORDIGLOBAL_ALLIANCE_T1R_LISTEN_PORT".to_string(),
                    port: val,
                })?;
            }
            if let Ok(val) = env::var("MARABUNTA_COORDIGLOBAL_ALLIANCE_T1R_RAFT_PORT") {
                coord.raft_port = val.parse().map_err(|_| ConfigError::InvalidPort {
                    field: "MARABUNTA_COORDIGLOBAL_ALLIANCE_T1R_RAFT_PORT".to_string(),
                    port: val,
                })?;
            }
        }

        // Master overrides
        if let Some(ref mut master) = self.master {
            if let Ok(val) = env::var("MARABUNTA_MASTER_LISTEN_PORT") {
                master.listen_port = val.parse().map_err(|_| ConfigError::InvalidPort {
                    field: "MARABUNTA_MASTER_LISTEN_PORT".to_string(),
                    port: val,
                })?;
            }
            if let Ok(val) = env::var("MARABUNTA_MASTER_MAX_WORKERS") {
                master.max_workers = val.parse().map_err(|_| ConfigError::InvalidEnvVar {
                    var: "MARABUNTA_MASTER_MAX_WORKERS".to_string(),
                    message: "must be a valid positive integer".to_string(),
                })?;
            }
        }

        // Worker overrides
        if let Some(ref mut worker) = self.worker {
            if let Ok(val) = env::var("MARABUNTA_WORKER_MAX_CONCURRENT_TASKS") {
                worker.max_concurrent_tasks =
                    val.parse().map_err(|_| ConfigError::InvalidEnvVar {
                        var: "MARABUNTA_WORKER_MAX_CONCURRENT_TASKS".to_string(),
                        message: "must be a valid positive integer".to_string(),
                    })?;
            }
            if let Ok(val) = env::var("MARABUNTA_WORKER_CPU_LIMIT") {
                worker.cpu_limit = Some(val.parse().map_err(|_| ConfigError::InvalidEnvVar {
                    var: "MARABUNTA_WORKER_CPU_LIMIT".to_string(),
                    message: "must be a decimal between 0.0 and 1.0".to_string(),
                })?);
            }
            if let Ok(val) = env::var("MARABUNTA_WORKER_MEMORY_LIMIT_MB") {
                worker.memory_limit_mb =
                    Some(val.parse().map_err(|_| ConfigError::InvalidEnvVar {
                        var: "MARABUNTA_WORKER_MEMORY_LIMIT_MB".to_string(),
                        message: "must be a valid positive integer".to_string(),
                    })?);
            }
        }

        Ok(())
    }

    /// Serialize the configuration to a TOML string
    pub fn to_toml(&self) -> ConfigResult<String> {
        toml::to_string_pretty(self).map_err(ConfigError::from)
    }

    /// Write the configuration to a file
    pub fn write_to_file(&self, path: impl AsRef<Path>) -> ConfigResult<()> {
        let content = self.to_toml()?;
        let path = path.as_ref();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| ConfigError::WriteError {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }

        fs::write(path, content).map_err(|e| ConfigError::WriteError {
            path: path.to_path_buf(),
            source: e,
        })?;

        Ok(())
    }
}

/// Parse a duration string in humantime format
fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

/// Example configuration content
pub const EXAMPLE_CONFIG: &str = r##"# Marabunta Compute Configuration
# This is an example configuration file with all available options.
# Copy this to marabunta.toml and modify as needed.

# ============================================================================
# Node Identity
# ============================================================================
[node]
# Unique node identifier. Leave empty to auto-generate.
# id = "node-001"

# Human-readable name for this node
name = "marabunta-node-01"

# Geographic or logical region
region = "us-east-1"

# Tags for node classification (used for placement decisions)
tags = ["production", "gpu", "high-memory"]

# ============================================================================
# Network Configuration
# ============================================================================
[network]
# Address to bind for incoming connections
bind_address = "0.0.0.0:7000"

# Public address if behind NAT (optional)
# public_address = "203.0.113.50:7000"

# Bootstrap servers for cluster discovery
bootstrap_servers = [
    "coordinator-1.example.com:7000",
    "coordinator-2.example.com:7000",
]

# Connection timeout
connect_timeout = "10s"

# Request timeout
request_timeout = "30s"

# ============================================================================
# Storage Configuration
# ============================================================================
[storage]
# Base directory for all data
data_dir = "/var/lib/marabunta"

# Database file path (default: data_dir/marabunta.db)
# database_path = "/var/lib/marabunta/marabunta.db"

# Checkpoint directory (default: data_dir/checkpoints)
# checkpoint_dir = "/var/lib/marabunta/checkpoints"

# Maximum checkpoint file size in MB
max_checkpoint_size_mb = 1024

# ============================================================================
# Coordinator Configuration (for coordinator nodes)
# ============================================================================
# Uncomment this section to run as a coordinator
# [coordinator]
# listen_port = 7000
# raft_port = 7001
# peers = [
#     "coordinator-2.example.com:7001",
#     "coordinator-3.example.com:7001",
# ]
# election_timeout = "1s"
# heartbeat_interval = "200ms"

# ============================================================================
# Master Configuration (for master nodes)
# ============================================================================
# Uncomment this section to run as a master
# [master]
# listen_port = 7100
# coordinator_addresses = [
#     "http://coordinator-1.example.com:7000",
#     "http://coordinator-2.example.com:7000",
# ]
# max_workers = 1000
# worker_timeout = "30s"
# scheduling_interval = "100ms"

# ============================================================================
# Worker Configuration (for worker nodes)
# ============================================================================
# Uncomment this section to run as a worker
# [worker]
# master_addresses = [
#     "http://master-1.example.com:7100",
#     "http://master-2.example.com:7100",
# ]
# max_concurrent_tasks = 8
# cpu_limit = 0.8          # Use up to 80% of CPU
# memory_limit_mb = 4096   # Limit to 4GB RAM
# heartbeat_interval = "5s"
# checkpoint_interval = "30s"

# ============================================================================
# Logging Configuration
# ============================================================================
[logging]
# Log level: trace, debug, info, warn, error
level = "info"

# Output format: pretty, json, compact
format = "pretty"

# Log file path (optional, logs to stderr if not set)
# file = "/var/log/marabunta/marabunta.log"

# ============================================================================
# Security Configuration
# ============================================================================
[security]
# Enable TLS for all connections
tls_enabled = false

# TLS certificate file (required if tls_enabled = true)
# cert_path = "/etc/marabunta/certs/server.crt"

# TLS private key file (required if tls_enabled = true)
# key_path = "/etc/marabunta/certs/server.key"

# CA certificate for verifying peer certificates
# ca_path = "/etc/marabunta/certs/ca.crt"

# Authentication token (simple auth for now)
# auth_token = "your-secret-token-here"
"##;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_temp_config(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    #[test]
    fn test_load_from_file() {
        let content = r#"
[node]
name = "test-node"

[network]
bind_address = "127.0.0.1:8000"
connect_timeout = "5s"
request_timeout = "15s"

[storage]
data_dir = "/tmp/marabunta-test"

[logging]
level = "debug"
format = "json"

[security]
tls_enabled = false
"#;

        let file = create_temp_config(content);
        let config = MarabuntaConfig::from_file(file.path()).unwrap();

        assert_eq!(config.node.name, "test-node");
        assert_eq!(
            config.network.bind_address,
            "127.0.0.1:8000".parse().unwrap()
        );
        assert_eq!(
            config.network.connect_timeout,
            std::time::Duration::from_secs(5)
        );
        assert_eq!(config.storage.data_dir, PathBuf::from("/tmp/marabunta-test"));
        assert_eq!(config.logging.level, "debug");
        assert_eq!(config.logging.format, LogFormat::Json);
    }

    #[test]
    fn test_load_with_coordinator_config() {
        let content = r#"
[node]
name = "coord-1"

[network]
bind_address = "0.0.0.0:7000"
connect_timeout = "10s"
request_timeout = "30s"

[storage]
data_dir = "/var/lib/marabunta"

[coordinator]
listen_port = 7000
raft_port = 7001
peers = ["peer1:7001", "peer2:7001"]
election_timeout = "1s"
heartbeat_interval = "200ms"

[logging]
level = "info"

[security]
tls_enabled = false
"#;

        let file = create_temp_config(content);
        let config = MarabuntaConfig::from_file(file.path()).unwrap();

        assert!(config.coordinator.is_some());
        let coord = config.coordinator.unwrap();
        assert_eq!(coord.listen_port, 7000);
        assert_eq!(coord.raft_port, 7001);
        assert_eq!(coord.peers.len(), 2);
    }

    #[test]
    fn test_load_file_not_found() {
        let result = MarabuntaConfig::from_file("/nonexistent/path/config.toml");
        assert!(matches!(result, Err(ConfigError::FileNotFound(_))));
    }

    #[test]
    fn test_load_invalid_toml() {
        let content = "this is not valid toml [[[";
        let file = create_temp_config(content);
        let result = MarabuntaConfig::from_file(file.path());
        assert!(matches!(result, Err(ConfigError::ParseError(_))));
    }

    #[test]
    fn test_default_coordinator() {
        let config = MarabuntaConfig::default_coordinator();
        assert!(config.coordinator.is_some());
        assert!(config.master.is_none());
        assert!(config.worker.is_none());
        assert_eq!(config.node.name, "coordinator");
    }

    #[test]
    fn test_default_master() {
        let config = MarabuntaConfig::default_master();
        assert!(config.coordinator.is_none());
        assert!(config.master.is_some());
        assert!(config.worker.is_none());
        assert_eq!(config.node.name, "master");
    }

    #[test]
    fn test_default_worker() {
        let config = MarabuntaConfig::default_worker();
        assert!(config.coordinator.is_none());
        assert!(config.master.is_none());
        assert!(config.worker.is_some());
        assert_eq!(config.node.name, "worker");
    }

    #[test]
    fn test_validate_valid_config() {
        let config = MarabuntaConfig::default_coordinator();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_node_name() {
        let mut config = MarabuntaConfig::default();
        config.node.name = String::new();
        let result = config.validate();
        assert!(matches!(result, Err(ConfigError::ValidationError { .. })));
    }

    #[test]
    fn test_validate_invalid_cpu_limit() {
        let mut config = MarabuntaConfig::default_worker();
        if let Some(ref mut worker) = config.worker {
            worker.cpu_limit = Some(1.5); // Invalid: > 1.0
        }
        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::ValidationError { .. }) | Err(ConfigError::MultipleErrors(_))
        ));
    }

    #[test]
    fn test_validate_tls_without_certs() {
        let mut config = MarabuntaConfig::default();
        config.security.tls_enabled = true;
        // No cert_path or key_path set
        let result = config.validate();
        assert!(matches!(
            result,
            Err(ConfigError::ValidationError { .. }) | Err(ConfigError::MultipleErrors(_))
        ));
    }

    #[test]
    fn test_validate_coordinator_same_ports() {
        let mut config = MarabuntaConfig::default_coordinator();
        if let Some(ref mut coord) = config.coordinator {
            coord.listen_port = 7000;
            coord.raft_port = 7000; // Same port - invalid
        }
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_master_no_coordinators() {
        let mut config = MarabuntaConfig::default_master();
        if let Some(ref mut master) = config.master {
            master.coordinator_addresses.clear();
        }
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_worker_no_masters() {
        let mut config = MarabuntaConfig::default_worker();
        if let Some(ref mut worker) = config.worker {
            worker.master_addresses.clear();
        }
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_to_toml() {
        let config = MarabuntaConfig::default_worker();
        let toml = config.to_toml().unwrap();
        assert!(toml.contains("[node]"));
        assert!(toml.contains("[network]"));
        assert!(toml.contains("[worker]"));
    }

    #[test]
    fn test_write_and_read_config() {
        let config = MarabuntaConfig::default_master();
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test-config.toml");

        config.write_to_file(&path).unwrap();
        let loaded = MarabuntaConfig::from_file(&path).unwrap();

        assert_eq!(config.node.name, loaded.node.name);
        assert_eq!(config.network.bind_address, loaded.network.bind_address);
        assert!(loaded.master.is_some());
    }

    #[test]
    fn test_write_example_config() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("example.toml");

        MarabuntaConfig::write_example(&path).unwrap();
        assert!(path.exists());

        // Verify it's valid TOML that can be parsed
        let content = fs::read_to_string(&path).unwrap();
        let _: toml::Value = toml::from_str(&content).unwrap();
    }

    #[test]
    fn test_env_override() {
        let content = r#"
[node]
name = "original-name"

[network]
bind_address = "127.0.0.1:8000"
connect_timeout = "5s"
request_timeout = "15s"

[storage]
data_dir = "/tmp/marabunta"

[logging]
level = "info"

[security]
tls_enabled = false
"#;

        // Set environment variables
        env::set_var("MARABUNTA_NODE_NAME", "overridden-name");
        env::set_var("MARABUNTA_LOGGING_LEVEL", "debug");

        let file = create_temp_config(content);
        let config = MarabuntaConfig::from_file_with_env(file.path()).unwrap();

        // Clean up
        env::remove_var("MARABUNTA_NODE_NAME");
        env::remove_var("MARABUNTA_LOGGING_LEVEL");

        assert_eq!(config.node.name, "overridden-name");
        assert_eq!(config.logging.level, "debug");
    }
}
