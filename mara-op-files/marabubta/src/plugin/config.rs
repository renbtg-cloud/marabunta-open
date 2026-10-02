// Marabunta - Licensed under the MIT License.
//! Plugin configuration for the Marabunta Swarm.
//!
//! Plugins are configured via `[[plugins]]` sections in `marabunta.toml`.
//! Each plugin entry specifies how to launch and communicate with the plugin.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use serde::{Deserialize, Serialize};

// ============================================================================
// Constants
// ============================================================================

/// Default health check interval for plugins.
pub const PLUGIN_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Maximum time to wait for a plugin to register after spawning.
pub const PLUGIN_REGISTER_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum time to wait for a plugin to start after registration.
pub const PLUGIN_START_TIMEOUT: Duration = Duration::from_secs(60);

/// Default graceful shutdown timeout for plugins.
pub const PLUGIN_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum consecutive health check failures before declaring plugin dead.
pub const PLUGIN_MAX_HEALTH_FAILURES: u32 = 3;

/// Maximum restart attempts before giving up on a plugin.
pub const PLUGIN_MAX_RESTART_ATTEMPTS: u32 = 5;

/// Backoff between restart attempts.
pub const PLUGIN_RESTART_BACKOFF: Duration = Duration::from_secs(5);

/// Maximum number of plugins that can be loaded simultaneously.
pub const MAX_PLUGINS: usize = 64;

/// Default Unix socket directory for plugin communication.
pub const PLUGIN_SOCKET_DIR: &str = "/tmp/marabunta/plugins";

/// Maximum wire message size for plugin communication (16 MB).
pub const PLUGIN_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Maximum number of pending requests per plugin.
pub const PLUGIN_MAX_PENDING_REQUESTS: usize = 1024;

/// Default number of replicas for plugin storage.
pub const PLUGIN_DEFAULT_REPLICAS: u32 = 3;

/// Default scatter timeout in milliseconds.
pub const PLUGIN_DEFAULT_SCATTER_TIMEOUT_MS: u32 = 30_000;

/// Maximum subscriptions per plugin.
pub const PLUGIN_MAX_SUBSCRIPTIONS: usize = 256;

/// Maximum topics in the pub/sub broker.
pub const PUBSUB_MAX_TOPICS: usize = 4096;

/// Maximum subscribers per topic.
pub const PUBSUB_MAX_SUBSCRIBERS_PER_TOPIC: usize = 256;

/// Maximum pending events in a subscription channel.
pub const PUBSUB_MAX_PENDING_EVENTS: usize = 10_000;

/// Default maximum entries in the plugin storage cache.
pub const PLUGIN_STORAGE_MAX_ENTRIES: usize = 100_000;

/// Default maximum value size in the plugin storage (4 MB).
pub const PLUGIN_STORAGE_MAX_VALUE_SIZE: usize = 4 * 1024 * 1024;

/// Default maximum concurrent scatter operations per plugin host.
pub const PLUGIN_SCATTER_MAX_CONCURRENT: usize = 128;

/// Maximum concurrent migrations.
pub const MIGRATION_MAX_CONCURRENT: usize = 16;

/// Migration timeout.
pub const MIGRATION_TIMEOUT: Duration = Duration::from_secs(300);

// ============================================================================
// PluginConfig
// ============================================================================

/// Configuration for a single plugin, parsed from `marabunta.toml`.
///
/// ```toml
/// [[plugins]]
/// name = "postgres"
/// binary = "./plugins/marabunta-postgres"
/// config = { port = 5432, instances = ["sales", "analytics"] }
///
/// [[plugins]]
/// name = "redis"
/// binary = "python3 ./plugins/redis_plugin.py"
/// config = { port = 6379 }
///
/// [[plugins]]
/// name = "custom"
/// library = "./plugins/libcustom.so"
/// config = { whatever = "you need" }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Plugin name (must be unique across all loaded plugins).
    pub name: String,

    /// Path to an external binary to spawn (out-of-process plugin).
    /// Mutually exclusive with `library`.
    #[serde(default)]
    pub binary: Option<String>,

    /// Path to a shared library to load (in-process plugin).
    /// Mutually exclusive with `binary`.
    #[serde(default)]
    pub library: Option<String>,

    /// Plugin-specific configuration (passed as JSON to Start()).
    #[serde(default = "default_plugin_config")]
    pub config: toml::Value,

    /// Whether the plugin should be automatically restarted on crash.
    #[serde(default = "default_auto_restart")]
    pub auto_restart: bool,

    /// Health check interval override.
    #[serde(default, with = "humantime_serde")]
    pub health_check_interval: Option<Duration>,

    /// Maximum restart attempts before giving up.
    #[serde(default)]
    pub max_restart_attempts: Option<u32>,

    /// Environment variables to set for the plugin process.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Working directory for the plugin process.
    #[serde(default)]
    pub working_dir: Option<PathBuf>,

    /// Whether this plugin is enabled (allows disabling without removing config).
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Maximum memory (address space) in MB. Applied via RLIMIT_AS.
    #[serde(default)]
    pub max_memory_mb: Option<u64>,

    /// Maximum CPU time in seconds. Applied via RLIMIT_CPU.
    #[serde(default)]
    pub max_cpu_seconds: Option<u64>,

    /// Maximum number of open file descriptors. Applied via RLIMIT_NOFILE.
    #[serde(default)]
    pub max_open_files: Option<u64>,

    /// Maximum number of processes/threads. Applied via RLIMIT_NPROC.
    #[serde(default)]
    pub max_processes: Option<u64>,
}

fn default_plugin_config() -> toml::Value {
    toml::Value::Table(toml::map::Map::new())
}

fn default_auto_restart() -> bool {
    true
}

fn default_enabled() -> bool {
    true
}

impl PluginConfig {
    /// Validate this configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("plugin name cannot be empty".into());
        }
        if self.name.len() > 64 {
            return Err("plugin name too long (max 64 chars)".into());
        }
        if !self.name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
            return Err("plugin name must be alphanumeric with hyphens/underscores".into());
        }
        if self.binary.is_none() && self.library.is_none() {
            return Err("plugin must have either 'binary' or 'library' set".into());
        }
        if self.binary.is_some() && self.library.is_some() {
            return Err("plugin cannot have both 'binary' and 'library' set".into());
        }
        Ok(())
    }

    /// Whether this plugin runs out-of-process (external binary).
    pub fn is_external(&self) -> bool {
        self.binary.is_some()
    }

    /// Whether this plugin runs in-process (shared library or Rust trait).
    pub fn is_internal(&self) -> bool {
        self.library.is_some()
    }

    /// Get the effective health check interval.
    pub fn effective_health_interval(&self) -> Duration {
        self.health_check_interval.unwrap_or(PLUGIN_HEALTH_CHECK_INTERVAL)
    }

    /// Get the effective max restart attempts.
    pub fn effective_max_restarts(&self) -> u32 {
        self.max_restart_attempts.unwrap_or(PLUGIN_MAX_RESTART_ATTEMPTS)
    }

    /// Serialize the plugin-specific config to JSON bytes for Start().
    pub fn config_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.config).unwrap_or_default()
    }
}

// ============================================================================
// PluginHostConfig
// ============================================================================

/// Top-level configuration for the plugin hosting system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginHostConfig {
    /// All plugin configurations.
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,

    /// Directory for Unix sockets used for plugin communication.
    #[serde(default = "default_socket_dir")]
    pub socket_dir: PathBuf,

    /// Whether to enable the gRPC listener for external plugins.
    #[serde(default)]
    pub grpc_enabled: bool,

    /// Address for the gRPC plugin host listener.
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,

    /// Maximum total plugins allowed.
    #[serde(default = "default_max_plugins")]
    pub max_plugins: usize,

    /// Maximum number of scatter units that may be in-flight simultaneously.
    ///
    /// With large shard counts (5000+), the old default of 128 is too
    /// restrictive. Set this higher to allow more parallel scatter work.
    /// Falls back to 1024 if absent from config (backward compatible).
    #[serde(default = "default_scatter_max_concurrent")]
    pub scatter_max_concurrent: usize,
}

fn default_socket_dir() -> PathBuf {
    PathBuf::from(PLUGIN_SOCKET_DIR)
}

fn default_grpc_addr() -> String {
    "0.0.0.0:4201".to_string()
}

fn default_max_plugins() -> usize {
    MAX_PLUGINS
}

fn default_scatter_max_concurrent() -> usize {
    1024
}

impl Default for PluginHostConfig {
    fn default() -> Self {
        Self {
            plugins: Vec::new(),
            socket_dir: default_socket_dir(),
            grpc_enabled: false,
            grpc_addr: default_grpc_addr(),
            max_plugins: MAX_PLUGINS,
            scatter_max_concurrent: default_scatter_max_concurrent(),
        }
    }
}

impl PluginHostConfig {
    /// Validate all plugin configurations.
    pub fn validate(&self) -> Result<(), String> {
        if self.plugins.len() > self.max_plugins {
            return Err(format!(
                "too many plugins configured ({}, max {})",
                self.plugins.len(),
                self.max_plugins
            ));
        }

        let mut names = std::collections::HashSet::new();
        for plugin in &self.plugins {
            plugin.validate()?;
            if !names.insert(&plugin.name) {
                return Err(format!("duplicate plugin name: {}", plugin.name));
            }
        }

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_config_validate_valid_binary() {
        let config = PluginConfig {
            name: "postgres".into(),
            binary: Some("./plugins/marabunta-postgres".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };
        assert!(config.validate().is_ok());
        assert!(config.is_external());
        assert!(!config.is_internal());
    }

    #[test]
    fn plugin_config_validate_valid_library() {
        let config = PluginConfig {
            name: "custom-plugin".into(),
            binary: None,
            library: Some("./plugins/libcustom.so".into()),
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };
        assert!(config.validate().is_ok());
        assert!(!config.is_external());
        assert!(config.is_internal());
    }

    #[test]
    fn plugin_config_validate_empty_name() {
        let config = PluginConfig {
            name: String::new(),
            binary: Some("test".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn plugin_config_validate_no_binary_or_library() {
        let config = PluginConfig {
            name: "orphan".into(),
            binary: None,
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn plugin_config_validate_both_binary_and_library() {
        let config = PluginConfig {
            name: "both".into(),
            binary: Some("bin".into()),
            library: Some("lib".into()),
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn plugin_config_validate_invalid_name_chars() {
        let config = PluginConfig {
            name: "my plugin!".into(),
            binary: Some("test".into()),
            library: None,
            config: toml::Value::Table(toml::map::Map::new()),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn plugin_host_config_validate_duplicate_names() {
        let config = PluginHostConfig {
            plugins: vec![
                PluginConfig {
                    name: "postgres".into(),
                    binary: Some("a".into()),
                    library: None,
                    config: toml::Value::Table(toml::map::Map::new()),
                    auto_restart: true,
                    health_check_interval: None,
                    max_restart_attempts: None,
                    env: HashMap::new(),
                    working_dir: None,
                    enabled: true,
                    max_memory_mb: None,
                    max_cpu_seconds: None,
                    max_open_files: None,
                    max_processes: None,
                },
                PluginConfig {
                    name: "postgres".into(),
                    binary: Some("b".into()),
                    library: None,
                    config: toml::Value::Table(toml::map::Map::new()),
                    auto_restart: true,
                    health_check_interval: None,
                    max_restart_attempts: None,
                    env: HashMap::new(),
                    working_dir: None,
                    enabled: true,
                    max_memory_mb: None,
                    max_cpu_seconds: None,
                    max_open_files: None,
                    max_processes: None,
                },
            ],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn plugin_host_config_default() {
        let config = PluginHostConfig::default();
        assert!(config.plugins.is_empty());
        assert!(!config.grpc_enabled);
        assert_eq!(config.max_plugins, MAX_PLUGINS);
    }

    #[test]
    fn config_bytes_serializes() {
        let mut map = toml::map::Map::new();
        map.insert("port".into(), toml::Value::Integer(5432));
        let config = PluginConfig {
            name: "test".into(),
            binary: Some("test".into()),
            library: None,
            config: toml::Value::Table(map),
            auto_restart: true,
            health_check_interval: None,
            max_restart_attempts: None,
            env: HashMap::new(),
            working_dir: None,
            enabled: true,
            max_memory_mb: None,
            max_cpu_seconds: None,
            max_open_files: None,
            max_processes: None,
        };
        let bytes = config.config_bytes();
        assert!(!bytes.is_empty());
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["port"], 5432);
    }

    #[test]
    fn toml_roundtrip() {
        let toml_str = r#"
[[plugins]]
name = "postgres"
binary = "./plugins/marabunta-postgres"
auto_restart = true

[plugins.config]
port = 5432
instances = ["sales", "analytics"]

[[plugins]]
name = "redis"
binary = "python3 ./plugins/redis_plugin.py"

[plugins.config]
port = 6379
"#;

        let host_config: PluginHostConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(host_config.plugins.len(), 2);
        assert_eq!(host_config.plugins[0].name, "postgres");
        assert_eq!(host_config.plugins[1].name, "redis");
        assert!(host_config.validate().is_ok());
    }
}
