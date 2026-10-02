// Marabunta - Licensed under the MIT License.
//! Configuration management for Marabunta Compute
//!
//! This module provides a comprehensive configuration system supporting:
//!
//! - Loading configuration from TOML files
//! - Environment variable overrides
//! - Default configuration templates for each node role
//! - Configuration validation
//! - Example configuration generation
//!
//! # Quick Start
//!
//! ```no_run
//! use marabunta_compute::config::MarabuntaConfig;
//!
//! // Load from default locations
//! let config = MarabuntaConfig::load_default()?;
//!
//! // Or load from a specific file
//! let config = MarabuntaConfig::from_file("./marabunta.toml")?;
//!
//! // Or load with environment variable overrides
//! let config = MarabuntaConfig::from_file_with_env("./marabunta.toml")?;
//!
//! // Validate the configuration
//! config.validate()?;
//! # Ok::<(), marabunta_compute::config::ConfigError>(())
//! ```
//!
//! # Default Configurations
//!
//! ```
//! use marabunta_compute::config::MarabuntaConfig;
//!
//! // Create default configs for different node roles
//! let coord_config = MarabuntaConfig::default_coordinator();
//! let master_config = MarabuntaConfig::default_master();
//! let worker_config = MarabuntaConfig::default_worker();
//! ```
//!
//! # Environment Variable Overrides
//!
//! Configuration values can be overridden using environment variables with the
//! `MARABUNTA_` prefix. The variable name follows the pattern `MARABUNTA_<SECTION>_<FIELD>`.
//!
//! Examples:
//! - `MARABUNTA_NODE_NAME=my-node` overrides `node.name`
//! - `MARABUNTA_NETWORK_BIND_ADDRESS=0.0.0.0:8080` overrides `network.bind_address`
//! - `MARABUNTA_LOGGING_LEVEL=debug` overrides `logging.level`
//! - `MARABUNTA_SECURITY_AUTH_TOKEN=secret` overrides `security.auth_token`
//!
//! # Configuration File Locations
//!
//! [`MarabuntaConfig::load_default`] checks the following locations in order:
//!
//! 1. `./marabunta.toml` - Current working directory
//! 2. `~/.config/marabunta/config.toml` - User configuration directory
//! 3. `/etc/marabunta/config.toml` - System configuration directory (Unix)
//! 4. `%ProgramData%/marabunta/config.toml` - System configuration directory (Windows)
//!
//! # Example Configuration
//!
//! Generate an example configuration file:
//!
//! ```no_run
//! use marabunta_compute::config::MarabuntaConfig;
//!
//! MarabuntaConfig::write_example("./marabunta.example.toml")?;
//! # Ok::<(), marabunta_compute::config::ConfigError>(())
//! ```

mod errors;
mod loader;
mod types;

// Re-export all public types
pub use errors::{ConfigError, ConfigResult};
pub use loader::{DEFAULT_CONFIG_NAME, EXAMPLE_CONFIG};
pub use types::{
    AuthLevelLimits, CoordinatorConfig, EndpointRateLimit, MarabuntaConfig, LogFormat, LoggingConfig,
    MasterConfig, NetworkConfig, NodeConfig, RateLimitValues, RateLimitingConfig, SecurityConfig,
    StorageConfig, WorkerConfig,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify all types are accessible
        let _ = MarabuntaConfig::default();
        let _ = NodeConfig::default();
        let _ = NetworkConfig::default();
        let _ = StorageConfig::default();
        let _ = CoordinatorConfig::default();
        let _ = MasterConfig::default();
        let _ = WorkerConfig::default();
        let _ = LoggingConfig::default();
        let _ = SecurityConfig::default();
        let _ = LogFormat::default();
    }

    #[test]
    fn test_default_config_name_constant() {
        assert_eq!(DEFAULT_CONFIG_NAME, "marabunta.toml");
    }

    #[test]
    fn test_example_config_not_empty() {
        assert!(!EXAMPLE_CONFIG.is_empty());
        assert!(EXAMPLE_CONFIG.contains("[node]"));
        assert!(EXAMPLE_CONFIG.contains("[network]"));
        assert!(EXAMPLE_CONFIG.contains("[storage]"));
        assert!(EXAMPLE_CONFIG.contains("[logging]"));
        assert!(EXAMPLE_CONFIG.contains("[security]"));
    }
}
