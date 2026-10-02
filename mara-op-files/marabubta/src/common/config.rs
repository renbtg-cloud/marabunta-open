// Marabunta - Licensed under the MIT License.
//! Configuration types for all components

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Coordinator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    /// Listen address for HTTP API
    pub listen_addr: String,
    /// Port for HTTP API
    pub port: u16,
    /// Raft cluster peers
    pub raft_peers: Vec<String>,
    /// Heartbeat timeout for masters
    pub master_heartbeat_timeout: Duration,
    /// State store URL
    pub state_store_url: String,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0".into(),
            port: 7000,
            raft_peers: Vec::new(),
            master_heartbeat_timeout: Duration::from_secs(30),
            state_store_url: "redis://localhost:6379".into(),
        }
    }
}

/// Master configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterConfig {
    /// Listen address
    pub listen_addr: String,
    /// Port
    pub port: u16,
    /// Region name
    pub region: String,
    /// Coordinator addresses
    pub coordinators: Vec<String>,
    /// Worker heartbeat timeout
    pub worker_heartbeat_timeout: Duration,
    /// Maximum workers
    pub max_workers: u32,
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0".into(),
            port: 7100,
            region: "default".into(),
            coordinators: vec!["http://localhost:7000".into()],
            worker_heartbeat_timeout: Duration::from_secs(15),
            max_workers: 1000,
        }
    }
}

/// Worker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Master addresses
    pub masters: Vec<String>,
    /// Maximum concurrent tasks
    pub max_concurrent_tasks: u32,
    /// Checkpoint directory
    pub checkpoint_dir: PathBuf,
    /// Checkpoint interval
    pub checkpoint_interval: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Task timeout
    pub task_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            masters: vec!["http://localhost:7100".into()],
            max_concurrent_tasks: num_cpus::get() as u32,
            checkpoint_dir: std::path::PathBuf::from("/tmp/marabunta_checkpoints"),
            checkpoint_interval: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(5),
            task_timeout: Duration::from_secs(3600),
        }
    }
}

/// Load configuration from TOML file with env overrides
pub fn load_config<T: serde::de::DeserializeOwned + Default>(
    path: Option<&str>,
) -> Result<T, Box<dyn std::error::Error>> {
    let config = if let Some(path) = path {
        let contents = std::fs::read_to_string(path)?;
        toml::from_str(&contents)?
    } else {
        T::default()
    };
    Ok(config)
}
