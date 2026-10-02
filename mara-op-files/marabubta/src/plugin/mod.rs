// Marabunta - Licensed under the MIT License.
//! Plugin system for the Marabunta Swarm.
//!
//! Provides a clean, opaque API surface for plugins to interact with the
//! swarm without exposing internal gossip, reputation, or marketplace details.
//!
//! The [`PluginHost`] orchestrator owns all plugin subsystems and manages the
//! full lifecycle: spawn processes → accept connections → register → start →
//! health-check → stop/restart.

pub mod config;
pub mod data_channels;
pub mod discovery;
pub mod handle;
pub mod migration;
pub mod process;
pub mod pubsub;
pub mod registry;
pub mod scatter;
pub mod storage;
pub mod transport;
pub mod types;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::plugin::config::{
    PluginHostConfig, PLUGIN_REGISTER_TIMEOUT,
    PLUGIN_START_TIMEOUT, PLUGIN_STOP_TIMEOUT, PLUGIN_STORAGE_MAX_ENTRIES,
    PLUGIN_STORAGE_MAX_VALUE_SIZE,
};
use crate::plugin::data_channels::DataChannelStore;
use crate::plugin::handle::SwarmHandleImpl;
use crate::plugin::process::ProcessManager;
use crate::plugin::pubsub::PubSubBroker;
use crate::plugin::registry::PluginRegistry;
use crate::plugin::scatter::ScatterEngine;
use crate::plugin::storage::PluginStorage;
use crate::plugin::transport::{PluginConnection, PluginListener};
use crate::plugin::types::*;
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::{NodeId, Trait};

// ============================================================================
// PluginHost — top-level orchestrator
// ============================================================================

/// Top-level orchestrator for the plugin system.
///
/// Owns all plugin subsystems and coordinates plugin lifecycle:
///
/// 1. **Spawn** plugin processes via [`ProcessManager`]
/// 2. **Accept** incoming connections via [`PluginListener`]
/// 3. **Register** plugins via the wire protocol (RegisterReq/RegisterResp)
/// 4. **Start** plugins (StartReq/StartResp)
/// 5. **Health-check** running plugins periodically
/// 6. **Restart** crashed plugins (if `auto_restart` is enabled)
/// 7. **Stop** all plugins on shutdown
///
/// The `PluginHost` is created by `SwarmNode` when plugin configuration
/// is present, and its [`run`](Self::run) method is spawned as a background
/// task alongside the other swarm subsystems.
pub struct PluginHost {
    /// This node's identity.
    node_id: NodeId,
    /// Plugin host configuration.
    host_config: PluginHostConfig,
    /// Plugin process lifecycle manager.
    process_manager: Arc<ProcessManager>,
    /// Plugin registry (registration, state, health tracking).
    registry: Arc<PluginRegistry>,
    /// Key-value storage engine for plugins.
    storage: Arc<PluginStorage>,
    /// Distributed scatter/gather engine.
    scatter_engine: Arc<ScatterEngine>,
    /// Topic-based pub/sub broker.
    pubsub: Arc<PubSubBroker>,
    /// The swarm handle that plugins interact with.
    swarm_handle: Arc<SwarmHandleImpl>,
    /// Data channel store for plugin-to-dashboard communication.
    data_channels: Arc<DataChannelStore>,
}

impl PluginHost {
    /// Create a new `PluginHost` wired to the given swarm subsystems.
    ///
    /// Does not start anything — call [`run`](Self::run) to begin the lifecycle.
    pub fn new(
        node_id: NodeId,
        region: String,
        host_config: PluginHostConfig,
        knowledge: Arc<KnowledgeStore>,
        current_traits: Arc<RwLock<HashSet<Trait>>>,
    ) -> Self {
        let node_id_str = node_id.to_string();

        let socket_dir = host_config.socket_dir.clone();
        let process_manager = Arc::new(ProcessManager::new(socket_dir));
        let registry = Arc::new(PluginRegistry::new(
            node_id_str.clone(),
            host_config.max_plugins,
        ));
        let storage = Arc::new(PluginStorage::new(
            PLUGIN_STORAGE_MAX_ENTRIES,
            PLUGIN_STORAGE_MAX_VALUE_SIZE,
        ));
        let scatter_engine = Arc::new(ScatterEngine::new(
            node_id,
            Arc::clone(&knowledge),
            host_config.scatter_max_concurrent,
        ));
        let pubsub = Arc::new(PubSubBroker::new(node_id_str));
        let data_channels = Arc::new(DataChannelStore::new());

        let swarm_handle = Arc::new(SwarmHandleImpl::new(
            node_id,
            region,
            knowledge,
            Arc::clone(&storage),
            Arc::clone(&scatter_engine),
            Arc::clone(&pubsub),
            Arc::clone(&registry),
            current_traits,
            Arc::clone(&data_channels),
        ));

        Self {
            node_id,
            host_config,
            process_manager,
            registry,
            storage,
            scatter_engine,
            pubsub,
            swarm_handle,
            data_channels,
        }
    }

    /// Get the plugin registry (for external queries like API endpoints).
    pub fn registry(&self) -> &Arc<PluginRegistry> {
        &self.registry
    }

    /// Get the process manager (for external queries).
    pub fn process_manager(&self) -> &Arc<ProcessManager> {
        &self.process_manager
    }

    /// Get the data channel store (for API endpoints and WebSocket feeds).
    pub fn data_channels(&self) -> &Arc<DataChannelStore> {
        &self.data_channels
    }

    /// Run the plugin host.
    ///
    /// 1. Validates all plugin configurations.
    /// 2. Spawns enabled plugin processes.
    /// 3. Listens for incoming connections on each plugin's Unix socket.
    /// 4. Handles the registration handshake.
    /// 5. Sends Start with plugin-specific config.
    /// 6. Spawns a health-check loop for each running plugin.
    /// 7. On shutdown, sends Stop to all running plugins and waits.
    pub async fn run(&self, mut shutdown_rx: watch::Receiver<bool>) {
        // Validate config.
        if let Err(e) = self.host_config.validate() {
            error!(error = %e, "plugin host config validation failed, not starting plugins");
            return;
        }

        let enabled_plugins: Vec<_> = self
            .host_config
            .plugins
            .iter()
            .filter(|p| p.enabled)
            .cloned()
            .collect();

        if enabled_plugins.is_empty() {
            info!("no enabled plugins configured, plugin host idle");
            // Just wait for shutdown.
            let _ = shutdown_rx.changed().await;
            return;
        }

        info!(
            count = enabled_plugins.len(),
            "starting plugin host with {} plugin(s)",
            enabled_plugins.len()
        );

        // Spawn each plugin and set up its connection.
        let mut plugin_handles = Vec::new();

        for plugin_config in &enabled_plugins {
            if !plugin_config.is_external() {
                warn!(
                    plugin = %plugin_config.name,
                    "in-process (library) plugins not yet supported, skipping"
                );
                continue;
            }

            // Spawn the plugin process.
            let plugin_id = PluginId(format!("process-{}", plugin_config.name));
            match self.process_manager.spawn_plugin(&plugin_id, plugin_config).await {
                Ok(pid) => {
                    info!(
                        plugin = %plugin_config.name,
                        pid = pid,
                        "plugin process spawned"
                    );
                }
                Err(e) => {
                    error!(
                        plugin = %plugin_config.name,
                        error = %e,
                        "failed to spawn plugin, skipping"
                    );
                    continue;
                }
            }

            // Wait for the plugin to connect to its socket.
            let socket_path = self
                .process_manager
                .get_socket_path(&plugin_id)
                .unwrap_or_else(|| {
                    self.host_config
                        .socket_dir
                        .join(format!("{}.sock", plugin_config.name))
                });

            let config_clone = plugin_config.clone();
            let registry = Arc::clone(&self.registry);
            let process_manager = Arc::clone(&self.process_manager);
            let swarm_handle = Arc::clone(&self.swarm_handle);
            let plugin_id_clone = plugin_id.clone();
            let shutdown_rx_clone = shutdown_rx.clone();

            // Spawn per-plugin lifecycle task.
            let handle = tokio::spawn(async move {
                run_plugin_lifecycle(
                    socket_path,
                    config_clone,
                    plugin_id_clone,
                    registry,
                    process_manager,
                    swarm_handle,
                    shutdown_rx_clone,
                )
                .await;
            });
            plugin_handles.push(handle);
        }

        // Wait for shutdown signal.
        loop {
            match shutdown_rx.changed().await {
                Ok(()) => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        info!("plugin host shutting down, stopping all plugins");

        // Stop all running plugin processes.
        for info in self.process_manager.list_processes() {
            if self.process_manager.is_running(&info.plugin_id) {
                if let Err(e) = self
                    .process_manager
                    .stop_plugin(&info.plugin_id, PLUGIN_STOP_TIMEOUT)
                    .await
                {
                    warn!(
                        plugin = %info.plugin_name,
                        error = %e,
                        "error stopping plugin on shutdown"
                    );
                }
            }
        }

        // Wait for all per-plugin tasks to complete (with timeout).
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            for handle in plugin_handles {
                let _ = handle.await;
            }
        })
        .await;

        info!("plugin host stopped");
    }
}

// ============================================================================
// Per-plugin lifecycle task
// ============================================================================

/// Manages the lifecycle of a single plugin process.
///
/// Accepts the connection, performs the registration handshake, sends Start,
/// then runs a health-check loop. Handles auto-restart on crash.
async fn run_plugin_lifecycle(
    socket_path: PathBuf,
    plugin_config: config::PluginConfig,
    plugin_id: PluginId,
    registry: Arc<PluginRegistry>,
    process_manager: Arc<ProcessManager>,
    _swarm_handle: Arc<SwarmHandleImpl>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let plugin_name = plugin_config.name.clone();

    // Wait for the plugin to create its socket and connect.
    let listener = match wait_for_socket_and_bind(&socket_path, PLUGIN_REGISTER_TIMEOUT).await {
        Ok(listener) => listener,
        Err(e) => {
            error!(
                plugin = %plugin_name,
                error = %e,
                "failed to bind plugin listener socket"
            );
            return;
        }
    };

    // Accept the plugin's connection.
    let conn = match tokio::time::timeout(PLUGIN_REGISTER_TIMEOUT, listener.accept()).await {
        Ok(Ok(conn)) => {
            info!(plugin = %plugin_name, "plugin connected");
            Arc::new(conn)
        }
        Ok(Err(e)) => {
            error!(plugin = %plugin_name, error = %e, "failed to accept plugin connection");
            return;
        }
        Err(_) => {
            error!(plugin = %plugin_name, "timed out waiting for plugin to connect");
            return;
        }
    };

    // Wait for RegisterReq from the plugin.
    let registered_id = match handle_registration(&conn, &registry, PLUGIN_REGISTER_TIMEOUT).await
    {
        Ok(id) => id,
        Err(e) => {
            error!(plugin = %plugin_name, error = %e, "plugin registration failed");
            return;
        }
    };

    info!(
        plugin = %plugin_name,
        plugin_id = %registered_id,
        "plugin registered successfully"
    );

    // Update process manager state.
    process_manager.set_state(&plugin_id, PluginState::Registered);

    // Send Start with plugin-specific config.
    let config_bytes = plugin_config.config_bytes();
    match handle_start(&conn, &config_bytes, PLUGIN_START_TIMEOUT).await {
        Ok(()) => {
            info!(plugin = %plugin_name, "plugin started");
            if let Err(e) = registry.update_state(&registered_id, PluginState::Running) {
                warn!(plugin = %plugin_name, error = %e, "failed to update registry state to Running");
            }
            process_manager.set_state(&plugin_id, PluginState::Running);
        }
        Err(e) => {
            error!(plugin = %plugin_name, error = %e, "plugin start failed");
            if let Err(e) = registry.update_state(&registered_id, PluginState::Failed) {
                warn!(plugin = %plugin_name, error = %e, "failed to update registry state to Failed");
            }
            return;
        }
    }

    // Run health-check loop.
    let health_interval = plugin_config.effective_health_interval();
    let max_failures = config::PLUGIN_MAX_HEALTH_FAILURES;
    let mut consecutive_failures = 0u32;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(health_interval) => {}
            result = shutdown_rx.changed() => {
                if result.is_err() || *shutdown_rx.borrow() {
                    debug!(plugin = %plugin_name, "shutdown signal, sending stop to plugin");
                    // Send Stop to the plugin.
                    let _ = handle_stop(&conn, 30).await;
                    return;
                }
            }
        }

        if *shutdown_rx.borrow() {
            let _ = handle_stop(&conn, 30).await;
            return;
        }

        // Check if process is still alive.
        if !process_manager.is_running(&plugin_id) {
            warn!(plugin = %plugin_name, "plugin process exited unexpectedly");
            if let Err(e) = registry.update_state(&registered_id, PluginState::Failed) {
                warn!(plugin = %plugin_name, error = %e, "failed to update registry state to Failed");
            }

            if plugin_config.auto_restart {
                info!(plugin = %plugin_name, "auto-restarting plugin");
                match process_manager.restart_plugin(&plugin_id).await {
                    Ok(new_pid) => {
                        info!(plugin = %plugin_name, pid = new_pid, "plugin restarted");
                        // The restarted plugin will need a new connection/registration cycle.
                        // For simplicity, we exit this lifecycle and let the host re-establish.
                        return;
                    }
                    Err(e) => {
                        error!(plugin = %plugin_name, error = %e, "plugin restart failed");
                        return;
                    }
                }
            }
            return;
        }

        // Send health check.
        match handle_health_check(&conn).await {
            Ok(health_resp) => {
                consecutive_failures = 0;
                if let Err(e) = registry.update_health(&registered_id, health_resp) {
                    warn!(plugin = %plugin_name, error = %e, "failed to update registry health");
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                warn!(
                    plugin = %plugin_name,
                    failures = consecutive_failures,
                    error = %e,
                    "health check failed"
                );

                if consecutive_failures >= max_failures {
                    error!(
                        plugin = %plugin_name,
                        "max health check failures reached, marking plugin as failed"
                    );
                    if let Err(e) = registry.update_state(&registered_id, PluginState::Failed) {
                        warn!(plugin = %plugin_name, error = %e, "failed to update registry state to Failed");
                    }

                    if plugin_config.auto_restart {
                        match process_manager.restart_plugin(&plugin_id).await {
                            Ok(new_pid) => {
                                info!(plugin = %plugin_name, pid = new_pid, "plugin restarted after health failures");
                                return;
                            }
                            Err(e) => {
                                error!(plugin = %plugin_name, error = %e, "plugin restart failed");
                                return;
                            }
                        }
                    }
                    return;
                }
            }
        }
    }
}

// ============================================================================
// Protocol helpers
// ============================================================================

/// Wait for a socket file to appear (the plugin process creates it), then bind a listener.
async fn wait_for_socket_and_bind(
    socket_path: &std::path::Path,
    _timeout: Duration,
) -> PluginResult<PluginListener> {
    // The plugin process creates the socket and connects as a client.
    // We (the host) listen on the socket and accept. But the ProcessManager
    // passes SWARM_SOCKET to the plugin, so we need to bind first.
    //
    // Actually: the host creates the listener socket, and the plugin
    // connects to it. So we just bind immediately.
    PluginListener::bind(socket_path).await
}

/// Handle the registration handshake: receive RegisterReq, process it, send RegisterResp.
async fn handle_registration(
    conn: &PluginConnection,
    registry: &PluginRegistry,
    timeout: Duration,
) -> PluginResult<PluginId> {
    let msg = tokio::time::timeout(timeout, conn.recv())
        .await
        .map_err(|_| PluginError::Timeout("registration timeout".into()))?
        .map_err(|e| PluginError::Transport(format!("failed to receive registration: {}", e)))?;

    match msg {
        PluginWireMessage::RegisterReq(req) => {
            let resp = registry.register_plugin(req)?;
            let plugin_id = PluginId(resp.plugin_id.clone());
            conn.send(&PluginWireMessage::RegisterResp(resp)).await?;
            Ok(plugin_id)
        }
        other => Err(PluginError::Lifecycle(format!(
            "expected RegisterReq, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

/// Send Start to a plugin and wait for StartResp.
async fn handle_start(
    conn: &PluginConnection,
    config_bytes: &[u8],
    timeout: Duration,
) -> PluginResult<()> {
    let start_req = PluginWireMessage::StartReq(StartRequest {
        config: config_bytes.to_vec(),
    });

    conn.send(&start_req).await?;

    let resp = tokio::time::timeout(timeout, conn.recv())
        .await
        .map_err(|_| PluginError::Timeout("start timeout".into()))?
        .map_err(|e| PluginError::Transport(format!("failed to receive start response: {}", e)))?;

    match resp {
        PluginWireMessage::StartResp(start_resp) => {
            if start_resp.success {
                Ok(())
            } else {
                Err(PluginError::Lifecycle(format!(
                    "plugin start failed: {}",
                    start_resp.error
                )))
            }
        }
        other => Err(PluginError::Lifecycle(format!(
            "expected StartResp, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

/// Send a health check and return the response.
async fn handle_health_check(conn: &PluginConnection) -> PluginResult<HealthResponse> {
    let req = PluginWireMessage::HealthReq(HealthRequest {});
    conn.send(&req).await?;

    let resp = tokio::time::timeout(Duration::from_secs(5), conn.recv())
        .await
        .map_err(|_| PluginError::Timeout("health check timeout".into()))?
        .map_err(|e| {
            PluginError::HealthCheck(format!("failed to receive health response: {}", e))
        })?;

    match resp {
        PluginWireMessage::HealthResp(health) => Ok(health),
        other => Err(PluginError::HealthCheck(format!(
            "expected HealthResp, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

/// Send a stop request to the plugin.
async fn handle_stop(conn: &PluginConnection, timeout_seconds: u32) -> PluginResult<bool> {
    let req = PluginWireMessage::StopReq(StopRequest { timeout_seconds });
    conn.send(&req).await?;

    let resp = tokio::time::timeout(Duration::from_secs(timeout_seconds as u64 + 5), conn.recv())
        .await
        .map_err(|_| PluginError::Timeout("stop timeout".into()))?
        .map_err(|e| PluginError::Transport(format!("failed to receive stop response: {}", e)))?;

    match resp {
        PluginWireMessage::StopResp(stop_resp) => Ok(stop_resp.clean),
        _ => Ok(false),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_host_can_be_created() {
        let node_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        let traits = Arc::new(RwLock::new(HashSet::new()));
        let config = PluginHostConfig::default();

        let host = PluginHost::new(
            node_id,
            "us-west-2".to_string(),
            config,
            knowledge,
            traits,
        );

        assert_eq!(host.node_id, node_id);
        assert_eq!(host.registry.plugin_count(), 0);
    }

    #[test]
    fn plugin_host_registry_accessible() {
        let node_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(node_id));
        let traits = Arc::new(RwLock::new(HashSet::new()));
        let config = PluginHostConfig::default();

        let host = PluginHost::new(
            node_id,
            "us-east-1".to_string(),
            config,
            knowledge,
            traits,
        );

        // Registry should start empty.
        assert_eq!(host.registry().plugin_count(), 0);
        assert!(host.registry().list_plugins().is_empty());
    }
}
