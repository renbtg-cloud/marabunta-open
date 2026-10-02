// Marabunta - Licensed under the MIT License.
//! Plugin registry for managing registered plugins, their capabilities, and endpoints.
//!
//! The `PluginRegistry` is the central data structure that tracks all plugins
//! in the system. It stores plugin metadata, lifecycle state, health status,
//! and references to plugin service implementations. Thread-safe via `DashMap`.

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use uuid::Uuid;

use crate::plugin::config::MAX_PLUGINS;
use crate::plugin::types::{
    Endpoint, HealthResponse, PluginError, PluginId, PluginResult, PluginService, PluginState,
    RegisterRequest, RegisterResponse,
};

// ============================================================================
// PluginRecord — internal record stored in the registry
// ============================================================================

/// Full internal record for a registered plugin.
///
/// This struct is stored inside the registry's `DashMap`. It contains the
/// `service` field as an `Option<Arc<dyn PluginService>>` which is set
/// after registration when the plugin service implementation is attached.
pub struct PluginRecord {
    /// Unique plugin identifier assigned at registration.
    pub id: PluginId,
    /// Human-readable plugin name (e.g., "postgres").
    pub name: String,
    /// Semver version string.
    pub version: String,
    /// Current lifecycle state.
    pub state: PluginState,
    /// Traits declared by this plugin (e.g., ["CanStoreState"]).
    pub traits: Vec<String>,
    /// Network endpoints exposed by this plugin.
    pub endpoints: Vec<Endpoint>,
    /// Timestamp when the plugin was registered.
    pub registered_at: chrono::DateTime<chrono::Utc>,
    /// Most recent health check response (None if never checked).
    pub last_health: Option<HealthResponse>,
    /// Timestamp of the most recent health check (None if never checked).
    pub last_health_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Number of times this plugin has been restarted.
    pub restart_count: u32,
    /// Reference to the plugin's service implementation (set after registration).
    pub service: Option<Arc<dyn PluginService>>,
}

// ============================================================================
// PluginInfo — external-facing snapshot without the service reference
// ============================================================================

/// A snapshot of plugin metadata suitable for external queries.
///
/// This struct is a clone-friendly subset of `PluginRecord` that excludes
/// the `service` field. Used by `list_plugins()` and `get_plugin()`.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    /// Unique plugin identifier.
    pub id: PluginId,
    /// Human-readable plugin name.
    pub name: String,
    /// Semver version string.
    pub version: String,
    /// Current lifecycle state.
    pub state: PluginState,
    /// Traits declared by this plugin.
    pub traits: Vec<String>,
    /// Network endpoints exposed by this plugin.
    pub endpoints: Vec<Endpoint>,
    /// Timestamp when the plugin was registered.
    pub registered_at: chrono::DateTime<chrono::Utc>,
    /// Status string from the most recent health check, if any.
    pub last_health_status: Option<String>,
    /// Number of times this plugin has been restarted.
    pub restart_count: u32,
}

impl PluginRecord {
    /// Convert this record into a `PluginInfo` snapshot (without the service reference).
    fn to_info(&self) -> PluginInfo {
        PluginInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
            state: self.state,
            traits: self.traits.clone(),
            endpoints: self.endpoints.clone(),
            registered_at: self.registered_at,
            last_health_status: self.last_health.as_ref().map(|h| h.status.clone()),
            restart_count: self.restart_count,
        }
    }
}

// ============================================================================
// PluginRegistry
// ============================================================================

/// Thread-safe registry for managing plugin records.
///
/// Uses `DashMap` for lock-free concurrent reads and writes. A secondary
/// `name_index` map provides O(1) lookup by plugin name to enforce
/// name uniqueness and support name-based queries.
pub struct PluginRegistry {
    /// Primary store: PluginId -> PluginRecord.
    plugins: DashMap<PluginId, PluginRecord>,
    /// Secondary index: plugin name -> PluginId for uniqueness enforcement.
    name_index: DashMap<String, PluginId>,
    /// Maximum number of plugins allowed in this registry.
    max_plugins: usize,
    /// The node ID string to include in registration responses.
    node_id_str: String,
}

impl PluginRegistry {
    /// Create a new plugin registry.
    ///
    /// # Arguments
    /// * `node_id_str` - The node ID string returned to plugins in `RegisterResponse`.
    /// * `max_plugins` - Maximum number of plugins allowed. Use `MAX_PLUGINS` for the default.
    pub fn new(node_id_str: String, max_plugins: usize) -> Self {
        Self {
            plugins: DashMap::new(),
            name_index: DashMap::new(),
            max_plugins,
            node_id_str,
        }
    }

    /// Create a new plugin registry with the default capacity from config.
    pub fn with_defaults(node_id_str: String) -> Self {
        Self::new(node_id_str, MAX_PLUGINS)
    }

    /// Register a new plugin.
    ///
    /// 1. Checks name uniqueness via the `name_index`.
    /// 2. Checks the `max_plugins` capacity limit.
    /// 3. Generates a `PluginId` using the format `"plugin-{name}-{short_uuid}"`.
    /// 4. Creates a `PluginRecord` with state `Registered`.
    /// 5. Inserts into both `plugins` and `name_index`.
    /// 6. Returns a `RegisterResponse` with `plugin_id` and `node_id`.
    pub fn register_plugin(&self, request: RegisterRequest) -> PluginResult<RegisterResponse> {
        // Check name uniqueness
        if self.name_index.contains_key(&request.name) {
            return Err(PluginError::AlreadyRegistered(format!(
                "plugin with name '{}' is already registered",
                request.name
            )));
        }

        // Check capacity
        if self.plugins.len() >= self.max_plugins {
            return Err(PluginError::CapacityExceeded(format!(
                "maximum plugin count reached ({}/{})",
                self.plugins.len(),
                self.max_plugins
            )));
        }

        // Generate plugin ID: plugin-{name}-{short_uuid}
        let short_uuid = &Uuid::new_v4().to_string()[..8];
        let plugin_id = PluginId(format!("plugin-{}-{}", request.name, short_uuid));

        // Create record
        let record = PluginRecord {
            id: plugin_id.clone(),
            name: request.name.clone(),
            version: request.version.clone(),
            state: PluginState::Registered,
            traits: request.traits.clone(),
            endpoints: request.endpoints.clone(),
            registered_at: Utc::now(),
            last_health: None,
            last_health_at: None,
            restart_count: 0,
            service: None,
        };

        // Insert into both maps
        self.name_index
            .insert(request.name, plugin_id.clone());
        self.plugins.insert(plugin_id.clone(), record);

        Ok(RegisterResponse {
            plugin_id: plugin_id.0,
            node_id: self.node_id_str.clone(),
            swarm_config: Vec::new(),
        })
    }

    /// Unregister a plugin, removing it from both the primary store and name index.
    ///
    /// Returns `PluginError::NotFound` if the plugin ID does not exist.
    pub fn unregister_plugin(&self, id: &PluginId) -> PluginResult<()> {
        let removed = self.plugins.remove(id);
        match removed {
            Some((_key, record)) => {
                self.name_index.remove(&record.name);
                Ok(())
            }
            None => Err(PluginError::NotFound(format!(
                "plugin '{}' not found",
                id.0
            ))),
        }
    }

    /// Get a snapshot of a plugin's metadata (without the service reference).
    ///
    /// Returns `None` if the plugin ID does not exist.
    pub fn get_plugin(&self, id: &PluginId) -> Option<PluginInfo> {
        self.plugins.get(id).map(|entry| entry.value().to_info())
    }

    /// Look up a plugin ID by its human-readable name.
    ///
    /// Returns `None` if no plugin with that name is registered.
    pub fn get_plugin_by_name(&self, name: &str) -> Option<PluginId> {
        self.name_index.get(name).map(|entry| entry.value().clone())
    }

    /// Update a plugin's lifecycle state.
    ///
    /// Returns `PluginError::NotFound` if the plugin ID does not exist.
    pub fn update_state(&self, id: &PluginId, state: PluginState) -> PluginResult<()> {
        match self.plugins.get_mut(id) {
            Some(mut entry) => {
                entry.value_mut().state = state;
                Ok(())
            }
            None => Err(PluginError::NotFound(format!(
                "plugin '{}' not found",
                id.0
            ))),
        }
    }

    /// Update a plugin's health status with the latest health check response.
    ///
    /// Stores both the `HealthResponse` and the current timestamp.
    /// Returns `PluginError::NotFound` if the plugin ID does not exist.
    pub fn update_health(&self, id: &PluginId, health: HealthResponse) -> PluginResult<()> {
        match self.plugins.get_mut(id) {
            Some(mut entry) => {
                let record = entry.value_mut();
                record.last_health = Some(health);
                record.last_health_at = Some(Utc::now());
                Ok(())
            }
            None => Err(PluginError::NotFound(format!(
                "plugin '{}' not found",
                id.0
            ))),
        }
    }

    /// Attach a plugin service implementation to an existing plugin record.
    ///
    /// This is typically called after registration when the service proxy
    /// or in-process implementation is ready.
    /// Returns `PluginError::NotFound` if the plugin ID does not exist.
    pub fn set_service(&self, id: &PluginId, service: Arc<dyn PluginService>) -> PluginResult<()> {
        match self.plugins.get_mut(id) {
            Some(mut entry) => {
                entry.value_mut().service = Some(service);
                Ok(())
            }
            None => Err(PluginError::NotFound(format!(
                "plugin '{}' not found",
                id.0
            ))),
        }
    }

    /// Get a reference to a plugin's service implementation.
    ///
    /// Returns `None` if the plugin does not exist or has no service attached.
    pub fn get_service(&self, id: &PluginId) -> Option<Arc<dyn PluginService>> {
        self.plugins
            .get(id)
            .and_then(|entry| entry.value().service.clone())
    }

    /// List all registered plugins as `PluginInfo` snapshots.
    pub fn list_plugins(&self) -> Vec<PluginInfo> {
        self.plugins
            .iter()
            .map(|entry| entry.value().to_info())
            .collect()
    }

    /// Return the IDs of all plugins currently in the `Running` state.
    pub fn running_plugins(&self) -> Vec<PluginId> {
        self.plugins
            .iter()
            .filter(|entry| entry.value().state == PluginState::Running)
            .map(|entry| entry.value().id.clone())
            .collect()
    }

    /// Return the total number of registered plugins.
    pub fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    /// Increment a plugin's restart counter and return the new count.
    ///
    /// Returns `PluginError::NotFound` if the plugin ID does not exist.
    pub fn increment_restart_count(&self, id: &PluginId) -> PluginResult<u32> {
        match self.plugins.get_mut(id) {
            Some(mut entry) => {
                let record = entry.value_mut();
                record.restart_count += 1;
                Ok(record.restart_count)
            }
            None => Err(PluginError::NotFound(format!(
                "plugin '{}' not found",
                id.0
            ))),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Helper: build a minimal RegisterRequest.
    fn make_request(name: &str) -> RegisterRequest {
        RegisterRequest {
            name: name.to_string(),
            version: "1.0.0".to_string(),
            traits: vec!["CanStoreState".to_string()],
            endpoints: vec![Endpoint {
                name: "pgwire".to_string(),
                protocol: "tcp".to_string(),
                default_port: 5432,
            }],
        }
    }

    /// Helper: build a RegisterRequest with multiple endpoints and traits.
    fn make_rich_request(name: &str) -> RegisterRequest {
        RegisterRequest {
            name: name.to_string(),
            version: "2.3.1".to_string(),
            traits: vec![
                "CanStoreState".to_string(),
                "CanExecute".to_string(),
                "HasGPU".to_string(),
            ],
            endpoints: vec![
                Endpoint {
                    name: "pgwire".to_string(),
                    protocol: "tcp".to_string(),
                    default_port: 5432,
                },
                Endpoint {
                    name: "http".to_string(),
                    protocol: "tcp".to_string(),
                    default_port: 8080,
                },
            ],
        }
    }

    /// Helper: build a HealthResponse.
    fn make_health(healthy: bool, status: &str) -> HealthResponse {
        let mut details = HashMap::new();
        details.insert("connections".to_string(), "42".to_string());
        HealthResponse {
            healthy,
            status: status.to_string(),
            details,
        }
    }

    /// A minimal PluginService implementation for testing.
    struct MockPluginService {
        name: String,
        version: String,
    }

    impl MockPluginService {
        fn new(name: &str, version: &str) -> Self {
            Self {
                name: name.to_string(),
                version: version.to_string(),
            }
        }
    }

    #[async_trait::async_trait]
    impl PluginService for MockPluginService {
        async fn start(
            &self,
            _request: crate::plugin::types::StartRequest,
        ) -> PluginResult<crate::plugin::types::StartResponse> {
            Ok(crate::plugin::types::StartResponse {
                success: true,
                error: String::new(),
            })
        }

        async fn stop(
            &self,
            _request: crate::plugin::types::StopRequest,
        ) -> PluginResult<crate::plugin::types::StopResponse> {
            Ok(crate::plugin::types::StopResponse { clean: true })
        }

        async fn handle(
            &self,
            _request: crate::plugin::types::HandleRequest,
        ) -> PluginResult<crate::plugin::types::HandleResponse> {
            Ok(crate::plugin::types::HandleResponse {
                payload: Vec::new(),
                error: String::new(),
            })
        }

        async fn health(
            &self,
            _request: crate::plugin::types::HealthRequest,
        ) -> PluginResult<HealthResponse> {
            Ok(make_health(true, "running"))
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn version(&self) -> &str {
            &self.version
        }
    }

    // ---- Construction ----

    #[test]
    fn new_registry_is_empty() {
        let reg = PluginRegistry::new("node-abc".to_string(), 16);
        assert_eq!(reg.plugin_count(), 0);
        assert!(reg.list_plugins().is_empty());
        assert!(reg.running_plugins().is_empty());
    }

    #[test]
    fn with_defaults_uses_max_plugins_constant() {
        let reg = PluginRegistry::with_defaults("node-xyz".to_string());
        assert_eq!(reg.max_plugins, MAX_PLUGINS);
        assert_eq!(reg.plugin_count(), 0);
    }

    // ---- Registration ----

    #[test]
    fn register_plugin_succeeds() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();

        assert!(resp.plugin_id.starts_with("plugin-postgres-"));
        assert_eq!(resp.plugin_id.len(), "plugin-postgres-".len() + 8);
        assert_eq!(resp.node_id, "node-1");
        assert!(resp.swarm_config.is_empty());
        assert_eq!(reg.plugin_count(), 1);
    }

    #[test]
    fn register_plugin_sets_state_to_registered() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("redis")).unwrap();
        let id = PluginId(resp.plugin_id);
        let info = reg.get_plugin(&id).unwrap();
        assert_eq!(info.state, PluginState::Registered);
    }

    #[test]
    fn register_plugin_stores_traits_and_endpoints() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_rich_request("pgdistr")).unwrap();
        let id = PluginId(resp.plugin_id);
        let info = reg.get_plugin(&id).unwrap();

        assert_eq!(info.traits.len(), 3);
        assert!(info.traits.contains(&"HasGPU".to_string()));
        assert_eq!(info.endpoints.len(), 2);
        assert_eq!(info.endpoints[0].name, "pgwire");
        assert_eq!(info.endpoints[1].name, "http");
        assert_eq!(info.version, "2.3.1");
    }

    #[test]
    fn register_plugin_duplicate_name_fails() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        reg.register_plugin(make_request("postgres")).unwrap();

        let result = reg.register_plugin(make_request("postgres"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::AlreadyRegistered(msg) => {
                assert!(msg.contains("postgres"));
            }
            other => panic!("expected AlreadyRegistered, got {:?}", other),
        }
    }

    #[test]
    fn register_plugin_capacity_exceeded() {
        let reg = PluginRegistry::new("node-1".to_string(), 2);
        reg.register_plugin(make_request("alpha")).unwrap();
        reg.register_plugin(make_request("beta")).unwrap();

        let result = reg.register_plugin(make_request("gamma"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::CapacityExceeded(msg) => {
                assert!(msg.contains("2/2"));
            }
            other => panic!("expected CapacityExceeded, got {:?}", other),
        }
    }

    #[test]
    fn register_multiple_plugins() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        reg.register_plugin(make_request("postgres")).unwrap();
        reg.register_plugin(make_request("redis")).unwrap();
        reg.register_plugin(make_request("custom")).unwrap();

        assert_eq!(reg.plugin_count(), 3);
        assert_eq!(reg.list_plugins().len(), 3);
    }

    #[test]
    fn register_plugin_has_no_health_initially() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id);
        let info = reg.get_plugin(&id).unwrap();
        assert!(info.last_health_status.is_none());
        assert_eq!(info.restart_count, 0);
    }

    // ---- Unregistration ----

    #[test]
    fn unregister_plugin_succeeds() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id);

        assert_eq!(reg.plugin_count(), 1);
        reg.unregister_plugin(&id).unwrap();
        assert_eq!(reg.plugin_count(), 0);
    }

    #[test]
    fn unregister_plugin_removes_name_index() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id);

        assert!(reg.get_plugin_by_name("postgres").is_some());
        reg.unregister_plugin(&id).unwrap();
        assert!(reg.get_plugin_by_name("postgres").is_none());
    }

    #[test]
    fn unregister_allows_re_registration() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp1 = reg.register_plugin(make_request("postgres")).unwrap();
        let id1 = PluginId(resp1.plugin_id.clone());
        reg.unregister_plugin(&id1).unwrap();

        let resp2 = reg.register_plugin(make_request("postgres")).unwrap();
        assert_ne!(resp1.plugin_id, resp2.plugin_id);
        assert_eq!(reg.plugin_count(), 1);
    }

    #[test]
    fn unregister_nonexistent_plugin_fails() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let fake_id = PluginId("plugin-ghost-12345678".to_string());
        let result = reg.unregister_plugin(&fake_id);
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::NotFound(msg) => {
                assert!(msg.contains("ghost"));
            }
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    // ---- Lookup ----

    #[test]
    fn get_plugin_returns_info() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("redis")).unwrap();
        let id = PluginId(resp.plugin_id.clone());

        let info = reg.get_plugin(&id).unwrap();
        assert_eq!(info.id.0, resp.plugin_id);
        assert_eq!(info.name, "redis");
        assert_eq!(info.version, "1.0.0");
        assert_eq!(info.state, PluginState::Registered);
    }

    #[test]
    fn get_plugin_nonexistent_returns_none() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let fake_id = PluginId("plugin-nope-00000000".to_string());
        assert!(reg.get_plugin(&fake_id).is_none());
    }

    #[test]
    fn get_plugin_by_name_works() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();

        let found_id = reg.get_plugin_by_name("postgres").unwrap();
        assert_eq!(found_id.0, resp.plugin_id);
    }

    #[test]
    fn get_plugin_by_name_nonexistent_returns_none() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        assert!(reg.get_plugin_by_name("nonexistent").is_none());
    }

    // ---- State updates ----

    #[test]
    fn update_state_succeeds() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id);

        reg.update_state(&id, PluginState::Starting).unwrap();
        assert_eq!(reg.get_plugin(&id).unwrap().state, PluginState::Starting);

        reg.update_state(&id, PluginState::Running).unwrap();
        assert_eq!(reg.get_plugin(&id).unwrap().state, PluginState::Running);

        reg.update_state(&id, PluginState::Stopping).unwrap();
        assert_eq!(reg.get_plugin(&id).unwrap().state, PluginState::Stopping);

        reg.update_state(&id, PluginState::Stopped).unwrap();
        assert_eq!(reg.get_plugin(&id).unwrap().state, PluginState::Stopped);
    }

    #[test]
    fn update_state_nonexistent_fails() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let fake_id = PluginId("plugin-ghost-12345678".to_string());
        let result = reg.update_state(&fake_id, PluginState::Running);
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::NotFound(_) => {}
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn update_state_to_failed() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("flaky")).unwrap();
        let id = PluginId(resp.plugin_id);

        reg.update_state(&id, PluginState::Running).unwrap();
        reg.update_state(&id, PluginState::Failed).unwrap();
        assert_eq!(reg.get_plugin(&id).unwrap().state, PluginState::Failed);
    }

    // ---- Health updates ----

    #[test]
    fn update_health_succeeds() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id);

        let health = make_health(true, "running");
        reg.update_health(&id, health).unwrap();

        let info = reg.get_plugin(&id).unwrap();
        assert_eq!(info.last_health_status, Some("running".to_string()));
    }

    #[test]
    fn update_health_overwrites_previous() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id);

        reg.update_health(&id, make_health(true, "running")).unwrap();
        reg.update_health(&id, make_health(false, "degraded")).unwrap();

        let info = reg.get_plugin(&id).unwrap();
        assert_eq!(info.last_health_status, Some("degraded".to_string()));
    }

    #[test]
    fn update_health_nonexistent_fails() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let fake_id = PluginId("plugin-ghost-12345678".to_string());
        let result = reg.update_health(&fake_id, make_health(true, "ok"));
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::NotFound(_) => {}
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn update_health_sets_timestamp() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("pg")).unwrap();
        let id = PluginId(resp.plugin_id);

        let before = Utc::now();
        reg.update_health(&id, make_health(true, "ok")).unwrap();
        let after = Utc::now();

        // Verify the health timestamp is set by checking the internal record.
        let entry = reg.plugins.get(&id).unwrap();
        let ts = entry.value().last_health_at.unwrap();
        assert!(ts >= before && ts <= after);
    }

    // ---- Service management ----

    #[test]
    fn set_and_get_service() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id);

        // Initially no service
        assert!(reg.get_service(&id).is_none());

        let svc: Arc<dyn PluginService> =
            Arc::new(MockPluginService::new("postgres", "1.0.0"));
        reg.set_service(&id, svc.clone()).unwrap();

        let retrieved = reg.get_service(&id).unwrap();
        assert_eq!(retrieved.name(), "postgres");
        assert_eq!(retrieved.version(), "1.0.0");
    }

    #[test]
    fn set_service_nonexistent_fails() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let fake_id = PluginId("plugin-ghost-12345678".to_string());
        let svc: Arc<dyn PluginService> = Arc::new(MockPluginService::new("test", "0.1.0"));
        let result = reg.set_service(&fake_id, svc);
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::NotFound(_) => {}
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn get_service_nonexistent_returns_none() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let fake_id = PluginId("plugin-ghost-12345678".to_string());
        assert!(reg.get_service(&fake_id).is_none());
    }

    #[test]
    fn set_service_replaces_previous() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("db")).unwrap();
        let id = PluginId(resp.plugin_id);

        let svc1: Arc<dyn PluginService> = Arc::new(MockPluginService::new("db", "1.0.0"));
        reg.set_service(&id, svc1).unwrap();
        assert_eq!(reg.get_service(&id).unwrap().version(), "1.0.0");

        let svc2: Arc<dyn PluginService> = Arc::new(MockPluginService::new("db", "2.0.0"));
        reg.set_service(&id, svc2).unwrap();
        assert_eq!(reg.get_service(&id).unwrap().version(), "2.0.0");
    }

    // ---- Listing and filtering ----

    #[test]
    fn list_plugins_returns_all() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        reg.register_plugin(make_request("alpha")).unwrap();
        reg.register_plugin(make_request("beta")).unwrap();
        reg.register_plugin(make_request("gamma")).unwrap();

        let list = reg.list_plugins();
        assert_eq!(list.len(), 3);

        let names: Vec<String> = list.iter().map(|p| p.name.clone()).collect();
        assert!(names.contains(&"alpha".to_string()));
        assert!(names.contains(&"beta".to_string()));
        assert!(names.contains(&"gamma".to_string()));
    }

    #[test]
    fn running_plugins_filters_by_state() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let r1 = reg.register_plugin(make_request("runner1")).unwrap();
        let r2 = reg.register_plugin(make_request("runner2")).unwrap();
        let _r3 = reg.register_plugin(make_request("stopped1")).unwrap();

        let id1 = PluginId(r1.plugin_id);
        let id2 = PluginId(r2.plugin_id);

        reg.update_state(&id1, PluginState::Running).unwrap();
        reg.update_state(&id2, PluginState::Running).unwrap();
        // stopped1 remains Registered

        let running = reg.running_plugins();
        assert_eq!(running.len(), 2);

        let running_strs: Vec<String> = running.iter().map(|id| id.0.clone()).collect();
        assert!(running_strs.contains(&id1.0));
        assert!(running_strs.contains(&id2.0));
    }

    #[test]
    fn running_plugins_empty_when_none_running() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        reg.register_plugin(make_request("a")).unwrap();
        reg.register_plugin(make_request("b")).unwrap();

        assert!(reg.running_plugins().is_empty());
    }

    // ---- Restart count ----

    #[test]
    fn increment_restart_count_works() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("flaky")).unwrap();
        let id = PluginId(resp.plugin_id);

        assert_eq!(reg.get_plugin(&id).unwrap().restart_count, 0);

        let count1 = reg.increment_restart_count(&id).unwrap();
        assert_eq!(count1, 1);
        assert_eq!(reg.get_plugin(&id).unwrap().restart_count, 1);

        let count2 = reg.increment_restart_count(&id).unwrap();
        assert_eq!(count2, 2);

        let count3 = reg.increment_restart_count(&id).unwrap();
        assert_eq!(count3, 3);
        assert_eq!(reg.get_plugin(&id).unwrap().restart_count, 3);
    }

    #[test]
    fn increment_restart_count_nonexistent_fails() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let fake_id = PluginId("plugin-ghost-12345678".to_string());
        let result = reg.increment_restart_count(&fake_id);
        assert!(result.is_err());
        match result.unwrap_err() {
            PluginError::NotFound(_) => {}
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    // ---- Plugin count ----

    #[test]
    fn plugin_count_tracks_additions_and_removals() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        assert_eq!(reg.plugin_count(), 0);

        let r1 = reg.register_plugin(make_request("a")).unwrap();
        assert_eq!(reg.plugin_count(), 1);

        let r2 = reg.register_plugin(make_request("b")).unwrap();
        assert_eq!(reg.plugin_count(), 2);

        reg.unregister_plugin(&PluginId(r1.plugin_id)).unwrap();
        assert_eq!(reg.plugin_count(), 1);

        reg.unregister_plugin(&PluginId(r2.plugin_id)).unwrap();
        assert_eq!(reg.plugin_count(), 0);
    }

    // ---- Edge cases ----

    #[test]
    fn register_plugin_id_format_is_deterministic_prefix() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let resp = reg.register_plugin(make_request("my-plugin")).unwrap();
        assert!(resp.plugin_id.starts_with("plugin-my-plugin-"));
        // 8 hex chars from the short UUID
        let suffix = &resp.plugin_id["plugin-my-plugin-".len()..];
        assert_eq!(suffix.len(), 8);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
    }

    #[test]
    fn unique_ids_for_different_registrations() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let r1 = reg.register_plugin(make_request("a")).unwrap();
        let r2 = reg.register_plugin(make_request("b")).unwrap();
        assert_ne!(r1.plugin_id, r2.plugin_id);
    }

    #[test]
    fn capacity_exactly_at_limit() {
        let reg = PluginRegistry::new("node-1".to_string(), 1);
        reg.register_plugin(make_request("only")).unwrap();
        assert_eq!(reg.plugin_count(), 1);

        let result = reg.register_plugin(make_request("overflow"));
        assert!(result.is_err());
    }

    #[test]
    fn unregister_frees_capacity() {
        let reg = PluginRegistry::new("node-1".to_string(), 1);
        let resp = reg.register_plugin(make_request("first")).unwrap();
        let id = PluginId(resp.plugin_id);

        assert!(reg.register_plugin(make_request("second")).is_err());

        reg.unregister_plugin(&id).unwrap();
        reg.register_plugin(make_request("second")).unwrap();
        assert_eq!(reg.plugin_count(), 1);
    }

    #[test]
    fn list_plugins_after_state_changes() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);
        let r1 = reg.register_plugin(make_request("a")).unwrap();
        let r2 = reg.register_plugin(make_request("b")).unwrap();

        reg.update_state(&PluginId(r1.plugin_id.clone()), PluginState::Running)
            .unwrap();
        reg.update_state(&PluginId(r2.plugin_id.clone()), PluginState::Failed)
            .unwrap();

        let list = reg.list_plugins();
        for info in &list {
            if info.name == "a" {
                assert_eq!(info.state, PluginState::Running);
            } else if info.name == "b" {
                assert_eq!(info.state, PluginState::Failed);
            }
        }
    }

    #[test]
    fn full_lifecycle_integration() {
        let reg = PluginRegistry::new("node-abc".to_string(), 64);

        // 1. Register
        let resp = reg.register_plugin(make_rich_request("postgres")).unwrap();
        let id = PluginId(resp.plugin_id.clone());
        assert_eq!(resp.node_id, "node-abc");
        assert_eq!(reg.plugin_count(), 1);

        // 2. Attach service
        let svc: Arc<dyn PluginService> =
            Arc::new(MockPluginService::new("postgres", "2.3.1"));
        reg.set_service(&id, svc).unwrap();
        assert!(reg.get_service(&id).is_some());

        // 3. Start
        reg.update_state(&id, PluginState::Starting).unwrap();
        assert_eq!(reg.get_plugin(&id).unwrap().state, PluginState::Starting);

        // 4. Running
        reg.update_state(&id, PluginState::Running).unwrap();
        assert_eq!(reg.running_plugins().len(), 1);

        // 5. Health checks
        reg.update_health(&id, make_health(true, "running")).unwrap();
        assert_eq!(
            reg.get_plugin(&id).unwrap().last_health_status,
            Some("running".to_string())
        );

        // 6. Failure and restart
        reg.update_state(&id, PluginState::Failed).unwrap();
        assert!(reg.running_plugins().is_empty());
        let count = reg.increment_restart_count(&id).unwrap();
        assert_eq!(count, 1);

        // 7. Restart cycle
        reg.update_state(&id, PluginState::Starting).unwrap();
        reg.update_state(&id, PluginState::Running).unwrap();
        reg.update_health(&id, make_health(true, "recovered")).unwrap();
        assert_eq!(reg.running_plugins().len(), 1);
        assert_eq!(
            reg.get_plugin(&id).unwrap().last_health_status,
            Some("recovered".to_string())
        );

        // 8. Clean shutdown
        reg.update_state(&id, PluginState::Stopping).unwrap();
        reg.update_state(&id, PluginState::Stopped).unwrap();
        assert!(reg.running_plugins().is_empty());

        // 9. Unregister
        reg.unregister_plugin(&id).unwrap();
        assert_eq!(reg.plugin_count(), 0);
        assert!(reg.get_plugin(&id).is_none());
        assert!(reg.get_plugin_by_name("postgres").is_none());
    }

    #[test]
    fn concurrent_safe_multiple_plugins() {
        let reg = PluginRegistry::new("node-1".to_string(), 64);

        // Register several plugins and exercise various operations
        let mut ids = Vec::new();
        for i in 0..10 {
            let name = format!("plugin-{}", i);
            let resp = reg.register_plugin(make_request(&name)).unwrap();
            ids.push(PluginId(resp.plugin_id));
        }
        assert_eq!(reg.plugin_count(), 10);

        // Set half to running
        for id in &ids[..5] {
            reg.update_state(id, PluginState::Running).unwrap();
        }
        assert_eq!(reg.running_plugins().len(), 5);

        // Health-check all running
        for id in &ids[..5] {
            reg.update_health(id, make_health(true, "healthy")).unwrap();
        }

        // Increment restarts on a few
        for id in &ids[5..8] {
            reg.increment_restart_count(id).unwrap();
        }

        // Unregister some
        for id in &ids[7..] {
            reg.unregister_plugin(id).unwrap();
        }
        assert_eq!(reg.plugin_count(), 7);
        assert_eq!(reg.running_plugins().len(), 5);

        let list = reg.list_plugins();
        assert_eq!(list.len(), 7);
    }
}
