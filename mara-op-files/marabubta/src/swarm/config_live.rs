// Marabunta - Licensed under the MIT License.
//! LiveConfig — lock-free hot-reloadable configuration.
//!
//! Uses [`ArcSwap`] for lock-free reads and a broadcast channel for change
//! notifications. Runtime patches (tier 3) are validated against the
//! [`ConfigRegistry`] and optionally persisted to PostgreSQL.

use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{info, warn};

use super::config::SwarmConfig;
use super::config_meta::{
    ComplianceOverride, ConfigRegistry, ConfigTier,
};

// ============================================================================
// Change event
// ============================================================================

/// Describes a single config key that was changed via a runtime patch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChange {
    pub key: String,
    pub old_value: serde_json::Value,
    pub new_value: serde_json::Value,
}

/// Event emitted after a successful config patch.
#[derive(Debug, Clone)]
pub struct ConfigChangeEvent {
    /// Who applied the patch (e.g. "admin", "api", "startup").
    pub source: String,
    /// Individual key changes.
    pub changes: Vec<ConfigChange>,
    /// Snapshot of the new config (cheap Arc clone).
    pub new_config: Arc<SwarmConfig>,
}

// ============================================================================
// Patch error
// ============================================================================

/// Errors from applying a config patch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ConfigPatchError {
    /// A setting is locked by a compliance profile.
    HardwiredByCompliance {
        key: String,
        profile_name: String,
        justification: String,
    },
    /// A setting requires a restart to change.
    RestartRequired { key: String },
    /// Value failed validation.
    ValidationError { key: String, message: String },
    /// Unknown config key.
    UnknownKey { key: String },
    /// No changes in the patch (all values identical to current).
    NoChanges,
}

impl std::fmt::Display for ConfigPatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HardwiredByCompliance { key, profile_name, justification } =>
                write!(f, "{}: locked by {} — {}", key, profile_name, justification),
            Self::RestartRequired { key } =>
                write!(f, "{}: requires restart to change", key),
            Self::ValidationError { key, message } =>
                write!(f, "{}: {}", key, message),
            Self::UnknownKey { key } =>
                write!(f, "unknown config key: {}", key),
            Self::NoChanges =>
                write!(f, "no changes in patch"),
        }
    }
}

impl std::error::Error for ConfigPatchError {}

// ============================================================================
// LiveConfig
// ============================================================================

/// Lock-free hot-reloadable configuration.
///
/// Reads are lock-free via [`ArcSwap`]. Runtime patches are validated,
/// applied atomically, and broadcast to subscribers.
pub struct LiveConfig {
    /// The current configuration, atomically swappable.
    config: ArcSwap<SwarmConfig>,
    /// Metadata registry for validation and tier checking.
    registry: ConfigRegistry,
    /// Broadcast channel for notifying subscribers of changes.
    change_tx: broadcast::Sender<ConfigChangeEvent>,
}

impl LiveConfig {
    /// Create a new LiveConfig with the given initial configuration.
    pub fn new(
        initial: SwarmConfig,
        compliance_overrides: Vec<ComplianceOverride>,
    ) -> Arc<Self> {
        let registry = ConfigRegistry::new(compliance_overrides);
        let (change_tx, _) = broadcast::channel(64);
        Arc::new(Self {
            config: ArcSwap::new(Arc::new(initial)),
            registry,
            change_tx,
        })
    }

    /// Cheap lock-free snapshot of the current config.
    pub fn load(&self) -> Guard<Arc<SwarmConfig>> {
        self.config.load()
    }

    /// Get a cloned Arc of the current config.
    pub fn load_full(&self) -> Arc<SwarmConfig> {
        self.config.load_full()
    }

    /// Subscribe to config change notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<ConfigChangeEvent> {
        self.change_tx.subscribe()
    }

    /// Access the config registry.
    pub fn registry(&self) -> &ConfigRegistry {
        &self.registry
    }

    /// Apply a patch of key-value pairs to the running config.
    ///
    /// Each key is validated against the registry:
    /// 1. Compliance locks are checked (hardwired → reject).
    /// 2. Tier is checked (startup → requires restart → reject).
    /// 3. Value type and constraints are validated.
    /// 4. Config is cloned, patched, and atomically swapped.
    /// 5. Change event is broadcast.
    ///
    /// Returns the list of keys that were actually changed.
    pub fn apply_patch(
        &self,
        patch: &std::collections::HashMap<String, serde_json::Value>,
        source: &str,
    ) -> Result<Vec<String>, ConfigPatchError> {
        if patch.is_empty() {
            return Err(ConfigPatchError::NoChanges);
        }

        // Phase 1: Validate all keys before applying anything.
        for (key, value) in patch {
            // Check compliance locks.
            if let Some(ovr) = self.registry.is_hardwired(key) {
                return Err(ConfigPatchError::HardwiredByCompliance {
                    key: key.clone(),
                    profile_name: ovr.profile_name.clone(),
                    justification: ovr.justification.clone(),
                });
            }

            // Check tier.
            let meta = self.registry.get(key).ok_or_else(|| ConfigPatchError::UnknownKey {
                key: key.clone(),
            })?;

            if meta.tier == ConfigTier::Startup || meta.restart_required {
                return Err(ConfigPatchError::RestartRequired {
                    key: key.clone(),
                });
            }

            // Validate value.
            if let Err(e) = self.registry.validate(key, value) {
                return Err(ConfigPatchError::ValidationError {
                    key: key.clone(),
                    message: e.to_string(),
                });
            }
        }

        // Phase 2: Clone current config and apply changes.
        let current = self.config.load_full();
        let mut new_config = (*current).clone();
        let mut changes = Vec::new();
        let current_json = serde_json::to_value(&*current).unwrap_or_default();

        for (key, new_value) in patch {
            // Get old value for the change record.
            let old_value = get_nested_value(&current_json, key)
                .cloned()
                .unwrap_or(serde_json::Value::Null);

            // Skip if value is unchanged.
            if old_value == *new_value {
                continue;
            }

            // Apply the change to the config struct.
            apply_value_to_config(&mut new_config, key, new_value);

            changes.push(ConfigChange {
                key: key.clone(),
                old_value,
                new_value: new_value.clone(),
            });
        }

        if changes.is_empty() {
            return Err(ConfigPatchError::NoChanges);
        }

        // Phase 3: Atomically swap in the new config.
        let new_arc = Arc::new(new_config);
        self.config.store(Arc::clone(&new_arc));

        let changed_keys: Vec<String> = changes.iter().map(|c| c.key.clone()).collect();
        info!(
            source = source,
            keys = ?changed_keys,
            "Config patched: {} keys changed",
            changed_keys.len()
        );

        // Phase 4: Broadcast change event (ignore send failures — no subscribers is OK).
        let _ = self.change_tx.send(ConfigChangeEvent {
            source: source.to_string(),
            changes,
            new_config: new_arc,
        });

        Ok(changed_keys)
    }

    /// Replace the entire config atomically (e.g. on startup from TOML).
    pub fn store(&self, config: SwarmConfig) {
        self.config.store(Arc::new(config));
    }

    /// Number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.change_tx.receiver_count()
    }
}

// ============================================================================
// Config value helpers
// ============================================================================

/// Get a value from a serialized SwarmConfig JSON by dotted registry key.
///
/// Public so the config API handler can use it for value extraction.
///
/// Registry keys use dotted format (e.g. "gossip.fanout") but SwarmConfig
/// fields may be flat (e.g. "gossip_fanout") or nested in sub-config structs
/// (e.g. "gossip_tuning.jitter_percent"). This tries both conventions.
pub fn get_nested_value<'a>(json: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    let parts: Vec<&str> = key.split('.').collect();
    if parts.len() == 2 {
        let obj = json.as_object()?;
        // Try flat field first: "gossip.fanout" → "gossip_fanout"
        let flat_key = format!("{}_{}", parts[0], parts[1]);
        if let Some(v) = obj.get(&flat_key) {
            return Some(v);
        }
        // Try nested: "gossip_tuning.jitter_percent"
        if let Some(nested) = obj.get(parts[0]) {
            if let Some(v) = nested.get(parts[1]) {
                return Some(v);
            }
        }
        // Try bare field name: "work.max_load" → "max_load"
        if let Some(v) = obj.get(parts[1]) {
            return Some(v);
        }
        None
    } else if parts.len() == 1 {
        json.get(parts[0])
    } else {
        // Deep nesting: walk the path
        let mut current = json;
        for part in &parts {
            current = current.get(part)?;
        }
        Some(current)
    }
}

/// Apply a value to a SwarmConfig by dotted key path.
///
/// This uses a serialize-modify-deserialize roundtrip to handle arbitrary keys.
/// Not the fastest, but correct and maintainable.
fn apply_value_to_config(
    config: &mut SwarmConfig,
    key: &str,
    value: &serde_json::Value,
) {
    let mut json = match serde_json::to_value(&*config) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to serialize config for patching: {}", e);
            return;
        }
    };

    // Map dotted registry key to SwarmConfig JSON field(s).
    // The registry uses "gossip.fanout" but SwarmConfig has "gossip_fanout" (flat).
    // Sub-configs use "gossip_tuning.jitter_percent" → nested {"gossip_tuning":{"jitter_percent":...}}.
    let parts: Vec<&str> = key.split('.').collect();
    if parts.len() == 2 {
        if let Some(obj) = json.as_object_mut() {
            // Try flat field first: "gossip.fanout" → "gossip_fanout"
            let flat_key = format!("{}_{}", parts[0], parts[1]);
            if obj.contains_key(&flat_key) {
                obj.insert(flat_key, value.clone());
            }
            // Also try nested: "gossip_tuning.jitter_percent" or "admission.jury_min_size"
            else if let Some(nested) = obj.get_mut(parts[0]) {
                if let Some(nested_obj) = nested.as_object_mut() {
                    nested_obj.insert(parts[1].to_string(), value.clone());
                }
            }
            // Some SwarmConfig fields don't follow the prefix pattern:
            // e.g. "work.max_load" → "max_load", "work.chunk_timeout" → "chunk_timeout"
            else if obj.contains_key(parts[1]) {
                obj.insert(parts[1].to_string(), value.clone());
            } else {
                warn!("Config key path not found in SwarmConfig JSON: {}", key);
            }
        }
    } else if parts.len() == 1 {
        if let Some(obj) = json.as_object_mut() {
            obj.insert(parts[0].to_string(), value.clone());
        }
    } else {
        warn!("Config key nesting depth > 2 not supported: {}", key);
    }

    // Deserialize back.
    match serde_json::from_value::<SwarmConfig>(json) {
        Ok(new) => *config = new,
        Err(e) => {
            warn!("Failed to deserialize patched config: {}", e);
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

    fn live_config() -> Arc<LiveConfig> {
        LiveConfig::new(SwarmConfig::default(), vec![])
    }

    fn live_config_with_overrides(overrides: Vec<ComplianceOverride>) -> Arc<LiveConfig> {
        LiveConfig::new(SwarmConfig::default(), overrides)
    }

    #[test]
    fn test_load_returns_default() {
        let lc = live_config();
        let cfg = lc.load();
        assert_eq!(cfg.gossip_fanout, 3);
    }

    #[test]
    fn test_load_full_returns_arc() {
        let lc = live_config();
        let arc = lc.load_full();
        assert_eq!(arc.gossip_fanout, 3);
    }

    #[test]
    fn test_store_replaces_config() {
        let lc = live_config();
        let mut cfg = SwarmConfig::default();
        cfg.gossip_fanout = 7;
        lc.store(cfg);
        assert_eq!(lc.load().gossip_fanout, 7);
    }

    #[test]
    fn test_apply_patch_runtime_field() {
        let lc = live_config();
        let mut patch = HashMap::new();
        patch.insert("gossip.fanout".to_string(), serde_json::json!(5));
        let keys = lc.apply_patch(&patch, "test").unwrap();
        assert_eq!(keys, vec!["gossip.fanout"]);
        assert_eq!(lc.load().gossip_fanout, 5);
    }

    #[test]
    fn test_apply_patch_rejects_startup_field() {
        let lc = live_config();
        let mut patch = HashMap::new();
        patch.insert("transport.listen_backlog".to_string(), serde_json::json!(1024));
        let err = lc.apply_patch(&patch, "test").unwrap_err();
        assert!(matches!(err, ConfigPatchError::RestartRequired { .. }));
    }

    #[test]
    fn test_apply_patch_rejects_hardwired() {
        let overrides = vec![ComplianceOverride {
            key: "gossip.fanout".to_string(),
            value: serde_json::json!(3),
            ui_visibility: super::super::config_meta::UiVisibility::VisibleReadonly,
            constraint_type: "exact".to_string(),
            justification: "locked for testing".to_string(),
            profile_name: "test-profile".to_string(),
        }];
        let lc = live_config_with_overrides(overrides);
        let mut patch = HashMap::new();
        patch.insert("gossip.fanout".to_string(), serde_json::json!(5));
        let err = lc.apply_patch(&patch, "test").unwrap_err();
        match err {
            ConfigPatchError::HardwiredByCompliance { profile_name, .. } => {
                assert_eq!(profile_name, "test-profile");
            }
            other => panic!("expected HardwiredByCompliance, got {:?}", other),
        }
    }

    #[test]
    fn test_apply_patch_rejects_unknown_key() {
        let lc = live_config();
        let mut patch = HashMap::new();
        patch.insert("nonexistent.key".to_string(), serde_json::json!(42));
        let err = lc.apply_patch(&patch, "test").unwrap_err();
        assert!(matches!(err, ConfigPatchError::UnknownKey { .. }));
    }

    #[test]
    fn test_apply_patch_validates_range() {
        let lc = live_config();
        let mut patch = HashMap::new();
        // gossip.fanout max is 20
        patch.insert("gossip.fanout".to_string(), serde_json::json!(100));
        let err = lc.apply_patch(&patch, "test").unwrap_err();
        assert!(matches!(err, ConfigPatchError::ValidationError { .. }));
    }

    #[test]
    fn test_apply_patch_no_changes() {
        let lc = live_config();
        let mut patch = HashMap::new();
        // Same as default
        patch.insert("gossip.fanout".to_string(), serde_json::json!(3));
        let err = lc.apply_patch(&patch, "test").unwrap_err();
        assert!(matches!(err, ConfigPatchError::NoChanges));
    }

    #[test]
    fn test_apply_patch_empty() {
        let lc = live_config();
        let patch = HashMap::new();
        let err = lc.apply_patch(&patch, "test").unwrap_err();
        assert!(matches!(err, ConfigPatchError::NoChanges));
    }

    #[test]
    fn test_subscribe_receives_events() {
        let lc = live_config();
        let mut rx = lc.subscribe();

        let mut patch = HashMap::new();
        patch.insert("gossip.fanout".to_string(), serde_json::json!(10));
        lc.apply_patch(&patch, "test").unwrap();

        let event = rx.try_recv().unwrap();
        assert_eq!(event.source, "test");
        assert_eq!(event.changes.len(), 1);
        assert_eq!(event.changes[0].key, "gossip.fanout");
        assert_eq!(event.changes[0].new_value, serde_json::json!(10));
    }

    #[test]
    fn test_subscriber_count() {
        let lc = live_config();
        assert_eq!(lc.subscriber_count(), 0);
        let _rx1 = lc.subscribe();
        assert_eq!(lc.subscriber_count(), 1);
        let _rx2 = lc.subscribe();
        assert_eq!(lc.subscriber_count(), 2);
    }

    #[test]
    fn test_registry_accessible() {
        let lc = live_config();
        assert!(lc.registry().len() > 100);
    }

    #[test]
    fn test_multiple_keys_in_patch() {
        let lc = live_config();
        let mut patch = HashMap::new();
        patch.insert("gossip.fanout".to_string(), serde_json::json!(7));
        patch.insert("work.max_load".to_string(), serde_json::json!(0.9));
        let keys = lc.apply_patch(&patch, "test").unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(lc.load().gossip_fanout, 7);
    }

    #[test]
    fn test_apply_patch_float_field() {
        let lc = live_config();
        let mut patch = HashMap::new();
        patch.insert("work.max_load".to_string(), serde_json::json!(0.6));
        let keys = lc.apply_patch(&patch, "test").unwrap();
        assert_eq!(keys, vec!["work.max_load"]);
        let load = lc.load().max_load;
        assert!((load - 0.6).abs() < 0.001);
    }

    #[test]
    fn test_config_patch_error_display() {
        let err = ConfigPatchError::UnknownKey { key: "foo".into() };
        assert!(err.to_string().contains("foo"));
    }

    #[test]
    fn test_get_nested_value() {
        let json = serde_json::json!({"gossip": {"fanout": 3}});
        let v = get_nested_value(&json, "gossip.fanout").unwrap();
        assert_eq!(v, &serde_json::json!(3));
    }

    #[test]
    fn test_get_nested_value_missing() {
        let json = serde_json::json!({"gossip": {"fanout": 3}});
        assert!(get_nested_value(&json, "gossip.missing").is_none());
    }

    #[test]
    fn test_get_nested_value_top_level() {
        let json = serde_json::json!({"gossip_fanout": 3});
        let v = get_nested_value(&json, "gossip_fanout").unwrap();
        assert_eq!(v, &serde_json::json!(3));
    }
}
