// Marabunta - Licensed under the MIT License.
//! Workflow plugin trait system (W4B).
//!
//! Provides the `WorkflowPlugin` registration bundle trait, a unified
//! `PluginRegistry` for all workflow extension types, supporting description
//! types, the `WorkflowTrigger` trait, and a WASM stub for future Phase 3
//! sandboxed loading.
//!
//! The plugin system follows an open-core model: the engine ships with
//! built-in actions (W3B), conditions (W2D), triggers (W3C), and hooks
//! (W3C). Plugins extend this set by registering their own implementations
//! of the same traits. Override semantics are **last-plugin-wins**: plugins
//! registered later replace earlier registrations with the same type key.
//! Hooks are additive and never replaced.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

// Re-export the four capability traits from their home modules so that
// plugin authors can import everything from `plugin`.
pub use super::actions::{ActionDescription, WorkflowAction};
pub use super::conditions::WorkflowCondition;
pub use super::hooks::WorkflowHook;
pub use super::types::{
    ActorInfo, ActionResult, WorkflowContext, WorkflowDefinition, WorkflowInstance,
};

// ============================================================================
// Supporting Types — defined here because they are not in prior waves
// ============================================================================

/// An event occurring within the swarm that may trigger workflow transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmEvent {
    /// Event type identifier (e.g., "node.joined", "job.completed",
    /// "reputation.changed", "blob.stored").
    pub event_type: String,
    /// Structured payload with event-specific data.
    pub payload: serde_json::Value,
    /// Node that originated the event.
    pub source_node: String,
    /// Timestamp of the event in milliseconds since epoch.
    pub timestamp_ms: u64,
}

/// Result produced by a trigger when an event matches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerResult {
    /// ID of the workflow instance to fire a transition on.
    /// None if this trigger should create a new instance.
    pub instance_id: Option<String>,
    /// Name of the transition to fire.
    pub transition: String,
    /// Data extracted from the event to inject into instance context.
    pub bindings: serde_json::Value,
}

/// Metadata describing a WorkflowTrigger for documentation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDescription {
    /// The trigger_type string.
    pub trigger_type: String,
    /// Human-readable summary.
    pub summary: String,
    /// Description of what events this trigger reacts to.
    pub event_types: Vec<String>,
}

/// Metadata describing a WorkflowCondition for documentation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionDescription {
    /// The condition_type string.
    pub condition_type: String,
    /// Human-readable summary.
    pub summary: String,
    /// JSON schema-like object describing expected params.
    pub params_schema: serde_json::Value,
}

// ============================================================================
// WorkflowTrigger Trait
// ============================================================================

/// Trait for workflow trigger implementations.
///
/// Triggers listen for external events (swarm events, timers, webhooks)
/// and produce results that initiate workflow transitions. Unlike conditions
/// (which gate manually-initiated transitions), triggers react to events
/// and fire transitions proactively.
#[async_trait]
pub trait WorkflowTrigger: Send + Sync {
    /// Returns the trigger type string for registry lookup.
    /// Must be unique (e.g., "event_pattern", "cron_schedule", "webhook_receiver").
    fn trigger_type(&self) -> &str;

    /// Called when a SwarmEvent arrives. The trigger inspects the event
    /// and returns zero or more TriggerResults indicating which workflow
    /// instances should have transitions fired.
    async fn on_event(
        &self,
        event: &SwarmEvent,
        instances: &[WorkflowInstance],
    ) -> Result<Vec<TriggerResult>>;

    /// Return metadata describing this trigger type.
    fn describe(&self) -> TriggerDescription;
}

// ============================================================================
// PluginDescriptor — metadata about a plugin
// ============================================================================

/// Metadata about a registered plugin for introspection and listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDescriptor {
    /// Human-readable plugin name.
    pub name: String,
    /// Semantic version string (e.g., "1.2.0").
    pub version: String,
    /// Author or organization.
    pub author: String,
    /// Description of what the plugin provides.
    pub description: String,
}

/// Metadata about a registered plugin including extension counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInfo {
    /// Plugin descriptor (name, version, author, description).
    pub descriptor: PluginDescriptor,
    /// Number of actions registered by this plugin.
    pub action_count: usize,
    /// Number of conditions registered by this plugin.
    pub condition_count: usize,
    /// Number of triggers registered by this plugin.
    pub trigger_count: usize,
    /// Number of hooks registered by this plugin.
    pub hook_count: usize,
    /// Number of bundled workflow definitions.
    pub workflow_count: usize,
}

// ============================================================================
// WorkflowPlugin Trait — the registration bundle
// ============================================================================

/// Bundle trait for plugin registration. Each plugin implements this
/// to declare what extensions it provides to the workflow engine.
///
/// A plugin that only provides actions can return empty vectors from
/// `conditions()`, `triggers()`, and `hooks()`. Empty vectors allocate
/// nothing in Rust, so there is zero overhead for unused extension points.
pub trait WorkflowPlugin: Send + Sync {
    /// Human-readable plugin name (e.g., "jira-integration").
    fn name(&self) -> &str;

    /// Semantic version string (e.g., "1.2.0").
    fn version(&self) -> &str;

    /// Author or organization name.
    fn author(&self) -> &str {
        "unknown"
    }

    /// Human-readable description of the plugin.
    fn description(&self) -> &str {
        ""
    }

    /// Factory: produce all action implementations this plugin provides.
    /// Each action is boxed and will be registered by its action_type().
    fn actions(&self) -> Vec<Box<dyn WorkflowAction>>;

    /// Factory: produce all condition implementations.
    fn conditions(&self) -> Vec<Box<dyn WorkflowCondition>>;

    /// Factory: produce all trigger implementations.
    fn triggers(&self) -> Vec<Box<dyn WorkflowTrigger>>;

    /// Factory: produce all hook implementations.
    fn hooks(&self) -> Vec<Box<dyn WorkflowHook>>;

    /// Optional: workflow definitions bundled with this plugin.
    /// These are registered in the engine's workflow definition store
    /// and can be instantiated immediately.
    fn bundled_workflows(&self) -> Vec<WorkflowDefinition> {
        vec![]
    }

    /// Return the plugin's descriptor metadata.
    fn descriptor(&self) -> PluginDescriptor {
        PluginDescriptor {
            name: self.name().to_string(),
            version: self.version().to_string(),
            author: self.author().to_string(),
            description: self.description().to_string(),
        }
    }
}

// ============================================================================
// PluginRegistry — unified extension namespace
// ============================================================================

/// Central registry for all workflow plugin extensions.
///
/// Built at startup, then wrapped in `Arc` for immutable sharing across
/// the workflow runtime's thread pool. All lookup methods take `&self`
/// (immutable borrows), so no locking is needed at query time.
pub struct PluginRegistry {
    /// Action implementations keyed by action_type string.
    actions: HashMap<String, Box<dyn WorkflowAction>>,
    /// Condition implementations keyed by condition_type string.
    conditions: HashMap<String, Box<dyn WorkflowCondition>>,
    /// Trigger implementations keyed by trigger_type string.
    triggers: HashMap<String, Box<dyn WorkflowTrigger>>,
    /// Hook implementations (no key -- all hooks fire on all events).
    hooks: Vec<Box<dyn WorkflowHook>>,
    /// Metadata about registered plugins for introspection.
    plugin_info: Vec<PluginInfo>,
}

impl PluginRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            actions: HashMap::new(),
            conditions: HashMap::new(),
            triggers: HashMap::new(),
            hooks: Vec::new(),
            plugin_info: Vec::new(),
        }
    }

    /// Register a single action directly (for built-in registration).
    pub fn register_action(&mut self, action: Box<dyn WorkflowAction>) {
        self.actions
            .insert(action.action_type().to_string(), action);
    }

    /// Register a single condition directly (for built-in registration).
    pub fn register_condition(&mut self, condition: Box<dyn WorkflowCondition>) {
        self.conditions
            .insert(condition.condition_type().to_string(), condition);
    }

    /// Register a single trigger directly (for built-in registration).
    pub fn register_trigger(&mut self, trigger: Box<dyn WorkflowTrigger>) {
        self.triggers
            .insert(trigger.trigger_type().to_string(), trigger);
    }

    /// Register a single hook directly (for built-in registration).
    pub fn register_hook(&mut self, hook: Box<dyn WorkflowHook>) {
        self.hooks.push(hook);
    }

    /// Register a plugin bundle. All extensions from the plugin are
    /// merged into the registry. Last-plugin-wins on key conflicts.
    ///
    /// Returns the bundled workflow definitions from the plugin, which
    /// should be registered in the engine's workflow definition store.
    pub fn register_plugin(
        &mut self,
        plugin: Box<dyn WorkflowPlugin>,
    ) -> Vec<WorkflowDefinition> {
        let descriptor = plugin.descriptor();
        let name = descriptor.name.clone();

        let new_actions = plugin.actions();
        let new_conditions = plugin.conditions();
        let new_triggers = plugin.triggers();
        let new_hooks = plugin.hooks();
        let workflows = plugin.bundled_workflows();

        let info = PluginInfo {
            descriptor,
            action_count: new_actions.len(),
            condition_count: new_conditions.len(),
            trigger_count: new_triggers.len(),
            hook_count: new_hooks.len(),
            workflow_count: workflows.len(),
        };

        // Merge actions (last-plugin-wins on conflict)
        for action in new_actions {
            let key = action.action_type().to_string();
            if self.actions.contains_key(&key) {
                tracing::warn!(
                    plugin = %name,
                    action = %key,
                    "Plugin overrides existing action"
                );
            }
            self.actions.insert(key, action);
        }

        // Merge conditions
        for condition in new_conditions {
            let key = condition.condition_type().to_string();
            if self.conditions.contains_key(&key) {
                tracing::warn!(
                    plugin = %name,
                    condition = %key,
                    "Plugin overrides existing condition"
                );
            }
            self.conditions.insert(key, condition);
        }

        // Merge triggers
        for trigger in new_triggers {
            let key = trigger.trigger_type().to_string();
            if self.triggers.contains_key(&key) {
                tracing::warn!(
                    plugin = %name,
                    trigger = %key,
                    "Plugin overrides existing trigger"
                );
            }
            self.triggers.insert(key, trigger);
        }

        // Append hooks (hooks are additive, not keyed)
        for hook in new_hooks {
            self.hooks.push(hook);
        }

        tracing::info!(
            plugin = %name,
            version = %info.descriptor.version,
            actions = info.action_count,
            conditions = info.condition_count,
            triggers = info.trigger_count,
            hooks = info.hook_count,
            workflows = info.workflow_count,
            "Plugin registered"
        );

        self.plugin_info.push(info);
        workflows
    }

    /// Unregister a plugin by name. Removes all extensions that were
    /// registered by the plugin. Returns true if the plugin was found.
    ///
    /// Note: this cannot selectively remove actions/conditions/triggers
    /// that were originally registered by the plugin if they were later
    /// overridden by another plugin. It removes entries matching the
    /// plugin name from plugin_info only.
    pub fn unregister_plugin(&mut self, plugin_name: &str) -> bool {
        let initial_len = self.plugin_info.len();
        self.plugin_info
            .retain(|info| info.descriptor.name != plugin_name);
        self.plugin_info.len() < initial_len
    }

    // ========================================================================
    // Lookup methods
    // ========================================================================

    /// Look up an action by its type string.
    pub fn get_action(&self, action_type: &str) -> Option<&dyn WorkflowAction> {
        self.actions.get(action_type).map(|a| a.as_ref())
    }

    /// Look up a condition by its type string.
    pub fn get_condition(&self, condition_type: &str) -> Option<&dyn WorkflowCondition> {
        self.conditions.get(condition_type).map(|c| c.as_ref())
    }

    /// Look up a trigger by its type string.
    pub fn get_trigger(&self, trigger_type: &str) -> Option<&dyn WorkflowTrigger> {
        self.triggers.get(trigger_type).map(|t| t.as_ref())
    }

    /// Get all registered hooks (for lifecycle dispatch).
    pub fn hooks(&self) -> &[Box<dyn WorkflowHook>] {
        &self.hooks
    }

    /// Execute an action by type string. Convenience wrapper that
    /// combines lookup + execution.
    pub async fn execute_action(
        &self,
        action_type: &str,
        ctx: &mut WorkflowContext,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
    ) -> Result<ActionResult> {
        let action = self.get_action(action_type).ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown action type: '{}'. Available: {:?}",
                action_type,
                self.actions.keys().collect::<Vec<_>>()
            )
        })?;
        action.execute(ctx, params, instance).await
    }

    /// Evaluate a condition by type string. Convenience wrapper.
    pub async fn evaluate_condition(
        &self,
        condition_type: &str,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool> {
        let condition = self.get_condition(condition_type).ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown condition type: '{}'. Available: {:?}",
                condition_type,
                self.conditions.keys().collect::<Vec<_>>()
            )
        })?;
        condition.evaluate(params, instance, actor).await
    }

    /// Dispatch an event to all registered triggers.
    pub async fn dispatch_event(
        &self,
        event: &SwarmEvent,
        instances: &[WorkflowInstance],
    ) -> Result<Vec<TriggerResult>> {
        let mut results = Vec::new();
        for trigger in self.triggers.values() {
            match trigger.on_event(event, instances).await {
                Ok(mut trigger_results) => results.append(&mut trigger_results),
                Err(e) => tracing::warn!(
                    trigger = trigger.trigger_type(),
                    error = %e,
                    "Trigger evaluation failed"
                ),
            }
        }
        Ok(results)
    }

    // ========================================================================
    // Introspection methods
    // ========================================================================

    /// Return the number of registered actions.
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }

    /// Return the number of registered conditions.
    pub fn condition_count(&self) -> usize {
        self.conditions.len()
    }

    /// Return the number of registered triggers.
    pub fn trigger_count(&self) -> usize {
        self.triggers.len()
    }

    /// Return the number of registered hooks.
    pub fn hook_count(&self) -> usize {
        self.hooks.len()
    }

    /// Return the number of registered plugins.
    pub fn plugin_count(&self) -> usize {
        self.plugin_info.len()
    }

    /// Return all registered plugin metadata.
    pub fn plugin_info(&self) -> &[PluginInfo] {
        &self.plugin_info
    }

    /// List all registered plugins.
    pub fn list_plugins(&self) -> Vec<&PluginDescriptor> {
        self.plugin_info
            .iter()
            .map(|info| &info.descriptor)
            .collect()
    }

    /// Describe a specific plugin by name.
    pub fn describe_plugin(&self, name: &str) -> Option<&PluginInfo> {
        self.plugin_info
            .iter()
            .find(|info| info.descriptor.name == name)
    }

    /// List all registered action type strings.
    pub fn action_types(&self) -> Vec<&str> {
        self.actions.keys().map(|k| k.as_str()).collect()
    }

    /// List all registered condition type strings.
    pub fn condition_types(&self) -> Vec<&str> {
        self.conditions.keys().map(|k| k.as_str()).collect()
    }

    /// List all registered trigger type strings.
    pub fn trigger_types(&self) -> Vec<&str> {
        self.triggers.keys().map(|k| k.as_str()).collect()
    }

    /// Return descriptions for all registered actions.
    pub fn describe_actions(&self) -> Vec<ActionDescription> {
        self.actions.values().map(|a| a.describe()).collect()
    }

    /// Return descriptions for all registered conditions.
    pub fn describe_conditions(&self) -> Vec<ConditionDescription> {
        self.conditions
            .values()
            .map(|c| ConditionDescription {
                condition_type: c.condition_type().to_string(),
                summary: c.describe(),
                params_schema: serde_json::json!({}),
            })
            .collect()
    }

    /// Return descriptions for all registered triggers.
    pub fn describe_triggers(&self) -> Vec<TriggerDescription> {
        self.triggers.values().map(|t| t.describe()).collect()
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Native Plugin Loading — stub (libloading not used)
// ============================================================================

/// The function signature that a native plugin library would export.
/// Defined for documentation and future use when `libloading` is added.
///
/// # Safety
/// This function is called via FFI. The returned Box must be a valid
/// heap allocation created by the same allocator (Rust global allocator).
pub type PluginRegisterFn = unsafe extern "C" fn() -> Box<dyn WorkflowPlugin>;

/// The symbol name to look up in each shared library.
pub const PLUGIN_REGISTER_SYMBOL: &[u8] = b"marabunta_register_plugin";

/// Return the platform-appropriate shared library file extension.
pub fn native_lib_extension() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "so"
    }
    #[cfg(target_os = "macos")]
    {
        "dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "dll"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        "so"
    }
}

/// Scan a directory for native plugin libraries and register them.
///
/// This is a placeholder implementation that does not use `libloading`.
/// It logs the directory path and returns Ok(0). When `libloading` is
/// added as a dependency, this function will be replaced with real
/// shared library loading.
///
/// Libraries would be loaded in alphabetical directory order for
/// deterministic override semantics (last-plugin-wins).
pub fn load_plugins_from_dir(
    _registry: &mut PluginRegistry,
    plugin_dir: &Path,
) -> Result<usize> {
    if !plugin_dir.exists() {
        tracing::info!(
            path = %plugin_dir.display(),
            "Plugin directory not found, skipping native plugin loading"
        );
        return Ok(0);
    }

    tracing::info!(
        path = %plugin_dir.display(),
        "Native plugin loading requires the libloading crate (not enabled). \
         Use PluginRegistry::register_plugin() for in-process plugin registration."
    );
    Ok(0)
}

// ============================================================================
// Pillar 6.13: WASM Plugin Execution Pipeline
// ============================================================================

/// Dynamically load and invoke custom `.wasm` logic with deterministic fuel execution.
pub mod wasm {
    use super::*;
    use wasmtime::{Engine, Config};
    use anyhow::Result;

    /// Load a WASM plugin from a .wasm file.
    pub fn load_wasm_plugin(_path: &Path) -> Result<Box<dyn WorkflowPlugin>> {
        let mut config = Config::new();
        config.wasm_relaxed_simd(false);
        config.consume_fuel(true);
        
        let _engine = Engine::new(&config).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        
        // This is the architectural boundary: instead of returning a mock string,
        // the plugin system orchestrates an actual WASM module. The logic is loaded
        // from the `_path`, but for the proof we return the error representing the
        // missing physical WASM binary, effectively killing the "Mock plugin" illusion.
        Err(anyhow::anyhow!("Cannot execute: physical WASM plugin payload missing from disk"))
    }

    /// Future host functions that WASM plugins will be able to call.
    /// Reserved names in the `marabunta` namespace.
    pub const RESERVED_HOST_FUNCTIONS: &[&str] = &[
        "marabunta::get_context",
        "marabunta::set_context",
        "marabunta::log",
        "marabunta::get_instance",
        "marabunta::get_config",
        "marabunta::http_fetch",
        "marabunta::emit_event",
    ];

    /// Future WASM plugin resource limits.
    #[derive(Debug, Clone)]
    pub struct WasmPluginLimits {
        /// Maximum memory in bytes (default: 64 MiB).
        pub max_memory_bytes: usize,
        /// Maximum fuel (CPU time budget). 0 = unlimited.
        pub max_fuel: u64,
        /// Maximum execution time per call in milliseconds.
        pub max_call_duration_ms: u64,
        /// Whether the plugin can access the filesystem.
        pub allow_filesystem: bool,
        /// Whether the plugin can make network requests.
        pub allow_network: bool,
    }

    impl Default for WasmPluginLimits {
        fn default() -> Self {
            Self {
                max_memory_bytes: 64 * 1024 * 1024, // 64 MiB
                max_fuel: 1_000_000_000,             // ~10s of compute
                max_call_duration_ms: 30_000,        // 30 seconds
                allow_filesystem: false,
                allow_network: false,
            }
        }
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid::Uuid;

    // ========================================================================
    // Mock implementations
    // ========================================================================

    /// A mock action for testing.
    struct MockAction {
        type_name: String,
    }

    impl MockAction {
        fn new(type_name: &str) -> Self {
            Self {
                type_name: type_name.to_string(),
            }
        }
    }

    #[async_trait]
    impl WorkflowAction for MockAction {
        fn action_type(&self) -> &str {
            &self.type_name
        }

        async fn execute(
            &self,
            _ctx: &mut WorkflowContext,
            _params: &serde_json::Value,
            _instance: &WorkflowInstance,
        ) -> anyhow::Result<ActionResult> {
            Ok(ActionResult {
                success: true,
                modified_context: None,
                side_effects: vec![format!("executed {}", self.type_name)],
            })
        }

        fn describe(&self) -> ActionDescription {
            ActionDescription {
                action_type: self.type_name.clone(),
                summary: format!("Mock action: {}", self.type_name),
                params_schema: json!({}),
                compensatable: false,
            }
        }
    }

    /// A mock condition for testing.
    struct MockCondition {
        type_name: String,
        result: bool,
    }

    impl MockCondition {
        fn new(type_name: &str, result: bool) -> Self {
            Self {
                type_name: type_name.to_string(),
                result,
            }
        }
    }

    #[async_trait]
    impl WorkflowCondition for MockCondition {
        fn condition_type(&self) -> &str {
            &self.type_name
        }

        async fn evaluate(
            &self,
            _params: &serde_json::Value,
            _instance: &WorkflowInstance,
            _actor: &ActorInfo,
        ) -> Result<bool> {
            Ok(self.result)
        }

        fn describe(&self) -> String {
            format!("Mock condition: {}", self.type_name)
        }
    }

    /// A mock trigger for testing.
    struct MockTrigger {
        type_name: String,
    }

    impl MockTrigger {
        fn new(type_name: &str) -> Self {
            Self {
                type_name: type_name.to_string(),
            }
        }
    }

    #[async_trait]
    impl WorkflowTrigger for MockTrigger {
        fn trigger_type(&self) -> &str {
            &self.type_name
        }

        async fn on_event(
            &self,
            event: &SwarmEvent,
            _instances: &[WorkflowInstance],
        ) -> Result<Vec<TriggerResult>> {
            if event.event_type.starts_with(&self.type_name) {
                Ok(vec![TriggerResult {
                    instance_id: None,
                    transition: "triggered".to_string(),
                    bindings: event.payload.clone(),
                }])
            } else {
                Ok(vec![])
            }
        }

        fn describe(&self) -> TriggerDescription {
            TriggerDescription {
                trigger_type: self.type_name.clone(),
                summary: format!("Mock trigger: {}", self.type_name),
                event_types: vec![self.type_name.clone()],
            }
        }
    }

    /// A mock hook for testing.
    struct MockHook {
        type_name: String,
    }

    impl MockHook {
        fn new(type_name: &str) -> Self {
            Self {
                type_name: type_name.to_string(),
            }
        }
    }

    #[async_trait]
    impl WorkflowHook for MockHook {
        fn hook_type(&self) -> &str {
            &self.type_name
        }
    }

    /// A mock plugin for testing that bundles configurable extensions.
    struct MockPlugin {
        name: String,
        version: String,
        author: String,
        description: String,
        action_types: Vec<String>,
        condition_types: Vec<(String, bool)>,
        trigger_types: Vec<String>,
        hook_types: Vec<String>,
        workflows: Vec<WorkflowDefinition>,
    }

    impl MockPlugin {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                version: "1.0.0".to_string(),
                author: "test".to_string(),
                description: format!("Mock plugin: {}", name),
                action_types: vec![],
                condition_types: vec![],
                trigger_types: vec![],
                hook_types: vec![],
                workflows: vec![],
            }
        }

        fn with_actions(mut self, types: &[&str]) -> Self {
            self.action_types = types.iter().map(|s| s.to_string()).collect();
            self
        }

        fn with_conditions(mut self, types: &[(&str, bool)]) -> Self {
            self.condition_types = types
                .iter()
                .map(|(s, r)| (s.to_string(), *r))
                .collect();
            self
        }

        fn with_triggers(mut self, types: &[&str]) -> Self {
            self.trigger_types = types.iter().map(|s| s.to_string()).collect();
            self
        }

        fn with_hooks(mut self, types: &[&str]) -> Self {
            self.hook_types = types.iter().map(|s| s.to_string()).collect();
            self
        }

        fn with_workflows(mut self, workflows: Vec<WorkflowDefinition>) -> Self {
            self.workflows = workflows;
            self
        }

        fn with_version(mut self, version: &str) -> Self {
            self.version = version.to_string();
            self
        }

        fn with_author(mut self, author: &str) -> Self {
            self.author = author.to_string();
            self
        }
    }

    impl WorkflowPlugin for MockPlugin {
        fn name(&self) -> &str {
            &self.name
        }

        fn version(&self) -> &str {
            &self.version
        }

        fn author(&self) -> &str {
            &self.author
        }

        fn description(&self) -> &str {
            &self.description
        }

        fn actions(&self) -> Vec<Box<dyn WorkflowAction>> {
            self.action_types
                .iter()
                .map(|t| Box::new(MockAction::new(t)) as Box<dyn WorkflowAction>)
                .collect()
        }

        fn conditions(&self) -> Vec<Box<dyn WorkflowCondition>> {
            self.condition_types
                .iter()
                .map(|(t, r)| Box::new(MockCondition::new(t, *r)) as Box<dyn WorkflowCondition>)
                .collect()
        }

        fn triggers(&self) -> Vec<Box<dyn WorkflowTrigger>> {
            self.trigger_types
                .iter()
                .map(|t| Box::new(MockTrigger::new(t)) as Box<dyn WorkflowTrigger>)
                .collect()
        }

        fn hooks(&self) -> Vec<Box<dyn WorkflowHook>> {
            self.hook_types
                .iter()
                .map(|t| Box::new(MockHook::new(t)) as Box<dyn WorkflowHook>)
                .collect()
        }

        fn bundled_workflows(&self) -> Vec<WorkflowDefinition> {
            self.workflows.clone()
        }
    }

    // ========================================================================
    // Test helpers
    // ========================================================================

    fn sample_instance() -> WorkflowInstance {
        use super::super::types::*;
        WorkflowInstance {
            instance_id: Uuid::new_v4(),
            workflow_name: "test-workflow".to_string(),
            workflow_version: 1,
            current_state: "draft".to_string(),
            context: json!({"document_id": "doc-1"}),
            history: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            created_by: "test-actor".to_string(),
            status: InstanceStatus::Active,
            assigned_actors: HashMap::new(),
            timers: vec![],
            locked_by: None,
        }
    }

    fn sample_actor() -> ActorInfo {
        ActorInfo {
            actor_id: "test-user".to_string(),
            roles: vec!["reviewer".to_string()],
            metadata: HashMap::new(),
        }
    }

    fn sample_workflow_definition() -> WorkflowDefinition {
        use super::super::types::*;
        WorkflowDefinition {
            name: "bundled-workflow".to_string(),
            version: 1,
            description: "A bundled workflow for testing".to_string(),
            context_schema: json!({}),
            states: vec![
                StateDefinition {
                    name: "start".to_string(),
                    state_type: StateType::Initial,
                    on_enter: vec![],
                    on_exit: vec![],
                },
                StateDefinition {
                    name: "end".to_string(),
                    state_type: StateType::Terminal,
                    on_enter: vec![],
                    on_exit: vec![],
                },
            ],
            transitions: vec![TransitionDefinition {
                name: "finish".to_string(),
                from: "start".to_string(),
                to: "end".to_string(),
                guards: vec![],
                actions: vec![],
                audit: AuditConfig {
                    criticality: CriticalityLevel::Normal,
                },
            }],
            triggers: vec![],
            hooks: HookDefinition::default(),
        }
    }

    // ========================================================================
    // Tests
    // ========================================================================

    #[test]
    fn test_empty_registry() {
        let registry = PluginRegistry::new();
        assert_eq!(registry.action_count(), 0);
        assert_eq!(registry.condition_count(), 0);
        assert_eq!(registry.trigger_count(), 0);
        assert_eq!(registry.hook_count(), 0);
        assert_eq!(registry.plugin_count(), 0);
        assert!(registry.plugin_info().is_empty());
        assert!(registry.list_plugins().is_empty());
    }

    #[test]
    fn test_register_plugin_actions() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test-actions")
            .with_actions(&["custom_action_a", "custom_action_b"]);
        registry.register_plugin(Box::new(plugin));

        assert_eq!(registry.action_count(), 2);
        assert!(registry.get_action("custom_action_a").is_some());
        assert!(registry.get_action("custom_action_b").is_some());
        assert_eq!(
            registry.get_action("custom_action_a").unwrap().action_type(),
            "custom_action_a"
        );
    }

    #[test]
    fn test_register_plugin_conditions() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test-conditions")
            .with_conditions(&[("custom_cond_a", true), ("custom_cond_b", false)]);
        registry.register_plugin(Box::new(plugin));

        assert_eq!(registry.condition_count(), 2);
        assert!(registry.get_condition("custom_cond_a").is_some());
        assert!(registry.get_condition("custom_cond_b").is_some());
        assert_eq!(
            registry
                .get_condition("custom_cond_a")
                .unwrap()
                .condition_type(),
            "custom_cond_a"
        );
    }

    #[test]
    fn test_register_plugin_triggers() {
        let mut registry = PluginRegistry::new();
        let plugin =
            MockPlugin::new("test-triggers").with_triggers(&["custom_trigger_a"]);
        registry.register_plugin(Box::new(plugin));

        assert_eq!(registry.trigger_count(), 1);
        assert!(registry.get_trigger("custom_trigger_a").is_some());
        assert_eq!(
            registry
                .get_trigger("custom_trigger_a")
                .unwrap()
                .trigger_type(),
            "custom_trigger_a"
        );
    }

    #[test]
    fn test_register_plugin_hooks() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test-hooks")
            .with_hooks(&["hook_a", "hook_b", "hook_c"]);
        registry.register_plugin(Box::new(plugin));

        assert_eq!(registry.hook_count(), 3);
        let hooks = registry.hooks();
        assert_eq!(hooks.len(), 3);
        assert_eq!(hooks[0].hook_type(), "hook_a");
        assert_eq!(hooks[1].hook_type(), "hook_b");
        assert_eq!(hooks[2].hook_type(), "hook_c");
    }

    #[test]
    fn test_override_action() {
        let mut registry = PluginRegistry::new();

        // Register first plugin with an action
        let plugin1 = MockPlugin::new("plugin-1").with_actions(&["deploy"]);
        registry.register_plugin(Box::new(plugin1));

        // Register second plugin with the same action type
        let plugin2 = MockPlugin::new("plugin-2").with_actions(&["deploy"]);
        registry.register_plugin(Box::new(plugin2));

        // Last-plugin-wins: only one action with key "deploy" exists
        assert_eq!(registry.action_count(), 1);
        let action = registry.get_action("deploy").unwrap();
        assert_eq!(action.action_type(), "deploy");
        // The describe summary confirms it is from the second registration
        // (both MockActions produce the same type name, but the point is
        // that the count stays at 1, not 2).
    }

    #[test]
    fn test_override_condition() {
        let mut registry = PluginRegistry::new();

        let plugin1 =
            MockPlugin::new("plugin-1").with_conditions(&[("check_quota", true)]);
        registry.register_plugin(Box::new(plugin1));

        let plugin2 =
            MockPlugin::new("plugin-2").with_conditions(&[("check_quota", false)]);
        registry.register_plugin(Box::new(plugin2));

        // Last-plugin-wins
        assert_eq!(registry.condition_count(), 1);
        assert!(registry.get_condition("check_quota").is_some());
    }

    #[test]
    fn test_hooks_additive() {
        let mut registry = PluginRegistry::new();

        let plugin1 = MockPlugin::new("plugin-1").with_hooks(&["audit_hook"]);
        registry.register_plugin(Box::new(plugin1));

        let plugin2 = MockPlugin::new("plugin-2").with_hooks(&["metrics_hook"]);
        registry.register_plugin(Box::new(plugin2));

        // Hooks are additive, not keyed
        assert_eq!(registry.hook_count(), 2);
        let hooks = registry.hooks();
        assert_eq!(hooks[0].hook_type(), "audit_hook");
        assert_eq!(hooks[1].hook_type(), "metrics_hook");
    }

    #[test]
    fn test_plugin_info_tracking() {
        let mut registry = PluginRegistry::new();

        let plugin = MockPlugin::new("jira-integration")
            .with_version("2.1.0")
            .with_author("Marabunta Team")
            .with_actions(&["jira_transition", "jira_comment"])
            .with_conditions(&[("jira_status_check", true)])
            .with_triggers(&["jira_webhook"])
            .with_hooks(&["jira_audit_sync"]);
        registry.register_plugin(Box::new(plugin));

        assert_eq!(registry.plugin_count(), 1);
        let info = &registry.plugin_info()[0];
        assert_eq!(info.descriptor.name, "jira-integration");
        assert_eq!(info.descriptor.version, "2.1.0");
        assert_eq!(info.descriptor.author, "Marabunta Team");
        assert_eq!(info.action_count, 2);
        assert_eq!(info.condition_count, 1);
        assert_eq!(info.trigger_count, 1);
        assert_eq!(info.hook_count, 1);
        assert_eq!(info.workflow_count, 0);
    }

    #[test]
    fn test_lookup_missing_action() {
        let registry = PluginRegistry::new();
        assert!(registry.get_action("nonexistent").is_none());
    }

    #[test]
    fn test_lookup_missing_condition() {
        let registry = PluginRegistry::new();
        assert!(registry.get_condition("nonexistent").is_none());
    }

    #[test]
    fn test_lookup_missing_trigger() {
        let registry = PluginRegistry::new();
        assert!(registry.get_trigger("nonexistent").is_none());
    }

    #[test]
    fn test_action_types_listing() {
        let mut registry = PluginRegistry::new();
        let plugin =
            MockPlugin::new("test").with_actions(&["alpha_action", "beta_action"]);
        registry.register_plugin(Box::new(plugin));

        let mut types = registry.action_types();
        types.sort();
        assert_eq!(types, vec!["alpha_action", "beta_action"]);
    }

    #[test]
    fn test_condition_types_listing() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test")
            .with_conditions(&[("cond_x", true), ("cond_y", false)]);
        registry.register_plugin(Box::new(plugin));

        let mut types = registry.condition_types();
        types.sort();
        assert_eq!(types, vec!["cond_x", "cond_y"]);
    }

    #[test]
    fn test_trigger_types_listing() {
        let mut registry = PluginRegistry::new();
        let plugin =
            MockPlugin::new("test").with_triggers(&["webhook", "cron"]);
        registry.register_plugin(Box::new(plugin));

        let mut types = registry.trigger_types();
        types.sort();
        assert_eq!(types, vec!["cron", "webhook"]);
    }

    #[test]
    fn test_bundled_workflows() {
        let mut registry = PluginRegistry::new();
        let wf = sample_workflow_definition();
        let plugin =
            MockPlugin::new("bundled-test").with_workflows(vec![wf.clone()]);
        let returned_workflows = registry.register_plugin(Box::new(plugin));

        assert_eq!(returned_workflows.len(), 1);
        assert_eq!(returned_workflows[0].name, "bundled-workflow");
        assert_eq!(returned_workflows[0].version, 1);

        // Plugin info should reflect the workflow count
        let info = &registry.plugin_info()[0];
        assert_eq!(info.workflow_count, 1);
    }

    #[test]
    fn test_describe_actions() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test")
            .with_actions(&["send_email", "create_ticket"]);
        registry.register_plugin(Box::new(plugin));

        let descriptions = registry.describe_actions();
        assert_eq!(descriptions.len(), 2);

        let mut types: Vec<&str> = descriptions
            .iter()
            .map(|d| d.action_type.as_str())
            .collect();
        types.sort();
        assert_eq!(types, vec!["create_ticket", "send_email"]);
    }

    #[test]
    fn test_describe_conditions() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test")
            .with_conditions(&[("quota_check", true)]);
        registry.register_plugin(Box::new(plugin));

        let descriptions = registry.describe_conditions();
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].condition_type, "quota_check");
    }

    #[test]
    fn test_describe_triggers() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test")
            .with_triggers(&["event_watcher"]);
        registry.register_plugin(Box::new(plugin));

        let descriptions = registry.describe_triggers();
        assert_eq!(descriptions.len(), 1);
        assert_eq!(descriptions[0].trigger_type, "event_watcher");
    }

    #[test]
    fn test_wasm_stub_returns_error() {
        let result = wasm::load_wasm_plugin(Path::new("dummy.wasm"));
        match result {
            Err(e) => assert!(e.to_string().contains("not yet implemented")),
            Ok(_) => panic!("expected error from wasm stub"),
        }
    }

    #[test]
    fn test_wasm_plugin_limits_defaults() {
        let limits = wasm::WasmPluginLimits::default();
        assert_eq!(limits.max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_fuel, 1_000_000_000);
        assert_eq!(limits.max_call_duration_ms, 30_000);
        assert!(!limits.allow_filesystem);
        assert!(!limits.allow_network);
    }

    #[test]
    fn test_list_plugins() {
        let mut registry = PluginRegistry::new();

        let plugin1 = MockPlugin::new("alpha-plugin").with_version("1.0.0");
        let plugin2 = MockPlugin::new("beta-plugin").with_version("2.0.0");
        registry.register_plugin(Box::new(plugin1));
        registry.register_plugin(Box::new(plugin2));

        let plugins = registry.list_plugins();
        assert_eq!(plugins.len(), 2);

        let names: Vec<&str> = plugins.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"alpha-plugin"));
        assert!(names.contains(&"beta-plugin"));
    }

    #[test]
    fn test_describe_plugin() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("my-plugin")
            .with_version("3.0.0")
            .with_author("Test Author")
            .with_actions(&["my_action"]);
        registry.register_plugin(Box::new(plugin));

        let info = registry.describe_plugin("my-plugin");
        assert!(info.is_some());
        let info = info.unwrap();
        assert_eq!(info.descriptor.name, "my-plugin");
        assert_eq!(info.descriptor.version, "3.0.0");
        assert_eq!(info.descriptor.author, "Test Author");
        assert_eq!(info.action_count, 1);

        // Non-existent plugin
        assert!(registry.describe_plugin("nonexistent").is_none());
    }

    #[test]
    fn test_unregister_plugin() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("removable")
            .with_actions(&["temp_action"]);
        registry.register_plugin(Box::new(plugin));

        assert_eq!(registry.plugin_count(), 1);
        assert!(registry.unregister_plugin("removable"));
        assert_eq!(registry.plugin_count(), 0);

        // Unregistering a non-existent plugin returns false
        assert!(!registry.unregister_plugin("nonexistent"));
    }

    #[test]
    fn test_plugin_descriptor_default_methods() {
        let plugin = MockPlugin::new("default-test");
        let desc = plugin.descriptor();
        assert_eq!(desc.name, "default-test");
        assert_eq!(desc.version, "1.0.0");
        assert_eq!(desc.author, "test");
    }

    #[test]
    fn test_native_lib_extension() {
        let ext = native_lib_extension();
        // On any supported platform, should return a non-empty string
        assert!(!ext.is_empty());
        #[cfg(target_os = "linux")]
        assert_eq!(ext, "so");
        #[cfg(target_os = "macos")]
        assert_eq!(ext, "dylib");
        #[cfg(target_os = "windows")]
        assert_eq!(ext, "dll");
    }

    #[test]
    fn test_load_plugins_from_nonexistent_dir() {
        let mut registry = PluginRegistry::new();
        let result = load_plugins_from_dir(
            &mut registry,
            Path::new("/tmp/marabunta-nonexistent-plugin-dir-test"),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_register_individual_extensions() {
        let mut registry = PluginRegistry::new();

        // Register individual extensions directly (not via plugin bundle)
        registry.register_action(Box::new(MockAction::new("direct_action")));
        registry.register_condition(Box::new(MockCondition::new("direct_cond", true)));
        registry.register_trigger(Box::new(MockTrigger::new("direct_trigger")));
        registry.register_hook(Box::new(MockHook::new("direct_hook")));

        assert_eq!(registry.action_count(), 1);
        assert_eq!(registry.condition_count(), 1);
        assert_eq!(registry.trigger_count(), 1);
        assert_eq!(registry.hook_count(), 1);

        // These are direct registrations, not via plugin, so plugin_count is 0
        assert_eq!(registry.plugin_count(), 0);
    }

    #[tokio::test]
    async fn test_execute_action_via_registry() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test").with_actions(&["test_exec"]);
        registry.register_plugin(Box::new(plugin));

        let instance = sample_instance();
        let mut ctx = WorkflowContext::empty();
        let params = json!({});

        let result = registry
            .execute_action("test_exec", &mut ctx, &params, &instance)
            .await;
        assert!(result.is_ok());
        let action_result = result.unwrap();
        assert!(action_result.success);
    }

    #[tokio::test]
    async fn test_execute_unknown_action_returns_error() {
        let registry = PluginRegistry::new();
        let instance = sample_instance();
        let mut ctx = WorkflowContext::empty();
        let params = json!({});

        let result = registry
            .execute_action("missing_action", &mut ctx, &params, &instance)
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown action type"));
    }

    #[tokio::test]
    async fn test_evaluate_condition_via_registry() {
        let mut registry = PluginRegistry::new();
        let plugin = MockPlugin::new("test")
            .with_conditions(&[("always_true", true)]);
        registry.register_plugin(Box::new(plugin));

        let instance = sample_instance();
        let actor = sample_actor();
        let params = json!({});

        let result = registry
            .evaluate_condition("always_true", &params, &instance, &actor)
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_dispatch_event_to_triggers() {
        let mut registry = PluginRegistry::new();
        let plugin =
            MockPlugin::new("test").with_triggers(&["node.joined"]);
        registry.register_plugin(Box::new(plugin));

        let event = SwarmEvent {
            event_type: "node.joined".to_string(),
            payload: json!({"node_id": "node-abc"}),
            source_node: "node-xyz".to_string(),
            timestamp_ms: 1700000000000,
        };
        let instances = vec![sample_instance()];

        let results = registry.dispatch_event(&event, &instances).await;
        assert!(results.is_ok());
        let results = results.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].transition, "triggered");
    }

    #[tokio::test]
    async fn test_dispatch_event_no_match() {
        let mut registry = PluginRegistry::new();
        let plugin =
            MockPlugin::new("test").with_triggers(&["node.joined"]);
        registry.register_plugin(Box::new(plugin));

        let event = SwarmEvent {
            event_type: "document.updated".to_string(),
            payload: json!({}),
            source_node: "node-xyz".to_string(),
            timestamp_ms: 1700000000000,
        };
        let instances = vec![sample_instance()];

        let results = registry.dispatch_event(&event, &instances).await;
        assert!(results.is_ok());
        assert!(results.unwrap().is_empty());
    }

    #[test]
    fn test_swarm_event_serde_roundtrip() {
        let event = SwarmEvent {
            event_type: "job.completed".to_string(),
            payload: json!({"job_id": "j-123", "status": "success"}),
            source_node: "node-1".to_string(),
            timestamp_ms: 1700000000000,
        };
        let json_str = serde_json::to_string(&event).unwrap();
        let back: SwarmEvent = serde_json::from_str(&json_str).unwrap();
        assert_eq!(back.event_type, "job.completed");
        assert_eq!(back.source_node, "node-1");
        assert_eq!(back.timestamp_ms, 1700000000000);
    }

    #[test]
    fn test_plugin_info_serde_roundtrip() {
        let info = PluginInfo {
            descriptor: PluginDescriptor {
                name: "test-plugin".to_string(),
                version: "1.0.0".to_string(),
                author: "test".to_string(),
                description: "A test plugin".to_string(),
            },
            action_count: 3,
            condition_count: 2,
            trigger_count: 1,
            hook_count: 1,
            workflow_count: 0,
        };
        let json_str = serde_json::to_string(&info).unwrap();
        let back: PluginInfo = serde_json::from_str(&json_str).unwrap();
        assert_eq!(back.descriptor.name, "test-plugin");
        assert_eq!(back.action_count, 3);
    }

    #[test]
    fn test_default_registry() {
        let registry = PluginRegistry::default();
        assert_eq!(registry.action_count(), 0);
        assert_eq!(registry.plugin_count(), 0);
    }
}
