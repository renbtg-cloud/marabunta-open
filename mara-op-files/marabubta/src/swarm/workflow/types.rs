// Marabunta - Licensed under the MIT License.
//! Foundation types for the L4 state machine workflow engine.
//!
//! Every type in this module is designed for gossip propagation and
//! serialization. All structs and enums derive `Serialize` and
//! `Deserialize` at minimum. The types here form the contract between
//! workflow definitions (authored by plugin developers) and the runtime
//! engine that executes them.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

// ============================================================================
// State Machine Enums
// ============================================================================

/// The type of a state in a workflow definition.
/// Every workflow must have exactly one Initial state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateType {
    /// Entry point of the workflow. Exactly one per definition.
    Initial,
    /// Normal active state where work happens.
    Active,
    /// End state. Workflow is complete when it reaches a terminal state.
    Terminal,
}

impl fmt::Display for StateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initial => write!(f, "initial"),
            Self::Active => write!(f, "active"),
            Self::Terminal => write!(f, "terminal"),
        }
    }
}

/// Criticality level for audit configuration.
/// Controls witness quorum size and retention policy in L3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CriticalityLevel {
    Low,
    Normal,
    High,
    Critical,
}

impl fmt::Display for CriticalityLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "LOW"),
            Self::Normal => write!(f, "NORMAL"),
            Self::High => write!(f, "HIGH"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Runtime status of a workflow instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    /// Instance is running normally.
    Active,
    /// Instance reached a terminal state.
    Completed,
    /// Unrecoverable error occurred.
    Failed,
    /// Manually paused by an operator.
    Suspended,
    /// Deadline exceeded without completion.
    TimedOut,
}

impl fmt::Display for InstanceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Suspended => write!(f, "suspended"),
            Self::TimedOut => write!(f, "timed_out"),
        }
    }
}

/// Status of a scheduled timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerStatus {
    /// Waiting to fire at the scheduled time.
    Pending,
    /// Has executed its transition.
    Fired,
    /// Manually or automatically cancelled.
    Cancelled,
}

impl fmt::Display for TimerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Fired => write!(f, "fired"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

// ============================================================================
// Definition Types — describe a workflow (immutable once deployed)
// ============================================================================

/// A guard condition on a transition.
/// All guards must pass for the transition to fire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GuardDefinition {
    /// Name of the condition to evaluate (e.g., "role_is", "has_content").
    pub condition: String,
    /// Arbitrary parameters for the condition evaluator.
    #[serde(default)]
    pub params: serde_json::Value,
}

/// An action to execute on transition fire or state enter/exit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionDefinition {
    /// Name of the action type (e.g., "notify", "lock_document").
    pub action: String,
    /// Arbitrary parameters for the action executor.
    #[serde(default)]
    pub params: serde_json::Value,
}

/// Audit configuration for a transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditConfig {
    /// How critical this transition is — controls witness quorum and retention.
    pub criticality: CriticalityLevel,
}

/// A single state in a workflow definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateDefinition {
    /// Unique name of this state within the workflow.
    pub name: String,
    /// Whether this is the initial, an active, or a terminal state.
    #[serde(rename = "type")]
    pub state_type: StateType,
    /// Actions to execute when entering this state.
    #[serde(default)]
    pub on_enter: Vec<ActionDefinition>,
    /// Actions to execute when leaving this state.
    #[serde(default)]
    pub on_exit: Vec<ActionDefinition>,
}

/// A transition between two states.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitionDefinition {
    /// Unique name of this transition (e.g., "submit", "approve").
    pub name: String,
    /// Source state name.
    pub from: String,
    /// Destination state name.
    pub to: String,
    /// Guard conditions — all must pass for this transition to fire.
    #[serde(default)]
    pub guards: Vec<GuardDefinition>,
    /// Actions to execute when this transition fires.
    #[serde(default)]
    pub actions: Vec<ActionDefinition>,
    /// Audit configuration for this transition.
    pub audit: AuditConfig,
}

/// An external event trigger that can initiate a transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerDefinition {
    /// Event pattern to match (e.g., "document.updated").
    pub event: String,
    /// Name of the transition to fire when the event matches.
    pub transition: String,
    /// Optional schedule expression (e.g., "48h after entering in_review").
    #[serde(default)]
    pub schedule: Option<String>,
    /// Additional guards beyond the transition's own guards.
    #[serde(default)]
    pub guards: Vec<GuardDefinition>,
    /// Additional actions beyond the transition's own actions.
    #[serde(default)]
    pub actions: Vec<ActionDefinition>,
}

/// Lifecycle hook definitions.
/// Each hook point maps to a list of actions executed when the event occurs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookDefinition {
    #[serde(default)]
    pub on_instance_created: Vec<ActionDefinition>,
    #[serde(default)]
    pub on_instance_completed: Vec<ActionDefinition>,
    #[serde(default)]
    pub on_instance_failed: Vec<ActionDefinition>,
    #[serde(default)]
    pub on_transition_blocked: Vec<ActionDefinition>,
    #[serde(default)]
    pub on_deadlock_detected: Vec<ActionDefinition>,
}

/// Top-level workflow definition — the complete state machine specification.
/// Immutable once deployed. New versions create new definitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    /// Human-readable name (e.g., "document-approval").
    pub name: String,
    /// Monotonically increasing version number.
    pub version: u32,
    /// Description of what this workflow does.
    pub description: String,
    /// Schema for the workflow context (free-form JSON for validation hints).
    #[serde(default)]
    pub context_schema: serde_json::Value,
    /// All possible states.
    pub states: Vec<StateDefinition>,
    /// All possible transitions between states.
    pub transitions: Vec<TransitionDefinition>,
    /// External event triggers.
    #[serde(default)]
    pub triggers: Vec<TriggerDefinition>,
    /// Lifecycle hooks for plugin integration.
    #[serde(default)]
    pub hooks: HookDefinition,
}

impl fmt::Display for WorkflowDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@v{} ({} states, {} transitions)",
            self.name,
            self.version,
            self.states.len(),
            self.transitions.len()
        )
    }
}

// ============================================================================
// Instance Types — runtime state of a running workflow
// ============================================================================

/// Record of a single transition that was fired.
/// Immutable once created — forms the audit history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitionRecord {
    /// Name of the transition that fired.
    pub transition_name: String,
    /// State before the transition.
    pub from_state: String,
    /// State after the transition.
    pub to_state: String,
    /// ID of the actor (human or system) that triggered this.
    pub actor_id: String,
    /// When the transition was executed.
    pub timestamp: DateTime<Utc>,
    /// Which swarm node executed this transition.
    pub node_id: String,
    /// Link to the L3 audit trail event for this transition.
    pub audit_event_id: Uuid,
    /// JSON diff of what changed in the workflow context.
    pub context_diff: serde_json::Value,
}

/// A scheduled timer attached to a workflow instance.
/// Timers survive node death — they are replicated via swarm gossip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveTimer {
    /// Unique timer identifier.
    pub timer_id: Uuid,
    /// The workflow instance this timer belongs to.
    pub instance_id: Uuid,
    /// Name of the trigger that created this timer.
    pub trigger_name: String,
    /// Name of the transition to fire when the timer expires.
    pub transition: String,
    /// When the timer should fire.
    pub fire_at: DateTime<Utc>,
    /// When the timer was created.
    pub created_at: DateTime<Utc>,
    /// Which node created this timer.
    pub created_by_node: String,
    /// Current status of the timer.
    pub status: TimerStatus,
}

/// A single execution of a workflow definition.
/// Carries its own context, state, history, actors, and timers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInstance {
    /// Unique instance identifier.
    pub instance_id: Uuid,
    /// Name of the workflow definition this instance is running.
    pub workflow_name: String,
    /// Version of the workflow definition (pinned at creation time).
    pub workflow_version: u32,
    /// Current state name within the workflow.
    pub current_state: String,
    /// User-defined data carried through the workflow.
    pub context: serde_json::Value,
    /// Complete history of all transitions fired on this instance.
    pub history: Vec<TransitionRecord>,
    /// When the instance was created.
    pub created_at: DateTime<Utc>,
    /// Last time any state change occurred.
    pub updated_at: DateTime<Utc>,
    /// ID of the actor who created this instance.
    pub created_by: String,
    /// Runtime status of the instance.
    pub status: InstanceStatus,
    /// Role-to-actor mappings (e.g., "reviewer" -> ["alice", "bob"]).
    pub assigned_actors: HashMap<String, Vec<String>>,
    /// Pending scheduled triggers.
    pub timers: Vec<ActiveTimer>,
    /// Node ID holding the transition lock (prevents concurrent transitions).
    pub locked_by: Option<String>,
}

// ============================================================================
// Supporting Types
// ============================================================================

/// Ergonomic wrapper around the workflow context (serde_json::Value).
/// Provides typed access methods so callers don't need to navigate raw JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowContext {
    inner: serde_json::Value,
}

impl WorkflowContext {
    /// Create a new context from a JSON value.
    pub fn new(value: serde_json::Value) -> Self {
        Self { inner: value }
    }

    /// Create an empty context (JSON object).
    pub fn empty() -> Self {
        Self {
            inner: serde_json::json!({}),
        }
    }

    /// Get a typed value from the context by key.
    /// Returns None if the key doesn't exist or the value can't be
    /// deserialized into T.
    pub fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.inner
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Set a value in the context by key.
    pub fn set<T: Serialize>(&mut self, key: &str, value: T) {
        if let serde_json::Value::Object(ref mut map) = self.inner {
            map.insert(
                key.to_string(),
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
            );
        }
    }

    /// Check whether a key exists in the context.
    pub fn has(&self, key: &str) -> bool {
        self.inner.get(key).is_some()
    }

    /// Remove a key from the context. Returns the removed value, if any.
    pub fn remove(&mut self, key: &str) -> Option<serde_json::Value> {
        if let serde_json::Value::Object(ref mut map) = self.inner {
            map.remove(key)
        } else {
            None
        }
    }

    /// Get a reference to the underlying JSON value.
    pub fn as_value(&self) -> &serde_json::Value {
        &self.inner
    }

    /// Consume the wrapper and return the underlying JSON value.
    pub fn into_value(self) -> serde_json::Value {
        self.inner
    }
}

/// Information about the actor performing a workflow action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActorInfo {
    /// Unique actor identifier.
    pub actor_id: String,
    /// Roles held by this actor (e.g., ["reviewer", "manager"]).
    pub roles: Vec<String>,
    /// Arbitrary metadata (e.g., department, location).
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Result of executing an action during a transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionResult {
    /// Whether the action succeeded.
    pub success: bool,
    /// Optional modified context to merge back into the workflow instance.
    pub modified_context: Option<serde_json::Value>,
    /// Descriptions of side effects for the audit trail
    /// (e.g., "sent email to alice@example.com").
    #[serde(default)]
    pub side_effects: Vec<String>,
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_workflow() -> WorkflowDefinition {
        WorkflowDefinition {
            name: "document-approval".to_string(),
            version: 1,
            description: "Multi-stage document review".to_string(),
            context_schema: serde_json::json!({
                "document_id": "string",
                "author_id": "string"
            }),
            states: vec![
                StateDefinition {
                    name: "draft".to_string(),
                    state_type: StateType::Initial,
                    on_enter: vec![],
                    on_exit: vec![],
                },
                StateDefinition {
                    name: "in_review".to_string(),
                    state_type: StateType::Active,
                    on_enter: vec![ActionDefinition {
                        action: "notify".to_string(),
                        params: serde_json::json!({"to": "reviewers"}),
                    }],
                    on_exit: vec![],
                },
                StateDefinition {
                    name: "approved".to_string(),
                    state_type: StateType::Terminal,
                    on_enter: vec![],
                    on_exit: vec![],
                },
            ],
            transitions: vec![
                TransitionDefinition {
                    name: "submit".to_string(),
                    from: "draft".to_string(),
                    to: "in_review".to_string(),
                    guards: vec![GuardDefinition {
                        condition: "has_content".to_string(),
                        params: serde_json::Value::Null,
                    }],
                    actions: vec![],
                    audit: AuditConfig {
                        criticality: CriticalityLevel::Normal,
                    },
                },
                TransitionDefinition {
                    name: "approve".to_string(),
                    from: "in_review".to_string(),
                    to: "approved".to_string(),
                    guards: vec![GuardDefinition {
                        condition: "role_is".to_string(),
                        params: serde_json::json!({"role": "reviewer"}),
                    }],
                    actions: vec![],
                    audit: AuditConfig {
                        criticality: CriticalityLevel::Critical,
                    },
                },
            ],
            triggers: vec![TriggerDefinition {
                event: "document.updated".to_string(),
                transition: "submit".to_string(),
                schedule: None,
                guards: vec![],
                actions: vec![],
            }],
            hooks: HookDefinition::default(),
        }
    }

    #[test]
    fn test_workflow_definition_construction() {
        let wf = sample_workflow();
        assert_eq!(wf.name, "document-approval");
        assert_eq!(wf.version, 1);
        assert_eq!(wf.states.len(), 3);
        assert_eq!(wf.transitions.len(), 2);
        assert_eq!(wf.triggers.len(), 1);
    }

    #[test]
    fn test_workflow_definition_display() {
        let wf = sample_workflow();
        let display = format!("{}", wf);
        assert_eq!(display, "document-approval@v1 (3 states, 2 transitions)");
    }

    #[test]
    fn test_workflow_definition_serde_roundtrip() {
        let wf = sample_workflow();
        let json = serde_json::to_string(&wf).unwrap();
        let deserialized: WorkflowDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(wf, deserialized);
    }

    #[test]
    fn test_state_type_display() {
        assert_eq!(StateType::Initial.to_string(), "initial");
        assert_eq!(StateType::Active.to_string(), "active");
        assert_eq!(StateType::Terminal.to_string(), "terminal");
    }

    #[test]
    fn test_instance_status_display() {
        assert_eq!(InstanceStatus::Active.to_string(), "active");
        assert_eq!(InstanceStatus::Completed.to_string(), "completed");
        assert_eq!(InstanceStatus::Failed.to_string(), "failed");
        assert_eq!(InstanceStatus::Suspended.to_string(), "suspended");
        assert_eq!(InstanceStatus::TimedOut.to_string(), "timed_out");
    }

    #[test]
    fn test_timer_status_display() {
        assert_eq!(TimerStatus::Pending.to_string(), "pending");
        assert_eq!(TimerStatus::Fired.to_string(), "fired");
        assert_eq!(TimerStatus::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_criticality_level_display() {
        assert_eq!(CriticalityLevel::Low.to_string(), "LOW");
        assert_eq!(CriticalityLevel::Normal.to_string(), "NORMAL");
        assert_eq!(CriticalityLevel::High.to_string(), "HIGH");
        assert_eq!(CriticalityLevel::Critical.to_string(), "CRITICAL");
    }

    #[test]
    fn test_instance_status_serde_roundtrip() {
        for status in [
            InstanceStatus::Active,
            InstanceStatus::Completed,
            InstanceStatus::Failed,
            InstanceStatus::Suspended,
            InstanceStatus::TimedOut,
        ] {
            let json = serde_json::to_string(&status).unwrap();
            let back: InstanceStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn test_workflow_context_get_set() {
        let mut ctx = WorkflowContext::empty();
        assert!(!ctx.has("name"));

        ctx.set("name", "Alice");
        assert!(ctx.has("name"));
        assert_eq!(ctx.get::<String>("name"), Some("Alice".to_string()));
    }

    #[test]
    fn test_workflow_context_remove() {
        let mut ctx = WorkflowContext::empty();
        ctx.set("temp", 42);
        assert!(ctx.has("temp"));

        let removed = ctx.remove("temp");
        assert!(removed.is_some());
        assert!(!ctx.has("temp"));
    }

    #[test]
    fn test_workflow_context_typed_access() {
        let mut ctx = WorkflowContext::empty();
        ctx.set("count", 42_i64);
        ctx.set("active", true);
        ctx.set("tags", vec!["a", "b", "c"]);

        assert_eq!(ctx.get::<i64>("count"), Some(42));
        assert_eq!(ctx.get::<bool>("active"), Some(true));
        assert_eq!(
            ctx.get::<Vec<String>>("tags"),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );

        // Type mismatch returns None
        assert_eq!(ctx.get::<bool>("count"), None);
    }

    #[test]
    fn test_workflow_context_from_json() {
        let json = serde_json::json!({
            "document_id": "doc-123",
            "author_id": "user-456"
        });
        let ctx = WorkflowContext::new(json);
        assert_eq!(
            ctx.get::<String>("document_id"),
            Some("doc-123".to_string())
        );
        assert!(!ctx.has("nonexistent"));
    }

    #[test]
    fn test_transition_record_serde_roundtrip() {
        let record = TransitionRecord {
            transition_name: "approve".to_string(),
            from_state: "in_review".to_string(),
            to_state: "approved".to_string(),
            actor_id: "user-123".to_string(),
            timestamp: Utc::now(),
            node_id: "node-abc".to_string(),
            audit_event_id: Uuid::new_v4(),
            context_diff: serde_json::json!({"status": "approved"}),
        };
        let json = serde_json::to_string(&record).unwrap();
        let back: TransitionRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(record, back);
    }

    #[test]
    fn test_active_timer_serde_roundtrip() {
        let timer = ActiveTimer {
            timer_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            trigger_name: "review_deadline".to_string(),
            transition: "escalate".to_string(),
            fire_at: Utc::now() + chrono::Duration::hours(48),
            created_at: Utc::now(),
            created_by_node: "node-xyz".to_string(),
            status: TimerStatus::Pending,
        };
        let json = serde_json::to_string(&timer).unwrap();
        let back: ActiveTimer = serde_json::from_str(&json).unwrap();
        assert_eq!(timer, back);
    }

    #[test]
    fn test_action_result_construction() {
        let result = ActionResult {
            success: true,
            modified_context: Some(serde_json::json!({"approved": true})),
            side_effects: vec!["sent notification".to_string()],
        };
        assert!(result.success);
        assert!(result.modified_context.is_some());
        assert_eq!(result.side_effects.len(), 1);
    }

    #[test]
    fn test_actor_info_serde_roundtrip() {
        let actor = ActorInfo {
            actor_id: "user-789".to_string(),
            roles: vec!["reviewer".to_string(), "manager".to_string()],
            metadata: HashMap::from([("department".to_string(), "engineering".to_string())]),
        };
        let json = serde_json::to_string(&actor).unwrap();
        let back: ActorInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(actor, back);
    }

    #[test]
    fn test_hook_definition_defaults() {
        let hooks = HookDefinition::default();
        assert!(hooks.on_instance_created.is_empty());
        assert!(hooks.on_instance_completed.is_empty());
        assert!(hooks.on_instance_failed.is_empty());
        assert!(hooks.on_transition_blocked.is_empty());
        assert!(hooks.on_deadlock_detected.is_empty());
    }
}
