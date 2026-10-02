// Marabunta - Licensed under the MIT License.
//! Built-in workflow actions (W3B).
//!
//! Provides 12 action implementations for workflow state machine execution:
//! notification (notify), actor assignment (assign_actor), document locking
//! (lock_document, unlock_document), versioning (snapshot_version,
//! archive_version), context manipulation (set_context), comments
//! (auto_comment), webhook integration (http_webhook), sub-workflow
//! spawning (spawn_subworkflow), delayed execution (delay), and
//! parallel gate synchronization (gate).

use std::collections::HashMap;
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Sha256, Digest};
use uuid::Uuid;

use super::types::{ActionResult, ActiveTimer, TimerStatus, WorkflowContext, WorkflowInstance};

// ============================================================================
// Helper Functions
// ============================================================================

/// Replace `{{field}}` and `{field}` placeholders in a template string with
/// values from the workflow context. Supports nested dot notation:
/// `{author.name}` traverses `context["author"]["name"]`.
///
/// Unresolved placeholders are left as-is.
fn interpolate_template(template: &str, context: &WorkflowContext) -> String {
    let ctx_value = context.as_value();
    // First pass: handle {{field}} double-brace placeholders
    let mut result = template.to_string();
    let mut search_from = 0;
    loop {
        let remaining = &result[search_from..];
        let start_rel = match remaining.find("{{") {
            Some(pos) => pos,
            None => break,
        };
        let start = search_from + start_rel;
        let after_braces = &result[start + 2..];
        let end_rel = match after_braces.find("}}") {
            Some(pos) => pos,
            None => break,
        };
        let end = start + 2 + end_rel;
        let field_path = &result[start + 2..end];
        let value = field_path
            .split('.')
            .fold(Some(ctx_value), |acc, key| acc.and_then(|v| v.get(key)));
        if let Some(val) = value {
            let replacement = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let before = &result[..start];
            let after = &result[end + 2..];
            let new_result = format!("{}{}{}", before, replacement, after);
            search_from = start + replacement.len();
            result = new_result;
        } else {
            search_from = end + 2;
        }
    }

    // Second pass: handle {field} single-brace placeholders
    search_from = 0;
    loop {
        let remaining = &result[search_from..];
        let start_rel = match remaining.find('{') {
            Some(pos) => pos,
            None => break,
        };
        let start = search_from + start_rel;
        // Skip if this is a double-brace (already handled or leftover)
        if result[start..].starts_with("{{") {
            search_from = start + 2;
            continue;
        }
        let after_brace = &result[start..];
        let end_rel = match after_brace.find('}') {
            Some(pos) => pos,
            None => break,
        };
        let end = start + end_rel;
        let field_path = &result[start + 1..end];
        let value = field_path
            .split('.')
            .fold(Some(ctx_value), |acc, key| acc.and_then(|v| v.get(key)));
        if let Some(val) = value {
            let replacement = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let before = &result[..start];
            let after = &result[end + 1..];
            let new_result = format!("{}{}{}", before, replacement, after);
            search_from = start + replacement.len();
            result = new_result;
        } else {
            search_from = end + 1;
        }
    }

    result
}

/// Get the current UTC time as milliseconds since UNIX epoch.
fn current_time_ms() -> u64 {
    Utc::now().timestamp_millis() as u64
}

// ============================================================================
// WorkflowAction Trait
// ============================================================================

/// Async trait for built-in workflow actions. Each action receives a mutable
/// workflow context, action parameters, and a reference to the instance.
///
/// Actions return an `ActionResult` on success. They may optionally implement
/// `compensate` for saga-style rollback.
#[async_trait]
pub trait WorkflowAction: Send + Sync {
    /// The unique type identifier for this action (e.g., "notify", "lock_document").
    fn action_type(&self) -> &str;

    /// Execute the action, potentially mutating the workflow context.
    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult>;

    /// Optional compensating action for saga-style rollback.
    /// Default implementation is a no-op.
    async fn compensate(
        &self,
        _ctx: &mut WorkflowContext,
        _params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Return a description of this action for documentation and introspection.
    fn describe(&self) -> ActionDescription;
}

// ============================================================================
// Supporting Types
// ============================================================================

/// Human-readable description of a workflow action for documentation and UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionDescription {
    /// The action type identifier.
    pub action_type: String,
    /// Short human-readable summary of what the action does.
    pub summary: String,
    /// JSON schema describing the expected parameters.
    pub params_schema: Value,
    /// Whether this action supports compensation (rollback).
    pub compensatable: bool,
}

/// Channel through which notifications are delivered.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum NotifyChannel {
    Email,
    Webhook,
    #[default]
    SwarmMessage,
}


/// Strategy for assigning actors to workflow roles.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum AssignStrategy {
    RoundRobin,
    LeastLoaded,
    #[default]
    Specific,
    Pool,
}


/// Whether to wait for a sub-workflow or fire and forget.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum WaitMode {
    Wait,
    #[default]
    FireAndForget,
}


/// State of a synchronization gate for N-of-M parallel completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateState {
    /// Unique identifier for this gate.
    pub gate_id: String,
    /// Total number of participants.
    pub total: usize,
    /// Number of completions required to open the gate.
    pub required: usize,
    /// IDs of participants that have completed.
    pub completed: Vec<String>,
    /// IDs of participants that are still pending.
    pub pending: Vec<String>,
    /// When the gate was created (millis since epoch).
    pub created_at_ms: u64,
    /// Optional timeout for the gate (millis since epoch).
    pub timeout_at_ms: Option<u64>,
}

/// A resource lock held by a workflow instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockRecord {
    /// The resource being locked.
    pub resource_id: String,
    /// The workflow instance holding the lock.
    pub instance_id: String,
    /// The node that acquired the lock.
    pub node_id: String,
    /// When the lock was acquired (millis since epoch).
    pub acquired_at_ms: u64,
    /// Time-to-live in seconds. Lock expires after this duration.
    pub ttl_secs: u64,
    /// Unique token for lock ownership verification.
    pub lock_token: String,
}

/// Link back to the parent workflow that spawned a sub-workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentLink {
    /// Instance ID of the parent workflow.
    pub parent_instance_id: String,
    /// The state in the parent workflow that triggered the spawn.
    pub originating_state: String,
}

// ============================================================================
// 1. NotifyAction — Send notifications
// ============================================================================

/// Record a notification intent in the workflow context.
/// Does not actually send messages; the runtime layer handles delivery.
///
/// Params:
/// - `channel`: "email" | "webhook" | "swarm_message" (default: swarm_message)
/// - `to`: recipient identifier (supports context interpolation)
/// - `subject`: notification subject (supports context interpolation)
/// - `body`: notification body (supports context interpolation)
pub struct NotifyAction;

#[async_trait]
impl WorkflowAction for NotifyAction {
    fn action_type(&self) -> &str {
        "notify"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let channel: NotifyChannel = params
            .get("channel")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let to_raw = params
            .get("to")
            .and_then(|v| v.as_str())
            .unwrap_or("unspecified");
        let subject_raw = params
            .get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let body_raw = params
            .get("body")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let to = interpolate_template(to_raw, ctx);
        let subject = interpolate_template(subject_raw, ctx);
        let body = interpolate_template(body_raw, ctx);

        let notification = json!({
            "channel": serde_json::to_value(&channel).unwrap_or(Value::Null),
            "to": to,
            "subject": subject,
            "body": body,
            "sent_at_ms": current_time_ms(),
        });

        ctx.set("_last_notification", &notification);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "_last_notification": notification })),
            side_effects: vec![format!("notification sent to {}", to)],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "notify".to_string(),
            summary: "Send a notification via the specified channel".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "channel": { "type": "string", "enum": ["email", "webhook", "swarm_message"] },
                    "to": { "type": "string" },
                    "subject": { "type": "string" },
                    "body": { "type": "string" }
                },
                "required": ["to"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 2. AssignActorAction — Assign actors to workflow roles
// ============================================================================

/// Assign one or more actors to a role in the workflow context.
///
/// Params:
/// - `role`: the role name to assign (e.g., "reviewer")
/// - `strategy`: "round_robin" | "least_loaded" | "specific" | "pool"
/// - `actors`: array of actor IDs (required for "specific" and "pool")
/// - `counter_key`: context key for round-robin counter (default: "_rr_{role}")
pub struct AssignActorAction;

#[async_trait]
impl WorkflowAction for AssignActorAction {
    fn action_type(&self) -> &str {
        "assign_actor"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let role = params
            .get("role")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("assign_actor: missing 'role' param"))?;

        let strategy: AssignStrategy = params
            .get("strategy")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let actors: Vec<String> = params
            .get("actors")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let assigned = match strategy {
            AssignStrategy::RoundRobin => {
                if actors.is_empty() {
                    return Err(anyhow::anyhow!(
                        "assign_actor: round_robin requires non-empty 'actors' array"
                    ));
                }
                let counter_key = format!("_rr_{}", role);
                let counter: u64 = ctx.get::<u64>(&counter_key).unwrap_or(0);
                let idx = (counter as usize) % actors.len();
                let selected = actors[idx].clone();
                ctx.set(&counter_key, counter + 1);
                vec![selected]
            }
            AssignStrategy::LeastLoaded => {
                // In a real system this would query load metrics.
                // For now, pick the first actor as a placeholder.
                if actors.is_empty() {
                    return Err(anyhow::anyhow!(
                        "assign_actor: least_loaded requires non-empty 'actors' array"
                    ));
                }
                vec![actors[0].clone()]
            }
            AssignStrategy::Specific => {
                if actors.is_empty() {
                    return Err(anyhow::anyhow!(
                        "assign_actor: specific requires non-empty 'actors' array"
                    ));
                }
                actors
            }
            AssignStrategy::Pool => {
                // Pool assigns all provided actors.
                actors
            }
        };

        // Store assignment under roles.{role_name} in context.
        let _roles_key = format!("roles.{}", role);
        let mut roles_obj: Value = ctx
            .get::<Value>("roles")
            .unwrap_or_else(|| json!({}));
        if let Value::Object(ref mut map) = roles_obj {
            map.insert(
                role.to_string(),
                serde_json::to_value(&assigned).unwrap_or(Value::Null),
            );
        }
        ctx.set("roles", &roles_obj);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "roles": roles_obj })),
            side_effects: vec![format!(
                "assigned {} to role '{}'",
                assigned.join(", "),
                role
            )],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "assign_actor".to_string(),
            summary: "Assign one or more actors to a workflow role".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "role": { "type": "string" },
                    "strategy": { "type": "string", "enum": ["round_robin", "least_loaded", "specific", "pool"] },
                    "actors": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["role"]
            }),
            compensatable: true,
        }
    }

    async fn compensate(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<()> {
        if let Some(role) = params.get("role").and_then(|v| v.as_str()) {
            let mut roles_obj: Value = ctx
                .get::<Value>("roles")
                .unwrap_or_else(|| json!({}));
            if let Value::Object(ref mut map) = roles_obj {
                map.remove(role);
            }
            ctx.set("roles", &roles_obj);
        }
        Ok(())
    }
}

// ============================================================================
// 3. LockDocumentAction — Acquire a resource lock
// ============================================================================

/// Acquire a lock on a named resource. If the resource is already locked
/// by a different instance, the action fails.
///
/// Params:
/// - `resource_id`: the resource to lock
/// - `ttl_secs`: time-to-live in seconds (default: 300)
/// - `node_id`: the acquiring node (default: "local")
pub struct LockDocumentAction;

#[async_trait]
impl WorkflowAction for LockDocumentAction {
    fn action_type(&self) -> &str {
        "lock_document"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let resource_id = params
            .get("resource_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("lock_document: missing 'resource_id' param"))?;

        let ttl_secs = params
            .get("ttl_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);

        let node_id = params
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or("local")
            .to_string();

        // Check for existing lock.
        let locks_key = "_locks";
        let mut locks: Value = ctx
            .get::<Value>(locks_key)
            .unwrap_or_else(|| json!({}));

        if let Some(existing) = locks.get(resource_id) {
            // Check if the existing lock belongs to a different instance.
            if let Some(existing_instance) = existing.get("instance_id").and_then(|v| v.as_str()) {
                let this_instance = instance.instance_id.to_string();
                if existing_instance != this_instance {
                    // Check if the existing lock has expired.
                    let acquired_at = existing
                        .get("acquired_at_ms")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let existing_ttl = existing
                        .get("ttl_secs")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let now = current_time_ms();
                    if now < acquired_at + (existing_ttl * 1000) {
                        return Ok(ActionResult {
                            success: false,
                            modified_context: None,
                            side_effects: vec![format!(
                                "lock conflict: resource '{}' held by instance {}",
                                resource_id, existing_instance
                            )],
                        });
                    }
                    // Lock has expired, allow override.
                }
            }
        }

        let lock_token = Uuid::new_v4().to_string();
        let lock_record = LockRecord {
            resource_id: resource_id.to_string(),
            instance_id: instance.instance_id.to_string(),
            node_id,
            acquired_at_ms: current_time_ms(),
            ttl_secs,
            lock_token: lock_token.clone(),
        };

        if let Value::Object(ref mut map) = locks {
            map.insert(
                resource_id.to_string(),
                serde_json::to_value(&lock_record).unwrap_or(Value::Null),
            );
        }
        ctx.set(locks_key, &locks);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "_locks": locks })),
            side_effects: vec![format!(
                "acquired lock on '{}' (token: {})",
                resource_id, lock_token
            )],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "lock_document".to_string(),
            summary: "Acquire a resource lock with TTL and conflict detection".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "resource_id": { "type": "string" },
                    "ttl_secs": { "type": "integer", "default": 300 },
                    "node_id": { "type": "string" }
                },
                "required": ["resource_id"]
            }),
            compensatable: true,
        }
    }

    async fn compensate(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<()> {
        if let Some(resource_id) = params.get("resource_id").and_then(|v| v.as_str()) {
            let locks_key = "_locks";
            let mut locks: Value = ctx
                .get::<Value>(locks_key)
                .unwrap_or_else(|| json!({}));
            if let Value::Object(ref mut map) = locks {
                map.remove(resource_id);
            }
            ctx.set(locks_key, &locks);
        }
        Ok(())
    }
}

// ============================================================================
// 4. UnlockDocumentAction — Release a resource lock
// ============================================================================

/// Release a lock on a named resource.
///
/// Params:
/// - `resource_id`: the resource to unlock
/// - `lock_token`: optional token for ownership verification
pub struct UnlockDocumentAction;

#[async_trait]
impl WorkflowAction for UnlockDocumentAction {
    fn action_type(&self) -> &str {
        "unlock_document"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let resource_id = params
            .get("resource_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("unlock_document: missing 'resource_id' param"))?;

        let locks_key = "_locks";
        let mut locks: Value = ctx
            .get::<Value>(locks_key)
            .unwrap_or_else(|| json!({}));

        // Verify ownership before unlocking.
        if let Some(existing) = locks.get(resource_id) {
            if let Some(existing_instance) = existing.get("instance_id").and_then(|v| v.as_str()) {
                let this_instance = instance.instance_id.to_string();
                if existing_instance != this_instance {
                    return Ok(ActionResult {
                        success: false,
                        modified_context: None,
                        side_effects: vec![format!(
                            "cannot unlock '{}': owned by instance {}",
                            resource_id, existing_instance
                        )],
                    });
                }
            }
            // Optional token verification.
            if let Some(expected_token) = params.get("lock_token").and_then(|v| v.as_str()) {
                if let Some(actual_token) =
                    existing.get("lock_token").and_then(|v| v.as_str())
                {
                    if expected_token != actual_token {
                        return Ok(ActionResult {
                            success: false,
                            modified_context: None,
                            side_effects: vec![format!(
                                "cannot unlock '{}': token mismatch",
                                resource_id
                            )],
                        });
                    }
                }
            }
        }

        if let Value::Object(ref mut map) = locks {
            map.remove(resource_id);
        }
        ctx.set(locks_key, &locks);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "_locks": locks })),
            side_effects: vec![format!("released lock on '{}'", resource_id)],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "unlock_document".to_string(),
            summary: "Release a resource lock with optional token verification".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "resource_id": { "type": "string" },
                    "lock_token": { "type": "string" }
                },
                "required": ["resource_id"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 5. SnapshotVersionAction — Create a content version snapshot
// ============================================================================

/// Compute a SHA-256 hash of a context field and store a version record
/// under `_versions` in the context.
///
/// Params:
/// - `source_key`: the context field to snapshot
/// - `label`: optional human-readable version label
pub struct SnapshotVersionAction;

#[async_trait]
impl WorkflowAction for SnapshotVersionAction {
    fn action_type(&self) -> &str {
        "snapshot_version"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let source_key = params
            .get("source_key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("snapshot_version: missing 'source_key' param"))?;

        let label = params
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or("snapshot");

        let source_data = ctx
            .get::<Value>(source_key)
            .unwrap_or(Value::Null);

        // Compute SHA-256 of the serialized data.
        let serialized = serde_json::to_string(&source_data).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(serialized.as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        let version_id = Uuid::new_v4().to_string();
        let version_record = json!({
            "version_id": version_id,
            "source_key": source_key,
            "hash": hash,
            "label": label,
            "created_at_ms": current_time_ms(),
            "archived": false,
            "data": source_data,
        });

        let mut versions: Vec<Value> = ctx
            .get::<Vec<Value>>("_versions")
            .unwrap_or_default();
        versions.push(version_record.clone());
        ctx.set("_versions", &versions);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "_versions": versions })),
            side_effects: vec![format!(
                "snapshot version '{}' created (hash: {})",
                label,
                &hash[..12]
            )],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "snapshot_version".to_string(),
            summary: "Create a SHA-256 versioned snapshot of a context field".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "source_key": { "type": "string" },
                    "label": { "type": "string" }
                },
                "required": ["source_key"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 6. ArchiveVersionAction — Mark a version as archived/immutable
// ============================================================================

/// Mark a specific version as archived. Archived versions cannot be modified
/// but remain in the version history for audit purposes.
///
/// Params:
/// - `version_id`: the version to archive
pub struct ArchiveVersionAction;

#[async_trait]
impl WorkflowAction for ArchiveVersionAction {
    fn action_type(&self) -> &str {
        "archive_version"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let version_id = params
            .get("version_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("archive_version: missing 'version_id' param"))?;

        let mut versions: Vec<Value> = ctx
            .get::<Vec<Value>>("_versions")
            .unwrap_or_default();

        let mut found = false;
        for version in &mut versions {
            if let Some(vid) = version.get("version_id").and_then(|v| v.as_str()) {
                if vid == version_id {
                    // Check if already archived.
                    if version
                        .get("archived")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    {
                        return Ok(ActionResult {
                            success: false,
                            modified_context: None,
                            side_effects: vec![format!(
                                "version '{}' is already archived (immutable)",
                                version_id
                            )],
                        });
                    }
                    if let Value::Object(ref mut map) = version {
                        map.insert("archived".to_string(), json!(true));
                        map.insert("archived_at_ms".to_string(), json!(current_time_ms()));
                    }
                    found = true;
                    break;
                }
            }
        }

        if !found {
            return Err(anyhow::anyhow!(
                "archive_version: version '{}' not found",
                version_id
            ));
        }

        ctx.set("_versions", &versions);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "_versions": versions })),
            side_effects: vec![format!("version '{}' archived", version_id)],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "archive_version".to_string(),
            summary: "Mark a version snapshot as archived and immutable".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "version_id": { "type": "string" }
                },
                "required": ["version_id"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 7. SetContextAction — Manipulate workflow context fields
// ============================================================================

/// Set, merge, or delete fields in the workflow context.
///
/// Params:
/// - `operation`: "set" | "merge" | "delete" (default: "set")
/// - `fields`: object of key-value pairs (for set/merge)
/// - `keys`: array of keys to remove (for delete)
pub struct SetContextAction;

#[async_trait]
impl WorkflowAction for SetContextAction {
    fn action_type(&self) -> &str {
        "set_context"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let operation = params
            .get("operation")
            .and_then(|v| v.as_str())
            .unwrap_or("set");

        let mut side_effects = Vec::new();

        match operation {
            "set" => {
                let fields = params
                    .get("fields")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| {
                        anyhow::anyhow!("set_context: 'set' operation requires 'fields' object")
                    })?;
                for (key, value) in fields {
                    ctx.set(key, value);
                    side_effects.push(format!("set context.{}", key));
                }
            }
            "merge" => {
                let fields = params
                    .get("fields")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "set_context: 'merge' operation requires 'fields' object"
                        )
                    })?;
                for (key, value) in fields {
                    // For objects, deep-merge. For other types, overwrite.
                    let existing = ctx.get::<Value>(key);
                    let merged = match (existing, value) {
                        (Some(Value::Object(mut existing_map)), Value::Object(new_map)) => {
                            for (k, v) in new_map {
                                existing_map.insert(k.clone(), v.clone());
                            }
                            Value::Object(existing_map)
                        }
                        _ => value.clone(),
                    };
                    ctx.set(key, &merged);
                    side_effects.push(format!("merged context.{}", key));
                }
            }
            "delete" => {
                let keys: Vec<String> = params
                    .get("keys")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "set_context: 'delete' operation requires 'keys' array"
                        )
                    })?;
                for key in &keys {
                    ctx.remove(key);
                    side_effects.push(format!("deleted context.{}", key));
                }
            }
            other => {
                return Err(anyhow::anyhow!(
                    "set_context: unknown operation '{}'",
                    other
                ));
            }
        }

        Ok(ActionResult {
            success: true,
            modified_context: Some(ctx.as_value().clone()),
            side_effects,
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "set_context".to_string(),
            summary: "Set, merge, or delete fields in the workflow context".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "operation": { "type": "string", "enum": ["set", "merge", "delete"] },
                    "fields": { "type": "object" },
                    "keys": { "type": "array", "items": { "type": "string" } }
                }
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 8. AutoCommentAction — Append a comment to the workflow context
// ============================================================================

/// Append a timestamped comment to the `_comments` array in context.
/// Supports context interpolation in the message text.
///
/// Params:
/// - `message`: the comment text (supports interpolation)
/// - `author`: comment author (default: "system")
pub struct AutoCommentAction;

#[async_trait]
impl WorkflowAction for AutoCommentAction {
    fn action_type(&self) -> &str {
        "auto_comment"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let message_raw = params
            .get("message")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("auto_comment: missing 'message' param"))?;

        let author = params
            .get("author")
            .and_then(|v| v.as_str())
            .unwrap_or("system");

        let message = interpolate_template(message_raw, ctx);

        let comment = json!({
            "id": Uuid::new_v4().to_string(),
            "author": author,
            "message": message,
            "created_at_ms": current_time_ms(),
        });

        let mut comments: Vec<Value> = ctx
            .get::<Vec<Value>>("_comments")
            .unwrap_or_default();
        comments.push(comment.clone());
        ctx.set("_comments", &comments);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "_comments": comments })),
            side_effects: vec![format!("comment added by {}", author)],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "auto_comment".to_string(),
            summary: "Append a timestamped comment to the workflow context".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string" },
                    "author": { "type": "string" }
                },
                "required": ["message"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 9. HttpWebhookAction — Record a webhook call intent
// ============================================================================

/// Record an HTTP webhook intent in the workflow context. The actual HTTP
/// request is not made here; the runtime delivery layer handles dispatch.
///
/// Params:
/// - `url`: the webhook URL (supports interpolation)
/// - `method`: HTTP method (default: "POST")
/// - `headers`: optional object of header key-value pairs
/// - `body_template`: optional body template (supports interpolation)
pub struct HttpWebhookAction;

#[async_trait]
impl WorkflowAction for HttpWebhookAction {
    fn action_type(&self) -> &str {
        "http_webhook"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let url_raw = params
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("http_webhook: missing 'url' param"))?;

        let method = params
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("POST");

        let headers: Value = params
            .get("headers")
            .cloned()
            .unwrap_or_else(|| json!({}));

        let body_template = params
            .get("body_template")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let url = interpolate_template(url_raw, ctx);
        let body = interpolate_template(body_template, ctx);

        let webhook_record = json!({
            "request_id": Uuid::new_v4().to_string(),
            "url": url,
            "method": method,
            "headers": headers,
            "body": body,
            "instance_id": instance.instance_id.to_string(),
            "created_at_ms": current_time_ms(),
            "status": "pending",
        });

        ctx.set("_last_webhook", &webhook_record);

        // Also append to a webhook history array.
        let mut webhook_history: Vec<Value> = ctx
            .get::<Vec<Value>>("_webhook_history")
            .unwrap_or_default();
        webhook_history.push(webhook_record.clone());
        ctx.set("_webhook_history", &webhook_history);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({
                "_last_webhook": webhook_record,
                "_webhook_history": webhook_history,
            })),
            side_effects: vec![format!("webhook {} {} queued", method, url)],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "http_webhook".to_string(),
            summary: "Record an HTTP webhook call intent for deferred delivery".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "method": { "type": "string", "default": "POST" },
                    "headers": { "type": "object" },
                    "body_template": { "type": "string" }
                },
                "required": ["url"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 10. SpawnSubworkflowAction — Launch a child workflow
// ============================================================================

/// Record a sub-workflow spawn intent in the workflow context. The actual
/// child creation is handled by the runtime orchestrator.
///
/// Params:
/// - `workflow_name`: name of the child workflow to spawn
/// - `workflow_version`: optional version (default: latest)
/// - `input`: context to pass to the child
/// - `wait_mode`: "wait" | "fire_and_forget" (default: fire_and_forget)
/// - `callback_state`: state to transition to when child completes (for wait mode)
pub struct SpawnSubworkflowAction;

#[async_trait]
impl WorkflowAction for SpawnSubworkflowAction {
    fn action_type(&self) -> &str {
        "spawn_subworkflow"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let workflow_name = params
            .get("workflow_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("spawn_subworkflow: missing 'workflow_name' param")
            })?;

        let workflow_version = params
            .get("workflow_version")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        let input = params
            .get("input")
            .cloned()
            .unwrap_or_else(|| json!({}));

        let wait_mode: WaitMode = params
            .get("wait_mode")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        let callback_state = params
            .get("callback_state")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let child_id = Uuid::new_v4().to_string();

        let spawn_record = json!({
            "child_instance_id": child_id,
            "workflow_name": workflow_name,
            "workflow_version": workflow_version,
            "input": input,
            "wait_mode": serde_json::to_value(&wait_mode).unwrap_or(Value::Null),
            "callback_state": callback_state,
            "parent_instance_id": instance.instance_id.to_string(),
            "parent_state": instance.current_state.clone(),
            "created_at_ms": current_time_ms(),
            "status": "pending",
        });

        let mut children: Vec<Value> = ctx
            .get::<Vec<Value>>("_children")
            .unwrap_or_default();
        children.push(spawn_record.clone());
        ctx.set("_children", &children);

        // Store the parent link for the child to discover.
        let _parent_link = ParentLink {
            parent_instance_id: instance.instance_id.to_string(),
            originating_state: instance.current_state.clone(),
        };
        ctx.set("_last_spawn", &spawn_record);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({
                "_children": children,
                "_last_spawn": spawn_record,
            })),
            side_effects: vec![format!(
                "spawned child workflow '{}' (id: {})",
                workflow_name, child_id
            )],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "spawn_subworkflow".to_string(),
            summary: "Spawn a child workflow instance with optional wait mode".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "workflow_name": { "type": "string" },
                    "workflow_version": { "type": "integer" },
                    "input": { "type": "object" },
                    "wait_mode": { "type": "string", "enum": ["wait", "fire_and_forget"] },
                    "callback_state": { "type": "string" }
                },
                "required": ["workflow_name"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 11. DelayAction — Create a timed delay via timer
// ============================================================================

/// Create a timer reference in the workflow context for delayed execution.
/// The runtime timer subsystem reads `_active_timers` and fires transitions
/// when the delay expires.
///
/// Params:
/// - `delay_secs`: number of seconds to delay
/// - `transition`: transition name to fire after delay
/// - `trigger_name`: name for the timer trigger (default: "delay_{transition}")
pub struct DelayAction;

#[async_trait]
impl WorkflowAction for DelayAction {
    fn action_type(&self) -> &str {
        "delay"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let delay_secs = params
            .get("delay_secs")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("delay: missing 'delay_secs' param"))?;

        let transition = params
            .get("transition")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("delay: missing 'transition' param"))?;

        let trigger_name = params
            .get("trigger_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("delay_{}", transition));

        let now = Utc::now();
        let fire_at = now + chrono::Duration::seconds(delay_secs as i64);

        let timer = ActiveTimer {
            timer_id: Uuid::new_v4(),
            instance_id: instance.instance_id,
            trigger_name: trigger_name.clone(),
            transition: transition.to_string(),
            fire_at,
            created_at: now,
            created_by_node: "local".to_string(),
            status: TimerStatus::Pending,
        };

        let timer_record = serde_json::to_value(&timer).unwrap_or(Value::Null);

        let mut active_timers: Vec<Value> = ctx
            .get::<Vec<Value>>("_active_timers")
            .unwrap_or_default();
        active_timers.push(timer_record.clone());
        ctx.set("_active_timers", &active_timers);

        Ok(ActionResult {
            success: true,
            modified_context: Some(json!({ "_active_timers": active_timers })),
            side_effects: vec![format!(
                "timer '{}' created: fires in {}s (transition: {})",
                trigger_name, delay_secs, transition
            )],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "delay".to_string(),
            summary: "Create a timed delay that fires a transition after N seconds".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "delay_secs": { "type": "integer" },
                    "transition": { "type": "string" },
                    "trigger_name": { "type": "string" }
                },
                "required": ["delay_secs", "transition"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// 12. GateAction — N-of-M synchronization gate
// ============================================================================

/// Initialize or advance a synchronization gate. Gates wait for N of M
/// participants to complete before allowing the workflow to proceed.
///
/// Params:
/// - `gate_id`: unique identifier for this gate
/// - `total`: total number of participants (M)
/// - `required`: number required to open the gate (N)
/// - `participant_id`: the completing participant (if advancing)
/// - `timeout_secs`: optional timeout in seconds
pub struct GateAction;

#[async_trait]
impl WorkflowAction for GateAction {
    fn action_type(&self) -> &str {
        "gate"
    }

    async fn execute(
        &self,
        ctx: &mut WorkflowContext,
        params: &Value,
        _instance: &WorkflowInstance,
    ) -> anyhow::Result<ActionResult> {
        let gate_id = params
            .get("gate_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("gate: missing 'gate_id' param"))?;

        let total = params
            .get("total")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("gate: missing 'total' param"))? as usize;

        let required = params
            .get("required")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("gate: missing 'required' param"))? as usize;

        let participant_id = params
            .get("participant_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let timeout_secs = params
            .get("timeout_secs")
            .and_then(|v| v.as_u64());

        let gates_key = "_gates";
        let mut gates: Value = ctx
            .get::<Value>(gates_key)
            .unwrap_or_else(|| json!({}));

        let now = current_time_ms();
        let timeout_at_ms = timeout_secs.map(|s| now + s * 1000);

        // Look for existing gate or create new one.
        let mut gate_state: GateState = if let Some(existing) = gates.get(gate_id) {
            serde_json::from_value(existing.clone()).unwrap_or_else(|_| GateState {
                gate_id: gate_id.to_string(),
                total,
                required,
                completed: Vec::new(),
                pending: (0..total).map(|i| format!("participant_{}", i)).collect(),
                created_at_ms: now,
                timeout_at_ms,
            })
        } else {
            GateState {
                gate_id: gate_id.to_string(),
                total,
                required,
                completed: Vec::new(),
                pending: (0..total).map(|i| format!("participant_{}", i)).collect(),
                created_at_ms: now,
                timeout_at_ms,
            }
        };

        // If a participant completed, record it.
        if let Some(pid) = participant_id {
            if !gate_state.completed.contains(&pid) {
                gate_state.completed.push(pid.clone());
                gate_state.pending.retain(|p| p != &pid);
            }
        }

        let gate_open = gate_state.completed.len() >= gate_state.required;

        if let Value::Object(ref mut map) = gates {
            map.insert(
                gate_id.to_string(),
                serde_json::to_value(&gate_state).unwrap_or(Value::Null),
            );
        }
        ctx.set(gates_key, &gates);

        // Record whether the gate is open.
        ctx.set(
            &format!("_gate_open_{}", gate_id),
            gate_open,
        );

        Ok(ActionResult {
            success: gate_open,
            modified_context: Some(json!({
                "_gates": gates,
                format!("_gate_open_{}", gate_id): gate_open,
            })),
            side_effects: vec![format!(
                "gate '{}': {}/{} completed (required: {}, open: {})",
                gate_id,
                gate_state.completed.len(),
                gate_state.total,
                gate_state.required,
                gate_open
            )],
        })
    }

    fn describe(&self) -> ActionDescription {
        ActionDescription {
            action_type: "gate".to_string(),
            summary: "N-of-M synchronization gate for parallel completion tracking".to_string(),
            params_schema: json!({
                "type": "object",
                "properties": {
                    "gate_id": { "type": "string" },
                    "total": { "type": "integer" },
                    "required": { "type": "integer" },
                    "participant_id": { "type": "string" },
                    "timeout_secs": { "type": "integer" }
                },
                "required": ["gate_id", "total", "required"]
            }),
            compensatable: false,
        }
    }
}

// ============================================================================
// ActionRegistry
// ============================================================================

/// Registry mapping action type names to implementations.
/// Used by the workflow engine to execute transition actions, on-enter/on-exit
/// hooks, and lifecycle event handlers.
pub struct ActionRegistry {
    actions: HashMap<String, Box<dyn WorkflowAction>>,
}

impl ActionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            actions: HashMap::new(),
        }
    }

    /// Register an action. The action's `action_type()` is used as the
    /// registry key. If an action with the same type already exists, it
    /// is replaced.
    pub fn register(&mut self, action: Box<dyn WorkflowAction>) {
        self.actions
            .insert(action.action_type().to_string(), action);
    }

    /// Look up an action by type name.
    pub fn get(&self, action_type: &str) -> Option<&dyn WorkflowAction> {
        self.actions.get(action_type).map(|a| a.as_ref())
    }

    /// Return the number of registered actions.
    pub fn len(&self) -> usize {
        self.actions.len()
    }

    /// Return true if no actions are registered.
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

/// Create an `ActionRegistry` pre-loaded with all 12 built-in actions.
pub fn register_builtins() -> ActionRegistry {
    let mut registry = ActionRegistry::new();
    registry.register(Box::new(NotifyAction));
    registry.register(Box::new(AssignActorAction));
    registry.register(Box::new(LockDocumentAction));
    registry.register(Box::new(UnlockDocumentAction));
    registry.register(Box::new(SnapshotVersionAction));
    registry.register(Box::new(ArchiveVersionAction));
    registry.register(Box::new(SetContextAction));
    registry.register(Box::new(AutoCommentAction));
    registry.register(Box::new(HttpWebhookAction));
    registry.register(Box::new(SpawnSubworkflowAction));
    registry.register(Box::new(DelayAction));
    registry.register(Box::new(GateAction));
    registry
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

    use crate::swarm::workflow::types::InstanceStatus;

    /// Build a minimal WorkflowInstance for testing.
    fn make_instance(state: &str, context: Value) -> WorkflowInstance {
        WorkflowInstance {
            instance_id: Uuid::new_v4(),
            workflow_name: "test-workflow".to_string(),
            workflow_version: 1,
            current_state: state.to_string(),
            context,
            history: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            created_by: "system".to_string(),
            status: InstanceStatus::Active,
            assigned_actors: HashMap::new(),
            timers: vec![],
            locked_by: None,
        }
    }

    // --- interpolation tests ---

    #[test]
    fn test_notify_template_interpolation() {
        let mut ctx = WorkflowContext::empty();
        ctx.set("user_name", "Alice");
        ctx.set("doc_title", "Q4 Report");

        let result = interpolate_template(
            "Hello {user_name}, your document '{doc_title}' needs review",
            &ctx,
        );
        assert_eq!(
            result,
            "Hello Alice, your document 'Q4 Report' needs review"
        );

        // Double-brace syntax
        let result2 = interpolate_template("Dear {{user_name}}", &ctx);
        assert_eq!(result2, "Dear Alice");
    }

    // --- assign_actor tests ---

    #[tokio::test]
    async fn test_assign_actor_round_robin() {
        let action = AssignActorAction;
        let instance = make_instance("draft", json!({}));
        let mut ctx = WorkflowContext::empty();

        let params = json!({
            "role": "reviewer",
            "strategy": "round_robin",
            "actors": ["alice", "bob", "carol"]
        });

        // First call: should assign alice (index 0).
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);
        let roles: Value = ctx.get("roles").unwrap();
        assert_eq!(roles["reviewer"], json!(["alice"]));

        // Second call: should assign bob (index 1).
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);
        let roles: Value = ctx.get("roles").unwrap();
        assert_eq!(roles["reviewer"], json!(["bob"]));

        // Third call: should assign carol (index 2).
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);
        let roles: Value = ctx.get("roles").unwrap();
        assert_eq!(roles["reviewer"], json!(["carol"]));

        // Fourth call: should wrap around to alice (index 0).
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);
        let roles: Value = ctx.get("roles").unwrap();
        assert_eq!(roles["reviewer"], json!(["alice"]));
    }

    #[tokio::test]
    async fn test_assign_actor_specific() {
        let action = AssignActorAction;
        let instance = make_instance("draft", json!({}));
        let mut ctx = WorkflowContext::empty();

        let params = json!({
            "role": "approver",
            "strategy": "specific",
            "actors": ["manager-1", "manager-2"]
        });

        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);
        let roles: Value = ctx.get("roles").unwrap();
        assert_eq!(roles["approver"], json!(["manager-1", "manager-2"]));
    }

    // --- lock/unlock tests ---

    #[tokio::test]
    async fn test_lock_unlock_roundtrip() {
        let lock_action = LockDocumentAction;
        let unlock_action = UnlockDocumentAction;
        let instance = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();

        // Acquire lock.
        let params = json!({ "resource_id": "doc-123", "ttl_secs": 60 });
        let result = lock_action
            .execute(&mut ctx, &params, &instance)
            .await
            .unwrap();
        assert!(result.success);

        // Verify lock exists.
        let locks: Value = ctx.get("_locks").unwrap();
        assert!(locks.get("doc-123").is_some());
        let lock_record = locks.get("doc-123").unwrap();
        assert_eq!(
            lock_record.get("instance_id").unwrap().as_str().unwrap(),
            instance.instance_id.to_string()
        );

        // Release lock.
        let params = json!({ "resource_id": "doc-123" });
        let result = unlock_action
            .execute(&mut ctx, &params, &instance)
            .await
            .unwrap();
        assert!(result.success);

        // Verify lock removed.
        let locks: Value = ctx.get("_locks").unwrap();
        assert!(locks.get("doc-123").is_none());
    }

    #[tokio::test]
    async fn test_lock_conflict() {
        let lock_action = LockDocumentAction;
        let instance_a = make_instance("active", json!({}));
        let instance_b = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();

        // Instance A acquires lock.
        let params = json!({ "resource_id": "doc-456", "ttl_secs": 3600 });
        let result = lock_action
            .execute(&mut ctx, &params, &instance_a)
            .await
            .unwrap();
        assert!(result.success);

        // Instance B tries to acquire the same lock.
        let result = lock_action
            .execute(&mut ctx, &params, &instance_b)
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.side_effects[0].contains("lock conflict"));
    }

    // --- snapshot_version tests ---

    #[tokio::test]
    async fn test_snapshot_version_creates_record() {
        let action = SnapshotVersionAction;
        let instance = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();
        ctx.set("document", json!({"title": "Test Doc", "body": "Hello world"}));

        let params = json!({ "source_key": "document", "label": "v1.0" });
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);

        let versions: Vec<Value> = ctx.get("_versions").unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0]["label"], "v1.0");
        assert_eq!(versions[0]["source_key"], "document");
        assert_eq!(versions[0]["archived"], false);
        // Hash should be a valid hex string.
        let hash = versions[0]["hash"].as_str().unwrap();
        assert_eq!(hash.len(), 64); // SHA-256 hex length
    }

    // --- archive_version tests ---

    #[tokio::test]
    async fn test_archive_version_immutable() {
        let snapshot_action = SnapshotVersionAction;
        let archive_action = ArchiveVersionAction;
        let instance = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();
        ctx.set("document", json!({"title": "Test"}));

        // Create a snapshot first.
        let params = json!({ "source_key": "document", "label": "v1" });
        snapshot_action
            .execute(&mut ctx, &params, &instance)
            .await
            .unwrap();

        let versions: Vec<Value> = ctx.get("_versions").unwrap();
        let version_id = versions[0]["version_id"].as_str().unwrap().to_string();

        // Archive the version.
        let params = json!({ "version_id": version_id });
        let result = archive_action
            .execute(&mut ctx, &params, &instance)
            .await
            .unwrap();
        assert!(result.success);

        // Verify it is archived.
        let versions: Vec<Value> = ctx.get("_versions").unwrap();
        assert_eq!(versions[0]["archived"], true);

        // Try to archive again: should fail (already archived).
        let result = archive_action
            .execute(&mut ctx, &params, &instance)
            .await
            .unwrap();
        assert!(!result.success);
        assert!(result.side_effects[0].contains("already archived"));
    }

    // --- set_context tests ---

    #[tokio::test]
    async fn test_set_context_merge() {
        let action = SetContextAction;
        let instance = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();
        ctx.set("metadata", json!({"author": "Alice", "version": 1}));

        let params = json!({
            "operation": "merge",
            "fields": {
                "metadata": { "reviewer": "Bob", "version": 2 }
            }
        });

        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);

        let metadata: Value = ctx.get("metadata").unwrap();
        // Merged: original author kept, new reviewer added, version overwritten.
        assert_eq!(metadata["author"], "Alice");
        assert_eq!(metadata["reviewer"], "Bob");
        assert_eq!(metadata["version"], 2);
    }

    #[tokio::test]
    async fn test_set_context_delete() {
        let action = SetContextAction;
        let instance = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();
        ctx.set("temp_data", "ephemeral");
        ctx.set("keep_data", "permanent");

        let params = json!({
            "operation": "delete",
            "keys": ["temp_data"]
        });

        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);
        assert!(!ctx.has("temp_data"));
        assert!(ctx.has("keep_data"));
    }

    // --- auto_comment tests ---

    #[tokio::test]
    async fn test_auto_comment_append() {
        let action = AutoCommentAction;
        let instance = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();
        ctx.set("reviewer_name", "Alice");

        // First comment.
        let params = json!({
            "message": "Review started by {reviewer_name}",
            "author": "system"
        });
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);

        // Second comment.
        let params = json!({
            "message": "All checks passed",
            "author": "ci-bot"
        });
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);

        let comments: Vec<Value> = ctx.get("_comments").unwrap();
        assert_eq!(comments.len(), 2);
        assert_eq!(comments[0]["message"], "Review started by Alice");
        assert_eq!(comments[0]["author"], "system");
        assert_eq!(comments[1]["message"], "All checks passed");
        assert_eq!(comments[1]["author"], "ci-bot");
    }

    // --- http_webhook tests ---

    #[tokio::test]
    async fn test_http_webhook_mock() {
        let action = HttpWebhookAction;
        let instance = make_instance("active", json!({}));
        let mut ctx = WorkflowContext::empty();
        ctx.set("callback_url", "https://example.com/hook");

        let params = json!({
            "url": "{callback_url}",
            "method": "POST",
            "headers": { "Authorization": "Bearer token123" },
            "body_template": "workflow completed"
        });

        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);

        let webhook: Value = ctx.get("_last_webhook").unwrap();
        assert_eq!(webhook["url"], "https://example.com/hook");
        assert_eq!(webhook["method"], "POST");
        assert_eq!(webhook["status"], "pending");
        assert_eq!(webhook["body"], "workflow completed");

        // Check history is populated.
        let history: Vec<Value> = ctx.get("_webhook_history").unwrap();
        assert_eq!(history.len(), 1);
    }

    // --- spawn_subworkflow tests ---

    #[tokio::test]
    async fn test_spawn_subworkflow_fire_and_forget() {
        let action = SpawnSubworkflowAction;
        let instance = make_instance("processing", json!({}));
        let mut ctx = WorkflowContext::empty();

        let params = json!({
            "workflow_name": "approval-chain",
            "input": { "doc_id": "doc-789" },
            "wait_mode": "fire_and_forget"
        });

        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);

        let children: Vec<Value> = ctx.get("_children").unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["workflow_name"], "approval-chain");
        assert_eq!(children[0]["status"], "pending");
        assert_eq!(children[0]["wait_mode"], "fire_and_forget");
        assert_eq!(
            children[0]["parent_instance_id"],
            instance.instance_id.to_string()
        );
        assert_eq!(children[0]["parent_state"], "processing");

        // Child ID should be a valid UUID.
        let child_id = children[0]["child_instance_id"].as_str().unwrap();
        assert!(Uuid::parse_str(child_id).is_ok());
    }

    // --- delay tests ---

    #[tokio::test]
    async fn test_delay_timer_creation() {
        let action = DelayAction;
        let instance = make_instance("waiting", json!({}));
        let mut ctx = WorkflowContext::empty();

        let params = json!({
            "delay_secs": 3600,
            "transition": "timeout_escalate",
            "trigger_name": "escalation_timer"
        });

        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        assert!(result.success);

        let timers: Vec<Value> = ctx.get("_active_timers").unwrap();
        assert_eq!(timers.len(), 1);
        assert_eq!(timers[0]["transition"], "timeout_escalate");
        assert_eq!(timers[0]["trigger_name"], "escalation_timer");
        assert_eq!(timers[0]["status"], "pending");
        assert_eq!(
            timers[0]["instance_id"],
            instance.instance_id.to_string()
        );

        // fire_at should be in the future.
        let fire_at_str = timers[0]["fire_at"].as_str().unwrap();
        let fire_at: chrono::DateTime<Utc> = fire_at_str.parse().unwrap();
        assert!(fire_at > Utc::now());
    }

    // --- gate tests ---

    #[tokio::test]
    async fn test_gate_n_of_m_immediate() {
        let action = GateAction;
        let instance = make_instance("parallel", json!({}));
        let mut ctx = WorkflowContext::empty();

        // Create a 2-of-3 gate and immediately satisfy it.
        let params = json!({
            "gate_id": "approval_gate",
            "total": 3,
            "required": 2,
            "participant_id": "reviewer-A"
        });
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        // 1 of 2 required: gate not yet open.
        assert!(!result.success);

        // Second participant completes.
        let params = json!({
            "gate_id": "approval_gate",
            "total": 3,
            "required": 2,
            "participant_id": "reviewer-B"
        });
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        // 2 of 2 required: gate is open.
        assert!(result.success);

        let gate_open: bool = ctx
            .get("_gate_open_approval_gate")
            .unwrap();
        assert!(gate_open);
    }

    #[tokio::test]
    async fn test_gate_n_of_m_pending() {
        let action = GateAction;
        let instance = make_instance("parallel", json!({}));
        let mut ctx = WorkflowContext::empty();

        // Initialize a 3-of-5 gate without any participant.
        let params = json!({
            "gate_id": "big_gate",
            "total": 5,
            "required": 3
        });
        let result = action.execute(&mut ctx, &params, &instance).await.unwrap();
        // No participants completed yet.
        assert!(!result.success);

        let gates: Value = ctx.get("_gates").unwrap();
        let gate: GateState =
            serde_json::from_value(gates["big_gate"].clone()).unwrap();
        assert_eq!(gate.total, 5);
        assert_eq!(gate.required, 3);
        assert_eq!(gate.completed.len(), 0);
        assert_eq!(gate.pending.len(), 5);
    }

    // --- registry tests ---

    #[test]
    fn test_registry_builtins_count() {
        let registry = register_builtins();
        assert_eq!(registry.len(), 12);
        assert!(!registry.is_empty());

        // Verify all 12 action types are registered.
        let expected = [
            "notify",
            "assign_actor",
            "lock_document",
            "unlock_document",
            "snapshot_version",
            "archive_version",
            "set_context",
            "auto_comment",
            "http_webhook",
            "spawn_subworkflow",
            "delay",
            "gate",
        ];
        for name in &expected {
            assert!(
                registry.get(name).is_some(),
                "Expected action '{}' to be registered",
                name
            );
        }

        // Unknown action returns None.
        assert!(registry.get("nonexistent_action").is_none());
    }
}
