// Marabunta - Licensed under the MIT License.
//! Built-in workflow conditions (W2D).
//!
//! Provides 14 condition implementations for workflow guard evaluation:
//! identity (role_is, actor_is), state (in_state), data inspection
//! (has_content, not_empty_field, field_equals, field_in, content_changed),
//! multi-party (min_approvers), temporal (time_elapsed), expression
//! evaluation (expression via evalexpr), and composite logic (all_of,
//! any_of, not).

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::types::{ActorInfo, TransitionRecord, WorkflowInstance};

// ============================================================================
// WorkflowCondition Trait
// ============================================================================

/// A guard condition that can be evaluated against a workflow instance and actor.
///
/// Conditions return `Ok(true)` when the guard is satisfied, `Ok(false)` when
/// the guard is not satisfied (normal — transition is blocked), and `Err` when
/// the evaluation itself fails (configuration or runtime error).
#[async_trait]
pub trait WorkflowCondition: Send + Sync {
    /// The unique type name for this condition (e.g., "role_is", "has_content").
    fn condition_type(&self) -> &str;

    /// Evaluate the condition against the given parameters, instance, and actor.
    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool>;

    /// Human-readable description of this condition for documentation/UI.
    fn describe(&self) -> String;
}

// ============================================================================
// Context Interpolation Utility
// ============================================================================

/// Replace `{field_name}` placeholders in a template string with values
/// from the workflow instance context. Supports nested dot notation:
/// `{author.name}` traverses `context["author"]["name"]`.
///
/// Unresolved placeholders are left as-is (not an error).
fn interpolate(template: &str, context: &serde_json::Value) -> String {
    let mut result = template.to_string();
    // We use a loop with find to locate each `{...}` placeholder.
    // We must be careful to avoid infinite loops when a placeholder
    // cannot be resolved — we skip past it by tracking a start offset.
    let mut search_from = 0;
    loop {
        let remaining = &result[search_from..];
        let start_rel = match remaining.find('{') {
            Some(pos) => pos,
            None => break,
        };
        let start = search_from + start_rel;

        let after_brace = &result[start..];
        let end_rel = match after_brace.find('}') {
            Some(pos) => pos,
            None => break,
        };
        let end = start + end_rel;

        let field_path = &result[start + 1..end];
        // Traverse dot-separated path through the JSON context
        let value = field_path
            .split('.')
            .fold(Some(context), |acc, key| acc.and_then(|v| v.get(key)));

        if let Some(val) = value {
            let replacement = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let before = &result[..start];
            let after = &result[end + 1..];
            let new_result = format!("{}{}{}", before, replacement, after);
            // After replacement, continue searching from the end of the
            // replaced text (in case template produces more placeholders
            // we should NOT re-expand them — that would be a security risk).
            search_from = before.len() + replacement.len();
            result = new_result;
        } else {
            // No match — skip past this placeholder to avoid infinite loop
            search_from = end + 1;
        }
    }
    result
}

// ============================================================================
// 1. RoleIsCondition — Identity
// ============================================================================

/// Check if the actor has a specified role.
/// Params: `{"role": "approver"}`
pub struct RoleIsCondition;

#[async_trait]
impl WorkflowCondition for RoleIsCondition {
    fn condition_type(&self) -> &str {
        "role_is"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        _instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool> {
        let role = params
            .get("role")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("role_is: missing 'role' param"))?;
        Ok(actor.roles.contains(&role.to_string()))
    }

    fn describe(&self) -> String {
        "Check if the actor has a specified role".to_string()
    }
}

// ============================================================================
// 2. ActorIsCondition — Identity (with interpolation)
// ============================================================================

/// Check if the actor's ID matches a specified value. The expected ID
/// supports context interpolation: `{author_id}` resolves from instance context.
/// Params: `{"actor_id": "{author_id}"}`
pub struct ActorIsCondition;

#[async_trait]
impl WorkflowCondition for ActorIsCondition {
    fn condition_type(&self) -> &str {
        "actor_is"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool> {
        let raw_id = params
            .get("actor_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("actor_is: missing 'actor_id' param"))?;
        let expected_id = interpolate(raw_id, &instance.context);
        Ok(actor.actor_id == expected_id)
    }

    fn describe(&self) -> String {
        "Check if actor matches a specific identity (supports context interpolation)".to_string()
    }
}

// ============================================================================
// 3. InStateCondition — State Predicate
// ============================================================================

/// Check if the workflow instance is in a specified state.
/// Params: `{"state": "pending_review"}`
pub struct InStateCondition;

#[async_trait]
impl WorkflowCondition for InStateCondition {
    fn condition_type(&self) -> &str {
        "in_state"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let state = params
            .get("state")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("in_state: missing 'state' param"))?;
        Ok(instance.current_state == state)
    }

    fn describe(&self) -> String {
        "Check if workflow instance is in the specified state".to_string()
    }
}

// ============================================================================
// 4. HasContentCondition — Data Inspection
// ============================================================================

/// Check if a context field exists and is non-empty.
/// A field is considered "has content" if it is not null, not an empty string,
/// not an empty array, and not an empty object.
/// Params: `{"field": "attachment"}`
pub struct HasContentCondition;

#[async_trait]
impl WorkflowCondition for HasContentCondition {
    fn condition_type(&self) -> &str {
        "has_content"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("has_content: missing 'field' param"))?;

        match instance.context.get(field) {
            None => Ok(false),
            Some(serde_json::Value::Null) => Ok(false),
            Some(serde_json::Value::String(s)) => Ok(!s.is_empty()),
            Some(serde_json::Value::Array(arr)) => Ok(!arr.is_empty()),
            Some(serde_json::Value::Object(map)) => Ok(!map.is_empty()),
            Some(_) => Ok(true), // numbers, bools are considered "has content"
        }
    }

    fn describe(&self) -> String {
        "Check if a context field exists and is non-empty".to_string()
    }
}

// ============================================================================
// 5. NotEmptyFieldCondition — Data Inspection
// ============================================================================

/// Check that a named context field is non-null and non-empty.
/// Unlike `has_content`, this returns `Ok(false)` if the field is present
/// but null or empty, and also `Ok(false)` if the field is absent.
/// Params: `{"field": "reviewer_notes"}`
pub struct NotEmptyFieldCondition;

#[async_trait]
impl WorkflowCondition for NotEmptyFieldCondition {
    fn condition_type(&self) -> &str {
        "not_empty_field"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("not_empty_field: missing 'field' param"))?;

        match instance.context.get(field) {
            None => Ok(false),
            Some(serde_json::Value::Null) => Ok(false),
            Some(serde_json::Value::String(s)) => Ok(!s.is_empty()),
            Some(serde_json::Value::Array(arr)) => Ok(!arr.is_empty()),
            Some(serde_json::Value::Object(map)) => Ok(!map.is_empty()),
            Some(_) => Ok(true),
        }
    }

    fn describe(&self) -> String {
        "Check that a context field is not null, missing, or empty".to_string()
    }
}

// ============================================================================
// 6. FieldEqualsCondition — Data Inspection
// ============================================================================

/// Type-aware field comparison: `context[field] == params.value`.
/// Uses direct serde_json::Value equality which handles string/number/bool/null.
/// Params: `{"field": "priority", "value": "high"}`
pub struct FieldEqualsCondition;

#[async_trait]
impl WorkflowCondition for FieldEqualsCondition {
    fn condition_type(&self) -> &str {
        "field_equals"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("field_equals: missing 'field' param"))?;
        let expected = params
            .get("value")
            .ok_or_else(|| anyhow!("field_equals: missing 'value' param"))?;

        match instance.context.get(field) {
            Some(actual) => Ok(actual == expected),
            None => Ok(false),
        }
    }

    fn describe(&self) -> String {
        "Check if a context field matches an expected value (type-aware)".to_string()
    }
}

// ============================================================================
// 7. FieldInCondition — Data Inspection
// ============================================================================

/// Check if a context field value is in a set of allowed values.
/// Params: `{"field": "region", "values": ["us-east", "eu-west"]}`
pub struct FieldInCondition;

#[async_trait]
impl WorkflowCondition for FieldInCondition {
    fn condition_type(&self) -> &str {
        "field_in"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("field_in: missing 'field' param"))?;
        let values = params
            .get("values")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("field_in: missing 'values' array param"))?;

        match instance.context.get(field) {
            Some(actual) => Ok(values.contains(actual)),
            None => Ok(false),
        }
    }

    fn describe(&self) -> String {
        "Check if a context field value is in a set of allowed values".to_string()
    }
}

// ============================================================================
// 8. ContentChangedCondition — Data Inspection
// ============================================================================

/// Check if a context field has changed since the reference snapshot.
/// Compares `context[field]` against `context["_snapshot_{field}"]`.
/// Returns `Err` if no snapshot exists (no baseline to compare against).
/// Params: `{"field": "document_hash"}`
pub struct ContentChangedCondition;

#[async_trait]
impl WorkflowCondition for ContentChangedCondition {
    fn condition_type(&self) -> &str {
        "content_changed"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let field = params
            .get("field")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("content_changed: missing 'field' param"))?;

        let snapshot_key = format!("_snapshot_{}", field);
        let snapshot = instance
            .context
            .get(&snapshot_key)
            .ok_or_else(|| anyhow!("content_changed: no snapshot '{}' found", snapshot_key))?;

        let current = instance.context.get(field);
        match current {
            Some(val) => Ok(val != snapshot),
            None => Ok(true), // field removed = changed
        }
    }

    fn describe(&self) -> String {
        "Check if a context field changed since the reference snapshot".to_string()
    }
}

// ============================================================================
// 9. MinApproversCondition — Multi-party
// ============================================================================

/// Count distinct actors in the instance history who performed a matching
/// action (transition). Returns true if the count >= `min`.
/// Params: `{"min": 3, "action": "approve"}`
///
/// Note: The `action` parameter matches against `TransitionRecord.transition_name`
/// in the instance history.
pub struct MinApproversCondition;

#[async_trait]
impl WorkflowCondition for MinApproversCondition {
    fn condition_type(&self) -> &str {
        "min_approvers"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let min = params
            .get("min")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("min_approvers: missing 'min' param"))? as usize;
        let action = params
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("approve");

        let distinct: HashSet<&str> = instance
            .history
            .iter()
            .filter(|r: &&TransitionRecord| r.transition_name == action)
            .map(|r| r.actor_id.as_str())
            .collect();
        Ok(distinct.len() >= min)
    }

    fn describe(&self) -> String {
        "Check if at least N distinct actors performed a specified action".to_string()
    }
}

// ============================================================================
// 10. TimeElapsedCondition — Temporal
// ============================================================================

/// Check if enough time has passed since the workflow instance was last updated
/// (entered the current state). Uses `instance.updated_at` as the reference
/// timestamp.
/// Params: `{"seconds": 86400}`
pub struct TimeElapsedCondition;

#[async_trait]
impl WorkflowCondition for TimeElapsedCondition {
    fn condition_type(&self) -> &str {
        "time_elapsed"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let threshold_secs = params
            .get("seconds")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("time_elapsed: missing 'seconds' param"))?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let state_entered_ms = instance.updated_at.timestamp_millis() as u64;
        let elapsed_secs = now_ms.saturating_sub(state_entered_ms) / 1000;
        Ok(elapsed_secs >= threshold_secs)
    }

    fn describe(&self) -> String {
        "Check if enough time has elapsed since entering the current state".to_string()
    }
}

// ============================================================================
// 11. ExpressionCondition — evalexpr integration
// ============================================================================

/// Evaluate a free-form expression against the workflow context using evalexpr.
/// Context fields are populated as evalexpr variables with "context." prefix.
/// Params: `{"expr": "context.amount > 10000"}`
pub struct ExpressionCondition;

impl ExpressionCondition {
    /// Recursively flatten JSON values into evalexpr variables.
    /// `{"amount": 5000}` with prefix "context" becomes variable
    /// `context.amount = Int(5000)`.
    fn populate_context(
        ctx: &mut evalexpr::HashMapContext,
        value: &serde_json::Value,
        prefix: &str,
    ) -> Result<()> {
        use evalexpr::ContextWithMutableVariables;
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    Self::populate_context(ctx, val, &format!("{}.{}", prefix, key))?;
                }
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ctx.set_value(prefix.into(), evalexpr::Value::Int(i))
                        .map_err(|e| anyhow!("expression: set_value failed: {}", e))?;
                } else if let Some(f) = n.as_f64() {
                    ctx.set_value(prefix.into(), evalexpr::Value::Float(f))
                        .map_err(|e| anyhow!("expression: set_value failed: {}", e))?;
                }
            }
            serde_json::Value::String(s) => {
                ctx.set_value(prefix.into(), evalexpr::Value::String(s.clone()))
                    .map_err(|e| anyhow!("expression: set_value failed: {}", e))?;
            }
            serde_json::Value::Bool(b) => {
                ctx.set_value(prefix.into(), evalexpr::Value::Boolean(*b))
                    .map_err(|e| anyhow!("expression: set_value failed: {}", e))?;
            }
            _ => {} // null / arrays skipped
        }
        Ok(())
    }
}

#[async_trait]
impl WorkflowCondition for ExpressionCondition {
    fn condition_type(&self) -> &str {
        "expression"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        _actor: &ActorInfo,
    ) -> Result<bool> {
        let expr_str = params
            .get("expr")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("expression: missing 'expr' param"))?;

        let mut eval_ctx = evalexpr::HashMapContext::new();
        Self::populate_context(&mut eval_ctx, &instance.context, "context")?;

        evalexpr::eval_boolean_with_context(expr_str, &eval_ctx)
            .map_err(|e| anyhow!("expression: eval failed: {}", e))
    }

    fn describe(&self) -> String {
        "Evaluate a free-form expression against workflow context using evalexpr".to_string()
    }
}

// ============================================================================
// 12-14. Composite Conditions — all_of, any_of, not
// ============================================================================

/// All sub-conditions must be true. Short-circuits on first false.
pub struct AllOfCondition {
    registry: Arc<ConditionRegistry>,
}

#[async_trait]
impl WorkflowCondition for AllOfCondition {
    fn condition_type(&self) -> &str {
        "all_of"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool> {
        let conditions = params
            .get("conditions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("all_of: missing 'conditions' array"))?;
        for sub in conditions {
            if !self.registry.evaluate_guard(sub, instance, actor).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn describe(&self) -> String {
        "Composite AND: all sub-conditions must be true".to_string()
    }
}

/// At least one sub-condition must be true. Short-circuits on first true.
pub struct AnyOfCondition {
    registry: Arc<ConditionRegistry>,
}

#[async_trait]
impl WorkflowCondition for AnyOfCondition {
    fn condition_type(&self) -> &str {
        "any_of"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool> {
        let conditions = params
            .get("conditions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("any_of: missing 'conditions' array"))?;
        for sub in conditions {
            if self.registry.evaluate_guard(sub, instance, actor).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn describe(&self) -> String {
        "Composite OR: at least one sub-condition must be true".to_string()
    }
}

/// Negate a single sub-condition. Errors are NOT inverted.
pub struct NotCondition {
    registry: Arc<ConditionRegistry>,
}

#[async_trait]
impl WorkflowCondition for NotCondition {
    fn condition_type(&self) -> &str {
        "not"
    }

    async fn evaluate(
        &self,
        params: &serde_json::Value,
        instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool> {
        let sub = params
            .get("condition")
            .ok_or_else(|| anyhow!("not: missing 'condition' param"))?;
        let result = self.registry.evaluate_guard(sub, instance, actor).await?;
        Ok(!result)
    }

    fn describe(&self) -> String {
        "Logical NOT: invert a single sub-condition (errors propagate unchanged)".to_string()
    }
}

// ============================================================================
// ConditionRegistry
// ============================================================================

/// Registry mapping condition type names to implementations.
/// Used by the workflow engine to evaluate transition guards.
pub struct ConditionRegistry {
    conditions: HashMap<String, Arc<dyn WorkflowCondition>>,
}

impl ConditionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            conditions: HashMap::new(),
        }
    }

    /// Create a registry pre-loaded with all 14 built-in conditions.
    /// Composite conditions (all_of, any_of, not) receive an Arc to
    /// this registry for recursive guard evaluation.
    pub fn with_builtins() -> Arc<Self> {
        // Phase 1: create registry with the 11 non-composite conditions.
        let mut registry = Self::new();
        registry.register(Arc::new(RoleIsCondition));
        registry.register(Arc::new(ActorIsCondition));
        registry.register(Arc::new(InStateCondition));
        registry.register(Arc::new(HasContentCondition));
        registry.register(Arc::new(NotEmptyFieldCondition));
        registry.register(Arc::new(FieldEqualsCondition));
        registry.register(Arc::new(FieldInCondition));
        registry.register(Arc::new(ContentChangedCondition));
        registry.register(Arc::new(MinApproversCondition));
        registry.register(Arc::new(TimeElapsedCondition));
        registry.register(Arc::new(ExpressionCondition));

        // Phase 2: wrap in Arc, then register composite conditions that
        // reference the registry for recursive evaluation.
        let arc = Arc::new(registry);
        let all_of = Arc::new(AllOfCondition {
            registry: arc.clone(),
        });
        let any_of = Arc::new(AnyOfCondition {
            registry: arc.clone(),
        });
        let not = Arc::new(NotCondition {
            registry: arc.clone(),
        });

        // We need to insert the composites into the same registry.
        // Since Arc wraps it, we use Arc::get_mut which works because
        // the composites hold clones, not the original Arc at this point.
        // Instead, we rebuild with all conditions.
        let mut full_registry = Self::new();
        // Copy all conditions from the initial registry
        for (name, cond) in &arc.conditions {
            full_registry
                .conditions
                .insert(name.clone(), cond.clone());
        }
        full_registry
            .conditions
            .insert("all_of".to_string(), all_of);
        full_registry
            .conditions
            .insert("any_of".to_string(), any_of);
        full_registry.conditions.insert("not".to_string(), not);
        Arc::new(full_registry)
    }

    /// Register a condition. The condition's `condition_type()` is used as
    /// the registry key.
    pub fn register(&mut self, cond: Arc<dyn WorkflowCondition>) {
        self.conditions
            .insert(cond.condition_type().to_string(), cond);
    }

    /// Look up a condition by type name.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn WorkflowCondition>> {
        self.conditions.get(name)
    }

    /// Evaluate a guard definition JSON object.
    /// Expected format: `{"type": "role_is", "params": {"role": "admin"}}`
    pub async fn evaluate_guard(
        &self,
        guard: &serde_json::Value,
        instance: &WorkflowInstance,
        actor: &ActorInfo,
    ) -> Result<bool> {
        let cond_type = guard
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Guard missing 'type' field"))?;
        let empty_params = serde_json::Value::Object(serde_json::Map::new());
        let params = guard.get("params").unwrap_or(&empty_params);
        let condition = self
            .conditions
            .get(cond_type)
            .ok_or_else(|| anyhow!("Unknown condition type: '{}'", cond_type))?;
        condition.evaluate(params, instance, actor).await
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

    use crate::swarm::workflow::types::{
        ActiveTimer, AuditConfig, CriticalityLevel, InstanceStatus,
    };

    /// Build a minimal WorkflowInstance for testing.
    fn make_instance(state: &str, context: serde_json::Value) -> WorkflowInstance {
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

    /// Build a minimal ActorInfo for testing.
    fn make_actor(id: &str, roles: Vec<&str>) -> ActorInfo {
        ActorInfo {
            actor_id: id.to_string(),
            roles: roles.into_iter().map(|r| r.to_string()).collect(),
            metadata: HashMap::new(),
        }
    }

    /// Build a TransitionRecord for testing history.
    fn make_history_entry(transition_name: &str, actor_id: &str) -> TransitionRecord {
        TransitionRecord {
            transition_name: transition_name.to_string(),
            from_state: "some_state".to_string(),
            to_state: "other_state".to_string(),
            actor_id: actor_id.to_string(),
            timestamp: Utc::now(),
            node_id: "test-node".to_string(),
            audit_event_id: Uuid::new_v4(),
            context_diff: json!({}),
        }
    }

    // --- Interpolation tests ---

    #[test]
    fn test_interpolation_simple() {
        let ctx = json!({"name": "Alice"});
        assert_eq!(interpolate("{name}", &ctx), "Alice");
    }

    #[test]
    fn test_interpolation_nested() {
        let ctx = json!({"user": {"email": "alice@example.com"}});
        assert_eq!(interpolate("{user.email}", &ctx), "alice@example.com");
    }

    #[test]
    fn test_interpolation_no_match() {
        let ctx = json!({"name": "Alice"});
        assert_eq!(interpolate("{missing}", &ctx), "{missing}");
    }

    #[test]
    fn test_interpolation_multiple() {
        let ctx = json!({"first": "Alice", "last": "Smith"});
        assert_eq!(interpolate("{first} {last}", &ctx), "Alice Smith");
    }

    // --- role_is tests ---

    #[tokio::test]
    async fn test_role_is_matching() {
        let cond = RoleIsCondition;
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec!["admin", "reviewer"]);
        let params = json!({"role": "admin"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_role_is_non_matching() {
        let cond = RoleIsCondition;
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec!["viewer"]);
        let params = json!({"role": "admin"});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_role_is_missing_param() {
        let cond = RoleIsCondition;
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec!["viewer"]);
        let params = json!({});
        assert!(cond.evaluate(&params, &inst, &actor).await.is_err());
    }

    // --- actor_is tests ---

    #[tokio::test]
    async fn test_actor_is_literal() {
        let cond = ActorIsCondition;
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-42", vec![]);
        let params = json!({"actor_id": "user-42"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_actor_is_interpolated() {
        let cond = ActorIsCondition;
        let inst = make_instance("draft", json!({"author_id": "user-42"}));
        let actor = make_actor("user-42", vec![]);
        let params = json!({"actor_id": "{author_id}"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_actor_is_interpolated_nested() {
        let cond = ActorIsCondition;
        let inst = make_instance("draft", json!({"author": {"id": "user-99"}}));
        let actor = make_actor("user-99", vec![]);
        let params = json!({"actor_id": "{author.id}"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- in_state tests ---

    #[tokio::test]
    async fn test_in_state_match() {
        let cond = InStateCondition;
        let inst = make_instance("pending_review", json!({}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"state": "pending_review"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_in_state_mismatch() {
        let cond = InStateCondition;
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"state": "pending_review"});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- has_content tests ---

    #[tokio::test]
    async fn test_has_content_present() {
        let cond = HasContentCondition;
        let inst = make_instance("draft", json!({"attachment": "doc.pdf"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "attachment"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_has_content_empty() {
        let cond = HasContentCondition;
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "attachment"});

        // Empty string
        let inst = make_instance("draft", json!({"attachment": ""}));
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());

        // Null
        let inst = make_instance("draft", json!({"attachment": null}));
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());

        // Missing key
        let inst = make_instance("draft", json!({}));
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());

        // Empty array
        let inst = make_instance("draft", json!({"attachment": []}));
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- not_empty_field tests ---

    #[tokio::test]
    async fn test_not_empty_field_null() {
        let cond = NotEmptyFieldCondition;
        let inst = make_instance("draft", json!({"notes": null}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "notes"});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_not_empty_field_empty_string() {
        let cond = NotEmptyFieldCondition;
        let inst = make_instance("draft", json!({"notes": ""}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "notes"});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_not_empty_field_present() {
        let cond = NotEmptyFieldCondition;
        let inst = make_instance("draft", json!({"notes": "LGTM"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "notes"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- field_equals tests ---

    #[tokio::test]
    async fn test_field_equals_string() {
        let cond = FieldEqualsCondition;
        let inst = make_instance("draft", json!({"priority": "high"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "priority", "value": "high"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_field_equals_number() {
        let cond = FieldEqualsCondition;
        let inst = make_instance("draft", json!({"count": 42}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "count", "value": 42});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_field_equals_bool() {
        let cond = FieldEqualsCondition;
        let inst = make_instance("draft", json!({"active": true}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "active", "value": true});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_field_equals_type_mismatch() {
        let cond = FieldEqualsCondition;
        // String "42" vs number 42 — should be false (type-aware)
        let inst = make_instance("draft", json!({"count": "42"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "count", "value": 42});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- field_in tests ---

    #[tokio::test]
    async fn test_field_in_match() {
        let cond = FieldInCondition;
        let inst = make_instance("draft", json!({"region": "us-east"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "region", "values": ["us-east", "eu-west"]});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_field_in_no_match() {
        let cond = FieldInCondition;
        let inst = make_instance("draft", json!({"region": "ap-south"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "region", "values": ["us-east", "eu-west"]});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- content_changed tests ---

    #[tokio::test]
    async fn test_content_changed_modified() {
        let cond = ContentChangedCondition;
        let inst = make_instance(
            "review",
            json!({
                "document_hash": "abc123",
                "_snapshot_document_hash": "old_hash"
            }),
        );
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "document_hash"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_content_changed_same() {
        let cond = ContentChangedCondition;
        let inst = make_instance(
            "review",
            json!({
                "document_hash": "abc123",
                "_snapshot_document_hash": "abc123"
            }),
        );
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "document_hash"});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_content_changed_no_snapshot() {
        let cond = ContentChangedCondition;
        let inst = make_instance("review", json!({"document_hash": "abc123"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"field": "document_hash"});
        assert!(cond.evaluate(&params, &inst, &actor).await.is_err());
    }

    // --- min_approvers tests ---

    #[tokio::test]
    async fn test_min_approvers_met() {
        let cond = MinApproversCondition;
        let mut inst = make_instance("review", json!({}));
        inst.history = vec![
            make_history_entry("approve", "alice"),
            make_history_entry("approve", "bob"),
            make_history_entry("approve", "carol"),
        ];
        let actor = make_actor("user-1", vec![]);
        let params = json!({"min": 3, "action": "approve"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_min_approvers_not_met() {
        let cond = MinApproversCondition;
        let mut inst = make_instance("review", json!({}));
        inst.history = vec![
            make_history_entry("approve", "alice"),
            make_history_entry("approve", "bob"),
        ];
        let actor = make_actor("user-1", vec![]);
        let params = json!({"min": 3, "action": "approve"});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_min_approvers_dedup() {
        let cond = MinApproversCondition;
        let mut inst = make_instance("review", json!({}));
        // Same actor twice should count as 1
        inst.history = vec![
            make_history_entry("approve", "alice"),
            make_history_entry("approve", "alice"),
        ];
        let actor = make_actor("user-1", vec![]);
        let params = json!({"min": 2, "action": "approve"});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- time_elapsed tests ---

    #[tokio::test]
    async fn test_time_elapsed_past() {
        let cond = TimeElapsedCondition;
        let mut inst = make_instance("review", json!({}));
        // Set updated_at to 2 hours ago
        inst.updated_at = Utc::now() - chrono::Duration::hours(2);
        let actor = make_actor("user-1", vec![]);
        // Threshold: 1 hour (3600 seconds)
        let params = json!({"seconds": 3600});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_time_elapsed_not_yet() {
        let cond = TimeElapsedCondition;
        let mut inst = make_instance("review", json!({}));
        // Set updated_at to 30 seconds ago
        inst.updated_at = Utc::now() - chrono::Duration::seconds(30);
        let actor = make_actor("user-1", vec![]);
        // Threshold: 1 hour (3600 seconds)
        let params = json!({"seconds": 3600});
        assert!(!cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    // --- expression tests ---

    #[tokio::test]
    async fn test_expression_arithmetic() {
        let cond = ExpressionCondition;
        let inst = make_instance("review", json!({"amount": 15000}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"expr": "context.amount > 10000"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_expression_boolean() {
        let cond = ExpressionCondition;
        let inst = make_instance("review", json!({"enabled": true, "verified": true}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"expr": "context.enabled && context.verified"});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_expression_string_compare() {
        let cond = ExpressionCondition;
        let inst = make_instance("review", json!({"status": "active"}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"expr": "context.status == \"active\""});
        assert!(cond.evaluate(&params, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_expression_malformed() {
        let cond = ExpressionCondition;
        let inst = make_instance("review", json!({}));
        let actor = make_actor("user-1", vec![]);
        let params = json!({"expr": "this is not a valid expression @@#$"});
        assert!(cond.evaluate(&params, &inst, &actor).await.is_err());
    }

    // --- composite all_of tests ---

    #[tokio::test]
    async fn test_all_of_all_true() {
        let registry = ConditionRegistry::with_builtins();
        let inst = make_instance(
            "draft",
            json!({"priority": "high", "attachment": "doc.pdf"}),
        );
        let actor = make_actor("user-1", vec!["admin"]);
        let guard = json!({
            "type": "all_of",
            "params": {
                "conditions": [
                    {"type": "role_is", "params": {"role": "admin"}},
                    {"type": "has_content", "params": {"field": "attachment"}}
                ]
            }
        });
        assert!(registry.evaluate_guard(&guard, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_all_of_one_false() {
        let registry = ConditionRegistry::with_builtins();
        let inst = make_instance("draft", json!({"attachment": "doc.pdf"}));
        let actor = make_actor("user-1", vec!["viewer"]); // NOT admin
        let guard = json!({
            "type": "all_of",
            "params": {
                "conditions": [
                    {"type": "role_is", "params": {"role": "admin"}},
                    {"type": "has_content", "params": {"field": "attachment"}}
                ]
            }
        });
        assert!(!registry.evaluate_guard(&guard, &inst, &actor).await.unwrap());
    }

    // --- composite any_of tests ---

    #[tokio::test]
    async fn test_any_of_one_true() {
        let registry = ConditionRegistry::with_builtins();
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec!["admin"]);
        let guard = json!({
            "type": "any_of",
            "params": {
                "conditions": [
                    {"type": "role_is", "params": {"role": "admin"}},
                    {"type": "role_is", "params": {"role": "superuser"}}
                ]
            }
        });
        assert!(registry.evaluate_guard(&guard, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_any_of_all_false() {
        let registry = ConditionRegistry::with_builtins();
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec!["viewer"]);
        let guard = json!({
            "type": "any_of",
            "params": {
                "conditions": [
                    {"type": "role_is", "params": {"role": "admin"}},
                    {"type": "role_is", "params": {"role": "superuser"}}
                ]
            }
        });
        assert!(!registry.evaluate_guard(&guard, &inst, &actor).await.unwrap());
    }

    // --- composite not tests ---

    #[tokio::test]
    async fn test_not_invert() {
        let registry = ConditionRegistry::with_builtins();
        let inst = make_instance("closed", json!({}));
        let actor = make_actor("user-1", vec![]);

        // in_state("closed") = true, not(in_state("closed")) = false
        let guard = json!({
            "type": "not",
            "params": {
                "condition": {"type": "in_state", "params": {"state": "closed"}}
            }
        });
        assert!(!registry.evaluate_guard(&guard, &inst, &actor).await.unwrap());

        // in_state("open") = false, not(in_state("open")) = true
        let guard = json!({
            "type": "not",
            "params": {
                "condition": {"type": "in_state", "params": {"state": "open"}}
            }
        });
        assert!(registry.evaluate_guard(&guard, &inst, &actor).await.unwrap());
    }

    #[tokio::test]
    async fn test_not_propagates_error() {
        let registry = ConditionRegistry::with_builtins();
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec![]);
        // role_is without "role" param = Err, not should propagate
        let guard = json!({
            "type": "not",
            "params": {
                "condition": {"type": "role_is", "params": {}}
            }
        });
        assert!(registry.evaluate_guard(&guard, &inst, &actor).await.is_err());
    }

    // --- registry tests ---

    #[tokio::test]
    async fn test_registry_get_builtin() {
        let registry = ConditionRegistry::with_builtins();
        let expected_names = [
            "role_is",
            "actor_is",
            "in_state",
            "has_content",
            "not_empty_field",
            "field_equals",
            "field_in",
            "content_changed",
            "min_approvers",
            "time_elapsed",
            "expression",
            "all_of",
            "any_of",
            "not",
        ];
        for name in &expected_names {
            assert!(
                registry.get(name).is_some(),
                "Expected condition '{}' to be registered",
                name
            );
        }
    }

    #[tokio::test]
    async fn test_registry_unknown_type() {
        let registry = ConditionRegistry::with_builtins();
        let inst = make_instance("draft", json!({}));
        let actor = make_actor("user-1", vec![]);
        let guard = json!({"type": "nonexistent_condition", "params": {}});
        assert!(registry.evaluate_guard(&guard, &inst, &actor).await.is_err());
    }
}
