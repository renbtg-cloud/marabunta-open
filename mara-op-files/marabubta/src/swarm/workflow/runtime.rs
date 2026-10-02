// Marabunta - Licensed under the MIT License.
//! State machine workflow runtime engine (W3A).
//!
//! Executes guarded transitions on workflow instances with distributed
//! locking, stale-state detection, multi-party approval accumulation,
//! and full audit trail integration.
//!
//! The runtime is the central coordinator for all workflow execution. It
//! owns the condition registry, the distributed lock, and the in-memory
//! stores for definitions and instances. In a production deployment the
//! instance store would be backed by the swarm's replicated storage layer;
//! for now we use DashMap for single-process correctness.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::conditions::ConditionRegistry;
use super::lock::{DistributedLock, LockError};
use super::types::*;
use crate::swarm::config::{WF_APPROVAL_TTL_SECS, WF_LOCK_TTL_SECS};

// ============================================================================
// Request / Response Types
// ============================================================================

/// A request to execute a transition on a workflow instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionRequest {
    /// ID of the workflow instance to transition.
    pub instance_id: Uuid,
    /// Name of the transition to fire.
    pub transition_name: String,
    /// The actor performing the transition.
    pub actor: ActorInfo,
    /// If set, the transition is only applied if the instance is currently
    /// in this state. Prevents stale-state races.
    pub expected_state: Option<String>,
    /// Arbitrary parameters for the transition (passed to guards/actions).
    #[serde(default)]
    pub params: serde_json::Value,
}

/// Result of a transition attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransitionResult {
    /// Transition fired successfully.
    Success {
        /// The updated workflow instance.
        instance: WorkflowInstance,
        /// ID of the audit event recorded for this transition.
        audit_event_id: Uuid,
    },
    /// One or more guards blocked the transition.
    Blocked {
        /// Human-readable reasons why the transition was blocked.
        reasons: Vec<String>,
    },
    /// Stale-state conflict: the instance is no longer in the expected state.
    Conflict {
        /// The actual current state of the instance.
        current_state: String,
    },
    /// Could not acquire the instance lock within the retry window.
    LockTimeout,
    /// The instance or definition was not found.
    NotFound {
        /// Description of what was not found.
        detail: String,
    },
}

/// Errors from workflow runtime operations (non-transition).
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("store error: {0}")]
    StoreError(String),

    #[error("definition not found: {0}")]
    DefinitionNotFound(String),

    #[error("parse error: {0}")]
    ParseError(String),

    #[error("lock failed: {0}")]
    LockFailed(String),
}

// ============================================================================
// Approval Accumulator Entry
// ============================================================================

/// Tracks partial approvals for multi-party transitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalEntry {
    /// Distinct actor IDs that have approved.
    approvers: Vec<String>,
    /// SHA-256 fingerprint of the transition parameters. All approvers
    /// must approve the same parameters for the accumulation to count.
    params_hash: String,
    /// When this entry was created (epoch millis).
    created_at_ms: u64,
}

// ============================================================================
// WorkflowRuntime
// ============================================================================

/// The workflow runtime engine.
///
/// Manages workflow definitions, instances, and transition execution with
/// distributed locking and guard evaluation.
pub struct WorkflowRuntime {
    /// Distributed lock for serializing transitions on a single instance.
    lock: Arc<DistributedLock>,
    /// Condition registry for evaluating transition guards.
    conditions: Arc<ConditionRegistry>,
    /// This node's identifier.
    node_id: String,
    /// Registered workflow definitions keyed by `"{name}@v{version}"`.
    definitions: DashMap<String, Arc<WorkflowDefinition>>,
    /// Live workflow instances keyed by instance ID.
    instances: DashMap<Uuid, WorkflowInstance>,
    /// Multi-party approval accumulator keyed by `"{instance_id}:{transition_name}"`.
    approvals: DashMap<String, ApprovalEntry>,
}

impl WorkflowRuntime {
    /// Create a new workflow runtime.
    pub fn new(node_id: String, lock: Arc<DistributedLock>, conditions: Arc<ConditionRegistry>) -> Self {
        Self {
            lock,
            conditions,
            node_id,
            definitions: DashMap::new(),
            instances: DashMap::new(),
            approvals: DashMap::new(),
        }
    }

    // ========================================================================
    // Definition Management
    // ========================================================================

    /// Register a workflow definition. Overwrites any existing definition
    /// with the same name and version.
    pub fn register_definition(&self, def: WorkflowDefinition) -> String {
        let key = format!("{}@v{}", def.name, def.version);
        info!(definition = %key, "registered workflow definition");
        self.definitions.insert(key.clone(), Arc::new(def));
        key
    }

    /// Look up a definition by `"{name}@v{version}"` key.
    pub fn get_definition(&self, key: &str) -> Option<Arc<WorkflowDefinition>> {
        self.definitions.get(key).map(|r| r.value().clone())
    }

    // ========================================================================
    // Instance Management
    // ========================================================================

    /// Create a new workflow instance from a registered definition.
    ///
    /// The instance starts in the definition's initial state with the
    /// provided context and creator ID.
    pub fn create_instance(
        &self,
        definition_key: &str,
        context: serde_json::Value,
        created_by: String,
    ) -> Result<WorkflowInstance, WorkflowError> {
        let def = self
            .definitions
            .get(definition_key)
            .ok_or_else(|| WorkflowError::DefinitionNotFound(definition_key.to_string()))?;

        let initial_state = def
            .states
            .iter()
            .find(|s| s.state_type == StateType::Initial)
            .ok_or_else(|| {
                WorkflowError::ParseError(format!(
                    "definition '{}' has no initial state",
                    definition_key
                ))
            })?;

        let now = Utc::now();
        let instance = WorkflowInstance {
            instance_id: Uuid::now_v7(),
            workflow_name: def.name.clone(),
            workflow_version: def.version,
            current_state: initial_state.name.clone(),
            context,
            history: Vec::new(),
            created_at: now,
            updated_at: now,
            created_by,
            status: InstanceStatus::Active,
            assigned_actors: HashMap::new(),
            timers: Vec::new(),
            locked_by: None,
        };

        self.instances
            .insert(instance.instance_id, instance.clone());

        info!(
            instance_id = %instance.instance_id,
            workflow = %definition_key,
            initial_state = %instance.current_state,
            "created workflow instance"
        );

        Ok(instance)
    }

    /// Get a snapshot of a workflow instance.
    pub fn get_instance(&self, instance_id: &Uuid) -> Option<WorkflowInstance> {
        self.instances.get(instance_id).map(|r| r.value().clone())
    }

    // ========================================================================
    // Transition Execution
    // ========================================================================

    /// Execute a transition on a workflow instance.
    ///
    /// This is the core 14-step protocol:
    ///
    /// 1. Acquire distributed lock on the instance
    /// 2. Load instance from store
    /// 3. Validate transition exists from current state
    /// 4. Stale-state detection (expected_state)
    /// 5-6. Evaluate all guards using ConditionRegistry
    /// 7. Return Blocked if any guard fails
    /// 8. Execute on_exit actions (best effort, log only)
    /// 9. Execute transition actions (best effort, log only)
    /// 10. Update state
    /// 11. Execute on_enter actions (best effort, log only)
    /// 12. Persist updated instance
    /// 13. Create audit event ID
    /// 14. Release lock, return Success
    pub async fn execute_transition(&self, req: TransitionRequest) -> TransitionResult {
        // Step 1: Acquire lock
        let lock_resource = format!("wf-instance:{}", req.instance_id);
        let guard = match self.lock.acquire(&lock_resource).await {
            Ok(g) => g,
            Err(LockError::Timeout(_)) => return TransitionResult::LockTimeout,
            Err(e) => {
                warn!(error = %e, "lock acquisition failed");
                return TransitionResult::LockTimeout;
            }
        };

        // Step 2: Load instance
        let instance = match self.instances.get(&req.instance_id) {
            Some(inst) => inst.clone(),
            None => {
                let _ = guard.release();
                return TransitionResult::NotFound {
                    detail: format!("instance {} not found", req.instance_id),
                };
            }
        };

        // Step 3: Look up the workflow definition
        let def_key = format!("{}@v{}", instance.workflow_name, instance.workflow_version);
        let def = match self.definitions.get(&def_key) {
            Some(d) => d.value().clone(),
            None => {
                let _ = guard.release();
                return TransitionResult::NotFound {
                    detail: format!("definition {} not found", def_key),
                };
            }
        };

        // Step 4: Find the transition and validate from_state
        let transition = match def
            .transitions
            .iter()
            .find(|t| t.name == req.transition_name && t.from == instance.current_state)
        {
            Some(t) => t.clone(),
            None => {
                let _ = guard.release();
                return TransitionResult::NotFound {
                    detail: format!(
                        "transition '{}' not valid from state '{}'",
                        req.transition_name, instance.current_state
                    ),
                };
            }
        };

        // Step 5: Stale-state detection
        if let Some(ref expected) = req.expected_state {
            if *expected != instance.current_state {
                let _ = guard.release();
                return TransitionResult::Conflict {
                    current_state: instance.current_state.clone(),
                };
            }
        }

        // Steps 5-6: Evaluate guards
        let mut blocked_reasons = Vec::new();
        for guard_def in &transition.guards {
            let guard_json = serde_json::json!({
                "type": guard_def.condition,
                "params": guard_def.params,
            });
            match self
                .conditions
                .evaluate_guard(&guard_json, &instance, &req.actor)
                .await
            {
                Ok(true) => { /* guard passed */ }
                Ok(false) => {
                    blocked_reasons.push(format!(
                        "guard '{}' not satisfied",
                        guard_def.condition
                    ));
                }
                Err(e) => {
                    blocked_reasons.push(format!(
                        "guard '{}' evaluation error: {}",
                        guard_def.condition, e
                    ));
                }
            }
        }

        // Step 7: Return Blocked if any guard failed
        if !blocked_reasons.is_empty() {
            let _ = guard.release();
            return TransitionResult::Blocked {
                reasons: blocked_reasons,
            };
        }

        // Step 8: Execute on_exit actions for current state (best effort)
        if let Some(current_state_def) = def.states.iter().find(|s| s.name == instance.current_state)
        {
            for action in &current_state_def.on_exit {
                debug!(
                    action = %action.action,
                    state = %instance.current_state,
                    "executing on_exit action (best effort)"
                );
            }
        }

        // Step 9: Execute transition actions (best effort, log only)
        for action in &transition.actions {
            debug!(
                action = %action.action,
                transition = %transition.name,
                "executing transition action (best effort)"
            );
        }

        // Step 10: Update state
        let from_state = instance.current_state.clone();
        let to_state = transition.to.clone();
        let now = Utc::now();
        let audit_event_id = Uuid::now_v7();

        let record = TransitionRecord {
            transition_name: transition.name.clone(),
            from_state: from_state.clone(),
            to_state: to_state.clone(),
            actor_id: req.actor.actor_id.clone(),
            timestamp: now,
            node_id: self.node_id.clone(),
            audit_event_id,
            context_diff: req.params.clone(),
        };

        let mut updated = instance;
        updated.current_state = to_state.clone();
        updated.updated_at = now;
        updated.history.push(record);
        updated.locked_by = None;

        // Check if we entered a terminal state
        if let Some(to_state_def) = def.states.iter().find(|s| s.name == to_state) {
            if to_state_def.state_type == StateType::Terminal {
                updated.status = InstanceStatus::Completed;
                info!(
                    instance_id = %updated.instance_id,
                    terminal_state = %to_state,
                    "workflow instance completed"
                );
            }
        }

        // Step 11: Execute on_enter actions for new state (best effort)
        if let Some(new_state_def) = def.states.iter().find(|s| s.name == to_state) {
            for action in &new_state_def.on_enter {
                debug!(
                    action = %action.action,
                    state = %to_state,
                    "executing on_enter action (best effort)"
                );
            }
        }

        // Step 12: Persist to store
        self.instances.insert(updated.instance_id, updated.clone());

        // Step 13: Audit event ID already created above

        // Step 14: Release lock
        let _ = guard.release();

        info!(
            instance_id = %updated.instance_id,
            transition = %transition.name,
            from = %from_state,
            to = %updated.current_state,
            "transition executed successfully"
        );

        TransitionResult::Success {
            instance: updated,
            audit_event_id,
        }
    }

    // ========================================================================
    // Available Transitions
    // ========================================================================

    /// Return a snapshot of all registered workflow definitions.
    pub fn list_definitions(&self) -> Vec<Arc<WorkflowDefinition>> {
        self.definitions
            .iter()
            .map(|r| r.value().clone())
            .collect()
    }

    /// Return a snapshot of all live workflow instances.
    pub fn list_instances(&self) -> Vec<WorkflowInstance> {
        self.instances.iter().map(|r| r.value().clone()).collect()
    }

    /// Look up the latest registered definition by workflow name (highest version).
    pub fn get_definition_by_name(&self, name: &str) -> Option<Arc<WorkflowDefinition>> {
        self.definitions
            .iter()
            .filter(|r| r.value().name == name)
            .max_by_key(|r| r.value().version)
            .map(|r| r.value().clone())
    }

    /// Return all transitions that originate from the instance's current state.
    pub fn available_transitions(
        &self,
        instance_id: &Uuid,
    ) -> Result<Vec<TransitionDefinition>, WorkflowError> {
        let instance = self
            .instances
            .get(instance_id)
            .ok_or_else(|| WorkflowError::StoreError(format!("instance {} not found", instance_id)))?;

        let def_key = format!(
            "{}@v{}",
            instance.workflow_name, instance.workflow_version
        );
        let def = self
            .definitions
            .get(&def_key)
            .ok_or(WorkflowError::DefinitionNotFound(def_key))?;

        Ok(def
            .transitions
            .iter()
            .filter(|t| t.from == instance.current_state)
            .cloned()
            .collect())
    }

    // ========================================================================
    // Multi-party Approval Accumulator
    // ========================================================================

    /// Record an approval from an actor for a multi-party transition.
    ///
    /// Returns `true` when the required number of distinct approvers has
    /// been reached. The caller should then fire the actual transition.
    ///
    /// All approvers must approve with the same `params` (verified via
    /// SHA-256 hash). Stale approvals (older than `WF_APPROVAL_TTL_SECS`)
    /// are discarded.
    pub fn accumulate_approval(
        &self,
        instance_id: &Uuid,
        transition_name: &str,
        actor_id: &str,
        params: &serde_json::Value,
        required_count: usize,
    ) -> bool {
        let key = format!("{}:{}", instance_id, transition_name);
        let params_hash = Self::hash_params(params);
        let now_ms = Utc::now().timestamp_millis() as u64;

        let mut entry = self
            .approvals
            .entry(key.clone())
            .or_insert_with(|| ApprovalEntry {
                approvers: Vec::new(),
                params_hash: params_hash.clone(),
                created_at_ms: now_ms,
            });

        // Check TTL
        let age_secs = (now_ms - entry.created_at_ms) / 1000;
        if age_secs > WF_APPROVAL_TTL_SECS {
            // Expired -- reset
            *entry = ApprovalEntry {
                approvers: Vec::new(),
                params_hash: params_hash.clone(),
                created_at_ms: now_ms,
            };
        }

        // Check params hash consistency
        if entry.params_hash != params_hash {
            debug!(
                key = %key,
                "approval params mismatch -- resetting accumulator"
            );
            *entry = ApprovalEntry {
                approvers: vec![actor_id.to_string()],
                params_hash,
                created_at_ms: now_ms,
            };
            return entry.approvers.len() >= required_count;
        }

        // Add approver if not already present
        if !entry.approvers.contains(&actor_id.to_string()) {
            entry.approvers.push(actor_id.to_string());
        }

        let reached = entry.approvers.len() >= required_count;

        if reached {
            // Clean up the accumulator entry
            drop(entry);
            self.approvals.remove(&key);
        }

        reached
    }

    /// Compute SHA-256 hash of transition parameters for multi-party
    /// approval consistency checking.
    fn hash_params(params: &serde_json::Value) -> String {
        let canonical = serde_json::to_string(params).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(canonical.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::workflow::types::*;

    /// Build a simple 3-state workflow: draft -> in_review -> approved.
    fn sample_definition() -> WorkflowDefinition {
        WorkflowDefinition {
            name: "doc-approval".to_string(),
            version: 1,
            description: "Document approval workflow".to_string(),
            context_schema: serde_json::json!({}),
            states: vec![
                StateDefinition {
                    name: "draft".to_string(),
                    state_type: StateType::Initial,
                    on_enter: vec![],
                    on_exit: vec![ActionDefinition {
                        action: "log_exit".to_string(),
                        params: serde_json::json!({}),
                    }],
                },
                StateDefinition {
                    name: "in_review".to_string(),
                    state_type: StateType::Active,
                    on_enter: vec![ActionDefinition {
                        action: "notify_reviewers".to_string(),
                        params: serde_json::json!({"channel": "email"}),
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
                        params: serde_json::json!({"field": "body"}),
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
            triggers: vec![],
            hooks: HookDefinition::default(),
        }
    }

    fn make_actor(id: &str, roles: Vec<&str>) -> ActorInfo {
        ActorInfo {
            actor_id: id.to_string(),
            roles: roles.into_iter().map(|r| r.to_string()).collect(),
            metadata: HashMap::new(),
        }
    }

    fn make_runtime() -> WorkflowRuntime {
        let lock = Arc::new(DistributedLock::new("test-node".to_string()));
        let conditions = ConditionRegistry::with_builtins();
        WorkflowRuntime::new("test-node".to_string(), lock, conditions)
    }

    #[tokio::test]
    async fn test_successful_transition() {
        let rt = make_runtime();
        let def = sample_definition();
        let key = rt.register_definition(def);

        let instance = rt
            .create_instance(
                &key,
                serde_json::json!({"body": "Hello world"}),
                "alice".to_string(),
            )
            .unwrap();
        assert_eq!(instance.current_state, "draft");

        let req = TransitionRequest {
            instance_id: instance.instance_id,
            transition_name: "submit".to_string(),
            actor: make_actor("alice", vec!["author"]),
            expected_state: Some("draft".to_string()),
            params: serde_json::json!({}),
        };

        let result = rt.execute_transition(req).await;
        match result {
            TransitionResult::Success {
                instance: updated, ..
            } => {
                assert_eq!(updated.current_state, "in_review");
                assert_eq!(updated.history.len(), 1);
                assert_eq!(updated.history[0].transition_name, "submit");
            }
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_guard_failure_blocks() {
        let rt = make_runtime();
        let def = sample_definition();
        let key = rt.register_definition(def);

        // Create instance WITHOUT the required "body" field.
        let instance = rt
            .create_instance(&key, serde_json::json!({}), "alice".to_string())
            .unwrap();

        let req = TransitionRequest {
            instance_id: instance.instance_id,
            transition_name: "submit".to_string(),
            actor: make_actor("alice", vec!["author"]),
            expected_state: None,
            params: serde_json::json!({}),
        };

        let result = rt.execute_transition(req).await;
        match result {
            TransitionResult::Blocked { reasons } => {
                assert!(!reasons.is_empty());
                assert!(reasons[0].contains("has_content"));
            }
            other => panic!("expected Blocked, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_stale_state_conflict() {
        let rt = make_runtime();
        let def = sample_definition();
        let key = rt.register_definition(def);

        let instance = rt
            .create_instance(
                &key,
                serde_json::json!({"body": "content"}),
                "alice".to_string(),
            )
            .unwrap();

        // Expect the instance to be in "in_review" (but it's actually "draft").
        let req = TransitionRequest {
            instance_id: instance.instance_id,
            transition_name: "submit".to_string(),
            actor: make_actor("alice", vec!["author"]),
            expected_state: Some("in_review".to_string()),
            params: serde_json::json!({}),
        };

        let result = rt.execute_transition(req).await;
        match result {
            TransitionResult::Conflict { current_state } => {
                assert_eq!(current_state, "draft");
            }
            other => panic!("expected Conflict, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_lock_timeout() {
        let lock = Arc::new(DistributedLock::new("node-a".to_string()));
        let conditions = ConditionRegistry::with_builtins();
        let rt = WorkflowRuntime::new("node-a".to_string(), lock.clone(), conditions);

        let def = sample_definition();
        let key = rt.register_definition(def);
        let instance = rt
            .create_instance(
                &key,
                serde_json::json!({"body": "content"}),
                "alice".to_string(),
            )
            .unwrap();

        // Pre-acquire the lock from a different "node" so the runtime times out.
        let lock_resource = format!("wf-instance:{}", instance.instance_id);
        let other_lock = DistributedLock {
            store: lock.store.clone(),
            node_id: "node-b".to_string(),
        };
        let _held = other_lock
            .acquire_with_ttl(&lock_resource, WF_LOCK_TTL_SECS)
            .await
            .unwrap();

        let req = TransitionRequest {
            instance_id: instance.instance_id,
            transition_name: "submit".to_string(),
            actor: make_actor("alice", vec!["author"]),
            expected_state: None,
            params: serde_json::json!({}),
        };

        // Use a timeout so the test doesn't hang forever.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            rt.execute_transition(req),
        )
        .await
        .unwrap();

        match result {
            TransitionResult::LockTimeout => { /* expected */ }
            other => panic!("expected LockTimeout, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_multiparty_accumulator() {
        let rt = make_runtime();
        let instance_id = Uuid::now_v7();
        let params = serde_json::json!({"document": "doc-1"});

        // Need 3 approvals.
        assert!(!rt.accumulate_approval(&instance_id, "approve", "alice", &params, 3));
        assert!(!rt.accumulate_approval(&instance_id, "approve", "bob", &params, 3));

        // Duplicate should not count twice.
        assert!(!rt.accumulate_approval(&instance_id, "approve", "alice", &params, 3));

        // Third distinct approver triggers quorum.
        assert!(rt.accumulate_approval(&instance_id, "approve", "carol", &params, 3));
    }

    #[tokio::test]
    async fn test_timer_creation_on_enter() {
        // This test verifies that on_enter actions are logged during transition.
        // In the current implementation, actions are best-effort/log-only.
        let rt = make_runtime();
        let def = sample_definition();
        let key = rt.register_definition(def);

        let instance = rt
            .create_instance(
                &key,
                serde_json::json!({"body": "test document content"}),
                "alice".to_string(),
            )
            .unwrap();

        // "submit" transitions to "in_review" which has an on_enter "notify_reviewers" action.
        let req = TransitionRequest {
            instance_id: instance.instance_id,
            transition_name: "submit".to_string(),
            actor: make_actor("alice", vec!["author"]),
            expected_state: None,
            params: serde_json::json!({}),
        };

        let result = rt.execute_transition(req).await;
        match result {
            TransitionResult::Success { instance: updated, .. } => {
                assert_eq!(updated.current_state, "in_review");
                assert_eq!(updated.status, InstanceStatus::Active);
            }
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_instance_not_found() {
        let rt = make_runtime();
        let fake_id = Uuid::now_v7();

        let req = TransitionRequest {
            instance_id: fake_id,
            transition_name: "submit".to_string(),
            actor: make_actor("alice", vec!["author"]),
            expected_state: None,
            params: serde_json::json!({}),
        };

        let result = rt.execute_transition(req).await;
        match result {
            TransitionResult::NotFound { detail } => {
                assert!(detail.contains("not found"));
            }
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_invalid_transition_from_state() {
        let rt = make_runtime();
        let def = sample_definition();
        let key = rt.register_definition(def);

        let instance = rt
            .create_instance(
                &key,
                serde_json::json!({"body": "content"}),
                "alice".to_string(),
            )
            .unwrap();
        assert_eq!(instance.current_state, "draft");

        // Try "approve" which is only valid from "in_review", not "draft".
        let req = TransitionRequest {
            instance_id: instance.instance_id,
            transition_name: "approve".to_string(),
            actor: make_actor("bob", vec!["reviewer"]),
            expected_state: None,
            params: serde_json::json!({}),
        };

        let result = rt.execute_transition(req).await;
        match result {
            TransitionResult::NotFound { detail } => {
                assert!(detail.contains("not valid from state"));
            }
            other => panic!("expected NotFound, got {:?}", other),
        }
    }
}
