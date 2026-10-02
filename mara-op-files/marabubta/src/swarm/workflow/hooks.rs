// Marabunta - Licensed under the MIT License.
//! Workflow lifecycle hook system (W3C).
//!
//! Provides the `WorkflowHook` async trait and `HookRegistry` for registering
//! and firing lifecycle callbacks on workflow instances. Hooks are notified
//! on instance creation, completion, transition firing/blocking, deadlock
//! detection, and timer firing.
//!
//! The `HookRegistry` guarantees **error isolation**: if a hook callback
//! returns an error, a warning is logged but the error is never propagated
//! to the caller. This ensures that a misbehaving hook cannot block or
//! crash workflow execution.

use anyhow::Result;
use async_trait::async_trait;
use tracing::warn;

use super::types::WorkflowInstance;

// ============================================================================
// WorkflowHook trait
// ============================================================================

/// Async trait for workflow lifecycle hooks.
///
/// Implementors can override any subset of the six callback methods.
/// All methods have default no-op implementations that return `Ok(())`.
#[async_trait]
pub trait WorkflowHook: Send + Sync {
    /// A human-readable type name for this hook (e.g., `"audit_logger"`).
    fn hook_type(&self) -> &str;

    /// Called when a new workflow instance is created.
    async fn on_instance_created(&self, _instance: &WorkflowInstance) -> Result<()> {
        Ok(())
    }

    /// Called when a workflow instance reaches a terminal state.
    async fn on_instance_completed(&self, _instance: &WorkflowInstance) -> Result<()> {
        Ok(())
    }

    /// Called when a transition fires successfully.
    async fn on_transition_fired(
        &self,
        _instance: &WorkflowInstance,
        _transition: &str,
    ) -> Result<()> {
        Ok(())
    }

    /// Called when a transition is blocked by a guard condition.
    async fn on_transition_blocked(
        &self,
        _instance: &WorkflowInstance,
        _transition: &str,
        _reason: &str,
    ) -> Result<()> {
        Ok(())
    }

    /// Called when a workflow instance is detected as deadlocked
    /// (no enabled transitions from the current state).
    async fn on_deadlock(&self, _instance: &WorkflowInstance) -> Result<()> {
        Ok(())
    }

    /// Called when a scheduled timer fires on a workflow instance.
    async fn on_timer_fired(
        &self,
        _instance: &WorkflowInstance,
        _timer: &str,
    ) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// HookRegistry
// ============================================================================

/// Registry of workflow lifecycle hooks.
///
/// Hooks are stored in registration order and fired sequentially.
/// Errors from individual hooks are logged and swallowed to provide
/// error isolation.
pub struct HookRegistry {
    hooks: Vec<Box<dyn WorkflowHook>>,
}

impl HookRegistry {
    /// Create an empty hook registry.
    pub fn new() -> Self {
        Self { hooks: Vec::new() }
    }

    /// Register a new hook. Hooks are called in registration order.
    pub fn register(&mut self, hook: Box<dyn WorkflowHook>) {
        self.hooks.push(hook);
    }

    /// Number of registered hooks.
    pub fn len(&self) -> usize {
        self.hooks.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }

    /// Fire the `on_instance_created` callback on all registered hooks.
    pub async fn fire_on_created(&self, instance: &WorkflowInstance) {
        for hook in &self.hooks {
            if let Err(e) = hook.on_instance_created(instance).await {
                warn!(
                    hook = hook.hook_type(),
                    error = %e,
                    "hook on_instance_created failed"
                );
            }
        }
    }

    /// Fire the `on_instance_completed` callback on all registered hooks.
    pub async fn fire_on_completed(&self, instance: &WorkflowInstance) {
        for hook in &self.hooks {
            if let Err(e) = hook.on_instance_completed(instance).await {
                warn!(
                    hook = hook.hook_type(),
                    error = %e,
                    "hook on_instance_completed failed"
                );
            }
        }
    }

    /// Fire the `on_transition_fired` callback on all registered hooks.
    pub async fn fire_on_transition_fired(&self, instance: &WorkflowInstance, transition: &str) {
        for hook in &self.hooks {
            if let Err(e) = hook.on_transition_fired(instance, transition).await {
                warn!(
                    hook = hook.hook_type(),
                    transition = transition,
                    error = %e,
                    "hook on_transition_fired failed"
                );
            }
        }
    }

    /// Fire the `on_transition_blocked` callback on all registered hooks.
    pub async fn fire_on_transition_blocked(
        &self,
        instance: &WorkflowInstance,
        transition: &str,
        reason: &str,
    ) {
        for hook in &self.hooks {
            if let Err(e) = hook
                .on_transition_blocked(instance, transition, reason)
                .await
            {
                warn!(
                    hook = hook.hook_type(),
                    transition = transition,
                    error = %e,
                    "hook on_transition_blocked failed"
                );
            }
        }
    }

    /// Fire the `on_deadlock` callback on all registered hooks.
    pub async fn fire_on_deadlock(&self, instance: &WorkflowInstance) {
        for hook in &self.hooks {
            if let Err(e) = hook.on_deadlock(instance).await {
                warn!(
                    hook = hook.hook_type(),
                    error = %e,
                    "hook on_deadlock failed"
                );
            }
        }
    }

    /// Fire the `on_timer_fired` callback on all registered hooks.
    pub async fn fire_on_timer_fired(&self, instance: &WorkflowInstance, timer: &str) {
        for hook in &self.hooks {
            if let Err(e) = hook.on_timer_fired(instance, timer).await {
                warn!(
                    hook = hook.hook_type(),
                    timer = timer,
                    error = %e,
                    "hook on_timer_fired failed"
                );
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::super::types::InstanceStatus;

    /// Test hook that records callback invocations for assertion.
    struct TestHook {
        name: String,
        calls: Arc<Mutex<Vec<String>>>,
        /// If true, all callbacks return an error (for error isolation tests).
        fail: bool,
    }

    impl TestHook {
        fn new(name: &str, calls: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                name: name.to_string(),
                calls,
                fail: false,
            }
        }

        fn failing(name: &str, calls: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                name: name.to_string(),
                calls,
                fail: true,
            }
        }
    }

    #[async_trait]
    impl WorkflowHook for TestHook {
        fn hook_type(&self) -> &str {
            &self.name
        }

        async fn on_instance_created(&self, _instance: &WorkflowInstance) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("{}:created", self.name));
            if self.fail {
                anyhow::bail!("intentional test failure");
            }
            Ok(())
        }

        async fn on_instance_completed(&self, _instance: &WorkflowInstance) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("{}:completed", self.name));
            if self.fail {
                anyhow::bail!("intentional test failure");
            }
            Ok(())
        }

        async fn on_transition_fired(
            &self,
            _instance: &WorkflowInstance,
            transition: &str,
        ) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("{}:fired:{}", self.name, transition));
            if self.fail {
                anyhow::bail!("intentional test failure");
            }
            Ok(())
        }

        async fn on_transition_blocked(
            &self,
            _instance: &WorkflowInstance,
            transition: &str,
            reason: &str,
        ) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("{}:blocked:{}:{}", self.name, transition, reason));
            if self.fail {
                anyhow::bail!("intentional test failure");
            }
            Ok(())
        }

        async fn on_deadlock(&self, _instance: &WorkflowInstance) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("{}:deadlock", self.name));
            if self.fail {
                anyhow::bail!("intentional test failure");
            }
            Ok(())
        }

        async fn on_timer_fired(
            &self,
            _instance: &WorkflowInstance,
            timer: &str,
        ) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("{}:timer:{}", self.name, timer));
            if self.fail {
                anyhow::bail!("intentional test failure");
            }
            Ok(())
        }
    }

    fn sample_instance() -> WorkflowInstance {
        WorkflowInstance {
            instance_id: Uuid::new_v4(),
            workflow_name: "test-workflow".to_string(),
            workflow_version: 1,
            current_state: "draft".to_string(),
            context: json!({}),
            history: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            created_by: "test-user".to_string(),
            status: InstanceStatus::Active,
            assigned_actors: HashMap::new(),
            timers: vec![],
            locked_by: None,
        }
    }

    #[tokio::test]
    async fn test_hook_registration_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        assert!(registry.is_empty());

        registry.register(Box::new(TestHook::new("alpha", calls.clone())));
        registry.register(Box::new(TestHook::new("beta", calls.clone())));
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());

        let instance = sample_instance();
        registry.fire_on_created(&instance).await;

        let recorded = calls.lock().await;
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0], "alpha:created");
        assert_eq!(recorded[1], "beta:created");
    }

    #[tokio::test]
    async fn test_hook_created_callback() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry.register(Box::new(TestHook::new("audit", calls.clone())));

        let instance = sample_instance();
        registry.fire_on_created(&instance).await;

        let recorded = calls.lock().await;
        assert_eq!(&*recorded, &["audit:created"]);
    }

    #[tokio::test]
    async fn test_hook_completed_callback() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry.register(Box::new(TestHook::new("logger", calls.clone())));

        let instance = sample_instance();
        registry.fire_on_completed(&instance).await;

        let recorded = calls.lock().await;
        assert_eq!(&*recorded, &["logger:completed"]);
    }

    #[tokio::test]
    async fn test_hook_transition_fired_callback() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry.register(Box::new(TestHook::new("tracer", calls.clone())));

        let instance = sample_instance();
        registry.fire_on_transition_fired(&instance, "submit").await;

        let recorded = calls.lock().await;
        assert_eq!(&*recorded, &["tracer:fired:submit"]);
    }

    #[tokio::test]
    async fn test_hook_transition_blocked_callback() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry.register(Box::new(TestHook::new("monitor", calls.clone())));

        let instance = sample_instance();
        registry
            .fire_on_transition_blocked(&instance, "approve", "insufficient_role")
            .await;

        let recorded = calls.lock().await;
        assert_eq!(&*recorded, &["monitor:blocked:approve:insufficient_role"]);
    }

    #[tokio::test]
    async fn test_hook_error_isolation() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();

        // First hook fails, second should still be called.
        registry.register(Box::new(TestHook::failing("bad_hook", calls.clone())));
        registry.register(Box::new(TestHook::new("good_hook", calls.clone())));

        let instance = sample_instance();
        registry.fire_on_created(&instance).await;

        let recorded = calls.lock().await;
        // Both hooks should have been called despite the first one failing.
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0], "bad_hook:created");
        assert_eq!(recorded[1], "good_hook:created");
    }

    #[tokio::test]
    async fn test_hook_timer_fired_callback() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry.register(Box::new(TestHook::new("timer_hook", calls.clone())));

        let instance = sample_instance();
        registry
            .fire_on_timer_fired(&instance, "review_deadline")
            .await;

        let recorded = calls.lock().await;
        assert_eq!(&*recorded, &["timer_hook:timer:review_deadline"]);
    }
}
