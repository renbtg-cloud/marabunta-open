// Marabunta - Licensed under the MIT License.
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::swarm::audit::{AuditActor, AuditLog, AuditOutcome, AuditTarget};
use crate::swarm::events::EventBus;
use super::config::OaiConfig;
use super::errors::{OaiError, OaiResult};
use super::guard::{ConditionGuard, GuardEvaluation, GuardEvaluator};
use super::impact::{ImpactAssessment, ImpactAssessor};
use super::lock::LockManager;
use super::operator::{Operator, OperatorStore};
use super::rollback::{PostCondition, RollbackManager};
use super::staleness;
use super::types::*;

/// A request to perform an intervention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterventionRequest {
    /// Unique ID for this request.
    pub id: InterventionId,
    /// Which tier and action.
    pub action: InterventionAction,
    /// Target entity.
    pub target: EntityRef,
    /// Condition guard (auto-generated or operator-provided).
    pub guard: Option<ConditionGuard>,
    /// Urgency class (determines staleness window).
    pub urgency: InterventionUrgency,
    /// Operator issuing this request.
    pub operator_id: OperatorId,
    /// Optional incident session.
    pub session_id: Option<SessionId>,
    /// Whether to skip guard evaluation (--no-guard).
    pub skip_guard: bool,
    /// Whether to force past safety blocks (--force).
    pub force: bool,
    /// Whether this is a dry-run (evaluate guard only, don't execute).
    pub dry_run: bool,
    /// Tier-specific parameters.
    pub params: serde_json::Value,
}

/// Result of an intervention execution.
#[derive(Debug, Clone, Serialize)]
pub struct InterventionResult {
    pub intervention_id: InterventionId,
    /// Guard evaluation result (None if guard was skipped).
    pub guard_evaluation: Option<GuardEvaluation>,
    /// Impact assessment.
    pub impact: Option<ImpactAssessment>,
    /// Whether the intervention was actually executed (false for dry-run).
    pub executed: bool,
    /// The subsystem result (tier-specific data).
    pub result: serde_json::Value,
    /// Audit entry ID in the hash-chained log.
    pub audit_entry_id: Option<u64>,
    /// Rollback info (for reversible interventions with optimistic execution).
    pub rollback_info: Option<String>,
}

/// Trait that each tier implements to handle its interventions.
pub trait TierHandler: Send + Sync {
    /// The tier this handler serves.
    fn tier(&self) -> InterventionTier;

    /// Execute an intervention. Called after all checks pass.
    fn execute(
        &self,
        target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> OaiResult<serde_json::Value>;

    /// Get the urgency class for this action.
    fn urgency(&self, action: &str) -> InterventionUrgency;

    /// Get the reversibility for this action.
    fn reversibility(&self, action: &str) -> Reversibility;

    /// Get post-conditions for optimistic execution (empty if not reversible).
    fn post_conditions(
        &self,
        target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> Vec<PostCondition>;

    /// Execute rollback for a reversible intervention.
    fn rollback(
        &self,
        target: &EntityRef,
        action: &str,
        snapshot_before: &serde_json::Value,
    ) -> OaiResult<()>;
}

/// The central intervention engine.
pub struct InterventionEngine {
    config: OaiConfig,
    guard_evaluator: Arc<GuardEvaluator>,
    lock_manager: Arc<LockManager>,
    impact_assessor: Arc<ImpactAssessor>,
    rollback_manager: Arc<RollbackManager>,
    operator_store: Arc<OperatorStore>,
    audit_log: Arc<AuditLog>,
    _event_bus: Arc<EventBus>,
    /// Registered tier handlers.
    handlers: std::collections::HashMap<InterventionTier, Arc<dyn TierHandler>>,
}

impl InterventionEngine {
    pub fn new(
        config: OaiConfig,
        guard_evaluator: Arc<GuardEvaluator>,
        lock_manager: Arc<LockManager>,
        impact_assessor: Arc<ImpactAssessor>,
        rollback_manager: Arc<RollbackManager>,
        operator_store: Arc<OperatorStore>,
        audit_log: Arc<AuditLog>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            config,
            guard_evaluator,
            lock_manager,
            impact_assessor,
            rollback_manager,
            operator_store,
            audit_log,
            _event_bus: event_bus,
            handlers: std::collections::HashMap::new(),
        }
    }

    /// Register a tier handler.
    pub fn register_handler(&mut self, handler: Arc<dyn TierHandler>) {
        self.handlers.insert(handler.tier(), handler);
    }

    /// Execute an intervention through the full pipeline.
    ///
    /// Pipeline steps:
    /// 1. Resolve operator from store
    /// 2. Check tier permission
    /// 3. Look up tier handler
    /// 4. Check staleness (if guard provided)
    /// 5. Check version (if guard provided)
    /// 6. Evaluate guard (if guard provided and not skipped)
    /// 7. Assess impact
    /// 8. Check safety block (impact × operator tier)
    /// 9. Acquire entity lock
    /// 10. Execute intervention
    /// 11. Record in audit log
    /// 12. Register rollback (if reversible + optimistic enabled)
    /// 13. Release lock
    /// 14. Return result
    pub fn execute(&self, request: InterventionRequest) -> OaiResult<InterventionResult> {
        let intervention_id = request.id.clone();

        // 1. Resolve operator
        let operator = self
            .operator_store
            .get(&request.operator_id)
            .ok_or_else(|| OaiError::EntityNotFound {
                entity: EntityRef {
                    entity_type: EntityType::Operator,
                    id: request.operator_id.0.clone(),
                },
            })?;

        // 2. Check tier permission
        operator.check_tier_permission(request.action.tier)?;

        // 3. Look up tier handler
        let handler = self
            .handlers
            .get(&request.action.tier)
            .ok_or_else(|| OaiError::SubsystemDisabled {
                tier: request.action.tier,
                subsystem: format!("{}", request.action.tier),
                config_flag: "enable_observe_and_interfere".to_string(),
            })?;

        // 4-6. Guard evaluation
        let guard_evaluation = if let Some(ref guard) = request.guard {
            if !request.skip_guard {
                // 4. Check staleness
                let urgency = handler.urgency(&request.action.name);
                staleness::check_guard_staleness(
                    &guard.conditions,
                    urgency,
                    &self.config.guard,
                )?;

                // 5. Check version
                staleness::check_version(&guard.conditions, |entity| {
                    match entity.entity_type {
                        EntityType::Node => {
                            let node_id = crate::swarm::types::NodeId(
                                Uuid::parse_str(&entity.id).ok()?,
                            );
                            self.guard_evaluator.resolve_generation(&node_id)
                        }
                        _ => None,
                    }
                })?;

                // 6. Evaluate guard
                let eval = self.guard_evaluator.evaluate(guard);
                if !eval.passed {
                    if let Some(failed) = eval.results.iter().find(|r| !r.passed) {
                        return Err(OaiError::GuardViolated {
                            entity: failed.condition.entity.clone(),
                            field: failed.condition.field.clone(),
                            expected: failed.condition.expected.clone(),
                            actual: failed
                                .actual
                                .clone()
                                .unwrap_or(serde_json::Value::Null),
                            drift_ms: (Utc::now() - failed.condition.snapshot_time)
                                .num_milliseconds()
                                .max(0)
                                as u64,
                        });
                    }
                }
                Some(eval)
            } else {
                None
            }
        } else {
            None
        };

        // If dry-run, stop here
        if request.dry_run {
            return Ok(InterventionResult {
                intervention_id,
                guard_evaluation,
                impact: None,
                executed: false,
                result: serde_json::json!({"dry_run": true}),
                audit_entry_id: None,
                rollback_info: None,
            });
        }

        // 7. Assess impact
        let impact = if request.action.tier == InterventionTier::NodeLifecycle {
            if let Ok(uuid) = Uuid::parse_str(&request.target.id) {
                let node_id = crate::swarm::types::NodeId(uuid);
                Some(
                    self.impact_assessor
                        .assess_node_intervention(&node_id, &request.action.name),
                )
            } else {
                None
            }
        } else {
            None
        };

        // 8. Check safety block
        if let Some(ref assessment) = impact {
            if !request.force {
                self.impact_assessor.check_safety_block(
                    assessment,
                    &operator,
                    &request.action.name,
                )?;
            }
        }

        // 9. Acquire lock
        let ttl = Duration::from_millis(self.config.lock.default_ttl_ms);
        let _lock = self.lock_manager.try_acquire(
            request.target.clone(),
            intervention_id.clone(),
            request.operator_id.clone(),
            request.action.to_string(),
            ttl,
        )?;

        // 10. Execute
        let exec_result =
            handler.execute(&request.target, &request.action.name, &request.params);

        // 13. Release lock (regardless of outcome)
        self.lock_manager.release(&request.target);

        let result_value = match exec_result {
            Ok(val) => val,
            Err(e) => {
                // 11b. Audit failure
                self.audit_intervention(
                    &request,
                    &operator,
                    AuditOutcome::Failure {
                        reason: e.to_string(),
                    },
                );
                return Err(e);
            }
        };

        // 11a. Audit success
        let audit_id = self.audit_intervention(&request, &operator, AuditOutcome::Success);

        // 12. Register rollback if reversible + optimistic
        let rollback_info = if self.config.impact.optimistic_execution
            && handler.reversibility(&request.action.name) == Reversibility::Reversible
        {
            let post_conds = handler.post_conditions(
                &request.target,
                &request.action.name,
                &request.params,
            );
            if !post_conds.is_empty() {
                let record = self.rollback_manager.register(
                    intervention_id.clone(),
                    request.action.name.clone(),
                    serde_json::json!({}),
                    post_conds,
                );
                Some(format!(
                    "rollback registered, deadline: {}",
                    record.rollback_deadline
                ))
            } else {
                None
            }
        } else {
            None
        };

        Ok(InterventionResult {
            intervention_id,
            guard_evaluation,
            impact,
            executed: true,
            result: result_value,
            audit_entry_id: audit_id,
            rollback_info,
        })
    }

    /// Record an intervention in the audit log.
    fn audit_intervention(
        &self,
        request: &InterventionRequest,
        operator: &Operator,
        outcome: AuditOutcome,
    ) -> Option<u64> {
        let entry_id = self.audit_log.record(
            AuditActor {
                actor_type: "operator".to_string(),
                id: operator.id.0.clone(),
                display_name: Some(operator.name.clone()),
            },
            format!(
                "intervene.{}.{}",
                request.action.tier, request.action.name
            ),
            AuditTarget {
                target_type: request.target.entity_type.to_string(),
                id: request.target.id.clone(),
                display_name: None,
            },
            outcome,
            serde_json::json!({
                "intervention_id": request.id.0,
                "force": request.force,
                "skip_guard": request.skip_guard,
                "session_id": request.session_id,
                "params": request.params,
            }),
        );
        Some(entry_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::audit::AuditLog;
    use crate::swarm::events::EventBus;
    use crate::swarm::fleet::FleetManager;
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::types::NodeId;
    use super::super::config::ImpactConfig;
    use super::super::guard::GuardEvaluator;
    use super::super::impact::ImpactAssessor;
    use super::super::lock::LockManager;
    use super::super::operator::{OperatorStore, ProfilePreset};
    use super::super::rollback::RollbackManager;

    struct MockTierHandler {
        tier_val: InterventionTier,
        result: OaiResult<serde_json::Value>,
    }

    impl MockTierHandler {
        fn success(tier: InterventionTier) -> Self {
            Self {
                tier_val: tier,
                result: Ok(serde_json::json!({"status": "ok"})),
            }
        }

        fn failing(tier: InterventionTier, err: OaiError) -> Self {
            Self {
                tier_val: tier,
                result: Err(err),
            }
        }
    }

    impl TierHandler for MockTierHandler {
        fn tier(&self) -> InterventionTier {
            self.tier_val
        }

        fn execute(
            &self,
            _target: &EntityRef,
            _action: &str,
            _params: &serde_json::Value,
        ) -> OaiResult<serde_json::Value> {
            match &self.result {
                Ok(val) => Ok(val.clone()),
                Err(e) => Err(OaiError::SubsystemError {
                    subsystem: "mock".to_string(),
                    message: format!("{}", e),
                }),
            }
        }

        fn urgency(&self, _action: &str) -> InterventionUrgency {
            InterventionUrgency::Operational
        }

        fn reversibility(&self, _action: &str) -> Reversibility {
            Reversibility::Reversible
        }

        fn post_conditions(
            &self,
            _target: &EntityRef,
            _action: &str,
            _params: &serde_json::Value,
        ) -> Vec<PostCondition> {
            vec![]
        }

        fn rollback(
            &self,
            _target: &EntityRef,
            _action: &str,
            _snapshot_before: &serde_json::Value,
        ) -> OaiResult<()> {
            Ok(())
        }
    }

    fn make_engine(
        operator_presets: Vec<(&str, ProfilePreset)>,
    ) -> InterventionEngine {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let event_bus = Arc::new(EventBus::new(1024));
        let fleet = Arc::new(FleetManager::new());
        let config = OaiConfig::default();

        let guard_eval = Arc::new(GuardEvaluator::new(knowledge.clone(), fleet.clone()));
        let lock_mgr = Arc::new(LockManager::new());
        let impact = Arc::new(ImpactAssessor::new(
            knowledge,
            fleet,
            ImpactConfig::default(),
        ));
        let rollback = Arc::new(RollbackManager::new(Duration::from_secs(10)));
        let operator_store = Arc::new(OperatorStore::new());
        let audit = Arc::new(AuditLog::new());

        for (id, preset) in &operator_presets {
            operator_store.register_from_preset(
                OperatorId::new(*id),
                id.to_string(),
                *preset,
            );
        }

        InterventionEngine::new(
            config,
            guard_eval,
            lock_mgr,
            impact,
            rollback,
            operator_store,
            audit,
            event_bus,
        )
    }

    fn make_request(
        operator_id: &str,
        tier: InterventionTier,
        action: &str,
    ) -> InterventionRequest {
        InterventionRequest {
            id: InterventionId::new(),
            action: InterventionAction {
                tier,
                name: action.to_string(),
            },
            target: EntityRef {
                entity_type: EntityType::Node,
                id: NodeId::new().0.to_string(),
            },
            guard: None,
            urgency: InterventionUrgency::Operational,
            operator_id: OperatorId::new(operator_id),
            session_id: None,
            skip_guard: false,
            force: false,
            dry_run: false,
            params: serde_json::json!({}),
        }
    }

    #[test]
    fn test_execute_dry_run() {
        let mut engine = make_engine(vec![("admin", ProfilePreset::PlatformAdmin)]);
        engine.register_handler(Arc::new(MockTierHandler::success(
            InterventionTier::NodeLifecycle,
        )));

        let mut req = make_request("admin", InterventionTier::NodeLifecycle, "drain");
        req.dry_run = true;

        let result = engine.execute(req).expect("dry run should succeed");
        assert!(!result.executed);
        assert!(result.result.get("dry_run").is_some());
    }

    #[test]
    fn test_execute_unknown_operator() {
        let mut engine = make_engine(vec![]);
        engine.register_handler(Arc::new(MockTierHandler::success(
            InterventionTier::NodeLifecycle,
        )));

        let req = make_request("nonexistent", InterventionTier::NodeLifecycle, "drain");
        let result = engine.execute(req);
        assert!(result.is_err());
        assert!(matches!(result, Err(OaiError::EntityNotFound { .. })));
    }

    #[test]
    fn test_execute_tier_permission_denied() {
        let mut engine = make_engine(vec![("spectator", ProfilePreset::Spectator)]);
        engine.register_handler(Arc::new(MockTierHandler::success(
            InterventionTier::NodeLifecycle,
        )));

        let req = make_request("spectator", InterventionTier::NodeLifecycle, "drain");
        let result = engine.execute(req);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(OaiError::TierPermissionDenied { .. })
        ));
    }

    #[test]
    fn test_execute_success() {
        let mut engine = make_engine(vec![("admin", ProfilePreset::PlatformAdmin)]);
        engine.register_handler(Arc::new(MockTierHandler::success(
            InterventionTier::NodeLifecycle,
        )));

        let req = make_request("admin", InterventionTier::NodeLifecycle, "drain");
        let result = engine.execute(req).expect("should succeed");
        assert!(result.executed);
        assert!(result.audit_entry_id.is_some());
    }

    #[test]
    fn test_execute_conflict() {
        let mut engine = make_engine(vec![("admin", ProfilePreset::PlatformAdmin)]);
        engine.register_handler(Arc::new(MockTierHandler::success(
            InterventionTier::NodeLifecycle,
        )));

        // Create a target that we'll lock manually
        let target = EntityRef {
            entity_type: EntityType::Node,
            id: NodeId::new().0.to_string(),
        };

        // Pre-acquire a lock on the target
        engine
            .lock_manager
            .try_acquire(
                target.clone(),
                InterventionId::new(),
                OperatorId::new("other"),
                "cordon".to_string(),
                Duration::from_secs(300),
            )
            .unwrap();

        // Try to intervene on the same target
        let mut req =
            make_request("admin", InterventionTier::NodeLifecycle, "drain");
        req.target = target;

        let result = engine.execute(req);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(OaiError::InterventionConflict { .. })
        ));
    }

    #[test]
    fn test_execute_no_handler() {
        let engine = make_engine(vec![("admin", ProfilePreset::PlatformAdmin)]);
        // Don't register any handler

        let req = make_request("admin", InterventionTier::NodeLifecycle, "drain");
        let result = engine.execute(req);
        assert!(result.is_err());
        assert!(matches!(result, Err(OaiError::SubsystemDisabled { .. })));
    }

    #[test]
    fn test_execute_with_force() {
        let mut engine = make_engine(vec![("eng", ProfilePreset::Engineer)]);
        engine.register_handler(Arc::new(MockTierHandler::success(
            InterventionTier::NodeLifecycle,
        )));

        // With force=true, should bypass safety blocks
        let mut req = make_request("eng", InterventionTier::NodeLifecycle, "drain");
        req.force = true;

        let result = engine.execute(req).expect("force should bypass blocks");
        assert!(result.executed);
    }
}
