// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use crate::swarm::admission::AdmissionStore;
use crate::swarm::collective::CollectiveStore;
use crate::swarm::policy::PolicyEngine;
use crate::swarm::reputation::ReputationStore;
use super::engine::TierHandler;
use super::errors::{OaiError, OaiResult};
use super::rollback::PostCondition;
use super::types::*;

pub struct Tier3Handler {
    _collective_store: Arc<CollectiveStore>,
    _reputation_store: Arc<ReputationStore>,
    _admission_store: Arc<AdmissionStore>,
    _policy_engine: Arc<PolicyEngine>,
}

impl Tier3Handler {
    pub fn new(
        collective_store: Arc<CollectiveStore>,
        reputation_store: Arc<ReputationStore>,
        admission_store: Arc<AdmissionStore>,
        policy_engine: Arc<PolicyEngine>,
    ) -> Self {
        Self {
            _collective_store: collective_store,
            _reputation_store: reputation_store,
            _admission_store: admission_store,
            _policy_engine: policy_engine,
        }
    }
}

impl TierHandler for Tier3Handler {
    fn tier(&self) -> InterventionTier {
        InterventionTier::Organic
    }

    fn execute(
        &self,
        target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> OaiResult<serde_json::Value> {
        match action {
            "eject_collective_member" => {
                let collective_id_str =
                    params
                        .get("collective_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| OaiError::SubsystemError {
                            subsystem: "tier3".to_string(),
                            message: "eject requires 'collective_id' parameter".to_string(),
                        })?;

                let reason = params
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("operator-initiated ejection");

                Ok(serde_json::json!({
                    "action": "eject_collective_member",
                    "node_id": target.id,
                    "collective_id": collective_id_str,
                    "reason": reason,
                    "status": "ejected"
                }))
            }

            

            "adjust_reputation" => {
                let badge_type =
                    params
                        .get("badge")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| OaiError::SubsystemError {
                            subsystem: "tier3".to_string(),
                            message: "adjust_reputation requires 'badge' parameter"
                                .to_string(),
                        })?;
                let add = params
                    .get("add")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                let reason = params
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("operator adjustment");

                Ok(serde_json::json!({
                    "action": "adjust_reputation",
                    "node_id": target.id,
                    "badge": badge_type,
                    "added": add,
                    "reason": reason,
                    "status": "adjusted"
                }))
            }

            "override_admission_verdict" => {
                let request_id = target.id.clone();
                let verdict =
                    params
                        .get("verdict")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| OaiError::SubsystemError {
                            subsystem: "tier3".to_string(),
                            message:
                                "override_admission_verdict requires 'verdict' (approve/reject)"
                                    .to_string(),
                        })?;
                let reason = params
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("operator override");

                if verdict != "approve" && verdict != "reject" {
                    return Err(OaiError::SubsystemError {
                        subsystem: "tier3".to_string(),
                        message: format!(
                            "verdict must be 'approve' or 'reject', got '{}'",
                            verdict
                        ),
                    });
                }

                Ok(serde_json::json!({
                    "action": "override_admission_verdict",
                    "request_id": request_id,
                    "verdict": verdict,
                    "reason": reason,
                    "status": "overridden"
                }))
            }

            "modify_policy" => {
                let policy_field =
                    params
                        .get("field")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| OaiError::SubsystemError {
                            subsystem: "tier3".to_string(),
                            message: "modify_policy requires 'field' parameter".to_string(),
                        })?;
                let value =
                    params
                        .get("value")
                        .ok_or_else(|| OaiError::SubsystemError {
                            subsystem: "tier3".to_string(),
                            message: "modify_policy requires 'value' parameter".to_string(),
                        })?;

                Ok(serde_json::json!({
                    "action": "modify_policy",
                    "field": policy_field,
                    "value": value,
                    "status": "modified"
                }))
            }

            _ => Err(OaiError::SubsystemError {
                subsystem: "tier3".to_string(),
                message: format!("unknown action: {}", action),
            }),
        }
    }

    fn urgency(&self, action: &str) -> InterventionUrgency {
        match action {
            "eject_collective_member" => InterventionUrgency::Operational,
            
            "adjust_reputation" => InterventionUrgency::Precise,
            "override_admission_verdict" => InterventionUrgency::Operational,
            "modify_policy" => InterventionUrgency::Precise,
            _ => InterventionUrgency::Operational,
        }
    }

    fn reversibility(&self, action: &str) -> Reversibility {
        match action {
            "adjust_reputation" | "modify_policy" => Reversibility::Reversible,
            _ => Reversibility::Irreversible,
        }
    }

    fn post_conditions(
        &self,
        _target: &EntityRef,
        _action: &str,
        _params: &serde_json::Value,
    ) -> Vec<PostCondition> {
        Vec::new()
    }

    fn rollback(
        &self,
        _target: &EntityRef,
        action: &str,
        _snapshot: &serde_json::Value,
    ) -> OaiResult<()> {
        match action {
            "adjust_reputation" | "modify_policy" => Ok(()),
            _ => Err(OaiError::SubsystemError {
                subsystem: "tier3".to_string(),
                message: format!("action '{}' is not reversible", action),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::admission::AdmissionStore;
    use crate::swarm::collective::CollectiveStore;
    use crate::swarm::policy::{PolicyEngine, PolicySet};
    use crate::swarm::reputation::ReputationStore;

    fn make_handler() -> Tier3Handler {
        Tier3Handler::new(
            Arc::new(CollectiveStore::new()),
            Arc::new(ReputationStore::new()),
            Arc::new(AdmissionStore::new()),
            Arc::new(PolicyEngine::new(PolicySet::default())),
        )
    }

    fn test_target() -> EntityRef {
        EntityRef {
            entity_type: EntityType::Node,
            id: "test-node-id".to_string(),
        }
    }

    #[test]
    fn test_tier3_eject_success() {
        let handler = make_handler();
        let result = handler.execute(
            &test_target(),
            "eject_collective_member",
            &serde_json::json!({"collective_id": "coll-1", "reason": "test"}),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["status"], "ejected");
    }

    #[test]
    fn test_tier3_eject_missing_collective_id() {
        let handler = make_handler();
        let result = handler.execute(
            &test_target(),
            "eject_collective_member",
            &serde_json::json!({}),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_tier3_override_award_success() {
        let handler = make_handler();
        let result = handler.execute(
            &test_target(),
            
            &serde_json::json!({"posting_id": "p1", "winner_id": "w1"}),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["status"], "overridden");
    }

    #[test]
    fn test_tier3_adjust_reputation_add() {
        let handler = make_handler();
        let result = handler.execute(
            &test_target(),
            "adjust_reputation",
            &serde_json::json!({"badge": "reliable", "add": true}),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["added"], true);
    }

    #[test]
    fn test_tier3_override_admission_approve() {
        let handler = make_handler();
        let result = handler.execute(
            &test_target(),
            "override_admission_verdict",
            &serde_json::json!({"verdict": "approve", "reason": "test"}),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["verdict"], "approve");
    }

    #[test]
    fn test_tier3_override_admission_invalid_verdict() {
        let handler = make_handler();
        let result = handler.execute(
            &test_target(),
            "override_admission_verdict",
            &serde_json::json!({"verdict": "maybe"}),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_tier3_modify_policy_success() {
        let handler = make_handler();
        let result = handler.execute(
            &test_target(),
            "modify_policy",
            &serde_json::json!({"field": "max_nodes", "value": 8}),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["status"], "modified");
    }

    #[test]
    fn test_tier3_unknown_action() {
        let handler = make_handler();
        let result = handler.execute(&test_target(), "explode", &serde_json::json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_tier3_urgency_values() {
        let handler = make_handler();
        assert_eq!(
            handler.urgency("eject_collective_member"),
            InterventionUrgency::Operational
        );
        assert_eq!(
            handler.urgency("override_marketplace_award"),
            InterventionUrgency::Precise
        );
    }
}
