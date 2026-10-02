// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use uuid::Uuid;

use crate::swarm::fleet::{FleetError, FleetManager};
use crate::swarm::types::NodeId;
use super::engine::TierHandler;
use super::errors::{OaiError, OaiResult};
use super::rollback::PostCondition;
use super::types::*;

fn parse_node_id(target: &EntityRef) -> OaiResult<NodeId> {
    Uuid::parse_str(&target.id)
        .map(NodeId)
        .map_err(|_| OaiError::EntityNotFound {
            entity: target.clone(),
        })
}

pub struct Tier1Handler {
    fleet: Arc<FleetManager>,
}

impl Tier1Handler {
    pub fn new(fleet: Arc<FleetManager>) -> Self {
        Self { fleet }
    }

    fn map_fleet_error(&self, err: FleetError, target: &EntityRef) -> OaiError {
        match err {
            FleetError::NodeNotFound(_) => OaiError::EntityNotFound {
                entity: target.clone(),
            },
            FleetError::InvalidState {
                current_state,
                attempted,
                ..
            } => OaiError::InvalidEntityState {
                entity: target.clone(),
                current_state,
                required_states: vec![],
                action: attempted,
            },
            other => OaiError::SubsystemError {
                subsystem: "fleet_manager".to_string(),
                message: other.to_string(),
            },
        }
    }
}

impl TierHandler for Tier1Handler {
    fn tier(&self) -> InterventionTier {
        InterventionTier::NodeLifecycle
    }

    fn execute(
        &self,
        target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> OaiResult<serde_json::Value> {
        let node_id = parse_node_id(target)?;

        match action {
            "drain" => {
                let timeout_secs = params
                    .get("timeout_secs")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(300);
                let reason = params
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("operator-initiated drain")
                    .to_string();

                self.fleet
                    .drain_node(node_id, timeout_secs, reason)
                    .map_err(|e| self.map_fleet_error(e, target))?;

                Ok(serde_json::json!({
                    "action": "drain",
                    "node_id": target.id,
                    "timeout_secs": timeout_secs,
                    "status": "draining"
                }))
            }

            "cordon" => {
                let reason = params
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("operator-initiated cordon")
                    .to_string();

                self.fleet
                    .cordon_node(node_id, reason)
                    .map_err(|e| self.map_fleet_error(e, target))?;

                Ok(serde_json::json!({
                    "action": "cordon",
                    "node_id": target.id,
                    "status": "cordoned"
                }))
            }

            "uncordon" => {
                self.fleet
                    .uncordon_node(node_id)
                    .map_err(|e| self.map_fleet_error(e, target))?;

                Ok(serde_json::json!({
                    "action": "uncordon",
                    "node_id": target.id,
                    "status": "normal"
                }))
            }

            "quarantine" => {
                let reason = params
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("operator-initiated quarantine")
                    .to_string();

                self.fleet
                    .quarantine_node(node_id, reason, Some("operator".to_string()))
                    .map_err(|e| self.map_fleet_error(e, target))?;

                Ok(serde_json::json!({
                    "action": "quarantine",
                    "node_id": target.id,
                    "status": "quarantined"
                }))
            }

            "unquarantine" => {
                self.fleet
                    .unquarantine_node(node_id)
                    .map_err(|e| self.map_fleet_error(e, target))?;

                Ok(serde_json::json!({
                    "action": "unquarantine",
                    "node_id": target.id,
                    "status": "normal"
                }))
            }

            _ => Err(OaiError::SubsystemError {
                subsystem: "tier1".to_string(),
                message: format!("unknown action: {}", action),
            }),
        }
    }

    fn urgency(&self, action: &str) -> InterventionUrgency {
        match action {
            "quarantine" | "unquarantine" => InterventionUrgency::Emergency,
            _ => InterventionUrgency::Operational,
        }
    }

    fn reversibility(&self, action: &str) -> Reversibility {
        match action {
            "cordon" => Reversibility::Reversible,
            "drain" => Reversibility::SelfResolving,
            "quarantine" => Reversibility::Reversible,
            "uncordon" | "unquarantine" => Reversibility::Irreversible,
            _ => Reversibility::Irreversible,
        }
    }

    fn post_conditions(
        &self,
        target: &EntityRef,
        action: &str,
        _params: &serde_json::Value,
    ) -> Vec<PostCondition> {
        match action {
            "cordon" => vec![PostCondition {
                description: format!("Node {} should be in Cordoned state", target.id),
                check: "fleet_state_eq".to_string(),
                expected: serde_json::json!("Cordoned"),
            }],
            "drain" => vec![PostCondition {
                description: format!("Node {} should be in Draining state", target.id),
                check: "fleet_state_eq".to_string(),
                expected: serde_json::json!("Draining"),
            }],
            "quarantine" => vec![PostCondition {
                description: format!("Node {} should be in Quarantined state", target.id),
                check: "fleet_state_eq".to_string(),
                expected: serde_json::json!("Quarantined"),
            }],
            _ => Vec::new(),
        }
    }

    fn rollback(
        &self,
        target: &EntityRef,
        action: &str,
        _snapshot_before: &serde_json::Value,
    ) -> OaiResult<()> {
        let node_id = parse_node_id(target)?;
        match action {
            "cordon" => self
                .fleet
                .uncordon_node(node_id)
                .map_err(|e| self.map_fleet_error(e, target)),
            "quarantine" => self
                .fleet
                .unquarantine_node(node_id)
                .map_err(|e| self.map_fleet_error(e, target)),
            _ => Err(OaiError::SubsystemError {
                subsystem: "tier1".to_string(),
                message: format!("action '{}' is not reversible", action),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::fleet::{FleetManager, FleetNodeState};

    fn make_handler() -> Tier1Handler {
        let fleet = Arc::new(FleetManager::new());
        Tier1Handler::new(fleet)
    }

    fn node_target(node_id: &NodeId) -> EntityRef {
        EntityRef {
            entity_type: EntityType::Node,
            id: node_id.0.to_string(),
        }
    }

    #[test]
    fn test_tier1_unknown_action() {
        let handler = make_handler();
        let target = EntityRef {
            entity_type: EntityType::Node,
            id: NodeId::new().0.to_string(),
        };
        let result = handler.execute(&target, "explode", &serde_json::json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_tier1_urgency_quarantine_is_emergency() {
        let handler = make_handler();
        assert_eq!(handler.urgency("quarantine"), InterventionUrgency::Emergency);
    }

    #[test]
    fn test_tier1_urgency_drain_is_operational() {
        let handler = make_handler();
        assert_eq!(handler.urgency("drain"), InterventionUrgency::Operational);
    }

    #[test]
    fn test_tier1_reversibility_cordon() {
        let handler = make_handler();
        assert_eq!(handler.reversibility("cordon"), Reversibility::Reversible);
    }

    #[test]
    fn test_tier1_reversibility_drain() {
        let handler = make_handler();
        assert_eq!(handler.reversibility("drain"), Reversibility::SelfResolving);
    }

    #[test]
    fn test_tier1_post_conditions_cordon() {
        let handler = make_handler();
        let target = EntityRef {
            entity_type: EntityType::Node,
            id: "test".to_string(),
        };
        let pcs = handler.post_conditions(&target, "cordon", &serde_json::json!({}));
        assert_eq!(pcs.len(), 1);
        assert!(pcs[0].description.contains("Cordoned"));
    }

    #[test]
    fn test_tier1_cordon_success() {
        let fleet = Arc::new(FleetManager::new());
        let node_id = NodeId::new();
        // Register the node in fleet store so cordon can find it
        fleet.fleet_store().set_state(node_id.clone(), FleetNodeState::Normal, None);
        let handler = Tier1Handler::new(fleet);
        let target = node_target(&node_id);
        let result = handler.execute(
            &target,
            "cordon",
            &serde_json::json!({"reason": "test cordon"}),
        );
        assert!(result.is_ok());
        let val = result.unwrap();
        assert_eq!(val["status"], "cordoned");
    }

    #[test]
    fn test_tier1_uncordon_not_cordoned() {
        let fleet = Arc::new(FleetManager::new());
        let node_id = NodeId::new();
        fleet.fleet_store().set_state(node_id.clone(), FleetNodeState::Normal, None);
        let handler = Tier1Handler::new(fleet);
        let target = node_target(&node_id);
        let result = handler.execute(&target, "uncordon", &serde_json::json!({}));
        // Uncordoning a Normal node should fail with InvalidState
        assert!(result.is_err());
    }

    #[test]
    fn test_tier1_drain_unknown_node_succeeds() {
        // FleetManager defaults unknown nodes to Normal, so drain succeeds
        let handler = make_handler();
        let target = EntityRef {
            entity_type: EntityType::Node,
            id: NodeId::new().0.to_string(),
        };
        let result = handler.execute(
            &target,
            "drain",
            &serde_json::json!({"timeout_secs": 60}),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["status"], "draining");
    }
}
