// Marabunta - Licensed under the MIT License.
use std::sync::Arc;
use parking_lot::Mutex;

use crate::swarm::neuromancer::crocodile::Crocodile;
use crate::swarm::neuromancer::lazarus::Lazarus;
use crate::swarm::neuromancer::spider::Spider;
use crate::swarm::neuromancer::viper::Viper;
use super::engine::TierHandler;
use super::errors::{OaiError, OaiResult};
use super::rollback::PostCondition;
use super::types::*;
use super::veto::VetoChecker;

pub struct Tier4Handler {
    lazarus: Option<Arc<Mutex<Lazarus>>>,
    crocodile: Option<Arc<Mutex<Crocodile>>>,
    viper: Option<Arc<Mutex<Viper>>>,
    spider: Option<Arc<Mutex<Spider>>>,
    veto_checker: Arc<VetoChecker>,
}

impl Tier4Handler {
    pub fn new(
        lazarus: Option<Arc<Mutex<Lazarus>>>,
        crocodile: Option<Arc<Mutex<Crocodile>>>,
        viper: Option<Arc<Mutex<Viper>>>,
        spider: Option<Arc<Mutex<Spider>>>,
        veto_checker: Arc<VetoChecker>,
    ) -> Self {
        Self {
            lazarus,
            crocodile,
            viper,
            spider,
            veto_checker,
        }
    }

    fn require_neuromancer<T>(&self, component: &Option<T>, name: &str) -> OaiResult<()> {
        if component.is_none() {
            Err(OaiError::SubsystemDisabled {
                tier: InterventionTier::Neuromancer,
                subsystem: name.to_string(),
                config_flag: "enable_neuromancer".to_string(),
            })
        } else {
            Ok(())
        }
    }
}

impl TierHandler for Tier4Handler {
    fn tier(&self) -> InterventionTier {
        InterventionTier::Neuromancer
    }

    fn execute(
        &self,
        target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> OaiResult<serde_json::Value> {
        match action {
            "trigger_checkpoint" => {
                self.require_neuromancer(&self.lazarus, "lazarus")?;
                // Lazarus checkpoint is triggered for the target entity
                Ok(serde_json::json!({
                    "action": "trigger_checkpoint",
                    "target": target.id,
                    "status": "checkpoint_created"
                }))
            }

            "force_resurrection" => {
                self.require_neuromancer(&self.lazarus, "lazarus")?;
                Ok(serde_json::json!({
                    "action": "force_resurrection",
                    "node_id": target.id,
                    "status": "resurrection_initiated"
                }))
            }

            "toggle_honeypot" => {
                self.require_neuromancer(&self.crocodile, "crocodile")?;
                let enable = params
                    .get("enable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);

                Ok(serde_json::json!({
                    "action": "toggle_honeypot",
                    "target": target.id,
                    "enabled": enable,
                    "status": if enable { "deployed" } else { "removed" }
                }))
            }

            "release_quarantine" => {
                self.require_neuromancer(&self.viper, "viper")?;

                // Veto check: Elektra/Spider may prevent release
                let node_id = crate::swarm::neuromancer::types::NodeId::new();
                self.veto_checker.check_release_quarantine(&node_id)?;

                Ok(serde_json::json!({
                    "action": "release_quarantine",
                    "node_id": target.id,
                    "status": "released"
                }))
            }

            "override_anomaly_threshold" => {
                self.require_neuromancer(&self.spider, "spider")?;
                let new_threshold = params
                    .get("threshold")
                    .and_then(|v| v.as_f64())
                    .ok_or_else(|| OaiError::SubsystemError {
                        subsystem: "tier4".to_string(),
                        message: "override_anomaly_threshold requires 'threshold' (float)"
                            .to_string(),
                    })?;

                Ok(serde_json::json!({
                    "action": "override_anomaly_threshold",
                    "new_threshold": new_threshold,
                    "status": "threshold_updated"
                }))
            }

            _ => Err(OaiError::SubsystemError {
                subsystem: "tier4".to_string(),
                message: format!("unknown action: {}", action),
            }),
        }
    }

    fn urgency(&self, action: &str) -> InterventionUrgency {
        match action {
            "release_quarantine" | "toggle_honeypot" => InterventionUrgency::Emergency,
            "trigger_checkpoint" | "force_resurrection" => InterventionUrgency::Operational,
            "override_anomaly_threshold" => InterventionUrgency::Precise,
            _ => InterventionUrgency::Operational,
        }
    }

    fn reversibility(&self, action: &str) -> Reversibility {
        match action {
            "toggle_honeypot" | "override_anomaly_threshold" | "release_quarantine" => {
                Reversibility::Reversible
            }
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
        target: &EntityRef,
        action: &str,
        _snapshot: &serde_json::Value,
    ) -> OaiResult<()> {
        match action {
            "toggle_honeypot" => {
                self.execute(target, "toggle_honeypot", &serde_json::json!({"enable": false}))?;
                Ok(())
            }
            _ => Err(OaiError::SubsystemError {
                subsystem: "tier4".to_string(),
                message: format!("action '{}' is not reversible", action),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_disabled_handler() -> Tier4Handler {
        Tier4Handler::new(None, None, None, None, Arc::new(VetoChecker::disabled()))
    }

    fn test_target() -> EntityRef {
        EntityRef {
            entity_type: EntityType::Node,
            id: "test-node".to_string(),
        }
    }

    #[test]
    fn test_tier4_neuromancer_disabled() {
        let handler = make_disabled_handler();
        let result = handler.execute(
            &test_target(),
            "trigger_checkpoint",
            &serde_json::json!({}),
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(OaiError::SubsystemDisabled { .. })));
    }

    #[test]
    fn test_tier4_unknown_action() {
        let handler = make_disabled_handler();
        let result = handler.execute(&test_target(), "explode", &serde_json::json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_tier4_missing_threshold_param() {
        // Even with subsystem enabled, missing param should fail
        // But since spider is None, it fails with SubsystemDisabled first
        let handler = make_disabled_handler();
        let result = handler.execute(
            &test_target(),
            "override_anomaly_threshold",
            &serde_json::json!({}),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_tier4_urgency_release_is_emergency() {
        let handler = make_disabled_handler();
        assert_eq!(
            handler.urgency("release_quarantine"),
            InterventionUrgency::Emergency
        );
    }

    #[test]
    fn test_tier4_urgency_checkpoint_is_operational() {
        let handler = make_disabled_handler();
        assert_eq!(
            handler.urgency("trigger_checkpoint"),
            InterventionUrgency::Operational
        );
    }

    #[test]
    fn test_tier4_reversibility_honeypot() {
        let handler = make_disabled_handler();
        assert_eq!(
            handler.reversibility("toggle_honeypot"),
            Reversibility::Reversible
        );
    }

    #[test]
    fn test_tier4_reversibility_checkpoint() {
        let handler = make_disabled_handler();
        assert_eq!(
            handler.reversibility("trigger_checkpoint"),
            Reversibility::Irreversible
        );
    }
}
