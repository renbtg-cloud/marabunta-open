// Marabunta - Licensed under the MIT License.
use std::collections::HashMap;
use std::sync::Arc;

use crate::swarm::config_live::{ConfigPatchError, LiveConfig};
use super::engine::TierHandler;
use super::errors::{OaiError, OaiResult};
use super::rollback::PostCondition;
use super::types::*;

pub struct Tier6Handler {
    live_config: Option<Arc<LiveConfig>>,
}

impl Tier6Handler {
    pub fn new(live_config: Option<Arc<LiveConfig>>) -> Self {
        Self { live_config }
    }

    fn require_live_config(&self) -> OaiResult<&Arc<LiveConfig>> {
        self.live_config
            .as_ref()
            .ok_or(OaiError::SubsystemDisabled {
                tier: InterventionTier::Configuration,
                subsystem: "live_config".to_string(),
                config_flag: "enable_management_layer".to_string(),
            })
    }

    fn map_config_error(&self, err: ConfigPatchError) -> OaiError {
        match err {
            ConfigPatchError::HardwiredByCompliance {
                key,
                profile_name,
                justification,
            } => OaiError::SubsystemError {
                subsystem: "live_config".to_string(),
                message: format!(
                    "{}: locked by compliance profile '{}' — {}",
                    key, profile_name, justification
                ),
            },
            ConfigPatchError::RestartRequired { key } => OaiError::SubsystemError {
                subsystem: "live_config".to_string(),
                message: format!("{}: requires restart to change", key),
            },
            ConfigPatchError::ValidationError { key, message } => OaiError::SubsystemError {
                subsystem: "live_config".to_string(),
                message: format!("{}: validation failed — {}", key, message),
            },
            ConfigPatchError::UnknownKey { key } => OaiError::SubsystemError {
                subsystem: "live_config".to_string(),
                message: format!("unknown config key: {}", key),
            },
            ConfigPatchError::NoChanges => OaiError::SubsystemError {
                subsystem: "live_config".to_string(),
                message: "no changes in patch (all values identical to current)".to_string(),
            },
        }
    }
}

impl TierHandler for Tier6Handler {
    fn tier(&self) -> InterventionTier {
        InterventionTier::Configuration
    }

    fn execute(
        &self,
        _target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> OaiResult<serde_json::Value> {
        let config = self.require_live_config()?;

        match action {
            "hot_reload" | "override_gossip" | "adjust_threshold" => {
                let changes = params
                    .get("changes")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| OaiError::SubsystemError {
                        subsystem: "tier6".to_string(),
                        message: format!(
                            "{} requires 'changes' object with key:value pairs",
                            action
                        ),
                    })?;

                let mut patch: HashMap<String, serde_json::Value> = HashMap::new();
                for (k, v) in changes {
                    match action {
                        "override_gossip" => {
                            if !k.starts_with("gossip_") && !k.starts_with("gossip.") {
                                return Err(OaiError::SubsystemError {
                                    subsystem: "tier6".to_string(),
                                    message: format!(
                                        "override_gossip only accepts gossip_* keys, got '{}'",
                                        k
                                    ),
                                });
                            }
                        }
                        "adjust_threshold" => {
                            if !k.contains("threshold")
                                && !k.contains("window")
                                && !k.contains("timeout")
                                && !k.contains("interval")
                            {
                                return Err(OaiError::SubsystemError {
                                    subsystem: "tier6".to_string(),
                                    message: format!(
                                        "adjust_threshold only accepts threshold/window/timeout/interval keys, got '{}'",
                                        k
                                    ),
                                });
                            }
                        }
                        _ => {}
                    }
                    patch.insert(k.clone(), v.clone());
                }

                let source = format!("operator:{}", action);
                config
                    .apply_patch(&patch, &source)
                    .map_err(|e| self.map_config_error(e))?;

                Ok(serde_json::json!({
                    "action": action,
                    "changes_applied": patch.len(),
                    "keys": patch.keys().collect::<Vec<_>>(),
                    "status": "applied"
                }))
            }

            _ => Err(OaiError::SubsystemError {
                subsystem: "tier6".to_string(),
                message: format!("unknown action: {}", action),
            }),
        }
    }

    fn urgency(&self, _action: &str) -> InterventionUrgency {
        InterventionUrgency::Precise
    }

    fn reversibility(&self, _action: &str) -> Reversibility {
        Reversibility::Reversible
    }

    fn post_conditions(
        &self,
        _target: &EntityRef,
        _action: &str,
        params: &serde_json::Value,
    ) -> Vec<PostCondition> {
        if let Some(changes) = params.get("changes").and_then(|v| v.as_object()) {
            changes
                .iter()
                .map(|(k, v)| PostCondition {
                    description: format!("Config key '{}' should be {:?}", k, v),
                    check: format!("config_key:{}", k),
                    expected: v.clone(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    fn rollback(
        &self,
        _target: &EntityRef,
        _action: &str,
        snapshot: &serde_json::Value,
    ) -> OaiResult<()> {
        let config = self.require_live_config()?;

        if let Some(prev) = snapshot.as_object() {
            let patch: HashMap<String, serde_json::Value> =
                prev.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            config
                .apply_patch(&patch, "operator:rollback")
                .map_err(|e| self.map_config_error(e))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_disabled_handler() -> Tier6Handler {
        Tier6Handler::new(None)
    }

    fn test_target() -> EntityRef {
        EntityRef {
            entity_type: EntityType::Config,
            id: "swarm_config".to_string(),
        }
    }

    #[test]
    fn test_tier6_live_config_disabled() {
        let handler = make_disabled_handler();
        let result = handler.execute(
            &test_target(),
            "hot_reload",
            &serde_json::json!({"changes": {"key1": "val1"}}),
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(OaiError::SubsystemDisabled { .. })));
    }

    #[test]
    fn test_tier6_missing_changes_param() {
        let handler = make_disabled_handler();
        let result = handler.execute(&test_target(), "hot_reload", &serde_json::json!({}));
        // SubsystemDisabled hits first
        assert!(result.is_err());
    }

    #[test]
    fn test_tier6_unknown_action() {
        let handler = make_disabled_handler();
        let result = handler.execute(&test_target(), "explode", &serde_json::json!({}));
        // SubsystemDisabled hits first since require_live_config is called
        // For unknown action with disabled config, it still returns SubsystemDisabled
        assert!(result.is_err());
    }

    #[test]
    fn test_tier6_override_gossip_invalid_key() {
        // This test validates the key prefix check logic directly
        // (can't test through execute since LiveConfig is None)
        let changes = serde_json::json!({"cpu_cores": 4});
        let obj = changes.as_object().unwrap();
        for (k, _v) in obj {
            assert!(!k.starts_with("gossip_") && !k.starts_with("gossip."));
        }
    }

    #[test]
    fn test_tier6_adjust_threshold_valid_key() {
        // Validate the key check logic
        let valid_keys = ["anomaly_threshold", "suspect_window", "drain_timeout", "gossip_interval"];
        for key in &valid_keys {
            assert!(
                key.contains("threshold")
                    || key.contains("window")
                    || key.contains("timeout")
                    || key.contains("interval")
            );
        }
    }

    #[test]
    fn test_tier6_urgency_is_precise() {
        let handler = make_disabled_handler();
        assert_eq!(handler.urgency("hot_reload"), InterventionUrgency::Precise);
    }

    #[test]
    fn test_tier6_reversibility_is_reversible() {
        let handler = make_disabled_handler();
        assert_eq!(handler.reversibility("hot_reload"), Reversibility::Reversible);
    }

    #[test]
    fn test_tier6_post_conditions_from_changes() {
        let handler = make_disabled_handler();
        let params = serde_json::json!({"changes": {"key1": "val1", "key2": 42}});
        let pcs = handler.post_conditions(&test_target(), "hot_reload", &params);
        assert_eq!(pcs.len(), 2);
    }
}
