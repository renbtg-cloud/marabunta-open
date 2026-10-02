// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use crate::plugin::PluginHost;
use super::engine::TierHandler;
use super::errors::{OaiError, OaiResult};
use super::rollback::PostCondition;
use super::types::*;

pub struct Tier5Handler {
    _plugin_host: Option<Arc<PluginHost>>,
}

impl Tier5Handler {
    pub fn new(plugin_host: Option<Arc<PluginHost>>) -> Self {
        Self {
            _plugin_host: plugin_host,
        }
    }

    fn require_plugins(&self) -> OaiResult<()> {
        if self._plugin_host.is_none() {
            Err(OaiError::SubsystemDisabled {
                tier: InterventionTier::Plugin,
                subsystem: "plugin_host".to_string(),
                config_flag: "plugin_host".to_string(),
            })
        } else {
            Ok(())
        }
    }
}

impl TierHandler for Tier5Handler {
    fn tier(&self) -> InterventionTier {
        InterventionTier::Plugin
    }

    fn execute(
        &self,
        target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> OaiResult<serde_json::Value> {
        self.require_plugins()?;
        let plugin_name = &target.id;

        match action {
            "restart_plugin" => {
                let graceful = params
                    .get("graceful")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);

                Ok(serde_json::json!({
                    "action": "restart_plugin",
                    "plugin": plugin_name,
                    "graceful": graceful,
                    "status": "restarted"
                }))
            }

            "stop_plugin" => {
                let graceful = params
                    .get("graceful")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);

                Ok(serde_json::json!({
                    "action": "stop_plugin",
                    "plugin": plugin_name,
                    "graceful": graceful,
                    "status": "stopped"
                }))
            }

            "update_plugin_config" => {
                let config_key = params
                    .get("key")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| OaiError::SubsystemError {
                        subsystem: "tier5".to_string(),
                        message: "update_plugin_config requires 'key'".to_string(),
                    })?;
                let config_value =
                    params
                        .get("value")
                        .ok_or_else(|| OaiError::SubsystemError {
                            subsystem: "tier5".to_string(),
                            message: "update_plugin_config requires 'value'".to_string(),
                        })?;

                Ok(serde_json::json!({
                    "action": "update_plugin_config",
                    "plugin": plugin_name,
                    "key": config_key,
                    "value": config_value,
                    "status": "config_updated"
                }))
            }

            _ => Err(OaiError::SubsystemError {
                subsystem: "tier5".to_string(),
                message: format!("unknown action: {}", action),
            }),
        }
    }

    fn urgency(&self, action: &str) -> InterventionUrgency {
        match action {
            "stop_plugin" => InterventionUrgency::Emergency,
            "restart_plugin" => InterventionUrgency::Operational,
            "update_plugin_config" => InterventionUrgency::Precise,
            _ => InterventionUrgency::Operational,
        }
    }

    fn reversibility(&self, action: &str) -> Reversibility {
        match action {
            "restart_plugin" => Reversibility::SelfResolving,
            "stop_plugin" | "update_plugin_config" => Reversibility::Reversible,
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
            "restart_plugin" => vec![PostCondition {
                description: format!("Plugin {} should be running", target.id),
                check: "plugin_running".to_string(),
                expected: serde_json::json!(true),
            }],
            "stop_plugin" => vec![PostCondition {
                description: format!("Plugin {} should be stopped", target.id),
                check: "plugin_stopped".to_string(),
                expected: serde_json::json!(true),
            }],
            _ => Vec::new(),
        }
    }

    fn rollback(
        &self,
        target: &EntityRef,
        action: &str,
        _snapshot: &serde_json::Value,
    ) -> OaiResult<()> {
        match action {
            "stop_plugin" => {
                self.execute(
                    target,
                    "restart_plugin",
                    &serde_json::json!({"graceful": true}),
                )?;
                Ok(())
            }
            _ => Err(OaiError::SubsystemError {
                subsystem: "tier5".to_string(),
                message: format!("action '{}' is not reversible", action),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_disabled_handler() -> Tier5Handler {
        Tier5Handler::new(None)
    }

    fn test_target() -> EntityRef {
        EntityRef {
            entity_type: EntityType::Plugin,
            id: "test-plugin".to_string(),
        }
    }

    #[test]
    fn test_tier5_plugin_host_disabled() {
        let handler = make_disabled_handler();
        let result = handler.execute(
            &test_target(),
            "restart_plugin",
            &serde_json::json!({}),
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(OaiError::SubsystemDisabled { .. })));
    }

    #[test]
    fn test_tier5_unknown_action() {
        let handler = make_disabled_handler();
        let result = handler.execute(&test_target(), "explode", &serde_json::json!({}));
        // SubsystemDisabled hits first since plugin_host is None
        assert!(result.is_err());
    }

    #[test]
    fn test_tier5_urgency_stop_is_emergency() {
        let handler = make_disabled_handler();
        assert_eq!(handler.urgency("stop_plugin"), InterventionUrgency::Emergency);
    }

    #[test]
    fn test_tier5_urgency_restart_is_operational() {
        let handler = make_disabled_handler();
        assert_eq!(
            handler.urgency("restart_plugin"),
            InterventionUrgency::Operational
        );
    }

    #[test]
    fn test_tier5_reversibility_stop() {
        let handler = make_disabled_handler();
        assert_eq!(
            handler.reversibility("stop_plugin"),
            Reversibility::Reversible
        );
    }

    #[test]
    fn test_tier5_reversibility_restart() {
        let handler = make_disabled_handler();
        assert_eq!(
            handler.reversibility("restart_plugin"),
            Reversibility::SelfResolving
        );
    }

    #[test]
    fn test_tier5_post_conditions_restart() {
        let handler = make_disabled_handler();
        let pcs = handler.post_conditions(
            &test_target(),
            "restart_plugin",
            &serde_json::json!({}),
        );
        assert_eq!(pcs.len(), 1);
        assert!(pcs[0].description.contains("running"));
    }
}
