// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use std::fmt;

use super::types::{EntityRef, InterventionId, InterventionTier, OperatorId};

// -- Exit codes (spec 07) --
pub const EXIT_OK: i32 = 0;
pub const EXIT_ERROR: i32 = 1;
pub const EXIT_GUARD_FAILED: i32 = 2;
pub const EXIT_CANCELLED: i32 = 3;
pub const EXIT_CONFLICT: i32 = 4;

/// Top-level error type for all OAI operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OaiError {
    // ---- Guard violations (exit code 2, HTTP 409) ----

    /// State drift: entity field changed between observation and execution.
    GuardViolated {
        entity: EntityRef,
        field: String,
        expected: serde_json::Value,
        actual: serde_json::Value,
        drift_ms: u64,
    },

    /// Entity version/generation advanced since observation.
    VersionMismatch {
        entity: EntityRef,
        expected_version: u64,
        actual_version: u64,
    },

    /// Observation snapshot is older than staleness tolerance.
    StaleObservation {
        entity: EntityRef,
        snapshot_age_ms: u64,
        max_staleness_ms: u64,
        intervention: String,
    },

    // ---- Conflicts (exit code 4, HTTP 409) ----

    /// Another intervention is already in progress on this entity.
    InterventionConflict {
        entity: EntityRef,
        existing_intervention: InterventionId,
        existing_operator: OperatorId,
        existing_action: String,
        started_at: String,
    },

    // ---- Permission errors (exit code 1, HTTP 403) ----

    /// Operator's safety tier forbids this action.
    SafetyTierBlock {
        operator_tier: u8,
        required_tier: u8,
        action: String,
        force_available: bool,
    },

    /// Operator doesn't have permission for this tier.
    TierPermissionDenied {
        operator: OperatorId,
        tier: InterventionTier,
    },

    /// Operator's scope doesn't cover this entity.
    ScopeViolation {
        operator: OperatorId,
        entity: EntityRef,
        operator_scope: String,
    },

    /// Parameter value exceeds operator's configured boundary.
    ParameterBoundaryExceeded {
        parameter: String,
        requested: serde_json::Value,
        boundary_min: Option<serde_json::Value>,
        boundary_max: Option<serde_json::Value>,
    },

    // ---- Target errors (exit code 1, HTTP 404/422) ----

    /// The specified entity doesn't exist.
    EntityNotFound { entity: EntityRef },

    /// The entity exists but is in an invalid state for this operation.
    InvalidEntityState {
        entity: EntityRef,
        current_state: String,
        required_states: Vec<String>,
        action: String,
    },

    /// The subsystem backing this tier is not enabled.
    SubsystemDisabled {
        tier: InterventionTier,
        subsystem: String,
        config_flag: String,
    },

    // ---- Neuromancer vetoes (exit code 1, HTTP 403) ----

    /// Neuromancer vetoed this intervention.
    NeuromancerVeto {
        reason: String,
        subsystem: String,
        evidence_refs: Vec<String>,
    },

    // ---- Impact blocks (exit code 1, HTTP 422) ----

    /// Hard block: action would affect too many entities.
    BlastRadiusBlock {
        action: String,
        affected_count: usize,
        max_allowed: usize,
        affected_percentage: f64,
    },

    /// Hard block: action would leave the swarm below minimum viable capacity.
    MinimumCapacityBlock {
        action: String,
        remaining_nodes: usize,
        minimum_required: usize,
    },

    // ---- System errors (exit code 1, HTTP 500) ----

    /// Internal error from a subsystem.
    SubsystemError {
        subsystem: String,
        message: String,
    },

    /// Intervention lock could not be acquired within timeout.
    LockTimeout {
        entity: EntityRef,
        timeout_ms: u64,
    },

    /// Optimistic execution post-condition failed; rollback attempted.
    PostConditionFailed {
        intervention: InterventionId,
        condition: String,
        rollback_result: String,
    },
}

impl OaiError {
    /// Exit code for CLI usage.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::GuardViolated { .. }
            | Self::VersionMismatch { .. }
            | Self::StaleObservation { .. } => EXIT_GUARD_FAILED,

            Self::InterventionConflict { .. } => EXIT_CONFLICT,

            _ => EXIT_ERROR,
        }
    }

    /// HTTP status code.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::GuardViolated { .. }
            | Self::VersionMismatch { .. }
            | Self::InterventionConflict { .. } => 409,

            Self::StaleObservation { .. } => 408,

            Self::SafetyTierBlock { .. }
            | Self::TierPermissionDenied { .. }
            | Self::ScopeViolation { .. }
            | Self::NeuromancerVeto { .. } => 403,

            Self::EntityNotFound { .. } => 404,

            Self::InvalidEntityState { .. }
            | Self::ParameterBoundaryExceeded { .. }
            | Self::BlastRadiusBlock { .. }
            | Self::MinimumCapacityBlock { .. } => 422,

            Self::SubsystemDisabled { .. } => 503,

            Self::SubsystemError { .. }
            | Self::LockTimeout { .. }
            | Self::PostConditionFailed { .. } => 500,
        }
    }

    /// Error code string for structured responses.
    pub fn code(&self) -> &'static str {
        match self {
            Self::GuardViolated { .. } => "GUARD_VIOLATED",
            Self::VersionMismatch { .. } => "VERSION_MISMATCH",
            Self::StaleObservation { .. } => "STALE_OBSERVATION",
            Self::InterventionConflict { .. } => "INTERVENTION_CONFLICT",
            Self::SafetyTierBlock { .. } => "SAFETY_TIER_BLOCK",
            Self::TierPermissionDenied { .. } => "TIER_PERMISSION_DENIED",
            Self::ScopeViolation { .. } => "SCOPE_VIOLATION",
            Self::ParameterBoundaryExceeded { .. } => "PARAMETER_BOUNDARY_EXCEEDED",
            Self::EntityNotFound { .. } => "ENTITY_NOT_FOUND",
            Self::InvalidEntityState { .. } => "INVALID_ENTITY_STATE",
            Self::SubsystemDisabled { .. } => "SUBSYSTEM_DISABLED",
            Self::NeuromancerVeto { .. } => "NEUROMANCER_VETO",
            Self::BlastRadiusBlock { .. } => "BLAST_RADIUS_BLOCK",
            Self::MinimumCapacityBlock { .. } => "MINIMUM_CAPACITY_BLOCK",
            Self::SubsystemError { .. } => "SUBSYSTEM_ERROR",
            Self::LockTimeout { .. } => "LOCK_TIMEOUT",
            Self::PostConditionFailed { .. } => "POST_CONDITION_FAILED",
        }
    }
}

impl fmt::Display for OaiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GuardViolated { entity, field, expected, actual, drift_ms } => {
                write!(f, "Guard violated: {}.{} changed from {} to {} ({:.1}s drift)",
                    entity, field, expected, actual, *drift_ms as f64 / 1000.0)
            }
            Self::VersionMismatch { entity, expected_version, actual_version } => {
                write!(f, "Version mismatch: {} expected gen {} but is now gen {}",
                    entity, expected_version, actual_version)
            }
            Self::StaleObservation { entity, snapshot_age_ms, max_staleness_ms, intervention } => {
                write!(f, "Stale observation: {} is {:.1}s old (max {:.1}s for {})",
                    entity, *snapshot_age_ms as f64 / 1000.0, *max_staleness_ms as f64 / 1000.0, intervention)
            }
            Self::InterventionConflict { entity, existing_operator, existing_action, .. } => {
                write!(f, "Conflict: {} already has '{}' by {} in progress",
                    entity, existing_action, existing_operator)
            }
            Self::SafetyTierBlock { action, operator_tier, required_tier, force_available } => {
                if *force_available {
                    write!(f, "Blocked: '{}' requires safety tier {} (you are tier {}). Use --force to override.",
                        action, required_tier, operator_tier)
                } else {
                    write!(f, "Blocked: '{}' requires safety tier {} (you are tier {}). No override available.",
                        action, required_tier, operator_tier)
                }
            }
            Self::TierPermissionDenied { operator, tier } => {
                write!(f, "Permission denied: operator {} lacks permission for {}", operator, tier)
            }
            Self::ScopeViolation { operator, entity, operator_scope } => {
                write!(f, "Scope violation: operator {} (scope={}) cannot access {}",
                    operator, operator_scope, entity)
            }
            Self::ParameterBoundaryExceeded { parameter, requested, .. } => {
                write!(f, "Parameter '{}' value {} exceeds your configured boundaries", parameter, requested)
            }
            Self::EntityNotFound { entity } => {
                write!(f, "Entity not found: {}", entity)
            }
            Self::InvalidEntityState { entity, current_state, action, .. } => {
                write!(f, "Invalid state: {} is '{}', cannot {}", entity, current_state, action)
            }
            Self::SubsystemDisabled { subsystem, config_flag, .. } => {
                write!(f, "Subsystem '{}' is disabled. Set {} = true in config.", subsystem, config_flag)
            }
            Self::NeuromancerVeto { reason, subsystem, .. } => {
                write!(f, "Neuromancer veto ({}): {}", subsystem, reason)
            }
            Self::BlastRadiusBlock { action, affected_count, max_allowed, .. } => {
                write!(f, "Blast radius block: '{}' would affect {} entities (max {})",
                    action, affected_count, max_allowed)
            }
            Self::MinimumCapacityBlock { action, remaining_nodes, minimum_required } => {
                write!(f, "Minimum capacity block: '{}' would leave {} nodes (minimum {})",
                    action, remaining_nodes, minimum_required)
            }
            Self::SubsystemError { subsystem, message } => {
                write!(f, "Subsystem error ({}): {}", subsystem, message)
            }
            Self::LockTimeout { entity, timeout_ms } => {
                write!(f, "Lock timeout: could not acquire lock on {} within {}ms", entity, timeout_ms)
            }
            Self::PostConditionFailed { intervention, condition, rollback_result } => {
                write!(f, "Post-condition failed for {}: {}. Rollback: {}",
                    intervention, condition, rollback_result)
            }
        }
    }
}

impl std::error::Error for OaiError {}

/// Result alias for OAI operations.
pub type OaiResult<T> = Result<T, OaiError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::observe::types::{EntityRef, EntityType, InterventionId, InterventionTier, OperatorId};

    fn sample_entity() -> EntityRef {
        EntityRef { entity_type: EntityType::Node, id: "n1".to_string() }
    }

    #[test]
    fn test_exit_codes() {
        let guard_err = OaiError::GuardViolated {
            entity: sample_entity(),
            field: "status".to_string(),
            expected: serde_json::json!("alive"),
            actual: serde_json::json!("dead"),
            drift_ms: 1000,
        };
        assert_eq!(guard_err.exit_code(), EXIT_GUARD_FAILED);

        let version_err = OaiError::VersionMismatch {
            entity: sample_entity(),
            expected_version: 1,
            actual_version: 2,
        };
        assert_eq!(version_err.exit_code(), EXIT_GUARD_FAILED);

        let stale_err = OaiError::StaleObservation {
            entity: sample_entity(),
            snapshot_age_ms: 10000,
            max_staleness_ms: 5000,
            intervention: "drain".to_string(),
        };
        assert_eq!(stale_err.exit_code(), EXIT_GUARD_FAILED);

        let conflict_err = OaiError::InterventionConflict {
            entity: sample_entity(),
            existing_intervention: InterventionId::new(),
            existing_operator: OperatorId::new("op1"),
            existing_action: "drain".to_string(),
            started_at: "2024-01-01T00:00:00Z".to_string(),
        };
        assert_eq!(conflict_err.exit_code(), EXIT_CONFLICT);

        let not_found = OaiError::EntityNotFound { entity: sample_entity() };
        assert_eq!(not_found.exit_code(), EXIT_ERROR);
    }

    #[test]
    fn test_http_status_codes() {
        assert_eq!(OaiError::GuardViolated {
            entity: sample_entity(), field: "x".into(),
            expected: serde_json::json!(1), actual: serde_json::json!(2), drift_ms: 0,
        }.http_status(), 409);

        assert_eq!(OaiError::StaleObservation {
            entity: sample_entity(), snapshot_age_ms: 0,
            max_staleness_ms: 0, intervention: "x".into(),
        }.http_status(), 408);

        assert_eq!(OaiError::SafetyTierBlock {
            operator_tier: 3, required_tier: 0, action: "x".into(), force_available: false,
        }.http_status(), 403);

        assert_eq!(OaiError::EntityNotFound { entity: sample_entity() }.http_status(), 404);

        assert_eq!(OaiError::InvalidEntityState {
            entity: sample_entity(), current_state: "x".into(),
            required_states: vec![], action: "y".into(),
        }.http_status(), 422);

        assert_eq!(OaiError::SubsystemDisabled {
            tier: InterventionTier::Neuromancer, subsystem: "x".into(), config_flag: "y".into(),
        }.http_status(), 503);

        assert_eq!(OaiError::SubsystemError {
            subsystem: "x".into(), message: "y".into(),
        }.http_status(), 500);
    }

    #[test]
    fn test_error_code_strings() {
        assert_eq!(OaiError::GuardViolated {
            entity: sample_entity(), field: "x".into(),
            expected: serde_json::json!(1), actual: serde_json::json!(2), drift_ms: 0,
        }.code(), "GUARD_VIOLATED");

        assert_eq!(OaiError::InterventionConflict {
            entity: sample_entity(),
            existing_intervention: InterventionId::new(),
            existing_operator: OperatorId::new("op"),
            existing_action: "x".into(), started_at: "t".into(),
        }.code(), "INTERVENTION_CONFLICT");

        assert_eq!(OaiError::BlastRadiusBlock {
            action: "x".into(), affected_count: 1, max_allowed: 0, affected_percentage: 100.0,
        }.code(), "BLAST_RADIUS_BLOCK");
    }

    #[test]
    fn test_error_display_not_empty() {
        let errors: Vec<OaiError> = vec![
            OaiError::GuardViolated {
                entity: sample_entity(), field: "s".into(),
                expected: serde_json::json!("a"), actual: serde_json::json!("b"), drift_ms: 100,
            },
            OaiError::VersionMismatch {
                entity: sample_entity(), expected_version: 1, actual_version: 2,
            },
            OaiError::StaleObservation {
                entity: sample_entity(), snapshot_age_ms: 6000,
                max_staleness_ms: 5000, intervention: "drain".into(),
            },
            OaiError::InterventionConflict {
                entity: sample_entity(),
                existing_intervention: InterventionId::new(),
                existing_operator: OperatorId::new("op"),
                existing_action: "drain".into(), started_at: "t".into(),
            },
            OaiError::SafetyTierBlock {
                operator_tier: 3, required_tier: 0, action: "kill".into(), force_available: true,
            },
            OaiError::TierPermissionDenied {
                operator: OperatorId::new("op"), tier: InterventionTier::Neuromancer,
            },
            OaiError::ScopeViolation {
                operator: OperatorId::new("op"), entity: sample_entity(), operator_scope: "own".into(),
            },
            OaiError::ParameterBoundaryExceeded {
                parameter: "x".into(), requested: serde_json::json!(99),
                boundary_min: None, boundary_max: Some(serde_json::json!(10)),
            },
            OaiError::EntityNotFound { entity: sample_entity() },
            OaiError::InvalidEntityState {
                entity: sample_entity(), current_state: "dead".into(),
                required_states: vec!["alive".into()], action: "drain".into(),
            },
            OaiError::SubsystemDisabled {
                tier: InterventionTier::Plugin, subsystem: "plugin_host".into(),
                config_flag: "enable_plugin_host".into(),
            },
            OaiError::NeuromancerVeto {
                reason: "suspicious".into(), subsystem: "spider".into(), evidence_refs: vec![],
            },
            OaiError::BlastRadiusBlock {
                action: "drain".into(), affected_count: 3, max_allowed: 1, affected_percentage: 75.0,
            },
            OaiError::MinimumCapacityBlock {
                action: "drain".into(), remaining_nodes: 1, minimum_required: 2,
            },
            OaiError::SubsystemError {
                subsystem: "fleet".into(), message: "oops".into(),
            },
            OaiError::LockTimeout { entity: sample_entity(), timeout_ms: 5000 },
            OaiError::PostConditionFailed {
                intervention: InterventionId::new(),
                condition: "node.status == draining".into(),
                rollback_result: "ok".into(),
            },
        ];
        for err in &errors {
            let msg = err.to_string();
            assert!(!msg.is_empty(), "Display for {:?} is empty", err);
        }
    }

    #[test]
    fn test_error_serde_roundtrip() {
        let err = OaiError::GuardViolated {
            entity: sample_entity(),
            field: "status".to_string(),
            expected: serde_json::json!("alive"),
            actual: serde_json::json!("dead"),
            drift_ms: 1234,
        };
        let json = serde_json::to_string(&err).expect("serialize");
        let back: OaiError = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.code(), "GUARD_VIOLATED");
        assert_eq!(back.exit_code(), EXIT_GUARD_FAILED);
    }
}
