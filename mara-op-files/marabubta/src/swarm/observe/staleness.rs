// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};

use super::config::GuardConfig;
use super::errors::OaiError;
use super::guard::GuardCondition;
use super::types::InterventionUrgency;

/// Check whether a guard condition's snapshot is fresh enough.
pub fn check_staleness(
    condition: &GuardCondition,
    urgency: InterventionUrgency,
    config: &GuardConfig,
    now: DateTime<Utc>,
) -> Result<(), OaiError> {
    let max_staleness_ms = match urgency {
        InterventionUrgency::Emergency => config.emergency_staleness_ms,
        InterventionUrgency::Operational => config.operational_staleness_ms,
        InterventionUrgency::Precise => config.precise_staleness_ms,
    };

    let age_ms = (now - condition.snapshot_time).num_milliseconds().max(0) as u64;

    if age_ms > max_staleness_ms {
        return Err(OaiError::StaleObservation {
            entity: condition.entity.clone(),
            snapshot_age_ms: age_ms,
            max_staleness_ms,
            intervention: format!("{:?}", urgency),
        });
    }

    Ok(())
}

/// Check staleness for all conditions in a guard.
pub fn check_guard_staleness(
    conditions: &[GuardCondition],
    urgency: InterventionUrgency,
    config: &GuardConfig,
) -> Result<(), OaiError> {
    let now = Utc::now();
    for condition in conditions {
        check_staleness(condition, urgency, config, now)?;
    }
    Ok(())
}

/// Version-aware check: verify entity generation hasn't advanced.
pub fn check_version(
    conditions: &[GuardCondition],
    resolve_generation: impl Fn(&super::types::EntityRef) -> Option<u64>,
) -> Result<(), OaiError> {
    for condition in conditions {
        if condition.field == "generation" {
            if let Some(expected_gen) = condition.expected.as_u64() {
                if let Some(actual_gen) = resolve_generation(&condition.entity) {
                    if actual_gen != expected_gen {
                        return Err(OaiError::VersionMismatch {
                            entity: condition.entity.clone(),
                            expected_version: expected_gen,
                            actual_version: actual_gen,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::guard::ComparisonOp;
    use super::super::types::{EntityRef, EntityType};
    use chrono::Duration;

    fn make_condition(age: Duration) -> GuardCondition {
        GuardCondition {
            entity: EntityRef { entity_type: EntityType::Node, id: "n1".to_string() },
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            expected: serde_json::json!("alive"),
            snapshot_time: Utc::now() - age,
        }
    }

    fn make_gen_condition(gen: u64, age: Duration) -> GuardCondition {
        GuardCondition {
            entity: EntityRef { entity_type: EntityType::Node, id: "n1".to_string() },
            field: "generation".to_string(),
            op: ComparisonOp::Eq,
            expected: serde_json::json!(gen),
            snapshot_time: Utc::now() - age,
        }
    }

    fn default_guard_config() -> GuardConfig {
        GuardConfig::default()
    }

    #[test]
    fn test_check_staleness_within_window() {
        let condition = make_condition(Duration::seconds(2));
        let config = default_guard_config();
        let result = check_staleness(&condition, InterventionUrgency::Emergency, &config, Utc::now());
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_staleness_expired_operational() {
        let condition = make_condition(Duration::seconds(6));
        let config = default_guard_config();
        let result = check_staleness(&condition, InterventionUrgency::Operational, &config, Utc::now());
        assert!(result.is_err());
        if let Err(OaiError::StaleObservation { max_staleness_ms, .. }) = result {
            assert_eq!(max_staleness_ms, 5_000);
        } else {
            panic!("Expected StaleObservation");
        }
    }

    #[test]
    fn test_check_staleness_expired_precise() {
        let condition = make_condition(Duration::seconds(1));
        let config = default_guard_config();
        let result = check_staleness(&condition, InterventionUrgency::Precise, &config, Utc::now());
        assert!(result.is_err());
    }

    #[test]
    fn test_check_staleness_just_at_boundary() {
        // Create a condition exactly at the boundary (5000ms for Operational)
        let config = default_guard_config();
        let now = Utc::now();
        let condition = GuardCondition {
            entity: EntityRef { entity_type: EntityType::Node, id: "n1".to_string() },
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            expected: serde_json::json!("alive"),
            snapshot_time: now - Duration::milliseconds(5000),
        };
        // At exactly max_staleness_ms, it should pass (not > boundary)
        let result = check_staleness(&condition, InterventionUrgency::Operational, &config, now);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_version_match() {
        let conditions = vec![make_gen_condition(5, Duration::seconds(1))];
        let result = check_version(&conditions, |_| Some(5));
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_version_mismatch() {
        let conditions = vec![make_gen_condition(5, Duration::seconds(1))];
        let result = check_version(&conditions, |_| Some(6));
        assert!(result.is_err());
        if let Err(OaiError::VersionMismatch { expected_version, actual_version, .. }) = result {
            assert_eq!(expected_version, 5);
            assert_eq!(actual_version, 6);
        } else {
            panic!("Expected VersionMismatch");
        }
    }

    #[test]
    fn test_check_version_no_generation_field() {
        let conditions = vec![make_condition(Duration::seconds(1))]; // "status" field, not "generation"
        let result = check_version(&conditions, |_| Some(99));
        assert!(result.is_ok()); // No generation conditions, nothing to check
    }

    #[test]
    fn test_check_guard_staleness_all_fresh() {
        let conditions = vec![
            make_condition(Duration::seconds(1)),
            make_condition(Duration::seconds(2)),
        ];
        let config = default_guard_config();
        let result = check_guard_staleness(&conditions, InterventionUrgency::Emergency, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_guard_staleness_one_stale() {
        let conditions = vec![
            make_condition(Duration::seconds(1)),
            make_condition(Duration::seconds(6)),
        ];
        let config = default_guard_config();
        let result = check_guard_staleness(&conditions, InterventionUrgency::Operational, &config);
        assert!(result.is_err());
    }
}
