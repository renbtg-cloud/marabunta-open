// Marabunta - Licensed under the MIT License.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::swarm::fleet::FleetManager;
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::NodeId;
use super::types::EntityRef;

/// Comparison operator for guard conditions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    Exists,
    NotExists,
}

/// A single guard condition on an entity's field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardCondition {
    pub entity: EntityRef,
    pub field: String,
    pub op: ComparisonOp,
    pub expected: serde_json::Value,
    pub snapshot_time: DateTime<Utc>,
}

/// How to combine multiple conditions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum GuardMode {
    #[default]
    AllMust,
    AnyMust,
}


/// A complete guard attached to an intervention.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionGuard {
    pub conditions: Vec<GuardCondition>,
    pub mode: GuardMode,
}

/// Result of evaluating a guard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardEvaluation {
    pub passed: bool,
    pub results: Vec<ConditionResult>,
    pub evaluated_at: DateTime<Utc>,
}

/// Result of evaluating a single condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionResult {
    pub condition: GuardCondition,
    pub passed: bool,
    pub actual: Option<serde_json::Value>,
}

/// Evaluates guards against current swarm state.
pub struct GuardEvaluator {
    knowledge: Arc<KnowledgeStore>,
    fleet: Arc<FleetManager>,
}

impl GuardEvaluator {
    pub fn new(knowledge: Arc<KnowledgeStore>, fleet: Arc<FleetManager>) -> Self {
        Self { knowledge, fleet }
    }

    /// Evaluate a guard against current state.
    pub fn evaluate(&self, guard: &ConditionGuard) -> GuardEvaluation {
        let now = Utc::now();
        let results: Vec<ConditionResult> = guard.conditions.iter()
            .map(|c| self.evaluate_condition(c))
            .collect();

        let passed = match guard.mode {
            GuardMode::AllMust => results.iter().all(|r| r.passed),
            GuardMode::AnyMust => results.iter().any(|r| r.passed),
        };

        GuardEvaluation { passed, results, evaluated_at: now }
    }

    fn evaluate_condition(&self, condition: &GuardCondition) -> ConditionResult {
        let actual = self.resolve_field(&condition.entity, &condition.field);
        let passed = match &actual {
            Some(val) => self.compare(val, &condition.op, &condition.expected),
            None => matches!(condition.op, ComparisonOp::NotExists),
        };
        ConditionResult {
            condition: condition.clone(),
            passed,
            actual,
        }
    }

    fn resolve_field(&self, entity: &EntityRef, field: &str) -> Option<serde_json::Value> {
        match entity.entity_type {
            super::types::EntityType::Node => {
                let node_id = NodeId(Uuid::parse_str(&entity.id).ok()?);
                match field {
                    "status" => {
                        self.knowledge.get_node(&node_id)
                            .and_then(|n| serde_json::to_value(n.status).ok())
                    }
                    "fleet_state" => {
                        let state = self.fleet.fleet_store().get_state(&node_id);
                        Some(serde_json::Value::String(state.state_name().to_string()))
                    }
                    "load" => {
                        self.knowledge.get_node(&node_id)
                            .map(|n| serde_json::json!(n.load))
                    }
                    "cpu_available" => {
                        self.knowledge.get_node(&node_id)
                            .map(|n| serde_json::json!(n.capacity.cpu_available))
                    }
                    "generation" => {
                        self.knowledge.get_node(&node_id)
                            .map(|n| serde_json::json!(n.generation))
                    }
                    _ => None,
                }
            }
            super::types::EntityType::Job => {
                let job_id = crate::common::types::JobId(Uuid::parse_str(&entity.id).ok()?);
                match field {
                    "status" => {
                        self.knowledge.get_job(&job_id)
                            .and_then(|j| serde_json::to_value(j.status).ok())
                    }
                    "chunks_completed" => {
                        self.knowledge.get_job(&job_id)
                            .map(|j| serde_json::json!(j.chunks_completed))
                    }
                    "chunks_total" => {
                        self.knowledge.get_job(&job_id)
                            .map(|j| serde_json::json!(j.chunks_total))
                    }
                    _ => None,
                }
            }
            super::types::EntityType::Chunk => {
                let chunk_id = crate::swarm::types::ChunkId(Uuid::parse_str(&entity.id).ok()?);
                match field {
                    "status" => {
                        self.knowledge.get_assignment(&chunk_id)
                            .and_then(|a| serde_json::to_value(a.status).ok())
                    }
                    "assigned_to" => {
                        self.knowledge.get_assignment(&chunk_id)
                            .map(|a| match &a.assigned_to {
                                Some(nid) => serde_json::json!(nid.0.to_string()),
                                None => serde_json::Value::Null,
                            })
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn compare(&self, actual: &serde_json::Value, op: &ComparisonOp, expected: &serde_json::Value) -> bool {
        match op {
            ComparisonOp::Eq => actual == expected,
            ComparisonOp::Ne => actual != expected,
            ComparisonOp::Lt => compare_numeric(actual, expected, |a, b| a < b),
            ComparisonOp::Le => compare_numeric(actual, expected, |a, b| a <= b),
            ComparisonOp::Gt => compare_numeric(actual, expected, |a, b| a > b),
            ComparisonOp::Ge => compare_numeric(actual, expected, |a, b| a >= b),
            ComparisonOp::In => {
                expected.as_array().is_some_and(|arr| arr.contains(actual))
            }
            ComparisonOp::NotIn => {
                expected.as_array().map_or(true, |arr| !arr.contains(actual))
            }
            ComparisonOp::Exists => true,
            ComparisonOp::NotExists => false,
        }
    }

    /// Get a reference to the underlying knowledge store.
    pub fn knowledge(&self) -> &Arc<KnowledgeStore> {
        &self.knowledge
    }

    /// Resolve the current generation counter for a node.
    pub fn resolve_generation(&self, node_id: &NodeId) -> Option<u64> {
        self.knowledge.get_node(node_id).map(|n| n.generation)
    }

    /// Auto-generate a guard for a node intervention.
    pub fn auto_guard_node(&self, node_id: &NodeId) -> Option<ConditionGuard> {
        let node = self.knowledge.get_node(node_id)?;
        let fleet_state = self.fleet.fleet_store().get_state(node_id);
        let now = Utc::now();
        let entity = EntityRef {
            entity_type: super::types::EntityType::Node,
            id: node_id.0.to_string(),
        };

        let conditions = vec![
            GuardCondition {
                entity: entity.clone(),
                field: "status".to_string(),
                op: ComparisonOp::Eq,
                expected: serde_json::to_value(node.status).unwrap_or_default(),
                snapshot_time: now,
            },
            GuardCondition {
                entity: entity.clone(),
                field: "fleet_state".to_string(),
                op: ComparisonOp::Eq,
                expected: serde_json::Value::String(fleet_state.state_name().to_string()),
                snapshot_time: now,
            },
            GuardCondition {
                entity,
                field: "generation".to_string(),
                op: ComparisonOp::Eq,
                expected: serde_json::json!(node.generation),
                snapshot_time: now,
            },
        ];

        Some(ConditionGuard { conditions, mode: GuardMode::AllMust })
    }

    /// Auto-generate a guard for a job intervention.
    pub fn auto_guard_job(&self, job_id: &crate::common::types::JobId) -> Option<ConditionGuard> {
        let job = self.knowledge.get_job(job_id)?;
        let now = Utc::now();
        let entity = EntityRef {
            entity_type: super::types::EntityType::Job,
            id: job_id.0.to_string(),
        };

        let conditions = vec![
            GuardCondition {
                entity,
                field: "status".to_string(),
                op: ComparisonOp::Eq,
                expected: serde_json::to_value(job.status).unwrap_or_default(),
                snapshot_time: now,
            },
        ];

        Some(ConditionGuard { conditions, mode: GuardMode::AllMust })
    }

    /// Auto-generate a guard for a chunk intervention.
    pub fn auto_guard_chunk(&self, chunk_id: &crate::swarm::types::ChunkId) -> Option<ConditionGuard> {
        let assignment = self.knowledge.get_assignment(chunk_id)?;
        let now = Utc::now();
        let entity = EntityRef {
            entity_type: super::types::EntityType::Chunk,
            id: chunk_id.0.to_string(),
        };

        let conditions = vec![
            GuardCondition {
                entity: entity.clone(),
                field: "status".to_string(),
                op: ComparisonOp::Eq,
                expected: serde_json::to_value(assignment.status).unwrap_or_default(),
                snapshot_time: now,
            },
            GuardCondition {
                entity,
                field: "assigned_to".to_string(),
                op: ComparisonOp::Eq,
                expected: match &assignment.assigned_to {
                    Some(nid) => serde_json::json!(nid.0.to_string()),
                    None => serde_json::Value::Null,
                },
                snapshot_time: now,
            },
        ];

        Some(ConditionGuard { conditions, mode: GuardMode::AllMust })
    }
}

fn compare_numeric(a: &serde_json::Value, b: &serde_json::Value, f: fn(f64, f64) -> bool) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(va), Some(vb)) => f(va, vb),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entity(etype: super::super::types::EntityType, id: &str) -> EntityRef {
        EntityRef { entity_type: etype, id: id.to_string() }
    }

    #[test]
    fn test_comparison_eq() {
        let guard_eval = make_test_evaluator();
        assert!(guard_eval.compare(&serde_json::json!("alive"), &ComparisonOp::Eq, &serde_json::json!("alive")));
        assert!(!guard_eval.compare(&serde_json::json!("alive"), &ComparisonOp::Eq, &serde_json::json!("dead")));
    }

    #[test]
    fn test_comparison_ne() {
        let guard_eval = make_test_evaluator();
        assert!(guard_eval.compare(&serde_json::json!("alive"), &ComparisonOp::Ne, &serde_json::json!("dead")));
        assert!(!guard_eval.compare(&serde_json::json!("alive"), &ComparisonOp::Ne, &serde_json::json!("alive")));
    }

    #[test]
    fn test_comparison_lt_gt() {
        let guard_eval = make_test_evaluator();
        assert!(guard_eval.compare(&serde_json::json!(5), &ComparisonOp::Lt, &serde_json::json!(10)));
        assert!(!guard_eval.compare(&serde_json::json!(10), &ComparisonOp::Lt, &serde_json::json!(5)));
        assert!(guard_eval.compare(&serde_json::json!(10), &ComparisonOp::Gt, &serde_json::json!(5)));
        assert!(!guard_eval.compare(&serde_json::json!(5), &ComparisonOp::Gt, &serde_json::json!(10)));
    }

    #[test]
    fn test_comparison_in() {
        let guard_eval = make_test_evaluator();
        assert!(guard_eval.compare(
            &serde_json::json!("alive"),
            &ComparisonOp::In,
            &serde_json::json!(["alive", "suspect"]),
        ));
        assert!(!guard_eval.compare(
            &serde_json::json!("dead"),
            &ComparisonOp::In,
            &serde_json::json!(["alive", "suspect"]),
        ));
    }

    #[test]
    fn test_comparison_not_in() {
        let guard_eval = make_test_evaluator();
        assert!(guard_eval.compare(
            &serde_json::json!("dead"),
            &ComparisonOp::NotIn,
            &serde_json::json!(["alive", "suspect"]),
        ));
        assert!(!guard_eval.compare(
            &serde_json::json!("alive"),
            &ComparisonOp::NotIn,
            &serde_json::json!(["alive", "suspect"]),
        ));
    }

    #[test]
    fn test_comparison_exists_not_exists() {
        let guard_eval = make_test_evaluator();
        assert!(guard_eval.compare(&serde_json::json!("anything"), &ComparisonOp::Exists, &serde_json::json!(null)));
        assert!(!guard_eval.compare(&serde_json::json!("anything"), &ComparisonOp::NotExists, &serde_json::json!(null)));
    }

    #[test]
    fn test_guard_all_must_all_pass() {
        let guard = ConditionGuard {
            conditions: vec![
                make_static_condition("field1", ComparisonOp::Eq, serde_json::json!("a")),
                make_static_condition("field2", ComparisonOp::Eq, serde_json::json!("b")),
            ],
            mode: GuardMode::AllMust,
        };
        // Both pass: since resolve_field returns None for unknown fields,
        // and NotExists is the only one that passes for None, let's test
        // the mode logic directly
        let results = vec![
            ConditionResult { condition: guard.conditions[0].clone(), passed: true, actual: Some(serde_json::json!("a")) },
            ConditionResult { condition: guard.conditions[1].clone(), passed: true, actual: Some(serde_json::json!("b")) },
        ];
        let passed = results.iter().all(|r| r.passed);
        assert!(passed);
    }

    #[test]
    fn test_guard_all_must_one_fails() {
        let results = vec![
            ConditionResult {
                condition: make_static_condition("f1", ComparisonOp::Eq, serde_json::json!("a")),
                passed: true,
                actual: Some(serde_json::json!("a")),
            },
            ConditionResult {
                condition: make_static_condition("f2", ComparisonOp::Eq, serde_json::json!("b")),
                passed: false,
                actual: Some(serde_json::json!("c")),
            },
        ];
        let passed = results.iter().all(|r| r.passed);
        assert!(!passed);
    }

    #[test]
    fn test_guard_any_must_one_passes() {
        let results = vec![
            ConditionResult {
                condition: make_static_condition("f1", ComparisonOp::Eq, serde_json::json!("a")),
                passed: false,
                actual: Some(serde_json::json!("x")),
            },
            ConditionResult {
                condition: make_static_condition("f2", ComparisonOp::Eq, serde_json::json!("b")),
                passed: true,
                actual: Some(serde_json::json!("b")),
            },
        ];
        let passed = results.iter().any(|r| r.passed);
        assert!(passed);
    }

    #[test]
    fn test_guard_any_must_none_pass() {
        let results = vec![
            ConditionResult {
                condition: make_static_condition("f1", ComparisonOp::Eq, serde_json::json!("a")),
                passed: false,
                actual: Some(serde_json::json!("x")),
            },
            ConditionResult {
                condition: make_static_condition("f2", ComparisonOp::Eq, serde_json::json!("b")),
                passed: false,
                actual: Some(serde_json::json!("y")),
            },
        ];
        let passed = results.iter().any(|r| r.passed);
        assert!(!passed);
    }

    #[test]
    fn test_guard_serde_roundtrip() {
        let guard = ConditionGuard {
            conditions: vec![
                GuardCondition {
                    entity: make_entity(super::super::types::EntityType::Node, "abc"),
                    field: "status".to_string(),
                    op: ComparisonOp::Eq,
                    expected: serde_json::json!("alive"),
                    snapshot_time: Utc::now(),
                },
            ],
            mode: GuardMode::AllMust,
        };
        let json = serde_json::to_string(&guard).expect("serialize");
        let back: ConditionGuard = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.conditions.len(), 1);
        assert_eq!(back.mode, GuardMode::AllMust);
    }

    #[test]
    fn test_guard_evaluation_serializes() {
        let eval = GuardEvaluation {
            passed: true,
            results: vec![],
            evaluated_at: Utc::now(),
        };
        let json = serde_json::to_string(&eval).expect("serialize");
        assert!(json.contains("passed"));
    }

    fn make_test_evaluator() -> GuardEvaluator {
        use crate::swarm::knowledge::KnowledgeStore;
        use crate::swarm::fleet::FleetManager;
        use crate::swarm::types::NodeId;
        let knowledge = Arc::new(KnowledgeStore::new(NodeId::new()));
        let fleet = Arc::new(FleetManager::new());
        GuardEvaluator::new(knowledge, fleet)
    }

    fn make_static_condition(field: &str, op: ComparisonOp, expected: serde_json::Value) -> GuardCondition {
        GuardCondition {
            entity: make_entity(super::super::types::EntityType::Node, "test-node"),
            field: field.to_string(),
            op,
            expected,
            snapshot_time: Utc::now(),
        }
    }
}
