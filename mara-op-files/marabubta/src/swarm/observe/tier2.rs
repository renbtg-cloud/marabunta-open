// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use uuid::Uuid;

use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::{ChunkId, ChunkStatus};
use super::engine::TierHandler;
use super::errors::{OaiError, OaiResult};
use super::rollback::PostCondition;
use super::types::*;

pub struct Tier2Handler {
    knowledge: Arc<KnowledgeStore>,
}

impl Tier2Handler {
    pub fn new(knowledge: Arc<KnowledgeStore>) -> Self {
        Self { knowledge }
    }
}

impl TierHandler for Tier2Handler {
    fn tier(&self) -> InterventionTier {
        InterventionTier::WorkControl
    }

    fn execute(
        &self,
        target: &EntityRef,
        action: &str,
        params: &serde_json::Value,
    ) -> OaiResult<serde_json::Value> {
        match action {
            "pause_chunk" => {
                let chunk_id = ChunkId(Uuid::parse_str(&target.id).map_err(|_| {
                    OaiError::EntityNotFound {
                        entity: target.clone(),
                    }
                })?);

                // Verify chunk exists
                let assignment = self
                    .knowledge
                    .get_assignment(&chunk_id)
                    .ok_or_else(|| OaiError::EntityNotFound {
                        entity: target.clone(),
                    })?;

                if assignment.status != ChunkStatus::InProgress {
                    return Err(OaiError::InvalidEntityState {
                        entity: target.clone(),
                        current_state: format!("{:?}", assignment.status),
                        required_states: vec!["InProgress".to_string()],
                        action: "pause_chunk".to_string(),
                    });
                }

                // Mark as Pending (paused = back in pending state)
                let mut updated = assignment.clone();
                updated.status = ChunkStatus::Pending;
                self.knowledge.merge_assignment(updated);

                Ok(serde_json::json!({
                    "action": "pause_chunk",
                    "chunk_id": target.id,
                    "status": "paused"
                }))
            }

            "resume_chunk" => {
                let chunk_id = ChunkId(Uuid::parse_str(&target.id).map_err(|_| {
                    OaiError::EntityNotFound {
                        entity: target.clone(),
                    }
                })?);

                let assignment = self
                    .knowledge
                    .get_assignment(&chunk_id)
                    .ok_or_else(|| OaiError::EntityNotFound {
                        entity: target.clone(),
                    })?;

                if assignment.status != ChunkStatus::Pending {
                    return Err(OaiError::InvalidEntityState {
                        entity: target.clone(),
                        current_state: format!("{:?}", assignment.status),
                        required_states: vec!["Pending".to_string()],
                        action: "resume_chunk".to_string(),
                    });
                }

                // Chunk is Pending — work engine's claim loop will pick it up
                Ok(serde_json::json!({
                    "action": "resume_chunk",
                    "chunk_id": target.id,
                    "status": "resumed"
                }))
            }

            "cancel_job" => {
                let job_id = crate::common::types::JobId(
                    Uuid::parse_str(&target.id).map_err(|_| OaiError::EntityNotFound {
                        entity: target.clone(),
                    })?,
                );

                let job = self
                    .knowledge
                    .get_job(&job_id)
                    .ok_or_else(|| OaiError::EntityNotFound {
                        entity: target.clone(),
                    })?;

                let reason = params
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("operator-cancelled");

                let mut updated_job = job.clone();
                updated_job.status = crate::swarm::types::SwarmJobStatus::Failed;
                self.knowledge.merge_job(updated_job);

                Ok(serde_json::json!({
                    "action": "cancel_job",
                    "job_id": target.id,
                    "reason": reason,
                    "status": "cancelled"
                }))
            }

            "reassign_chunk" => {
                let chunk_id = ChunkId(Uuid::parse_str(&target.id).map_err(|_| {
                    OaiError::EntityNotFound {
                        entity: target.clone(),
                    }
                })?);

                let target_node_str =
                    params
                        .get("target_node")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| OaiError::SubsystemError {
                            subsystem: "tier2".to_string(),
                            message: "reassign_chunk requires 'target_node' parameter"
                                .to_string(),
                        })?;

                let target_node_id = crate::swarm::types::NodeId(
                    Uuid::parse_str(target_node_str).map_err(|_| {
                        OaiError::SubsystemError {
                            subsystem: "tier2".to_string(),
                            message: format!(
                                "invalid target_node UUID: {}",
                                target_node_str
                            ),
                        }
                    })?,
                );

                // Verify chunk exists
                let _assignment = self
                    .knowledge
                    .get_assignment(&chunk_id)
                    .ok_or_else(|| OaiError::EntityNotFound {
                        entity: target.clone(),
                    })?;

                // Claim the chunk for the new node
                self.knowledge.claim_chunk(&chunk_id, target_node_id);

                Ok(serde_json::json!({
                    "action": "reassign_chunk",
                    "chunk_id": target.id,
                    "target_node": target_node_str,
                    "status": "reassigned"
                }))
            }

            _ => Err(OaiError::SubsystemError {
                subsystem: "tier2".to_string(),
                message: format!("unknown action: {}", action),
            }),
        }
    }

    fn urgency(&self, action: &str) -> InterventionUrgency {
        match action {
            "cancel_job" => InterventionUrgency::Operational,
            "reassign_chunk" => InterventionUrgency::Precise,
            "pause_chunk" | "resume_chunk" => InterventionUrgency::Operational,
            _ => InterventionUrgency::Operational,
        }
    }

    fn reversibility(&self, action: &str) -> Reversibility {
        match action {
            "pause_chunk" => Reversibility::Reversible,
            "resume_chunk" => Reversibility::Irreversible,
            "reassign_chunk" => Reversibility::Irreversible,
            "cancel_job" => Reversibility::Irreversible,
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
            "pause_chunk" => vec![PostCondition {
                description: format!("Chunk {} should be paused", target.id),
                check: "chunk_paused".to_string(),
                expected: serde_json::json!(true),
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
        match action {
            "pause_chunk" => {
                // Resume the chunk by executing the resume action
                self.execute(target, "resume_chunk", &serde_json::json!({}))?;
                Ok(())
            }
            _ => Err(OaiError::SubsystemError {
                subsystem: "tier2".to_string(),
                message: format!("action '{}' is not reversible", action),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::types::NodeId;

    fn make_handler() -> Tier2Handler {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        Tier2Handler::new(knowledge)
    }

    #[test]
    fn test_tier2_unknown_action() {
        let handler = make_handler();
        let target = EntityRef {
            entity_type: EntityType::Chunk,
            id: ChunkId::new().0.to_string(),
        };
        let result = handler.execute(&target, "explode", &serde_json::json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_tier2_pause_chunk_not_found() {
        let handler = make_handler();
        let target = EntityRef {
            entity_type: EntityType::Chunk,
            id: ChunkId::new().0.to_string(),
        };
        let result = handler.execute(&target, "pause_chunk", &serde_json::json!({}));
        assert!(result.is_err());
        assert!(matches!(result, Err(OaiError::EntityNotFound { .. })));
    }

    #[test]
    fn test_tier2_cancel_job_not_found() {
        let handler = make_handler();
        let target = EntityRef {
            entity_type: EntityType::Job,
            id: crate::common::types::JobId::new().0.to_string(),
        };
        let result = handler.execute(
            &target,
            "cancel_job",
            &serde_json::json!({"reason": "testing"}),
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(OaiError::EntityNotFound { .. })));
    }

    #[test]
    fn test_tier2_reassign_missing_target() {
        let handler = make_handler();
        let target = EntityRef {
            entity_type: EntityType::Chunk,
            id: ChunkId::new().0.to_string(),
        };
        let result = handler.execute(&target, "reassign_chunk", &serde_json::json!({}));
        assert!(result.is_err());
    }

    #[test]
    fn test_tier2_urgency_cancel_is_operational() {
        let handler = make_handler();
        assert_eq!(handler.urgency("cancel_job"), InterventionUrgency::Operational);
    }

    #[test]
    fn test_tier2_urgency_reassign_is_precise() {
        let handler = make_handler();
        assert_eq!(
            handler.urgency("reassign_chunk"),
            InterventionUrgency::Precise
        );
    }

    #[test]
    fn test_tier2_reversibility_pause() {
        let handler = make_handler();
        assert_eq!(handler.reversibility("pause_chunk"), Reversibility::Reversible);
    }
}
