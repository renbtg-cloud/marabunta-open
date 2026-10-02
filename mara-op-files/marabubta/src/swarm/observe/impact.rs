// Marabunta - Licensed under the MIT License.
use std::sync::Arc;

use serde::Serialize;

use crate::swarm::fleet::FleetManager;
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::types::NodeId;
use super::config::ImpactConfig;
use super::errors::OaiError;
use super::operator::{Operator, SafetyTier};

/// Computed impact assessment for a proposed intervention.
#[derive(Debug, Clone, Serialize)]
pub struct ImpactAssessment {
    /// How many entities will be directly affected.
    pub affected_count: usize,
    /// How many entities remain operational after the intervention.
    pub remaining_operational: usize,
    /// Total entities in the category.
    pub total_entities: usize,
    /// Percentage of entities affected.
    pub blast_radius_pct: f64,
    /// Whether this triggers a hard block.
    pub blocked: bool,
    /// Reason for block (if any).
    pub block_reason: Option<String>,
    /// Human-readable impact summary.
    pub summary: String,
    /// Whether the intervention is reversible.
    pub reversible: bool,
}

pub struct ImpactAssessor {
    knowledge: Arc<KnowledgeStore>,
    _fleet: Arc<FleetManager>,
    config: ImpactConfig,
}

impl ImpactAssessor {
    pub fn new(
        knowledge: Arc<KnowledgeStore>,
        fleet: Arc<FleetManager>,
        config: ImpactConfig,
    ) -> Self {
        Self {
            knowledge,
            _fleet: fleet,
            config,
        }
    }

    /// Assess the impact of a node-targeting intervention.
    pub fn assess_node_intervention(
        &self,
        node_id: &NodeId,
        action: &str,
    ) -> ImpactAssessment {
        let total_nodes = self.knowledge.node_count();
        let alive_nodes = self.knowledge.get_live_nodes().len();

        // How many nodes would be taken offline by this action?
        let affected = match action {
            "drain" | "cordon" | "quarantine" => 1,
            _ => 0,
        };

        let remaining = alive_nodes.saturating_sub(affected);
        let blast_pct = if total_nodes > 0 {
            (affected as f64 / total_nodes as f64) * 100.0
        } else {
            0.0
        };

        let reversible = matches!(action, "cordon" | "drain");

        // Check blocks
        let (blocked, block_reason) = if blast_pct > self.config.max_blast_radius_pct {
            (
                true,
                Some(format!(
                    "Would affect {:.0}% of nodes (max {:.0}%)",
                    blast_pct, self.config.max_blast_radius_pct
                )),
            )
        } else if remaining < self.config.minimum_viable_nodes {
            (
                true,
                Some(format!(
                    "Would leave {} operational nodes (minimum {})",
                    remaining, self.config.minimum_viable_nodes
                )),
            )
        } else {
            (false, None)
        };

        let summary = format!(
            "{} node {} → {}/{} nodes remain operational ({:.0}% blast radius)",
            action, node_id, remaining, total_nodes, blast_pct
        );

        ImpactAssessment {
            affected_count: affected,
            remaining_operational: remaining,
            total_entities: total_nodes,
            blast_radius_pct: blast_pct,
            blocked,
            block_reason,
            summary,
            reversible,
        }
    }

    /// Check impact against operator's safety tier.
    /// Returns the OaiError if blocked, with --force availability info.
    pub fn check_safety_block(
        &self,
        assessment: &ImpactAssessment,
        operator: &Operator,
        action: &str,
    ) -> Result<(), OaiError> {
        if !assessment.blocked {
            return Ok(());
        }

        match operator.safety_tier {
            SafetyTier::Unrestricted | SafetyTier::Informed => {
                // Tiers 0-1: no hard blocks (but assessment is still reported)
                Ok(())
            }
            SafetyTier::Guarded => {
                // Tier 2: hard block, but --force is available
                Err(OaiError::SafetyTierBlock {
                    operator_tier: operator.safety_tier as u8,
                    required_tier: 0,
                    action: action.to_string(),
                    force_available: true,
                })
            }
            SafetyTier::Supervised => {
                // Tier 3: hard block, no --force
                Err(OaiError::SafetyTierBlock {
                    operator_tier: operator.safety_tier as u8,
                    required_tier: 0,
                    action: action.to_string(),
                    force_available: false,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::fleet::FleetManager;
    use crate::swarm::knowledge::KnowledgeStore;
    use crate::swarm::types::{NodeId, NodeInfo, NodeStatus, ResourceSnapshot};
    use std::collections::HashSet;

    fn make_node_info(nid: NodeId, status: NodeStatus) -> NodeInfo {
        NodeInfo {
            node_id: nid.clone(),
            last_seen: chrono::Utc::now(),
            traits: HashSet::new(),
            load: 0.5,
            capacity: ResourceSnapshot::default(),
            address: None,
            via: nid,
            status,
            generation: 1,
            trust_level: Default::default(),
            ..Default::default()
        }
    }

    fn make_assessor(node_count: usize) -> (ImpactAssessor, Vec<NodeId>) {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let fleet = Arc::new(FleetManager::new());
        let config = ImpactConfig::default();

        let mut node_ids = Vec::new();
        for _ in 0..node_count {
            let nid = NodeId::new();
            knowledge.merge_node(make_node_info(nid.clone(), NodeStatus::Alive));
            node_ids.push(nid);
        }

        (ImpactAssessor::new(knowledge, fleet, config), node_ids)
    }

    fn make_assessor_mixed(alive: usize, dead: usize) -> (ImpactAssessor, Vec<NodeId>) {
        let self_id = NodeId::new();
        let knowledge = Arc::new(KnowledgeStore::new(self_id));
        let fleet = Arc::new(FleetManager::new());
        let config = ImpactConfig::default();

        let mut node_ids = Vec::new();
        for _ in 0..alive {
            let nid = NodeId::new();
            knowledge.merge_node(make_node_info(nid.clone(), NodeStatus::Alive));
            node_ids.push(nid);
        }
        for _ in 0..dead {
            let nid = NodeId::new();
            knowledge.merge_node(make_node_info(nid.clone(), NodeStatus::Dead));
            node_ids.push(nid);
        }

        (ImpactAssessor::new(knowledge, fleet, config), node_ids)
    }

    #[test]
    fn test_assess_drain_4node() {
        let (assessor, nodes) = make_assessor(4);
        let assessment = assessor.assess_node_intervention(&nodes[0], "drain");
        assert_eq!(assessment.affected_count, 1);
        assert_eq!(assessment.remaining_operational, 3);
        assert_eq!(assessment.total_entities, 4);
        assert!((assessment.blast_radius_pct - 25.0).abs() < 0.1);
        // 25% is NOT > 25%, so not blocked
        assert!(!assessment.blocked);
        assert!(assessment.reversible);
    }

    #[test]
    fn test_assess_quarantine_4node_min_capacity() {
        // 4 nodes total: 2 alive, 2 dead
        let (assessor, nodes) = make_assessor_mixed(2, 2);
        let assessment = assessor.assess_node_intervention(&nodes[0], "quarantine");
        // 1 affected out of 4 total = 25% blast radius
        assert_eq!(assessment.affected_count, 1);
        // remaining = 2 alive - 1 = 1
        assert_eq!(assessment.remaining_operational, 1);
        // 1 < minimum_viable_nodes(2) → blocked
        assert!(assessment.blocked);
        assert!(assessment.block_reason.as_ref().unwrap().contains("minimum"));
    }

    #[test]
    fn test_assess_drain_2node() {
        let (assessor, nodes) = make_assessor(2);
        let assessment = assessor.assess_node_intervention(&nodes[0], "drain");
        // 1 out of 2 = 50% > max_blast_radius_pct(25%) → blocked
        assert!((assessment.blast_radius_pct - 50.0).abs() < 0.1);
        assert!(assessment.blocked);
    }

    #[test]
    fn test_safety_block_unrestricted() {
        let (assessor, nodes) = make_assessor(2);
        let assessment = assessor.assess_node_intervention(&nodes[0], "drain");
        assert!(assessment.blocked);

        let operator = super::super::operator::ProfilePreset::Unrestricted
            .to_operator(super::super::types::OperatorId::new("op1"), "op1".to_string());
        let result = assessor.check_safety_block(&assessment, &operator, "drain");
        // Unrestricted → not blocked
        assert!(result.is_ok());
    }

    #[test]
    fn test_safety_block_guarded() {
        let (assessor, nodes) = make_assessor(2);
        let assessment = assessor.assess_node_intervention(&nodes[0], "drain");
        assert!(assessment.blocked);

        let operator = super::super::operator::ProfilePreset::Engineer
            .to_operator(super::super::types::OperatorId::new("op1"), "op1".to_string());
        let result = assessor.check_safety_block(&assessment, &operator, "drain");
        assert!(result.is_err());
        if let Err(OaiError::SafetyTierBlock { force_available, .. }) = result {
            assert!(force_available);
        } else {
            panic!("Expected SafetyTierBlock");
        }
    }

    #[test]
    fn test_safety_block_supervised() {
        let (assessor, nodes) = make_assessor(2);
        let assessment = assessor.assess_node_intervention(&nodes[0], "drain");
        assert!(assessment.blocked);

        let operator = super::super::operator::ProfilePreset::Spectator
            .to_operator(super::super::types::OperatorId::new("op1"), "op1".to_string());
        let result = assessor.check_safety_block(&assessment, &operator, "drain");
        assert!(result.is_err());
        if let Err(OaiError::SafetyTierBlock { force_available, .. }) = result {
            assert!(!force_available);
        } else {
            panic!("Expected SafetyTierBlock");
        }
    }
}
