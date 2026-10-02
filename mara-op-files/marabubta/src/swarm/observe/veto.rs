// Marabunta - Licensed under the MIT License.
use std::sync::Arc;
use parking_lot::Mutex;

use crate::swarm::neuromancer::elektra::Elektra;
use crate::swarm::neuromancer::spider::Spider;
use crate::swarm::neuromancer::types::NodeId;
use super::errors::OaiError;

/// Neuromancer veto mechanism. Checks whether Neuromancer subsystems would
/// object to an operator intervention.
pub struct VetoChecker {
    elektra: Option<Arc<Mutex<Elektra>>>,
    spider: Option<Arc<Mutex<Spider>>>,
}

impl VetoChecker {
    pub fn new(
        elektra: Option<Arc<Mutex<Elektra>>>,
        spider: Option<Arc<Mutex<Spider>>>,
    ) -> Self {
        Self { elektra, spider }
    }

    /// Create a no-op veto checker (no Neuromancer subsystems).
    pub fn disabled() -> Self {
        Self {
            elektra: None,
            spider: None,
        }
    }

    /// Check if Neuromancer vetoes releasing a quarantined node.
    ///
    /// Veto conditions:
    /// 1. Elektra has a blacklist entry for this node
    /// 2. Spider shows active anomaly above critical threshold
    pub fn check_release_quarantine(&self, node_id: &NodeId) -> Result<(), OaiError> {
        // Check Elektra blacklist
        if let Some(ref elektra) = self.elektra {
            let elektra = elektra.lock();
            if elektra.is_blacklisted(node_id) {
                return Err(OaiError::NeuromancerVeto {
                    reason: format!(
                        "Node {} is on Elektra's blacklist (kill chain active)",
                        node_id
                    ),
                    subsystem: "elektra".to_string(),
                    evidence_refs: elektra.get_evidence_refs(node_id),
                });
            }
        }

        // Check Spider anomaly
        if let Some(ref spider) = self.spider {
            let spider = spider.lock();
            if let Some(score) = spider.current_anomaly_score(node_id) {
                if score > spider.critical_threshold() {
                    return Err(OaiError::NeuromancerVeto {
                        reason: format!(
                            "Node {} has active anomaly score {:.2} (critical threshold: {:.2})",
                            node_id,
                            score,
                            spider.critical_threshold()
                        ),
                        subsystem: "spider".to_string(),
                        evidence_refs: vec![],
                    });
                }
            }
        }

        Ok(())
    }

    /// Check if Neuromancer vetoes unquarantining a node.
    pub fn check_unquarantine(&self, node_id: &NodeId) -> Result<(), OaiError> {
        self.check_release_quarantine(node_id)
    }

    /// Check if Neuromancer vetoes any action on a node.
    pub fn check_node_intervention(
        &self,
        node_id: &NodeId,
        action: &str,
    ) -> Result<(), OaiError> {
        match action {
            "unquarantine" | "release_quarantine" | "uncordon" => {
                self.check_release_quarantine(node_id)
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_veto_neuromancer_not_enabled() {
        let checker = VetoChecker::disabled();
        let node_id = NodeId::new();
        assert!(checker.check_release_quarantine(&node_id).is_ok());
    }

    #[test]
    fn test_veto_check_node_intervention_non_quarantine() {
        let checker = VetoChecker::disabled();
        let node_id = NodeId::new();
        // Non-quarantine actions should pass
        assert!(checker.check_node_intervention(&node_id, "drain").is_ok());
        assert!(checker.check_node_intervention(&node_id, "cordon").is_ok());
    }

    #[test]
    fn test_veto_check_node_intervention_quarantine_actions() {
        let checker = VetoChecker::disabled();
        let node_id = NodeId::new();
        // These actions would be checked but pass since no subsystems are enabled
        assert!(
            checker
                .check_node_intervention(&node_id, "unquarantine")
                .is_ok()
        );
        assert!(
            checker
                .check_node_intervention(&node_id, "release_quarantine")
                .is_ok()
        );
    }
}
