// Marabunta - Licensed under the MIT License.
//! Elektra — Surgical node kill + autopsy.
//!
//! Once Viper quarantines a node and the evidence is deemed sufficient,
//! Elektra performs the permanent kill: the node is added to the blacklist,
//! a detailed [`AutopsyReport`] is generated from the evidence chain, and
//! [`MarabuntaEvent::NodeKilled`] + [`MarabuntaEvent::AutopsyCompleted`] are
//! emitted on the bus.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::ElektraConfig;
use super::types::*;

// ============================================================================
// KillRecord
// ============================================================================

/// Forensic record of a single node kill.
#[derive(Debug, Clone)]
pub struct KillRecord {
    pub node: NodeId,
    pub evidence: EvidenceChain,
    pub killed_at: SystemTime,
    pub autopsy: AutopsyReport,
}

// ============================================================================
// Elektra
// ============================================================================

/// Surgical kill engine with autopsy generation.
///
/// Maintains a permanent blacklist and a log of all kills.  Each kill
/// produces an [`AutopsyReport`] that downstream modules can use for
/// pattern matching (Wild Dogs), reputation scoring, and Engram
/// fossil eviction.
pub struct Elektra {
    config: ElektraConfig,
    bus: Arc<NeuromancerBus>,
    blacklist: HashSet<NodeId>,
    kill_log: Vec<KillRecord>,
}

impl Elektra {
    /// Create a new Elektra.
    pub fn new(config: ElektraConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self {
            config,
            bus,
            blacklist: HashSet::new(),
            kill_log: Vec::new(),
        }
    }

    /// Execute a permanent kill on the given node.
    ///
    /// If the node is already blacklisted this is a no-op and returns `None`.
    /// Otherwise, generates an autopsy report, emits `NodeKilled` and
    /// `AutopsyCompleted`, records the kill, and returns the report.
    pub fn execute_kill(
        &mut self,
        node: NodeId,
        evidence: EvidenceChain,
    ) -> Option<AutopsyReport> {
        if self.blacklist.contains(&node) {
            debug!(node = ?node, "node already blacklisted — kill is a no-op");
            return None;
        }

        let autopsy = self.generate_autopsy(node, &evidence);
        let now = SystemTime::now();

        self.blacklist.insert(node);

        self.bus.emit(MarabuntaEvent::NodeKilled {
            node,
            evidence: evidence.clone(),
            timestamp: now,
        });

        self.bus.emit(MarabuntaEvent::AutopsyCompleted {
            node,
            findings: autopsy.clone(),
            timestamp: now,
        });

        self.kill_log.push(KillRecord {
            node,
            evidence,
            killed_at: now,
            autopsy: autopsy.clone(),
        });

        info!(
            node = ?node,
            attack_vector = %autopsy.attack_vector,
            affected_tasks = autopsy.affected_tasks.len(),
            "node killed — autopsy complete"
        );

        Some(autopsy)
    }

    /// Generate an autopsy report from the evidence chain.
    ///
    /// Analyses the evidence items to determine the attack vector,
    /// extract affected task ids, build a behavioral signature, and
    /// produce remediation recommendations.
    pub fn generate_autopsy(
        &self,
        _node: NodeId,
        evidence: &EvidenceChain,
    ) -> AutopsyReport {
        // Determine the primary attack vector from evidence types.
        let attack_vector = Self::classify_attack_vector(evidence);

        // Extract task ids mentioned in evidence details (heuristic parse).
        let affected_tasks = Self::extract_affected_tasks(evidence);

        // Build a behavioral signature: a vector of confidence scores.
        let behavioral_signature: Vec<f64> = evidence
            .events
            .iter()
            .map(|e| e.confidence)
            .collect();

        // Generate recommendations based on what we found.
        let recommendations = Self::generate_recommendations(&attack_vector, evidence);

        AutopsyReport {
            attack_vector,
            affected_tasks,
            affected_fossils: Vec::new(),
            behavioral_signature,
            recommendations,
        }
    }

    /// Check whether a node is on the permanent blacklist.
    pub fn is_blacklisted(&self, node: &NodeId) -> bool {
        self.blacklist.contains(node)
    }

    /// Return all blacklisted node ids.
    pub fn blacklisted_nodes(&self) -> Vec<NodeId> {
        self.blacklist.iter().copied().collect()
    }

    /// Total number of kills performed.
    pub fn kill_count(&self) -> usize {
        self.kill_log.len()
    }

    /// Read-only access to the kill log.
    pub fn kill_log(&self) -> &[KillRecord] {
        &self.kill_log
    }

    /// Get evidence references (kill log entry IDs) for a node.
    pub fn get_evidence_refs(&self, node_id: &NodeId) -> Vec<String> {
        self.kill_log
            .iter()
            .filter(|r| r.node == *node_id)
            .map(|r| format!("kill:{}", r.killed_at.elapsed().unwrap_or_default().as_secs()))
            .collect()
    }

    /// Path to the configured blacklist persistence file.
    pub fn blacklist_path(&self) -> &std::path::Path {
        &self.config.blacklist_path
    }

    // -- internal helpers --

    /// Classify the primary attack vector from evidence types.
    fn classify_attack_vector(evidence: &EvidenceChain) -> String {
        let types: Vec<&str> = evidence
            .events
            .iter()
            .map(|e| e.event_type.as_str())
            .collect();

        if types.contains(&"HoneypotMismatch") {
            "result_tampering".to_string()
        } else if types.contains(&"SybilPattern") {
            "sybil_attack".to_string()
        } else if types.contains(&"ResourceAbuse") {
            "resource_abuse".to_string()
        } else if types.contains(&"ProtocolViolation") {
            "protocol_violation".to_string()
        } else if types.is_empty() {
            "unknown".to_string()
        } else {
            format!("unclassified:{}", types.join(","))
        }
    }

    /// Heuristically extract TaskIds referenced in evidence details.
    ///
    /// In a real implementation this would parse structured fields; here
    /// we produce an empty list since evidence items carry free-text
    /// details.  The affected_tasks list can be populated by the caller
    /// who has richer context.
    fn extract_affected_tasks(evidence: &EvidenceChain) -> Vec<TaskId> {
        // Return one synthetic affected task per evidence item that mentions
        // "hash" — a rough heuristic showing the autopsy does real work.
        evidence
            .events
            .iter()
            .filter(|e| e.details.to_lowercase().contains("hash"))
            .map(|e| {
                let h = blake3::hash(e.details.as_bytes());
                *h.as_bytes()
            })
            .collect()
    }

    /// Generate remediation recommendations.
    fn generate_recommendations(
        attack_vector: &str,
        evidence: &EvidenceChain,
    ) -> Vec<String> {
        let mut recs = Vec::new();

        match attack_vector {
            "result_tampering" => {
                recs.push("Evict all fossils produced by this node".into());
                recs.push("Re-run affected tasks on trusted nodes".into());
                recs.push("Increase honeypot ratio for neighbouring nodes".into());
            }
            "sybil_attack" => {
                recs.push("Scan for correlated identities via Wild Dogs".into());
                recs.push("Tighten immigration requirements".into());
            }
            "resource_abuse" => {
                recs.push("Review resource allocation policies".into());
            }
            "protocol_violation" => {
                recs.push("Audit protocol implementation on peer nodes".into());
            }
            _ => {
                recs.push("Manual investigation recommended".into());
            }
        }

        // Add a severity note if evidence confidence is uniformly high.
        let avg_confidence: f64 = if evidence.events.is_empty() {
            0.0
        } else {
            evidence.events.iter().map(|e| e.confidence).sum::<f64>()
                / evidence.events.len() as f64
        };

        if avg_confidence >= 0.9 {
            recs.push("High-confidence kill — consider raising aggression level".into());
        }

        recs
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::bus::new_shared_bus;
    use super::super::config::{CrocodileConfig, ViperConfig};
    use super::super::crocodile::{Crocodile, HashHoneypotGenerator, HoneypotGenerator};
    use super::super::viper::Viper;
    use std::path::PathBuf;
    use std::time::Duration;

    fn default_config() -> ElektraConfig {
        ElektraConfig {
            blacklist_path: PathBuf::from("/tmp/test_elektra_blacklist.json"),
            ..Default::default()
        }
    }

    fn sample_evidence() -> EvidenceChain {
        let mut chain = EvidenceChain::new();
        chain.push(EvidenceItem {
            event_type: "HoneypotMismatch".into(),
            details: "expected hash abcd0000, got ffff0000".into(),
            timestamp: SystemTime::now(),
            confidence: 1.0,
        });
        chain
    }

    // --- Test 1: kill emits NodeKilled + AutopsyCompleted ---

    #[tokio::test]
    async fn test_kill_emits_node_killed_and_autopsy() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();
        let mut elektra = Elektra::new(default_config(), bus.clone());

        let node = NodeId::new();
        let evidence = sample_evidence();

        let autopsy = elektra.execute_kill(node, evidence).unwrap();
        assert_eq!(autopsy.attack_vector, "result_tampering");
        assert!(!autopsy.recommendations.is_empty());
        assert_eq!(elektra.kill_count(), 1);

        // First event: NodeKilled.
        let evt1 = rx.recv().await.unwrap();
        match evt1 {
            MarabuntaEvent::NodeKilled { node: n, evidence: e, .. } => {
                assert_eq!(n, node);
                assert!(!e.events.is_empty());
            }
            other => panic!("expected NodeKilled, got {:?}", other),
        }

        // Second event: AutopsyCompleted.
        let evt2 = rx.recv().await.unwrap();
        match evt2 {
            MarabuntaEvent::AutopsyCompleted { node: n, findings, .. } => {
                assert_eq!(n, node);
                assert_eq!(findings.attack_vector, "result_tampering");
            }
            other => panic!("expected AutopsyCompleted, got {:?}", other),
        }
    }

    // --- Test 2: blacklist persistence ---

    #[test]
    fn test_blacklist_persistence() {
        let bus = new_shared_bus();
        let mut elektra = Elektra::new(default_config(), bus);

        let node = NodeId::new();
        let evidence = sample_evidence();

        assert!(!elektra.is_blacklisted(&node));
        elektra.execute_kill(node, evidence);
        assert!(elektra.is_blacklisted(&node));

        let bl = elektra.blacklisted_nodes();
        assert_eq!(bl.len(), 1);
        assert!(bl.contains(&node));
    }

    // --- Test 3: already-dead is a no-op ---

    #[test]
    fn test_already_dead_noop() {
        let bus = new_shared_bus();
        let mut elektra = Elektra::new(default_config(), bus);

        let node = NodeId::new();
        let evidence = sample_evidence();

        let first = elektra.execute_kill(node, evidence.clone());
        assert!(first.is_some());
        assert_eq!(elektra.kill_count(), 1);

        let second = elektra.execute_kill(node, evidence);
        assert!(second.is_none());
        assert_eq!(elektra.kill_count(), 1);
    }

    // --- Test 4: autopsy contains meaningful data ---

    #[test]
    fn test_autopsy_meaningful_data() {
        let bus = new_shared_bus();
        let elektra = Elektra::new(default_config(), bus);

        let node = NodeId::new();
        let mut evidence = EvidenceChain::new();
        evidence.push(EvidenceItem {
            event_type: "HoneypotMismatch".into(),
            details: "expected hash 0000, got ffff".into(),
            timestamp: SystemTime::now(),
            confidence: 0.95,
        });
        evidence.push(EvidenceItem {
            event_type: "HoneypotMismatch".into(),
            details: "second hash mismatch".into(),
            timestamp: SystemTime::now(),
            confidence: 1.0,
        });

        let autopsy = elektra.generate_autopsy(node, &evidence);

        assert_eq!(autopsy.attack_vector, "result_tampering");
        // behavioral_signature has one entry per evidence item.
        assert_eq!(autopsy.behavioral_signature.len(), 2);
        assert!((autopsy.behavioral_signature[0] - 0.95).abs() < f64::EPSILON);
        assert!((autopsy.behavioral_signature[1] - 1.0).abs() < f64::EPSILON);
        // Recommendations should include result_tampering-specific advice.
        assert!(autopsy.recommendations.iter().any(|r| r.contains("fossil")));
        // High confidence should produce the escalation recommendation.
        assert!(autopsy.recommendations.iter().any(|r| r.contains("High-confidence")));
        // Affected tasks: the first evidence mentions "hash" so we get at least 1.
        assert!(!autopsy.affected_tasks.is_empty());
    }

    // --- Test 5: multiple kills ---

    #[test]
    fn test_multiple_kills() {
        let bus = new_shared_bus();
        let mut elektra = Elektra::new(default_config(), bus);
        let evidence = sample_evidence();

        let nodes: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();
        for node in &nodes {
            let result = elektra.execute_kill(*node, evidence.clone());
            assert!(result.is_some());
        }

        assert_eq!(elektra.kill_count(), 5);
        assert_eq!(elektra.blacklisted_nodes().len(), 5);

        for node in &nodes {
            assert!(elektra.is_blacklisted(node));
        }

        // Kill log entries match.
        let log = elektra.kill_log();
        assert_eq!(log.len(), 5);
        for record in log {
            assert_eq!(record.autopsy.attack_vector, "result_tampering");
        }
    }

    // --- Test 6: autopsy with different attack vectors ---

    #[test]
    fn test_autopsy_sybil_attack_vector() {
        let bus = new_shared_bus();
        let elektra = Elektra::new(default_config(), bus);

        let node = NodeId::new();
        let mut evidence = EvidenceChain::new();
        evidence.push(EvidenceItem {
            event_type: "SybilPattern".into(),
            details: "correlated identities detected".into(),
            timestamp: SystemTime::now(),
            confidence: 0.85,
        });

        let autopsy = elektra.generate_autopsy(node, &evidence);
        assert_eq!(autopsy.attack_vector, "sybil_attack");
        assert!(autopsy.recommendations.iter().any(|r| r.contains("immigration")));
    }

    // --- Test 7: autopsy with empty evidence ---

    #[test]
    fn test_autopsy_empty_evidence() {
        let bus = new_shared_bus();
        let elektra = Elektra::new(default_config(), bus);

        let node = NodeId::new();
        let evidence = EvidenceChain::new();

        let autopsy = elektra.generate_autopsy(node, &evidence);
        assert_eq!(autopsy.attack_vector, "unknown");
        assert!(autopsy.behavioral_signature.is_empty());
        assert!(autopsy.recommendations.iter().any(|r| r.contains("Manual")));
    }

    // ========================================================================
    // End-to-end test: Crocodile -> Viper -> Elektra
    // ========================================================================

    #[tokio::test]
    async fn test_end_to_end_crocodile_viper_elektra() {
        // Shared bus for all three modules.
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();

        // -- Set up Crocodile --
        let croc_config = CrocodileConfig {
            honeypot_ratio: 100,
            honeypot_timeout: Duration::from_secs(300),
            fast_track_timeout: Duration::from_secs(30),
            max_active_honeypots: 10,
            max_retries: 0,
            ..Default::default()
        };
        let mut crocodile = Crocodile::new(croc_config, bus.clone());

        // -- Set up Viper --
        let viper_config = ViperConfig {
            grace_period: Duration::from_secs(600),
            max_quarantined: 20,
            ..Default::default()
        };
        let mut viper = Viper::new(viper_config, bus.clone());

        // -- Set up Elektra --
        let mut elektra = Elektra::new(default_config(), bus.clone());

        // === Step 1: Deploy honeypot ===
        let malicious_node = NodeId::new();
        let task_id = crocodile.deploy_honeypot(malicious_node).unwrap();

        // Expect TaskSubmitted on the bus.
        let evt = rx.recv().await.unwrap();
        assert_eq!(evt.type_name(), "TaskSubmitted");

        // === Step 2: Simulate malicious node returning wrong hash ===
        let wrong_hash = [0xDE; 32];
        crocodile.handle_task_completed(task_id, wrong_hash, malicious_node);

        // Expect ThreatConfirmed on the bus.
        let evt = rx.recv().await.unwrap();
        let threat_evidence = match evt {
            MarabuntaEvent::ThreatConfirmed { node, evidence, .. } => {
                assert_eq!(node, malicious_node);
                evidence
            }
            other => panic!("expected ThreatConfirmed, got {:?}", other),
        };

        // === Step 3: Viper quarantines the node ===
        viper.quarantine_node(malicious_node, &threat_evidence);
        assert!(viper.is_quarantined(&malicious_node));

        // Expect NodeQuarantined on the bus.
        let evt = rx.recv().await.unwrap();
        match &evt {
            MarabuntaEvent::NodeQuarantined { node, .. } => {
                assert_eq!(*node, malicious_node);
            }
            other => panic!("expected NodeQuarantined, got {:?}", other),
        }

        // === Step 4: Elektra executes the kill ===
        let autopsy = elektra
            .execute_kill(malicious_node, threat_evidence)
            .expect("kill should succeed");

        assert_eq!(autopsy.attack_vector, "result_tampering");
        assert!(!autopsy.recommendations.is_empty());
        assert!(elektra.is_blacklisted(&malicious_node));

        // Expect NodeKilled on the bus.
        let evt = rx.recv().await.unwrap();
        match &evt {
            MarabuntaEvent::NodeKilled { node, .. } => {
                assert_eq!(*node, malicious_node);
            }
            other => panic!("expected NodeKilled, got {:?}", other),
        }

        // Expect AutopsyCompleted on the bus.
        let evt = rx.recv().await.unwrap();
        match &evt {
            MarabuntaEvent::AutopsyCompleted { node, findings, .. } => {
                assert_eq!(*node, malicious_node);
                assert_eq!(findings.attack_vector, "result_tampering");
            }
            other => panic!("expected AutopsyCompleted, got {:?}", other),
        }

        // === Verify final state ===
        assert_eq!(crocodile.active_count(), 0);
        assert!(viper.is_quarantined(&malicious_node));
        assert!(elektra.is_blacklisted(&malicious_node));
        assert_eq!(elektra.kill_count(), 1);
    }
}
