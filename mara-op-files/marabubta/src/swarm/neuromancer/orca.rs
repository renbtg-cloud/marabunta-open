// Marabunta - Licensed under the MIT License.
//! Baby Orca — Predator coordinator for the Neuromancer subsystem.
//!
//! Orca is the central decision-maker for the security kill chain.  It
//! receives anomaly, threat, quarantine, kill, and autopsy events from
//! the bus and decides what happens next based on the configured
//! [`AggressionLevel`].
//!
//! # Responsibilities
//!
//! - **Anomaly triage**: decide whether an anomaly warrants investigation
//!   (threshold depends on aggression level).
//! - **Threat response**: advance active responses through stages
//!   (Monitoring -> Investigating -> Responding -> Hunting -> Recovering).
//! - **Auto-escalation**: if many threats arrive within the configured
//!   window, automatically raise the swarm-wide threat level.
//! - **Statistics**: track anomalies, confirmed threats, quarantines,
//!   kills, pack hunts, and autopsies.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::{AggressionLevel, OrcaConfig};
use super::types::*;

// ============================================================================
// Enums
// ============================================================================

/// Swarm-wide threat level maintained by Orca.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreatLevel {
    Low,
    Elevated,
    High,
    Critical,
}

/// Stage of an active security response for a particular node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseStage {
    Monitoring,
    Investigating,
    Responding,
    Hunting,
    Recovering,
}

// ============================================================================
// ResponseState
// ============================================================================

/// Tracks the current state of a security response targeting a single node.
pub struct ResponseState {
    pub node: NodeId,
    pub stage: ResponseStage,
    pub evidence: EvidenceChain,
    pub started_at: SystemTime,
    pub last_updated: SystemTime,
}

// ============================================================================
// OrcaStats
// ============================================================================

/// Cumulative statistics collected by the Orca coordinator.
#[derive(Debug, Clone, Default)]
pub struct OrcaStats {
    pub anomalies_seen: u64,
    pub threats_confirmed: u64,
    pub nodes_quarantined: u64,
    pub nodes_killed: u64,
    pub pack_hunts_initiated: u64,
    pub autopsies_completed: u64,
}

// ============================================================================
// BabyOrca
// ============================================================================

/// The Baby Orca predator coordinator.
///
/// Consumes [`MarabuntaEvent`]s from the bus, maintains per-node response
/// state, and emits follow-up events (e.g. triggering Crocodile honeypots
/// or Viper quarantines).
use crate::swarm::transport::SwarmTransport;

pub struct BabyOrca {
    config: OrcaConfig,
    bus: Arc<NeuromancerBus>,
    transport: Option<Arc<SwarmTransport>>,
    active_responses: HashMap<NodeId, ResponseState>,
    threat_level: ThreatLevel,
    stats: OrcaStats,
    /// Timestamps of recently confirmed threats, used for auto-escalation.
    recent_threat_timestamps: Vec<SystemTime>,
}

impl BabyOrca {
    /// Create a new Orca coordinator with the given configuration and bus.
    pub fn new(config: OrcaConfig, bus: Arc<NeuromancerBus>, transport: Option<Arc<SwarmTransport>>) -> Self {
        Self {
            config,
            bus,
            transport,
            active_responses: HashMap::new(),
            threat_level: ThreatLevel::Low,
            stats: OrcaStats::default(),
            recent_threat_timestamps: Vec::new(),
        }
    }

    // --------------------------------------------------------------------
    // Decision methods
    // --------------------------------------------------------------------

    /// Decide how to react to an anomaly detection event.
    ///
    /// The reaction depends on the configured aggression level:
    /// - **Paranoid**: immediately start an investigation for any anomaly.
    /// - **Balanced**: investigate only if `score >= 3.0`.
    /// - **Permissive**: investigate only if `score >= 5.0`.
    pub fn decide_on_anomaly(&mut self, node: NodeId, score: f64, details: &str) {
        self.stats.anomalies_seen += 1;

        let should_investigate = match self.config.aggression {
            AggressionLevel::Paranoid => true,
            AggressionLevel::Balanced => score >= 3.0,
            AggressionLevel::Permissive => score >= 5.0,
            AggressionLevel::Low => score >= 7.0,
            AggressionLevel::Medium => score >= 4.0,
            AggressionLevel::High => score >= 2.0,
        };

        if should_investigate {
            info!(
                ?node,
                score,
                aggression = ?self.config.aggression,
                "Orca: opening investigation for anomaly"
            );

            let now = SystemTime::now();
            let mut evidence = EvidenceChain::new();
            evidence.push(EvidenceItem {
                event_type: "AnomalyDetected".into(),
                details: details.to_string(),
                timestamp: now,
                confidence: score / 10.0, // normalise to 0..1 range
            });

            let state = ResponseState {
                node,
                stage: ResponseStage::Investigating,
                evidence,
                started_at: now,
                last_updated: now,
            };
            self.active_responses.insert(node, state);

            // Emit a deception event to trigger Crocodile honeypot
            self.bus.emit(MarabuntaEvent::DeceptionStarted {
                node,
                timestamp: now,
            });
        } else {
            debug!(
                ?node,
                score,
                aggression = ?self.config.aggression,
                "Orca: anomaly below threshold, ignoring"
            );
        }
    }

    /// Decide how to react to a confirmed threat.
    ///
    /// Moves the response state for the node to [`ResponseStage::Responding`]
    /// and records the threat timestamp for auto-escalation tracking.
    pub fn decide_on_threat(&mut self, node: NodeId, evidence: EvidenceChain) {
        self.stats.threats_confirmed += 1;
        let now = SystemTime::now();
        self.recent_threat_timestamps.push(now);

        info!(?node, "Orca: threat confirmed, advancing to Responding");

        if let Some(state) = self.active_responses.get_mut(&node) {
            state.stage = ResponseStage::Responding;
            state.evidence = evidence;
            state.last_updated = now;
        } else {
            // Threat arrived without a prior anomaly — create fresh state
            let state = ResponseState {
                node,
                stage: ResponseStage::Responding,
                evidence,
                started_at: now,
                last_updated: now,
            };
            self.active_responses.insert(node, state);
        }

        self.update_threat_level();
    }

    /// Decide what to do after a node has been quarantined.
    ///
    /// If a pack-hunt pattern is detected (multiple active responses), the
    /// response advances to [`ResponseStage::Hunting`] and a
    /// [`MarabuntaEvent::PackHuntInitiated`] is emitted.
    pub fn decide_post_quarantine(&mut self, node: NodeId) {
        self.stats.nodes_quarantined += 1;
        let now = SystemTime::now();

        // Detect pack-hunt pattern: 3 or more active responses in Responding
        let responding_nodes: Vec<NodeId> = self
            .active_responses
            .iter()
            .filter(|(_, s)| s.stage == ResponseStage::Responding)
            .map(|(n, _)| *n)
            .collect();

        let pack_hunt = responding_nodes.len() >= 3;

        if let Some(state) = self.active_responses.get_mut(&node) {
            if pack_hunt {
                info!(
                    ?node,
                    count = responding_nodes.len(),
                    "Orca: pack-hunt pattern detected, advancing to Hunting"
                );
                state.stage = ResponseStage::Hunting;
                state.last_updated = now;
                self.stats.pack_hunts_initiated += 1;

                self.bus.emit(MarabuntaEvent::PackHuntInitiated {
                    targets: responding_nodes,
                    pattern: "concurrent-responses".into(),
                    timestamp: now,
                });
            } else {
                debug!(?node, "Orca: node quarantined, no pack-hunt pattern");
                state.last_updated = now;
            }
        }
    }

    /// Decide what to do after a node kill + autopsy.
    ///
    /// Moves the response to [`ResponseStage::Recovering`] and updates stats.
    pub fn decide_post_kill(&mut self, node: NodeId, _autopsy: &AutopsyReport) {
        self.stats.nodes_killed += 1;
        self.stats.autopsies_completed += 1;
        let now = SystemTime::now();

        info!(?node, "Orca: node killed, enforcing active network ban and advancing to Recovering");
        
        // Enforce the actual transport ban, transforming this from a dashboard metric
        // into a concrete network defense mechanism.
        if let Some(ref transport) = self.transport {
            transport.ban_node(node);
        } else {
            warn!(?node, "Orca: cannot enforce active network ban (transport not linked)");
        }

        if let Some(state) = self.active_responses.get_mut(&node) {
            state.stage = ResponseStage::Recovering;
            state.last_updated = now;
        } else {
            let state = ResponseState {
                node,
                stage: ResponseStage::Recovering,
                evidence: EvidenceChain::new(),
                started_at: now,
                last_updated: now,
            };
            self.active_responses.insert(node, state);
        }
    }

    /// Recalculate the swarm-wide threat level based on how many confirmed
    /// threats have occurred within the auto-escalation window.
    ///
    /// | Recent threats | Level    |
    /// |----------------|----------|
    /// | 0              | Low      |
    /// | 1-2            | Elevated |
    /// | 3-5            | High     |
    /// | 6+             | Critical |
    pub fn update_threat_level(&mut self) {
        let now = SystemTime::now();
        let window = self.config.auto_escalate_window;

        // Prune timestamps outside the window
        self.recent_threat_timestamps.retain(|ts| {
            now.duration_since(*ts).unwrap_or_default() < window
        });

        let count = self.recent_threat_timestamps.len();
        let new_level = match count {
            0 => ThreatLevel::Low,
            1..=2 => ThreatLevel::Elevated,
            3..=5 => ThreatLevel::High,
            _ => ThreatLevel::Critical,
        };

        if new_level != self.threat_level {
            warn!(
                old = ?self.threat_level,
                new = ?new_level,
                recent_threats = count,
                "Orca: threat level changed"
            );
            self.threat_level = new_level;
        }
    }

    // --------------------------------------------------------------------
    // Event dispatch
    // --------------------------------------------------------------------

    /// Handle a single event from the bus, dispatching to the appropriate
    /// decision method.
    pub fn handle_event(&mut self, event: &MarabuntaEvent) {
        match event {
            MarabuntaEvent::AnomalyDetected {
                node,
                score,
                details,
                ..
            } => {
                self.decide_on_anomaly(*node, *score, details);
            }
            MarabuntaEvent::ThreatConfirmed { node, evidence, .. } => {
                self.decide_on_threat(*node, evidence.clone());
            }
            MarabuntaEvent::NodeQuarantined { node, .. } => {
                self.decide_post_quarantine(*node);
            }
            MarabuntaEvent::AutopsyCompleted { node, findings, .. } => {
                self.decide_post_kill(*node, findings);
            }
            MarabuntaEvent::PackHuntInitiated { .. } => {
                // Already counted when we initiate; ignore echoes
            }
            _ => {
                // Events we don't act on
            }
        }
    }

    // --------------------------------------------------------------------
    // Accessors
    // --------------------------------------------------------------------

    /// Current swarm-wide threat level.
    pub fn threat_level(&self) -> &ThreatLevel {
        &self.threat_level
    }

    /// Cumulative statistics.
    pub fn stats(&self) -> &OrcaStats {
        &self.stats
    }

    /// Number of currently active response states.
    pub fn active_response_count(&self) -> usize {
        self.active_responses.len()
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::neuromancer::bus::NeuromancerBus;
    use std::time::Duration;

    /// Helper: create an Orca with a given aggression level.
    fn make_orca(aggression: AggressionLevel) -> BabyOrca {
        let config = OrcaConfig {
            aggression,
            auto_escalate_threshold: 3,
            auto_escalate_window: Duration::from_secs(300),
        };
        let bus = Arc::new(NeuromancerBus::new(64));
        BabyOrca::new(config, bus, None)
    }

    fn sample_evidence() -> EvidenceChain {
        let mut chain = EvidenceChain::new();
        chain.push(EvidenceItem {
            event_type: "AnomalyDetected".into(),
            details: "test evidence".into(),
            timestamp: SystemTime::now(),
            confidence: 0.9,
        });
        chain
    }

    fn sample_autopsy() -> AutopsyReport {
        AutopsyReport {
            attack_vector: "sybil".into(),
            affected_tasks: vec![[1u8; 32]],
            affected_fossils: vec![[2u8; 32]],
            behavioral_signature: vec![0.5, 0.7, 0.3],
            recommendations: vec!["blacklist".into()],
        }
    }

    // -- Test 1: stats increment correctly on each event type ----------------
    #[test]
    fn test_stats_increment_on_events() {
        let mut orca = make_orca(AggressionLevel::Paranoid);
        let node = NodeId::new();

        // Anomaly → anomalies_seen
        orca.decide_on_anomaly(node, 5.0, "test");
        assert_eq!(orca.stats().anomalies_seen, 1);

        // Threat → threats_confirmed
        orca.decide_on_threat(node, sample_evidence());
        assert_eq!(orca.stats().threats_confirmed, 1);

        // Quarantine → nodes_quarantined
        orca.decide_post_quarantine(node);
        assert_eq!(orca.stats().nodes_quarantined, 1);

        // Kill → nodes_killed + autopsies_completed
        orca.decide_post_kill(node, &sample_autopsy());
        assert_eq!(orca.stats().nodes_killed, 1);
        assert_eq!(orca.stats().autopsies_completed, 1);
    }

    // -- Test 2: Paranoid mode starts investigation at score 2.0 -------------
    #[test]
    fn test_paranoid_low_score_starts_investigation() {
        let mut orca = make_orca(AggressionLevel::Paranoid);
        let node = NodeId::new();

        orca.decide_on_anomaly(node, 2.0, "low anomaly");
        assert_eq!(orca.active_response_count(), 1);
        let state = orca.active_responses.get(&node).unwrap();
        assert_eq!(state.stage, ResponseStage::Investigating);
    }

    // -- Test 3: Balanced mode ignores score below 3.0 -----------------------
    #[test]
    fn test_balanced_below_threshold_no_action() {
        let mut orca = make_orca(AggressionLevel::Balanced);
        let node = NodeId::new();

        orca.decide_on_anomaly(node, 2.0, "low anomaly");
        assert_eq!(orca.active_response_count(), 0);
        assert_eq!(orca.stats().anomalies_seen, 1); // still counted
    }

    // -- Test 4: Balanced mode starts investigation at score 4.0 -------------
    #[test]
    fn test_balanced_above_threshold_starts_investigation() {
        let mut orca = make_orca(AggressionLevel::Balanced);
        let node = NodeId::new();

        orca.decide_on_anomaly(node, 4.0, "high anomaly");
        assert_eq!(orca.active_response_count(), 1);
        let state = orca.active_responses.get(&node).unwrap();
        assert_eq!(state.stage, ResponseStage::Investigating);
    }

    // -- Test 5: threat level computation ------------------------------------
    #[test]
    fn test_threat_level_computation() {
        let mut orca = make_orca(AggressionLevel::Balanced);

        // 0 threats = Low
        orca.update_threat_level();
        assert_eq!(*orca.threat_level(), ThreatLevel::Low);

        // 3 threats = High
        for _ in 0..3 {
            orca.decide_on_threat(NodeId::new(), sample_evidence());
        }
        assert_eq!(*orca.threat_level(), ThreatLevel::High);

        // 6 threats = Critical (3 already + 3 more)
        for _ in 0..3 {
            orca.decide_on_threat(NodeId::new(), sample_evidence());
        }
        assert_eq!(*orca.threat_level(), ThreatLevel::Critical);
    }

    // -- Test 6: auto-escalation with windowed timestamps --------------------
    #[test]
    fn test_auto_escalation_window() {
        let config = OrcaConfig {
            aggression: AggressionLevel::Balanced,
            auto_escalate_threshold: 3,
            // Very short window — 1 second
            auto_escalate_window: Duration::from_secs(1),
        };
        let bus = Arc::new(NeuromancerBus::new(64));
        let mut orca = BabyOrca::new(config, bus, None);

        // Add old timestamps (well outside the 1s window)
        let old_time = SystemTime::now() - Duration::from_secs(10);
        for _ in 0..10 {
            orca.recent_threat_timestamps.push(old_time);
        }

        // All old timestamps should be pruned
        orca.update_threat_level();
        assert_eq!(*orca.threat_level(), ThreatLevel::Low);

        // Now add fresh threats
        for _ in 0..3 {
            orca.recent_threat_timestamps.push(SystemTime::now());
        }
        orca.update_threat_level();
        assert_eq!(*orca.threat_level(), ThreatLevel::High);
    }

    // -- Test 7: post-kill triggers Recovering stage -------------------------
    #[test]
    fn test_post_kill_recovering_stage() {
        let mut orca = make_orca(AggressionLevel::Paranoid);
        let node = NodeId::new();

        // Build up response state through normal flow
        orca.decide_on_anomaly(node, 8.0, "nasty");
        orca.decide_on_threat(node, sample_evidence());

        // Now kill
        orca.decide_post_kill(node, &sample_autopsy());

        let state = orca.active_responses.get(&node).unwrap();
        assert_eq!(state.stage, ResponseStage::Recovering);
        assert_eq!(orca.stats().nodes_killed, 1);
        assert_eq!(orca.stats().autopsies_completed, 1);
    }

    // -- Test 8: handle_event dispatches correctly ---------------------------
    #[test]
    fn test_handle_event_dispatch() {
        let mut orca = make_orca(AggressionLevel::Paranoid);
        let node = NodeId::new();
        let now = SystemTime::now();

        // AnomalyDetected
        let event = MarabuntaEvent::AnomalyDetected {
            node,
            score: 7.0,
            details: "cpu spike".into(),
            timestamp: now,
        };
        orca.handle_event(&event);
        assert_eq!(orca.stats().anomalies_seen, 1);
        assert_eq!(orca.active_response_count(), 1);

        // ThreatConfirmed
        let event = MarabuntaEvent::ThreatConfirmed {
            node,
            evidence: sample_evidence(),
            timestamp: now,
        };
        orca.handle_event(&event);
        assert_eq!(orca.stats().threats_confirmed, 1);

        // AutopsyCompleted
        let event = MarabuntaEvent::AutopsyCompleted {
            node,
            findings: sample_autopsy(),
            timestamp: now,
        };
        orca.handle_event(&event);
        assert_eq!(orca.stats().nodes_killed, 1);
        assert_eq!(orca.stats().autopsies_completed, 1);
    }

    // -- Test 9: Permissive mode ignores moderate scores ---------------------
    #[test]
    fn test_permissive_mode_high_threshold() {
        let mut orca = make_orca(AggressionLevel::Permissive);
        let node = NodeId::new();

        // Score 4.9 — below 5.0 threshold
        orca.decide_on_anomaly(node, 4.9, "moderate anomaly");
        assert_eq!(orca.active_response_count(), 0);

        // Score 5.0 — exactly at threshold
        orca.decide_on_anomaly(node, 5.0, "serious anomaly");
        assert_eq!(orca.active_response_count(), 1);
    }

    // -- Test 10: pack hunt detection ----------------------------------------
    #[test]
    fn test_pack_hunt_detection() {
        let config = OrcaConfig {
            aggression: AggressionLevel::Paranoid,
            auto_escalate_threshold: 3,
            auto_escalate_window: Duration::from_secs(300),
        };
        let bus = Arc::new(NeuromancerBus::new(64));
        let mut rx = bus.subscribe();
        let mut orca = BabyOrca::new(config, bus, None);

        // Create 3 nodes in Responding stage to trigger pack hunt
        let nodes: Vec<NodeId> = (0..3).map(|_| NodeId::new()).collect();
        for &node in &nodes {
            orca.decide_on_anomaly(node, 9.0, "attack");
            orca.decide_on_threat(node, sample_evidence());
        }

        // Quarantine one — should detect pack-hunt pattern (3 responding)
        orca.decide_post_quarantine(nodes[0]);
        assert_eq!(orca.stats().pack_hunts_initiated, 1);

        // Verify PackHuntInitiated was emitted on the bus
        // (first 3 events are DeceptionStarted from decide_on_anomaly,
        //  then the PackHuntInitiated)
        let mut found_pack_hunt = false;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, MarabuntaEvent::PackHuntInitiated { .. }) {
                found_pack_hunt = true;
                break;
            }
        }
        assert!(found_pack_hunt, "PackHuntInitiated event should be emitted");
    }
}
