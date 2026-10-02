// Marabunta - Licensed under the MIT License.
//! Mantis — Deception & immigration for the Neuromancer subsystem.
//!
//! Provides two deception modes:
//!
//! - **IntelGathering**: silently feeds honeypot tasks to a suspected node
//!   in order to capture its responses and learn about its attack strategy.
//!   Sessions expire after `max_deception_duration`.
//!
//! - **Immigration**: vets newly joining nodes by sending them a configurable
//!   number of honeypot tasks and requiring a minimum pass rate before
//!   granting graduation (full membership).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::MantisConfig;
use super::types::*;

// ============================================================================
// Types
// ============================================================================

/// The purpose of a deception session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeceptionMode {
    /// Silently gather intelligence about a suspected malicious node.
    IntelGathering,
    /// Vet a new node before granting it full swarm membership.
    Immigration,
}

/// A captured response from a deception target.
#[derive(Debug, Clone)]
pub struct CapturedResponse {
    pub task_id: TaskId,
    pub response_data: Vec<u8>,
    pub timestamp: SystemTime,
    pub passed: bool,
}

/// Tracks an active deception session against a single node.
#[derive(Debug, Clone)]
pub struct DeceptionSession {
    pub node: NodeId,
    pub mode: DeceptionMode,
    pub started_at: SystemTime,
    pub honeypot_tasks_sent: usize,
    pub honeypot_tasks_passed: usize,
    pub honeypot_tasks_failed: usize,
    pub captured_responses: Vec<CapturedResponse>,
    pub graduated: bool,
}

// ============================================================================
// Mantis engine
// ============================================================================

/// Deception and immigration coordinator.
///
/// Manages active deception sessions, captures responses from targets, and
/// evaluates immigration candidates for graduation into the swarm.
pub struct Mantis {
    config: MantisConfig,
    bus: Arc<NeuromancerBus>,
    active_sessions: HashMap<NodeId, DeceptionSession>,
    graduated_nodes: HashSet<NodeId>,
    total_sessions: u64,
}

impl Mantis {
    /// Create a new Mantis engine.
    pub fn new(config: MantisConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self {
            config,
            bus,
            active_sessions: HashMap::new(),
            graduated_nodes: HashSet::new(),
            total_sessions: 0,
        }
    }

    /// Start a deception session for the given node.
    ///
    /// Respects configuration flags: if `enable_intel_gathering` is false and
    /// the mode is `IntelGathering`, the call is a no-op (likewise for
    /// `enable_immigration` and `Immigration`).
    pub fn start_deception(&mut self, node: NodeId, mode: DeceptionMode) {
        match mode {
            DeceptionMode::IntelGathering if !self.config.enable_intel_gathering => {
                debug!(?node, "Mantis: intel gathering disabled, ignoring");
                return;
            }
            DeceptionMode::Immigration if !self.config.enable_immigration => {
                debug!(?node, "Mantis: immigration disabled, ignoring");
                return;
            }
            _ => {}
        }

        if self.active_sessions.contains_key(&node) {
            warn!(?node, "Mantis: session already active for node");
            return;
        }

        let now = SystemTime::now();

        let session = DeceptionSession {
            node,
            mode,
            started_at: now,
            honeypot_tasks_sent: 0,
            honeypot_tasks_passed: 0,
            honeypot_tasks_failed: 0,
            captured_responses: Vec::new(),
            graduated: false,
        };

        info!(?node, ?mode, "Mantis: starting deception session");

        self.active_sessions.insert(node, session);
        self.total_sessions += 1;

        self.bus.emit(MarabuntaEvent::DeceptionStarted {
            node,
            timestamp: now,
        });
    }

    /// Record a response from a node under deception.
    ///
    /// Updates the session's pass/fail counters and appends the captured
    /// response.  If the node does not have an active session, the call
    /// is silently ignored.
    pub fn record_response(
        &mut self,
        node: &NodeId,
        task_id: TaskId,
        response_data: Vec<u8>,
        passed: bool,
    ) {
        let session = match self.active_sessions.get_mut(node) {
            Some(s) => s,
            None => {
                debug!(
                    ?node,
                    "Mantis: no active session for node, ignoring response"
                );
                return;
            }
        };

        session.honeypot_tasks_sent += 1;
        if passed {
            session.honeypot_tasks_passed += 1;
        } else {
            session.honeypot_tasks_failed += 1;
        }

        session.captured_responses.push(CapturedResponse {
            task_id,
            response_data,
            timestamp: SystemTime::now(),
            passed,
        });

        debug!(
            ?node,
            sent = session.honeypot_tasks_sent,
            passed_count = session.honeypot_tasks_passed,
            failed_count = session.honeypot_tasks_failed,
            "Mantis: recorded response"
        );
    }

    /// Expire intel-gathering sessions that have exceeded `max_deception_duration`.
    ///
    /// Immigration sessions are not affected by this method; they are
    /// evaluated via [`evaluate_immigration`](Self::evaluate_immigration).
    pub fn check_expirations(&mut self) {
        let now = SystemTime::now();
        let max_duration = self.config.max_deception_duration;

        let expired: Vec<NodeId> = self
            .active_sessions
            .iter()
            .filter(|(_, s)| s.mode == DeceptionMode::IntelGathering)
            .filter(|(_, s)| {
                now.duration_since(s.started_at)
                    .map(|d| d > max_duration)
                    .unwrap_or(false)
            })
            .map(|(node, _)| *node)
            .collect();

        for node in &expired {
            info!(?node, "Mantis: intel-gathering session expired");
            self.active_sessions.remove(node);
        }

        if !expired.is_empty() {
            debug!(count = expired.len(), "Mantis: expired intel sessions");
        }
    }

    /// Evaluate all active immigration sessions.
    ///
    /// For each immigration session where the node has been sent at least
    /// `immigration_honeypot_count` tasks, check whether the pass rate meets
    /// `immigration_pass_rate`.  Returns a list of `(NodeId, passed)` pairs.
    ///
    /// Graduated nodes are moved to the `graduated_nodes` set and their
    /// session is removed.  Rejected nodes have their session removed.
    /// A `NodeJoined` event is emitted for graduates.
    pub fn evaluate_immigration(&mut self) -> Vec<(NodeId, bool)> {
        let required_count = self.config.immigration_honeypot_count;
        let required_rate = self.config.immigration_pass_rate;

        // Collect candidates that have received enough honeypot tasks.
        let candidates: Vec<NodeId> = self
            .active_sessions
            .iter()
            .filter(|(_, s)| s.mode == DeceptionMode::Immigration)
            .filter(|(_, s)| s.honeypot_tasks_sent >= required_count)
            .map(|(node, _)| *node)
            .collect();

        let mut results = Vec::new();

        for node in candidates {
            let session = match self.active_sessions.remove(&node) {
                Some(s) => s,
                None => continue,
            };

            let pass_rate = if session.honeypot_tasks_sent == 0 {
                0.0
            } else {
                session.honeypot_tasks_passed as f64 / session.honeypot_tasks_sent as f64
            };

            let passed = pass_rate >= required_rate as f64;

            if passed {
                info!(?node, pass_rate, "Mantis: immigration candidate graduated");
                self.graduated_nodes.insert(node);

                self.bus.emit(MarabuntaEvent::NodeJoined {
                    node,
                    capabilities: Vec::new(),
                    timestamp: SystemTime::now(),
                });
            } else {
                warn!(
                    ?node,
                    pass_rate, required_rate, "Mantis: immigration candidate rejected"
                );
            }

            results.push((node, passed));
        }

        results
    }

    /// Check whether a node is currently under deception (active session).
    pub fn is_under_deception(&self, node: &NodeId) -> bool {
        self.active_sessions.contains_key(node)
    }

    /// Check whether a node has graduated from immigration.
    pub fn has_graduated(&self, node: &NodeId) -> bool {
        self.graduated_nodes.contains(node)
    }

    /// How many active deception sessions exist.
    pub fn active_session_count(&self) -> usize {
        self.active_sessions.len()
    }

    /// How many total sessions have been created since construction.
    pub fn total_sessions(&self) -> u64 {
        self.total_sessions
    }

    /// How many nodes have graduated from immigration.
    pub fn graduated_count(&self) -> usize {
        self.graduated_nodes.len()
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::bus::NeuromancerBus;
    use super::*;
    use std::time::Duration;

    fn test_config() -> MantisConfig {
        MantisConfig {
            max_deception_duration: Duration::from_secs(60),
            enable_intel_gathering: true,
            enable_immigration: true,
            immigration_honeypot_count: 3,
            immigration_pass_rate: 0.8,
            ..Default::default()
        }
    }

    fn make_bus() -> Arc<NeuromancerBus> {
        Arc::new(NeuromancerBus::new(64))
    }

    #[test]
    fn test_start_intel_gathering_emits_deception_started() {
        let bus = make_bus();
        let mut rx = bus.subscribe();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::IntelGathering);

        assert_eq!(mantis.active_session_count(), 1);
        assert!(mantis.is_under_deception(&node));

        let event = rx.try_recv().expect("expected DeceptionStarted event");
        assert_eq!(event.type_name(), "DeceptionStarted");
    }

    #[test]
    fn test_start_immigration_session_tracked() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::Immigration);

        assert_eq!(mantis.active_session_count(), 1);
        assert!(mantis.is_under_deception(&node));
        assert!(!mantis.has_graduated(&node));
        assert_eq!(mantis.total_sessions(), 1);
    }

    #[test]
    fn test_expiration_removes_intel_sessions_past_duration() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::IntelGathering);

        // Manually backdate the session start to trigger expiration.
        if let Some(session) = mantis.active_sessions.get_mut(&node) {
            session.started_at = SystemTime::now() - Duration::from_secs(3600);
        }

        mantis.check_expirations();

        assert_eq!(mantis.active_session_count(), 0);
        assert!(!mantis.is_under_deception(&node));
    }

    #[test]
    fn test_expiration_does_not_remove_immigration_sessions() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::Immigration);

        // Manually backdate the session start.
        if let Some(session) = mantis.active_sessions.get_mut(&node) {
            session.started_at = SystemTime::now() - Duration::from_secs(3600);
        }

        mantis.check_expirations();

        // Immigration sessions should survive expiration check.
        assert_eq!(mantis.active_session_count(), 1);
        assert!(mantis.is_under_deception(&node));
    }

    #[test]
    fn test_immigration_all_pass_graduated() {
        let bus = make_bus();
        let mut rx = bus.subscribe();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::Immigration);

        // Consume the DeceptionStarted event.
        let _ = rx.try_recv();

        // Send 3 passing honeypot results (config requires 3).
        for i in 0..3 {
            mantis.record_response(&node, [i; 32], vec![1, 2, 3], true);
        }

        let results = mantis.evaluate_immigration();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (node, true));
        assert!(mantis.has_graduated(&node));
        assert!(!mantis.is_under_deception(&node));
        assert_eq!(mantis.graduated_count(), 1);

        // NodeJoined event should have been emitted.
        let event = rx.try_recv().expect("expected NodeJoined event");
        assert_eq!(event.type_name(), "NodeJoined");
    }

    #[test]
    fn test_immigration_fail_rate_too_high_rejected() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::Immigration);

        // Send 3 responses: 1 pass, 2 fails → pass_rate = 0.333 < 0.8.
        mantis.record_response(&node, [1; 32], vec![1], true);
        mantis.record_response(&node, [2; 32], vec![2], false);
        mantis.record_response(&node, [3; 32], vec![3], false);

        let results = mantis.evaluate_immigration();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (node, false));
        assert!(!mantis.has_graduated(&node));
        assert!(!mantis.is_under_deception(&node));
    }

    #[test]
    fn test_is_under_deception_returns_true_during_active_session() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        let other = NodeId::new();

        mantis.start_deception(node, DeceptionMode::IntelGathering);

        assert!(mantis.is_under_deception(&node));
        assert!(!mantis.is_under_deception(&other));
    }

    #[test]
    fn test_response_capture_records_correctly() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::IntelGathering);

        let task_id = [42u8; 32];
        let data = vec![10, 20, 30];
        mantis.record_response(&node, task_id, data.clone(), true);

        let session = mantis.active_sessions.get(&node).unwrap();
        assert_eq!(session.honeypot_tasks_sent, 1);
        assert_eq!(session.honeypot_tasks_passed, 1);
        assert_eq!(session.honeypot_tasks_failed, 0);
        assert_eq!(session.captured_responses.len(), 1);
        assert_eq!(session.captured_responses[0].task_id, task_id);
        assert_eq!(session.captured_responses[0].response_data, data);
        assert!(session.captured_responses[0].passed);
    }

    #[test]
    fn test_response_for_unknown_node_is_ignored() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let unknown = NodeId::new();
        mantis.record_response(&unknown, [0; 32], vec![], true);

        // Should not panic and should have no sessions.
        assert_eq!(mantis.active_session_count(), 0);
    }

    #[test]
    fn test_disabled_intel_gathering_noop() {
        let bus = make_bus();
        let config = MantisConfig {
            enable_intel_gathering: false,
            ..test_config()
        };
        let mut mantis = Mantis::new(config, bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::IntelGathering);

        assert_eq!(mantis.active_session_count(), 0);
        assert!(!mantis.is_under_deception(&node));
    }

    #[test]
    fn test_disabled_immigration_noop() {
        let bus = make_bus();
        let config = MantisConfig {
            enable_immigration: false,
            ..test_config()
        };
        let mut mantis = Mantis::new(config, bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::Immigration);

        assert_eq!(mantis.active_session_count(), 0);
        assert!(!mantis.is_under_deception(&node));
    }

    #[test]
    fn test_immigration_not_evaluated_until_enough_tasks() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::Immigration);

        // Only 2 of 3 required responses sent.
        mantis.record_response(&node, [1; 32], vec![], true);
        mantis.record_response(&node, [2; 32], vec![], true);

        let results = mantis.evaluate_immigration();

        // Should not evaluate yet (only 2 of 3 tasks sent).
        assert!(results.is_empty());
        assert!(mantis.is_under_deception(&node));
    }

    #[test]
    fn test_duplicate_session_start_ignored() {
        let bus = make_bus();
        let mut mantis = Mantis::new(test_config(), bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::IntelGathering);
        mantis.start_deception(node, DeceptionMode::Immigration);

        // Second call should be ignored — still only one session.
        assert_eq!(mantis.active_session_count(), 1);
        assert_eq!(mantis.total_sessions(), 1);
    }

    #[test]
    fn test_immigration_exact_pass_rate_threshold() {
        let bus = make_bus();
        let config = MantisConfig {
            immigration_honeypot_count: 5,
            immigration_pass_rate: 0.6,
            ..test_config()
        };
        let mut mantis = Mantis::new(config, bus);

        let node = NodeId::new();
        mantis.start_deception(node, DeceptionMode::Immigration);

        // 3 pass, 2 fail → pass_rate = 0.6, exactly meeting the threshold.
        mantis.record_response(&node, [1; 32], vec![], true);
        mantis.record_response(&node, [2; 32], vec![], true);
        mantis.record_response(&node, [3; 32], vec![], true);
        mantis.record_response(&node, [4; 32], vec![], false);
        mantis.record_response(&node, [5; 32], vec![], false);

        let results = mantis.evaluate_immigration();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (node, true));
        assert!(mantis.has_graduated(&node));
    }
}
