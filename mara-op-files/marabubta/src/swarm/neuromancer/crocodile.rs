// Marabunta - Licensed under the MIT License.
//! Crocodile — Honeypot ambush trap.
//!
//! Deploys deterministic hash-verification tasks to nodes suspected of
//! malicious behaviour.  If a node returns the wrong blake3 hash for
//! known input data, the mismatch is irrefutable proof of tampering and
//! a [`MarabuntaEvent::ThreatConfirmed`] is emitted on the bus.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::CrocodileConfig;
use super::types::*;

// ============================================================================
// HoneypotResult
// ============================================================================

/// Outcome of a honeypot task once the target node responds (or fails to).
#[derive(Debug, Clone)]
pub enum HoneypotResult {
    /// Node returned the correct blake3 hash.
    Correct,
    /// Node returned an incorrect hash.
    Incorrect { actual_hash: Blake3Hash },
    /// Node did not respond within the configured timeout.
    Timeout,
    /// Node could not be reached at all.
    NodeUnreachable,
}

// ============================================================================
// HoneypotTask
// ============================================================================

/// A single honeypot task deployed to a target node.
#[derive(Debug, Clone)]
pub struct HoneypotTask {
    pub task_id: TaskId,
    pub target_node: NodeId,
    pub expected_hash: Blake3Hash,
    pub input_data: Vec<u8>,
    pub created_at: SystemTime,
    pub timeout: Duration,
    pub fast_track: bool,
    pub retries: u32,
}

// ============================================================================
// HoneypotGenerator trait + default implementation
// ============================================================================

/// Generates random input data and the expected blake3 hash for honeypot
/// verification tasks.
pub trait HoneypotGenerator: Send + Sync {
    fn generate(&self) -> (Vec<u8>, Blake3Hash);
}

/// Default generator: 256 random bytes hashed with blake3.
pub struct HashHoneypotGenerator;

impl HoneypotGenerator for HashHoneypotGenerator {
    fn generate(&self) -> (Vec<u8>, Blake3Hash) {
        let data: Vec<u8> = (0..256).map(|_| rand::random::<u8>()).collect();
        let hash = blake3::hash(&data);
        (data, *hash.as_bytes())
    }
}

// ============================================================================
// Crocodile
// ============================================================================

/// Honeypot ambush engine.
///
/// Deploys deterministic verification tasks to nodes, tracks active
/// honeypots, and emits threat-confirmation events when a node returns
/// an incorrect result.
pub struct Crocodile {
    config: CrocodileConfig,
    bus: Arc<NeuromancerBus>,
    active_honeypots: HashMap<TaskId, HoneypotTask>,
    generator: Box<dyn HoneypotGenerator>,
    results: Vec<(TaskId, HoneypotResult)>,
}

impl Crocodile {
    /// Create a new Crocodile with the default [`HashHoneypotGenerator`].
    pub fn new(config: CrocodileConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self {
            config,
            bus,
            active_honeypots: HashMap::new(),
            generator: Box::new(HashHoneypotGenerator),
            results: Vec::new(),
        }
    }

    /// Create a Crocodile with a custom generator (useful for testing).
    #[cfg(test)]
    pub fn with_generator(
        config: CrocodileConfig,
        bus: Arc<NeuromancerBus>,
        generator: Box<dyn HoneypotGenerator>,
    ) -> Self {
        Self {
            config,
            bus,
            active_honeypots: HashMap::new(),
            generator,
            results: Vec::new(),
        }
    }

    /// Deploy a standard honeypot to the given node.
    ///
    /// Returns the generated `TaskId`, or `None` if the max-active limit
    /// has been reached.
    pub fn deploy_honeypot(&mut self, target_node: NodeId) -> Option<TaskId> {
        if self.active_honeypots.len() >= self.config.max_active_honeypots {
            warn!(
                "max active honeypots ({}) reached, cannot deploy",
                self.config.max_active_honeypots
            );
            return None;
        }

        let task_id = rand::random::<[u8; 32]>();
        let (input_data, expected_hash) = self.generator.generate();

        let task = HoneypotTask {
            task_id,
            target_node,
            expected_hash,
            input_data: input_data.clone(),
            created_at: SystemTime::now(),
            timeout: self.config.honeypot_timeout,
            fast_track: false,
            retries: 0,
        };

        self.active_honeypots.insert(task_id, task);

        // Emit TaskSubmitted so the scheduler dispatches it to the target.
        self.bus.emit(MarabuntaEvent::TaskSubmitted {
            id: task_id,
            input_hash: *blake3::hash(&input_data).as_bytes(),
            requirements: ResourceRequirements::default(),
            timestamp: SystemTime::now(),
        });

        info!(
            node = ?target_node,
            "deployed honeypot task"
        );
        Some(task_id)
    }

    /// Deploy a fast-track honeypot (shorter timeout, used during pack hunts).
    pub fn deploy_fast_track(&mut self, target_node: NodeId) -> Option<TaskId> {
        if self.active_honeypots.len() >= self.config.max_active_honeypots {
            warn!(
                "max active honeypots ({}) reached, cannot deploy fast-track",
                self.config.max_active_honeypots
            );
            return None;
        }

        let task_id = rand::random::<[u8; 32]>();
        let (input_data, expected_hash) = self.generator.generate();

        let task = HoneypotTask {
            task_id,
            target_node,
            expected_hash,
            input_data: input_data.clone(),
            created_at: SystemTime::now(),
            timeout: self.config.fast_track_timeout,
            fast_track: true,
            retries: 0,
        };

        self.active_honeypots.insert(task_id, task);

        self.bus.emit(MarabuntaEvent::TaskSubmitted {
            id: task_id,
            input_hash: *blake3::hash(&input_data).as_bytes(),
            requirements: ResourceRequirements::default(),
            timestamp: SystemTime::now(),
        });

        info!(
            node = ?target_node,
            "deployed fast-track honeypot task"
        );
        Some(task_id)
    }

    /// Handle a completed task result.
    ///
    /// If `task_id` matches an active honeypot, verifies the result hash.
    /// On mismatch, emits [`MarabuntaEvent::ThreatConfirmed`] with evidence.
    /// On match, removes the honeypot from the active set.
    ///
    /// Returns `true` if the task_id was a known honeypot.
    pub fn handle_task_completed(
        &mut self,
        task_id: TaskId,
        result_hash: Blake3Hash,
        node: NodeId,
    ) -> bool {
        let task = match self.active_honeypots.remove(&task_id) {
            Some(t) => t,
            None => return false,
        };

        if result_hash == task.expected_hash {
            debug!(node = ?node, "honeypot passed — node is clean");
            self.results.push((task_id, HoneypotResult::Correct));
        } else {
            warn!(
                node = ?node,
                expected = ?hex_short(&task.expected_hash),
                actual = ?hex_short(&result_hash),
                "honeypot FAILED — hash mismatch"
            );

            let result = HoneypotResult::Incorrect {
                actual_hash: result_hash,
            };
            self.results.push((task_id, result));

            let mut evidence = EvidenceChain::new();
            evidence.push(EvidenceItem {
                event_type: "HoneypotMismatch".into(),
                details: format!(
                    "expected hash {:?}, got {:?}",
                    hex_short(&task.expected_hash),
                    hex_short(&result_hash),
                ),
                timestamp: SystemTime::now(),
                confidence: 1.0,
            });

            self.bus.emit(MarabuntaEvent::ThreatConfirmed {
                node,
                evidence,
                timestamp: SystemTime::now(),
            });
        }

        true
    }

    /// Expire active honeypots whose timeout has elapsed.
    ///
    /// Timed-out honeypots are recorded as [`HoneypotResult::Timeout`] and
    /// treated as suspicious (but not yet confirmed threats).
    pub fn check_timeouts(&mut self) {
        let now = SystemTime::now();
        let mut expired: Vec<TaskId> = Vec::new();

        for (task_id, task) in &self.active_honeypots {
            let elapsed = now.duration_since(task.created_at).unwrap_or_default();
            if elapsed >= task.timeout {
                expired.push(*task_id);
            }
        }

        for task_id in expired {
            if let Some(task) = self.active_honeypots.remove(&task_id) {
                warn!(
                    node = ?task.target_node,
                    fast_track = task.fast_track,
                    "honeypot timed out"
                );
                self.results.push((task_id, HoneypotResult::Timeout));

                self.bus.emit(MarabuntaEvent::AnomalyDetected {
                    node: task.target_node,
                    score: 0.8,
                    details: format!(
                        "honeypot task timed out after {:?} (fast_track={})",
                        task.timeout, task.fast_track
                    ),
                    timestamp: SystemTime::now(),
                });
            }
        }
    }

    /// Number of currently active (pending) honeypots.
    pub fn active_count(&self) -> usize {
        self.active_honeypots.len()
    }

    /// Collected results (for diagnostics).
    pub fn results(&self) -> &[(TaskId, HoneypotResult)] {
        &self.results
    }
}

/// Truncate a 32-byte hash to a short hex prefix for logging.
fn hex_short(h: &[u8; 32]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}...",
        h[0], h[1], h[2], h[3]
    )
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::bus::new_shared_bus;

    fn default_config() -> CrocodileConfig {
        CrocodileConfig {
            honeypot_ratio: 20,
            honeypot_timeout: Duration::from_secs(300),
            fast_track_timeout: Duration::from_secs(30),
            max_active_honeypots: 100,
            max_retries: 1,
            ..Default::default()
        }
    }

    /// Deterministic generator for tests — always returns the same data/hash.
    struct FixedGenerator {
        data: Vec<u8>,
        hash: Blake3Hash,
    }

    impl FixedGenerator {
        fn new() -> Self {
            let data = vec![0xAB; 64];
            let hash = *blake3::hash(&data).as_bytes();
            Self { data, hash }
        }
    }

    impl HoneypotGenerator for FixedGenerator {
        fn generate(&self) -> (Vec<u8>, Blake3Hash) {
            (self.data.clone(), self.hash)
        }
    }

    // --- Test 1: correct result triggers no threat ---

    #[tokio::test]
    async fn test_correct_result_no_threat() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();
        let gen = FixedGenerator::new();
        let expected_hash = gen.hash;

        let mut croc = Crocodile::with_generator(
            default_config(),
            bus.clone(),
            Box::new(gen),
        );

        let node = NodeId::new();
        let task_id = croc.deploy_honeypot(node).unwrap();

        // Drain the TaskSubmitted event.
        let _ = rx.recv().await.unwrap();

        // Provide the correct hash.
        let was_honeypot = croc.handle_task_completed(task_id, expected_hash, node);
        assert!(was_honeypot);
        assert_eq!(croc.active_count(), 0);

        // No further event should be emitted — try_recv should fail.
        assert!(rx.try_recv().is_err());
    }

    // --- Test 2: wrong result emits ThreatConfirmed ---

    #[tokio::test]
    async fn test_wrong_result_emits_threat_confirmed() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();
        let gen = FixedGenerator::new();

        let mut croc = Crocodile::with_generator(
            default_config(),
            bus.clone(),
            Box::new(gen),
        );

        let node = NodeId::new();
        let task_id = croc.deploy_honeypot(node).unwrap();

        // Drain TaskSubmitted.
        let _ = rx.recv().await.unwrap();

        // Provide a wrong hash.
        let wrong_hash = [0xFFu8; 32];
        croc.handle_task_completed(task_id, wrong_hash, node);

        let event = rx.recv().await.unwrap();
        match event {
            MarabuntaEvent::ThreatConfirmed {
                node: n, evidence, ..
            } => {
                assert_eq!(n, node);
                assert!(!evidence.events.is_empty());
                assert_eq!(evidence.events[0].event_type, "HoneypotMismatch");
                assert!((evidence.events[0].confidence - 1.0).abs() < f64::EPSILON);
            }
            other => panic!("expected ThreatConfirmed, got {:?}", other),
        }
    }

    // --- Test 3: timeout handling ---

    #[tokio::test]
    async fn test_timeout_handling() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();

        let mut config = default_config();
        config.honeypot_timeout = Duration::from_millis(1);

        let gen = FixedGenerator::new();
        let mut croc = Crocodile::with_generator(
            config,
            bus.clone(),
            Box::new(gen),
        );

        let node = NodeId::new();
        croc.deploy_honeypot(node).unwrap();
        assert_eq!(croc.active_count(), 1);

        // Drain TaskSubmitted.
        let _ = rx.recv().await.unwrap();

        // Wait just long enough for the 1ms timeout.
        tokio::time::sleep(Duration::from_millis(10)).await;

        croc.check_timeouts();
        assert_eq!(croc.active_count(), 0);

        // Should have emitted AnomalyDetected for the timeout.
        let event = rx.recv().await.unwrap();
        match event {
            MarabuntaEvent::AnomalyDetected { node: n, .. } => {
                assert_eq!(n, node);
            }
            other => panic!("expected AnomalyDetected, got {:?}", other),
        }
    }

    // --- Test 4: fast-track uses shorter timeout ---

    #[tokio::test]
    async fn test_fast_track_honeypot() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();

        let mut config = default_config();
        config.fast_track_timeout = Duration::from_millis(1);
        config.honeypot_timeout = Duration::from_secs(999);

        let gen = FixedGenerator::new();
        let mut croc = Crocodile::with_generator(
            config,
            bus.clone(),
            Box::new(gen),
        );

        let node = NodeId::new();
        let task_id = croc.deploy_fast_track(node).unwrap();

        // Drain TaskSubmitted.
        let _ = rx.recv().await.unwrap();

        // Verify the task is marked fast_track with the short timeout.
        // We cannot peek inside, but we can verify timeout fires quickly.
        tokio::time::sleep(Duration::from_millis(10)).await;
        croc.check_timeouts();
        assert_eq!(croc.active_count(), 0);

        // The task should have timed out (fast_track_timeout = 1ms).
        let found_timeout = croc
            .results()
            .iter()
            .any(|(id, r)| *id == task_id && matches!(r, HoneypotResult::Timeout));
        assert!(found_timeout);
    }

    // --- Test 5: max active limit ---

    #[test]
    fn test_max_active_honeypots_limit() {
        let bus = new_shared_bus();

        let mut config = default_config();
        config.max_active_honeypots = 3;

        let mut croc = Crocodile::new(config, bus);

        for _ in 0..3 {
            let node = NodeId::new();
            assert!(croc.deploy_honeypot(node).is_some());
        }
        assert_eq!(croc.active_count(), 3);

        // Fourth deployment should be rejected.
        let node = NodeId::new();
        assert!(croc.deploy_honeypot(node).is_none());
        assert_eq!(croc.active_count(), 3);
    }

    // --- Test 6: generator produces valid pairs ---

    #[test]
    fn test_hash_honeypot_generator_produces_valid_pairs() {
        let gen = HashHoneypotGenerator;
        for _ in 0..50 {
            let (data, hash) = gen.generate();
            assert_eq!(data.len(), 256);
            let computed = *blake3::hash(&data).as_bytes();
            assert_eq!(computed, hash);
        }
    }

    // --- Test 7: unknown task_id is a no-op ---

    #[test]
    fn test_unknown_task_id_is_noop() {
        let bus = new_shared_bus();
        let mut croc = Crocodile::new(default_config(), bus);

        let unknown_id = rand::random::<[u8; 32]>();
        let result = croc.handle_task_completed(unknown_id, [0u8; 32], NodeId::new());
        assert!(!result);
    }

    // --- Test 8: max active limit also applies to fast-track ---

    #[test]
    fn test_max_active_limit_applies_to_fast_track() {
        let bus = new_shared_bus();

        let mut config = default_config();
        config.max_active_honeypots = 2;

        let mut croc = Crocodile::new(config, bus);

        assert!(croc.deploy_honeypot(NodeId::new()).is_some());
        assert!(croc.deploy_fast_track(NodeId::new()).is_some());
        assert_eq!(croc.active_count(), 2);

        // Both slots full.
        assert!(croc.deploy_honeypot(NodeId::new()).is_none());
        assert!(croc.deploy_fast_track(NodeId::new()).is_none());
    }
}
