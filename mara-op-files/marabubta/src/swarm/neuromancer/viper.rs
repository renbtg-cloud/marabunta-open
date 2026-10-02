// Marabunta - Licensed under the MIT License.
//! Viper — Instant node quarantine.
//!
//! When a [`MarabuntaEvent::ThreatConfirmed`] arrives, the Viper immediately
//! isolates the offending node by adding it to the quarantine set and
//! emitting [`MarabuntaEvent::NodeQuarantined`].  Quarantined nodes are held
//! for a configurable grace period before they can be released (e.g. for
//! immigration re-evaluation by Mantis).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info, warn};

use super::bus::NeuromancerBus;
use crate::swarm::neuromancer::config::ViperConfig;
use super::types::*;

// ============================================================================
// Viper
// ============================================================================

/// Instant quarantine engine.
///
/// Maintains a set of quarantined nodes and emits bus events on state
/// transitions.  The grace period allows downstream modules (Mantis,
/// Elektra) to inspect or rehabilitate the node before permanent action.
pub struct Viper {
    config: ViperConfig,
    bus: Arc<NeuromancerBus>,
    quarantined: HashSet<NodeId>,
    quarantine_times: HashMap<NodeId, SystemTime>,
}

impl Viper {
    /// Create a new Viper.
    pub fn new(config: ViperConfig, bus: Arc<NeuromancerBus>) -> Self {
        Self {
            config,
            bus,
            quarantined: HashSet::new(),
            quarantine_times: HashMap::new(),
        }
    }

    /// Quarantine a node with supporting evidence.
    ///
    /// If the node is already quarantined, this is a no-op (idempotent).
    /// If the quarantine set is full (`max_quarantined`), the oldest
    /// quarantined node is evicted to make room.
    pub fn quarantine_node(&mut self, node: NodeId, evidence: &EvidenceChain) {
        if self.quarantined.contains(&node) {
            debug!(node = ?node, "node already quarantined — skipping");
            return;
        }

        // Evict the oldest entry if we have hit the cap.
        if self.quarantined.len() >= self.config.max_quarantined {
            if let Some(oldest) = self.oldest_quarantined_node() {
                warn!(
                    evicted = ?oldest,
                    "quarantine full ({}) — evicting oldest to make room",
                    self.config.max_quarantined
                );
                self.quarantined.remove(&oldest);
                self.quarantine_times.remove(&oldest);
            }
        }

        let now = SystemTime::now();
        self.quarantined.insert(node);
        self.quarantine_times.insert(node, now);

        let reason = if evidence.events.is_empty() {
            "threat confirmed (no detail)".to_string()
        } else {
            evidence
                .events
                .iter()
                .map(|e| format!("{}(conf={:.2})", e.event_type, e.confidence))
                .collect::<Vec<_>>()
                .join("; ")
        };

        self.bus.emit(MarabuntaEvent::NodeQuarantined {
            node,
            reason,
            timestamp: now,
        });

        info!(node = ?node, "node quarantined");
    }

    /// Release a node from quarantine.
    ///
    /// Returns `true` if the node was actually quarantined.
    pub fn release_node(&mut self, node: NodeId) -> bool {
        let removed = self.quarantined.remove(&node);
        self.quarantine_times.remove(&node);
        if removed {
            info!(node = ?node, "node released from quarantine");
        }
        removed
    }

    /// Check whether a node is currently quarantined.
    pub fn is_quarantined(&self, node: &NodeId) -> bool {
        self.quarantined.contains(node)
    }

    /// Return all currently quarantined node ids.
    pub fn quarantined_nodes(&self) -> Vec<NodeId> {
        self.quarantined.iter().copied().collect()
    }

    /// Release nodes whose quarantine has exceeded the grace period.
    ///
    /// This allows Mantis or other modules to pick them up for immigration
    /// re-evaluation instead of leaving them quarantined forever.
    pub fn check_grace_period(&mut self) {
        let now = SystemTime::now();
        let grace = self.config.grace_period;

        let expired: Vec<NodeId> = self
            .quarantine_times
            .iter()
            .filter_map(|(node, &time)| {
                let elapsed = now.duration_since(time).unwrap_or_default();
                if elapsed >= grace {
                    Some(*node)
                } else {
                    None
                }
            })
            .collect();

        for node in expired {
            self.quarantined.remove(&node);
            self.quarantine_times.remove(&node);
            info!(node = ?node, "quarantine grace period expired — releasing");
        }
    }

    /// Number of currently quarantined nodes.
    pub fn quarantined_count(&self) -> usize {
        self.quarantined.len()
    }

    // -- internal helpers --

    /// Find the node with the oldest quarantine timestamp.
    fn oldest_quarantined_node(&self) -> Option<NodeId> {
        self.quarantine_times
            .iter()
            .min_by_key(|(_, &time)| time)
            .map(|(node, _)| *node)
    }
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::bus::new_shared_bus;
    use std::time::Duration;

    fn default_config() -> ViperConfig {
        ViperConfig {
            grace_period: Duration::from_secs(60),
            max_quarantined: 20,
            ..Default::default()
        }
    }

    fn sample_evidence() -> EvidenceChain {
        let mut chain = EvidenceChain::new();
        chain.push(EvidenceItem {
            event_type: "HoneypotMismatch".into(),
            details: "wrong hash".into(),
            timestamp: SystemTime::now(),
            confidence: 1.0,
        });
        chain
    }

    // --- Test 1: quarantine emits NodeQuarantined ---

    #[tokio::test]
    async fn test_quarantine_emits_event() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();
        let mut viper = Viper::new(default_config(), bus.clone());

        let node = NodeId::new();
        let evidence = sample_evidence();
        viper.quarantine_node(node, &evidence);

        assert!(viper.is_quarantined(&node));
        assert_eq!(viper.quarantined_count(), 1);

        let event = rx.recv().await.unwrap();
        match event {
            MarabuntaEvent::NodeQuarantined { node: n, reason, .. } => {
                assert_eq!(n, node);
                assert!(reason.contains("HoneypotMismatch"));
            }
            other => panic!("expected NodeQuarantined, got {:?}", other),
        }
    }

    // --- Test 2: idempotent quarantine ---

    #[tokio::test]
    async fn test_quarantine_is_idempotent() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();
        let mut viper = Viper::new(default_config(), bus.clone());

        let node = NodeId::new();
        let evidence = sample_evidence();

        viper.quarantine_node(node, &evidence);
        // Drain first event.
        let _ = rx.recv().await.unwrap();

        // Second call is a no-op.
        viper.quarantine_node(node, &evidence);
        assert_eq!(viper.quarantined_count(), 1);

        // No second event should be emitted.
        assert!(rx.try_recv().is_err());
    }

    // --- Test 3: is_quarantined works ---

    #[test]
    fn test_is_quarantined() {
        let bus = new_shared_bus();
        let mut viper = Viper::new(default_config(), bus);

        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let evidence = sample_evidence();

        assert!(!viper.is_quarantined(&node_a));
        assert!(!viper.is_quarantined(&node_b));

        viper.quarantine_node(node_a, &evidence);
        assert!(viper.is_quarantined(&node_a));
        assert!(!viper.is_quarantined(&node_b));
    }

    // --- Test 4: multiple nodes ---

    #[test]
    fn test_multiple_nodes_quarantined() {
        let bus = new_shared_bus();
        let mut viper = Viper::new(default_config(), bus);
        let evidence = sample_evidence();

        let nodes: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();
        for node in &nodes {
            viper.quarantine_node(*node, &evidence);
        }

        assert_eq!(viper.quarantined_count(), 5);
        let q = viper.quarantined_nodes();
        for node in &nodes {
            assert!(q.contains(node));
        }
    }

    // --- Test 5: grace period release ---

    #[tokio::test]
    async fn test_grace_period_release() {
        let bus = new_shared_bus();
        let mut viper = Viper::new(
            ViperConfig {
                grace_period: Duration::from_millis(1),
                max_quarantined: 20,
                ..Default::default()
                },
            bus.clone(),
        );

        let node = NodeId::new();
        let evidence = sample_evidence();
        viper.quarantine_node(node, &evidence);
        assert!(viper.is_quarantined(&node));

        // Wait for grace period to expire.
        tokio::time::sleep(Duration::from_millis(10)).await;

        viper.check_grace_period();
        assert!(!viper.is_quarantined(&node));
        assert_eq!(viper.quarantined_count(), 0);
    }

    // --- Test 6: release_node ---

    #[test]
    fn test_release_node() {
        let bus = new_shared_bus();
        let mut viper = Viper::new(default_config(), bus);
        let evidence = sample_evidence();

        let node = NodeId::new();
        viper.quarantine_node(node, &evidence);
        assert!(viper.is_quarantined(&node));

        let released = viper.release_node(node);
        assert!(released);
        assert!(!viper.is_quarantined(&node));

        // Releasing again returns false.
        let released2 = viper.release_node(node);
        assert!(!released2);
    }

    // --- Test 7: max_quarantined eviction ---

    #[tokio::test]
    async fn test_max_quarantined_evicts_oldest() {
        let bus = new_shared_bus();
        let mut viper = Viper::new(
            ViperConfig {
                grace_period: Duration::from_secs(600),
                max_quarantined: 2,
            },
            bus.clone(),
        );
        let evidence = sample_evidence();

        let node_a = NodeId::new();
        let node_b = NodeId::new();
        let node_c = NodeId::new();

        viper.quarantine_node(node_a, &evidence);
        // Small sleep so timestamps are ordered.
        tokio::time::sleep(Duration::from_millis(2)).await;
        viper.quarantine_node(node_b, &evidence);
        assert_eq!(viper.quarantined_count(), 2);

        // Adding a third should evict node_a (the oldest).
        viper.quarantine_node(node_c, &evidence);
        assert_eq!(viper.quarantined_count(), 2);
        assert!(!viper.is_quarantined(&node_a));
        assert!(viper.is_quarantined(&node_b));
        assert!(viper.is_quarantined(&node_c));
    }

    // --- Test 8: quarantine with empty evidence ---

    #[tokio::test]
    async fn test_quarantine_empty_evidence() {
        let bus = new_shared_bus();
        let mut rx = bus.subscribe();
        let mut viper = Viper::new(default_config(), bus.clone());

        let node = NodeId::new();
        let evidence = EvidenceChain::new();
        viper.quarantine_node(node, &evidence);

        let event = rx.recv().await.unwrap();
        match event {
            MarabuntaEvent::NodeQuarantined { reason, .. } => {
                assert!(reason.contains("no detail"));
            }
            other => panic!("expected NodeQuarantined, got {:?}", other),
        }
    }
}
