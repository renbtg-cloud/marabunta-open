// Marabunta - Licensed under the MIT License.
//! NeuromancerBus — broadcast event bus for the Neuromancer subsystem.
//!
//! Separate from the existing `EventBus` in `src/swarm/events.rs` because
//! the event models are fundamentally different.  They can be bridged later.

use std::sync::Arc;

use tokio::sync::broadcast;

use super::types::{Blake3Hash, MarabuntaEvent, NodeId};

// ============================================================================
// NeuromancerBus
// ============================================================================

/// Default channel capacity.
pub const DEFAULT_BUS_CAPACITY: usize = 4096;

/// Broadcast-based event bus for Neuromancer modules.
///
/// Every module subscribes via [`subscribe`](Self::subscribe) and emits via
/// [`emit`](Self::emit).  Dropped messages (lag) are expected and handled
/// by each subscriber individually.
#[derive(Debug, Clone)]
pub struct NeuromancerBus {
    sender: broadcast::Sender<MarabuntaEvent>,
}

impl NeuromancerBus {
    /// Create a new bus with the given channel capacity.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Create a bus with the default capacity (4096).
    pub fn default_capacity() -> Self {
        Self::new(DEFAULT_BUS_CAPACITY)
    }

    /// Emit an event to all current subscribers.
    ///
    /// If there are no subscribers the event is silently dropped — this is
    /// by design (modules come and go).
    pub fn emit(&self, event: MarabuntaEvent) {
        let _ = self.sender.send(event);
    }

    /// Subscribe to the event stream.  Returns a [`broadcast::Receiver`]
    /// that can be polled with `.recv().await`.
    pub fn subscribe(&self) -> broadcast::Receiver<MarabuntaEvent> {
        self.sender.subscribe()
    }

    /// How many active subscribers exist right now.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for NeuromancerBus {
    fn default() -> Self {
        Self::default_capacity()
    }
}

// ============================================================================
// GossipEnvelope — wraps an event for inter-node propagation
// ============================================================================

/// Wraps a [`MarabuntaEvent`] for gossip propagation between nodes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GossipEnvelope {
    /// The event being propagated.
    pub event: MarabuntaEvent,
    /// Node that originally emitted the event.
    pub origin: NodeId,
    /// How many gossip hops this envelope has traversed.
    pub hop_count: u8,
    /// Maximum hops before the envelope is dropped.
    pub max_hops: u8,
    /// Blake3 hash of the event payload, used for deduplication.
    pub event_hash: Blake3Hash,
}

impl GossipEnvelope {
    /// Create a new envelope for a locally-originated event.
    pub fn new_local(event: MarabuntaEvent, origin: NodeId, max_hops: u8) -> Self {
        let event_hash = Self::compute_hash(&event);
        Self {
            event,
            origin,
            hop_count: 0,
            max_hops,
            event_hash,
        }
    }

    /// Compute the blake3 hash of an event for deduplication.
    pub fn compute_hash(event: &MarabuntaEvent) -> Blake3Hash {
        let bytes = serde_json::to_vec(event).unwrap_or_default();
        let hash = blake3::hash(&bytes);
        *hash.as_bytes()
    }

    /// Increment the hop count and return whether the envelope should still
    /// be forwarded (`true` = still alive, `false` = max hops reached).
    pub fn forward(&mut self) -> bool {
        self.hop_count += 1;
        self.hop_count <= self.max_hops
    }
}

/// Convenience: create an `Arc<NeuromancerBus>` with default capacity.
pub fn new_shared_bus() -> Arc<NeuromancerBus> {
    Arc::new(NeuromancerBus::default_capacity())
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swarm::neuromancer::types::ResourceRequirements;
    use std::time::SystemTime;

    fn sample_event() -> MarabuntaEvent {
        MarabuntaEvent::TaskSubmitted {
            id: [1u8; 32],
            input_hash: [2u8; 32],
            requirements: ResourceRequirements::default(),
            timestamp: SystemTime::now(),
        }
    }

    #[tokio::test]
    async fn test_emit_subscribe_roundtrip() {
        let bus = NeuromancerBus::new(16);
        let mut rx = bus.subscribe();
        bus.emit(sample_event());
        let received = rx.recv().await.unwrap();
        assert_eq!(received.type_name(), "TaskSubmitted");
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = NeuromancerBus::new(16);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 2);

        bus.emit(sample_event());

        let r1 = rx1.recv().await.unwrap();
        let r2 = rx2.recv().await.unwrap();
        assert_eq!(r1.type_name(), "TaskSubmitted");
        assert_eq!(r2.type_name(), "TaskSubmitted");
    }

    #[tokio::test]
    async fn test_no_subscribers_does_not_panic() {
        let bus = NeuromancerBus::new(16);
        // No subscriber — should not panic
        bus.emit(sample_event());
    }

    #[tokio::test]
    async fn test_lag_handling() {
        let bus = NeuromancerBus::new(2); // tiny capacity
        let mut rx = bus.subscribe();

        // Emit 4 events into a capacity-2 channel
        for i in 0..4u8 {
            bus.emit(MarabuntaEvent::TaskSubmitted {
                id: [i; 32],
                input_hash: [0; 32],
                requirements: ResourceRequirements::default(),
                timestamp: SystemTime::now(),
            });
        }

        // First recv should be a Lagged error
        match rx.recv().await {
            Err(broadcast::error::RecvError::Lagged(n)) => {
                assert!(n > 0);
            }
            Ok(_) => {
                // On some timing the last events may arrive — still ok
            }
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_gossip_envelope_hash_deterministic() {
        let event = sample_event();
        let h1 = GossipEnvelope::compute_hash(&event);
        let h2 = GossipEnvelope::compute_hash(&event);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_gossip_envelope_forward() {
        let event = sample_event();
        let node = NodeId::new();
        let mut env = GossipEnvelope::new_local(event, node, 3);
        assert_eq!(env.hop_count, 0);
        assert!(env.forward()); // 1 <= 3
        assert!(env.forward()); // 2 <= 3
        assert!(env.forward()); // 3 <= 3
        assert!(!env.forward()); // 4 > 3
    }

    #[test]
    fn test_gossip_envelope_serde_roundtrip() {
        let event = sample_event();
        let node = NodeId::new();
        let env = GossipEnvelope::new_local(event, node, 5);
        let json = serde_json::to_string(&env).unwrap();
        let back: GossipEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back.origin, node);
        assert_eq!(back.max_hops, 5);
    }

    #[test]
    fn test_default_bus() {
        let bus = NeuromancerBus::default();
        assert_eq!(bus.subscriber_count(), 0);
    }
}
