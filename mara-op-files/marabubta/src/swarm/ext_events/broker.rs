// Marabunta - Licensed under the MIT License.
//! Pillar 7.2: Ext. Event Bus (Planetary Pub/Sub)
//! 
//! Manages local WASM actor subscriptions and routes incoming pheromones 
//! to sleeping instances. Triggers awakenings across the 15-billion-node swarm.

use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{info, debug};

use super::protocol::{ExtEvent, RetentionPolicy};
use super::bft_ledger::SovereignEventRouter;

type TopicHash = [u8; 32];

pub struct ExtEventBroker {
    /// Maps a Topic Hash to a list of channels connected to sleeping WASM actors.
    subscriptions: Arc<DashMap<TopicHash, Vec<mpsc::Sender<ExtEvent>>>>,
    sovereign_router: Arc<SovereignEventRouter>,
}

impl ExtEventBroker {
    pub fn new(sovereign_router: Arc<SovereignEventRouter>) -> Self {
        Self {
            subscriptions: Arc::new(DashMap::new()),
            sovereign_router,
        }
    }

    /// Called by a WASM Actor (e.g., via the Marabunta SDK) to subscribe to a pheromone frequency.
    pub fn subscribe(&self, topic: &str, tx: mpsc::Sender<ExtEvent>) {
        let mut hasher = blake3::Hasher::new();
        hasher.update(topic.as_bytes());
        let hash: TopicHash = hasher.finalize().into();

        let mut entry = self.subscriptions.entry(hash).or_default();
        entry.push(tx);
        info!("Ext. Event Bus: WASM Actor subscribed to topic hash {:x?}", &hash[0..4]);
    }

    /// Publish an event to the local node AND the global swarm.
    pub async fn publish(&self, event: ExtEvent) {
        match event.retention {
            RetentionPolicy::Ephemeral { ttl_ms } => {
                debug!("Publishing Ephemeral event (TTL: {}ms) to Kademlia Epidemic Gossip", ttl_ms);
                // In production, this injects the event into `src/swarm/gossip.rs` for UDP broadcast.
                self.deliver_locally(event).await;
            }
            RetentionPolicy::Sovereign { .. } => {
                let event_clone = event.clone();
                // Push through the Hashgraph
                if let Err(e) = self.sovereign_router.route_and_seal(event).await {
                    tracing::error!("Sovereign Event Routing Failed: {}", e);
                } else {
                    // Only deliver locally if BFT consensus was mathematically proven
                    self.deliver_locally(event_clone).await;
                }
            }
        }
    }

    /// Wake up any local WASM actors subscribed to this event's topic.
    async fn deliver_locally(&self, event: ExtEvent) {
        if let Some(mut subs) = self.subscriptions.get_mut(&event.topic_hash) {
            let mut dead_channels = Vec::new();
            for (i, tx) in subs.iter().enumerate() {
                if tx.send(event.clone()).await.is_err() {
                    dead_channels.push(i);
                }
            }
            // Clean up disconnected WASM actors
            for i in dead_channels.into_iter().rev() {
                subs.remove(i);
            }
        }
    }
}
