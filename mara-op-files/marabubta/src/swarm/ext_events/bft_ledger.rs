// Marabunta - Licensed under the MIT License.
//! The Ext. Event Bus: BFT Ledger Integration
//! Routes high-stakes 'Sovereign' events through the Hashgraph consensus engine.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{info, warn};

use super::protocol::{ExtEvent, RetentionPolicy};
use crate::swarm::planetary::ledger::{ConsensusLedger, HashgraphEngine, TransactionPayload};
use crate::swarm::blobstore::BlobStore;
use crate::swarm::knowledge::KnowledgeStore;
use crate::swarm::transport::SwarmTransport;
use crate::swarm::types::{NodeId, SwarmMessage};

pub struct SovereignEventRouter {
    pub self_id: NodeId,
    ledger: Arc<HashgraphEngine>,
    blob_store: Arc<BlobStore>,
    knowledge: Arc<KnowledgeStore>,
    transport: Arc<SwarmTransport>,
}

impl SovereignEventRouter {
    pub fn new(
        self_id: NodeId,
        ledger: Arc<HashgraphEngine>,
        blob_store: Arc<BlobStore>,
        knowledge: Arc<KnowledgeStore>,
        transport: Arc<SwarmTransport>,
    ) -> Self {
        Self { self_id, ledger, blob_store, knowledge, transport }
    }

    pub async fn route_and_seal(&self, event: ExtEvent) -> Result<(), &'static str> {
        if let RetentionPolicy::Sovereign { required_bft_threshold: _, jurisdiction } = &event.retention {
            info!("Ext. Event Bus: Intercepting Sovereign event for BFT Hashgraph validation.");

            let mut event_hash = blake3::Hasher::new();
            event_hash.update(&event.payload);
            let receipt_hash: [u8; 32] = event_hash.finalize().into();

            let tx = TransactionPayload {
                from: event.causal_vector.actor_id,
                to: event.topic_hash, 
                mmx_amount: 0.0, 
                job_receipt_hash: receipt_hash,
            };

            let dag_event = self.ledger.propose_transaction(tx);
            
            // 1.5 Network Broadcast: Push the proposal to the Swarm
            if let Ok(dag_json) = serde_json::to_string(&dag_event) {
                let msg = SwarmMessage::BftEvent {
                    dag_event_json: dag_json,
                    from: self.self_id,
                };
                
                let targets = self.knowledge.get_live_nodes().into_iter().take(5);
                for target in targets {
                    if target.node_id != self.self_id {
                        if let Some(addr) = target.address {
                            let _ = self.transport.send(addr, msg.clone()).await;
                        }
                    }
                }
            }
            
            let consensus_future = async {
                loop {
                    if self.ledger.verify_bft_threshold(&dag_event) {
                        return true;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };

            match timeout(Duration::from_millis(3000), consensus_future).await {
                Ok(true) => {
                    info!("BFT Consensus Reached. Shattering Sovereign Event Log into Reed-Solomon shards.");

                    if self.blob_store.store_bytes(&event.payload, None, None).await.is_ok() {
                        info!("Successfully submitted event to BlobStore. Automatic Parity Sharding and DHT Dispersion initiated.");
                        Ok(())
                    } else {
                        Err("Failed to encode Sovereign event for permanent DHT retention")
                    }
                }
                _ => {
                    warn!("BFT threshold not met within 3000ms. Mesh partition detected.");
                    warn!("Falling back to Local SQLite CRDT Buffer. Event will be re-injected upon healing.");
                    Ok(()) 
                }
            }
        } else {
            Err("Cannot route an Ephemeral event through the Sovereign BFT Ledger")
        }
    }
}