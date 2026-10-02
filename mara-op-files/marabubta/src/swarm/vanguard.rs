// Marabunta - Licensed under the MIT License.
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};

use crate::swarm::types::{ChunkId, NodeId, SwarmMessage};
use crate::swarm::transport::SwarmTransport;
use crate::swarm::neuromancer::bus::NeuromancerBus;
use crate::swarm::neuromancer::types::{MarabuntaEvent, EvidenceChain, EvidenceItem};

/// A trusted node that injects trapdoor jobs into the Swarm to catch lying workers.
pub struct VanguardNode {
    self_id: NodeId,
    transport: Arc<SwarmTransport>,
    nm_bus: Arc<NeuromancerBus>,
    inbound_rx: mpsc::Receiver<(NodeId, SwarmMessage)>,
    
    // Tracks injected trapdoors: ChunkId -> (Expected Fuel, Expected Output Hash)
    trapdoors: HashMap<ChunkId, (u64, String)>,
}

impl VanguardNode {
    pub fn new(
        self_id: NodeId,
        transport: Arc<SwarmTransport>,
        nm_bus: Arc<NeuromancerBus>,
        inbound_rx: mpsc::Receiver<(NodeId, SwarmMessage)>,
    ) -> Self {
        Self {
            self_id,
            transport,
            nm_bus,
            inbound_rx,
            trapdoors: HashMap::new(),
        }
    }

    /// Run the Vanguard validation loop.
    pub async fn run(mut self) {
        info!("Vanguard Node started. Hunting for forged fuel metrics.");
        
        while let Some((_sender_id, message)) = self.inbound_rx.recv().await {
            if let SwarmMessage::ChunkResult { chunk_id, result, from, .. } = message {
                self.validate_chunk(from, chunk_id, result.success, result.fuel_consumed, result.output);
            }
        }
    }

    /// Validate an incoming chunk result.
    fn validate_chunk(&mut self, worker_id: NodeId, chunk_id: ChunkId, success: bool, reported_fuel: u64, reported_output: Vec<u8>) {
        if let Some((expected_fuel, expected_hash)) = self.trapdoors.remove(&chunk_id) {
            info!(%worker_id, %chunk_id, "Intercepted trapdoor result from worker");

            // Calculate output hash to compare
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&reported_output);
            let reported_hash = format!("{:x}", hasher.finalize());

            if reported_fuel != expected_fuel || reported_hash != expected_hash || !success {
                error!(
                    %worker_id, 
                    expected_fuel, reported_fuel,
                    "VANGUARD TRIGGER: Worker forged fuel metrics or output!"
                );

                let mut evidence = EvidenceChain::new();
                evidence.push(EvidenceItem {
                    event_type: "VanguardTrapdoorFailed".into(),
                    details: format!("Expected fuel {}, got {}", expected_fuel, reported_fuel),
                    confidence: 1.0,
                    timestamp: std::time::SystemTime::now(),
                });

                // Emit lethal threat directly to the Predator (BabyOrca)
                self.nm_bus.emit(MarabuntaEvent::ThreatConfirmed {
                    node: worker_id,
                    evidence,
                    timestamp: std::time::SystemTime::now(),
                });
            } else {
                info!(%worker_id, "Worker successfully passed Vanguard trapdoor. Fuel metrics are honest.");
            }
        }
    }

    /// Inject a trapdoor job into the Gossip network.
    pub fn inject_trapdoor_job(&mut self, chunk_id: ChunkId, expected_fuel: u64, expected_output_hash: String) {
        self.trapdoors.insert(chunk_id, (expected_fuel, expected_output_hash));
        info!(%chunk_id, expected_fuel, "Injected Vanguard Trapdoor into Swarm");
        // Normally we would construct a TaskPayload and gossip it via SwarmTransport here.
    }
}
