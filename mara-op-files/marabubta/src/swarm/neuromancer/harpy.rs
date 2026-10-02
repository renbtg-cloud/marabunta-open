// Marabunta - Licensed under the MIT License.
use tracing::{info, warn, error};
use crate::swarm::types::NodeId;
use ed25519_dalek::{Signer, SigningKey};
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use std::sync::Arc;

/// A cryptographic proof of statistical data poisoning.
/// This JSON structure is submitted to the decentralized ledger or 
/// Kademlia Sub-Swarm to trigger collateral forfeiture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashingWitness {
    pub target_node: String,
    pub violation_type: String,
    pub evidence: serde_json::Value,
    pub harpy_signature: String,
}

/// The Harpy Eagle (Apex Supervisor & Statistical Slashing).
/// Sits above the Swarm (Aggregator layer) and watches for coordinated data poisoning.
/// Utilizes a high-performance, concurrent DashMap to track exponential moving averages
/// of Cosine Similarity across millions of incoming gradient payloads.
#[derive(Clone)]
pub struct HarpyEagle {
    /// The private key of the Harpy Authority (to sign kill-orders)
    authority_key: Arc<SigningKey>,
    /// Tracks the historical cosine similarity of nodes' submissions. (O(1) concurrent access)
    reputation_matrix: Arc<DashMap<NodeId, f64>>,
    /// The mathematical threshold below which a node is classified as an active poisoner.
    slash_threshold: f64,
    /// The decay factor for the Exponential Moving Average (rho).
    decay_factor: f64,
}

impl HarpyEagle {
    pub fn new(authority_key: SigningKey) -> Self {
        Self {
            authority_key: Arc::new(authority_key),
            reputation_matrix: Arc::new(DashMap::new()),
            slash_threshold: -0.5, // High negative cosine similarity indicates deliberate poisoning
            decay_factor: 0.2,     // Recent behavior is weighted at 20%
        }
    }

    /// Evaluates a batch of DiLoCo pseudo-gradients.
    /// Returns a list of signed `HarpyStrike` payloads for any compromised nodes.
    pub fn inspect_canopy(&self, updates: &[(NodeId, f64)]) -> Vec<crate::swarm::types::SwarmMessage> {
        let mut strikes = Vec::new();

        for (node, alignment_score) in updates {
            let mut current_score_ref = self.reputation_matrix.entry(*node).or_insert(1.0);
            
            // O(1) Exponential moving average of alignment
            *current_score_ref = (*current_score_ref * (1.0 - self.decay_factor)) + (alignment_score * self.decay_factor);

            if *current_score_ref < self.slash_threshold {
                let current_score = *current_score_ref;
                let reason = format!("Statistical data poisoning detected. EMA Cosine Similarity: {:.3}", current_score);
                
                // Generate the formal Slashing Witness for the ledger
                let witness = SlashingWitness {
                    target_node: node.to_string(),
                    violation_type: "STATISTICAL_POISONING".to_string(),
                    evidence: serde_json::json!({
                        "moving_average_alignment": current_score,
                        "latest_alignment": alignment_score,
                        "threshold": self.slash_threshold
                    }),
                    harpy_signature: String::new(), // Populated below
                };
                
                // Construct the binary payload to sign
                let mut payload = Vec::new();
                payload.extend_from_slice(node.0.as_bytes()); // NodeId is a Uuid wrapper, .0 is the Uuid, as_bytes gives [u8; 16]
                payload.extend_from_slice(reason.as_bytes());
                
                // Sign the kill order
                let signature = self.authority_key.sign(&payload).to_bytes().to_vec();

                let mut signed_witness = witness;
                signed_witness.harpy_signature = hex::encode(&signature);
                
                // We log the formal witness generation. In production, this JSON is broadcast to the smart contract.
                info!(
                    witness = %serde_json::to_string(&signed_witness).unwrap(),
                    "🦅 HARPY: Formal Slashing Witness generated."
                );

                strikes.push(crate::swarm::types::SwarmMessage::HarpyStrike {
                    target: *node,
                    reason,
                    signature,
                    from: NodeId::new(), // The Harpy doesn't have a standard Kademlia ID
                });

                // Reset the score so we don't spam the network with duplicate strikes
                *current_score_ref = 1.0; 
            }
        }

        if !strikes.is_empty() {
            error!("🦅 HARPY STRIKE INITIATED: Coordinated Data Poisoning Detected.");
            info!("🦅 HARPY: Canopy cleared. Swarm momentum preserved.");
        }

        strikes
    }
}
