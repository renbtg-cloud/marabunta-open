// Marabunta - Licensed under the MIT License.
use std::sync::Arc;
use tracing::{info, warn, error};
use crate::swarm::types::{NodeId, SwarmMessage};
use ed25519_dalek::{Verifier, VerifyingKey, SigningKey, Signer};
use sha2::{Digest, Sha256};

/// The Cordyceps Protocol (Parasitic Suicide & Forensic Confession).
/// When a node is struck by the Harpy Eagle (slashed for poisoning data),
/// this protocol takes over. It halts execution, dumps the RAM state to a 
/// forensic hash, signs its own death warrant, and broadcasts it to the network.
pub struct CordycepsProtocol {
    local_id: NodeId,
    identity_key: Arc<SigningKey>,
    harpy_pubkey: VerifyingKey,
}

impl CordycepsProtocol {
    pub fn new(local_id: NodeId, identity_key: SigningKey, harpy_pubkey_bytes: [u8; 32]) -> Self {
        let harpy_pubkey = VerifyingKey::from_bytes(&harpy_pubkey_bytes).expect("Invalid Harpy pubkey");
        Self {
            local_id,
            identity_key: Arc::new(identity_key),
            harpy_pubkey,
        }
    }

    /// Receives a HarpyStrike. If valid, executes the suicide protocol.
    /// Returns true if the strike was valid and the node is dying.
    pub fn handle_strike(&self, target: NodeId, reason: &str, signature: &[u8]) -> Option<SwarmMessage> {
        if target != self.local_id {
            return None;
        }

        // 1. Verify the Strike is actually from the Harpy Authority
        let mut payload = Vec::new();
        payload.extend_from_slice(target.0.as_bytes());
        payload.extend_from_slice(reason.as_bytes());

        if let Ok(sig) = ed25519_dalek::Signature::from_slice(signature) {
            if self.harpy_pubkey.verify(&payload, &sig).is_err() {
                warn!("🍄 CORDYCEPS: Ignored invalid Harpy Strike signature.");
                return None;
            }
        } else {
            return None;
        }

        error!("🍄 CORDYCEPS PROTOCOL ACTIVATED: Harpy Strike verified. Reason: {}", reason);
        error!("🍄 CORDYCEPS: Freezing WASM memory state...");

        // 2. Generate the forensic memory dump hash (The Confession)
        // In production, this hashes the actual Mantis Journal and live RAM.
        let simulated_ram_dump = b"simulated_corrupted_ram_state";
        let mut hasher = Sha256::new();
        hasher.update(simulated_ram_dump);
        let memory_dump_hash: [u8; 32] = hasher.finalize().into();

        // 3. Sign the Confession with the dying node's own key
        let mut confession_payload = Vec::new();
        confession_payload.extend_from_slice(reason.as_bytes());
        confession_payload.extend_from_slice(&memory_dump_hash);
        let self_signature = self.identity_key.sign(&confession_payload).to_bytes().to_vec();

        info!("🍄 CORDYCEPS: Emitting cryptographically signed death confession to Swarm.");

        // 4. Return the Confession message to be broadcast to the network
        Some(SwarmMessage::CordycepsConfession {
            reason: reason.to_string(),
            memory_dump_hash,
            signature: self_signature,
            from: self.local_id,
        })
    }
}
