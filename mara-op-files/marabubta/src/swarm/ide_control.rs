// Marabunta - Licensed under the MIT License.
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{warn, info};

use super::types::NodeId;
use crate::common::types::JobId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IdeAction {
    KillJob { job_id: JobId },
    QuarantineSubnet { target_nodes: Vec<NodeId> },
    OverrideMarketBid { job_id: JobId, new_bid_usd: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingControlCommand {
    pub payload: Vec<u8>,
    pub action: IdeAction,
    pub signatures: Vec<Signature>,
    pub required_signatures: usize,
    pub expires_at: u64,
}

pub struct IdeAuthorizer {
    authorized_keys: Arc<RwLock<HashSet<[u8; 32]>>>,
    pending_commands: Arc<RwLock<HashMap<Vec<u8>, PendingControlCommand>>>,
}

impl Default for IdeAuthorizer {
    fn default() -> Self {
        Self::new()
    }
}

impl IdeAuthorizer {
    pub fn new() -> Self {
        Self {
            authorized_keys: Arc::new(RwLock::new(HashSet::new())),
            pending_commands: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_trusted_key(&self, pubkey_bytes: [u8; 32]) {
        self.authorized_keys.write().await.insert(pubkey_bytes);
    }

    pub async fn process_signature(
        &self,
        issuer_id: &NodeId,
        payload: &[u8],
        signature_bytes: &[u8],
        required_consensus: usize,
    ) -> Result<Option<IdeAction>, &'static str> {
        let keys = self.authorized_keys.read().await;
        let sig = Signature::from_slice(signature_bytes).map_err(|_| "Invalid signature format")?;

        let mut verified = false;
        for pk_bytes in keys.iter() {
            if let Ok(pubkey) = VerifyingKey::from_bytes(pk_bytes) {
                if pubkey.verify(payload, &sig).is_ok() {
                    verified = true;
                    break;
                }
            }
        }

        if !verified {
            warn!(issuer = %issuer_id, "Rejected ControlPlaneCommand signature");
            return Err("Signature verification failed");
        }

        let mut pending = self.pending_commands.write().await;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

        // Expire old commands
        pending.retain(|_, v| v.expires_at > now);

        let entry = pending.entry(payload.to_vec()).or_insert_with(|| {
            PendingControlCommand {
                payload: payload.to_vec(),
                action: bincode::deserialize(payload).unwrap(),
                signatures: Vec::new(),
                required_signatures: required_consensus,
                expires_at: now + 30, // 30 second consensus window
            }
        });

        // Ensure we don't count duplicate signatures from the same key
        if !entry.signatures.iter().any(|s| s.to_bytes() == sig.to_bytes()) {
            entry.signatures.push(sig);
            info!("Recorded valid signature {}/{} for IdeAction", entry.signatures.len(), entry.required_signatures);
        }

        if entry.signatures.len() >= entry.required_signatures {
            let action = entry.action.clone();
            pending.remove(payload);
            info!("Multi-signature consensus reached. Executing ControlPlaneCommand.");
            Ok(Some(action))
        } else {
            Ok(None) // Still pending
        }
    }
}
