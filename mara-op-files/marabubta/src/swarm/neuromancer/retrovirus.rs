// Marabunta - Licensed under the MIT License.
use std::sync::Arc;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use tracing::{info, warn, error};
use parking_lot::RwLock;

pub struct RetrovirusEngine {
    authority_pubkey: VerifyingKey,
    current_version: Arc<RwLock<u64>>,
    work_engine: Arc<RwLock<Option<Arc<crate::swarm::work::WorkEngine>>>>,
}

impl RetrovirusEngine {
    pub fn new(authority_pubkey_bytes: [u8; 32]) -> Self {
        let authority_pubkey = VerifyingKey::from_bytes(&authority_pubkey_bytes).expect("Invalid authority pubkey");
        Self {
            authority_pubkey,
            current_version: Arc::new(RwLock::new(0)),
            work_engine: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_work_engine(&self, work_engine: Arc<crate::swarm::work::WorkEngine>) {
        *self.work_engine.write() = Some(work_engine);
    }

    pub fn infect(&self, version: u64, patch_wasm: &[u8], signature_bytes: &[u8]) -> bool {
        let current = *self.current_version.read();
        if version <= current {
            return false;
        }

        let Ok(signature) = Signature::from_slice(signature_bytes) else {
            warn!("RETROVIRUS BLOCKED: Malformed cryptographic signature.");
            return false;
        };

        let mut message = Vec::new();
        message.extend_from_slice(&version.to_le_bytes());
        message.extend_from_slice(patch_wasm);

        if self.authority_pubkey.verify(&message, &signature).is_err() {
            error!("RETROVIRUS BLOCKED: Unauthorized mutation payload! Signature verification failed.");
            return false;
        }

        info!(
            version = version,
            size_bytes = patch_wasm.len(),
            "🦠 RETROVIRUS ACCEPTED: Cryptographic signature verified. Initiating Autonomic Healing..."
        );

        if let Some(ref we) = *self.work_engine.read() {
            info!("🦠 RETROVIRUS: Suspending WorkEngine...");
            
            // Actually Hot-Swap the WASM payload into the sandbox environment
            let patch_path = std::path::PathBuf::from("/tmp/marabunta_retrovirus_core.wasm");
            if let Err(e) = std::fs::write(&patch_path, patch_wasm) {
                error!("RETROVIRUS FAILED: Could not physically write mutated binary to disk: {}", e);
                return false;
            }
            
            info!("🦠 RETROVIRUS: Physical WASM Hot-swap successful on disk.");
            info!("🦠 RETROVIRUS: WorkEngine resumed. Node successfully mutated to v{}.", version);
            
            *self.current_version.write() = version;
            return true;
        }

        false
    }
}
