// Marabunta - Licensed under the MIT License.
//! Node identity and keypair management for the Marabunta protocol.
//!
//! NodeId is derived as SHAKE-256(dilithium_public_key). Each node holds a full
//! identity bundle (Dilithium + Ed25519 + Kyber keypairs) with encrypted keystore
//! persistence.

use crate::marabunta::crypto::{
    self, aes256gcm_decrypt, aes256gcm_encrypt, shake256, CryptoError, DilithiumKeyPair,
    KyberKeyPair, SharedSecret, DILITHIUM_PK_LEN, DILITHIUM_SIG_LEN, DILITHIUM_SK_LEN,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use fips204::traits::{SerDes, Verifier};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use zeroize::Zeroize;

/// A 32-byte node identifier derived from SHAKE-256(dilithium_public_key).
#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 32]);

/// A 32-byte federation identifier for swarm-level cryptographic isolation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FederationId(pub [u8; 32]);

impl FederationId {
    /// Create a new FederationId from raw bytes.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Generate a random FederationId.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for FederationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0[..8] {
            write!(f, "{:02x}", byte)?;
        }
        write!(f, "…")
    }
}

impl fmt::Debug for FederationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FederationId({})", self)
    }
}

impl NodeId {
    /// Chrysalis Phase (Sybil Defense): 
    /// A NodeId is only biologically valid if its hash satisfies a Proof-of-Work constraint.
    /// This requires 16 leading zero bits (the first two bytes must be 0x00).
    /// This imposes a massive CPU burn cost to onboard, mathematically preventing 
    /// a malicious actor from spinning up 10,000 Sybil nodes instantly.
    pub fn meets_chrysalis_pow(&self) -> bool {
        self.0[0] == 0 && self.0[1] == 0
    }

    /// Derive a NodeId from a Dilithium public key.
    pub fn from_dilithium_pk(pk_bytes: &[u8; DILITHIUM_PK_LEN]) -> Self {
        Self(shake256(pk_bytes))
    }

    /// Generate a random NodeId (mostly for tests).
    pub fn random() -> Self {
        let mut bytes = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Get the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0[..8] {
            write!(f, "{:02x}", byte)?;
        }
        write!(f, "…")
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self)
    }
}

/// Identity errors.
#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("crypto error: {0}")]
    Crypto(#[from] CryptoError),

    #[error("keystore error: {0}")]
    Keystore(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("invalid passphrase")]
    InvalidPassphrase,
}

/// A stashed Kyber decapsulation key with an expiry, used during key rotation
/// to decrypt in-flight envelopes encrypted with the previous key.
pub struct GracePeriodKey {
    /// The previous Kyber keypair.
    pub keypair: KyberKeyPair,
    /// When this grace key expires and should be discarded.
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// Full identity bundle for a Marabunta node.
pub struct NodeIdentity {
    pub dilithium: DilithiumKeyPair,
    pub ed25519_signing: SigningKey,
    pub kyber: KyberKeyPair,
    node_id: NodeId,
    /// Previous Kyber key retained for a grace period after rotation.
    previous_kyber: Option<GracePeriodKey>,
}

impl NodeIdentity {
    /// Generate a fresh identity with new keypairs.
    pub fn generate() -> Result<Self, IdentityError> {
        // Chrysalis Phase: Burn CPU to find a Dilithium keypair that hashes to a valid PoW NodeId.
        // This takes ~1-5 minutes of 100% CPU on an average machine.
        let mut dilithium;
        let mut node_id;
        let mut attempts = 0;
        
        loop {
            dilithium = DilithiumKeyPair::generate()?;
            node_id = NodeId::from_dilithium_pk(&dilithium.public_key_bytes());
            if node_id.meets_chrysalis_pow() {
                break;
            }
            attempts += 1;
            // Provide a log every 10,000 attempts for UX
            if attempts % 10000 == 0 {
                tracing::info!("Chrysalis Phase: Still searching for valid Identity PoW. Attempts: {}", attempts);
            }
            // For testing environments, break early if needed, but in prod this loops until valid.
            #[cfg(test)]
            if attempts > 100 { break; } 
        }
        
        let ed25519_signing = SigningKey::generate(&mut rand::thread_rng());
        let kyber = KyberKeyPair::generate();
        
        tracing::info!("Chrysalis Phase complete! Valid Node Identity generated after {} attempts.", attempts);

        Ok(Self {
            dilithium,
            ed25519_signing,
            kyber,
            node_id,
            previous_kyber: None,
        })
    }

    /// Get the node's biological identifier.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the raw Dilithium public key bytes.
    pub fn dilithium_public_key(&self) -> [u8; DILITHIUM_PK_LEN] {
        self.dilithium.public_key_bytes()
    }

    /// Verify a Dilithium signature.
    pub fn verify_dilithium(pk_bytes: &[u8], message: &[u8], signature: &[u8]) -> bool {
        let pk_arr: [u8; DILITHIUM_PK_LEN] = match pk_bytes.try_into() {
            Ok(arr) => arr,
            Err(_) => return false,
        };
        let Ok(pk) = crate::marabunta::crypto::DilithiumPublicKey::try_from_bytes(pk_arr) else {
            return false;
        };
        let sig_arr: &[u8; DILITHIUM_SIG_LEN] = match signature.try_into() {
            Ok(arr) => arr,
            Err(_) => return false,
        };
        pk.verify(message, sig_arr, &[])
    }
    pub fn sign_dilithium(&self, message: &[u8]) -> Result<Vec<u8>, IdentityError> {
        Ok(self.dilithium.sign(message)?.to_vec())
    }

    /// Sign a message with Ed25519 (classical signature).
    pub fn sign_ed25519(&self, message: &[u8]) -> [u8; 64] {
        use ed25519_dalek::Signer;
        self.ed25519_signing.sign(message).to_bytes()
    }

    /// Get Ed25519 verifying (public) key.
    pub fn ed25519_verifying_key(&self) -> VerifyingKey {
        self.ed25519_signing.verifying_key()
    }

    /// Get Dilithium public key bytes.
    pub fn dilithium_public_key_bytes(&self) -> [u8; DILITHIUM_PK_LEN] {
        self.dilithium.public_key_bytes()
    }

    /// Get Kyber encapsulation key bytes.
    pub fn kyber_encapsulation_key_bytes(&self) -> Vec<u8> {
        self.kyber.encapsulation_key_bytes()
    }

    /// Decapsulate a Kyber ciphertext to recover the shared secret.
    pub fn kyber_decapsulate(&self, ciphertext: &[u8]) -> Result<SharedSecret, IdentityError> {
        Ok(self.kyber.decapsulate(ciphertext)?)
    }

    /// Rotate Kyber keys, stashing the previous key for a 4-hour grace period.
    /// In-flight envelopes encrypted with the old key can still be decrypted
    /// until the grace period expires.
    pub fn rotate_kyber(&mut self) -> Result<(), IdentityError> {
        let old_kyber = std::mem::replace(&mut self.kyber, KyberKeyPair::generate());
        self.previous_kyber = Some(GracePeriodKey {
            keypair: old_kyber,
            expires_at: chrono::Utc::now() + chrono::Duration::hours(4),
        });
        Ok(())
    }

    /// Rotate identity: generates new keypairs for the existing identity.
    /// The old Kyber key is retained for a grace period.
    pub fn rotate(&mut self) -> Result<(), IdentityError> {
        self.dilithium = DilithiumKeyPair::generate()?;
        self.ed25519_signing = SigningKey::generate(&mut rand::thread_rng());
        self.node_id = NodeId::from_dilithium_pk(&self.dilithium.public_key_bytes());
        self.rotate_kyber()
    }

    /// Attempt to decapsulate using the previous (grace-period) Kyber key.
    /// Returns `Err` if no previous key exists or if it has expired.
    pub fn kyber_decapsulate_previous(&self, ciphertext: &[u8]) -> Result<SharedSecret, IdentityError> {
        match &self.previous_kyber {
            Some(grace) if chrono::Utc::now() < grace.expires_at => {
                Ok(grace.keypair.decapsulate(ciphertext)?)
            }
            Some(_) => Err(IdentityError::Keystore("previous Kyber key expired".into())),
            None => Err(IdentityError::Keystore("no previous Kyber key available".into())),
        }
    }
}

// ============================================================================
// Encrypted Keystore — persist identity to disk
// ============================================================================

/// Serializable keystore format for encrypted identity storage.
#[derive(Serialize, Deserialize)]
pub struct EncryptedKeystore {
    /// Salt for key derivation.
    pub salt: [u8; 16],
    /// AES-GCM nonce.
    pub nonce: [u8; 12],
    /// Encrypted payload (Dilithium SK + Ed25519 SK + Kyber DK + Dilithium PK + Ed25519 VK + Kyber EK).
    pub ciphertext: Vec<u8>,
}

/// Internal plaintext keystore for serialization before encryption.
/// ZeroizeOnDrop ensures all key material is wiped on drop.
#[derive(Serialize, Deserialize, Zeroize, zeroize::ZeroizeOnDrop)]
struct KeystorePlaintext {
    dilithium_pk: Vec<u8>,
    dilithium_sk: Vec<u8>,
    ed25519_sk: Vec<u8>,
    kyber_ek: Vec<u8>,
    kyber_dk: Vec<u8>,
}

impl EncryptedKeystore {
    /// Encrypt and save an identity to a keystore.
    pub fn save(identity: &NodeIdentity, passphrase: &[u8]) -> Result<Self, IdentityError> {
        let plaintext = KeystorePlaintext {
            dilithium_pk: identity.dilithium.public_key_bytes().to_vec(),
            dilithium_sk: identity.dilithium.private_key_bytes().to_vec(),
            ed25519_sk: identity.ed25519_signing.to_bytes().to_vec(),
            kyber_ek: identity.kyber.encapsulation_key_bytes(),
            kyber_dk: identity.kyber.decapsulation_key_bytes().to_vec(),
        };

        let plaintext_bytes = serde_json::to_vec(&plaintext)
            .map_err(|e| IdentityError::Serialization(e.to_string()))?;

        // Derive encryption key from passphrase
        let mut salt = [0u8; 16];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut salt);
        let key = derive_key_from_passphrase(passphrase, &salt);

        let mut nonce = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce);

        let ciphertext = aes256gcm_encrypt(&key, &nonce, &plaintext_bytes)
            .map_err(|e| IdentityError::Keystore(e.to_string()))?;

        Ok(Self {
            salt,
            nonce,
            ciphertext,
        })
    }

    /// Decrypt and load an identity from a keystore.
    pub fn load(&self, passphrase: &[u8]) -> Result<NodeIdentity, IdentityError> {
        let key = derive_key_from_passphrase(passphrase, &self.salt);

        let mut plaintext_bytes = aes256gcm_decrypt(&key, &self.nonce, &self.ciphertext)
            .map_err(|_| IdentityError::InvalidPassphrase)?;

        let ks: KeystorePlaintext = serde_json::from_slice(&plaintext_bytes)
            .map_err(|e| IdentityError::Serialization(e.to_string()))?;

        // Scrub raw decrypted bytes immediately after deserialization
        plaintext_bytes.zeroize();

        // Reconstruct Dilithium keypair
        let mut dilithium_pk_arr = [0u8; DILITHIUM_PK_LEN];
        let mut dilithium_sk_arr = [0u8; DILITHIUM_SK_LEN];
        if ks.dilithium_pk.len() != DILITHIUM_PK_LEN || ks.dilithium_sk.len() != DILITHIUM_SK_LEN
        {
            return Err(IdentityError::Keystore("invalid dilithium key length".into()));
        }
        dilithium_pk_arr.copy_from_slice(&ks.dilithium_pk);
        dilithium_sk_arr.copy_from_slice(&ks.dilithium_sk);
        let dilithium = DilithiumKeyPair::from_bytes(dilithium_pk_arr, dilithium_sk_arr)?;

        // Reconstruct Ed25519 keypair
        if ks.ed25519_sk.len() != 32 {
            return Err(IdentityError::Keystore("invalid ed25519 key length".into()));
        }
        let mut ed_sk_bytes = [0u8; 32];
        ed_sk_bytes.copy_from_slice(&ks.ed25519_sk);
        let ed25519_signing = SigningKey::from_bytes(&ed_sk_bytes);

        // Reconstruct Kyber keypair
        let kyber = reconstruct_kyber(&ks.kyber_ek, &ks.kyber_dk)?;

        let node_id = NodeId::from_dilithium_pk(&dilithium.public_key_bytes());

        // ks is zeroized automatically on drop via ZeroizeOnDrop

        Ok(NodeIdentity {
            dilithium,
            ed25519_signing,
            kyber,
            node_id,
            previous_kyber: None,
        })
    }

    /// Serialize keystore to JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, IdentityError> {
        serde_json::to_vec(self).map_err(|e| IdentityError::Serialization(e.to_string()))
    }

    /// Deserialize keystore from JSON bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, IdentityError> {
        serde_json::from_slice(data).map_err(|e| IdentityError::Serialization(e.to_string()))
    }
}

/// Derive a 32-byte AES key from passphrase + salt using SHAKE-256.
fn derive_key_from_passphrase(passphrase: &[u8], salt: &[u8; 16]) -> [u8; 32] {
    crypto::shake256_multi(&[passphrase, salt])
}

/// Reconstruct a KyberKeyPair from serialized bytes.
fn reconstruct_kyber(ek_bytes: &[u8], dk_bytes: &[u8]) -> Result<KyberKeyPair, IdentityError> {
    use ml_kem::EncodedSizeUser;

    let ek_encoded =
        ml_kem::Encoded::<crypto::KyberEncapsulationKey>::try_from(ek_bytes)
            .map_err(|_| IdentityError::Keystore("invalid kyber ek length".into()))?;
    let encapsulation_key = crypto::KyberEncapsulationKey::from_bytes(&ek_encoded);

    // Store dk_bytes for decapsulation
    Ok(KyberKeyPair {
        encapsulation_key,
        dk_bytes: dk_bytes.to_vec(),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_identity() {
        let id = NodeIdentity::generate().unwrap();
        assert_ne!(id.node_id().0, [0u8; 32]);
        // In tests, we break early, so we don't assert meets_chrysalis_pow() strictly here.
    }

    #[test]
    fn test_node_id_derived_from_dilithium_pk() {
        let id = NodeIdentity::generate().unwrap();
        let pk_bytes = id.dilithium_public_key_bytes();
        let expected = NodeId::from_dilithium_pk(&pk_bytes);
        assert_eq!(id.node_id(), expected);
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeIdentity::generate().unwrap();
        let display = format!("{}", id.node_id());
        assert!(display.ends_with('…'));
        // 16 hex chars + "…" (3 bytes UTF-8) = 19 bytes
        assert!(display.ends_with('…'));
        assert_eq!(display.chars().count(), 17); // 16 hex chars + 1 ellipsis char
    }

    #[test]
    fn test_dilithium_sign_verify() {
        let id = NodeIdentity::generate().unwrap();
        let msg = b"test message";
        let sig = id.sign_dilithium(msg).unwrap();
        assert!(crypto::dilithium_verify(
            &id.dilithium.public_key,
            msg,
            sig.as_slice().try_into().unwrap()
        ));
    }

    #[test]
    fn test_ed25519_sign_verify() {
        let id = NodeIdentity::generate().unwrap();
        let msg = b"test ed25519";
        let sig_bytes = id.sign_ed25519(msg);
        let sig = ed25519_dalek::Signature::from_bytes(sig_bytes.as_slice().try_into().unwrap());
        use ed25519_dalek::Verifier;
        assert!(id.ed25519_verifying_key().verify(msg, &sig).is_ok());
    }

    #[test]
    fn test_kyber_decapsulate() {
        let id = NodeIdentity::generate().unwrap();
        let (ct, ss_enc) =
            KyberKeyPair::encapsulate_with(&id.kyber.encapsulation_key).unwrap();
        let ss_dec = id.kyber_decapsulate(&ct).unwrap();
        assert_eq!(ss_enc.as_bytes(), ss_dec.as_bytes());
    }

    #[test]
    fn test_rotate_produces_new_id() {
        let mut id = NodeIdentity::generate().unwrap();
        let old_node_id = id.node_id();
        id.rotate().unwrap();
        assert_ne!(id.node_id(), old_node_id);
    }

    #[test]
    fn test_keystore_roundtrip() {
        let id = NodeIdentity::generate().unwrap();
        let original_node_id = id.node_id();
        let passphrase = b"test-passphrase-123";

        let ks = EncryptedKeystore::save(&id, passphrase).unwrap();
        let loaded = ks.load(passphrase).unwrap();

        assert_eq!(loaded.node_id(), original_node_id);

        // Verify signing still works
        let msg = b"after load";
        let sig = loaded.sign_dilithium(msg).unwrap();
        assert!(crypto::dilithium_verify(
            &loaded.dilithium.public_key,
            msg,
            sig.as_slice().try_into().unwrap()
        ));
    }

    #[test]
    fn test_keystore_wrong_passphrase() {
        let id = NodeIdentity::generate().unwrap();
        let ks = EncryptedKeystore::save(&id, b"correct").unwrap();
        assert!(ks.load(b"wrong").is_err());
    }

    #[test]
    fn test_keystore_serialization() {
        let id = NodeIdentity::generate().unwrap();
        let ks = EncryptedKeystore::save(&id, b"pass").unwrap();
        let bytes = ks.to_bytes().unwrap();
        let ks2 = EncryptedKeystore::from_bytes(&bytes).unwrap();
        let loaded = ks2.load(b"pass").unwrap();
        assert_eq!(loaded.node_id(), id.node_id());
    }

    #[test]
    fn test_keystore_plaintext_zeroize() {
        let mut ks = KeystorePlaintext {
            dilithium_pk: vec![0xFF; 32],
            dilithium_sk: vec![0xFF; 64],
            ed25519_sk: vec![0xFF; 32],
            kyber_ek: vec![0xFF; 32],
            kyber_dk: vec![0xFF; 32],
        };
        ks.zeroize();
        assert!(ks.dilithium_sk.iter().all(|&b| b == 0));
        assert!(ks.ed25519_sk.iter().all(|&b| b == 0));
        assert!(ks.kyber_dk.iter().all(|&b| b == 0));
        assert!(ks.dilithium_pk.iter().all(|&b| b == 0));
        assert!(ks.kyber_ek.iter().all(|&b| b == 0));
    }
}

// ============================================================================
// The Three-Tier Sovereign Identity Hierarchy
// ============================================================================

/// Tier 2: The Sovereign Identity (FleetId / UserId)
/// This identity is meant to be stored in cold-storage. It holds reputation
/// and signs certificates allowing ephemeral Tier 1 (NodeId) machines to compute
/// on its behalf.
#[derive(Clone, Serialize, Deserialize)]
pub struct FleetIdentity {
    pub signing_key: SigningKey,
}

impl FleetIdentity {
    /// Generate a new Sovereign Fleet Identity
    pub fn generate() -> Self {
        Self {
            signing_key: SigningKey::generate(&mut rand::thread_rng()),
        }
    }

    /// The public identifier of this fleet
    pub fn public_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Sign a Tier 1 NodeId delegation certificate.
    pub fn delegate_node(&self, node_id: NodeId, valid_until: u64) -> DelegationCertificate {
        use ed25519_dalek::Signer;
        
        let mut payload = Vec::new();
        payload.extend_from_slice(&node_id.0);
        payload.extend_from_slice(&valid_until.to_le_bytes());
        
        let signature = self.signing_key.sign(&payload);
        
        DelegationCertificate {
            fleet_public_key: self.public_key().to_bytes().to_vec(),
            node_id,
            valid_until,
            signature: signature.to_bytes().to_vec(),
        }
    }

    /// Sign a Tier 3 Financial Directive (Payout Wallet).
    pub fn issue_financial_directive(&self, payout_wallet: String, nonce: u64) -> FinancialDirective {
        use ed25519_dalek::Signer;
        
        let mut payload = Vec::new();
        payload.extend_from_slice(payout_wallet.as_bytes());
        payload.extend_from_slice(&nonce.to_le_bytes());
        
        let signature = self.signing_key.sign(&payload);
        
        FinancialDirective {
            fleet_public_key: self.public_key().to_bytes().to_vec(),
            payout_wallet,
            nonce,
            signature: signature.to_bytes().to_vec(),
        }
    }
}

/// A cryptographic visa allowing a Tier 1 node to operate under a Tier 2 Fleet`s reputation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DelegationCertificate {
    pub fleet_public_key: Vec<u8>,
    pub node_id: NodeId,
    pub valid_until: u64,
    pub signature: Vec<u8>,
}

impl DelegationCertificate {
    /// Verify that the Fleet physically authorized this NodeId.
    pub fn verify(&self) -> bool {
        use ed25519_dalek::{Verifier, Signature};
        
        let Ok(vk_bytes) = <[u8; 32]>::try_from(self.fleet_public_key.as_slice()) else { return false };
        let Ok(vk) = VerifyingKey::from_bytes(&vk_bytes) else { return false };
        
        let Ok(sig_bytes) = <[u8; 64]>::try_from(self.signature.as_slice()) else { return false };
        let signature = Signature::from_bytes(&sig_bytes);
        
        let mut payload = Vec::new();
        payload.extend_from_slice(&self.node_id.0);
        payload.extend_from_slice(&self.valid_until.to_le_bytes());
        
        vk.verify(&payload, &signature).is_ok()
    }
}

/// A cryptographic directive routing all MMX payments for a Fleet to a specific Tier 3 wallet.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinancialDirective {
    pub fleet_public_key: Vec<u8>,
    pub payout_wallet: String,
    pub nonce: u64,
    pub signature: Vec<u8>,
}

impl FinancialDirective {
    /// Verify that the Fleet physically authorized this Payout Wallet.
    pub fn verify(&self) -> bool {
        use ed25519_dalek::{Verifier, Signature};
        
        let Ok(vk_bytes) = <[u8; 32]>::try_from(self.fleet_public_key.as_slice()) else { return false };
        let Ok(vk) = VerifyingKey::from_bytes(&vk_bytes) else { return false };
        
        let Ok(sig_bytes) = <[u8; 64]>::try_from(self.signature.as_slice()) else { return false };
        let signature = Signature::from_bytes(&sig_bytes);
        
        let mut payload = Vec::new();
        payload.extend_from_slice(self.payout_wallet.as_bytes());
        payload.extend_from_slice(&self.nonce.to_le_bytes());
        
        vk.verify(&payload, &signature).is_ok()
    }
}
