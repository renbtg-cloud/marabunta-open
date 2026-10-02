// Marabunta - Licensed under the MIT License.
//! Post-quantum cryptographic primitives for the Marabunta protocol.
//!
//! Provides ML-KEM-768 (FIPS 203) key encapsulation, ML-DSA-65 (FIPS 204) digital
//! signatures, hybrid classical+PQ modes, AES-256-GCM symmetric encryption, and
//! SHAKE-256 hashing.

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use fips204::ml_dsa_65;
use fips204::traits::{KeyGen, SerDes, Signer, Verifier};
use hkdf::Hkdf;
use kem::{Decapsulate, Encapsulate};
use ml_kem::kem::{DecapsulationKey, EncapsulationKey};
use ml_kem::{Encoded, EncodedSizeUser, KemCore, MlKem768, MlKem768Params};
use sha3::{
    digest::{ExtendableOutput, Update, XofReader},
    Shake256,
};
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

// Re-export for use by other marabunta modules
pub use fips204::ml_dsa_65::{PublicKey as DilithiumPublicKey, PrivateKey as DilithiumPrivateKey};

/// Size of a Dilithium signature in bytes.
pub const DILITHIUM_SIG_LEN: usize = ml_dsa_65::SIG_LEN;

/// Size of a Dilithium public key in bytes.
pub const DILITHIUM_PK_LEN: usize = ml_dsa_65::PK_LEN;

/// Size of a Dilithium private key in bytes.
pub const DILITHIUM_SK_LEN: usize = ml_dsa_65::SK_LEN;

/// AES-256-GCM nonce size in bytes.
pub const AES_NONCE_LEN: usize = 12;

/// AES-256-GCM key size in bytes.
pub const AES_KEY_LEN: usize = 32;

/// Shared secret size from KEM operations.
pub const SHARED_SECRET_LEN: usize = 32;

/// Cryptographic errors.
#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("key generation failed: {0}")]
    KeyGenFailed(String),

    #[error("signing failed: {0}")]
    SigningFailed(String),

    #[error("verification failed")]
    VerificationFailed,

    #[error("encapsulation failed")]
    EncapsulationFailed,

    #[error("decapsulation failed")]
    DecapsulationFailed,

    #[error("encryption failed: {0}")]
    EncryptionFailed(String),

    #[error("decryption failed: {0}")]
    DecryptionFailed(String),

    #[error("invalid key bytes")]
    InvalidKeyBytes,
}

// ============================================================================
// Dilithium (ML-DSA-65) — Digital Signatures
// ============================================================================

/// Dilithium keypair wrapper with zeroize-on-drop for the private key.
pub struct DilithiumKeyPair {
    pub public_key: ml_dsa_65::PublicKey,
    sk_bytes: [u8; DILITHIUM_SK_LEN],
}

impl Drop for DilithiumKeyPair {
    fn drop(&mut self) {
        self.sk_bytes.zeroize();
    }
}

impl DilithiumKeyPair {
    /// Generate a new Dilithium keypair.
    pub fn generate() -> Result<Self, CryptoError> {
        let (pk, sk) =
            ml_dsa_65::KG::try_keygen().map_err(|e| CryptoError::KeyGenFailed(e.to_string()))?;
        let sk_bytes = sk.into_bytes();
        Ok(Self {
            public_key: pk,
            sk_bytes,
        })
    }

    /// Sign a message. Context is set to empty (protocol-level domain separation).
    pub fn sign(&self, message: &[u8]) -> Result<[u8; DILITHIUM_SIG_LEN], CryptoError> {
        let sk = ml_dsa_65::PrivateKey::try_from_bytes(self.sk_bytes)
            .map_err(|e| CryptoError::SigningFailed(e.to_string()))?;
        sk.try_sign(message, &[])
            .map_err(|e| CryptoError::SigningFailed(e.to_string()))
    }

    /// Get public key bytes.
    pub fn public_key_bytes(&self) -> [u8; DILITHIUM_PK_LEN] {
        self.public_key.clone().into_bytes()
    }

    /// Get private key bytes (caller must handle securely).
    pub fn private_key_bytes(&self) -> &[u8; DILITHIUM_SK_LEN] {
        &self.sk_bytes
    }

    /// Reconstruct from raw bytes.
    pub fn from_bytes(
        pk_bytes: [u8; DILITHIUM_PK_LEN],
        sk_bytes: [u8; DILITHIUM_SK_LEN],
    ) -> Result<Self, CryptoError> {
        let public_key =
            ml_dsa_65::PublicKey::try_from_bytes(pk_bytes).map_err(|_| CryptoError::InvalidKeyBytes)?;
        Ok(Self {
            public_key,
            sk_bytes,
        })
    }
}

/// Verify a Dilithium signature against a public key.
pub fn dilithium_verify(
    pk: &ml_dsa_65::PublicKey,
    message: &[u8],
    signature: &[u8; DILITHIUM_SIG_LEN],
) -> bool {
    pk.verify(message, signature, &[])
}

/// Verify a Dilithium signature from raw public key bytes.
pub fn dilithium_verify_bytes(
    pk_bytes: &[u8; DILITHIUM_PK_LEN],
    message: &[u8],
    signature: &[u8; DILITHIUM_SIG_LEN],
) -> Result<bool, CryptoError> {
    let pk =
        ml_dsa_65::PublicKey::try_from_bytes(*pk_bytes).map_err(|_| CryptoError::InvalidKeyBytes)?;
    Ok(pk.verify(message, signature, &[]))
}

// ============================================================================
// Kyber (ML-KEM-768) — Key Encapsulation
// ============================================================================

/// Kyber encapsulation key (public).
pub type KyberEncapsulationKey = EncapsulationKey<MlKem768Params>;

/// Kyber decapsulation key (private).
pub type KyberDecapsulationKey = DecapsulationKey<MlKem768Params>;

/// Kyber keypair with decapsulation key bytes for secure storage.
pub struct KyberKeyPair {
    pub encapsulation_key: KyberEncapsulationKey,
    pub(crate) dk_bytes: Vec<u8>,
}

impl Drop for KyberKeyPair {
    fn drop(&mut self) {
        self.dk_bytes.zeroize();
    }
}

impl KyberKeyPair {
    /// Generate a new Kyber keypair.
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        let (dk, ek) = MlKem768::generate(&mut rng);
        let dk_bytes = dk.as_bytes().to_vec();
        Self {
            encapsulation_key: ek,
            dk_bytes,
        }
    }

    /// Encapsulate: produce (ciphertext, shared_secret) using the encapsulation key.
    pub fn encapsulate_with(
        ek: &KyberEncapsulationKey,
    ) -> Result<(Vec<u8>, SharedSecret), CryptoError> {
        let mut rng = rand::thread_rng();
        let (ct, ss): (ml_kem::Ciphertext<MlKem768>, ml_kem::SharedKey<MlKem768>) = ek
            .encapsulate(&mut rng)
            .map_err(|_| CryptoError::EncapsulationFailed)?;
        let mut secret = [0u8; 32];
        secret.copy_from_slice(ss.as_slice());
        Ok((ct.as_slice().to_vec(), SharedSecret(secret)))
    }

    /// Decapsulate: recover shared_secret from ciphertext using the decapsulation key.
    pub fn decapsulate(&self, ciphertext: &[u8]) -> Result<SharedSecret, CryptoError> {
        let dk_encoded: Encoded<KyberDecapsulationKey> =
            Encoded::<KyberDecapsulationKey>::try_from(self.dk_bytes.as_slice())
                .map_err(|_| CryptoError::InvalidKeyBytes)?;
        let dk = KyberDecapsulationKey::from_bytes(&dk_encoded);

        // Build ciphertext array from slice
        let ct_encoded: ml_kem::Ciphertext<MlKem768> =
            ml_kem::Ciphertext::<MlKem768>::try_from(ciphertext)
                .map_err(|_| CryptoError::DecapsulationFailed)?;
        let ss: ml_kem::SharedKey<MlKem768> = dk
            .decapsulate(&ct_encoded)
            .map_err(|_| CryptoError::DecapsulationFailed)?;
        let mut secret = [0u8; 32];
        secret.copy_from_slice(ss.as_slice());
        Ok(SharedSecret(secret))
    }

    /// Get encapsulation key bytes.
    pub fn encapsulation_key_bytes(&self) -> Vec<u8> {
        self.encapsulation_key.as_bytes().to_vec()
    }

    /// Get decapsulation key bytes (caller must handle securely).
    pub fn decapsulation_key_bytes(&self) -> &[u8] {
        &self.dk_bytes
    }
}

/// Reconstruct a Kyber encapsulation key from raw bytes.
pub fn kyber_ek_from_bytes(bytes: &[u8]) -> Result<KyberEncapsulationKey, CryptoError> {
    let encoded: Encoded<KyberEncapsulationKey> =
        Encoded::<KyberEncapsulationKey>::try_from(bytes)
            .map_err(|_| CryptoError::InvalidKeyBytes)?;
    Ok(KyberEncapsulationKey::from_bytes(&encoded))
}

/// A 32-byte shared secret with zeroize on drop.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SharedSecret(pub [u8; SHARED_SECRET_LEN]);

impl SharedSecret {
    pub fn as_bytes(&self) -> &[u8; SHARED_SECRET_LEN] {
        &self.0
    }
}

// ============================================================================
// Hybrid KEM — X25519 + Kyber768
// ============================================================================

/// Result of a hybrid key exchange combining classical (X25519) and PQ (Kyber) shared secrets.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct HybridKemResult {
    pub combined_secret: [u8; 32],
}

/// Derive a hybrid shared secret by combining classical and PQ shared secrets via HKDF.
pub fn hybrid_kem_combine(
    classical_ss: &[u8; 32],
    pq_ss: &[u8; 32],
) -> HybridKemResult {
    let mut ikm = [0u8; 64];
    ikm[..32].copy_from_slice(classical_ss);
    ikm[32..].copy_from_slice(pq_ss);

    let hk = Hkdf::<sha2::Sha256>::new(Some(b"marabunta-hybrid-kem"), &ikm);
    let mut combined = [0u8; 32];
    hk.expand(b"hybrid-shared-secret", &mut combined)
        .expect("HKDF expand for 32 bytes should never fail");

    // Zeroize input key material
    let mut ikm_z = ikm;
    ikm_z.zeroize();

    HybridKemResult {
        combined_secret: combined,
    }
}

// ============================================================================
// X25519 classical key exchange helpers
// ============================================================================

/// Generate an X25519 keypair (ephemeral secret, public key bytes).
pub fn x25519_keypair() -> (x25519_dalek::StaticSecret, x25519_dalek::PublicKey) {
    let secret = x25519_dalek::StaticSecret::random_from_rng(rand::thread_rng());
    let public = x25519_dalek::PublicKey::from(&secret);
    (secret, public)
}

/// Perform X25519 Diffie-Hellman.
pub fn x25519_diffie_hellman(
    my_secret: &x25519_dalek::StaticSecret,
    their_public: &x25519_dalek::PublicKey,
) -> [u8; 32] {
    my_secret.diffie_hellman(their_public).to_bytes()
}

// ============================================================================
// AES-256-GCM symmetric encryption
// ============================================================================

/// Encrypt data with AES-256-GCM.
pub fn aes256gcm_encrypt(
    key: &[u8; AES_KEY_LEN],
    nonce: &[u8; AES_NONCE_LEN],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new(key.into());
    let nonce = Nonce::from_slice(nonce);
    cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))
}

/// Decrypt data with AES-256-GCM.
pub fn aes256gcm_decrypt(
    key: &[u8; AES_KEY_LEN],
    nonce: &[u8; AES_NONCE_LEN],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new(key.into());
    let nonce = Nonce::from_slice(nonce);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| CryptoError::DecryptionFailed(e.to_string()))
}

// ============================================================================
// SHAKE-256 hash
// ============================================================================

/// Compute SHAKE-256 hash producing a 32-byte digest.
pub fn shake256(input: &[u8]) -> [u8; 32] {
    let mut hasher = Shake256::default();
    hasher.update(input);
    let mut reader = hasher.finalize_xof();
    let mut output = [0u8; 32];
    reader.read(&mut output);
    output
}

/// Compute SHAKE-256 hash over multiple inputs (concatenated).
pub fn shake256_multi(inputs: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Shake256::default();
    for input in inputs {
        hasher.update(input);
    }
    let mut reader = hasher.finalize_xof();
    let mut output = [0u8; 32];
    reader.read(&mut output);
    output
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dilithium_keygen_sign_verify() {
        let kp = DilithiumKeyPair::generate().unwrap();
        let msg = b"hello marabunta protocol";
        let sig = kp.sign(msg).unwrap();
        assert!(dilithium_verify(&kp.public_key, msg, &sig));
    }

    #[test]
    fn test_dilithium_wrong_message_fails() {
        let kp = DilithiumKeyPair::generate().unwrap();
        let sig = kp.sign(b"correct message").unwrap();
        assert!(!dilithium_verify(&kp.public_key, b"wrong message", &sig));
    }

    #[test]
    fn test_dilithium_wrong_key_fails() {
        let kp1 = DilithiumKeyPair::generate().unwrap();
        let kp2 = DilithiumKeyPair::generate().unwrap();
        let msg = b"test message";
        let sig = kp1.sign(msg).unwrap();
        assert!(!dilithium_verify(&kp2.public_key, msg, &sig));
    }

    #[test]
    fn test_dilithium_bytes_roundtrip() {
        let kp = DilithiumKeyPair::generate().unwrap();
        let pk_bytes = kp.public_key_bytes();
        let sk_bytes = *kp.private_key_bytes();
        let kp2 = DilithiumKeyPair::from_bytes(pk_bytes, sk_bytes).unwrap();
        let msg = b"roundtrip test";
        let sig = kp2.sign(msg).unwrap();
        assert!(dilithium_verify(&kp.public_key, msg, &sig));
    }

    #[test]
    fn test_dilithium_verify_bytes() {
        let kp = DilithiumKeyPair::generate().unwrap();
        let msg = b"verify bytes test";
        let sig = kp.sign(msg).unwrap();
        let pk_bytes = kp.public_key_bytes();
        assert!(dilithium_verify_bytes(&pk_bytes, msg, &sig).unwrap());
    }

    #[test]
    fn test_kyber_encapsulate_decapsulate() {
        let kp = KyberKeyPair::generate();
        let (ct, ss_enc) = KyberKeyPair::encapsulate_with(&kp.encapsulation_key).unwrap();
        let ss_dec = kp.decapsulate(&ct).unwrap();
        assert_eq!(ss_enc.as_bytes(), ss_dec.as_bytes());
    }

    #[test]
    fn test_kyber_wrong_ciphertext_differs() {
        let kp = KyberKeyPair::generate();
        let (mut ct, ss_enc) = KyberKeyPair::encapsulate_with(&kp.encapsulation_key).unwrap();
        // Tamper with ciphertext
        ct[0] ^= 0xff;
        let ss_dec = kp.decapsulate(&ct).unwrap();
        // ML-KEM implicit rejection: decapsulation succeeds but produces different secret
        assert_ne!(ss_enc.as_bytes(), ss_dec.as_bytes());
    }

    #[test]
    fn test_aes256gcm_encrypt_decrypt() {
        let key = [0x42u8; 32];
        let nonce = [0x01u8; 12];
        let plaintext = b"secret data for marabunta";
        let ciphertext = aes256gcm_encrypt(&key, &nonce, plaintext).unwrap();
        let decrypted = aes256gcm_decrypt(&key, &nonce, &ciphertext).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn test_aes256gcm_wrong_key_fails() {
        let key = [0x42u8; 32];
        let wrong_key = [0x43u8; 32];
        let nonce = [0x01u8; 12];
        let ct = aes256gcm_encrypt(&key, &nonce, b"data").unwrap();
        assert!(aes256gcm_decrypt(&wrong_key, &nonce, &ct).is_err());
    }

    #[test]
    fn test_aes256gcm_tampered_ciphertext_fails() {
        let key = [0x42u8; 32];
        let nonce = [0x01u8; 12];
        let mut ct = aes256gcm_encrypt(&key, &nonce, b"data").unwrap();
        ct[0] ^= 0xff;
        assert!(aes256gcm_decrypt(&key, &nonce, &ct).is_err());
    }

    #[test]
    fn test_shake256_deterministic() {
        let input = b"marabunta shake256 test";
        let h1 = shake256(input);
        let h2 = shake256(input);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_shake256_different_inputs() {
        let h1 = shake256(b"input A");
        let h2 = shake256(b"input B");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_shake256_multi() {
        let h1 = shake256_multi(&[b"hello", b" ", b"world"]);
        let h2 = shake256(b"hello world");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hybrid_kem_combine() {
        let classical = [0xAAu8; 32];
        let pq = [0xBBu8; 32];
        let result = hybrid_kem_combine(&classical, &pq);
        assert_ne!(result.combined_secret, [0u8; 32]);
        // Deterministic
        let result2 = hybrid_kem_combine(&classical, &pq);
        assert_eq!(result.combined_secret, result2.combined_secret);
    }

    #[test]
    fn test_hybrid_kem_different_inputs_differ() {
        let r1 = hybrid_kem_combine(&[0xAAu8; 32], &[0xBBu8; 32]);
        let r2 = hybrid_kem_combine(&[0xCCu8; 32], &[0xBBu8; 32]);
        assert_ne!(r1.combined_secret, r2.combined_secret);
    }

    #[test]
    fn test_x25519_diffie_hellman() {
        let (sk_a, pk_a) = x25519_keypair();
        let (sk_b, pk_b) = x25519_keypair();
        let ss_a = x25519_diffie_hellman(&sk_a, &pk_b);
        let ss_b = x25519_diffie_hellman(&sk_b, &pk_a);
        assert_eq!(ss_a, ss_b);
    }

    #[test]
    fn test_full_hybrid_kem_flow() {
        // Simulate full hybrid KEM: X25519 + Kyber
        let (x_sk_a, x_pk_a) = x25519_keypair();
        let (x_sk_b, x_pk_b) = x25519_keypair();
        let classical_ss_a = x25519_diffie_hellman(&x_sk_a, &x_pk_b);
        let classical_ss_b = x25519_diffie_hellman(&x_sk_b, &x_pk_a);
        assert_eq!(classical_ss_a, classical_ss_b);

        let kyber_kp = KyberKeyPair::generate();
        let (ct, pq_ss_enc) = KyberKeyPair::encapsulate_with(&kyber_kp.encapsulation_key).unwrap();
        let pq_ss_dec = kyber_kp.decapsulate(&ct).unwrap();
        assert_eq!(pq_ss_enc.as_bytes(), pq_ss_dec.as_bytes());

        let hybrid_a = hybrid_kem_combine(&classical_ss_a, pq_ss_enc.as_bytes());
        let hybrid_b = hybrid_kem_combine(&classical_ss_b, pq_ss_dec.as_bytes());
        assert_eq!(hybrid_a.combined_secret, hybrid_b.combined_secret);
    }
}
