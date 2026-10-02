// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use rand::RngCore;

use crate::highestsec::audit_events::RevocationReason;
use crate::highestsec::manifest::{Ed25519Signature, PluginManifest, SemVer};
use crate::highestsec::plugin::PluginId;

// ---------------------------------------------------------------------------
// GatekeeperError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum GatekeeperError {
    BinaryHashMismatch,
    InvalidSignature,
    KeyLoadFailed(String),
    KeySaveFailed(String),
    ManifestExpired,
    IoError(std::io::Error),
}

impl std::fmt::Display for GatekeeperError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BinaryHashMismatch => write!(f, "binary hash mismatch"),
            Self::InvalidSignature => write!(f, "invalid signature"),
            Self::KeyLoadFailed(msg) => write!(f, "key load failed: {}", msg),
            Self::KeySaveFailed(msg) => write!(f, "key save failed: {}", msg),
            Self::ManifestExpired => write!(f, "manifest expired"),
            Self::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl From<std::io::Error> for GatekeeperError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

// ---------------------------------------------------------------------------
// GatekeeperKey
// ---------------------------------------------------------------------------

/// Gatekeeper identity -- signs plugin manifests.
///
/// The signing key is an Ed25519 key. When persisted to disk, it is encrypted
/// with AES-256-GCM keyed by Argon2id(passphrase, salt). File format:
///   16-byte salt || 12-byte nonce || AES-256-GCM(key_bytes) (32 bytes ciphertext + 16 tag)
pub struct GatekeeperKey {
    signing_key: SigningKey,
}

impl std::fmt::Debug for GatekeeperKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatekeeperKey")
            .field("public_key", &hex::encode(self.signing_key.verifying_key().as_bytes()))
            .finish()
    }
}

impl GatekeeperKey {
    /// Generate a new keypair using the operating-system RNG.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        Self { signing_key }
    }

    /// Load a signing key from an AES-256-GCM encrypted file.
    ///
    /// File layout: `salt (16 bytes) || nonce (12 bytes) || ciphertext+tag (48 bytes)` = 76 bytes.
    pub fn load(path: &std::path::Path, passphrase: &[u8]) -> Result<Self, GatekeeperError> {
        let data = std::fs::read(path)?;
        if data.len() < ARGON2_SALT_LEN + 12 + 48 {
            return Err(GatekeeperError::KeyLoadFailed(
                "file too small".to_string(),
            ));
        }

        let salt = &data[..ARGON2_SALT_LEN];
        let nonce_bytes = &data[ARGON2_SALT_LEN..ARGON2_SALT_LEN + 12];
        let ciphertext = &data[ARGON2_SALT_LEN + 12..];

        let mut salt_arr = [0u8; ARGON2_SALT_LEN];
        salt_arr.copy_from_slice(salt);
        let enc_key = derive_aes_key(passphrase, &salt_arr);
        let cipher = Aes256Gcm::new_from_slice(&enc_key)
            .map_err(|e| GatekeeperError::KeyLoadFailed(format!("cipher init: {}", e)))?;
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| {
            GatekeeperError::KeyLoadFailed(
                "decryption failed (wrong passphrase or corrupted file)".to_string(),
            )
        })?;

        if plaintext.len() != 32 {
            return Err(GatekeeperError::KeyLoadFailed(
                "decrypted key has wrong length".to_string(),
            ));
        }

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&plaintext);
        let signing_key = SigningKey::from_bytes(&key_bytes);

        Ok(Self { signing_key })
    }

    /// Save the signing key to an AES-256-GCM encrypted file.
    pub fn save(
        &self,
        path: &std::path::Path,
        passphrase: &[u8],
    ) -> Result<(), GatekeeperError> {
        let mut salt = [0u8; ARGON2_SALT_LEN];
        OsRng.fill_bytes(&mut salt);

        let enc_key = derive_aes_key(passphrase, &salt);
        let cipher = Aes256Gcm::new_from_slice(&enc_key)
            .map_err(|e| GatekeeperError::KeySaveFailed(format!("cipher init: {}", e)))?;

        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, self.signing_key.to_bytes().as_ref())
            .map_err(|e| GatekeeperError::KeySaveFailed(format!("encryption: {}", e)))?;

        let mut output = Vec::with_capacity(ARGON2_SALT_LEN + 12 + ciphertext.len());
        output.extend_from_slice(&salt);
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);

        std::fs::write(path, &output)?;
        Ok(())
    }

    /// Get the 32-byte public key.
    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Sign a plugin manifest.
    ///
    /// 1. Verify that `manifest.binary_hash` matches `SHA-256(wasm_bytes)`.
    /// 2. Compute the canonical byte representation of the manifest.
    /// 3. Sign those bytes and set `manifest.gatekeeper_signature`.
    pub fn sign_manifest(
        &self,
        manifest: &mut PluginManifest,
        wasm_bytes: &[u8],
    ) -> Result<(), GatekeeperError> {
        // Step 1: verify binary hash
        let actual_hash = sha256(wasm_bytes);
        if manifest.binary_hash != actual_hash.as_slice() {
            return Err(GatekeeperError::BinaryHashMismatch);
        }

        // Step 2: canonical bytes (everything except gatekeeper_signature)
        let canonical = manifest.canonical_bytes();

        // Step 3: sign
        let sig = self.signing_key.sign(&canonical);
        manifest.gatekeeper_signature = Ed25519Signature(sig.to_bytes().to_vec());

        Ok(())
    }

    /// Verify a manifest's gatekeeper signature against the supplied public key.
    pub fn verify_manifest(
        public_key: &[u8; 32],
        manifest: &PluginManifest,
    ) -> Result<(), GatekeeperError> {
        let verifying_key = VerifyingKey::from_bytes(public_key)
            .map_err(|_| GatekeeperError::InvalidSignature)?;

        let sig_bytes = &manifest.gatekeeper_signature.0;
        if sig_bytes.len() != 64 {
            return Err(GatekeeperError::InvalidSignature);
        }
        let mut sig_arr = [0u8; 64];
        sig_arr.copy_from_slice(sig_bytes);
        let signature =
            Signature::from_bytes(&sig_arr);

        let canonical = manifest.canonical_bytes();
        verifying_key
            .verify(&canonical, &signature)
            .map_err(|_| GatekeeperError::InvalidSignature)
    }
}

// ---------------------------------------------------------------------------
// ManifestRevocation
// ---------------------------------------------------------------------------

/// A signed revocation message for a specific plugin version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestRevocation {
    pub plugin_id: PluginId,
    pub version: SemVer,
    pub reason: RevocationReason,
    pub revoked_at: String,
    /// Ed25519 signature over `canonical_bytes()`.
    pub gatekeeper_signature: Vec<u8>,
}

impl ManifestRevocation {
    /// Create and sign a new revocation.
    pub fn create(
        key: &GatekeeperKey,
        plugin_id: PluginId,
        version: SemVer,
        reason: RevocationReason,
    ) -> Self {
        let revoked_at = chrono::Utc::now().to_rfc3339();
        let mut revocation = Self {
            plugin_id,
            version,
            reason,
            revoked_at,
            gatekeeper_signature: Vec::new(),
        };
        let canonical = revocation.canonical_bytes();
        let sig = key.signing_key.sign(&canonical);
        revocation.gatekeeper_signature = sig.to_bytes().to_vec();
        revocation
    }

    /// Verify the revocation signature against the supplied public key.
    pub fn verify(&self, public_key: &[u8; 32]) -> bool {
        let Ok(verifying_key) = VerifyingKey::from_bytes(public_key) else {
            return false;
        };
        if self.gatekeeper_signature.len() != 64 {
            return false;
        }
        let mut sig_arr = [0u8; 64];
        sig_arr.copy_from_slice(&self.gatekeeper_signature);
        let signature = Signature::from_bytes(&sig_arr);
        let canonical = self.canonical_bytes();
        verifying_key.verify(&canonical, &signature).is_ok()
    }

    /// Canonical byte representation used for signing.
    /// Covers: plugin_id, version, reason, revoked_at (but NOT gatekeeper_signature).
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(self.plugin_id.as_bytes());
        buf.extend_from_slice(self.version.to_string().as_bytes());
        buf.extend_from_slice(&serde_json::to_vec(&self.reason).unwrap_or_default());
        buf.extend_from_slice(self.revoked_at.as_bytes());
        buf
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const ARGON2_SALT_LEN: usize = 16;

/// Derive a 32-byte AES key from a passphrase via Argon2id.
///
/// Parameters: 19 MiB memory, 2 iterations, 1 lane (OWASP minimum recommendation).
fn derive_aes_key(passphrase: &[u8], salt: &[u8; ARGON2_SALT_LEN]) -> [u8; 32] {
    let params = Params::new(19456, 2, 1, Some(32)).expect("valid Argon2 params");
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = [0u8; 32];
    argon2
        .hash_password_into(passphrase, salt, &mut key)
        .expect("Argon2id hash should not fail with valid params");
    key
}

/// SHA-256 hash helper.
pub(crate) fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::classification::ClassificationLevel;
    use crate::highestsec::plugin::{CriticalityLevel, PluginKind};
    use tempfile::NamedTempFile;

    /// Build a minimal test manifest and matching WASM bytes.
    fn make_test_manifest() -> (PluginManifest, Vec<u8>) {
        let wasm_bytes = b"(module (func (export \"run\")))";
        let hash_vec = sha256(wasm_bytes);
        let mut binary_hash = [0u8; 32];
        binary_hash.copy_from_slice(&hash_vec);
        let manifest = PluginManifest {
            id: "test-plugin".to_string(),
            version: SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            kind: PluginKind::Computation,
            binary_hash,
            gatekeeper_signature: Ed25519Signature(Vec::new()),
            authority_signatures: Vec::new(),
            max_classification: ClassificationLevel::Restricted,
            compartments: Vec::new(),
            max_memory_pages: 256,
            max_fuel: 1_000_000,
            max_execution_ms: 5000,
            max_output_bytes: 1_048_576,
            execution_jurisdictions: crate::highestsec::jurisdiction::JurisdictionSet::Any,
            output_jurisdictions: crate::highestsec::jurisdiction::JurisdictionSet::Any,
            nonexport_controlled: false,
            criticality: CriticalityLevel::Standard,
            required_validators: Vec::new(),
            description: "test plugin".to_string(),
            author: "test-author".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            expires_at: "2027-01-01T00:00:00Z".to_string(),
        };
        (manifest, wasm_bytes.to_vec())
    }

    #[test]
    fn test_keygen() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        assert_eq!(pubkey.len(), 32);
        // Two generated keys should differ (astronomically unlikely collision).
        let key2 = GatekeeperKey::generate();
        assert_ne!(key.public_key_bytes(), key2.public_key_bytes());
    }

    #[test]
    fn test_save_load_roundtrip() {
        let key = GatekeeperKey::generate();
        let passphrase = b"super-secret-passphrase";

        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        key.save(&path, passphrase).unwrap();
        let loaded = GatekeeperKey::load(&path, passphrase).unwrap();

        assert_eq!(key.public_key_bytes(), loaded.public_key_bytes());
        assert_eq!(
            key.signing_key.to_bytes(),
            loaded.signing_key.to_bytes()
        );
    }

    #[test]
    fn test_wrong_passphrase() {
        let key = GatekeeperKey::generate();

        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        key.save(&path, b"correct").unwrap();
        let result = GatekeeperKey::load(&path, b"wrong");

        assert!(result.is_err());
        match result.unwrap_err() {
            GatekeeperError::KeyLoadFailed(msg) => {
                assert!(msg.contains("decryption failed"));
            }
            other => panic!("expected KeyLoadFailed, got: {:?}", other),
        }
    }

    #[test]
    fn test_sign_manifest() {
        let key = GatekeeperKey::generate();
        let (mut manifest, wasm_bytes) = make_test_manifest();

        key.sign_manifest(&mut manifest, &wasm_bytes).unwrap();

        // Signature should be 64 bytes
        assert_eq!(manifest.gatekeeper_signature.0.len(), 64);
    }

    #[test]
    fn test_verify_valid_signature() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let (mut manifest, wasm_bytes) = make_test_manifest();

        key.sign_manifest(&mut manifest, &wasm_bytes).unwrap();
        GatekeeperKey::verify_manifest(&pubkey, &manifest).unwrap();
    }

    #[test]
    fn test_verify_tampered_manifest() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let (mut manifest, wasm_bytes) = make_test_manifest();

        key.sign_manifest(&mut manifest, &wasm_bytes).unwrap();

        // Tamper with the manifest after signing
        manifest.max_fuel = 999_999_999;

        let result = GatekeeperKey::verify_manifest(&pubkey, &manifest);
        assert!(result.is_err());
        match result.unwrap_err() {
            GatekeeperError::InvalidSignature => {}
            other => panic!("expected InvalidSignature, got: {:?}", other),
        }
    }

    #[test]
    fn test_verify_wrong_key() {
        let key = GatekeeperKey::generate();
        let other_key = GatekeeperKey::generate();
        let wrong_pubkey = other_key.public_key_bytes();

        let (mut manifest, wasm_bytes) = make_test_manifest();
        key.sign_manifest(&mut manifest, &wasm_bytes).unwrap();

        let result = GatekeeperKey::verify_manifest(&wrong_pubkey, &manifest);
        assert!(result.is_err());
        match result.unwrap_err() {
            GatekeeperError::InvalidSignature => {}
            other => panic!("expected InvalidSignature, got: {:?}", other),
        }
    }

    #[test]
    fn test_binary_hash_mismatch() {
        let key = GatekeeperKey::generate();
        let (mut manifest, _wasm_bytes) = make_test_manifest();

        // Provide different WASM bytes than what the manifest expects
        let wrong_bytes = b"totally different wasm";
        let result = key.sign_manifest(&mut manifest, wrong_bytes);
        assert!(result.is_err());
        match result.unwrap_err() {
            GatekeeperError::BinaryHashMismatch => {}
            other => panic!("expected BinaryHashMismatch, got: {:?}", other),
        }
    }

    #[test]
    fn test_revocation_sign_and_verify() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();

        let revocation = ManifestRevocation::create(
            &key,
            "test-plugin".to_string(),
            SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            RevocationReason::SecurityVulnerability,
        );

        assert!(revocation.verify(&pubkey));
        assert_eq!(revocation.plugin_id, "test-plugin");
        assert_eq!(revocation.gatekeeper_signature.len(), 64);
    }

    #[test]
    fn test_revocation_tampered() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();

        let mut revocation = ManifestRevocation::create(
            &key,
            "test-plugin".to_string(),
            SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            RevocationReason::Compromised,
        );

        // Tamper with revocation
        revocation.plugin_id = "evil-plugin".to_string();
        assert!(!revocation.verify(&pubkey));
    }

    #[test]
    fn test_revocation_wrong_key() {
        let key = GatekeeperKey::generate();
        let other_key = GatekeeperKey::generate();
        let wrong_pubkey = other_key.public_key_bytes();

        let revocation = ManifestRevocation::create(
            &key,
            "test-plugin".to_string(),
            SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            RevocationReason::Expired,
        );

        assert!(!revocation.verify(&wrong_pubkey));
    }

    #[test]
    fn test_revocation_canonical_bytes_deterministic() {
        let key = GatekeeperKey::generate();
        let rev1 = ManifestRevocation {
            plugin_id: "p".to_string(),
            version: SemVer {
                major: 1,
                minor: 2,
                patch: 3,
            },
            reason: RevocationReason::Compromised,
            revoked_at: "2026-01-01T00:00:00Z".to_string(),
            gatekeeper_signature: Vec::new(),
        };
        let rev2 = ManifestRevocation {
            plugin_id: "p".to_string(),
            version: SemVer {
                major: 1,
                minor: 2,
                patch: 3,
            },
            reason: RevocationReason::Compromised,
            revoked_at: "2026-01-01T00:00:00Z".to_string(),
            gatekeeper_signature: vec![0xff; 64], // different sig, should not affect canonical
        };
        assert_eq!(rev1.canonical_bytes(), rev2.canonical_bytes());
    }

    #[test]
    fn test_revocation_superseded_variant() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();

        let revocation = ManifestRevocation::create(
            &key,
            "test-plugin".to_string(),
            SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            RevocationReason::Superseded {
                replacement_id: "test-plugin-v2".to_string(),
            },
        );

        assert!(revocation.verify(&pubkey));

        // Serialization roundtrip
        let json = serde_json::to_string(&revocation).unwrap();
        let deserialized: ManifestRevocation = serde_json::from_str(&json).unwrap();
        assert!(deserialized.verify(&pubkey));
    }

    #[test]
    fn test_save_to_nonexistent_dir_fails() {
        let key = GatekeeperKey::generate();
        let result = key.save(
            std::path::Path::new("/nonexistent/dir/key.bin"),
            b"pass",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_load_from_nonexistent_file_fails() {
        let result = GatekeeperKey::load(
            std::path::Path::new("/nonexistent/key.bin"),
            b"pass",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_load_truncated_file() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        std::fs::write(&path, &[0u8; 10]).unwrap(); // too small
        let result = GatekeeperKey::load(&path, b"pass");
        assert!(result.is_err());
        match result.unwrap_err() {
            GatekeeperError::KeyLoadFailed(msg) => {
                assert!(msg.contains("too small"));
            }
            other => panic!("expected KeyLoadFailed, got: {:?}", other),
        }
    }

    #[test]
    fn test_argon2id_different_passphrases_different_keys() {
        let salt = [0u8; ARGON2_SALT_LEN];
        let key1 = derive_aes_key(b"passphrase-one", &salt);
        let key2 = derive_aes_key(b"passphrase-two", &salt);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_argon2id_deterministic() {
        let salt = [42u8; ARGON2_SALT_LEN];
        let key1 = derive_aes_key(b"same-passphrase", &salt);
        let key2 = derive_aes_key(b"same-passphrase", &salt);
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_sha256_helper() {
        let hash = sha256(b"hello");
        assert_eq!(hash.len(), 32);
        // Known SHA-256 of "hello"
        let expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(hex, expected);
    }
}
