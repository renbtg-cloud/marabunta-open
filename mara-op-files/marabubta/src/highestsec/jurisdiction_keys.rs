// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::RngCore;
use std::sync::Arc;

use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::jurisdiction::ZoneClass;
use crate::highestsec::plugin::KeyId;
use crate::highestsec::types::CountryCode;
use crate::highestsec::zone_membership::ZoneCertificateStore;

// ---------------------------------------------------------------------------
// JurisdictionBoundKey
// ---------------------------------------------------------------------------

/// A cryptographic key wrapped with jurisdiction constraints.
///
/// The `encrypted_key` field contains the raw key material encrypted (or
/// wrapped) in a way that can only be released to a node that holds a valid
/// zone-membership certificate matching the constraints.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JurisdictionBoundKey {
    pub key_id: KeyId,
    pub encrypted_key: Vec<u8>,
    /// AES-256-GCM nonce used for key wrapping.
    #[serde(default)]
    pub wrapping_nonce: [u8; 12],
    pub jurisdiction: CountryCode,
    pub min_zone_class: ZoneClass,
    pub min_proof_level: u8,
    pub expires_at: String,
    pub authority_signature: Vec<u8>,
    pub authority_pubkey: Vec<u8>,
}

impl JurisdictionBoundKey {
    /// Produce canonical bytes for signature verification -- everything
    /// except `authority_signature`.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut value =
            serde_json::to_value(self).expect("JurisdictionBoundKey serialization");
        if let serde_json::Value::Object(ref mut map) = value {
            map.remove("authority_signature");
        }
        serde_json::to_vec(&value).expect("canonical serialization")
    }

    /// Verify the Ed25519 signature over `canonical_bytes()`.
    pub fn verify_signature(&self) -> bool {
        if self.authority_pubkey.len() != 32 {
            return false;
        }
        if self.authority_signature.len() != 64 {
            return false;
        }

        let Ok(vk) = VerifyingKey::from_bytes(
            self.authority_pubkey
                .as_slice()
                .try_into()
                .expect("length checked"),
        ) else {
            return false;
        };

        let Ok(sig) = Signature::from_slice(&self.authority_signature) else {
            return false;
        };

        let canonical = self.canonical_bytes();
        vk.verify(&canonical, &sig).is_ok()
    }

    /// Return `true` when `expires_at` is in the past.
    pub fn is_expired(&self) -> bool {
        match self.expires_at.parse::<DateTime<Utc>>() {
            Ok(expiry) => Utc::now() > expiry,
            Err(_) => true,
        }
    }
}

// ---------------------------------------------------------------------------
// KeyReleaseRecord
// ---------------------------------------------------------------------------

/// Audit record of a key release event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyReleaseRecord {
    pub key_id: KeyId,
    pub node_id: Vec<u8>,
    pub jurisdiction: CountryCode,
    pub proof_level: u8,
    pub released_at: String,
    pub expires_at: String,
}

// ---------------------------------------------------------------------------
// EncryptedKeyRelease
// ---------------------------------------------------------------------------

/// The payload returned to a node when a key is released.
#[derive(Debug)]
pub struct EncryptedKeyRelease {
    pub key_id: KeyId,
    pub encrypted_key: Vec<u8>,
    pub valid_until: String,
}

// ---------------------------------------------------------------------------
// KeyReleaseError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum KeyReleaseError {
    InvalidNodeCertificate,
    NodeCertificateExpired,
    JurisdictionMismatch {
        key: CountryCode,
        node: CountryCode,
    },
    InsufficientZoneClass {
        required: ZoneClass,
        actual: ZoneClass,
    },
    InsufficientProofLevel {
        required: u8,
        actual: u8,
    },
    KeyExpired,
    ClassificationTooHigh {
        job: ClassificationLevel,
        node_max: ClassificationLevel,
    },
    EncryptionError(String),
}

impl std::fmt::Display for KeyReleaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNodeCertificate => write!(f, "invalid node certificate"),
            Self::NodeCertificateExpired => write!(f, "node certificate expired"),
            Self::JurisdictionMismatch { key, node } => {
                write!(f, "jurisdiction mismatch: key={}, node={}", key, node)
            }
            Self::InsufficientZoneClass { required, actual } => {
                write!(
                    f,
                    "insufficient zone class: required {:?}, actual {:?}",
                    required, actual
                )
            }
            Self::InsufficientProofLevel { required, actual } => {
                write!(
                    f,
                    "insufficient proof level: required {}, actual {}",
                    required, actual
                )
            }
            Self::KeyExpired => write!(f, "key expired"),
            Self::ClassificationTooHigh { job, node_max } => {
                write!(
                    f,
                    "classification too high: job {:?}, node max {:?}",
                    job, node_max
                )
            }
            Self::EncryptionError(msg) => write!(f, "encryption error: {}", msg),
        }
    }
}

// ---------------------------------------------------------------------------
// KeyReleaseAuthority
// ---------------------------------------------------------------------------

/// Issues jurisdiction-bound keys and releases them to authorized nodes.
pub struct KeyReleaseAuthority {
    authority_key: SigningKey,
    cert_store: Arc<ZoneCertificateStore>,
    issued_keys: DashMap<KeyId, KeyReleaseRecord>,
}

impl KeyReleaseAuthority {
    pub fn new(authority_key: SigningKey, cert_store: Arc<ZoneCertificateStore>) -> Self {
        Self {
            authority_key,
            cert_store,
            issued_keys: DashMap::new(),
        }
    }

    /// Bind a raw key with jurisdiction constraints, sign with the authority
    /// key.
    ///
    /// The raw key is encrypted with AES-256-GCM using a wrapping key
    /// derived from the authority signing key and key ID.
    pub fn bind_key(
        &self,
        raw_key: &[u8],
        jurisdiction: CountryCode,
        min_zone_class: ZoneClass,
        min_proof_level: u8,
        valid_for: std::time::Duration,
    ) -> JurisdictionBoundKey {
        let key_id = generate_key_id(raw_key);
        let expires_at = (Utc::now()
            + Duration::from_std(valid_for).unwrap_or_else(|_| Duration::days(365)))
        .to_rfc3339();

        let wrapping_key = derive_wrapping_key(&self.authority_key, &key_id);
        let (encrypted_key, nonce) = wrap_key_material(raw_key, &wrapping_key);

        let mut bound_key = JurisdictionBoundKey {
            key_id,
            encrypted_key,
            wrapping_nonce: nonce,
            jurisdiction,
            min_zone_class,
            min_proof_level,
            expires_at,
            authority_signature: Vec::new(),
            authority_pubkey: self.authority_key.verifying_key().to_bytes().to_vec(),
        };

        let canonical = bound_key.canonical_bytes();
        let sig = self.authority_key.sign(&canonical);
        bound_key.authority_signature = sig.to_bytes().to_vec();

        bound_key
    }

    /// Release a key to a node, checking all jurisdiction and classification
    /// constraints.
    ///
    /// Checks (in order):
    /// 1. Key not expired.
    /// 2. Node has a valid certificate in the store.
    /// 3. Node certificate not expired.
    /// 4. Jurisdiction matches.
    /// 5. Zone class sufficient.
    /// 6. Proof level sufficient.
    /// 7. Classification level within node cert ceiling.
    pub fn release_key(
        &self,
        bound_key: &JurisdictionBoundKey,
        node_id: &[u8],
        job_classification: ClassificationLevel,
    ) -> Result<EncryptedKeyRelease, KeyReleaseError> {
        // 1. Key expiry.
        if bound_key.is_expired() {
            return Err(KeyReleaseError::KeyExpired);
        }

        // 2. Node certificate.
        let cert = self
            .cert_store
            .get_valid(node_id)
            .ok_or(KeyReleaseError::InvalidNodeCertificate)?;

        // 3. Certificate expiry (get_valid already checks, but be explicit).
        if cert.is_expired() {
            return Err(KeyReleaseError::NodeCertificateExpired);
        }

        // 4. Jurisdiction.
        if cert.jurisdiction != bound_key.jurisdiction {
            return Err(KeyReleaseError::JurisdictionMismatch {
                key: bound_key.jurisdiction,
                node: cert.jurisdiction,
            });
        }

        // 5. Zone class.
        if cert.zone_class < bound_key.min_zone_class {
            return Err(KeyReleaseError::InsufficientZoneClass {
                required: bound_key.min_zone_class,
                actual: cert.zone_class,
            });
        }

        // 6. Proof level.
        if cert.proof_level < bound_key.min_proof_level {
            return Err(KeyReleaseError::InsufficientProofLevel {
                required: bound_key.min_proof_level,
                actual: cert.proof_level,
            });
        }

        // 7. Classification ceiling.
        if !cert.authorizes_classification(job_classification) {
            return Err(KeyReleaseError::ClassificationTooHigh {
                job: job_classification,
                node_max: cert.max_classification,
            });
        }

        // Record the release.
        let now = Utc::now();
        let release_expires = bound_key.expires_at.clone();
        let record = KeyReleaseRecord {
            key_id: bound_key.key_id.clone(),
            node_id: node_id.to_vec(),
            jurisdiction: cert.jurisdiction,
            proof_level: cert.proof_level,
            released_at: now.to_rfc3339(),
            expires_at: release_expires.clone(),
        };
        self.issued_keys
            .insert(bound_key.key_id.clone(), record);

        // Return the encrypted key material (node decrypts with the shared
        // secret in a real system; here we just pass the XOR-encrypted blob).
        Ok(EncryptedKeyRelease {
            key_id: bound_key.key_id.clone(),
            encrypted_key: bound_key.encrypted_key.clone(),
            valid_until: release_expires,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate a deterministic key ID from the raw key material.
fn generate_key_id(raw_key: &[u8]) -> KeyId {
    let mut hasher = Sha256::new();
    hasher.update(raw_key);
    let hash = hasher.finalize();
    format!("key-{}", hex::encode(&hash[..8]))
}

/// Derive a 32-byte AES wrapping key from the authority signing key and key ID.
fn derive_wrapping_key(authority_signing_key: &SigningKey, key_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"jurisdiction-key-wrapping-v1");
    hasher.update(authority_signing_key.verifying_key().as_bytes());
    hasher.update(key_id.as_bytes());
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

/// Wrap key material using AES-256-GCM.
fn wrap_key_material(raw_key: &[u8], wrapping_key: &[u8; 32]) -> (Vec<u8>, [u8; 12]) {
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);
    let cipher = Aes256Gcm::new_from_slice(wrapping_key).expect("valid 32-byte key");
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), raw_key)
        .expect("AES-GCM encryption should not fail");
    (ciphertext, nonce)
}

/// Unwrap key material using AES-256-GCM.
fn unwrap_key_material(
    encrypted: &[u8],
    wrapping_key: &[u8; 32],
    nonce: &[u8; 12],
) -> Result<Vec<u8>, KeyReleaseError> {
    let cipher = Aes256Gcm::new_from_slice(wrapping_key)
        .map_err(|e| KeyReleaseError::EncryptionError(format!("{}", e)))?;
    cipher
        .decrypt(Nonce::from_slice(nonce), encrypted)
        .map_err(|e| KeyReleaseError::EncryptionError(format!("{}", e)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::zone_membership::ZoneMembershipCertificate;
    use rand::rngs::OsRng;

    fn gen_key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    /// Insert a cert into the store, signed by `authority_key`.
    fn insert_cert(
        store: &ZoneCertificateStore,
        authority_key: &SigningKey,
        node_id: Vec<u8>,
        zone_id: &str,
        jurisdiction: CountryCode,
        zone_class: ZoneClass,
        proof_level: u8,
        max_classification: ClassificationLevel,
        expires_at: &str,
    ) {
        let mut cert = ZoneMembershipCertificate {
            node_id,
            zone_id: zone_id.to_string(),
            jurisdiction,
            zone_class,
            proof_level,
            issued_at: Utc::now().to_rfc3339(),
            expires_at: expires_at.to_string(),
            proof_hash: [0u8; 32],
            authority_signature: Vec::new(),
            authority_pubkey: authority_key.verifying_key().to_bytes().to_vec(),
            max_classification,
            requires_clearnet: false,
            kyber_ek: vec![0x42u8; 32],
        };
        let canonical = cert.canonical_bytes();
        let sig = authority_key.sign(&canonical);
        cert.authority_signature = sig.to_bytes().to_vec();
        store.upsert(cert);
    }

    // ========================================================================
    // JurisdictionBoundKey tests
    // ========================================================================

    #[test]
    fn test_bind_key_and_verify_signature() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());
        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let raw_key = b"my-super-secret-key-material-32b";
        let bound = kra.bind_key(
            raw_key,
            CountryCode::FR,
            ZoneClass::GovCloud,
            1,
            std::time::Duration::from_secs(3600),
        );

        assert!(bound.verify_signature());
        assert!(!bound.is_expired());
        assert_eq!(bound.jurisdiction, CountryCode::FR);
        assert_eq!(bound.min_zone_class, ZoneClass::GovCloud);
        assert_eq!(bound.min_proof_level, 1);
    }

    #[test]
    fn test_bound_key_tampered_signature_fails() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());
        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let raw_key = b"secret-key";
        let mut bound = kra.bind_key(
            raw_key,
            CountryCode::FR,
            ZoneClass::GovCloud,
            1,
            std::time::Duration::from_secs(3600),
        );

        // tamper
        bound.min_proof_level = 99;
        assert!(!bound.verify_signature());
    }

    #[test]
    fn test_bound_key_expired() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());
        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let raw_key = b"secret";
        let mut bound = kra.bind_key(
            raw_key,
            CountryCode::FR,
            ZoneClass::Civilian,
            0,
            std::time::Duration::from_secs(1),
        );
        // force expiry to past
        bound.expires_at = "2020-01-01T00:00:00Z".into();
        assert!(bound.is_expired());
    }

    // ========================================================================
    // KeyReleaseAuthority::release_key tests
    // ========================================================================

    #[test]
    fn test_release_key_valid() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());

        // Insert a valid cert for node [1,2,3] in FR / GovCloud / Restricted
        insert_cert(
            &cert_store,
            &auth_key,
            vec![1, 2, 3],
            "zone-gov-fr",
            CountryCode::FR,
            ZoneClass::GovCloud,
            1,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        );

        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let raw_key = b"the-actual-key-material-for-job";
        let bound = kra.bind_key(
            raw_key,
            CountryCode::FR,
            ZoneClass::GovCloud,
            1,
            std::time::Duration::from_secs(86400),
        );

        let release = kra
            .release_key(&bound, &[1, 2, 3], ClassificationLevel::Restricted)
            .expect("release should succeed");

        assert_eq!(release.key_id, bound.key_id);
        assert!(!release.encrypted_key.is_empty());
    }

    #[test]
    fn test_release_key_wrong_jurisdiction() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());

        // Node cert is for DE
        insert_cert(
            &cert_store,
            &auth_key,
            vec![1],
            "zone-de",
            CountryCode::DE,
            ZoneClass::GovCloud,
            1,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        );

        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        // Key is bound to FR
        let bound = kra.bind_key(
            b"key",
            CountryCode::FR,
            ZoneClass::GovCloud,
            1,
            std::time::Duration::from_secs(3600),
        );

        let result = kra.release_key(&bound, &[1], ClassificationLevel::Restricted);
        assert!(result.is_err());
        match result.unwrap_err() {
            KeyReleaseError::JurisdictionMismatch { key, node } => {
                assert_eq!(key, CountryCode::FR);
                assert_eq!(node, CountryCode::DE);
            }
            other => panic!("expected JurisdictionMismatch, got: {:?}", other),
        }
    }

    #[test]
    fn test_release_key_insufficient_zone_class() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());

        // Node cert is Civilian
        insert_cert(
            &cert_store,
            &auth_key,
            vec![1],
            "zone-civ",
            CountryCode::FR,
            ZoneClass::Civilian,
            0,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        );

        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        // Key requires MilRestricted
        let bound = kra.bind_key(
            b"key",
            CountryCode::FR,
            ZoneClass::MilRestricted,
            0,
            std::time::Duration::from_secs(3600),
        );

        let result = kra.release_key(&bound, &[1], ClassificationLevel::Unclassified);
        assert!(result.is_err());
        match result.unwrap_err() {
            KeyReleaseError::InsufficientZoneClass { required, actual } => {
                assert_eq!(required, ZoneClass::MilRestricted);
                assert_eq!(actual, ZoneClass::Civilian);
            }
            other => panic!("expected InsufficientZoneClass, got: {:?}", other),
        }
    }

    #[test]
    fn test_release_key_insufficient_proof_level() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());

        // Node cert has proof level 1
        insert_cert(
            &cert_store,
            &auth_key,
            vec![1],
            "zone-fr",
            CountryCode::FR,
            ZoneClass::MilRestricted,
            1,
            ClassificationLevel::Confidential,
            "2099-01-01T00:00:00Z",
        );

        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        // Key requires proof level 2
        let bound = kra.bind_key(
            b"key",
            CountryCode::FR,
            ZoneClass::MilRestricted,
            2,
            std::time::Duration::from_secs(3600),
        );

        let result = kra.release_key(&bound, &[1], ClassificationLevel::Restricted);
        assert!(result.is_err());
        match result.unwrap_err() {
            KeyReleaseError::InsufficientProofLevel { required, actual } => {
                assert_eq!(required, 2);
                assert_eq!(actual, 1);
            }
            other => panic!("expected InsufficientProofLevel, got: {:?}", other),
        }
    }

    #[test]
    fn test_release_key_classification_too_high() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());

        // Node cert allows up to Restricted
        insert_cert(
            &cert_store,
            &auth_key,
            vec![1],
            "zone-gov",
            CountryCode::FR,
            ZoneClass::GovCloud,
            1,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        );

        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let bound = kra.bind_key(
            b"key",
            CountryCode::FR,
            ZoneClass::GovCloud,
            1,
            std::time::Duration::from_secs(3600),
        );

        // Job is Secret -- too high for node
        let result = kra.release_key(&bound, &[1], ClassificationLevel::Secret);
        assert!(result.is_err());
        match result.unwrap_err() {
            KeyReleaseError::ClassificationTooHigh { job, node_max } => {
                assert_eq!(job, ClassificationLevel::Secret);
                assert_eq!(node_max, ClassificationLevel::Restricted);
            }
            other => panic!("expected ClassificationTooHigh, got: {:?}", other),
        }
    }

    #[test]
    fn test_release_key_expired() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());

        insert_cert(
            &cert_store,
            &auth_key,
            vec![1],
            "zone",
            CountryCode::FR,
            ZoneClass::Civilian,
            0,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        );

        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let mut bound = kra.bind_key(
            b"key",
            CountryCode::FR,
            ZoneClass::Civilian,
            0,
            std::time::Duration::from_secs(1),
        );
        bound.expires_at = "2020-01-01T00:00:00Z".into();

        let result = kra.release_key(&bound, &[1], ClassificationLevel::Unclassified);
        assert!(matches!(result, Err(KeyReleaseError::KeyExpired)));
    }

    #[test]
    fn test_release_key_no_certificate() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());

        // No cert inserted for node [42]
        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let bound = kra.bind_key(
            b"key",
            CountryCode::FR,
            ZoneClass::Civilian,
            0,
            std::time::Duration::from_secs(3600),
        );

        let result = kra.release_key(&bound, &[42], ClassificationLevel::Unclassified);
        assert!(matches!(
            result,
            Err(KeyReleaseError::InvalidNodeCertificate)
        ));
    }

    #[test]
    fn test_bound_key_serialization_roundtrip() {
        let auth_key = gen_key();
        let cert_store = Arc::new(ZoneCertificateStore::new());
        let kra = KeyReleaseAuthority::new(auth_key, cert_store);

        let bound = kra.bind_key(
            b"roundtrip-key",
            CountryCode::DE,
            ZoneClass::MilClassified,
            3,
            std::time::Duration::from_secs(7200),
        );

        let json = serde_json::to_string(&bound).unwrap();
        let deserialized: JurisdictionBoundKey = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.key_id, bound.key_id);
        assert_eq!(deserialized.jurisdiction, CountryCode::DE);
        assert!(deserialized.verify_signature());
    }

    #[test]
    fn test_bound_key_aes_gcm_roundtrip() {
        let auth_key = gen_key();
        let raw_key = b"my-super-secret-key-material-32b";
        let key_id = generate_key_id(raw_key);

        let wrapping_key = derive_wrapping_key(&auth_key, &key_id);
        let (encrypted, nonce) = wrap_key_material(raw_key, &wrapping_key);

        // Ciphertext should differ from plaintext
        assert_ne!(encrypted.as_slice(), raw_key.as_slice());
        // Ciphertext is plaintext + 16-byte GCM tag
        assert_eq!(encrypted.len(), raw_key.len() + 16);

        let decrypted = unwrap_key_material(&encrypted, &wrapping_key, &nonce).unwrap();
        assert_eq!(decrypted, raw_key);
    }
}
