// Marabunta - Licensed under the MIT License.
use dashmap::DashMap;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::Digest;
use std::sync::Arc;

use crate::highestsec::audit_events::RevocationReason;
use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::manifest::{PluginManifest, SemVer};
use crate::highestsec::plugin::{PluginId, PluginKind};
use crate::highestsec::signing::{sha256, GatekeeperKey, ManifestRevocation};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes embedded in the binary trailer.
pub const SIGNATURE_MAGIC: &[u8; 8] = b"CMBRSIG\0";

/// Total trailer length: magic (8) + Ed25519 signature (64) + SHA-256 hash (32) + length field (8).
pub const TRAILER_LEN: usize = 8 + 64 + 32 + 8; // = 112

// ---------------------------------------------------------------------------
// BinaryVerifyError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum BinaryVerifyError {
    CannotReadSelf(std::io::Error),
    NoEmbeddedSignature,
    InvalidSignature,
    HashMismatch,
    InvalidMagic,
    BinaryTooSmall,
}

impl std::fmt::Display for BinaryVerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotReadSelf(e) => write!(f, "cannot read /proc/self/exe: {}", e),
            Self::NoEmbeddedSignature => write!(f, "no embedded signature found"),
            Self::InvalidSignature => write!(f, "invalid Ed25519 signature"),
            Self::HashMismatch => write!(f, "binary hash mismatch"),
            Self::InvalidMagic => write!(f, "invalid magic bytes"),
            Self::BinaryTooSmall => write!(f, "binary too small to contain trailer"),
        }
    }
}

// ---------------------------------------------------------------------------
// BinaryIntegrityVerifier
// ---------------------------------------------------------------------------

/// Verifies the integrity and authenticity of agent binaries by checking an
/// appended trailer containing: magic + Ed25519 signature + SHA-256 hash + length.
pub struct BinaryIntegrityVerifier {
    gatekeeper_pubkey: [u8; 32],
}

impl BinaryIntegrityVerifier {
    pub fn new(gatekeeper_pubkey: [u8; 32]) -> Self {
        Self { gatekeeper_pubkey }
    }

    /// Verify the currently running binary by reading `/proc/self/exe`.
    ///
    /// Steps:
    /// 1. Read the binary from `/proc/self/exe`.
    /// 2. Check last 8 bytes encode the trailer length (112).
    /// 3. Extract magic, signature, and hash from the trailer.
    /// 4. Verify magic == `CMBRSIG\0`.
    /// 5. Compute SHA-256 of the binary content before the trailer.
    /// 6. Verify the computed hash matches the embedded hash.
    /// 7. Verify the Ed25519 signature over the hash.
    pub fn verify_self(&self) -> Result<(), BinaryVerifyError> {
        let binary =
            std::fs::read("/proc/self/exe").map_err(BinaryVerifyError::CannotReadSelf)?;
        self.verify_binary(&binary)
    }

    /// Verify arbitrary binary bytes that have an appended signature trailer.
    ///
    /// Trailer layout (last TRAILER_LEN bytes):
    /// ```text
    /// [magic: 8][signature: 64][hash: 32][trailer_len: 8]
    /// ```
    pub fn verify_binary(&self, binary: &[u8]) -> Result<(), BinaryVerifyError> {
        if binary.len() < TRAILER_LEN {
            return Err(BinaryVerifyError::BinaryTooSmall);
        }

        // Step 1: read the trailer length field (last 8 bytes, big-endian u64)
        let len_offset = binary.len() - 8;
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&binary[len_offset..]);
        let stored_len = u64::from_be_bytes(len_bytes) as usize;

        if stored_len != TRAILER_LEN {
            return Err(BinaryVerifyError::NoEmbeddedSignature);
        }

        // Step 2: extract trailer components
        let trailer_start = binary.len() - TRAILER_LEN;
        let trailer = &binary[trailer_start..];

        let magic = &trailer[0..8];
        let sig_bytes = &trailer[8..72];
        let hash_bytes = &trailer[72..104];
        // trailer[104..112] is the length field we already read

        // Step 3: verify magic
        if magic != SIGNATURE_MAGIC.as_slice() {
            return Err(BinaryVerifyError::InvalidMagic);
        }

        // Step 4: compute SHA-256 of the binary content BEFORE the trailer
        let content = &binary[..trailer_start];
        let computed_hash = sha256(content);

        // Step 5: verify hash matches
        if computed_hash.as_slice() != hash_bytes {
            return Err(BinaryVerifyError::HashMismatch);
        }

        // Step 6: verify Ed25519 signature over the hash
        let verifying_key = VerifyingKey::from_bytes(&self.gatekeeper_pubkey)
            .map_err(|_| BinaryVerifyError::InvalidSignature)?;

        let mut sig_arr = [0u8; 64];
        sig_arr.copy_from_slice(sig_bytes);
        let signature = Signature::from_bytes(&sig_arr);

        verifying_key
            .verify(hash_bytes, &signature)
            .map_err(|_| BinaryVerifyError::InvalidSignature)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PluginVerifyError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum PluginVerifyError {
    InvalidManifest(crate::highestsec::manifest::ManifestValidationError),
    ManifestExpired,
    PluginRevoked(RevocationReason),
    BinaryHashMismatch,
    InvalidGatekeeperSignature,
    InvalidAuthoritySignature(String),
    MissingAuthoritySignature(ClassificationLevel),
    VersionNotMonotonic {
        current: SemVer,
        last_known: SemVer,
    },
}

impl std::fmt::Display for PluginVerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidManifest(e) => write!(f, "invalid manifest: {:?}", e),
            Self::ManifestExpired => write!(f, "manifest expired"),
            Self::PluginRevoked(reason) => write!(f, "plugin revoked: {:?}", reason),
            Self::BinaryHashMismatch => write!(f, "binary hash mismatch"),
            Self::InvalidGatekeeperSignature => write!(f, "invalid gatekeeper signature"),
            Self::InvalidAuthoritySignature(id) => {
                write!(f, "invalid authority signature: {}", id)
            }
            Self::MissingAuthoritySignature(level) => {
                write!(f, "missing authority signature for {:?}", level)
            }
            Self::VersionNotMonotonic {
                current,
                last_known,
            } => {
                write!(
                    f,
                    "version {} not newer than {}",
                    current, last_known
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// VerificationResult
// ---------------------------------------------------------------------------

/// Successful verification result returned by `PluginVerifier::verify`.
pub struct VerificationResult {
    pub plugin_id: PluginId,
    pub version: SemVer,
    pub kind: PluginKind,
    pub classification: ClassificationLevel,
    pub verified_at: String,
}

// ---------------------------------------------------------------------------
// PluginVerifier
// ---------------------------------------------------------------------------

/// Performs 8-step verification of plugin manifests and WASM binaries.
pub struct PluginVerifier {
    gatekeeper_pubkey: [u8; 32],
    revocation_store: Arc<RevocationStore>,
}

impl PluginVerifier {
    pub fn new(gatekeeper_pubkey: [u8; 32], revocation_store: Arc<RevocationStore>) -> Self {
        Self {
            gatekeeper_pubkey,
            revocation_store,
        }
    }

    /// 8-step plugin verification:
    ///
    /// 1. Manifest structure validation (non-zero fuel, valid dates, etc.).
    /// 2. Manifest not expired.
    /// 3. Plugin not revoked.
    /// 4. Binary SHA-256 matches `manifest.binary_hash`.
    /// 5. Gatekeeper signature valid.
    /// 6. For RESTRICTED+: at least one authority signature present.
    /// 7. (Version monotonicity -- skipped, needs external state.)
    /// 8. Return `VerificationResult`.
    pub fn verify(
        &self,
        manifest: &PluginManifest,
        wasm_bytes: &[u8],
    ) -> Result<VerificationResult, PluginVerifyError> {
        // Step 1: structural validation
        manifest
            .validate()
            .map_err(PluginVerifyError::InvalidManifest)?;

        // Step 2: expiry check
        if let Ok(expires) = chrono::DateTime::parse_from_rfc3339(&manifest.expires_at) {
            if expires < chrono::Utc::now() {
                return Err(PluginVerifyError::ManifestExpired);
            }
        }

        // Step 3: revocation check
        if let Some(rev) = self
            .revocation_store
            .is_revoked(&manifest.id, &manifest.version)
        {
            return Err(PluginVerifyError::PluginRevoked(rev.reason.clone()));
        }

        // Step 4: binary hash verification
        let actual_hash = sha256(wasm_bytes);
        if manifest.binary_hash != actual_hash.as_slice() {
            return Err(PluginVerifyError::BinaryHashMismatch);
        }

        // Step 5: gatekeeper signature verification
        GatekeeperKey::verify_manifest(&self.gatekeeper_pubkey, manifest)
            .map_err(|_| PluginVerifyError::InvalidGatekeeperSignature)?;

        // Step 6: authority signature check for RESTRICTED and above
        if manifest.max_classification >= ClassificationLevel::Restricted
            && manifest.authority_signatures.is_empty()
        {
            return Err(PluginVerifyError::MissingAuthoritySignature(
                manifest.max_classification,
            ));
        }

        // Step 7: version monotonicity -- skipped (requires persistent state)

        // Step 8: build and return result
        Ok(VerificationResult {
            plugin_id: manifest.id.clone(),
            version: manifest.version.clone(),
            kind: manifest.kind.clone(),
            classification: manifest.max_classification,
            verified_at: chrono::Utc::now().to_rfc3339(),
        })
    }
}

// ---------------------------------------------------------------------------
// RevocationStore
// ---------------------------------------------------------------------------

/// Thread-safe, in-memory store of plugin revocations.
/// Keys are formatted as `"plugin_id:major.minor.patch"`.
pub struct RevocationStore {
    revocations: DashMap<String, ManifestRevocation>,
}

impl RevocationStore {
    pub fn new() -> Self {
        Self {
            revocations: DashMap::new(),
        }
    }

    /// Insert a revocation into the store.
    pub fn add(&self, revocation: ManifestRevocation) {
        let key = format!("{}:{}", revocation.plugin_id, revocation.version);
        self.revocations.insert(key, revocation);
    }

    /// Check whether a specific plugin version has been revoked.
    /// Returns a clone of the revocation if found.
    pub fn is_revoked(&self, plugin_id: &str, version: &SemVer) -> Option<ManifestRevocation> {
        let key = format!("{}:{}", plugin_id, version);
        self.revocations.get(&key).map(|r| r.clone())
    }

    /// Number of revocations stored.
    pub fn len(&self) -> usize {
        self.revocations.len()
    }

    /// Whether the store is empty.
    pub fn is_empty(&self) -> bool {
        self.revocations.is_empty()
    }
}

impl Default for RevocationStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Trailer building helper (for tests and tooling)
// ---------------------------------------------------------------------------

/// Build a signed trailer that can be appended to a binary.
///
/// This is not part of the public verification API but is useful for testing
/// and for the build pipeline that signs release binaries.
pub fn build_trailer(
    binary_content: &[u8],
    signing_key: &ed25519_dalek::SigningKey,
) -> Vec<u8> {
    use ed25519_dalek::Signer;

    let hash = sha256(binary_content);

    // Sign the hash
    let signature = signing_key.sign(&hash);

    let mut trailer = Vec::with_capacity(TRAILER_LEN);
    trailer.extend_from_slice(SIGNATURE_MAGIC); // 8 bytes
    trailer.extend_from_slice(&signature.to_bytes()); // 64 bytes
    trailer.extend_from_slice(&hash); // 32 bytes
    trailer.extend_from_slice(&(TRAILER_LEN as u64).to_be_bytes()); // 8 bytes
    debug_assert_eq!(trailer.len(), TRAILER_LEN);
    trailer
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::classification::ClassificationLevel;
    use crate::highestsec::manifest::Ed25519Signature;
    use crate::highestsec::plugin::{CriticalityLevel, PluginKind};
    use crate::highestsec::signing::GatekeeperKey;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    /// Build a signed test manifest with matching WASM bytes.
    fn hash_to_array(v: &[u8]) -> [u8; 32] {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(v);
        arr
    }

    fn make_signed_manifest(
        key: &GatekeeperKey,
        classification: ClassificationLevel,
    ) -> (PluginManifest, Vec<u8>) {
        let wasm_bytes = b"(module (func (export \"run\")))";
        let binary_hash = hash_to_array(&sha256(wasm_bytes));

        let mut manifest = PluginManifest {
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
            max_classification: classification,
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

        // If classification >= Restricted, add a dummy authority signature
        if classification >= ClassificationLevel::Restricted {
            manifest.authority_signatures.push(
                crate::highestsec::plugin::AuthoritySignature {
                    authority_id: "authority-0".to_string(),
                    public_key: vec![0u8; 32],
                    signature: vec![0u8; 64],
                    signed_at: "2026-01-01T00:00:00Z".to_string(),
                },
            );
        }

        key.sign_manifest(&mut manifest, wasm_bytes.as_slice())
            .unwrap();
        (manifest, wasm_bytes.to_vec())
    }

    // ---- PluginVerifier tests ----

    #[test]
    fn test_plugin_verify_valid() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());
        let verifier = PluginVerifier::new(pubkey, store);

        let (manifest, wasm) = make_signed_manifest(&key, ClassificationLevel::Unclassified);
        let result = verifier.verify(&manifest, &wasm).unwrap();

        assert_eq!(result.plugin_id, "test-plugin");
        assert_eq!(result.version.major, 1);
        assert_eq!(result.kind, PluginKind::Computation);
        assert_eq!(result.classification, ClassificationLevel::Unclassified);
    }

    #[test]
    fn test_plugin_verify_expired() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());
        let verifier = PluginVerifier::new(pubkey, store);

        let wasm_bytes = b"(module (func (export \"run\")))";
        let binary_hash = hash_to_array(&sha256(wasm_bytes));
        let mut manifest = PluginManifest {
            id: "expired-plugin".to_string(),
            version: SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            kind: PluginKind::Computation,
            binary_hash,
            gatekeeper_signature: Ed25519Signature(Vec::new()),
            authority_signatures: Vec::new(),
            max_classification: ClassificationLevel::Unclassified,
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
            description: "expired plugin".to_string(),
            author: "test-author".to_string(),
            created_at: "2020-01-01T00:00:00Z".to_string(),
            expires_at: "2020-06-01T00:00:00Z".to_string(), // already expired
        };

        key.sign_manifest(&mut manifest, wasm_bytes.as_slice())
            .unwrap();

        let result = verifier.verify(&manifest, wasm_bytes);
        assert!(matches!(result, Err(PluginVerifyError::ManifestExpired)));
    }

    #[test]
    fn test_plugin_verify_revoked() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());

        let (manifest, wasm) = make_signed_manifest(&key, ClassificationLevel::Unclassified);

        // Add revocation
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
        store.add(revocation);

        let verifier = PluginVerifier::new(pubkey, store);
        let result = verifier.verify(&manifest, &wasm);
        assert!(matches!(
            result,
            Err(PluginVerifyError::PluginRevoked(
                RevocationReason::SecurityVulnerability
            ))
        ));
    }

    #[test]
    fn test_plugin_verify_bad_hash() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());
        let verifier = PluginVerifier::new(pubkey, store);

        let (manifest, _wasm) = make_signed_manifest(&key, ClassificationLevel::Unclassified);

        // Provide wrong WASM bytes
        let wrong_wasm = b"this is not the right wasm";
        let result = verifier.verify(&manifest, wrong_wasm);
        assert!(matches!(result, Err(PluginVerifyError::BinaryHashMismatch)));
    }

    #[test]
    fn test_plugin_verify_bad_gatekeeper_sig() {
        let key = GatekeeperKey::generate();
        let other_key = GatekeeperKey::generate();
        let wrong_pubkey = other_key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());
        let verifier = PluginVerifier::new(wrong_pubkey, store);

        let (manifest, wasm) = make_signed_manifest(&key, ClassificationLevel::Unclassified);

        let result = verifier.verify(&manifest, &wasm);
        assert!(matches!(
            result,
            Err(PluginVerifyError::InvalidGatekeeperSignature)
        ));
    }

    #[test]
    fn test_plugin_verify_restricted_needs_authority() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());
        let verifier = PluginVerifier::new(pubkey, store);

        // Build a RESTRICTED manifest WITHOUT authority signatures
        let wasm_bytes = b"(module (func (export \"run\")))";
        let binary_hash = hash_to_array(&sha256(wasm_bytes));
        let mut manifest = PluginManifest {
            id: "restricted-plugin".to_string(),
            version: SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            kind: PluginKind::Computation,
            binary_hash,
            gatekeeper_signature: Ed25519Signature(Vec::new()),
            authority_signatures: Vec::new(), // deliberately empty
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
            description: "restricted plugin".to_string(),
            author: "test-author".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            expires_at: "2027-01-01T00:00:00Z".to_string(),
        };

        key.sign_manifest(&mut manifest, wasm_bytes.as_slice())
            .unwrap();

        let result = verifier.verify(&manifest, wasm_bytes);
        assert!(matches!(
            result,
            Err(PluginVerifyError::InvalidManifest(_))
        ));
    }

    #[test]
    fn test_plugin_verify_restricted_with_authority_passes() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());
        let verifier = PluginVerifier::new(pubkey, store);

        let (manifest, wasm) = make_signed_manifest(&key, ClassificationLevel::Restricted);

        let result = verifier.verify(&manifest, &wasm);
        assert!(result.is_ok());
        let res = result.unwrap();
        assert_eq!(res.classification, ClassificationLevel::Restricted);
    }

    #[test]
    fn test_plugin_verify_secret_needs_authority() {
        let key = GatekeeperKey::generate();
        let pubkey = key.public_key_bytes();
        let store = Arc::new(RevocationStore::new());
        let verifier = PluginVerifier::new(pubkey, store);

        let wasm_bytes = b"(module (func (export \"run\")))";
        let binary_hash = hash_to_array(&sha256(wasm_bytes));
        let mut manifest = PluginManifest {
            id: "secret-plugin".to_string(),
            version: SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            kind: PluginKind::Sovereignty,
            binary_hash,
            gatekeeper_signature: Ed25519Signature(Vec::new()),
            authority_signatures: Vec::new(),
            max_classification: ClassificationLevel::Secret,
            compartments: Vec::new(),
            max_memory_pages: 512,
            max_fuel: 2_000_000,
            max_execution_ms: 10000,
            max_output_bytes: 2_097_152,
            execution_jurisdictions: crate::highestsec::jurisdiction::JurisdictionSet::Any,
            output_jurisdictions: crate::highestsec::jurisdiction::JurisdictionSet::Any,
            nonexport_controlled: false,
            criticality: CriticalityLevel::Critical,
            required_validators: Vec::new(),
            description: "secret plugin".to_string(),
            author: "test-author".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            expires_at: "2027-01-01T00:00:00Z".to_string(),
        };

        key.sign_manifest(&mut manifest, wasm_bytes.as_slice())
            .unwrap();

        let result = verifier.verify(&manifest, wasm_bytes);
        assert!(matches!(
            result,
            Err(PluginVerifyError::InvalidManifest(_))
        ));
    }

    // ---- RevocationStore tests ----

    #[test]
    fn test_revocation_store_add_and_check() {
        let key = GatekeeperKey::generate();
        let store = RevocationStore::new();

        let revocation = ManifestRevocation::create(
            &key,
            "plugin-a".to_string(),
            SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            RevocationReason::Compromised,
        );

        store.add(revocation);
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        let found = store.is_revoked(
            "plugin-a",
            &SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
        );
        assert!(found.is_some());
        assert_eq!(found.unwrap().plugin_id, "plugin-a");
    }

    #[test]
    fn test_revocation_store_not_found() {
        let store = RevocationStore::new();
        let result = store.is_revoked(
            "nonexistent",
            &SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
        );
        assert!(result.is_none());
    }

    #[test]
    fn test_revocation_store_different_versions() {
        let key = GatekeeperKey::generate();
        let store = RevocationStore::new();

        let rev_v1 = ManifestRevocation::create(
            &key,
            "plugin-b".to_string(),
            SemVer {
                major: 1,
                minor: 0,
                patch: 0,
            },
            RevocationReason::Expired,
        );
        store.add(rev_v1);

        // v1.0.0 is revoked
        assert!(store
            .is_revoked(
                "plugin-b",
                &SemVer {
                    major: 1,
                    minor: 0,
                    patch: 0,
                },
            )
            .is_some());

        // v2.0.0 is NOT revoked
        assert!(store
            .is_revoked(
                "plugin-b",
                &SemVer {
                    major: 2,
                    minor: 0,
                    patch: 0,
                },
            )
            .is_none());
    }

    #[test]
    fn test_revocation_store_multiple_plugins() {
        let key = GatekeeperKey::generate();
        let store = RevocationStore::new();

        for i in 0..5 {
            let rev = ManifestRevocation::create(
                &key,
                format!("plugin-{}", i),
                SemVer {
                    major: 1,
                    minor: 0,
                    patch: 0,
                },
                RevocationReason::SecurityVulnerability,
            );
            store.add(rev);
        }

        assert_eq!(store.len(), 5);
        for i in 0..5 {
            assert!(store
                .is_revoked(
                    &format!("plugin-{}", i),
                    &SemVer {
                        major: 1,
                        minor: 0,
                        patch: 0,
                    },
                )
                .is_some());
        }
    }

    #[test]
    fn test_revocation_store_empty() {
        let store = RevocationStore::new();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    // ---- Binary trailer tests ----

    #[test]
    fn test_trailer_format() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let content = b"hello binary content";

        let trailer = build_trailer(content, &signing_key);
        assert_eq!(trailer.len(), TRAILER_LEN);

        // Check magic
        assert_eq!(&trailer[0..8], SIGNATURE_MAGIC.as_slice());

        // Check length field
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&trailer[104..112]);
        let stored_len = u64::from_be_bytes(len_bytes) as usize;
        assert_eq!(stored_len, TRAILER_LEN);

        // Check hash
        let expected_hash = sha256(content);
        assert_eq!(&trailer[72..104], expected_hash.as_slice());
    }

    #[test]
    fn test_binary_trailer_verify() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let content = b"hello binary content";

        let trailer = build_trailer(content, &signing_key);

        // Construct the full "binary"
        let mut full_binary = content.to_vec();
        full_binary.extend_from_slice(&trailer);

        let verifier = BinaryIntegrityVerifier::new(pubkey);
        verifier.verify_binary(&full_binary).unwrap();
    }

    #[test]
    fn test_binary_trailer_tampered_content() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let content = b"hello binary content";

        let trailer = build_trailer(content, &signing_key);

        // Tamper with the binary content
        let mut full_binary = b"TAMPERED binary content".to_vec();
        full_binary.extend_from_slice(&trailer);

        let verifier = BinaryIntegrityVerifier::new(pubkey);
        let result = verifier.verify_binary(&full_binary);
        assert!(matches!(result, Err(BinaryVerifyError::HashMismatch)));
    }

    #[test]
    fn test_binary_trailer_wrong_key() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let other_key = SigningKey::generate(&mut OsRng);
        let wrong_pubkey = other_key.verifying_key().to_bytes();
        let content = b"hello binary content";

        let trailer = build_trailer(content, &signing_key);

        let mut full_binary = content.to_vec();
        full_binary.extend_from_slice(&trailer);

        let verifier = BinaryIntegrityVerifier::new(wrong_pubkey);
        let result = verifier.verify_binary(&full_binary);
        assert!(matches!(result, Err(BinaryVerifyError::InvalidSignature)));
    }

    #[test]
    fn test_binary_too_small() {
        let verifier = BinaryIntegrityVerifier::new([0u8; 32]);
        let result = verifier.verify_binary(&[0u8; 10]);
        assert!(matches!(result, Err(BinaryVerifyError::BinaryTooSmall)));
    }

    #[test]
    fn test_binary_no_embedded_signature() {
        let verifier = BinaryIntegrityVerifier::new([0u8; 32]);
        // Create a binary that is large enough but has wrong trailer length
        let mut binary = vec![0u8; 200];
        // Set the last 8 bytes to an invalid trailer length (not 112)
        let wrong_len: u64 = 999;
        binary[192..200].copy_from_slice(&wrong_len.to_be_bytes());

        let result = verifier.verify_binary(&binary);
        assert!(matches!(
            result,
            Err(BinaryVerifyError::NoEmbeddedSignature)
        ));
    }

    #[test]
    fn test_binary_invalid_magic() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let content = b"hello binary content";

        let mut trailer = build_trailer(content, &signing_key);
        // Corrupt the magic bytes
        trailer[0] = 0xFF;
        trailer[1] = 0xFF;

        let mut full_binary = content.to_vec();
        full_binary.extend_from_slice(&trailer);

        let verifier = BinaryIntegrityVerifier::new(pubkey);
        let result = verifier.verify_binary(&full_binary);
        assert!(matches!(result, Err(BinaryVerifyError::InvalidMagic)));
    }

    #[test]
    fn test_binary_tampered_signature() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let content = b"hello binary content";

        let mut trailer = build_trailer(content, &signing_key);
        // Corrupt one byte of the signature
        trailer[10] ^= 0xFF;

        let mut full_binary = content.to_vec();
        full_binary.extend_from_slice(&trailer);

        let verifier = BinaryIntegrityVerifier::new(pubkey);
        let result = verifier.verify_binary(&full_binary);
        assert!(matches!(result, Err(BinaryVerifyError::InvalidSignature)));
    }

    #[test]
    fn test_trailer_length_constant() {
        // Sanity-check the TRAILER_LEN constant
        assert_eq!(TRAILER_LEN, 112);
        assert_eq!(TRAILER_LEN, 8 + 64 + 32 + 8);
    }
}
