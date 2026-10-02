// Marabunta - Licensed under the MIT License.
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};
use crate::highestsec::classification::{ClassificationLevel, Compartment};
use crate::highestsec::jurisdiction::JurisdictionSet;
use crate::highestsec::plugin::{
    AuthoritySignature, CriticalityLevel, PluginId, PluginKind,
};

/// Semantic version.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SemVer {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl std::fmt::Display for SemVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl std::str::FromStr for SemVer {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(format!("expected 3 parts in semver, got {}", parts.len()));
        }
        let major = parts[0]
            .parse::<u32>()
            .map_err(|e| format!("invalid major: {}", e))?;
        let minor = parts[1]
            .parse::<u32>()
            .map_err(|e| format!("invalid minor: {}", e))?;
        let patch = parts[2]
            .parse::<u32>()
            .map_err(|e| format!("invalid patch: {}", e))?;
        Ok(SemVer { major, minor, patch })
    }
}

/// Ed25519 signature (64 bytes).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ed25519Signature(pub Vec<u8>);

/// The plugin manifest.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginManifest {
    pub id: PluginId,
    pub version: SemVer,
    pub kind: PluginKind,
    pub binary_hash: [u8; 32],
    pub gatekeeper_signature: Ed25519Signature,
    pub authority_signatures: Vec<AuthoritySignature>,
    pub max_classification: ClassificationLevel,
    pub compartments: Vec<Compartment>,
    pub max_memory_pages: u32,
    pub max_fuel: u64,
    pub max_execution_ms: u64,
    pub max_output_bytes: u64,
    pub execution_jurisdictions: JurisdictionSet,
    pub output_jurisdictions: JurisdictionSet,
    pub nonexport_controlled: bool,
    pub criticality: CriticalityLevel,
    pub required_validators: Vec<PluginId>,
    pub description: String,
    pub author: String,
    pub created_at: String,
    pub expires_at: String,
}

impl PluginManifest {
    /// Produce canonical bytes: serialize all fields EXCEPT gatekeeper_signature
    /// to a JSON object with sorted keys, then return as bytes.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut value = serde_json::to_value(self).expect("manifest serialization must succeed");
        if let serde_json::Value::Object(ref mut map) = value {
            map.remove("gatekeeper_signature");
        }
        // serde_json::to_value with BTreeMap-backed Map already sorts keys at the
        // top level but nested objects also need stable ordering. We recursively
        // sort all keys by converting through sorted canonical form.
        let canonical = sort_json_value(value);
        serde_json::to_vec(&canonical).expect("canonical serialization must succeed")
    }

    /// Verify the gatekeeper Ed25519 signature over canonical_bytes using the
    /// given 32-byte public key.
    pub fn verify_gatekeeper_signature(&self, public_key: &[u8]) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};

        if public_key.len() != 32 {
            return false;
        }
        let sig_bytes = &self.gatekeeper_signature.0;
        if sig_bytes.len() != 64 {
            return false;
        }

        let Ok(vk) = VerifyingKey::from_bytes(
            public_key.try_into().expect("already checked length"),
        ) else {
            return false;
        };

        let Ok(sig) = Signature::from_slice(sig_bytes) else {
            return false;
        };

        let canonical = self.canonical_bytes();
        vk.verify(&canonical, &sig).is_ok()
    }

    /// Check if the manifest has expired by parsing `expires_at` as RFC 3339 /
    /// ISO 8601 and comparing with `Utc::now()`.
    pub fn is_expired(&self) -> bool {
        use chrono::{DateTime, Utc};
        match self.expires_at.parse::<DateTime<Utc>>() {
            Ok(expiry) => Utc::now() > expiry,
            Err(_) => true, // unparseable dates treated as expired
        }
    }

    /// Verify binary integrity: compute SHA-256 of `wasm_bytes` and compare
    /// with `binary_hash`.
    pub fn verify_binary(&self, wasm_bytes: &[u8]) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(wasm_bytes);
        let digest: [u8; 32] = hasher.finalize().into();
        digest == self.binary_hash
    }

    /// Validate manifest invariants:
    /// - Non-empty id
    /// - Non-zero fuel, memory pages, output bytes
    /// - Valid dates (created_at and expires_at parse as ISO 8601, expires_at > created_at)
    /// - Expiry not more than 10 years from created_at
    /// - Classification >= Restricted requires at least one authority signature
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        use chrono::{DateTime, Utc};

        if self.id.is_empty() {
            return Err(ManifestValidationError::EmptyPluginId);
        }

        if self.max_fuel == 0 {
            return Err(ManifestValidationError::ZeroFuelLimit);
        }

        if self.max_memory_pages == 0 {
            return Err(ManifestValidationError::ZeroMemoryPages);
        }

        if self.max_output_bytes == 0 {
            return Err(ManifestValidationError::ZeroOutputBytes);
        }

        let created = self
            .created_at
            .parse::<DateTime<Utc>>()
            .map_err(|_| ManifestValidationError::InvalidDates)?;
        let expires = self
            .expires_at
            .parse::<DateTime<Utc>>()
            .map_err(|_| ManifestValidationError::InvalidDates)?;

        if expires <= created {
            return Err(ManifestValidationError::InvalidDates);
        }

        let max_duration = chrono::Duration::days(365 * 10);
        if expires - created > max_duration {
            return Err(ManifestValidationError::ExpiryTooFar);
        }

        if self.max_classification >= ClassificationLevel::Restricted
            && self.authority_signatures.is_empty()
        {
            return Err(ManifestValidationError::ClassificationRequiresAuthoritySig(
                self.max_classification,
            ));
        }

        Ok(())
    }
}

/// Recursively sort JSON object keys so canonical_bytes is deterministic.
fn sort_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, sort_json_value(v)))
                .collect();
            // serde_json::Map is backed by BTreeMap when feature
            // "preserve_order" is NOT enabled, which sorts keys.
            // If "preserve_order" is enabled it uses IndexMap. To guarantee
            // sorted output regardless we rebuild via BTreeMap.
            let bt: std::collections::BTreeMap<String, serde_json::Value> =
                sorted.into_iter().collect();
            let m: serde_json::Map<String, serde_json::Value> = bt.into_iter().collect();
            serde_json::Value::Object(m)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(sort_json_value).collect())
        }
        other => other,
    }
}

#[derive(Debug)]
pub enum ManifestValidationError {
    ZeroFuelLimit,
    ZeroMemoryPages,
    ZeroOutputBytes,
    InvalidDates,
    ExpiryTooFar,
    MissingGatekeeperSig,
    ClassificationRequiresAuthoritySig(ClassificationLevel),
    EmptyPluginId,
    InvalidVersion,
}

impl std::fmt::Display for ManifestValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroFuelLimit => write!(f, "fuel limit must be non-zero"),
            Self::ZeroMemoryPages => write!(f, "memory pages must be non-zero"),
            Self::ZeroOutputBytes => write!(f, "output bytes limit must be non-zero"),
            Self::InvalidDates => write!(f, "created_at/expires_at are invalid or misordered"),
            Self::ExpiryTooFar => write!(f, "expiry is more than 10 years from creation"),
            Self::MissingGatekeeperSig => write!(f, "gatekeeper signature is missing"),
            Self::ClassificationRequiresAuthoritySig(level) => {
                write!(
                    f,
                    "classification {:?} requires at least one authority signature",
                    level
                )
            }
            Self::EmptyPluginId => write!(f, "plugin id must not be empty"),
            Self::InvalidVersion => write!(f, "invalid semver version"),
        }
    }
}

/// Builder for constructing manifests.
pub struct ManifestBuilder {
    id: PluginId,
    version: SemVer,
    kind: PluginKind,
    binary_hash: [u8; 32],
    gatekeeper_signature: Ed25519Signature,
    authority_signatures: Vec<AuthoritySignature>,
    max_classification: ClassificationLevel,
    compartments: Vec<Compartment>,
    max_memory_pages: u32,
    max_fuel: u64,
    max_execution_ms: u64,
    max_output_bytes: u64,
    execution_jurisdictions: JurisdictionSet,
    output_jurisdictions: JurisdictionSet,
    nonexport_controlled: bool,
    criticality: CriticalityLevel,
    required_validators: Vec<PluginId>,
    description: String,
    author: String,
    created_at: String,
    expires_at: String,
}

impl ManifestBuilder {
    pub fn new(id: PluginId, version: SemVer, kind: PluginKind) -> Self {
        let now = chrono::Utc::now().to_rfc3339();
        let expires = (chrono::Utc::now() + chrono::Duration::days(365)).to_rfc3339();
        Self {
            id,
            version,
            kind,
            binary_hash: [0u8; 32],
            gatekeeper_signature: Ed25519Signature(vec![0u8; 64]),
            authority_signatures: Vec::new(),
            max_classification: ClassificationLevel::Unclassified,
            compartments: Vec::new(),
            max_memory_pages: 256,
            max_fuel: 1_000_000,
            max_execution_ms: 30_000,
            max_output_bytes: 1_048_576,
            execution_jurisdictions: JurisdictionSet::Any,
            output_jurisdictions: JurisdictionSet::Any,
            nonexport_controlled: false,
            criticality: CriticalityLevel::Standard,
            required_validators: Vec::new(),
            description: String::new(),
            author: String::new(),
            created_at: now,
            expires_at: expires,
        }
    }

    pub fn binary_hash(mut self, hash: [u8; 32]) -> Self {
        self.binary_hash = hash;
        self
    }

    pub fn gatekeeper_signature(mut self, sig: Ed25519Signature) -> Self {
        self.gatekeeper_signature = sig;
        self
    }

    pub fn authority_signatures(mut self, sigs: Vec<AuthoritySignature>) -> Self {
        self.authority_signatures = sigs;
        self
    }

    pub fn max_memory_pages(mut self, pages: u32) -> Self {
        self.max_memory_pages = pages;
        self
    }

    pub fn max_fuel(mut self, fuel: u64) -> Self {
        self.max_fuel = fuel;
        self
    }

    pub fn max_execution_ms(mut self, ms: u64) -> Self {
        self.max_execution_ms = ms;
        self
    }

    pub fn max_output_bytes(mut self, bytes: u64) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    pub fn execution_jurisdictions(mut self, set: JurisdictionSet) -> Self {
        self.execution_jurisdictions = set;
        self
    }

    pub fn output_jurisdictions(mut self, set: JurisdictionSet) -> Self {
        self.output_jurisdictions = set;
        self
    }

    pub fn nonexport_controlled(mut self, v: bool) -> Self {
        self.nonexport_controlled = v;
        self
    }

    pub fn criticality(mut self, level: CriticalityLevel) -> Self {
        self.criticality = level;
        self
    }

    pub fn max_classification(mut self, level: ClassificationLevel) -> Self {
        self.max_classification = level;
        self
    }

    pub fn compartments(mut self, compartments: Vec<Compartment>) -> Self {
        self.compartments = compartments;
        self
    }

    pub fn required_validators(mut self, validators: Vec<PluginId>) -> Self {
        self.required_validators = validators;
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.author = author.into();
        self
    }

    pub fn created_at(mut self, ts: impl Into<String>) -> Self {
        self.created_at = ts.into();
        self
    }

    pub fn expires_at(mut self, ts: impl Into<String>) -> Self {
        self.expires_at = ts.into();
        self
    }

    pub fn expires_in_days(mut self, days: u32) -> Self {
        let created = self
            .created_at
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap_or_else(|_| chrono::Utc::now());
        let expiry = created + chrono::Duration::days(days as i64);
        self.expires_at = expiry.to_rfc3339();
        self
    }

    pub fn build(self) -> Result<PluginManifest, ManifestValidationError> {
        let manifest = PluginManifest {
            id: self.id,
            version: self.version,
            kind: self.kind,
            binary_hash: self.binary_hash,
            gatekeeper_signature: self.gatekeeper_signature,
            authority_signatures: self.authority_signatures,
            max_classification: self.max_classification,
            compartments: self.compartments,
            max_memory_pages: self.max_memory_pages,
            max_fuel: self.max_fuel,
            max_execution_ms: self.max_execution_ms,
            max_output_bytes: self.max_output_bytes,
            execution_jurisdictions: self.execution_jurisdictions,
            output_jurisdictions: self.output_jurisdictions,
            nonexport_controlled: self.nonexport_controlled,
            criticality: self.criticality,
            required_validators: self.required_validators,
            description: self.description,
            author: self.author,
            created_at: self.created_at,
            expires_at: self.expires_at,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginState {
    Submitted,
    Signed,
    Distributed,
    Loaded,
    Executing,
    Completed,
    Attested,
    Revoked { reason: String, revoked_at: String },
    Expired,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::classification::ClassificationLevel;
    use crate::highestsec::jurisdiction::JurisdictionSet;
    use crate::highestsec::plugin::{AuthoritySignature, CriticalityLevel, PluginKind};

    fn sample_manifest() -> PluginManifest {
        ManifestBuilder::new(
            "test-plugin".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .description("A test plugin")
        .author("test-author")
        .build()
        .unwrap()
    }

    fn sample_manifest_with_dates(created: &str, expires: &str) -> PluginManifest {
        PluginManifest {
            id: "test-plugin".into(),
            version: SemVer { major: 1, minor: 0, patch: 0 },
            kind: PluginKind::Computation,
            binary_hash: [0u8; 32],
            gatekeeper_signature: Ed25519Signature(vec![0u8; 64]),
            authority_signatures: Vec::new(),
            max_classification: ClassificationLevel::Unclassified,
            compartments: Vec::new(),
            max_memory_pages: 256,
            max_fuel: 1_000_000,
            max_execution_ms: 30_000,
            max_output_bytes: 1_048_576,
            execution_jurisdictions: JurisdictionSet::Any,
            output_jurisdictions: JurisdictionSet::Any,
            nonexport_controlled: false,
            criticality: CriticalityLevel::Standard,
            required_validators: Vec::new(),
            description: String::new(),
            author: String::new(),
            created_at: created.into(),
            expires_at: expires.into(),
        }
    }

    #[test]
    fn test_canonical_bytes_deterministic() {
        let m1 = sample_manifest_with_dates("2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z");
        let m2 = sample_manifest_with_dates("2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z");
        assert_eq!(m1.canonical_bytes(), m2.canonical_bytes());
    }

    #[test]
    fn test_canonical_excludes_signature() {
        let m = sample_manifest();
        let bytes = m.canonical_bytes();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.contains("gatekeeper_signature"),
            "canonical bytes must not contain gatekeeper_signature"
        );
    }

    #[test]
    fn test_canonical_bytes_change_on_modification() {
        let m1 = sample_manifest();
        let bytes1 = m1.canonical_bytes();

        let mut m2 = sample_manifest();
        m2.max_fuel = 999;
        let bytes2 = m2.canonical_bytes();

        assert_ne!(bytes1, bytes2);
    }

    #[test]
    fn test_builder_defaults() {
        let m = sample_manifest();
        assert_eq!(m.id, "test-plugin");
        assert_eq!(m.version, SemVer { major: 1, minor: 0, patch: 0 });
        assert_eq!(m.kind, PluginKind::Computation);
        assert_eq!(m.max_classification, ClassificationLevel::Unclassified);
        assert_eq!(m.max_memory_pages, 256);
        assert_eq!(m.max_fuel, 1_000_000);
        assert_eq!(m.max_execution_ms, 30_000);
        assert_eq!(m.max_output_bytes, 1_048_576);
        assert!(!m.nonexport_controlled);
        assert_eq!(m.criticality, CriticalityLevel::Standard);
    }

    #[test]
    fn test_validate_empty_id() {
        let result = ManifestBuilder::new(
            "".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .build();
        assert!(matches!(result, Err(ManifestValidationError::EmptyPluginId)));
    }

    #[test]
    fn test_validate_zero_fuel() {
        let result = ManifestBuilder::new(
            "p".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .max_fuel(0)
        .build();
        assert!(matches!(result, Err(ManifestValidationError::ZeroFuelLimit)));
    }

    #[test]
    fn test_validate_zero_memory_pages() {
        let result = ManifestBuilder::new(
            "p".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .max_memory_pages(0)
        .build();
        assert!(matches!(
            result,
            Err(ManifestValidationError::ZeroMemoryPages)
        ));
    }

    #[test]
    fn test_validate_zero_output_bytes() {
        let result = ManifestBuilder::new(
            "p".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .max_output_bytes(0)
        .build();
        assert!(matches!(
            result,
            Err(ManifestValidationError::ZeroOutputBytes)
        ));
    }

    #[test]
    fn test_validate_classification_requires_authority_sig() {
        let result = ManifestBuilder::new(
            "p".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .max_classification(ClassificationLevel::Restricted)
        .build();
        assert!(matches!(
            result,
            Err(ManifestValidationError::ClassificationRequiresAuthoritySig(
                ClassificationLevel::Restricted
            ))
        ));
    }

    #[test]
    fn test_validate_classification_with_authority_sig_passes() {
        let sig = AuthoritySignature {
            authority_id: "nato-auth".into(),
            public_key: vec![0u8; 32],
            signature: vec![0u8; 64],
            signed_at: chrono::Utc::now().to_rfc3339(),
        };
        let result = ManifestBuilder::new(
            "p".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .max_classification(ClassificationLevel::Secret)
        .authority_signatures(vec![sig])
        .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_expired_past() {
        let m = sample_manifest_with_dates(
            "2020-01-01T00:00:00Z",
            "2020-06-01T00:00:00Z",
        );
        assert!(m.is_expired());
    }

    #[test]
    fn test_is_expired_future() {
        let m = sample_manifest_with_dates(
            "2020-01-01T00:00:00Z",
            "2099-01-01T00:00:00Z",
        );
        assert!(!m.is_expired());
    }

    #[test]
    fn test_is_expired_invalid_date() {
        let m = sample_manifest_with_dates(
            "2020-01-01T00:00:00Z",
            "not-a-date",
        );
        assert!(m.is_expired()); // invalid dates treated as expired
    }

    #[test]
    fn test_verify_binary_correct() {
        let data = b"hello wasm binary";
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash: [u8; 32] = hasher.finalize().into();

        let mut m = sample_manifest();
        m.binary_hash = hash;
        assert!(m.verify_binary(data));
    }

    #[test]
    fn test_verify_binary_incorrect() {
        let m = sample_manifest(); // hash is all zeros
        assert!(!m.verify_binary(b"hello wasm binary"));
    }

    #[test]
    fn test_verify_gatekeeper_signature_valid() {
        use ed25519_dalek::{Signer, SigningKey};
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        let mut m = sample_manifest();
        let canonical = m.canonical_bytes();
        let sig = signing_key.sign(&canonical);
        m.gatekeeper_signature = Ed25519Signature(sig.to_bytes().to_vec());

        assert!(m.verify_gatekeeper_signature(verifying_key.as_bytes()));
    }

    #[test]
    fn test_verify_gatekeeper_signature_tampered() {
        use ed25519_dalek::{Signer, SigningKey};
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        let mut m = sample_manifest();
        let canonical = m.canonical_bytes();
        let sig = signing_key.sign(&canonical);
        m.gatekeeper_signature = Ed25519Signature(sig.to_bytes().to_vec());

        // Tamper with a field after signing
        m.max_fuel = 42;
        assert!(!m.verify_gatekeeper_signature(verifying_key.as_bytes()));
    }

    #[test]
    fn test_verify_gatekeeper_signature_wrong_key() {
        use ed25519_dalek::{Signer, SigningKey};
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let wrong_key = SigningKey::generate(&mut OsRng);

        let mut m = sample_manifest();
        let canonical = m.canonical_bytes();
        let sig = signing_key.sign(&canonical);
        m.gatekeeper_signature = Ed25519Signature(sig.to_bytes().to_vec());

        assert!(!m.verify_gatekeeper_signature(wrong_key.verifying_key().as_bytes()));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let m = sample_manifest();
        let json = serde_json::to_string(&m).unwrap();
        let m2: PluginManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m.id, m2.id);
        assert_eq!(m.version, m2.version);
        assert_eq!(m.kind, m2.kind);
        assert_eq!(m.binary_hash, m2.binary_hash);
        assert_eq!(m.max_fuel, m2.max_fuel);
        assert_eq!(m.max_memory_pages, m2.max_memory_pages);
        assert_eq!(m.max_output_bytes, m2.max_output_bytes);
        assert_eq!(m.max_execution_ms, m2.max_execution_ms);
        assert_eq!(m.nonexport_controlled, m2.nonexport_controlled);
        assert_eq!(m.max_classification, m2.max_classification);
    }

    #[test]
    fn test_semver_ordering() {
        let v1: SemVer = "1.0.0".parse().unwrap();
        let v1_1: SemVer = "1.1.0".parse().unwrap();
        let v2: SemVer = "2.0.0".parse().unwrap();
        let v1_0_1: SemVer = "1.0.1".parse().unwrap();

        assert!(v1 < v1_0_1);
        assert!(v1_0_1 < v1_1);
        assert!(v1_1 < v2);
        assert_eq!(v1, "1.0.0".parse().unwrap());
    }

    #[test]
    fn test_semver_parse() {
        let v: SemVer = "2.10.3".parse().unwrap();
        assert_eq!(v.major, 2);
        assert_eq!(v.minor, 10);
        assert_eq!(v.patch, 3);
    }

    #[test]
    fn test_semver_display() {
        let v = SemVer { major: 1, minor: 2, patch: 3 };
        assert_eq!(v.to_string(), "1.2.3");
    }

    #[test]
    fn test_semver_parse_invalid() {
        assert!("1.2".parse::<SemVer>().is_err());
        assert!("a.b.c".parse::<SemVer>().is_err());
        assert!("1.2.3.4".parse::<SemVer>().is_err());
    }

    #[test]
    fn test_plugin_state_serialization() {
        let states = vec![
            PluginState::Submitted,
            PluginState::Signed,
            PluginState::Distributed,
            PluginState::Loaded,
            PluginState::Executing,
            PluginState::Completed,
            PluginState::Attested,
            PluginState::Revoked {
                reason: "compromised".into(),
                revoked_at: "2025-01-01T00:00:00Z".into(),
            },
            PluginState::Expired,
        ];
        for state in &states {
            let json = serde_json::to_string(state).unwrap();
            let state2: PluginState = serde_json::from_str(&json).unwrap();
            assert_eq!(state, &state2);
        }
    }

    #[test]
    fn test_builder_expires_in_days() {
        let m = ManifestBuilder::new(
            "test".into(),
            SemVer { major: 1, minor: 0, patch: 0 },
            PluginKind::Computation,
        )
        .expires_in_days(30)
        .build()
        .unwrap();

        use chrono::{DateTime, Utc};
        let created: DateTime<Utc> = m.created_at.parse().unwrap();
        let expires: DateTime<Utc> = m.expires_at.parse().unwrap();
        let diff = expires - created;
        assert_eq!(diff.num_days(), 30);
    }

    #[test]
    fn test_validate_invalid_dates() {
        let m = sample_manifest_with_dates("not-a-date", "also-not-a-date");
        assert!(matches!(m.validate(), Err(ManifestValidationError::InvalidDates)));
    }

    #[test]
    fn test_validate_misordered_dates() {
        let m = sample_manifest_with_dates(
            "2025-06-01T00:00:00Z",
            "2025-01-01T00:00:00Z",
        );
        assert!(matches!(m.validate(), Err(ManifestValidationError::InvalidDates)));
    }

    #[test]
    fn test_validate_expiry_too_far() {
        let m = sample_manifest_with_dates(
            "2020-01-01T00:00:00Z",
            "2040-01-01T00:00:00Z", // 20 years > 10 year limit
        );
        assert!(matches!(m.validate(), Err(ManifestValidationError::ExpiryTooFar)));
    }
}
