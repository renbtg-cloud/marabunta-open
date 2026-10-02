// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::jurisdiction::ZoneClass;
use crate::highestsec::jurisdiction_proof::{self, JurisdictionProof};
use crate::highestsec::plugin::ZoneId;
use crate::highestsec::types::CountryCode;

// ---------------------------------------------------------------------------
// ZoneMembershipCertificate
// ---------------------------------------------------------------------------

/// A signed certificate proving that a given node has been admitted into
/// a specific zone at a known jurisdiction, zone class and classification
/// ceiling.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZoneMembershipCertificate {
    pub node_id: Vec<u8>,
    pub zone_id: ZoneId,
    pub jurisdiction: CountryCode,
    pub zone_class: ZoneClass,
    pub proof_level: u8,
    pub issued_at: String,
    pub expires_at: String,
    pub proof_hash: [u8; 32],
    pub authority_signature: Vec<u8>,
    pub authority_pubkey: Vec<u8>,
    pub max_classification: ClassificationLevel,
    /// Hybrid Swarm (take05): Whether workloads in this zone mandate static IP and mTLS,
    /// rejecting highly evasive DarkNet routing (Pillar 6.1 / 15.3).
    #[serde(default)]
    pub requires_clearnet: bool,
    /// Kyber encapsulation key (public) for post-quantum key exchange.
    /// Included in canonical bytes so that the authority signature covers it.
    /// Must not be empty — certificates without a Kyber key are rejected.
    pub kyber_ek: Vec<u8>,
}

impl ZoneMembershipCertificate {
    /// Produce canonical bytes for signature verification.
    ///
    /// Serializes every field **except** `authority_signature` into a
    /// deterministic JSON byte vector.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut value =
            serde_json::to_value(self).expect("ZoneMembershipCertificate serialization");
        if let serde_json::Value::Object(ref mut map) = value {
            map.remove("authority_signature");
        }
        let mut buf = serde_json::to_vec(&value).expect("canonical serialization");
        // Explicitly include kyber_ek with length prefix so the authority
        // signature always covers the Kyber encapsulation key.
        buf.extend_from_slice(&(self.kyber_ek.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.kyber_ek);
        buf
    }

    /// Verify the Ed25519 signature in `authority_signature` over
    /// `canonical_bytes()` using the embedded `authority_pubkey`.
    pub fn verify(&self) -> bool {
        if self.kyber_ek.is_empty() {
            return false; // reject certs without Kyber key
        }
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
            Err(_) => true, // unparseable => treat as expired
        }
    }

    /// Return `true` when `level` is at or below this certificate's ceiling.
    pub fn authorizes_classification(&self, level: ClassificationLevel) -> bool {
        level <= self.max_classification
    }
}

// ---------------------------------------------------------------------------
// ZoneRevocation
// ---------------------------------------------------------------------------

/// Signed statement revoking a node's membership in a zone.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZoneRevocation {
    pub node_id: Vec<u8>,
    pub zone_id: ZoneId,
    pub reason: String,
    pub revoked_at: String,
    pub authority_signature: Vec<u8>,
    pub authority_pubkey: Vec<u8>,
}

// ---------------------------------------------------------------------------
// AdmissionError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum AdmissionError {
    InsufficientProofLevel {
        required: u8,
        provided: u8,
    },
    JurisdictionMismatch {
        expected: CountryCode,
        proved: CountryCode,
    },
    ClassificationTooHigh {
        requested: ClassificationLevel,
        zone_max: ClassificationLevel,
    },
    /// The (zone_class, classification) combination is outright forbidden by
    /// the proof-level matrix.
    ForbiddenCombination,
    AlreadyMember,
    NodeNotFound,
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientProofLevel { required, provided } => {
                write!(
                    f,
                    "insufficient proof level: required {}, provided {}",
                    required, provided
                )
            }
            Self::JurisdictionMismatch { expected, proved } => {
                write!(
                    f,
                    "jurisdiction mismatch: expected {}, proved {}",
                    expected, proved
                )
            }
            Self::ClassificationTooHigh {
                requested,
                zone_max,
            } => {
                write!(
                    f,
                    "classification too high: requested {:?}, zone max {:?}",
                    requested, zone_max
                )
            }
            Self::ForbiddenCombination => {
                write!(f, "forbidden zone-class / classification combination")
            }
            Self::AlreadyMember => write!(f, "node is already a member"),
            Self::NodeNotFound => write!(f, "node not found"),
        }
    }
}

// ---------------------------------------------------------------------------
// ZoneAdmissionController
// ---------------------------------------------------------------------------

/// Issues and revokes zone-membership certificates after verifying
/// jurisdiction proofs.
pub struct ZoneAdmissionController {
    zone_id: ZoneId,
    zone_class: ZoneClass,
    jurisdiction: CountryCode,
    authority_key: SigningKey,
    max_classification: ClassificationLevel,
    issued_certs: DashMap<Vec<u8>, ZoneMembershipCertificate>,
}

impl ZoneAdmissionController {
    pub fn new(
        zone_id: ZoneId,
        zone_class: ZoneClass,
        jurisdiction: CountryCode,
        authority_key: SigningKey,
        max_classification: ClassificationLevel,
    ) -> Self {
        Self {
            zone_id,
            zone_class,
            jurisdiction,
            authority_key,
            max_classification,
            issued_certs: DashMap::new(),
        }
    }

    /// Admit a node into this zone.
    ///
    /// 1. Reject if node is already admitted.
    /// 2. Reject if `requested_classification > zone max_classification`.
    /// 3. Reject if `geo_country != self.jurisdiction`.
    /// 4. Look up `min_proof_level(zone_class, requested_classification)`.
    ///    - `None` => ForbiddenCombination.
    ///    - `Some(required)` with `proof.level() < required` =>
    ///      InsufficientProofLevel.
    /// 5. Issue a signed certificate.
    pub fn admit(
        &self,
        node_id: &[u8],
        proof: &JurisdictionProof,
        geo_country: &CountryCode,
        requested_classification: ClassificationLevel,
        kyber_ek: Vec<u8>,
    ) -> Result<ZoneMembershipCertificate, AdmissionError> {
        // 1. Already a member?
        if self.issued_certs.contains_key(node_id) {
            return Err(AdmissionError::AlreadyMember);
        }

        // 2. Classification ceiling check.
        if requested_classification > self.max_classification {
            return Err(AdmissionError::ClassificationTooHigh {
                requested: requested_classification,
                zone_max: self.max_classification,
            });
        }

        // 3. Jurisdiction match.
        if *geo_country != self.jurisdiction {
            return Err(AdmissionError::JurisdictionMismatch {
                expected: self.jurisdiction,
                proved: *geo_country,
            });
        }

        // 4. Proof-level matrix.
        let required = jurisdiction_proof::min_proof_level(self.zone_class, requested_classification)
            .ok_or(AdmissionError::ForbiddenCombination)?;

        let provided = proof.level();
        if provided < required {
            return Err(AdmissionError::InsufficientProofLevel {
                required,
                provided,
            });
        }

        // 5. Hash the proof for inclusion in the certificate.
        let proof_json =
            serde_json::to_vec(proof).expect("JurisdictionProof serialization");
        let proof_hash: [u8; 32] = {
            let mut h = Sha256::new();
            h.update(&proof_json);
            h.finalize().into()
        };

        let now = Utc::now();
        let expires = now + chrono::Duration::days(365);

        let mut cert = ZoneMembershipCertificate {
            node_id: node_id.to_vec(),
            zone_id: self.zone_id.clone(),
            jurisdiction: self.jurisdiction,
            zone_class: self.zone_class,
            proof_level: provided,
            issued_at: now.to_rfc3339(),
            expires_at: expires.to_rfc3339(),
            proof_hash,
            authority_signature: Vec::new(),
            authority_pubkey: self.authority_key.verifying_key().to_bytes().to_vec(),
            max_classification: requested_classification,
            requires_clearnet: false,
            kyber_ek,
        };

        // Sign.
        let canonical = cert.canonical_bytes();
        let sig = self.authority_key.sign(&canonical);
        cert.authority_signature = sig.to_bytes().to_vec();

        self.issued_certs.insert(node_id.to_vec(), cert.clone());
        Ok(cert)
    }

    /// Revoke a node's membership.
    pub fn revoke(
        &self,
        node_id: &[u8],
        reason: &str,
    ) -> Result<ZoneRevocation, AdmissionError> {
        if !self.issued_certs.contains_key(node_id) {
            return Err(AdmissionError::NodeNotFound);
        }

        self.issued_certs.remove(node_id);

        let now = Utc::now();
        let mut revocation = ZoneRevocation {
            node_id: node_id.to_vec(),
            zone_id: self.zone_id.clone(),
            reason: reason.to_string(),
            revoked_at: now.to_rfc3339(),
            authority_signature: Vec::new(),
            authority_pubkey: self.authority_key.verifying_key().to_bytes().to_vec(),
        };

        // Sign the revocation: hash over (node_id || zone_id || reason || revoked_at).
        let mut to_sign = Vec::new();
        to_sign.extend_from_slice(node_id);
        to_sign.extend_from_slice(revocation.zone_id.as_bytes());
        to_sign.extend_from_slice(revocation.reason.as_bytes());
        to_sign.extend_from_slice(revocation.revoked_at.as_bytes());

        let sig = self.authority_key.sign(&to_sign);
        revocation.authority_signature = sig.to_bytes().to_vec();

        Ok(revocation)
    }

    /// Return `true` if the node currently holds a valid (non-expired)
    /// certificate in this controller's local store.
    pub fn is_member(&self, node_id: &[u8]) -> bool {
        self.issued_certs
            .get(node_id)
            .map(|c| !c.is_expired())
            .unwrap_or(false)
    }

    /// Remove all expired certificates and return the number removed.
    pub fn cleanup_expired(&self) -> usize {
        let expired_keys: Vec<Vec<u8>> = self
            .issued_certs
            .iter()
            .filter(|entry| entry.value().is_expired())
            .map(|entry| entry.key().clone())
            .collect();
        let count = expired_keys.len();
        for key in expired_keys {
            self.issued_certs.remove(&key);
        }
        count
    }
}

// ---------------------------------------------------------------------------
// ZoneCertificateStore
// ---------------------------------------------------------------------------

/// Distributed store for zone-membership certificates and revocations.
pub struct ZoneCertificateStore {
    certificates: DashMap<Vec<u8>, ZoneMembershipCertificate>,
    revocations: DashMap<Vec<u8>, ZoneRevocation>,
}

impl Default for ZoneCertificateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ZoneCertificateStore {
    pub fn new() -> Self {
        Self {
            certificates: DashMap::new(),
            revocations: DashMap::new(),
        }
    }

    /// Insert or update a certificate for a node.
    pub fn upsert(&self, cert: ZoneMembershipCertificate) {
        self.certificates.insert(cert.node_id.clone(), cert);
    }

    /// Record a revocation, which shadows the corresponding certificate.
    pub fn revoke(&self, revocation: ZoneRevocation) {
        self.revocations
            .insert(revocation.node_id.clone(), revocation);
    }

    /// Return the certificate for `node_id` if it exists, is not revoked
    /// and is not expired.
    pub fn get_valid(&self, node_id: &[u8]) -> Option<ZoneMembershipCertificate> {
        if self.revocations.contains_key(node_id) {
            return None;
        }
        self.certificates.get(node_id).and_then(|c| {
            if c.is_expired() {
                None
            } else {
                Some(c.value().clone())
            }
        })
    }

    /// Check whether `node_id` holds a valid certificate for `zone_id`
    /// that authorizes the requested `classification`.
    pub fn is_authorized(
        &self,
        node_id: &[u8],
        zone_id: &str,
        classification: ClassificationLevel,
    ) -> bool {
        self.get_valid(node_id)
            .map(|c| c.zone_id == zone_id && c.authorizes_classification(classification))
            .unwrap_or(false)
    }

    /// Remove all expired certificates and return the number removed.
    pub fn cleanup_expired(&self) -> usize {
        let expired: Vec<Vec<u8>> = self
            .certificates
            .iter()
            .filter(|e| e.value().is_expired())
            .map(|e| e.key().clone())
            .collect();
        let n = expired.len();
        for key in expired {
            self.certificates.remove(&key);
        }
        n
    }

    /// Return all valid (non-revoked, non-expired) certificates across all zones.
    pub fn all_valid_certificates(&self) -> Vec<ZoneMembershipCertificate> {
        self.certificates
            .iter()
            .filter(|e| !e.value().is_expired() && !self.revocations.contains_key(e.key()))
            .map(|e| e.value().clone())
            .collect()
    }

    /// Return all recent revocations (no expiry filter — revocations are always relevant).
    pub fn recent_revocations(&self) -> Vec<ZoneRevocation> {
        self.revocations.iter().map(|e| e.value().clone()).collect()
    }

    /// Check whether `node_id` holds any valid certificate for `zone_id`
    /// (regardless of classification level). Used for zone-based work claiming.
    pub fn is_zone_member(&self, node_id: &[u8], zone_id: &str) -> bool {
        self.get_valid(node_id)
            .map(|c| c.zone_id == zone_id)
            .unwrap_or(false)
    }

    /// Return all zone IDs that a node holds valid certificates for.
    pub fn zones_for_node(&self, node_id: &[u8]) -> Vec<ZoneMembershipCertificate> {
        // A node can only have one cert in this store (keyed by node_id),
        // but return as Vec for forward compatibility with multi-zone.
        self.get_valid(node_id).into_iter().collect()
    }

    /// Return all valid (non-revoked, non-expired) certificates for a zone.
    pub fn zone_members(&self, zone_id: &str) -> Vec<ZoneMembershipCertificate> {
        self.certificates
            .iter()
            .filter(|e| {
                let cert = e.value();
                cert.zone_id == zone_id
                    && !cert.is_expired()
                    && !self.revocations.contains_key(e.key())
            })
            .map(|e| e.value().clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::jurisdiction_proof::GeoResult;
    use rand::rngs::OsRng;

    // -- helpers --

    fn gen_key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    fn make_geo_result(country: CountryCode) -> GeoResult {
        GeoResult {
            country,
            region: None,
            city: None,
            latitude: 48.8566,
            longitude: 2.3522,
            provider: "test".into(),
            confidence: 0.99,
            queried_at: "2026-01-15T12:00:00Z".into(),
        }
    }

    fn make_network_proof(country: CountryCode) -> JurisdictionProof {
        JurisdictionProof::NetworkLocation {
            ip_geolocation: make_geo_result(country),
            tls_cert_locality: Some(country.as_str().to_string()),
            vdf_proof: None,
        }
    }

    fn make_national_cert_proof() -> JurisdictionProof {
        JurisdictionProof::NationalCertificate {
            certificate_der: vec![0xDE, 0xAD],
            issuer: "ANSSI".into(),
            node_id: vec![1, 2, 3],
            valid_until: "2027-01-01T00:00:00Z".into(),
        }
    }

    fn make_hw_attestation_proof() -> JurisdictionProof {
        JurisdictionProof::HardwareAttestation {
            tpm_quote: vec![0xAB],
            facility_id: "FAC-001".into(),
            platform_id: "PLAT-001".into(),
        }
    }

    fn make_physical_inspection_proof() -> JurisdictionProof {
        JurisdictionProof::PhysicalInspection {
            hardware_attestation: Box::new(make_hw_attestation_proof()),
            national_certificate: Box::new(make_national_cert_proof()),
            inspection_report_hash: [0u8; 32],
            inspector_id: "INSP-42".into(),
            inspection_date: "2026-01-10".into(),
            next_inspection_due: "2026-07-10".into(),
        }
    }

    /// Build a cert manually, sign it with `key`.
    fn make_signed_cert(
        key: &SigningKey,
        node_id: Vec<u8>,
        zone_id: &str,
        jurisdiction: CountryCode,
        zone_class: ZoneClass,
        max_classification: ClassificationLevel,
        expires_at: &str,
    ) -> ZoneMembershipCertificate {
        let mut cert = ZoneMembershipCertificate {
            node_id,
            zone_id: zone_id.to_string(),
            jurisdiction,
            zone_class,
            proof_level: 1,
            issued_at: Utc::now().to_rfc3339(),
            expires_at: expires_at.to_string(),
            proof_hash: [0u8; 32],
            authority_signature: Vec::new(),
            authority_pubkey: key.verifying_key().to_bytes().to_vec(),
            max_classification,
            requires_clearnet: false,
            kyber_ek: vec![0u8; 1184], // Mock Kyber768 public key for architectural tests
        };
        let canonical = cert.canonical_bytes();
        let sig = key.sign(&canonical);
        cert.authority_signature = sig.to_bytes().to_vec();
        cert
    }

    // ========================================================================
    // ZoneMembershipCertificate tests
    // ========================================================================

    #[test]
    fn test_cert_creation_and_verify() {
        let key = gen_key();
        let cert = make_signed_cert(
            &key,
            vec![1, 2, 3],
            "zone-fr-1",
            CountryCode::FR,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        );
        assert!(cert.verify(), "valid signature should verify");
    }

    #[test]
    fn test_cert_tampered_fails() {
        let key = gen_key();
        let mut cert = make_signed_cert(
            &key,
            vec![1, 2, 3],
            "zone-fr-1",
            CountryCode::FR,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        );
        // tamper with a field after signing
        cert.proof_level = 99;
        assert!(!cert.verify(), "tampered cert should not verify");
    }

    #[test]
    fn test_cert_expired() {
        let key = gen_key();
        let cert = make_signed_cert(
            &key,
            vec![1],
            "zone-1",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2020-01-01T00:00:00Z", // in the past
        );
        assert!(cert.is_expired());
    }

    #[test]
    fn test_cert_not_expired() {
        let key = gen_key();
        let cert = make_signed_cert(
            &key,
            vec![1],
            "zone-1",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        );
        assert!(!cert.is_expired());
    }

    #[test]
    fn test_cert_classification_check() {
        let key = gen_key();
        let cert = make_signed_cert(
            &key,
            vec![1],
            "zone-1",
            CountryCode::FR,
            ZoneClass::MilRestricted,
            ClassificationLevel::Confidential,
            "2099-01-01T00:00:00Z",
        );
        assert!(cert.authorizes_classification(ClassificationLevel::Unclassified));
        assert!(cert.authorizes_classification(ClassificationLevel::Restricted));
        assert!(cert.authorizes_classification(ClassificationLevel::Confidential));
        assert!(!cert.authorizes_classification(ClassificationLevel::Secret));
        assert!(!cert.authorizes_classification(ClassificationLevel::TopSecret));
    }

    // ========================================================================
    // ZoneAdmissionController tests
    // ========================================================================

    #[test]
    fn test_admit_level0_civilian() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-civ".into(),
            ZoneClass::Civilian,
            CountryCode::FR,
            key,
            ClassificationLevel::Unclassified,
        );
        let proof = make_network_proof(CountryCode::FR); // level 0
        let result = ctrl.admit(
            &[1, 2, 3],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Unclassified,
            vec![0x42u8; 32],
        );
        assert!(result.is_ok());
        let cert = result.unwrap();
        assert!(cert.verify());
        assert_eq!(cert.zone_class, ZoneClass::Civilian);
        assert_eq!(cert.max_classification, ClassificationLevel::Unclassified);
    }

    #[test]
    fn test_admit_level0_govcloud_restricted_fails() {
        // GovCloud + Restricted requires proof level 1, but level-0 proof
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-gov".into(),
            ZoneClass::GovCloud,
            CountryCode::FR,
            key,
            ClassificationLevel::Restricted,
        );
        let proof = make_network_proof(CountryCode::FR); // level 0
        let result = ctrl.admit(
            &[1, 2, 3],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Restricted,
            vec![0x42u8; 32],
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            AdmissionError::InsufficientProofLevel {
                required,
                provided,
            } => {
                assert_eq!(required, 1);
                assert_eq!(provided, 0);
            }
            other => panic!("expected InsufficientProofLevel, got: {:?}", other),
        }
    }

    #[test]
    fn test_admit_level1_govcloud_restricted_passes() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-gov".into(),
            ZoneClass::GovCloud,
            CountryCode::FR,
            key,
            ClassificationLevel::Restricted,
        );
        let proof = make_national_cert_proof(); // level 1
        let result = ctrl.admit(
            &[1, 2, 3],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Restricted,
            vec![0x42u8; 32],
        );
        assert!(result.is_ok());
        let cert = result.unwrap();
        assert!(cert.verify());
        assert_eq!(cert.proof_level, 1);
    }

    #[test]
    fn test_admit_jurisdiction_mismatch() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-fr".into(),
            ZoneClass::Civilian,
            CountryCode::FR,
            key,
            ClassificationLevel::Unclassified,
        );
        let proof = make_network_proof(CountryCode::DE);
        let result = ctrl.admit(
            &[1],
            &proof,
            &CountryCode::DE, // does not match zone (FR)
            ClassificationLevel::Unclassified,
            vec![0x42u8; 32],
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            AdmissionError::JurisdictionMismatch { expected, proved } => {
                assert_eq!(expected, CountryCode::FR);
                assert_eq!(proved, CountryCode::DE);
            }
            other => panic!("expected JurisdictionMismatch, got: {:?}", other),
        }
    }

    #[test]
    fn test_admit_classification_too_high() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-gov".into(),
            ZoneClass::GovCloud,
            CountryCode::FR,
            key,
            ClassificationLevel::Restricted, // zone max
        );
        let proof = make_national_cert_proof(); // level 1
        let result = ctrl.admit(
            &[1],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Secret, // above zone max
            vec![0x42u8; 32],
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            AdmissionError::ClassificationTooHigh {
                requested,
                zone_max,
            } => {
                assert_eq!(requested, ClassificationLevel::Secret);
                assert_eq!(zone_max, ClassificationLevel::Restricted);
            }
            other => panic!("expected ClassificationTooHigh, got: {:?}", other),
        }
    }

    #[test]
    fn test_admit_forbidden_combination() {
        // Civilian zone + Restricted classification => forbidden
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-civ".into(),
            ZoneClass::Civilian,
            CountryCode::FR,
            key,
            ClassificationLevel::Restricted, // artificially high, but matrix says no
        );
        let proof = make_national_cert_proof(); // level 1
        let result = ctrl.admit(
            &[1],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Restricted,
            vec![0x42u8; 32],
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            AdmissionError::ForbiddenCombination => {}
            other => panic!("expected ForbiddenCombination, got: {:?}", other),
        }
    }

    #[test]
    fn test_admit_already_member() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-civ".into(),
            ZoneClass::Civilian,
            CountryCode::FR,
            key,
            ClassificationLevel::Unclassified,
        );
        let proof = make_network_proof(CountryCode::FR);
        ctrl.admit(
            &[1],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Unclassified,
            vec![0x42u8; 32],
        )
        .unwrap();

        // second admission should fail
        let result = ctrl.admit(
            &[1],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Unclassified,
            vec![0x42u8; 32],
        );
        assert!(matches!(result, Err(AdmissionError::AlreadyMember)));
    }

    #[test]
    fn test_revoke_membership() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-civ".into(),
            ZoneClass::Civilian,
            CountryCode::FR,
            key,
            ClassificationLevel::Unclassified,
        );
        let proof = make_network_proof(CountryCode::FR);
        ctrl.admit(
            &[1],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Unclassified,
            vec![0x42u8; 32],
        )
        .unwrap();

        assert!(ctrl.is_member(&[1]));

        let rev = ctrl.revoke(&[1], "policy change").unwrap();
        assert_eq!(rev.zone_id, "zone-civ");
        assert_eq!(rev.reason, "policy change");
        assert!(!ctrl.is_member(&[1]));
    }

    #[test]
    fn test_revoke_nonexistent_node() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-civ".into(),
            ZoneClass::Civilian,
            CountryCode::FR,
            key,
            ClassificationLevel::Unclassified,
        );
        let result = ctrl.revoke(&[99], "reason");
        assert!(matches!(result, Err(AdmissionError::NodeNotFound)));
    }

    #[test]
    fn test_controller_cleanup_expired() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-civ".into(),
            ZoneClass::Civilian,
            CountryCode::FR,
            key.clone(),
            ClassificationLevel::Unclassified,
        );

        // Insert an already-expired cert directly into the map.
        let expired_cert = ZoneMembershipCertificate {
            node_id: vec![99],
            zone_id: "zone-civ".into(),
            jurisdiction: CountryCode::FR,
            zone_class: ZoneClass::Civilian,
            proof_level: 0,
            issued_at: "2020-01-01T00:00:00Z".into(),
            expires_at: "2020-06-01T00:00:00Z".into(),
            proof_hash: [0u8; 32],
            authority_signature: vec![0u8; 64],
            authority_pubkey: key.verifying_key().to_bytes().to_vec(),
            max_classification: ClassificationLevel::Unclassified,
            requires_clearnet: false,
            kyber_ek: vec![0x42u8; 32],
        };
        ctrl.issued_certs.insert(vec![99], expired_cert);

        assert_eq!(ctrl.cleanup_expired(), 1);
        assert!(!ctrl.is_member(&[99]));
    }

    #[test]
    fn test_admit_milclassified_secret_level3() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-milc".into(),
            ZoneClass::MilClassified,
            CountryCode::FR,
            key,
            ClassificationLevel::TopSecret,
        );
        let proof = make_physical_inspection_proof(); // level 3
        let result = ctrl.admit(
            &[10],
            &proof,
            &CountryCode::FR,
            ClassificationLevel::Secret,
            vec![0x42u8; 32],
        );
        assert!(result.is_ok());
        let cert = result.unwrap();
        assert!(cert.verify());
        assert_eq!(cert.proof_level, 3);
        assert_eq!(cert.max_classification, ClassificationLevel::Secret);
    }

    #[test]
    fn test_admit_milrestricted_confidential_level2() {
        let key = gen_key();
        let ctrl = ZoneAdmissionController::new(
            "zone-milr".into(),
            ZoneClass::MilRestricted,
            CountryCode::DE,
            key,
            ClassificationLevel::Confidential,
        );
        let proof = make_hw_attestation_proof(); // level 2
        let result = ctrl.admit(
            &[20],
            &proof,
            &CountryCode::DE,
            ClassificationLevel::Confidential,
            vec![0x42u8; 32],
        );
        assert!(result.is_ok());
        let cert = result.unwrap();
        assert!(cert.verify());
        assert_eq!(cert.proof_level, 2);
    }

    // ========================================================================
    // ZoneCertificateStore tests
    // ========================================================================

    #[test]
    fn test_store_upsert_and_get() {
        let key = gen_key();
        let store = ZoneCertificateStore::new();
        let cert = make_signed_cert(
            &key,
            vec![1],
            "zone-1",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        );
        store.upsert(cert.clone());
        let got = store.get_valid(&[1]);
        assert!(got.is_some());
        assert_eq!(got.unwrap().zone_id, "zone-1");
    }

    #[test]
    fn test_store_revocation_overrides() {
        let key = gen_key();
        let store = ZoneCertificateStore::new();
        let cert = make_signed_cert(
            &key,
            vec![1],
            "zone-1",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        );
        store.upsert(cert);

        let revocation = ZoneRevocation {
            node_id: vec![1],
            zone_id: "zone-1".into(),
            reason: "compromised".into(),
            revoked_at: Utc::now().to_rfc3339(),
            authority_signature: vec![0u8; 64],
            authority_pubkey: key.verifying_key().to_bytes().to_vec(),
        };
        store.revoke(revocation);

        assert!(store.get_valid(&[1]).is_none());
    }

    #[test]
    fn test_store_is_authorized() {
        let key = gen_key();
        let store = ZoneCertificateStore::new();
        let cert = make_signed_cert(
            &key,
            vec![5],
            "zone-gov",
            CountryCode::DE,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        );
        store.upsert(cert);

        assert!(store.is_authorized(&[5], "zone-gov", ClassificationLevel::Unclassified));
        assert!(store.is_authorized(&[5], "zone-gov", ClassificationLevel::Restricted));
        assert!(!store.is_authorized(&[5], "zone-gov", ClassificationLevel::Secret));
        assert!(!store.is_authorized(&[5], "wrong-zone", ClassificationLevel::Unclassified));
        assert!(!store.is_authorized(&[99], "zone-gov", ClassificationLevel::Unclassified));
    }

    #[test]
    fn test_store_cleanup_expired() {
        let key = gen_key();
        let store = ZoneCertificateStore::new();

        // valid cert
        store.upsert(make_signed_cert(
            &key,
            vec![1],
            "z",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        ));

        // expired cert
        store.upsert(make_signed_cert(
            &key,
            vec![2],
            "z",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2020-01-01T00:00:00Z",
        ));

        assert_eq!(store.cleanup_expired(), 1);
        assert!(store.get_valid(&[1]).is_some());
        // node 2's cert was removed
        assert!(store.get_valid(&[2]).is_none());
    }

    #[test]
    fn test_store_zone_members() {
        let key = gen_key();
        let store = ZoneCertificateStore::new();

        store.upsert(make_signed_cert(
            &key,
            vec![1],
            "zone-a",
            CountryCode::FR,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        ));
        store.upsert(make_signed_cert(
            &key,
            vec![2],
            "zone-a",
            CountryCode::DE,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        ));
        store.upsert(make_signed_cert(
            &key,
            vec![3],
            "zone-b",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        ));

        let members_a = store.zone_members("zone-a");
        assert_eq!(members_a.len(), 2);

        let members_b = store.zone_members("zone-b");
        assert_eq!(members_b.len(), 1);
        assert_eq!(members_b[0].node_id, vec![3]);
    }

    #[test]
    fn test_store_zone_members_excludes_revoked() {
        let key = gen_key();
        let store = ZoneCertificateStore::new();

        store.upsert(make_signed_cert(
            &key,
            vec![1],
            "zone-a",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        ));
        store.upsert(make_signed_cert(
            &key,
            vec![2],
            "zone-a",
            CountryCode::FR,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            "2099-01-01T00:00:00Z",
        ));

        store.revoke(ZoneRevocation {
            node_id: vec![2],
            zone_id: "zone-a".into(),
            reason: "revoked".into(),
            revoked_at: Utc::now().to_rfc3339(),
            authority_signature: vec![0u8; 64],
            authority_pubkey: key.verifying_key().to_bytes().to_vec(),
        });

        let members = store.zone_members("zone-a");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].node_id, vec![1]);
    }

    #[test]
    fn test_cert_serialization_roundtrip() {
        let key = gen_key();
        let cert = make_signed_cert(
            &key,
            vec![1, 2, 3],
            "zone-fr-1",
            CountryCode::FR,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            "2099-01-01T00:00:00Z",
        );
        let json = serde_json::to_string(&cert).unwrap();
        let deserialized: ZoneMembershipCertificate = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.zone_id, cert.zone_id);
        assert_eq!(deserialized.node_id, cert.node_id);
        assert!(deserialized.verify());
    }
}
