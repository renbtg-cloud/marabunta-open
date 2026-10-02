// Marabunta - Licensed under the MIT License.
use std::collections::HashMap;
use std::sync::Arc;

use crate::highestsec::classification::{
    HandlingRestriction, ClassificationLevel, ClassificationRequirements, Compartment, HandlingHandlingRestriction,
    ReleasabilityMarking,
};
use crate::highestsec::jurisdiction::ZoneClass;
use crate::highestsec::zone_membership::ZoneCertificateStore;

// ---------------------------------------------------------------------------
// ClassificationEngine
// ---------------------------------------------------------------------------

/// Enforces classification-level requirements against node capabilities,
/// cryptographic parameters, and zone memberships.
pub struct ClassificationEngine {
    requirements: HashMap<ClassificationLevel, ClassificationRequirements>,
    cert_store: Arc<ZoneCertificateStore>,
}

impl ClassificationEngine {
    /// Create a new engine pre-loaded with default requirements for every
    /// classification level and backed by the given certificate store.
    pub fn new(cert_store: Arc<ZoneCertificateStore>) -> Self {
        let mut requirements = HashMap::new();
        let levels = [
            ClassificationLevel::Unclassified,
            ClassificationLevel::Restricted,
            ClassificationLevel::Confidential,
            ClassificationLevel::Secret,
            ClassificationLevel::TopSecret,
        ];
        for level in levels {
            requirements.insert(level, ClassificationRequirements::defaults_for(level));
        }
        Self {
            requirements,
            cert_store,
        }
    }

    /// Replace the requirements for a specific classification level.
    pub fn override_requirements(
        &mut self,
        level: ClassificationLevel,
        reqs: ClassificationRequirements,
    ) {
        self.requirements.insert(level, reqs);
    }

    /// Verify that a node is permitted to handle data at the given
    /// classification level.
    ///
    /// Checks performed (in order):
    /// 1. Node must have a valid zone certificate.
    /// 2. Certificate zone_class >= required min_zone_class.
    /// 3. Certificate proof_level >= required min_proof_level.
    /// 4. Certificate max_classification >= data_classification.
    /// 5. TEMPEST requirement: if required and zone_class < MilClassified.
    /// 6. Airgap requirement: if required and zone_class < MilClassified.
    /// 7. HSM requirement: if required and zone_class < MilClassified.
    pub fn check_node(
        &self,
        node_id: &[u8],
        data_classification: ClassificationLevel,
    ) -> Result<(), ClassificationError> {
        let cert = self
            .cert_store
            .get_valid(node_id)
            .ok_or(ClassificationError::NodeNotInZone)?;

        let reqs = self.requirements_for(data_classification);

        // Zone class check.
        if cert.zone_class < reqs.min_zone_class {
            return Err(ClassificationError::InsufficientZoneClass {
                required: reqs.min_zone_class,
                actual: cert.zone_class,
            });
        }

        // Proof level check.
        if cert.proof_level < reqs.min_proof_level {
            return Err(ClassificationError::InsufficientProofLevel {
                required: reqs.min_proof_level,
                actual: cert.proof_level,
            });
        }

        // Max classification check.
        if cert.max_classification < data_classification {
            return Err(ClassificationError::ClassificationExceedsNodeMax {
                data: data_classification,
                node_max: cert.max_classification,
            });
        }

        // TEMPEST requirement (simplified: zone_class < MilClassified means TEMPEST
        // cannot be guaranteed by infrastructure alone).
        if reqs.tempest_required && cert.zone_class < ZoneClass::MilClassified {
            return Err(ClassificationError::TempestRequired);
        }

        // Airgap requirement.
        if reqs.airgap_required && cert.zone_class < ZoneClass::MilClassified {
            return Err(ClassificationError::AirgapRequired);
        }

        // HSM requirement.
        if reqs.hsm_required && cert.zone_class < ZoneClass::MilClassified {
            return Err(ClassificationError::HsmRequired);
        }

        Ok(())
    }

    /// Verify that cryptographic parameters meet the requirements for the
    /// given classification level.
    pub fn check_crypto(
        &self,
        data_classification: ClassificationLevel,
        _symmetric_algorithm: &str,
        symmetric_key_bits: u32,
        _asymmetric_algorithm: &str,
        pq_in_use: bool,
    ) -> Result<(), ClassificationError> {
        let reqs = self.requirements_for(data_classification);

        if symmetric_key_bits < reqs.min_symmetric_key_bits {
            return Err(ClassificationError::InsufficientKeySize {
                required: reqs.min_symmetric_key_bits,
                actual: symmetric_key_bits,
            });
        }

        if reqs.pq_required && !pq_in_use {
            return Err(ClassificationError::PqRequired);
        }

        Ok(())
    }

    /// Return the requirements for a given classification level.
    /// Falls back to `Unclassified` defaults if the level is not configured.
    pub fn requirements_for(&self, level: ClassificationLevel) -> &ClassificationRequirements {
        self.requirements
            .get(&level)
            .unwrap_or_else(|| {
                self.requirements
                    .get(&ClassificationLevel::Unclassified)
                    .expect("Unclassified requirements must always be present")
            })
    }
}

// ---------------------------------------------------------------------------
// ClassificationError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ClassificationError {
    InsufficientZoneClass {
        required: ZoneClass,
        actual: ZoneClass,
    },
    InsufficientProofLevel {
        required: u8,
        actual: u8,
    },
    InvalidZoneCertificate,
    ClassificationExceedsNodeMax {
        data: ClassificationLevel,
        node_max: ClassificationLevel,
    },
    TempestRequired,
    AirgapRequired,
    HsmRequired,
    InsufficientKeySize {
        required: u32,
        actual: u32,
    },
    PqRequired,
    AlgorithmNotAllowed(String),
    NodeNotInZone,
}

impl std::fmt::Display for ClassificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
            Self::InvalidZoneCertificate => write!(f, "invalid zone certificate"),
            Self::ClassificationExceedsNodeMax { data, node_max } => {
                write!(
                    f,
                    "data classification {:?} exceeds node max {:?}",
                    data, node_max
                )
            }
            Self::TempestRequired => write!(f, "TEMPEST shielding required"),
            Self::AirgapRequired => write!(f, "airgap required"),
            Self::HsmRequired => write!(f, "HSM required"),
            Self::InsufficientKeySize { required, actual } => {
                write!(
                    f,
                    "insufficient key size: required {} bits, actual {} bits",
                    required, actual
                )
            }
            Self::PqRequired => write!(f, "post-quantum cryptography required"),
            Self::AlgorithmNotAllowed(alg) => write!(f, "algorithm not allowed: {}", alg),
            Self::NodeNotInZone => write!(f, "node has no valid zone certificate"),
        }
    }
}

// ---------------------------------------------------------------------------
// Output classification computation
// ---------------------------------------------------------------------------

/// Compute the output classification from multiple input handling_restrictions.
///
/// Rules:
/// - Level: MAX of all input levels.
/// - Releasability: INTERSECTION of all input markings.
/// - Compartments: UNION of all input compartments (dedup by id).
/// - Handling: MOST RESTRICTIVE (union of all unique handling_restrictions).
pub fn compute_output_classification(inputs: &[HandlingRestriction]) -> HandlingRestriction {
    if inputs.is_empty() {
        return HandlingRestriction {
            level: ClassificationLevel::Unclassified,
            releasable_to: ReleasabilityMarking::NoForn,
            compartments: Vec::new(),
            handling: Vec::new(),
        };
    }

    // MAX level.
    let level = inputs
        .iter()
        .map(|c| c.level)
        .max()
        .unwrap_or(ClassificationLevel::Unclassified);

    // INTERSECTION of releasability markings.
    let marking_refs: Vec<&ReleasabilityMarking> =
        inputs.iter().map(|c| &c.releasable_to).collect();
    let releasable_to = intersect_releasability(&marking_refs);

    // UNION of compartments (dedup by id).
    let mut seen_ids: Vec<String> = Vec::new();
    let mut compartments: Vec<Compartment> = Vec::new();
    for input in inputs {
        for comp in &input.compartments {
            if !seen_ids.contains(&comp.id) {
                seen_ids.push(comp.id.clone());
                compartments.push(comp.clone());
            }
        }
    }

    // MOST RESTRICTIVE handling (union of all unique handling_restrictions).
    let handling_refs: Vec<&HandlingHandlingRestriction> = inputs.iter().flat_map(|c| &c.handling).collect();
    let handling = most_restrictive_handling(&handling_refs);

    HandlingRestriction {
        level,
        releasable_to,
        compartments,
        handling,
    }
}

/// Intersect releasability markings.
///
/// - RelTo(A) intersect RelTo(B) = RelTo(A & B)
/// - NoForn intersect anything = NoForn
/// - EyesOnly(A) intersect EyesOnly(B) = EyesOnly(A & B)
/// - EyesOnly(A) intersect RelTo(B) = EyesOnly(A & B)
/// - OrCon intersect anything = OrCon (requires originator approval)
/// - RelToOrg intersect RelTo = filtered by org members
/// - Incompatible combinations default to NoForn (most restrictive).
fn intersect_releasability(markings: &[&ReleasabilityMarking]) -> ReleasabilityMarking {
    if markings.is_empty() {
        return ReleasabilityMarking::NoForn;
    }
    if markings.len() == 1 {
        return (*markings[0]).clone();
    }

    let mut result = markings[0].clone();

    for marking in &markings[1..] {
        result = intersect_two(&result, marking);
    }

    result
}

/// Intersect two releasability markings.
fn intersect_two(a: &ReleasabilityMarking, b: &ReleasabilityMarking) -> ReleasabilityMarking {
    use ReleasabilityMarking::*;

    match (a, b) {
        // NoForn is maximally restrictive.
        (NoForn, _) | (_, NoForn) => NoForn,

        // OrCon is very restrictive.
        (OrCon, _) | (_, OrCon) => OrCon,

        // Two RelTo lists: intersection of countries.
        (RelTo(a_countries), RelTo(b_countries)) => {
            let intersection: Vec<_> = a_countries
                .iter()
                .filter(|c| b_countries.contains(c))
                .copied()
                .collect();
            if intersection.is_empty() {
                NoForn
            } else {
                RelTo(intersection)
            }
        }

        // Two EyesOnly lists: intersection of countries.
        (EyesOnly(a_countries), EyesOnly(b_countries)) => {
            let intersection: Vec<_> = a_countries
                .iter()
                .filter(|c| b_countries.contains(c))
                .copied()
                .collect();
            if intersection.is_empty() {
                NoForn
            } else {
                EyesOnly(intersection)
            }
        }

        // EyesOnly intersect RelTo: EyesOnly filtered to common countries.
        (EyesOnly(eyes), RelTo(rel)) | (RelTo(rel), EyesOnly(eyes)) => {
            let intersection: Vec<_> = eyes
                .iter()
                .filter(|c| rel.contains(c))
                .copied()
                .collect();
            if intersection.is_empty() {
                NoForn
            } else {
                EyesOnly(intersection)
            }
        }

        // RelToOrg intersect RelTo: cannot resolve without a AllianceResolver at
        // this level, so fall back to most restrictive.
        (RelToOrg(_), RelTo(_)) | (RelTo(_), RelToOrg(_)) => NoForn,

        // Two RelToOrg: fall back to NoForn if different orgs.
        (RelToOrg(org_a), RelToOrg(org_b)) => {
            if org_a == org_b {
                RelToOrg(org_a.clone())
            } else {
                NoForn
            }
        }

        // EyesOnly intersect RelToOrg: cannot resolve, fall back.
        (EyesOnly(_), RelToOrg(_)) | (RelToOrg(_), EyesOnly(_)) => NoForn,
    }
}

/// Collect the most restrictive (union of unique) handling handling_restrictions.
///
/// `CryptoMarked`, `Atomal`, `Bohemia` are singletons (appear at most once).
/// `SpecialCategory(name)` is deduplicated by its inner string.
fn most_restrictive_handling(handling_restrictions: &[&HandlingHandlingRestriction]) -> Vec<HandlingHandlingRestriction> {
    let mut has_crypto = false;
    let mut has_atomal = false;
    let mut has_bohemia = false;
    let mut special_categories: Vec<String> = Vec::new();

    for handling_restriction in handling_restrictions {
        match handling_restriction {
            HandlingHandlingRestriction::CryptoMarked => has_crypto = true,
            HandlingHandlingRestriction::Atomal => has_atomal = true,
            HandlingHandlingRestriction::Bohemia => has_bohemia = true,
            HandlingHandlingRestriction::SpecialCategory(name) => {
                if !special_categories.contains(name) {
                    special_categories.push(name.clone());
                }
            }
        }
    }

    let mut result = Vec::new();
    for name in special_categories {
        result.push(HandlingHandlingRestriction::SpecialCategory(name));
    }
    if has_crypto {
        result.push(HandlingHandlingRestriction::CryptoMarked);
    }
    if has_atomal {
        result.push(HandlingHandlingRestriction::Atomal);
    }
    if has_bohemia {
        result.push(HandlingHandlingRestriction::Bohemia);
    }
    result
}

// ---------------------------------------------------------------------------
// KeyLifetimeEnforcer
// ---------------------------------------------------------------------------

/// Enforces key rotation schedules based on classification level requirements.
pub struct KeyLifetimeEnforcer {
    requirements: HashMap<ClassificationLevel, ClassificationRequirements>,
}

impl KeyLifetimeEnforcer {
    /// Create a new enforcer pre-loaded with default requirements.
    pub fn new() -> Self {
        let mut requirements = HashMap::new();
        let levels = [
            ClassificationLevel::Unclassified,
            ClassificationLevel::Restricted,
            ClassificationLevel::Confidential,
            ClassificationLevel::Secret,
            ClassificationLevel::TopSecret,
        ];
        for level in levels {
            requirements.insert(level, ClassificationRequirements::defaults_for(level));
        }
        Self { requirements }
    }

    /// Return the maximum key lifetime for a given classification level.
    pub fn max_lifetime(&self, classification: ClassificationLevel) -> std::time::Duration {
        let hours = self
            .requirements
            .get(&classification)
            .map(|r| r.max_key_lifetime_hours)
            .unwrap_or(8760); // Default: 1 year
        std::time::Duration::from_secs(u64::from(hours) * 3600)
    }

    /// Determine whether a key should be rotated, given its creation time
    /// as an ISO 8601 / RFC 3339 string.
    pub fn should_rotate(
        &self,
        classification: ClassificationLevel,
        key_created_at: &str,
    ) -> bool {
        use chrono::{DateTime, Utc};

        let Ok(created) = key_created_at.parse::<DateTime<Utc>>() else {
            // Unparseable dates are treated as needing rotation.
            return true;
        };

        let now = Utc::now();
        let elapsed = now.signed_duration_since(created);

        let max_hours = self
            .requirements
            .get(&classification)
            .map(|r| r.max_key_lifetime_hours)
            .unwrap_or(8760);

        let max_duration = chrono::Duration::hours(i64::from(max_hours));
        elapsed >= max_duration
    }
}

impl Default for KeyLifetimeEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::classification::{
        HandlingRestriction, ClassificationLevel, Compartment, HandlingHandlingRestriction, ReleasabilityMarking,
    };
    use crate::highestsec::jurisdiction::{NationalAuthority, ZoneClass};
    use crate::highestsec::types::CountryCode;
    use crate::highestsec::zone_membership::{ZoneCertificateStore, ZoneMembershipCertificate};
    use std::sync::Arc;

    /// Helper: build a certificate store with one node at the given parameters.
    fn make_cert_store(
        node_id: &[u8],
        zone_class: ZoneClass,
        max_classification: ClassificationLevel,
        proof_level: u8,
    ) -> Arc<ZoneCertificateStore> {
        let store = ZoneCertificateStore::new();
        let cert = ZoneMembershipCertificate {
            node_id: node_id.to_vec(),
            zone_id: "test-zone".into(),
            jurisdiction: CountryCode::FR,
            zone_class,
            max_classification,
            proof_level,
            issued_at: chrono::Utc::now().to_rfc3339(),
            expires_at: (chrono::Utc::now() + chrono::Duration::days(365)).to_rfc3339(),
            proof_hash: [0u8; 32],
            authority_signature: vec![0u8; 64],
            authority_pubkey: vec![0u8; 32],
            requires_clearnet: false,
            kyber_ek: vec![0x42u8; 32],
        };
        store.upsert(cert);
        Arc::new(store)
    }

    // ---- check_node tests ----

    #[test]
    fn test_check_node_unclassified_on_civilian() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(
            &node_id,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
            0,
        );
        let engine = ClassificationEngine::new(store);
        assert!(engine
            .check_node(&node_id, ClassificationLevel::Unclassified)
            .is_ok());
    }

    #[test]
    fn test_check_node_restricted_on_govcloud() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(
            &node_id,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            1,
        );
        let engine = ClassificationEngine::new(store);
        assert!(engine
            .check_node(&node_id, ClassificationLevel::Restricted)
            .is_ok());
    }

    #[test]
    fn test_check_node_secret_requires_milclassified() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(
            &node_id,
            ZoneClass::MilRestricted,
            ClassificationLevel::Secret,
            3,
        );
        let engine = ClassificationEngine::new(store);
        let result = engine.check_node(&node_id, ClassificationLevel::Secret);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::InsufficientZoneClass { .. }
        ));
    }

    #[test]
    fn test_check_node_secret_on_milclassified() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(
            &node_id,
            ZoneClass::MilClassified,
            ClassificationLevel::Secret,
            3,
        );
        let engine = ClassificationEngine::new(store);
        assert!(engine
            .check_node(&node_id, ClassificationLevel::Secret)
            .is_ok());
    }

    #[test]
    fn test_check_node_classification_exceeds_max() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(
            &node_id,
            ZoneClass::MilClassified,
            ClassificationLevel::Restricted, // max is Restricted
            3,
        );
        let engine = ClassificationEngine::new(store);
        let result = engine.check_node(&node_id, ClassificationLevel::Secret);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::ClassificationExceedsNodeMax { .. }
        ));
    }

    #[test]
    fn test_check_node_insufficient_proof_level() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(
            &node_id,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
            0, // proof level 0, but Restricted requires 1
        );
        let engine = ClassificationEngine::new(store);
        let result = engine.check_node(&node_id, ClassificationLevel::Restricted);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::InsufficientProofLevel { required: 1, actual: 0 }
        ));
    }

    #[test]
    fn test_check_node_not_in_zone() {
        let store = Arc::new(ZoneCertificateStore::new());
        let engine = ClassificationEngine::new(store);
        let result = engine.check_node(&[9, 9, 9], ClassificationLevel::Unclassified);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::NodeNotInZone
        ));
    }

    #[test]
    fn test_check_node_tempest_required() {
        let node_id = vec![1, 2, 3];
        // Secret requires TEMPEST, zone MilRestricted < MilClassified triggers error.
        // But Secret also requires MilClassified zone, so this would fail earlier.
        // Use override to test TEMPEST specifically.
        let store = make_cert_store(
            &node_id,
            ZoneClass::MilRestricted,
            ClassificationLevel::Confidential,
            2,
        );
        let mut engine = ClassificationEngine::new(store);
        // Override Confidential to require TEMPEST.
        let mut reqs = ClassificationRequirements::defaults_for(ClassificationLevel::Confidential);
        reqs.tempest_required = true;
        engine.override_requirements(ClassificationLevel::Confidential, reqs);

        let result = engine.check_node(&node_id, ClassificationLevel::Confidential);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::TempestRequired
        ));
    }

    #[test]
    fn test_check_node_airgap_on_milclassified_passes() {
        let node_id = vec![1, 2, 3];
        // TopSecret requires airgap, but MilClassified satisfies it.
        let store = make_cert_store(
            &node_id,
            ZoneClass::MilClassified,
            ClassificationLevel::TopSecret,
            3,
        );
        let engine = ClassificationEngine::new(store);
        assert!(engine
            .check_node(&node_id, ClassificationLevel::TopSecret)
            .is_ok());
    }

    // ---- check_crypto tests ----

    #[test]
    fn test_check_crypto_unclassified_128bit() {
        let store = Arc::new(ZoneCertificateStore::new());
        let engine = ClassificationEngine::new(store);
        assert!(engine
            .check_crypto(
                ClassificationLevel::Unclassified,
                "AES-GCM",
                128,
                "RSA-2048",
                false,
            )
            .is_ok());
    }

    #[test]
    fn test_check_crypto_secret_requires_256bit() {
        let store = Arc::new(ZoneCertificateStore::new());
        let engine = ClassificationEngine::new(store);
        let result = engine.check_crypto(
            ClassificationLevel::Secret,
            "AES-GCM",
            128, // Only 128, but Secret requires 256
            "RSA-4096",
            true,
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::InsufficientKeySize {
                required: 256,
                actual: 128
            }
        ));
    }

    #[test]
    fn test_check_crypto_confidential_requires_pq() {
        let store = Arc::new(ZoneCertificateStore::new());
        let engine = ClassificationEngine::new(store);
        let result = engine.check_crypto(
            ClassificationLevel::Confidential,
            "AES-GCM",
            256,
            "RSA-3072",
            false, // PQ not in use, but Confidential requires it
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ClassificationError::PqRequired
        ));
    }

    #[test]
    fn test_check_crypto_secret_with_pq_passes() {
        let store = Arc::new(ZoneCertificateStore::new());
        let engine = ClassificationEngine::new(store);
        assert!(engine
            .check_crypto(
                ClassificationLevel::Secret,
                "AES-256-GCM",
                256,
                "Kyber-1024",
                true,
            )
            .is_ok());
    }

    #[test]
    fn test_check_crypto_restricted_no_pq_needed() {
        let store = Arc::new(ZoneCertificateStore::new());
        let engine = ClassificationEngine::new(store);
        assert!(engine
            .check_crypto(
                ClassificationLevel::Restricted,
                "AES-GCM",
                128,
                "RSA-3072",
                false,
            )
            .is_ok());
    }

    // ---- compute_output_classification tests ----

    fn make_handling_restriction(
        level: ClassificationLevel,
        releasable_to: ReleasabilityMarking,
        compartments: Vec<Compartment>,
        handling: Vec<HandlingHandlingRestriction>,
    ) -> HandlingRestriction {
        HandlingRestriction {
            level,
            releasable_to,
            compartments,
            handling,
        }
    }

    fn make_compartment(id: &str) -> Compartment {
        Compartment {
            id: id.into(),
            controlling_authority: NationalAuthority {
                id: "auth-1".into(),
                country: CountryCode::FR,
                public_key: vec![0u8; 32],
                name: "Test Authority".into(),
            },
        }
    }

    #[test]
    fn test_output_classification_max_level() {
        let inputs = vec![
            make_handling_restriction(
                ClassificationLevel::Restricted,
                ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
                vec![],
                vec![],
            ),
            make_handling_restriction(
                ClassificationLevel::Secret,
                ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
                vec![],
                vec![],
            ),
        ];
        let output = compute_output_classification(&inputs);
        assert_eq!(output.level, ClassificationLevel::Secret);
    }

    #[test]
    fn test_output_classification_releasability_intersection() {
        let inputs = vec![
            make_handling_restriction(
                ClassificationLevel::Restricted,
                ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
                vec![],
                vec![],
            ),
            make_handling_restriction(
                ClassificationLevel::Restricted,
                ReleasabilityMarking::RelTo(vec![CountryCode::DE, CountryCode::ES]),
                vec![],
                vec![],
            ),
        ];
        let output = compute_output_classification(&inputs);
        match &output.releasable_to {
            ReleasabilityMarking::RelTo(countries) => {
                assert_eq!(countries.len(), 1);
                assert_eq!(countries[0], CountryCode::DE);
            }
            other => panic!("expected RelTo, got {:?}", other),
        }
    }

    #[test]
    fn test_output_classification_noforn_dominates() {
        let inputs = vec![
            make_handling_restriction(
                ClassificationLevel::Secret,
                ReleasabilityMarking::NoForn,
                vec![],
                vec![],
            ),
            make_handling_restriction(
                ClassificationLevel::Restricted,
                ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
                vec![],
                vec![],
            ),
        ];
        let output = compute_output_classification(&inputs);
        assert!(matches!(output.releasable_to, ReleasabilityMarking::NoForn));
    }

    #[test]
    fn test_output_classification_compartments_union() {
        let inputs = vec![
            make_handling_restriction(
                ClassificationLevel::Secret,
                ReleasabilityMarking::NoForn,
                vec![make_compartment("SCI-A")],
                vec![],
            ),
            make_handling_restriction(
                ClassificationLevel::Secret,
                ReleasabilityMarking::NoForn,
                vec![make_compartment("SCI-B"), make_compartment("SCI-A")],
                vec![],
            ),
        ];
        let output = compute_output_classification(&inputs);
        assert_eq!(output.compartments.len(), 2);
        let ids: Vec<&str> = output.compartments.iter().map(|c| c.id.as_str()).collect();
        assert!(ids.contains(&"SCI-A"));
        assert!(ids.contains(&"SCI-B"));
    }

    #[test]
    fn test_output_classification_handling_union() {
        let inputs = vec![
            make_handling_restriction(
                ClassificationLevel::Secret,
                ReleasabilityMarking::NoForn,
                vec![],
                vec![HandlingHandlingRestriction::CryptoMarked],
            ),
            make_handling_restriction(
                ClassificationLevel::Secret,
                ReleasabilityMarking::NoForn,
                vec![],
                vec![HandlingHandlingRestriction::Atomal, HandlingHandlingRestriction::CryptoMarked],
            ),
            make_handling_restriction(
                ClassificationLevel::Secret,
                ReleasabilityMarking::NoForn,
                vec![],
                vec![HandlingHandlingRestriction::Bohemia],
            ),
        ];
        let output = compute_output_classification(&inputs);
        // Should have CryptoMarked, Atomal, and Bohemia (each once).
        assert_eq!(output.handling.len(), 3);

        let has_crypto = output
            .handling
            .iter()
            .any(|h| matches!(h, HandlingHandlingRestriction::CryptoMarked));
        let has_atomal = output
            .handling
            .iter()
            .any(|h| matches!(h, HandlingHandlingRestriction::Atomal));
        let has_bohemia = output
            .handling
            .iter()
            .any(|h| matches!(h, HandlingHandlingRestriction::Bohemia));
        assert!(has_crypto);
        assert!(has_atomal);
        assert!(has_bohemia);
    }

    #[test]
    fn test_output_classification_special_category_dedup() {
        let inputs = vec![
            make_handling_restriction(
                ClassificationLevel::Restricted,
                ReleasabilityMarking::NoForn,
                vec![],
                vec![HandlingHandlingRestriction::SpecialCategory("GAMMA".into())],
            ),
            make_handling_restriction(
                ClassificationLevel::Restricted,
                ReleasabilityMarking::NoForn,
                vec![],
                vec![
                    HandlingHandlingRestriction::SpecialCategory("GAMMA".into()),
                    HandlingHandlingRestriction::SpecialCategory("DELTA".into()),
                ],
            ),
        ];
        let output = compute_output_classification(&inputs);
        let special_count = output
            .handling
            .iter()
            .filter(|h| matches!(h, HandlingHandlingRestriction::SpecialCategory(_)))
            .count();
        assert_eq!(special_count, 2); // GAMMA and DELTA, not duplicated
    }

    #[test]
    fn test_output_classification_empty_inputs() {
        let output = compute_output_classification(&[]);
        assert_eq!(output.level, ClassificationLevel::Unclassified);
        assert!(output.compartments.is_empty());
        assert!(output.handling.is_empty());
    }

    #[test]
    fn test_output_classification_single_input() {
        let input = make_handling_restriction(
            ClassificationLevel::Confidential,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR]),
            vec![make_compartment("COMP-X")],
            vec![HandlingHandlingRestriction::Atomal],
        );
        let output = compute_output_classification(&[input]);
        assert_eq!(output.level, ClassificationLevel::Confidential);
        assert_eq!(output.compartments.len(), 1);
        assert_eq!(output.handling.len(), 1);
    }

    #[test]
    fn test_intersect_eyes_only_and_relto() {
        let a = ReleasabilityMarking::EyesOnly(vec![CountryCode::FR, CountryCode::DE]);
        let b = ReleasabilityMarking::RelTo(vec![CountryCode::DE, CountryCode::ES]);
        let result = intersect_two(&a, &b);
        match result {
            ReleasabilityMarking::EyesOnly(countries) => {
                assert_eq!(countries.len(), 1);
                assert_eq!(countries[0], CountryCode::DE);
            }
            other => panic!("expected EyesOnly, got {:?}", other),
        }
    }

    #[test]
    fn test_intersect_disjoint_becomes_noforn() {
        let a = ReleasabilityMarking::RelTo(vec![CountryCode::FR]);
        let b = ReleasabilityMarking::RelTo(vec![CountryCode::DE]);
        let result = intersect_two(&a, &b);
        assert!(matches!(result, ReleasabilityMarking::NoForn));
    }

    #[test]
    fn test_intersect_orcon_dominates() {
        let a = ReleasabilityMarking::OrCon;
        let b = ReleasabilityMarking::RelTo(vec![CountryCode::FR]);
        let result = intersect_two(&a, &b);
        assert!(matches!(result, ReleasabilityMarking::OrCon));
    }

    // ---- KeyLifetimeEnforcer tests ----

    #[test]
    fn test_key_lifetime_unclassified() {
        let enforcer = KeyLifetimeEnforcer::new();
        let lifetime = enforcer.max_lifetime(ClassificationLevel::Unclassified);
        // 8760 hours = 365 days.
        assert_eq!(lifetime, std::time::Duration::from_secs(8760 * 3600));
    }

    #[test]
    fn test_key_lifetime_top_secret() {
        let enforcer = KeyLifetimeEnforcer::new();
        let lifetime = enforcer.max_lifetime(ClassificationLevel::TopSecret);
        // 8 hours.
        assert_eq!(lifetime, std::time::Duration::from_secs(8 * 3600));
    }

    #[test]
    fn test_key_lifetime_secret() {
        let enforcer = KeyLifetimeEnforcer::new();
        let lifetime = enforcer.max_lifetime(ClassificationLevel::Secret);
        // 24 hours.
        assert_eq!(lifetime, std::time::Duration::from_secs(24 * 3600));
    }

    #[test]
    fn test_should_rotate_expired_key() {
        let enforcer = KeyLifetimeEnforcer::new();
        // Key created 2 years ago. TopSecret max is 8 hours.
        let old_date = "2024-01-01T00:00:00Z";
        assert!(enforcer.should_rotate(ClassificationLevel::TopSecret, old_date));
    }

    #[test]
    fn test_should_not_rotate_fresh_key() {
        let enforcer = KeyLifetimeEnforcer::new();
        let fresh_date = chrono::Utc::now().to_rfc3339();
        assert!(!enforcer.should_rotate(ClassificationLevel::Unclassified, &fresh_date));
    }

    #[test]
    fn test_should_rotate_unparseable_date() {
        let enforcer = KeyLifetimeEnforcer::new();
        assert!(enforcer.should_rotate(ClassificationLevel::Restricted, "not-a-date"));
    }

    #[test]
    fn test_should_rotate_restricted_30_day_key() {
        let enforcer = KeyLifetimeEnforcer::new();
        // Restricted max is 720 hours (30 days). A key created 31 days ago should rotate.
        let old_date = (chrono::Utc::now() - chrono::Duration::days(31)).to_rfc3339();
        assert!(enforcer.should_rotate(ClassificationLevel::Restricted, &old_date));
    }

    #[test]
    fn test_should_not_rotate_restricted_recent_key() {
        let enforcer = KeyLifetimeEnforcer::new();
        // Key created 1 day ago. Restricted max is 30 days.
        let recent_date = (chrono::Utc::now() - chrono::Duration::days(1)).to_rfc3339();
        assert!(!enforcer.should_rotate(ClassificationLevel::Restricted, &recent_date));
    }

    // ---- requirements_for tests ----

    #[test]
    fn test_requirements_for_defaults() {
        let store = Arc::new(ZoneCertificateStore::new());
        let engine = ClassificationEngine::new(store);
        let reqs = engine.requirements_for(ClassificationLevel::Secret);
        assert_eq!(reqs.min_symmetric_key_bits, 256);
        assert!(reqs.pq_required);
        assert!(reqs.tempest_required);
    }

    #[test]
    fn test_override_requirements() {
        let store = Arc::new(ZoneCertificateStore::new());
        let mut engine = ClassificationEngine::new(store);
        let mut custom_reqs =
            ClassificationRequirements::defaults_for(ClassificationLevel::Restricted);
        custom_reqs.min_symmetric_key_bits = 512;
        engine.override_requirements(ClassificationLevel::Restricted, custom_reqs);

        let reqs = engine.requirements_for(ClassificationLevel::Restricted);
        assert_eq!(reqs.min_symmetric_key_bits, 512);
    }
}
