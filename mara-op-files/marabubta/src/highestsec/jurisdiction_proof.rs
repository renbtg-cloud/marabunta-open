// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};

use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::jurisdiction::ZoneClass;
use crate::highestsec::types::CountryCode;

/// How a node proves its jurisdiction residency. Each variant corresponds
/// to an increasing assurance level (0..3).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JurisdictionProof {
    /// Level 0: IP geolocation + TLS cert locality + Speed-of-Light VDF.
    NetworkLocation {
        ip_geolocation: GeoResult,
        tls_cert_locality: Option<String>,
        /// Hardware-Anchored Verifiable Delay Function proof.
        /// Proves the actual silicon doing the computation is within physical
        /// reach of the verifier, defeating proxy-based location spoofing.
        vdf_proof: Option<VdfProof>,
    },
    /// Level 1: National authority certificate.
    NationalCertificate {
        certificate_der: Vec<u8>,
        issuer: NationalAuthorityId,
        node_id: Vec<u8>,
        valid_until: String,
    },
    /// Level 2: Hardware attestation (stub).
    HardwareAttestation {
        tpm_quote: Vec<u8>,
        facility_id: String,
        platform_id: String,
    },
    /// Level 3: Physical inspection — wraps lower-level proofs plus an
    /// inspection report.
    PhysicalInspection {
        hardware_attestation: Box<JurisdictionProof>,
        national_certificate: Box<JurisdictionProof>,
        inspection_report_hash: [u8; 32],
        inspector_id: String,
        inspection_date: String,
        next_inspection_due: String,
    },
}

impl JurisdictionProof {
    /// Returns the assurance level of this proof variant.
    pub fn level(&self) -> u8 {
        match self {
            Self::NetworkLocation { .. } => 0,
            Self::NationalCertificate { .. } => 1,
            Self::HardwareAttestation { .. } => 2,
            Self::PhysicalInspection { .. } => 3,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VdfProof {
    pub challenge: [u8; 32],
    pub result: Vec<u8>,
    /// Time taken to compute the result in milliseconds.
    pub duration_ms: u64,
}

/// Result from an IP geolocation provider.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoResult {
    pub country: CountryCode,
    pub region: Option<String>,
    pub city: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub provider: String,
    pub confidence: f64,
    pub queried_at: String,
}

/// Identifier for a national security authority (e.g. ANSSI, BSI).
pub type NationalAuthorityId = String;

/// Minimum proof level matrix: given a `(ZoneClass, ClassificationLevel)` pair,
/// returns the minimum proof level required, or `None` if the combination is
/// outright forbidden.
pub fn min_proof_level(zone_class: ZoneClass, classification: ClassificationLevel) -> Option<u8> {
    match (zone_class, classification) {
        // Civilian zones only allow Unclassified data.
        (ZoneClass::Civilian, ClassificationLevel::Unclassified) => Some(0),
        (ZoneClass::Civilian, _) => None,

        // GovCloud: Unclassified (level 0) and Restricted (level 1).
        (ZoneClass::GovCloud, ClassificationLevel::Unclassified) => Some(0),
        (ZoneClass::GovCloud, ClassificationLevel::Restricted) => Some(1),
        (ZoneClass::GovCloud, _) => None,

        // MilRestricted: up to Confidential.
        (ZoneClass::MilRestricted, ClassificationLevel::Unclassified) => Some(1),
        (ZoneClass::MilRestricted, ClassificationLevel::Restricted) => Some(1),
        (ZoneClass::MilRestricted, ClassificationLevel::Confidential) => Some(2),
        (ZoneClass::MilRestricted, _) => None,

        // MilClassified: all levels allowed with escalating proof.
        (ZoneClass::MilClassified, ClassificationLevel::Unclassified) => Some(1),
        (ZoneClass::MilClassified, ClassificationLevel::Restricted) => Some(2),
        (ZoneClass::MilClassified, ClassificationLevel::Confidential) => Some(2),
        (ZoneClass::MilClassified, ClassificationLevel::Secret) => Some(3),
        (ZoneClass::MilClassified, ClassificationLevel::TopSecret) => Some(3),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_geo_result(country: CountryCode) -> GeoResult {
        GeoResult {
            country,
            region: Some("Ile-de-France".into()),
            city: Some("Paris".into()),
            latitude: 48.8566,
            longitude: 2.3522,
            provider: "test-provider".into(),
            confidence: 0.95,
            queried_at: "2026-01-15T12:00:00Z".into(),
        }
    }

    #[test]
    fn test_proof_level_network_location() {
        let proof = JurisdictionProof::NetworkLocation {
            ip_geolocation: make_geo_result(CountryCode::FR),
            tls_cert_locality: Some("FR".into()),
        };
        assert_eq!(proof.level(), 0);
    }

    #[test]
    fn test_proof_level_national_certificate() {
        let proof = JurisdictionProof::NationalCertificate {
            certificate_der: vec![0xDE, 0xAD],
            issuer: "ANSSI".into(),
            node_id: vec![1, 2, 3],
            valid_until: "2027-01-01T00:00:00Z".into(),
        };
        assert_eq!(proof.level(), 1);
    }

    #[test]
    fn test_proof_level_hardware_attestation() {
        let proof = JurisdictionProof::HardwareAttestation {
            tpm_quote: vec![0xAB],
            facility_id: "FAC-001".into(),
            platform_id: "PLAT-001".into(),
        };
        assert_eq!(proof.level(), 2);
    }

    #[test]
    fn test_proof_level_physical_inspection() {
        let inner_hw = JurisdictionProof::HardwareAttestation {
            tpm_quote: vec![0xAB],
            facility_id: "FAC-001".into(),
            platform_id: "PLAT-001".into(),
        };
        let inner_cert = JurisdictionProof::NationalCertificate {
            certificate_der: vec![0xDE, 0xAD],
            issuer: "ANSSI".into(),
            node_id: vec![1, 2, 3],
            valid_until: "2027-01-01T00:00:00Z".into(),
        };
        let proof = JurisdictionProof::PhysicalInspection {
            hardware_attestation: Box::new(inner_hw),
            national_certificate: Box::new(inner_cert),
            inspection_report_hash: [0u8; 32],
            inspector_id: "INSP-42".into(),
            inspection_date: "2026-01-10".into(),
            next_inspection_due: "2026-07-10".into(),
        };
        assert_eq!(proof.level(), 3);
    }

    // ---- min_proof_level matrix tests ----

    #[test]
    fn test_min_proof_civilian_unclassified() {
        assert_eq!(
            min_proof_level(ZoneClass::Civilian, ClassificationLevel::Unclassified),
            Some(0)
        );
    }

    #[test]
    fn test_min_proof_civilian_restricted_forbidden() {
        assert_eq!(
            min_proof_level(ZoneClass::Civilian, ClassificationLevel::Restricted),
            None
        );
    }

    #[test]
    fn test_min_proof_civilian_confidential_forbidden() {
        assert_eq!(
            min_proof_level(ZoneClass::Civilian, ClassificationLevel::Confidential),
            None
        );
    }

    #[test]
    fn test_min_proof_civilian_secret_forbidden() {
        assert_eq!(
            min_proof_level(ZoneClass::Civilian, ClassificationLevel::Secret),
            None
        );
    }

    #[test]
    fn test_min_proof_civilian_top_secret_forbidden() {
        assert_eq!(
            min_proof_level(ZoneClass::Civilian, ClassificationLevel::TopSecret),
            None
        );
    }

    #[test]
    fn test_min_proof_govcloud_unclassified() {
        assert_eq!(
            min_proof_level(ZoneClass::GovCloud, ClassificationLevel::Unclassified),
            Some(0)
        );
    }

    #[test]
    fn test_min_proof_govcloud_restricted() {
        assert_eq!(
            min_proof_level(ZoneClass::GovCloud, ClassificationLevel::Restricted),
            Some(1)
        );
    }

    #[test]
    fn test_min_proof_govcloud_confidential_forbidden() {
        assert_eq!(
            min_proof_level(ZoneClass::GovCloud, ClassificationLevel::Confidential),
            None
        );
    }

    #[test]
    fn test_min_proof_milrestricted_unclassified() {
        assert_eq!(
            min_proof_level(ZoneClass::MilRestricted, ClassificationLevel::Unclassified),
            Some(1)
        );
    }

    #[test]
    fn test_min_proof_milrestricted_restricted() {
        assert_eq!(
            min_proof_level(ZoneClass::MilRestricted, ClassificationLevel::Restricted),
            Some(1)
        );
    }

    #[test]
    fn test_min_proof_milrestricted_confidential() {
        assert_eq!(
            min_proof_level(ZoneClass::MilRestricted, ClassificationLevel::Confidential),
            Some(2)
        );
    }

    #[test]
    fn test_min_proof_milrestricted_secret_forbidden() {
        assert_eq!(
            min_proof_level(ZoneClass::MilRestricted, ClassificationLevel::Secret),
            None
        );
    }

    #[test]
    fn test_min_proof_milclassified_unclassified() {
        assert_eq!(
            min_proof_level(ZoneClass::MilClassified, ClassificationLevel::Unclassified),
            Some(1)
        );
    }

    #[test]
    fn test_min_proof_milclassified_restricted() {
        assert_eq!(
            min_proof_level(ZoneClass::MilClassified, ClassificationLevel::Restricted),
            Some(2)
        );
    }

    #[test]
    fn test_min_proof_milclassified_confidential() {
        assert_eq!(
            min_proof_level(ZoneClass::MilClassified, ClassificationLevel::Confidential),
            Some(2)
        );
    }

    #[test]
    fn test_min_proof_milclassified_secret() {
        assert_eq!(
            min_proof_level(ZoneClass::MilClassified, ClassificationLevel::Secret),
            Some(3)
        );
    }

    #[test]
    fn test_min_proof_milclassified_top_secret() {
        assert_eq!(
            min_proof_level(ZoneClass::MilClassified, ClassificationLevel::TopSecret),
            Some(3)
        );
    }

    // ---- GeoResult serialization ----

    #[test]
    fn test_geo_result_serialization_roundtrip() {
        let geo = make_geo_result(CountryCode::DE);
        let json = serde_json::to_string(&geo).unwrap();
        let deserialized: GeoResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.country, CountryCode::DE);
        assert_eq!(deserialized.latitude, 48.8566);
        assert_eq!(deserialized.provider, "test-provider");
    }

    #[test]
    fn test_geo_result_optional_fields() {
        let geo = GeoResult {
            country: CountryCode::US,
            region: None,
            city: None,
            latitude: 0.0,
            longitude: 0.0,
            provider: "minimal".into(),
            confidence: 0.5,
            queried_at: "2026-02-01T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&geo).unwrap();
        let deserialized: GeoResult = serde_json::from_str(&json).unwrap();
        assert!(deserialized.region.is_none());
        assert!(deserialized.city.is_none());
    }

    #[test]
    fn test_jurisdiction_proof_serialization_roundtrip() {
        let proof = JurisdictionProof::NationalCertificate {
            certificate_der: vec![1, 2, 3, 4],
            issuer: "BSI".into(),
            node_id: vec![10, 20],
            valid_until: "2027-06-15T00:00:00Z".into(),
        };
        let json = serde_json::to_string(&proof).unwrap();
        let deserialized: JurisdictionProof = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.level(), 1);
    }

    #[test]
    fn test_physical_inspection_serialization_nested() {
        let inner_hw = JurisdictionProof::HardwareAttestation {
            tpm_quote: vec![0xAB, 0xCD],
            facility_id: "FAC-X".into(),
            platform_id: "PLAT-X".into(),
        };
        let inner_cert = JurisdictionProof::NationalCertificate {
            certificate_der: vec![0xFF],
            issuer: "ANSSI".into(),
            node_id: vec![42],
            valid_until: "2028-01-01T00:00:00Z".into(),
        };
        let proof = JurisdictionProof::PhysicalInspection {
            hardware_attestation: Box::new(inner_hw),
            national_certificate: Box::new(inner_cert),
            inspection_report_hash: [0xAA; 32],
            inspector_id: "INSP-99".into(),
            inspection_date: "2026-03-01".into(),
            next_inspection_due: "2026-09-01".into(),
        };
        let json = serde_json::to_string(&proof).unwrap();
        let deserialized: JurisdictionProof = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.level(), 3);
    }
}
