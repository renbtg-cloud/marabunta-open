// Marabunta - Licensed under the MIT License.
use serde::{Serialize, Deserialize};
use crate::highestsec::jurisdiction::ZoneClass;

/// GLOBAL_ALLIANCE_T1/EU classification levels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[derive(Default)]
pub enum ClassificationLevel {
    #[default]
    Unclassified = 0,
    Restricted = 1,
    Confidential = 2,
    Secret = 3,
    TopSecret = 4,
}

impl ClassificationLevel {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Unclassified => "UNCLASSIFIED",
            Self::Restricted => "RESTRICTED",
            Self::Confidential => "CONFIDENTIAL",
            Self::Secret => "SECRET",
            Self::TopSecret => "TOP SECRET",
        }
    }

    pub fn nato_label(&self) -> &'static str {
        match self {
            Self::Unclassified => "GLOBAL_ALLIANCE_T1 UNCLASSIFIED",
            Self::Restricted => "GLOBAL_ALLIANCE_T1 RESTRICTED",
            Self::Confidential => "GLOBAL_ALLIANCE_T1 CONFIDENTIAL",
            Self::Secret => "GLOBAL_ALLIANCE_T1 SECRET",
            Self::TopSecret => "RESTRICTED_LEVEL_4",
        }
    }

    pub fn eu_label(&self) -> &'static str {
        match self {
            Self::Unclassified => "NON CLASSIFIE UE",
            Self::Restricted => "RESTREINT UE",
            Self::Confidential => "RESTRICTED_LEVEL_2",
            Self::Secret => "SECRET UE",
            Self::TopSecret => "TRES SECRET UE",
        }
    }
}


/// Releasability marking.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReleasabilityMarking {
    RelTo(Vec<crate::highestsec::types::CountryCode>),
    RelToOrg(crate::highestsec::jurisdiction::AllianceOrg),
    NoForn,
    OrCon,
    EyesOnly(Vec<crate::highestsec::types::CountryCode>),
}

/// Compartment — named sub-classification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Compartment {
    pub id: String,
    pub controlling_authority: crate::highestsec::jurisdiction::NationalAuthority,
}

/// Handling handling_restrictions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HandlingHandlingRestriction {
    SpecialCategory(String),
    CryptoMarked,
    Atomal,
    Bohemia,
}

/// Full handling_restriction bundle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandlingRestriction {
    pub level: ClassificationLevel,
    pub releasable_to: ReleasabilityMarking,
    pub compartments: Vec<Compartment>,
    pub handling: Vec<HandlingHandlingRestriction>,
}

/// Per-level requirements (from spec Section 3.1.1 table).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassificationRequirements {
    pub level: ClassificationLevel,
    pub min_symmetric_key_bits: u32,
    pub min_asymmetric_key_bits: u32,
    pub pq_required: bool,
    pub min_zone_class: ZoneClass,
    pub min_proof_level: u8,
    pub tempest_required: bool,
    pub airgap_required: bool,
    pub signed_audit_entries: bool,
    pub realtime_audit_forward: bool,
    pub max_key_lifetime_hours: u32,
    pub key_escrow_required: bool,
    pub hsm_required: bool,
    pub audit_retention_days: u32,
}

impl ClassificationRequirements {
    /// Return default requirements for a given classification level.
    pub fn defaults_for(level: ClassificationLevel) -> Self {
        match level {
            ClassificationLevel::Unclassified => Self {
                level,
                min_symmetric_key_bits: 128,
                min_asymmetric_key_bits: 2048,
                pq_required: false,
                min_zone_class: ZoneClass::Civilian,
                min_proof_level: 0,
                tempest_required: false,
                airgap_required: false,
                signed_audit_entries: false,
                realtime_audit_forward: false,
                max_key_lifetime_hours: 8760, // 1 year
                key_escrow_required: false,
                hsm_required: false,
                audit_retention_days: 90,
            },
            ClassificationLevel::Restricted => Self {
                level,
                min_symmetric_key_bits: 128,
                min_asymmetric_key_bits: 3072,
                pq_required: false,
                min_zone_class: ZoneClass::GovCloud,
                min_proof_level: 1,
                tempest_required: false,
                airgap_required: false,
                signed_audit_entries: true,
                realtime_audit_forward: false,
                max_key_lifetime_hours: 720, // 30 days
                key_escrow_required: false,
                hsm_required: false,
                audit_retention_days: 365,
            },
            ClassificationLevel::Confidential => Self {
                level,
                min_symmetric_key_bits: 256,
                min_asymmetric_key_bits: 3072,
                pq_required: true,
                min_zone_class: ZoneClass::MilRestricted,
                min_proof_level: 2,
                tempest_required: false,
                airgap_required: false,
                signed_audit_entries: true,
                realtime_audit_forward: true,
                max_key_lifetime_hours: 168, // 7 days
                key_escrow_required: false,
                hsm_required: false,
                audit_retention_days: 730, // 2 years
            },
            ClassificationLevel::Secret => Self {
                level,
                min_symmetric_key_bits: 256,
                min_asymmetric_key_bits: 4096,
                pq_required: true,
                min_zone_class: ZoneClass::MilClassified,
                min_proof_level: 3,
                tempest_required: true,
                airgap_required: false,
                signed_audit_entries: true,
                realtime_audit_forward: true,
                max_key_lifetime_hours: 24,
                key_escrow_required: true,
                hsm_required: true,
                audit_retention_days: 2555, // 7 years
            },
            ClassificationLevel::TopSecret => Self {
                level,
                min_symmetric_key_bits: 256,
                min_asymmetric_key_bits: 4096,
                pq_required: true,
                min_zone_class: ZoneClass::MilClassified,
                min_proof_level: 3,
                tempest_required: true,
                airgap_required: true,
                signed_audit_entries: true,
                realtime_audit_forward: true,
                max_key_lifetime_hours: 8,
                key_escrow_required: true,
                hsm_required: true,
                audit_retention_days: 3650, // 10 years
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classification_ordering() {
        assert!(ClassificationLevel::Unclassified < ClassificationLevel::Restricted);
        assert!(ClassificationLevel::Restricted < ClassificationLevel::Confidential);
        assert!(ClassificationLevel::Confidential < ClassificationLevel::Secret);
        assert!(ClassificationLevel::Secret < ClassificationLevel::TopSecret);
    }

    #[test]
    fn test_defaults_unclassified() {
        let reqs = ClassificationRequirements::defaults_for(ClassificationLevel::Unclassified);
        assert_eq!(reqs.min_symmetric_key_bits, 128);
        assert!(!reqs.pq_required);
        assert_eq!(reqs.min_zone_class, ZoneClass::Civilian);
        assert!(!reqs.tempest_required);
        assert!(!reqs.hsm_required);
    }

    #[test]
    fn test_defaults_secret() {
        let reqs = ClassificationRequirements::defaults_for(ClassificationLevel::Secret);
        assert_eq!(reqs.min_symmetric_key_bits, 256);
        assert!(reqs.pq_required);
        assert_eq!(reqs.min_zone_class, ZoneClass::MilClassified);
        assert!(reqs.tempest_required);
        assert!(reqs.hsm_required);
        assert_eq!(reqs.max_key_lifetime_hours, 24);
    }

    #[test]
    fn test_defaults_top_secret() {
        let reqs = ClassificationRequirements::defaults_for(ClassificationLevel::TopSecret);
        assert!(reqs.airgap_required);
        assert_eq!(reqs.max_key_lifetime_hours, 8);
        assert!(reqs.key_escrow_required);
    }

    #[test]
    fn test_classification_labels() {
        assert_eq!(ClassificationLevel::Secret.label(), "SECRET");
        assert_eq!(ClassificationLevel::TopSecret.nato_label(), "RESTRICTED_LEVEL_4");
        assert_eq!(ClassificationLevel::Restricted.eu_label(), "RESTREINT UE");
        assert_eq!(ClassificationLevel::Unclassified.label(), "UNCLASSIFIED");
    }
}
