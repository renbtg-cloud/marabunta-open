// Marabunta - Licensed under the MIT License.
//! Pillar 7.1: Jurisdictional Boundary Fencing
//! 
//! Implements strict mathematical enforcement of GDPR and ITAR borders.
//! Prevents WASM shards and BFT gossip from crossing unauthorized GeoCell boundaries.

use serde::{Serialize, Deserialize};
use crate::highestsec::types::CountryCode;

/// ZoneClass — what kind of infrastructure.
/// Ordering: Civilian < GovCloud < MilRestricted < MilClassified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ZoneClass {
    Civilian = 0,
    GovCloud = 1,
    MilRestricted = 2,
    MilClassified = 3,
}

/// Jurisdiction — country + zone class + optional authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Jurisdiction {
    pub country: CountryCode,
    pub zone_class: ZoneClass,
    pub authority: Option<NationalAuthority>,
}

/// NationalAuthority — recognized security authority for a jurisdiction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NationalAuthority {
    pub id: String,
    pub country: CountryCode,
    pub public_key: Vec<u8>,
    pub name: String,
}

/// AllianceOrg — agreement organizations.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AllianceOrg {
    EU,
    GLOBAL_ALLIANCE_T1,
    ALLIANCE_FVEY_EQ,
    EU_STRATEGIC_PACT,
    Custom(String),
}

/// JurisdictionSet — composable jurisdiction constraints.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JurisdictionSet {
    Any,
    Only(Vec<CountryCode>),
    Except(Vec<CountryCode>),
    Agreement(Vec<AllianceOrg>),
    AllOf(Vec<JurisdictionSet>),
    AnyOf(Vec<JurisdictionSet>),
}

/// Trait for resolving agreement membership to country lists.
pub trait AllianceResolver: Send + Sync {
    fn members(&self, org: &AllianceOrg) -> Vec<CountryCode>;
}

impl JurisdictionSet {
    /// Check if a country is contained in this set.
    pub fn contains(&self, country: &CountryCode, resolver: &dyn AllianceResolver) -> bool {
        match self {
            Self::Any => true,
            Self::Only(countries) => countries.contains(country),
            Self::Except(countries) => !countries.contains(country),
            Self::Agreement(orgs) => {
                orgs.iter().any(|org| resolver.members(org).contains(country))
            }
            Self::AllOf(sets) => sets.iter().all(|s| s.contains(country, resolver)),
            Self::AnyOf(sets) => sets.iter().any(|s| s.contains(country, resolver)),
        }
    }

    /// Compute intersection of two JurisdictionSets.
    pub fn intersect(&self, other: &JurisdictionSet) -> JurisdictionSet {
        match (self, other) {
            (Self::Any, other) => other.clone(),
            (me, Self::Any) => me.clone(),
            (Self::Only(a), Self::Only(b)) => {
                let intersection: Vec<CountryCode> =
                    a.iter().filter(|c| b.contains(c)).copied().collect();
                Self::Only(intersection)
            }
            (Self::Except(a), Self::Except(b)) => {
                let mut union = a.clone();
                for c in b {
                    if !union.contains(c) {
                        union.push(*c);
                    }
                }
                Self::Except(union)
            }
            (Self::Only(only), Self::Except(except)) | (Self::Except(except), Self::Only(only)) => {
                let filtered: Vec<CountryCode> =
                    only.iter().filter(|c| !except.contains(c)).copied().collect();
                Self::Only(filtered)
            }
            _ => Self::AllOf(vec![self.clone(), other.clone()]),
        }
    }

    /// Compute union of two JurisdictionSets.
    pub fn union(&self, other: &JurisdictionSet) -> JurisdictionSet {
        match (self, other) {
            (Self::Any, _) | (_, Self::Any) => Self::Any,
            (Self::Only(a), Self::Only(b)) => {
                let mut combined = a.clone();
                for c in b {
                    if !combined.contains(c) {
                        combined.push(*c);
                    }
                }
                Self::Only(combined)
            }
            (Self::Except(a), Self::Except(b)) => {
                let intersection: Vec<CountryCode> =
                    a.iter().filter(|c| b.contains(c)).copied().collect();
                Self::Except(intersection)
            }
            _ => Self::AnyOf(vec![self.clone(), other.clone()]),
        }
    }

    /// Is this set empty (no countries match)?
    pub fn is_empty(&self, resolver: &dyn AllianceResolver) -> bool {
        match self {
            Self::Any => false,
            Self::Only(countries) => countries.is_empty(),
            Self::Except(_) => false, // infinite countries minus finite = non-empty
            Self::Agreement(orgs) => orgs.iter().all(|org| resolver.members(org).is_empty()),
            Self::AllOf(sets) => {
                // Conservative: check if any subset is empty
                sets.iter().any(|s| s.is_empty(resolver))
            }
            Self::AnyOf(sets) => sets.iter().all(|s| s.is_empty(resolver)),
        }
    }
}

/// Default AllianceResolver with hardcoded 2026 membership lists.
pub struct StaticAllianceResolver;

impl AllianceResolver for StaticAllianceResolver {
    fn members(&self, org: &AllianceOrg) -> Vec<CountryCode> {
        match org {
            AllianceOrg::EU => vec![
                CountryCode::FR, CountryCode::DE, CountryCode::ES, CountryCode::IT,
                CountryCode::NL, CountryCode::BE, CountryCode::PL, CountryCode::RO,
                CountryCode::SE, CountryCode::PT, CountryCode::GR, CountryCode::CZ,
                CountryCode::HU, CountryCode::AT, CountryCode::BG, CountryCode::DK,
                CountryCode::FI, CountryCode::SK, CountryCode::IE, CountryCode::HR,
                CountryCode::LT, CountryCode::SI, CountryCode::LV, CountryCode::EE,
                CountryCode::CY, CountryCode::LU, CountryCode::MT,
            ],
            AllianceOrg::GLOBAL_ALLIANCE_T1 => vec![
                CountryCode::US, CountryCode::GB, CountryCode::FR, CountryCode::DE,
                CountryCode::ES, CountryCode::IT, CountryCode::NL, CountryCode::BE,
                CountryCode::PL, CountryCode::RO, CountryCode::SE, CountryCode::PT,
                CountryCode::GR, CountryCode::CZ, CountryCode::HU, CountryCode::BG,
                CountryCode::DK, CountryCode::FI, CountryCode::SK, CountryCode::HR,
                CountryCode::LT, CountryCode::SI, CountryCode::LV, CountryCode::EE,
                CountryCode::LU, CountryCode::CA, CountryCode::TR, CountryCode::NO,
                CountryCode::IS, CountryCode::ME, CountryCode::MK, CountryCode::AL,
            ],
            AllianceOrg::ALLIANCE_FVEY_EQ => vec![
                CountryCode::US, CountryCode::GB, CountryCode::CA,
                CountryCode::AU, CountryCode::NZ,
            ],
            AllianceOrg::EU_STRATEGIC_PACT => vec![
                CountryCode::FR, CountryCode::DE, CountryCode::ES, CountryCode::IT,
                CountryCode::NL, CountryCode::BE, CountryCode::PL, CountryCode::RO,
                CountryCode::SE, CountryCode::PT, CountryCode::GR, CountryCode::CZ,
                CountryCode::HU, CountryCode::AT, CountryCode::BG, CountryCode::FI,
                CountryCode::SK, CountryCode::IE, CountryCode::HR, CountryCode::LT,
                CountryCode::SI, CountryCode::LV, CountryCode::EE, CountryCode::CY,
                CountryCode::LU,
            ],
            AllianceOrg::Custom(_) => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver() -> StaticAllianceResolver {
        StaticAllianceResolver
    }

    #[test]
    fn test_zone_class_ordering() {
        assert!(ZoneClass::Civilian < ZoneClass::GovCloud);
        assert!(ZoneClass::GovCloud < ZoneClass::MilRestricted);
        assert!(ZoneClass::MilRestricted < ZoneClass::MilClassified);
    }

    #[test]
    fn test_jurisdiction_set_contains_only() {
        let set = JurisdictionSet::Only(vec![CountryCode::FR, CountryCode::DE]);
        let r = resolver();
        assert!(set.contains(&CountryCode::FR, &r));
        assert!(set.contains(&CountryCode::DE, &r));
        assert!(!set.contains(&CountryCode::ES, &r));
    }

    #[test]
    fn test_jurisdiction_set_contains_except() {
        let set = JurisdictionSet::Except(vec![CountryCode::US]);
        let r = resolver();
        assert!(set.contains(&CountryCode::FR, &r));
        assert!(!set.contains(&CountryCode::US, &r));
    }

    #[test]
    fn test_jurisdiction_set_contains_agreement() {
        let set = JurisdictionSet::Agreement(vec![AllianceOrg::EU]);
        let r = resolver();
        assert!(set.contains(&CountryCode::FR, &r));
        assert!(set.contains(&CountryCode::DE, &r));
        assert!(!set.contains(&CountryCode::US, &r));
    }

    #[test]
    fn test_jurisdiction_set_intersect() {
        let a = JurisdictionSet::Only(vec![CountryCode::FR, CountryCode::DE]);
        let b = JurisdictionSet::Only(vec![CountryCode::DE, CountryCode::ES]);
        let r = resolver();
        let result = a.intersect(&b);
        assert!(result.contains(&CountryCode::DE, &r));
        assert!(!result.contains(&CountryCode::FR, &r));
        assert!(!result.contains(&CountryCode::ES, &r));
    }

    #[test]
    fn test_jurisdiction_set_union() {
        let a = JurisdictionSet::Only(vec![CountryCode::FR]);
        let b = JurisdictionSet::Only(vec![CountryCode::DE]);
        let r = resolver();
        let result = a.union(&b);
        assert!(result.contains(&CountryCode::FR, &r));
        assert!(result.contains(&CountryCode::DE, &r));
    }

    #[test]
    fn test_jurisdiction_set_empty() {
        let a = JurisdictionSet::Only(vec![CountryCode::FR]);
        let b = JurisdictionSet::Only(vec![CountryCode::DE]);
        let r = resolver();
        let result = a.intersect(&b);
        assert!(result.is_empty(&r));
    }

    #[test]
    fn test_jurisdiction_set_allof() {
        let set = JurisdictionSet::AllOf(vec![
            JurisdictionSet::Agreement(vec![AllianceOrg::EU]),
            JurisdictionSet::Agreement(vec![AllianceOrg::GLOBAL_ALLIANCE_T1]),
        ]);
        let r = resolver();
        // FR is in both EU and GLOBAL_ALLIANCE_T1
        assert!(set.contains(&CountryCode::FR, &r));
        // US is in GLOBAL_ALLIANCE_T1 but not EU
        assert!(!set.contains(&CountryCode::US, &r));
    }

    #[test]
    fn test_static_agreement_resolver() {
        let r = resolver();
        let eu = r.members(&AllianceOrg::EU);
        assert!(eu.contains(&CountryCode::FR));
        assert!(eu.contains(&CountryCode::DE));
        assert!(!eu.contains(&CountryCode::US));

        let five_eyes = r.members(&AllianceOrg::ALLIANCE_FVEY_EQ);
        assert!(five_eyes.contains(&CountryCode::US));
        assert!(five_eyes.contains(&CountryCode::GB));
        assert!(!five_eyes.contains(&CountryCode::FR));
    }
}
