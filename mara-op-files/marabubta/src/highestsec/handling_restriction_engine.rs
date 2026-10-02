// Marabunta - Licensed under the MIT License.
use crate::highestsec::classification::{
    HandlingRestriction, ClassificationLevel, ReleasabilityMarking,
};
use crate::highestsec::jurisdiction::{StaticAllianceResolver, AllianceOrg, AllianceResolver};
use crate::highestsec::types::CountryCode;

// ---------------------------------------------------------------------------
// HandlingRestrictionEngine
// ---------------------------------------------------------------------------

/// Enforces handling_restriction-based access control and data crossing rules.
///
/// Two operations:
/// - **check_access**: verify that an individual's credentials satisfy a
///   data item's handling_restrictions (clearance, nationality, compartments, handling).
/// - **check_crossing**: verify that a data item's handling_restrictions permit transfer
///   to a destination jurisdiction.
pub struct HandlingRestrictionEngine;

impl HandlingRestrictionEngine {
    /// Create a new HandlingRestrictionEngine.
    pub fn new() -> Self {
        Self
    }

    /// Check whether a requester's credentials satisfy the data's handling_restrictions.
    ///
    /// Checks (in order):
    /// 1. Clearance level >= data's classification level.
    /// 2. Nationality is within the releasability marking.
    /// 3. All required compartments are accessible.
    /// 4. Handling handling_restrictions are satisfiable (currently a pass-through).
    pub fn check_access(
        &self,
        requester: &AccessCredentials,
        data_handling_restrictions: &HandlingRestriction,
    ) -> Result<(), HandlingRestrictionError> {
        // 1. Clearance check.
        if requester.clearance_level < data_handling_restrictions.level {
            return Err(HandlingRestrictionError::InsufficientClearance {
                required: data_handling_restrictions.level,
                actual: requester.clearance_level,
            });
        }

        // 2. Releasability check.
        self.check_nationality_releasability(
            &requester.nationality,
            &data_handling_restrictions.releasable_to,
            requester.organization.as_ref(),
        )?;

        // 3. Compartment check.
        for compartment in &data_handling_restrictions.compartments {
            if !requester.compartment_access.contains(&compartment.id) {
                return Err(HandlingRestrictionError::MissingCompartmentAccess(
                    compartment.id.clone(),
                ));
            }
        }

        // 4. Handling handling_restrictions: pass-through for now.
        // In a real system, this would verify that the requester's environment
        // satisfies each handling handling_restriction (e.g. TEMPEST-shielded terminal for
        // CryptoMarked data).

        Ok(())
    }

    /// Check whether a data item's handling_restrictions permit crossing from
    /// `source_jurisdiction` to `destination_jurisdiction`.
    ///
    /// Rules:
    /// - `RelTo(countries)`: destination must be in the country list.
    /// - `RelToOrg(org)`: destination must be a member of the organization.
    /// - `NoForn`: destination must be US (foreign dissemination prohibited).
    /// - `OrCon`: always returns `NeedsApproval` (originator consent required).
    /// - `EyesOnly(countries)`: destination must be in the country list.
    pub fn check_crossing(
        &self,
        data_handling_restrictions: &HandlingRestriction,
        _source_jurisdiction: &CountryCode,
        destination_jurisdiction: &CountryCode,
    ) -> Result<(), HandlingRestrictionError> {
        match &data_handling_restrictions.releasable_to {
            ReleasabilityMarking::RelTo(countries) => {
                if !countries.contains(destination_jurisdiction) {
                    return Err(HandlingRestrictionError::NationalityNotInReleasability {
                        nationality: *destination_jurisdiction,
                    });
                }
            }

            ReleasabilityMarking::RelToOrg(org) => {
                let resolver = StaticAllianceResolver;
                let members = resolver.members(org);
                if !members.contains(destination_jurisdiction) {
                    return Err(HandlingRestrictionError::NationalityNotInReleasability {
                        nationality: *destination_jurisdiction,
                    });
                }
            }

            ReleasabilityMarking::NoForn => {
                if *destination_jurisdiction != CountryCode::US {
                    return Err(HandlingRestrictionError::NoFornViolation);
                }
            }

            ReleasabilityMarking::OrCon => {
                // OrCon always requires originator consent. Use FR as the
                // default originator when the actual originator is unknown.
                return Err(HandlingRestrictionError::OrConRequiresApproval {
                    originator: CountryCode::FR,
                });
            }

            ReleasabilityMarking::EyesOnly(countries) => {
                if !countries.contains(destination_jurisdiction) {
                    return Err(HandlingRestrictionError::EyesOnlyViolation {
                        allowed: countries.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Internal helper: check nationality against a releasability marking.
    fn check_nationality_releasability(
        &self,
        nationality: &CountryCode,
        releasable_to: &ReleasabilityMarking,
        _organization: Option<&AllianceOrg>,
    ) -> Result<(), HandlingRestrictionError> {
        match releasable_to {
            ReleasabilityMarking::RelTo(countries) => {
                if !countries.contains(nationality) {
                    return Err(HandlingRestrictionError::NationalityNotInReleasability {
                        nationality: *nationality,
                    });
                }
            }

            ReleasabilityMarking::RelToOrg(org) => {
                let resolver = StaticAllianceResolver;
                let members = resolver.members(org);
                if !members.contains(nationality) {
                    return Err(HandlingRestrictionError::NationalityNotInReleasability {
                        nationality: *nationality,
                    });
                }
            }

            ReleasabilityMarking::NoForn => {
                if *nationality != CountryCode::US {
                    return Err(HandlingRestrictionError::NoFornViolation);
                }
            }

            ReleasabilityMarking::OrCon => {
                // OrCon: originator consent required for any access.
                return Err(HandlingRestrictionError::OrConRequiresApproval {
                    originator: CountryCode::FR,
                });
            }

            ReleasabilityMarking::EyesOnly(countries) => {
                if !countries.contains(nationality) {
                    return Err(HandlingRestrictionError::EyesOnlyViolation {
                        allowed: countries.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

impl Default for HandlingRestrictionEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// AccessCredentials
// ---------------------------------------------------------------------------

/// Credentials presented by a requester when accessing classified data.
#[derive(Clone, Debug)]
pub struct AccessCredentials {
    /// The requester's security clearance level.
    pub clearance_level: ClassificationLevel,
    /// The requester's nationality.
    pub nationality: CountryCode,
    /// Compartment identifiers the requester is cleared for.
    pub compartment_access: Vec<String>,
    /// Optional agreement organization membership.
    pub organization: Option<AllianceOrg>,
}

// ---------------------------------------------------------------------------
// HandlingRestrictionError
// ---------------------------------------------------------------------------

/// Errors from handling_restriction checks.
#[derive(Debug)]
pub enum HandlingRestrictionError {
    /// Requester's clearance is below the data's classification.
    InsufficientClearance {
        required: ClassificationLevel,
        actual: ClassificationLevel,
    },
    /// Requester's nationality is not in the releasability set.
    NationalityNotInReleasability {
        nationality: CountryCode,
    },
    /// Requester lacks access to a required compartment.
    MissingCompartmentAccess(String),
    /// Data is marked NOFORN and requester is not US.
    NoFornViolation,
    /// Data is marked ORCON; originator consent is required.
    OrConRequiresApproval {
        originator: CountryCode,
    },
    /// Data is EYES ONLY for specific countries; requester is not among them.
    EyesOnlyViolation {
        allowed: Vec<CountryCode>,
    },
    /// A handling handling_restriction cannot be satisfied.
    HandlingHandlingRestrictionUnsatisfied(String),
}

impl std::fmt::Display for HandlingRestrictionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientClearance { required, actual } => {
                write!(
                    f,
                    "insufficient clearance: required {:?}, actual {:?}",
                    required, actual
                )
            }
            Self::NationalityNotInReleasability { nationality } => {
                write!(
                    f,
                    "nationality {} not in releasability set",
                    nationality
                )
            }
            Self::MissingCompartmentAccess(id) => {
                write!(f, "missing compartment access: {}", id)
            }
            Self::NoFornViolation => write!(f, "NOFORN violation"),
            Self::OrConRequiresApproval { originator } => {
                write!(
                    f,
                    "ORCON: originator consent required from {}",
                    originator
                )
            }
            Self::EyesOnlyViolation { allowed } => {
                let countries: Vec<String> =
                    allowed.iter().map(|c| c.as_str().to_string()).collect();
                write!(
                    f,
                    "EYES ONLY violation: allowed countries are [{}]",
                    countries.join(", ")
                )
            }
            Self::HandlingHandlingRestrictionUnsatisfied(desc) => {
                write!(f, "handling handling_restriction unsatisfied: {}", desc)
            }
        }
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
    use crate::highestsec::jurisdiction::{NationalAuthority, AllianceOrg};
    use crate::highestsec::types::CountryCode;

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

    fn make_credentials(
        clearance: ClassificationLevel,
        nationality: CountryCode,
        compartments: Vec<&str>,
    ) -> AccessCredentials {
        AccessCredentials {
            clearance_level: clearance,
            nationality,
            compartment_access: compartments.into_iter().map(String::from).collect(),
            organization: None,
        }
    }

    // ---- check_access: clearance ----

    #[test]
    fn test_access_sufficient_clearance() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::FR,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR]),
            vec![],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_exact_clearance() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::FR,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR]),
            vec![],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_insufficient_clearance() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Restricted,
            CountryCode::FR,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR]),
            vec![],
            vec![],
        );
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::InsufficientClearance {
                required: ClassificationLevel::Secret,
                actual: ClassificationLevel::Restricted,
            }
        ));
    }

    // ---- check_access: nationality / releasability ----

    #[test]
    fn test_access_nationality_in_relto() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::FR,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
            vec![],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_nationality_not_in_relto() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::ES,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
            vec![],
            vec![],
        );
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::NationalityNotInReleasability { .. }
        ));
    }

    #[test]
    fn test_access_noforn_us_passes() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::US,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::NoForn,
            vec![],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_noforn_non_us_fails() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::TopSecret,
            CountryCode::FR,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::NoForn,
            vec![],
            vec![],
        );
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HandlingRestrictionError::NoFornViolation));
    }

    #[test]
    fn test_access_eyes_only_in_list() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::GB,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::EyesOnly(vec![CountryCode::US, CountryCode::GB]),
            vec![],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_eyes_only_not_in_list() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::TopSecret,
            CountryCode::DE,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::EyesOnly(vec![CountryCode::US, CountryCode::GB]),
            vec![],
            vec![],
        );
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::EyesOnlyViolation { .. }
        ));
    }

    #[test]
    fn test_access_relto_org_nato_member() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::FR, // FR is a GLOBAL_ALLIANCE_T1 member
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelToOrg(AllianceOrg::GLOBAL_ALLIANCE_T1),
            vec![],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_relto_org_non_member() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::AU, // AU is not a GLOBAL_ALLIANCE_T1 member
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelToOrg(AllianceOrg::GLOBAL_ALLIANCE_T1),
            vec![],
            vec![],
        );
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::NationalityNotInReleasability { .. }
        ));
    }

    #[test]
    fn test_access_orcon_requires_approval() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::TopSecret,
            CountryCode::US,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::OrCon,
            vec![],
            vec![],
        );
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::OrConRequiresApproval { .. }
        ));
    }

    // ---- check_access: compartments ----

    #[test]
    fn test_access_compartment_present() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Secret,
            CountryCode::FR,
            vec!["SCI-ALPHA", "SCI-BETA"],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR]),
            vec![make_compartment("SCI-ALPHA")],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_compartment_missing() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::TopSecret,
            CountryCode::FR,
            vec!["SCI-ALPHA"],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR]),
            vec![make_compartment("SCI-ALPHA"), make_compartment("SCI-GAMMA")],
            vec![],
        );
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::MissingCompartmentAccess(ref id) if id == "SCI-GAMMA"
        ));
    }

    #[test]
    fn test_access_multiple_compartments_all_present() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::TopSecret,
            CountryCode::US,
            vec!["SCI-A", "SCI-B", "SCI-C"],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::NoForn,
            vec![
                make_compartment("SCI-A"),
                make_compartment("SCI-B"),
            ],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    // ---- check_crossing tests ----

    #[test]
    fn test_crossing_relto_allowed() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
            vec![],
            vec![],
        );
        assert!(engine
            .check_crossing(&handling_restriction, &CountryCode::FR, &CountryCode::DE)
            .is_ok());
    }

    #[test]
    fn test_crossing_relto_denied() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelTo(vec![CountryCode::FR, CountryCode::DE]),
            vec![],
            vec![],
        );
        let result = engine.check_crossing(&handling_restriction, &CountryCode::FR, &CountryCode::ES);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::NationalityNotInReleasability { .. }
        ));
    }

    #[test]
    fn test_crossing_noforn_blocks_non_us() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::NoForn,
            vec![],
            vec![],
        );
        let result = engine.check_crossing(&handling_restriction, &CountryCode::US, &CountryCode::GB);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HandlingRestrictionError::NoFornViolation));
    }

    #[test]
    fn test_crossing_noforn_allows_us() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::NoForn,
            vec![],
            vec![],
        );
        assert!(engine
            .check_crossing(&handling_restriction, &CountryCode::US, &CountryCode::US)
            .is_ok());
    }

    #[test]
    fn test_crossing_orcon_needs_approval() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::OrCon,
            vec![],
            vec![],
        );
        let result = engine.check_crossing(&handling_restriction, &CountryCode::FR, &CountryCode::DE);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::OrConRequiresApproval { .. }
        ));
    }

    #[test]
    fn test_crossing_eyes_only_allowed() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::TopSecret,
            ReleasabilityMarking::EyesOnly(vec![CountryCode::US, CountryCode::GB]),
            vec![],
            vec![],
        );
        assert!(engine
            .check_crossing(&handling_restriction, &CountryCode::US, &CountryCode::GB)
            .is_ok());
    }

    #[test]
    fn test_crossing_eyes_only_denied() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::TopSecret,
            ReleasabilityMarking::EyesOnly(vec![CountryCode::US, CountryCode::GB]),
            vec![],
            vec![],
        );
        let result = engine.check_crossing(&handling_restriction, &CountryCode::US, &CountryCode::FR);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::EyesOnlyViolation { .. }
        ));
    }

    #[test]
    fn test_crossing_relto_org_member() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelToOrg(AllianceOrg::EU),
            vec![],
            vec![],
        );
        // FR is an EU member.
        assert!(engine
            .check_crossing(&handling_restriction, &CountryCode::DE, &CountryCode::FR)
            .is_ok());
    }

    #[test]
    fn test_crossing_relto_org_non_member() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelToOrg(AllianceOrg::EU),
            vec![],
            vec![],
        );
        // US is not an EU member.
        let result = engine.check_crossing(&handling_restriction, &CountryCode::DE, &CountryCode::US);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::NationalityNotInReleasability { .. }
        ));
    }

    #[test]
    fn test_crossing_five_eyes_member() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::RelToOrg(AllianceOrg::ALLIANCE_FVEY_EQ),
            vec![],
            vec![],
        );
        // AU is a Five Eyes member.
        assert!(engine
            .check_crossing(&handling_restriction, &CountryCode::US, &CountryCode::AU)
            .is_ok());
    }

    #[test]
    fn test_crossing_five_eyes_non_member() {
        let engine = HandlingRestrictionEngine::new();
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::RelToOrg(AllianceOrg::ALLIANCE_FVEY_EQ),
            vec![],
            vec![],
        );
        // FR is not a Five Eyes member.
        let result = engine.check_crossing(&handling_restriction, &CountryCode::US, &CountryCode::FR);
        assert!(result.is_err());
    }

    // ---- combined access checks ----

    #[test]
    fn test_access_full_check_pass() {
        let engine = HandlingRestrictionEngine::new();
        let creds = AccessCredentials {
            clearance_level: ClassificationLevel::TopSecret,
            nationality: CountryCode::US,
            compartment_access: vec!["SCI-X".into(), "SCI-Y".into()],
            organization: Some(AllianceOrg::ALLIANCE_FVEY_EQ),
        };
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Secret,
            ReleasabilityMarking::RelToOrg(AllianceOrg::ALLIANCE_FVEY_EQ),
            vec![make_compartment("SCI-X")],
            vec![HandlingHandlingRestriction::CryptoMarked],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    #[test]
    fn test_access_full_check_fail_clearance_first() {
        let engine = HandlingRestrictionEngine::new();
        let creds = AccessCredentials {
            clearance_level: ClassificationLevel::Restricted,
            nationality: CountryCode::US,
            compartment_access: vec![],
            organization: None,
        };
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::TopSecret,
            ReleasabilityMarking::NoForn,
            vec![make_compartment("MISSING")],
            vec![],
        );
        // Should fail on clearance first (before compartment check).
        let result = engine.check_access(&creds, &handling_restriction);
        assert!(matches!(
            result.unwrap_err(),
            HandlingRestrictionError::InsufficientClearance { .. }
        ));
    }

    #[test]
    fn test_access_no_compartments_no_handling() {
        let engine = HandlingRestrictionEngine::new();
        let creds = make_credentials(
            ClassificationLevel::Confidential,
            CountryCode::DE,
            vec![],
        );
        let handling_restriction = make_handling_restriction(
            ClassificationLevel::Restricted,
            ReleasabilityMarking::RelTo(vec![CountryCode::DE, CountryCode::FR]),
            vec![],
            vec![],
        );
        assert!(engine.check_access(&creds, &handling_restriction).is_ok());
    }

    // ---- Display tests ----

    #[test]
    fn test_handling_restriction_error_display() {
        let err = HandlingRestrictionError::InsufficientClearance {
            required: ClassificationLevel::Secret,
            actual: ClassificationLevel::Restricted,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("insufficient clearance"));

        let err2 = HandlingRestrictionError::NoFornViolation;
        assert_eq!(format!("{}", err2), "NOFORN violation");

        let err3 = HandlingRestrictionError::MissingCompartmentAccess("SCI-X".into());
        assert!(format!("{}", err3).contains("SCI-X"));

        let err4 = HandlingRestrictionError::EyesOnlyViolation {
            allowed: vec![CountryCode::US, CountryCode::GB],
        };
        let msg4 = format!("{}", err4);
        assert!(msg4.contains("US"));
        assert!(msg4.contains("GB"));
    }
}
