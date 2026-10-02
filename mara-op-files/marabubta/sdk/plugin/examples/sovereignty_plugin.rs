// Marabunta - Licensed under the MIT License.
//! French defence data-residency sovereignty plugin.
//!
//! Implements [`SovereigntyPlugin`] to enforce that:
//!
//! - All data resides exclusively on French soil.
//! - RESTRICTED data may flow outbound to EU/EU_STRATEGIC_PACT allies only.
//! - CONFIDENTIAL and above may never leave the `fr-mil` zone.
//! - Outbound data to EU allies is re-encrypted and PII is anonymised.
//! - Key release requires GovCloud or higher and proof-level >= 3.
//! - Maximum classification ceiling is SECRET (Top Secret handled by a
//!   separate air-gapped pipeline).

use marabunta_plugin_sdk::prelude::*;

// ---------------------------------------------------------------------------
// Plugin definition
// ---------------------------------------------------------------------------

/// French Ministry of Defence data-residency policy.
///
/// Designed for EU_STRATEGIC_PACT collaborative projects where France is the
/// data-originating nation and RESTRICTED-level intelligence may be
/// shared with participating EU member states.
#[marabunta_plugin]
#[derive(Default)]
struct FrenchDefenceSovereignty;

impl SovereigntyPlugin for FrenchDefenceSovereignty {
    /// Data may only be processed on nodes located in France.
    fn jurisdictions(&self) -> JurisdictionSet {
        JurisdictionSet::Only(vec![CountryCode::FR])
    }

    /// Membrane crossing rules for the `fr-mil` zone.
    fn crossing_rules(&self) -> Vec<CrossingRule> {
        vec![
            // Rule 1 (priority 100): Allow RESTRICTED data outbound from
            // fr-mil to eu-pesco zone for EU/EU_STRATEGIC_PACT allies.  Data is
            // re-encrypted for the target zone and PII fields are anonymised.
            CrossingRule {
                source_zone: "fr-mil".into(),
                destination_zone: "eu-pesco".into(),
                direction: CrossingDirection::Outbound,
                max_classification: ClassificationLevel::Restricted,
                allowed_jurisdictions: JurisdictionSet::AnyOf(vec![
                    JurisdictionSet::Agreement(vec![AllianceOrg::EU]),
                    JurisdictionSet::Agreement(vec![AllianceOrg::EU_STRATEGIC_PACT]),
                ]),
                transforms: vec![
                    MembraneTransform::ReEncrypt {
                        target_zone: "eu-pesco".into(),
                    },
                    MembraneTransform::Anonymize {
                        fields: vec![
                            FieldPattern("$.personnel.name".into()),
                            FieldPattern("$.personnel.rank".into()),
                            FieldPattern("$.personnel.unit_id".into()),
                        ],
                    },
                ],
                action: CrossingAction::Allow,
                priority: 100,
            },
            // Rule 2 (priority 200): Deny any CONFIDENTIAL or higher data
            // from leaving the fr-mil zone, regardless of destination.
            CrossingRule {
                source_zone: "fr-mil".into(),
                destination_zone: "*".into(),
                direction: CrossingDirection::Outbound,
                max_classification: ClassificationLevel::Secret,
                allowed_jurisdictions: JurisdictionSet::Only(vec![CountryCode::FR]),
                transforms: vec![],
                action: CrossingAction::Deny,
                priority: 200,
            },
            // Rule 3 (priority 50): Allow UNCLASSIFIED inbound data from
            // any EU member state into the fr-mil zone (e.g. open-source
            // intelligence feeds).
            CrossingRule {
                source_zone: "*".into(),
                destination_zone: "fr-mil".into(),
                direction: CrossingDirection::Inbound,
                max_classification: ClassificationLevel::Unclassified,
                allowed_jurisdictions: JurisdictionSet::Agreement(vec![AllianceOrg::EU]),
                transforms: vec![],
                action: CrossingAction::Allow,
                priority: 50,
            },
        ]
    }

    /// Keys may only be released to GovCloud (or higher) nodes in France
    /// that pass proof-level 3 attestation.
    fn key_release_policy(&self) -> KeyReleasePolicy {
        KeyReleasePolicy {
            min_proof_level: 3,
            min_zone_class: ZoneClass::GovCloud,
            allowed_jurisdictions: JurisdictionSet::Only(vec![CountryCode::FR]),
            require_classification_check: true,
        }
    }

    /// Default outbound transforms: redact geolocation and anonymise
    /// personnel records.
    fn data_transforms(&self) -> Vec<MembraneTransform> {
        vec![
            MembraneTransform::Redact {
                field_pattern: FieldPattern("$.metadata.geolocation".into()),
                commitment: true,
            },
            MembraneTransform::Anonymize {
                fields: vec![
                    FieldPattern("$.personnel.*".into()),
                ],
            },
        ]
    }

    /// Classification constraints: ceiling is SECRET, releasable to France
    /// only by default.
    fn classification_constraints(&self) -> ClassificationPolicy {
        ClassificationPolicy {
            max_level: ClassificationLevel::Secret,
            required_compartments: vec!["FR-DEF".into()],
            releasability: Some(ReleasabilityMarking::RelTo(vec![CountryCode::FR])),
        }
    }

    /// Maximum classification this plugin is authorised to handle.
    fn max_classification(&self) -> ClassificationLevel {
        ClassificationLevel::Secret
    }
}

// ---------------------------------------------------------------------------
// Binary entry point (required for `cargo run --example sovereignty_plugin`)
// ---------------------------------------------------------------------------

fn main() {}
