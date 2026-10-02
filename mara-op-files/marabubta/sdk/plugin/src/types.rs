// Marabunta - Licensed under the MIT License.
//! Public types for the marabunta-plugin-sdk.
//!
//! This module contains standalone copies of the highestsec subsystem types that
//! plugin contractors need. These are **copies**, not re-exports, so the SDK
//! crate has no dependency on the main marabunta-compute crate.

use serde::{Serialize, Deserialize};

// ============================================================================
// types.rs — CountryCode
// ============================================================================

/// ISO 3166-1 alpha-2 country code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CountryCode(pub [u8; 2]);

impl CountryCode {
    pub const FR: Self = Self(*b"FR");
    pub const DE: Self = Self(*b"DE");
    pub const ES: Self = Self(*b"ES");
    pub const IT: Self = Self(*b"IT");
    pub const US: Self = Self(*b"US");
    pub const GB: Self = Self(*b"GB");
    pub const NL: Self = Self(*b"NL");
    pub const BE: Self = Self(*b"BE");
    pub const PL: Self = Self(*b"PL");
    pub const RO: Self = Self(*b"RO");
    pub const SE: Self = Self(*b"SE");
    pub const PT: Self = Self(*b"PT");
    pub const GR: Self = Self(*b"GR");
    pub const CZ: Self = Self(*b"CZ");
    pub const HU: Self = Self(*b"HU");
    pub const AT: Self = Self(*b"AT");
    pub const BG: Self = Self(*b"BG");
    pub const DK: Self = Self(*b"DK");
    pub const FI: Self = Self(*b"FI");
    pub const SK: Self = Self(*b"SK");
    pub const IE: Self = Self(*b"IE");
    pub const HR: Self = Self(*b"HR");
    pub const LT: Self = Self(*b"LT");
    pub const SI: Self = Self(*b"SI");
    pub const LV: Self = Self(*b"LV");
    pub const EE: Self = Self(*b"EE");
    pub const CY: Self = Self(*b"CY");
    pub const LU: Self = Self(*b"LU");
    pub const MT: Self = Self(*b"MT");
    pub const CA: Self = Self(*b"CA");
    pub const AU: Self = Self(*b"AU");
    pub const NZ: Self = Self(*b"NZ");
    pub const TR: Self = Self(*b"TR");
    pub const NO: Self = Self(*b"NO");
    pub const IS: Self = Self(*b"IS");
    pub const ME: Self = Self(*b"ME");
    pub const MK: Self = Self(*b"MK");
    pub const AL: Self = Self(*b"AL");

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("??")
    }
}

impl std::fmt::Display for CountryCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialOrd for CountryCode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CountryCode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

// ============================================================================
// jurisdiction.rs — ZoneClass, Jurisdiction, NationalAuthority, AllianceOrg,
//                   JurisdictionSet, AllianceResolver, StaticAllianceResolver
// ============================================================================

/// ZoneClass -- what kind of infrastructure.
/// Ordering: Civilian < GovCloud < MilRestricted < MilClassified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ZoneClass {
    Civilian = 0,
    GovCloud = 1,
    MilRestricted = 2,
    MilClassified = 3,
}

/// Jurisdiction -- country + zone class + optional authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Jurisdiction {
    pub country: CountryCode,
    pub zone_class: ZoneClass,
    pub authority: Option<NationalAuthority>,
}

/// NationalAuthority -- recognized security authority for a jurisdiction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NationalAuthority {
    pub id: String,
    pub country: CountryCode,
    pub public_key: Vec<u8>,
    pub name: String,
}

/// AllianceOrg -- agreement organizations.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AllianceOrg {
    EU,
    GLOBAL_ALLIANCE_T1,
    ALLIANCE_FVEY_EQ,
    EU_STRATEGIC_PACT,
    Custom(String),
}

/// JurisdictionSet -- composable jurisdiction constraints.
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
            Self::Except(_) => false,
            Self::Agreement(orgs) => orgs.iter().all(|org| resolver.members(org).is_empty()),
            Self::AllOf(sets) => sets.iter().any(|s| s.is_empty(resolver)),
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

// ============================================================================
// classification.rs — ClassificationLevel, HandlingRestriction, ReleasabilityMarking,
//                     Compartment, HandlingHandlingRestriction, ClassificationRequirements
// ============================================================================

/// GLOBAL_ALLIANCE_T1/EU classification levels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ClassificationLevel {
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

impl Default for ClassificationLevel {
    fn default() -> Self {
        Self::Unclassified
    }
}

/// Releasability marking.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReleasabilityMarking {
    RelTo(Vec<CountryCode>),
    RelToOrg(AllianceOrg),
    NoForn,
    OrCon,
    EyesOnly(Vec<CountryCode>),
}

/// Compartment -- named sub-classification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Compartment {
    pub id: String,
    pub controlling_authority: NationalAuthority,
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

// ============================================================================
// plugin.rs — ComputePlugin, SovereigntyPlugin, ValidationPlugin,
//             PluginError, PluginErrorKind, PluginKind, CriticalityLevel,
//             and all associated types
// ============================================================================

/// The ONLY trait contractors implement for computation plugins.
pub trait ComputePlugin {
    fn execute(&self, input: &[u8], params: &[u8]) -> Result<Vec<u8>, PluginError>;
}

/// Coarse-grained errors to prevent information leakage.
#[derive(Debug, Clone)]
pub enum PluginError {
    InvalidInput,
    InvalidParams,
    ResourceExhausted,
    Internal(String),
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput => write!(f, "invalid input"),
            Self::InvalidParams => write!(f, "invalid params"),
            Self::ResourceExhausted => write!(f, "resource exhausted"),
            Self::Internal(msg) => write!(f, "internal: {}", msg),
        }
    }
}

/// For audit logging -- coarse kind without detail string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginErrorKind {
    InvalidInput,
    InvalidParams,
    ResourceExhausted,
    Internal,
}

impl From<&PluginError> for PluginErrorKind {
    fn from(e: &PluginError) -> Self {
        match e {
            PluginError::InvalidInput => Self::InvalidInput,
            PluginError::InvalidParams => Self::InvalidParams,
            PluginError::ResourceExhausted => Self::ResourceExhausted,
            PluginError::Internal(_) => Self::Internal,
        }
    }
}

/// Sovereignty plugins define WHERE data can go.
pub trait SovereigntyPlugin {
    fn jurisdictions(&self) -> JurisdictionSet;
    fn crossing_rules(&self) -> Vec<CrossingRule>;
    fn key_release_policy(&self) -> KeyReleasePolicy;
    fn data_transforms(&self) -> Vec<MembraneTransform>;
    fn classification_constraints(&self) -> ClassificationPolicy;
    fn max_classification(&self) -> ClassificationLevel;
}

/// Rule governing data at a membrane boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrossingRule {
    pub source_zone: ZoneId,
    pub destination_zone: ZoneId,
    pub direction: CrossingDirection,
    pub max_classification: ClassificationLevel,
    pub allowed_jurisdictions: JurisdictionSet,
    pub transforms: Vec<MembraneTransform>,
    pub action: CrossingAction,
    pub priority: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrossingDirection { Inbound, Outbound, Both }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CrossingAction { Allow, Deny }

/// Transforms applied at membrane boundaries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MembraneTransform {
    ReEncrypt { target_zone: ZoneId },
    Redact { field_pattern: FieldPattern, commitment: bool },
    Anonymize { fields: Vec<FieldPattern> },
    Declassify {
        from: ClassificationLevel,
        to: ClassificationLevel,
        authority: AuthorityChain,
    },
    Aggregate { method: AggregationMethod },
    Custom { transform_plugin_id: PluginId },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldPattern(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggregationMethod { Count, Sum, Mean, Median, Min, Max }

/// Policy for when decryption keys may be released.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyReleasePolicy {
    pub min_proof_level: u8,
    pub min_zone_class: ZoneClass,
    pub allowed_jurisdictions: JurisdictionSet,
    pub require_classification_check: bool,
}

/// Classification constraints a sovereignty plugin enforces.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClassificationPolicy {
    pub max_level: ClassificationLevel,
    pub required_compartments: Vec<String>,
    pub releasability: Option<ReleasabilityMarking>,
}

/// Authority chain for declassification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthorityChain {
    pub authorities: Vec<AuthoritySignature>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthoritySignature {
    pub authority_id: String,
    pub public_key: Vec<u8>,
    pub signature: Vec<u8>,
    pub signed_at: String, // ISO 8601
}

/// Validation plugins provide domain-specific result verification.
pub trait ValidationPlugin {
    fn verify(
        &self,
        output: &[u8],
        attestation: &Attestation,
        reference_data: &[u8],
    ) -> Verdict;

    fn criticality(&self) -> CriticalityLevel;
    fn reference_data_manifest(&self) -> ReferenceDataManifest;
}

/// Opaque attestation -- contents defined by blind.rs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attestation {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub decision: VerdictDecision,
    pub confidence: f64,
    pub reason: VerdictReason,
    pub explanation: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerdictDecision { Accept, Reject, Inconclusive }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerdictReason {
    pub code: String,
    pub category: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CriticalityLevel {
    Standard,
    Elevated,
    Critical,
    Extreme,
}

impl CriticalityLevel {
    pub fn jury_size(&self) -> u32 {
        match self {
            Self::Standard => 3,
            Self::Elevated => 5,
            Self::Critical => 7,
            Self::Extreme => 11,
        }
    }

    pub fn quorum(&self) -> u32 {
        match self {
            Self::Standard => 2,
            Self::Elevated => 4,
            Self::Critical => 6,
            Self::Extreme => 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceDataManifest {
    pub data_id: String,
    pub hash: [u8; 32],
    pub size_bytes: u64,
    pub description: String,
}

/// Plugin kind -- determines sandbox and trust level.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginKind { Computation, Sovereignty, Validation }

/// Opaque identifiers.
pub type PluginId = String;
pub type ZoneId = String;
pub type KeyId = String;
pub type JobId = String;

// ============================================================================
// manifest.rs — SemVer, Ed25519Signature, PluginManifest, ManifestBuilder,
//               PluginState, ManifestValidationError
// ============================================================================

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

impl PluginManifest {
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

    /// Check if the manifest has expired by parsing `expires_at` as RFC 3339 /
    /// ISO 8601 and comparing with `Utc::now()`.
    pub fn is_expired(&self) -> bool {
        use chrono::{DateTime, Utc};
        match self.expires_at.parse::<DateTime<Utc>>() {
            Ok(expiry) => Utc::now() > expiry,
            Err(_) => true,
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

// ============================================================================
// constraints.rs — DataConstraint, ConstraintSet, NonexportTracking,
//                  CustodyRecord, CustodyOperation
// ============================================================================

/// A single data constraint describing jurisdictional or sovereignty requirements.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataConstraint {
    /// Data may only be decrypted within these jurisdictions.
    DecryptionBoundary(JurisdictionSet),
    /// Data may transit through these jurisdictions.
    TransitAllowed(JurisdictionSet),
    /// Data must NOT transit through these jurisdictions.
    TransitDenied(JurisdictionSet),
    /// Results may only be consolidated (aggregated) within these jurisdictions.
    ConsolidationBoundary(JurisdictionSet),
    /// Cryptographic keys must reside in these jurisdictions.
    KeyResidency(JurisdictionSet),
    /// NONEXPORT tainted data: originated in a specific jurisdiction and may only go
    /// to approved recipients.
    NonexportTainted {
        original_jurisdiction: CountryCode,
        approved_recipients: JurisdictionSet,
    },
    /// Temporal jurisdiction: constraints that apply under a specific legal
    /// regime at a given point in time, allowing nesting.
    TemporalJurisdiction {
        jurisdiction: CountryCode,
        applicable_law_date: String,
        constraints_at_time: Box<DataConstraint>,
    },
}

/// A composed set of constraints representing the high-water-mark union of all
/// input constraint sets for a particular data pipeline.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConstraintSet {
    /// Jurisdictions where decryption is permitted (intersection of inputs).
    pub decryption_boundary: Option<JurisdictionSet>,
    /// Jurisdictions where transit is allowed.
    pub transit_allowed: Option<JurisdictionSet>,
    /// Jurisdictions where transit is denied (union of inputs).
    pub transit_denied: Option<JurisdictionSet>,
    /// Jurisdictions where consolidation is permitted (intersection of inputs).
    pub consolidation_boundary: Option<JurisdictionSet>,
    /// Jurisdictions where keys must reside (intersection of inputs).
    pub key_residency: Option<JurisdictionSet>,
    /// NONEXPORT tracking state.
    pub nonexport: Option<NonexportTracking>,
    /// Classification level (max of inputs).
    pub classification: ClassificationLevel,
    /// Compartments (union of inputs, deduplicated).
    pub compartments: Vec<String>,
}

/// Tracks NONEXPORT taint propagation through the compute pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NonexportTracking {
    /// Whether data is NONEXPORT-tainted.
    pub is_tainted: bool,
    /// The job IDs that introduced the taint.
    pub taint_source: Vec<JobId>,
    /// Jurisdictions approved to receive NONEXPORT-controlled data.
    pub approved_jurisdictions: JurisdictionSet,
    /// NONEXPORT control category (e.g., EXPORT_CATEGORY_A category).
    pub control_category: Option<String>,
    /// Chain of custody records.
    pub custody_chain: Vec<CustodyRecord>,
}

impl NonexportTracking {
    /// Create a clean (untainted) NONEXPORT tracking record.
    pub fn clean() -> Self {
        Self {
            is_tainted: false,
            taint_source: Vec::new(),
            approved_jurisdictions: JurisdictionSet::Any,
            control_category: None,
            custody_chain: Vec::new(),
        }
    }

    /// Whether this data is NONEXPORT-tainted.
    pub fn is_tainted(&self) -> bool {
        self.is_tainted
    }
}

/// A record of data custody at a particular node and time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CustodyRecord {
    /// Node that held custody.
    pub node_id: Vec<u8>,
    /// Jurisdiction of the node.
    pub jurisdiction: CountryCode,
    /// Proof level established.
    pub proof_level: u8,
    /// ISO 8601 timestamp.
    pub timestamp: String,
    /// What operation was performed.
    pub operation: CustodyOperation,
}

/// Operations that generate custody records.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CustodyOperation {
    Received,
    Executed,
    Forwarded,
    Consolidated,
    Delivered,
}

// ============================================================================
// audit_events.rs — HighestsecAuditEventKind (enum only)
// ============================================================================

/// The 22 kinds of audit events that can occur in the highestsec subsystem.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HighestsecAuditEventKind {
    /// Plugin loaded into sandbox.
    PluginLoaded {
        plugin_id: PluginId,
        version: SemVer,
        kind: PluginKind,
        binary_hash: [u8; 32],
    },
    /// Plugin load failed.
    PluginLoadFailed {
        plugin_id: PluginId,
        reason: LoadFailureReason,
    },
    /// Plugin execution started.
    ExecutionStarted {
        job_id: JobId,
        plugin_id: PluginId,
        classification: ClassificationLevel,
    },
    /// Plugin execution completed successfully.
    ExecutionCompleted {
        job_id: JobId,
        plugin_id: PluginId,
        output_hash: [u8; 32],
        execution_ms: u64,
    },
    /// Plugin execution failed.
    ExecutionFailed {
        job_id: JobId,
        plugin_id: PluginId,
        error_kind: PluginErrorKind,
    },
    /// Validation verdict rendered.
    ValidationVerdict {
        job_id: JobId,
        validator_id: PluginId,
        decision: VerdictDecision,
        confidence: f64,
    },
    /// Data crossed a membrane boundary.
    MembraneCrossing {
        job_id: JobId,
        source_zone: ZoneId,
        destination_zone: ZoneId,
        classification: ClassificationLevel,
        transforms_applied: Vec<String>,
    },
    /// Cryptographic key released.
    KeyRelease {
        key_id: String,
        zone_id: ZoneId,
        proof_level: u8,
        classification: ClassificationLevel,
    },
    /// Key release denied.
    KeyReleaseDenied {
        key_id: String,
        zone_id: ZoneId,
        reason: String,
    },
    /// Attestation generated.
    AttestationGenerated {
        job_id: JobId,
        attestation_hash: [u8; 32],
    },
    /// Attestation verified.
    AttestationVerified {
        job_id: JobId,
        attestation_hash: [u8; 32],
        verified: bool,
    },
    /// Classification data_leakage detected.
    DataLeakageDetected {
        job_id: JobId,
        severity: DataLeakageSeverity,
        source_classification: ClassificationLevel,
        target_classification: ClassificationLevel,
        containment_action: String,
    },
    /// Plugin revoked.
    PluginRevoked {
        plugin_id: PluginId,
        reason: RevocationReason,
        authority_id: String,
    },
    /// Jurisdiction violation detected.
    JurisdictionViolation {
        job_id: JobId,
        expected_jurisdiction: CountryCode,
        actual_jurisdiction: CountryCode,
        action_taken: String,
    },
    /// NONEXPORT taint propagated.
    NonexportTaintPropagated {
        job_id: JobId,
        source_job: JobId,
        taint_chain_length: u32,
    },
    /// Node joined highestsec zone.
    NodeJoinedZone {
        zone_id: ZoneId,
        proof_level: u8,
    },
    /// Node left highestsec zone.
    NodeLeftZone {
        zone_id: ZoneId,
        reason: String,
    },
    /// Classification upgraded.
    ClassificationUpgrade {
        job_id: JobId,
        from: ClassificationLevel,
        to: ClassificationLevel,
        authority_id: String,
    },
    /// Classification downgrade (declassification).
    ClassificationDowngrade {
        job_id: JobId,
        from: ClassificationLevel,
        to: ClassificationLevel,
        authority_chain: Vec<String>,
    },
    /// Sandbox resource limit hit.
    ResourceLimitHit {
        job_id: JobId,
        plugin_id: PluginId,
        resource: String,
        limit: u64,
        actual: u64,
    },
    /// Criticality level changed.
    CriticalityChanged {
        plugin_id: PluginId,
        from: CriticalityLevel,
        to: CriticalityLevel,
    },
    /// Chain checkpoint (periodic integrity marker).
    ChainCheckpoint {
        chain_length: u64,
        cumulative_hash: [u8; 32],
    },
}

/// Reasons a plugin load can fail.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LoadFailureReason {
    BinaryHashMismatch,
    SignatureInvalid,
    Expired,
    Revoked,
    InsufficientZoneClass,
    JurisdictionDenied,
    ValidationFailed(String),
}

/// Reasons a plugin can be revoked.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RevocationReason {
    SecurityVulnerability,
    PolicyViolation,
    Compromised,
    Superseded { replacement_id: PluginId },
    Expired,
    Administrative(String),
}

/// Severity of a classification data_leakage incident.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DataLeakageSeverity {
    /// Minor (e.g. Restricted data in Unclassified zone).
    Minor = 0,
    /// Major (e.g. Secret data in lower-classified zone).
    Major = 1,
    /// Critical (e.g. Top Secret data leaked).
    Critical = 2,
}
