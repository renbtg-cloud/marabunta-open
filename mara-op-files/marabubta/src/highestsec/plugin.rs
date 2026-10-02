// Marabunta - Licensed under the MIT License.
use serde::{Serialize, Deserialize};
use crate::highestsec::jurisdiction::*;
use crate::highestsec::classification::*;

// ==================== COMPUTATION PLUGIN ====================

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

/// For audit logging — coarse kind without detail string.
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

// ==================== SOVEREIGNTY PLUGIN ====================

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

// ==================== VALIDATION PLUGIN ====================

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

/// Opaque attestation — contents defined by blind.rs.
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

// ==================== SHARED TYPES ====================

/// Plugin kind — determines sandbox and trust level.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginKind { Computation, Sovereignty, Validation }

/// Opaque identifiers.
pub type PluginId = String;
pub type ZoneId = String;
pub type KeyId = String;
pub type JobId = String;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::types::CountryCode;

    #[test]
    fn test_plugin_error_kind_conversion() {
        assert_eq!(PluginErrorKind::from(&PluginError::InvalidInput), PluginErrorKind::InvalidInput);
        assert_eq!(PluginErrorKind::from(&PluginError::InvalidParams), PluginErrorKind::InvalidParams);
        assert_eq!(PluginErrorKind::from(&PluginError::ResourceExhausted), PluginErrorKind::ResourceExhausted);
        assert_eq!(PluginErrorKind::from(&PluginError::Internal("x".into())), PluginErrorKind::Internal);
    }

    #[test]
    fn test_criticality_jury_size() {
        assert_eq!(CriticalityLevel::Standard.jury_size(), 3);
        assert_eq!(CriticalityLevel::Elevated.jury_size(), 5);
        assert_eq!(CriticalityLevel::Critical.jury_size(), 7);
        assert_eq!(CriticalityLevel::Extreme.jury_size(), 11);
    }

    #[test]
    fn test_criticality_quorum() {
        assert_eq!(CriticalityLevel::Standard.quorum(), 2);
        assert_eq!(CriticalityLevel::Elevated.quorum(), 4);
        assert_eq!(CriticalityLevel::Critical.quorum(), 6);
        assert_eq!(CriticalityLevel::Extreme.quorum(), 10);
    }

    #[test]
    fn test_criticality_ordering() {
        assert!(CriticalityLevel::Standard < CriticalityLevel::Elevated);
        assert!(CriticalityLevel::Elevated < CriticalityLevel::Critical);
        assert!(CriticalityLevel::Critical < CriticalityLevel::Extreme);
    }

    #[test]
    fn test_crossing_rule_serialization() {
        let rule = CrossingRule {
            source_zone: "zone-a".into(),
            destination_zone: "zone-b".into(),
            direction: CrossingDirection::Outbound,
            max_classification: ClassificationLevel::Restricted,
            allowed_jurisdictions: JurisdictionSet::Only(vec![CountryCode::FR]),
            transforms: vec![MembraneTransform::ReEncrypt { target_zone: "zone-b".into() }],
            action: CrossingAction::Allow,
            priority: 10,
        };
        let json = serde_json::to_string(&rule).unwrap();
        let rule2: CrossingRule = serde_json::from_str(&json).unwrap();
        assert_eq!(rule2.priority, 10);
    }

    #[test]
    fn test_membrane_transform_variants() {
        let transforms = vec![
            MembraneTransform::ReEncrypt { target_zone: "z".into() },
            MembraneTransform::Redact { field_pattern: FieldPattern("*.ssn".into()), commitment: true },
            MembraneTransform::Anonymize { fields: vec![FieldPattern("*.name".into())] },
            MembraneTransform::Declassify {
                from: ClassificationLevel::Secret,
                to: ClassificationLevel::Restricted,
                authority: AuthorityChain { authorities: vec![] },
            },
            MembraneTransform::Aggregate { method: AggregationMethod::Count },
            MembraneTransform::Custom { transform_plugin_id: "custom.plugin".into() },
        ];
        for t in &transforms {
            let json = serde_json::to_string(t).unwrap();
            let _t2: MembraneTransform = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_verdict_confidence_range() {
        let v = Verdict {
            decision: VerdictDecision::Accept,
            confidence: 0.95,
            reason: VerdictReason { code: "OK".into(), category: "validation".into() },
            explanation: "passed".into(),
        };
        assert!(v.confidence >= 0.0 && v.confidence <= 1.0);
    }

    #[test]
    fn test_plugin_kind_serialization() {
        for kind in [PluginKind::Computation, PluginKind::Sovereignty, PluginKind::Validation] {
            let json = serde_json::to_string(&kind).unwrap();
            let kind2: PluginKind = serde_json::from_str(&json).unwrap();
            assert_eq!(kind, kind2);
        }
    }
}
