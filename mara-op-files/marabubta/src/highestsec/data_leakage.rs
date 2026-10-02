// Marabunta - Licensed under the MIT License.
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::highestsec::audit_events::DataLeakageSeverity;
use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::jurisdiction_proof::NationalAuthorityId;
use crate::highestsec::plugin::{KeyId, ZoneId};
use crate::highestsec::zone_membership::ZoneCertificateStore;

// ---------------------------------------------------------------------------
// DataLeakageDetector
// ---------------------------------------------------------------------------

/// Detects classification data_leakage -- when classified data ends up in
/// infrastructure not cleared to handle it.
///
/// Three check points:
/// - **Pre-execution**: before a job runs, verify that the executing node is
///   cleared for the job's classification level.
/// - **Post-execution**: after a job completes, verify that the output
///   classification does not exceed the zone's maximum.
/// - **Transit**: when data moves through an intermediate zone, verify
///   encrypted transit or matching clearance.
pub struct DataLeakageDetector {
    cert_store: Arc<ZoneCertificateStore>,
}

impl DataLeakageDetector {
    /// Create a new detector backed by the given certificate store.
    pub fn new(cert_store: Arc<ZoneCertificateStore>) -> Self {
        Self { cert_store }
    }

    /// Pre-execution check: verify that the node identified by `node_id`
    /// within zone `zone_id` is authorized to handle data at
    /// `job_classification`.
    ///
    /// If the node has a valid certificate and its `max_classification` is
    /// at least `job_classification`, returns `DataLeakageCheck::Clear`.
    /// Otherwise, returns a `DataLeakageIncident`.
    pub fn check_pre_execution(
        &self,
        job_classification: ClassificationLevel,
        node_id: &[u8],
        zone_id: &str,
    ) -> DataLeakageCheck {
        let infra_level = self
            .cert_store
            .get_valid(node_id)
            .map(|cert| cert.max_classification)
            .unwrap_or(ClassificationLevel::Unclassified);

        if infra_level >= job_classification {
            return DataLeakageCheck::Clear;
        }

        let severity =
            DataLeakageIncident::compute_severity(job_classification, infra_level);

        let mut recommended_actions = vec![
            DataLeakageAction::QuarantineNodes(vec![node_id.to_vec()]),
        ];

        if severity >= DataLeakageSeverity::Major {
            recommended_actions.push(DataLeakageAction::ForensicDump(vec![node_id.to_vec()]));
        }

        if severity == DataLeakageSeverity::Critical {
            recommended_actions.push(DataLeakageAction::EmergencyZeroize(vec![node_id.to_vec()]));
            recommended_actions.push(DataLeakageAction::NotifyAuthority(
                "national-authority".into(),
            ));
        }

        DataLeakageCheck::DataLeakage(DataLeakageIncident {
            severity,
            data_classification: job_classification,
            infrastructure_level: infra_level,
            affected_nodes: vec![node_id.to_vec()],
            affected_zones: vec![zone_id.to_string()],
            timestamp: chrono::Utc::now().to_rfc3339(),
            recommended_actions,
        })
    }

    /// Post-execution check: verify that the `output_classification` does
    /// not exceed the `zone_max` classification for the zone.
    pub fn check_post_execution(
        &self,
        output_classification: ClassificationLevel,
        zone_id: &str,
        zone_max: ClassificationLevel,
    ) -> DataLeakageCheck {
        if output_classification <= zone_max {
            return DataLeakageCheck::Clear;
        }

        let severity =
            DataLeakageIncident::compute_severity(output_classification, zone_max);

        let mut recommended_actions = Vec::new();
        recommended_actions.push(DataLeakageAction::RevokeZoneMembership(Vec::new()));

        if severity >= DataLeakageSeverity::Major {
            recommended_actions.push(DataLeakageAction::NotifyAuthority(
                "national-authority".into(),
            ));
        }

        if severity == DataLeakageSeverity::Critical {
            recommended_actions.push(DataLeakageAction::EmergencyZeroize(Vec::new()));
        }

        DataLeakageCheck::DataLeakage(DataLeakageIncident {
            severity,
            data_classification: output_classification,
            infrastructure_level: zone_max,
            affected_nodes: Vec::new(),
            affected_zones: vec![zone_id.to_string()],
            timestamp: chrono::Utc::now().to_rfc3339(),
            recommended_actions,
        })
    }

    /// Transit check: verify that data transiting through a zone is either
    /// encrypted (in which case the zone's clearance level does not matter)
    /// or the zone is cleared for the data's classification.
    pub fn check_transit(
        &self,
        data_classification: ClassificationLevel,
        is_encrypted: bool,
        zone_id: &str,
        zone_max: ClassificationLevel,
    ) -> DataLeakageCheck {
        // Encrypted transit through lower zones is acceptable.
        if is_encrypted {
            return DataLeakageCheck::Clear;
        }

        // Unencrypted transit: zone must be cleared.
        if data_classification <= zone_max {
            return DataLeakageCheck::Clear;
        }

        let severity =
            DataLeakageIncident::compute_severity(data_classification, zone_max);

        let mut recommended_actions = vec![
            DataLeakageAction::EmergencyKeyRotation(Vec::new()),
        ];

        if severity >= DataLeakageSeverity::Major {
            recommended_actions.push(DataLeakageAction::ForensicDump(Vec::new()));
            recommended_actions.push(DataLeakageAction::NotifyAuthority(
                "national-authority".into(),
            ));
        }

        if severity == DataLeakageSeverity::Critical {
            recommended_actions.push(DataLeakageAction::EmergencyZeroize(Vec::new()));
        }

        DataLeakageCheck::DataLeakage(DataLeakageIncident {
            severity,
            data_classification,
            infrastructure_level: zone_max,
            affected_nodes: Vec::new(),
            affected_zones: vec![zone_id.to_string()],
            timestamp: chrono::Utc::now().to_rfc3339(),
            recommended_actions,
        })
    }
}

// ---------------------------------------------------------------------------
// DataLeakageCheck
// ---------------------------------------------------------------------------

/// Result of a data_leakage check: either clear or a data_leakage incident.
#[derive(Debug)]
pub enum DataLeakageCheck {
    Clear,
    DataLeakage(DataLeakageIncident),
}

impl DataLeakageCheck {
    /// Returns `true` if this check found no data_leakage.
    pub fn is_clear(&self) -> bool {
        matches!(self, Self::Clear)
    }

    /// Returns the incident if data_leakage was detected.
    pub fn incident(&self) -> Option<&DataLeakageIncident> {
        match self {
            Self::Clear => None,
            Self::DataLeakage(incident) => Some(incident),
        }
    }
}

// ---------------------------------------------------------------------------
// DataLeakageIncident
// ---------------------------------------------------------------------------

/// Details of a detected classification data_leakage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataLeakageIncident {
    pub severity: DataLeakageSeverity,
    pub data_classification: ClassificationLevel,
    pub infrastructure_level: ClassificationLevel,
    pub affected_nodes: Vec<Vec<u8>>,
    pub affected_zones: Vec<ZoneId>,
    pub timestamp: String,
    pub recommended_actions: Vec<DataLeakageAction>,
}

impl DataLeakageIncident {
    /// Compute the severity of a data_leakage based on the gap between the data
    /// classification and the infrastructure level.
    ///
    /// - Gap of 0: Minor (defensive default).
    /// - Gap of 1: Minor.
    /// - TopSecret data in any lower zone: Critical.
    /// - Everything else with gap >= 2: Major.
    pub fn compute_severity(
        data_level: ClassificationLevel,
        infra_level: ClassificationLevel,
    ) -> DataLeakageSeverity {
        let gap = (data_level as u8).saturating_sub(infra_level as u8);
        match gap {
            0 => DataLeakageSeverity::Minor,
            1 => DataLeakageSeverity::Minor,
            _ if data_level == ClassificationLevel::TopSecret => DataLeakageSeverity::Critical,
            _ => DataLeakageSeverity::Major,
        }
    }
}

// ---------------------------------------------------------------------------
// DataLeakageAction
// ---------------------------------------------------------------------------

/// Recommended action to contain or remediate a data_leakage incident.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataLeakageAction {
    /// Isolate the affected nodes from the network.
    QuarantineNodes(Vec<Vec<u8>>),
    /// Emergency zeroization of cryptographic material on affected nodes.
    EmergencyZeroize(Vec<Vec<u8>>),
    /// Revoke zone membership for affected nodes.
    RevokeZoneMembership(Vec<Vec<u8>>),
    /// Notify the responsible national authority.
    NotifyAuthority(NationalAuthorityId),
    /// Capture forensic memory/state dumps from affected nodes.
    ForensicDump(Vec<Vec<u8>>),
    /// Emergency rotation of the specified keys.
    EmergencyKeyRotation(Vec<KeyId>),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::classification::ClassificationLevel;
    use crate::highestsec::zone_membership::{ZoneCertificateStore, ZoneMembershipCertificate};
    use std::sync::Arc;

    /// Helper: build a certificate store with a node at the given max classification.
    fn make_cert_store(
        node_id: &[u8],
        max_classification: ClassificationLevel,
    ) -> Arc<ZoneCertificateStore> {
        let store = ZoneCertificateStore::new();
        let zone_class = match max_classification {
            ClassificationLevel::Unclassified => {
                crate::highestsec::jurisdiction::ZoneClass::Civilian
            }
            ClassificationLevel::Restricted => {
                crate::highestsec::jurisdiction::ZoneClass::GovCloud
            }
            ClassificationLevel::Confidential => {
                crate::highestsec::jurisdiction::ZoneClass::MilRestricted
            }
            ClassificationLevel::Secret | ClassificationLevel::TopSecret => {
                crate::highestsec::jurisdiction::ZoneClass::MilClassified
            }
        };
        let cert = ZoneMembershipCertificate {
            node_id: node_id.to_vec(),
            zone_id: "test-zone".into(),
            jurisdiction: crate::highestsec::types::CountryCode::FR,
            zone_class,
            max_classification,
            proof_level: 3,
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

    // ---- pre-execution tests ----

    #[test]
    fn test_pre_execution_clear_restricted_on_govcloud() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(&node_id, ClassificationLevel::Restricted);
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_pre_execution(
            ClassificationLevel::Restricted,
            &node_id,
            "govcloud-zone",
        );
        assert!(result.is_clear());
    }

    #[test]
    fn test_pre_execution_clear_unclassified_on_restricted() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(&node_id, ClassificationLevel::Restricted);
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_pre_execution(
            ClassificationLevel::Unclassified,
            &node_id,
            "govcloud-zone",
        );
        assert!(result.is_clear());
    }

    #[test]
    fn test_pre_execution_detected_secret_on_govcloud() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(&node_id, ClassificationLevel::Restricted);
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_pre_execution(
            ClassificationLevel::Secret,
            &node_id,
            "govcloud-zone",
        );
        assert!(!result.is_clear());
        let incident = result.incident().unwrap();
        assert_eq!(incident.data_classification, ClassificationLevel::Secret);
        assert_eq!(
            incident.infrastructure_level,
            ClassificationLevel::Restricted
        );
        assert_eq!(incident.severity, DataLeakageSeverity::Major);
    }

    #[test]
    fn test_pre_execution_no_cert_treats_as_unclassified() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_pre_execution(
            ClassificationLevel::Restricted,
            &[9, 9, 9],
            "unknown-zone",
        );
        assert!(!result.is_clear());
        let incident = result.incident().unwrap();
        assert_eq!(
            incident.infrastructure_level,
            ClassificationLevel::Unclassified
        );
    }

    #[test]
    fn test_pre_execution_critical_top_secret_on_unclassified() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(&node_id, ClassificationLevel::Unclassified);
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_pre_execution(
            ClassificationLevel::TopSecret,
            &node_id,
            "civilian-zone",
        );
        assert!(!result.is_clear());
        let incident = result.incident().unwrap();
        assert_eq!(incident.severity, DataLeakageSeverity::Critical);
        // Critical should recommend emergency zeroize.
        let has_zeroize = incident.recommended_actions.iter().any(|a| {
            matches!(a, DataLeakageAction::EmergencyZeroize(_))
        });
        assert!(has_zeroize);
    }

    // ---- post-execution tests ----

    #[test]
    fn test_post_execution_clear() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_post_execution(
            ClassificationLevel::Restricted,
            "govcloud-zone",
            ClassificationLevel::Secret,
        );
        assert!(result.is_clear());
    }

    #[test]
    fn test_post_execution_equal_level_clear() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_post_execution(
            ClassificationLevel::Secret,
            "milclassified-zone",
            ClassificationLevel::Secret,
        );
        assert!(result.is_clear());
    }

    #[test]
    fn test_post_execution_elevated_output() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_post_execution(
            ClassificationLevel::Secret,
            "govcloud-zone",
            ClassificationLevel::Restricted,
        );
        assert!(!result.is_clear());
        let incident = result.incident().unwrap();
        assert_eq!(incident.data_classification, ClassificationLevel::Secret);
        assert_eq!(
            incident.infrastructure_level,
            ClassificationLevel::Restricted
        );
    }

    // ---- transit tests ----

    #[test]
    fn test_transit_encrypted_ok() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_transit(
            ClassificationLevel::Secret,
            true, // encrypted
            "civilian-zone",
            ClassificationLevel::Unclassified,
        );
        assert!(result.is_clear());
    }

    #[test]
    fn test_transit_decrypted_fail() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_transit(
            ClassificationLevel::Secret,
            false, // NOT encrypted
            "civilian-zone",
            ClassificationLevel::Unclassified,
        );
        assert!(!result.is_clear());
        let incident = result.incident().unwrap();
        assert_eq!(incident.data_classification, ClassificationLevel::Secret);
    }

    #[test]
    fn test_transit_unencrypted_within_clearance() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_transit(
            ClassificationLevel::Restricted,
            false,
            "govcloud-zone",
            ClassificationLevel::Secret, // zone can handle up to Secret
        );
        assert!(result.is_clear());
    }

    // ---- severity tests ----

    #[test]
    fn test_severity_minor_one_gap() {
        let severity = DataLeakageIncident::compute_severity(
            ClassificationLevel::Restricted,
            ClassificationLevel::Unclassified,
        );
        assert_eq!(severity, DataLeakageSeverity::Minor);
    }

    #[test]
    fn test_severity_minor_zero_gap() {
        let severity = DataLeakageIncident::compute_severity(
            ClassificationLevel::Restricted,
            ClassificationLevel::Restricted,
        );
        assert_eq!(severity, DataLeakageSeverity::Minor);
    }

    #[test]
    fn test_severity_major_two_gap() {
        let severity = DataLeakageIncident::compute_severity(
            ClassificationLevel::Secret,          // 3
            ClassificationLevel::Restricted,      // 1
        );
        assert_eq!(severity, DataLeakageSeverity::Major);
    }

    #[test]
    fn test_severity_critical_top_secret() {
        let severity = DataLeakageIncident::compute_severity(
            ClassificationLevel::TopSecret,       // 4
            ClassificationLevel::Restricted,      // 1
        );
        assert_eq!(severity, DataLeakageSeverity::Critical);
    }

    #[test]
    fn test_severity_critical_top_secret_on_unclassified() {
        let severity = DataLeakageIncident::compute_severity(
            ClassificationLevel::TopSecret,
            ClassificationLevel::Unclassified,
        );
        assert_eq!(severity, DataLeakageSeverity::Critical);
    }

    #[test]
    fn test_severity_major_confidential_on_unclassified() {
        let severity = DataLeakageIncident::compute_severity(
            ClassificationLevel::Confidential, // 2
            ClassificationLevel::Unclassified, // 0
        );
        assert_eq!(severity, DataLeakageSeverity::Major);
    }

    // ---- recommended actions tests ----

    #[test]
    fn test_recommended_actions_minor_pre_execution() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(&node_id, ClassificationLevel::Unclassified);
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_pre_execution(
            ClassificationLevel::Restricted,
            &node_id,
            "civilian-zone",
        );
        let incident = result.incident().unwrap();
        assert_eq!(incident.severity, DataLeakageSeverity::Minor);
        // Minor should have quarantine but no emergency zeroize.
        let has_quarantine = incident.recommended_actions.iter().any(|a| {
            matches!(a, DataLeakageAction::QuarantineNodes(_))
        });
        assert!(has_quarantine);
    }

    #[test]
    fn test_recommended_actions_critical_has_notify() {
        let node_id = vec![1, 2, 3];
        let store = make_cert_store(&node_id, ClassificationLevel::Unclassified);
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_pre_execution(
            ClassificationLevel::TopSecret,
            &node_id,
            "civilian-zone",
        );
        let incident = result.incident().unwrap();
        assert_eq!(incident.severity, DataLeakageSeverity::Critical);
        let has_notify = incident.recommended_actions.iter().any(|a| {
            matches!(a, DataLeakageAction::NotifyAuthority(_))
        });
        assert!(has_notify);
    }

    // ---- serialization tests ----

    #[test]
    fn test_data_leakage_incident_serialization() {
        let incident = DataLeakageIncident {
            severity: DataLeakageSeverity::Major,
            data_classification: ClassificationLevel::Secret,
            infrastructure_level: ClassificationLevel::Restricted,
            affected_nodes: vec![vec![1, 2, 3]],
            affected_zones: vec!["zone-a".into()],
            timestamp: "2026-02-11T12:00:00Z".into(),
            recommended_actions: vec![
                DataLeakageAction::QuarantineNodes(vec![vec![1, 2, 3]]),
                DataLeakageAction::NotifyAuthority("authority-x".into()),
            ],
        };
        let json = serde_json::to_string(&incident).unwrap();
        let deserialized: DataLeakageIncident = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.severity, DataLeakageSeverity::Major);
        assert_eq!(
            deserialized.data_classification,
            ClassificationLevel::Secret
        );
        assert_eq!(deserialized.recommended_actions.len(), 2);
    }

    #[test]
    fn test_data_leakage_action_serialization() {
        let actions: Vec<DataLeakageAction> = vec![
            DataLeakageAction::QuarantineNodes(vec![vec![1]]),
            DataLeakageAction::EmergencyZeroize(vec![vec![2]]),
            DataLeakageAction::RevokeZoneMembership(vec![vec![3]]),
            DataLeakageAction::NotifyAuthority("auth".into()),
            DataLeakageAction::ForensicDump(vec![vec![4]]),
            DataLeakageAction::EmergencyKeyRotation(vec!["key-1".into()]),
        ];
        for action in &actions {
            let json = serde_json::to_string(action).unwrap();
            let _deserialized: DataLeakageAction = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_transit_critical_has_key_rotation() {
        let store = Arc::new(ZoneCertificateStore::new());
        let detector = DataLeakageDetector::new(store);
        let result = detector.check_transit(
            ClassificationLevel::TopSecret,
            false,
            "civilian-zone",
            ClassificationLevel::Unclassified,
        );
        let incident = result.incident().unwrap();
        let has_key_rotation = incident.recommended_actions.iter().any(|a| {
            matches!(a, DataLeakageAction::EmergencyKeyRotation(_))
        });
        assert!(has_key_rotation);
    }
}
