// Marabunta - Licensed under the MIT License.
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};
use crate::highestsec::types::CountryCode;
use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::plugin::{
    CriticalityLevel, JobId, PluginErrorKind, PluginId, PluginKind, VerdictDecision, ZoneId,
};
use crate::highestsec::manifest::SemVer;

/// A single event in the tamper-evident highestsec audit chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HighestsecAuditEvent {
    pub sequence: u64,
    pub previous_hash: [u8; 32],
    pub event_hash: [u8; 32],
    pub timestamp: String,
    pub node_id: Vec<u8>,
    pub jurisdiction: CountryCode,
    pub zone_id: ZoneId,
    pub kind: HighestsecAuditEventKind,
    pub signature: Option<Vec<u8>>,
}

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
    /// WASM module emitted an audit event during blind execution via __marabunta_audit_emit.
    WasmAuditEmit {
        job_id: JobId,
        message: Vec<u8>,
        envelope_id: String,
    },
    /// A blind job envelope has been submitted.
    BlindJobSubmitted {
        envelope_hash: [u8; 32],
        submitter_id: Vec<u8>,
    },
    /// A blind job has been assigned to a node within a zone.
    BlindJobAssigned {
        envelope_id: String,
        node_id: Vec<u8>,
        zone_id: String,
    },
    /// Decryption of a blind envelope has started.
    DecryptionStarted {
        envelope_hash: [u8; 32],
        node_id: Vec<u8>,
    },
    /// Output padding has been applied to a blind result.
    OutputPaddingApplied {
        padded_size: u64,
    },
    /// A blind result has been re-encrypted for the recipient.
    ResultReEncrypted {
        output_hash: [u8; 32],
    },
    /// Memory scrub completed after blind execution.
    MemoryScrubCompleted {
        buffer_count: u32,
        scrub_method: String,
    },
    /// A blind result has been delivered to the recipient.
    ResultDelivered {
        envelope_id: String,
        recipient_id: Vec<u8>,
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

impl HighestsecAuditEvent {
    /// Compute the SHA-256 hash of all fields except `event_hash` and `signature`.
    pub fn compute_hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.sequence.to_le_bytes());
        hasher.update(self.previous_hash);
        hasher.update(self.timestamp.as_bytes());
        hasher.update(&self.node_id);
        hasher.update(self.jurisdiction.0);
        hasher.update(self.zone_id.as_bytes());
        // Include the event kind by serializing it to JSON.
        let kind_bytes =
            serde_json::to_vec(&self.kind).expect("audit event kind serialization must succeed");
        hasher.update(&kind_bytes);
        hasher.finalize().into()
    }

    /// Create a new audit event with the hash automatically computed.
    pub fn new(
        sequence: u64,
        previous_hash: [u8; 32],
        node_id: Vec<u8>,
        jurisdiction: CountryCode,
        zone_id: ZoneId,
        kind: HighestsecAuditEventKind,
    ) -> Self {
        let timestamp = chrono::Utc::now().to_rfc3339();
        let mut event = Self {
            sequence,
            previous_hash,
            event_hash: [0u8; 32],
            timestamp,
            node_id,
            jurisdiction,
            zone_id,
            kind,
            signature: None,
        };
        event.event_hash = event.compute_hash();
        event
    }

    /// Verify that the stored event_hash matches the computed hash.
    pub fn verify_hash(&self) -> bool {
        self.event_hash == self.compute_hash()
    }

    /// Sign the event with an Ed25519 signing key.
    /// Computes the event hash and signs it cryptographically.
    pub fn sign(&mut self, signing_key: &ed25519_dalek::SigningKey) {
        use ed25519_dalek::Signer;
        let message = self.compute_hash();
        let signature = signing_key.sign(&message);
        self.signature = Some(signature.to_bytes().to_vec());
    }

    /// Verify the event signature against an Ed25519 verifying key.
    pub fn verify_signature(&self, verifying_key: &ed25519_dalek::VerifyingKey) -> bool {
        use ed25519_dalek::Verifier;
        let Some(sig_bytes) = &self.signature else { return false };
        let Ok(sig_array): Result<[u8; 64], _> = sig_bytes.as_slice().try_into() else {
            return false;
        };
        let signature = ed25519_dalek::Signature::from_bytes(&sig_array);
        let message = self.compute_hash();
        verifying_key.verify(&message, &signature).is_ok()
    }
}

/// A tamper-evident chain of highestsec audit events.
pub struct HighestsecAuditChain {
    events: Vec<HighestsecAuditEvent>,
    last_hash: [u8; 32],
    next_sequence: u64,
    signing_key: ed25519_dalek::SigningKey,
}

impl HighestsecAuditChain {
    /// Create a new empty audit chain.
    pub fn new(signing_key: ed25519_dalek::SigningKey) -> Self {
        Self {
            events: Vec::new(),
            last_hash: [0u8; 32],
            next_sequence: 0,
            signing_key,
        }
    }

    /// Append a new event to the chain. Returns a reference to the appended event.
    pub fn append(
        &mut self,
        node_id: Vec<u8>,
        jurisdiction: CountryCode,
        zone_id: ZoneId,
        kind: HighestsecAuditEventKind,
    ) -> &HighestsecAuditEvent {
        let mut event = HighestsecAuditEvent::new(
            self.next_sequence,
            self.last_hash,
            node_id,
            jurisdiction,
            zone_id,
            kind,
        );
        event.sign(&self.signing_key);
        self.last_hash = event.event_hash;
        self.next_sequence += 1;
        self.events.push(event);
        self.events.last().unwrap()
    }

    /// Verify the integrity of the entire chain.
    /// Checks:
    /// 1. Each event's hash matches its computed hash.
    /// 2. Each event's previous_hash matches the preceding event's event_hash.
    /// 3. Sequence numbers are contiguous starting from 0.
    pub fn verify_chain(&self) -> Result<(), ChainVerificationError> {
        let mut expected_prev = [0u8; 32];

        for (i, event) in self.events.iter().enumerate() {
            // Verify sequence number.
            if event.sequence != i as u64 {
                return Err(ChainVerificationError::SequenceGap { index: i });
            }

            // Verify hash integrity.
            if !event.verify_hash() {
                return Err(ChainVerificationError::HashMismatch { index: i });
            }

            // Verify chain linkage.
            if event.previous_hash != expected_prev {
                return Err(ChainVerificationError::ChainBroken { index: i });
            }

            expected_prev = event.event_hash;
        }

        Ok(())
    }

    /// Return the number of events in the chain.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Return a slice of all events.
    pub fn events(&self) -> &[HighestsecAuditEvent] {
        &self.events
    }
}

/// Errors detected when verifying a chain.
#[derive(Debug)]
pub enum ChainVerificationError {
    /// The hash stored in the event does not match its computed value.
    HashMismatch { index: usize },
    /// The sequence number at this index is not contiguous.
    SequenceGap { index: usize },
    /// The previous_hash does not match the preceding event's event_hash.
    ChainBroken { index: usize },
}

impl std::fmt::Display for ChainVerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HashMismatch { index } => {
                write!(f, "hash mismatch at event index {}", index)
            }
            Self::SequenceGap { index } => {
                write!(f, "sequence gap at event index {}", index)
            }
            Self::ChainBroken { index } => {
                write!(f, "chain linkage broken at event index {}", index)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::classification::ClassificationLevel;
    use crate::highestsec::plugin::{PluginErrorKind, PluginKind, VerdictDecision};
    use crate::highestsec::types::CountryCode;

    fn sample_node_id() -> Vec<u8> {
        vec![1, 2, 3, 4, 5, 6, 7, 8]
    }

    fn sample_kind() -> HighestsecAuditEventKind {
        HighestsecAuditEventKind::PluginLoaded {
            plugin_id: "test-plugin".into(),
            version: SemVer { major: 1, minor: 0, patch: 0 },
            kind: PluginKind::Computation,
            binary_hash: [0xAA; 32],
        }
    }

    #[test]
    fn test_hash_deterministic() {
        let e1 = HighestsecAuditEvent {
            sequence: 0,
            previous_hash: [0u8; 32],
            event_hash: [0u8; 32],
            timestamp: "2025-01-01T00:00:00Z".into(),
            node_id: sample_node_id(),
            jurisdiction: CountryCode::FR,
            zone_id: "zone-fr-1".into(),
            kind: sample_kind(),
            signature: None,
        };

        let e2 = HighestsecAuditEvent {
            sequence: 0,
            previous_hash: [0u8; 32],
            event_hash: [0u8; 32],
            timestamp: "2025-01-01T00:00:00Z".into(),
            node_id: sample_node_id(),
            jurisdiction: CountryCode::FR,
            zone_id: "zone-fr-1".into(),
            kind: sample_kind(),
            signature: None,
        };

        assert_eq!(e1.compute_hash(), e2.compute_hash());
    }

    #[test]
    fn test_hash_changes_on_modification() {
        let e1 = HighestsecAuditEvent {
            sequence: 0,
            previous_hash: [0u8; 32],
            event_hash: [0u8; 32],
            timestamp: "2025-01-01T00:00:00Z".into(),
            node_id: sample_node_id(),
            jurisdiction: CountryCode::FR,
            zone_id: "zone-fr-1".into(),
            kind: sample_kind(),
            signature: None,
        };

        let e2 = HighestsecAuditEvent {
            sequence: 1, // changed
            previous_hash: [0u8; 32],
            event_hash: [0u8; 32],
            timestamp: "2025-01-01T00:00:00Z".into(),
            node_id: sample_node_id(),
            jurisdiction: CountryCode::FR,
            zone_id: "zone-fr-1".into(),
            kind: sample_kind(),
            signature: None,
        };

        assert_ne!(e1.compute_hash(), e2.compute_hash());

        // Changing jurisdiction also changes hash.
        let e3 = HighestsecAuditEvent {
            sequence: 0,
            previous_hash: [0u8; 32],
            event_hash: [0u8; 32],
            timestamp: "2025-01-01T00:00:00Z".into(),
            node_id: sample_node_id(),
            jurisdiction: CountryCode::DE, // changed
            zone_id: "zone-fr-1".into(),
            kind: sample_kind(),
            signature: None,
        };
        assert_ne!(e1.compute_hash(), e3.compute_hash());
    }

    #[test]
    fn test_verify_hash_valid() {
        let event = HighestsecAuditEvent::new(
            0,
            [0u8; 32],
            sample_node_id(),
            CountryCode::FR,
            "zone-fr-1".into(),
            sample_kind(),
        );
        assert!(event.verify_hash());
    }

    #[test]
    fn test_verify_hash_tampered() {
        let mut event = HighestsecAuditEvent::new(
            0,
            [0u8; 32],
            sample_node_id(),
            CountryCode::FR,
            "zone-fr-1".into(),
            sample_kind(),
        );
        // Tamper with the timestamp.
        event.timestamp = "2099-12-31T23:59:59Z".into();
        assert!(!event.verify_hash());
    }

    #[test]
    fn test_chain_append_and_verify() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        let signing_key = SigningKey::generate(&mut OsRng);
        let mut chain = HighestsecAuditChain::new(signing_key);
        assert!(chain.is_empty());

        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-fr-1".into(),
            sample_kind(),
        );
        chain.append(
            sample_node_id(),
            CountryCode::DE,
            "zone-de-1".into(),
            HighestsecAuditEventKind::ExecutionStarted {
                job_id: "job-1".into(),
                plugin_id: "test-plugin".into(),
                classification: ClassificationLevel::Restricted,
            },
        );
        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-fr-1".into(),
            HighestsecAuditEventKind::ExecutionCompleted {
                job_id: "job-1".into(),
                plugin_id: "test-plugin".into(),
                output_hash: [0xBB; 32],
                execution_ms: 150,
            },
        );

        assert_eq!(chain.len(), 3);
        assert!(chain.verify_chain().is_ok());

        // Verify sequence numbers.
        for (i, event) in chain.events().iter().enumerate() {
            assert_eq!(event.sequence, i as u64);
        }
    }

    #[test]
    fn test_chain_linkage() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        let signing_key = SigningKey::generate(&mut OsRng);
        let mut chain = HighestsecAuditChain::new(signing_key);

        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );
        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );

        // First event should have zero previous_hash.
        assert_eq!(chain.events()[0].previous_hash, [0u8; 32]);
        // Second event's previous_hash should be first event's hash.
        assert_eq!(chain.events()[1].previous_hash, chain.events()[0].event_hash);
    }

    #[test]
    fn test_chain_detect_tamper() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        let signing_key = SigningKey::generate(&mut OsRng);
        let mut chain = HighestsecAuditChain::new(signing_key);

        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );
        chain.append(
            sample_node_id(),
            CountryCode::DE,
            "zone-2".into(),
            sample_kind(),
        );
        chain.append(
            sample_node_id(),
            CountryCode::ES,
            "zone-3".into(),
            sample_kind(),
        );

        // Tamper with the middle event's timestamp.
        chain.events[1].timestamp = "tampered".into();

        let result = chain.verify_chain();
        assert!(result.is_err());
        match result {
            Err(ChainVerificationError::HashMismatch { index }) => {
                assert_eq!(index, 1);
            }
            other => panic!("expected HashMismatch at index 1, got {:?}", other),
        }
    }

    #[test]
    fn test_chain_detect_broken_link() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        let signing_key = SigningKey::generate(&mut OsRng);
        let mut chain = HighestsecAuditChain::new(signing_key);

        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );
        chain.append(
            sample_node_id(),
            CountryCode::DE,
            "zone-2".into(),
            sample_kind(),
        );

        // Break the chain by modifying previous_hash of the second event.
        // We need to also update the event_hash so the hash check passes
        // but the chain linkage fails.
        chain.events[1].previous_hash = [0xFF; 32];
        chain.events[1].event_hash = chain.events[1].compute_hash();

        let result = chain.verify_chain();
        assert!(result.is_err());
        match result {
            Err(ChainVerificationError::ChainBroken { index }) => {
                assert_eq!(index, 1);
            }
            other => panic!("expected ChainBroken at index 1, got {:?}", other),
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let event = HighestsecAuditEvent::new(
            0,
            [0u8; 32],
            sample_node_id(),
            CountryCode::FR,
            "zone-fr-1".into(),
            HighestsecAuditEventKind::DataLeakageDetected {
                job_id: "job-spill".into(),
                severity: DataLeakageSeverity::Critical,
                source_classification: ClassificationLevel::Secret,
                target_classification: ClassificationLevel::Unclassified,
                containment_action: "halted and quarantined".into(),
            },
        );

        let json = serde_json::to_string(&event).unwrap();
        let event2: HighestsecAuditEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event.sequence, event2.sequence);
        assert_eq!(event.event_hash, event2.event_hash);
        assert_eq!(event.previous_hash, event2.previous_hash);
        assert_eq!(event.jurisdiction, event2.jurisdiction);
        assert_eq!(event.zone_id, event2.zone_id);
        assert!(event2.verify_hash());
    }

    #[test]
    fn test_data_leakage_severity_ordering() {
        assert!(DataLeakageSeverity::Minor < DataLeakageSeverity::Major);
        assert!(DataLeakageSeverity::Major < DataLeakageSeverity::Critical);
    }

    #[test]
    fn test_all_event_kinds_serialize() {
        let kinds: Vec<HighestsecAuditEventKind> = vec![
            HighestsecAuditEventKind::PluginLoaded {
                plugin_id: "p".into(),
                version: SemVer { major: 1, minor: 0, patch: 0 },
                kind: PluginKind::Computation,
                binary_hash: [0u8; 32],
            },
            HighestsecAuditEventKind::PluginLoadFailed {
                plugin_id: "p".into(),
                reason: LoadFailureReason::Expired,
            },
            HighestsecAuditEventKind::ExecutionStarted {
                job_id: "j".into(),
                plugin_id: "p".into(),
                classification: ClassificationLevel::Secret,
            },
            HighestsecAuditEventKind::ExecutionCompleted {
                job_id: "j".into(),
                plugin_id: "p".into(),
                output_hash: [0u8; 32],
                execution_ms: 100,
            },
            HighestsecAuditEventKind::ExecutionFailed {
                job_id: "j".into(),
                plugin_id: "p".into(),
                error_kind: PluginErrorKind::ResourceExhausted,
            },
            HighestsecAuditEventKind::ValidationVerdict {
                job_id: "j".into(),
                validator_id: "v".into(),
                decision: VerdictDecision::Accept,
                confidence: 0.99,
            },
            HighestsecAuditEventKind::MembraneCrossing {
                job_id: "j".into(),
                source_zone: "z1".into(),
                destination_zone: "z2".into(),
                classification: ClassificationLevel::Confidential,
                transforms_applied: vec!["re-encrypt".into()],
            },
            HighestsecAuditEventKind::KeyRelease {
                key_id: "k1".into(),
                zone_id: "z".into(),
                proof_level: 2,
                classification: ClassificationLevel::Restricted,
            },
            HighestsecAuditEventKind::KeyReleaseDenied {
                key_id: "k1".into(),
                zone_id: "z".into(),
                reason: "insufficient proof".into(),
            },
            HighestsecAuditEventKind::AttestationGenerated {
                job_id: "j".into(),
                attestation_hash: [0xCC; 32],
            },
            HighestsecAuditEventKind::AttestationVerified {
                job_id: "j".into(),
                attestation_hash: [0xCC; 32],
                verified: true,
            },
            HighestsecAuditEventKind::DataLeakageDetected {
                job_id: "j".into(),
                severity: DataLeakageSeverity::Major,
                source_classification: ClassificationLevel::Secret,
                target_classification: ClassificationLevel::Restricted,
                containment_action: "quarantine".into(),
            },
            HighestsecAuditEventKind::PluginRevoked {
                plugin_id: "p".into(),
                reason: RevocationReason::Compromised,
                authority_id: "auth-1".into(),
            },
            HighestsecAuditEventKind::JurisdictionViolation {
                job_id: "j".into(),
                expected_jurisdiction: CountryCode::FR,
                actual_jurisdiction: CountryCode::US,
                action_taken: "blocked".into(),
            },
            HighestsecAuditEventKind::NonexportTaintPropagated {
                job_id: "j".into(),
                source_job: "j0".into(),
                taint_chain_length: 3,
            },
            HighestsecAuditEventKind::NodeJoinedZone {
                zone_id: "z".into(),
                proof_level: 2,
            },
            HighestsecAuditEventKind::NodeLeftZone {
                zone_id: "z".into(),
                reason: "maintenance".into(),
            },
            HighestsecAuditEventKind::ClassificationUpgrade {
                job_id: "j".into(),
                from: ClassificationLevel::Restricted,
                to: ClassificationLevel::Secret,
                authority_id: "auth".into(),
            },
            HighestsecAuditEventKind::ClassificationDowngrade {
                job_id: "j".into(),
                from: ClassificationLevel::Secret,
                to: ClassificationLevel::Restricted,
                authority_chain: vec!["auth-a".into(), "auth-b".into()],
            },
            HighestsecAuditEventKind::ResourceLimitHit {
                job_id: "j".into(),
                plugin_id: "p".into(),
                resource: "memory_pages".into(),
                limit: 256,
                actual: 300,
            },
            HighestsecAuditEventKind::CriticalityChanged {
                plugin_id: "p".into(),
                from: CriticalityLevel::Standard,
                to: CriticalityLevel::Critical,
            },
            HighestsecAuditEventKind::ChainCheckpoint {
                chain_length: 1000,
                cumulative_hash: [0xDD; 32],
            },
            HighestsecAuditEventKind::WasmAuditEmit {
                job_id: "j".into(),
                message: b"audit checkpoint reached".to_vec(),
                envelope_id: "env-1".into(),
            },
            HighestsecAuditEventKind::BlindJobSubmitted {
                envelope_hash: [0xEE; 32],
                submitter_id: vec![1, 2, 3],
            },
            HighestsecAuditEventKind::BlindJobAssigned {
                envelope_id: "env-1".into(),
                node_id: vec![4, 5, 6],
                zone_id: "zone-1".into(),
            },
            HighestsecAuditEventKind::DecryptionStarted {
                envelope_hash: [0xFF; 32],
                node_id: vec![7, 8, 9],
            },
            HighestsecAuditEventKind::OutputPaddingApplied {
                padded_size: 2048,
            },
            HighestsecAuditEventKind::ResultReEncrypted {
                output_hash: [0xAB; 32],
            },
            HighestsecAuditEventKind::MemoryScrubCompleted {
                buffer_count: 5,
                scrub_method: "zeroize".into(),
            },
            HighestsecAuditEventKind::ResultDelivered {
                envelope_id: "env-2".into(),
                recipient_id: vec![10, 11, 12],
            },
        ];

        for kind in &kinds {
            let json = serde_json::to_string(kind).unwrap();
            let _kind2: HighestsecAuditEventKind = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_sign_and_verify_ed25519() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        let mut event = HighestsecAuditEvent::new(
            0,
            [0u8; 32],
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );

        // Sign and verify
        event.sign(&signing_key);
        assert!(event.signature.is_some());
        assert!(event.verify_signature(&verifying_key));

        // Wrong key should fail
        let wrong_key = SigningKey::generate(&mut OsRng);
        assert!(!event.verify_signature(&wrong_key.verifying_key()));
    }

    #[test]
    fn test_unsigned_event_verify_fails() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let key = SigningKey::generate(&mut OsRng);
        let event = HighestsecAuditEvent::new(
            0,
            [0u8; 32],
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );
        assert!(!event.verify_signature(&key.verifying_key()));
    }

    #[test]
    fn test_load_failure_reasons_serialize() {
        let reasons = vec![
            LoadFailureReason::BinaryHashMismatch,
            LoadFailureReason::SignatureInvalid,
            LoadFailureReason::Expired,
            LoadFailureReason::Revoked,
            LoadFailureReason::InsufficientZoneClass,
            LoadFailureReason::JurisdictionDenied,
            LoadFailureReason::ValidationFailed("bad wasm".into()),
        ];
        for r in &reasons {
            let json = serde_json::to_string(r).unwrap();
            let _r2: LoadFailureReason = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_revocation_reasons_serialize() {
        let reasons = vec![
            RevocationReason::SecurityVulnerability,
            RevocationReason::PolicyViolation,
            RevocationReason::Compromised,
            RevocationReason::Superseded { replacement_id: "new-plugin".into() },
            RevocationReason::Expired,
            RevocationReason::Administrative("policy update".into()),
        ];
        for r in &reasons {
            let json = serde_json::to_string(r).unwrap();
            let _r2: RevocationReason = serde_json::from_str(&json).unwrap();
        }
    }

    #[test]
    fn test_auto_sign_on_append() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        let mut chain = HighestsecAuditChain::new(signing_key);
        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );

        let event = &chain.events()[0];
        assert!(event.signature.is_some(), "event should be auto-signed when chain has signing key");
        assert!(event.verify_signature(&verifying_key), "auto-signature should verify");
    }

    fn test_new_with_key_is_signed() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let mut chain = HighestsecAuditChain::new(signing_key);
        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );

        let event = &chain.events()[0];
        assert!(event.signature.is_some(), "event should be signed when chain has a signing key");
    }

    #[test]
    fn test_new_audit_event_kinds() {
        let kinds: Vec<HighestsecAuditEventKind> = vec![
            HighestsecAuditEventKind::BlindJobSubmitted {
                envelope_hash: [0xAA; 32],
                submitter_id: vec![1, 2, 3],
            },
            HighestsecAuditEventKind::BlindJobAssigned {
                envelope_id: "env-1".into(),
                node_id: vec![4, 5, 6],
                zone_id: "zone-fr-1".into(),
            },
            HighestsecAuditEventKind::DecryptionStarted {
                envelope_hash: [0xBB; 32],
                node_id: vec![7, 8, 9],
            },
            HighestsecAuditEventKind::OutputPaddingApplied {
                padded_size: 4096,
            },
            HighestsecAuditEventKind::ResultReEncrypted {
                output_hash: [0xCC; 32],
            },
            HighestsecAuditEventKind::MemoryScrubCompleted {
                buffer_count: 12,
                scrub_method: "zeroize".into(),
            },
            HighestsecAuditEventKind::ResultDelivered {
                envelope_id: "env-2".into(),
                recipient_id: vec![10, 11, 12],
            },
        ];

        for kind in &kinds {
            let json = serde_json::to_string(kind).expect("new audit kind should serialize");
            let deserialized: HighestsecAuditEventKind =
                serde_json::from_str(&json).expect("new audit kind should deserialize");
            // Re-serialize and compare to ensure round-trip fidelity.
            let json2 = serde_json::to_string(&deserialized).unwrap();
            assert_eq!(json, json2);
        }
    }

    #[test]
    fn test_empty_chain_verifies() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        let signing_key = SigningKey::generate(&mut OsRng);
        let chain = HighestsecAuditChain::new(signing_key);
        assert!(chain.verify_chain().is_ok());
        assert_eq!(chain.len(), 0);
        assert!(chain.is_empty());
    }

    #[test]
    fn test_single_event_chain() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        let signing_key = SigningKey::generate(&mut OsRng);
        let mut chain = HighestsecAuditChain::new(signing_key);
        chain.append(
            sample_node_id(),
            CountryCode::FR,
            "zone-1".into(),
            sample_kind(),
        );
        assert_eq!(chain.len(), 1);
        assert!(!chain.is_empty());
        assert!(chain.verify_chain().is_ok());
    }
}
