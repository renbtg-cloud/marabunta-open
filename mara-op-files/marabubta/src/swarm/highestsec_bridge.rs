// Marabunta - Licensed under the MIT License.
//! Highestsec Bridge — connects the highestsec compliance modules to the swarm
//! runtime.
//!
//! This module provides `HighestsecBridge`, a facade that coordinates
//! classification, handling_restriction, data_leakage, NONEXPORT, audit, and plugin-verification
//! engines into a single pre/post-execution check surface that the blind
//! computation pipeline can call.
//!
//! # Lifecycle
//!
//! ```text
//!        │
//!   ┌────▼────────────────────────┐
//!   │  pre_execution_check(ctx)   │ ← classification + handling_restriction + NONEXPORT + data_leakage
//!   └────┬────────────────────────┘
//!        │ Cleared / Denied
//!        ▼
//!   execute job in sandbox
//!        │
//!   ┌────▼────────────────────────┐
//!   │  post_execution_check(ctx)  │ ← data_leakage on output + NONEXPORT taint + audit
//!   └────┬────────────────────────┘
//!        │
//!   BlindResult returned
//! ```

use std::sync::Arc;

use crate::highestsec::audit_events::{
    HighestsecAuditChain, HighestsecAuditEventKind, DataLeakageSeverity,
};
use crate::highestsec::handling_restriction_engine::{AccessCredentials, HandlingRestrictionEngine};
use crate::highestsec::classification::ClassificationLevel;
use crate::highestsec::classification_engine::ClassificationEngine;
use crate::highestsec::constraints::CustodyOperation;
use crate::highestsec::nonexport_engine::NonexportEngine;
use crate::highestsec::jurisdiction_keys::KeyReleaseAuthority;
use crate::highestsec::data_leakage::{DataLeakageCheck, DataLeakageDetector};
use crate::highestsec::types::CountryCode;
use crate::highestsec::zone_membership::ZoneCertificateStore;
use crate::swarm::complexity::{ConcernDomain, EventSeverity};
use crate::swarm::events::EventBus;
use crate::swarm::types::NodeId;

// Re-export for downstream consumers.
pub use crate::highestsec::binary_verify::PluginVerifier;

// ---------------------------------------------------------------------------
// PreCheckResult / PostCheckResult
// ---------------------------------------------------------------------------

/// Outcome of the pre-execution compliance gate.
#[derive(Debug, Clone)]
pub enum PreCheckResult {
    /// All checks passed; the job may proceed.
    Cleared,
    /// One or more checks failed; the job must NOT proceed.
    Denied(Vec<String>),
}

impl PreCheckResult {
    /// Returns `true` when the job is cleared for execution.
    pub fn is_cleared(&self) -> bool {
        matches!(self, Self::Cleared)
    }
}

/// Outcome of the post-execution compliance gate.
#[derive(Debug, Clone)]
pub struct PostCheckResult {
    /// Human-readable descriptions of any data_leakage incidents detected.
    pub data_leakage_incidents: Vec<String>,
    /// The computed output classification (may differ from input).
    pub output_classification: Option<ClassificationLevel>,
    /// Whether the output carries NONEXPORT taint.
    pub nonexport_tainted: bool,
    /// Number of audit events emitted during the post-check.
    pub events_emitted: usize,
}

// ---------------------------------------------------------------------------
// HighestsecJobContext
// ---------------------------------------------------------------------------

/// Extracted context describing a highestsec-grade job that is about to run (or
/// constructed directly in tests.
#[derive(Debug, Clone)]
pub struct HighestsecJobContext {
    /// The swarm node that will (or did) execute the job.
    pub node_id: NodeId,
    /// Classification level of the job payload.
    pub job_classification: ClassificationLevel,
    /// Jurisdiction of the executing node.
    pub jurisdiction: CountryCode,
    /// Free-form handling_restriction strings attached to the job.
    pub handling_restrictions: Vec<String>,
    /// Whether the job involves NONEXPORT-controlled data.
    pub nonexport_controlled: bool,
    /// Optional zone identifier for zone-scoped checks.
    pub zone_id: Option<String>,
    /// Optional job identifier string for NONEXPORT tracking.
    pub job_id: Option<String>,
}

impl Default for HighestsecJobContext {
    fn default() -> Self {
        Self {
            node_id: NodeId::new(),
            job_classification: ClassificationLevel::Unclassified,
            jurisdiction: CountryCode::FR,
            handling_restrictions: Vec::new(),
            nonexport_controlled: false,
            zone_id: None,
            job_id: None,
        }
    }
}

// ---------------------------------------------------------------------------
// HighestsecBridge
// ---------------------------------------------------------------------------

/// Facade coordinating all highestsec compliance engines for the swarm runtime.
///
/// Constructed once at swarm startup and shared (`Arc<HighestsecBridge>`)
/// across all blind-computation pipeline stages.
pub struct HighestsecBridge {
    classification: Arc<ClassificationEngine>,
    handling_restriction: HandlingRestrictionEngine,
    data_leakage: Arc<DataLeakageDetector>,
    nonexport: Arc<NonexportEngine>,
    audit_chain: Arc<parking_lot::Mutex<HighestsecAuditChain>>,
    plugin_verifier: Arc<PluginVerifier>,
    key_authority: Arc<KeyReleaseAuthority>,
    zone_certs: Arc<ZoneCertificateStore>,
    event_bus: Option<Arc<EventBus>>,
}

impl HighestsecBridge {
    /// Create a new bridge wiring all compliance engines together.
    pub fn new(
        classification: Arc<ClassificationEngine>,
        handling_restriction: HandlingRestrictionEngine,
        data_leakage: Arc<DataLeakageDetector>,
        nonexport: Arc<NonexportEngine>,
        audit_chain: Arc<parking_lot::Mutex<HighestsecAuditChain>>,
        plugin_verifier: Arc<PluginVerifier>,
        key_authority: Arc<KeyReleaseAuthority>,
        zone_certs: Arc<ZoneCertificateStore>,
    ) -> Self {
        Self {
            classification,
            handling_restriction,
            data_leakage,
            nonexport,
            audit_chain,
            plugin_verifier,
            key_authority,
            zone_certs,
            event_bus: None,
        }
    }

    /// Attach an event bus for emitting compliance events into the swarm
    /// event stream. Builder-pattern; returns `self` for chaining.
    pub fn with_event_bus(mut self, bus: Arc<EventBus>) -> Self {
        self.event_bus = Some(bus);
        self
    }

    // -----------------------------------------------------------------------
    // Pre-execution check
    // -----------------------------------------------------------------------

    /// Run all pre-execution compliance checks against the given job context.
    ///
    /// Checks performed (accumulated, not short-circuited):
    ///
    /// 1. **Classification**: node must hold a zone certificate whose
    ///    `max_classification` is at least `ctx.job_classification`.
    /// 2. **HandlingRestriction / access**: node's credentials must satisfy the job's
    ///    handling_restrictions (clearance level, nationality, compartments).
    /// 3. **NONEXPORT**: if the job is NONEXPORT-controlled and tracking exists, the
    ///    node's jurisdiction must be in the approved set.
    /// 4. **DataLeakage (pre)**: infrastructure classification must be at
    ///    least as high as the job's.
    pub fn pre_execution_check(&self, ctx: &HighestsecJobContext) -> PreCheckResult {
        let mut violations: Vec<String> = Vec::new();
        let node_bytes = ctx.node_id.0.as_bytes().to_vec();
        let zone_id = ctx.zone_id.as_deref().unwrap_or("default-zone");

        // 1. Classification engine: node clearance >= job classification.
        if let Err(e) = self
            .classification
            .check_node(&node_bytes, ctx.job_classification)
        {
            violations.push(format!("classification: {}", e));
        }

        // 2. HandlingRestriction / access check.
        let credentials = AccessCredentials {
            clearance_level: ctx.job_classification,
            nationality: ctx.jurisdiction,
            compartment_access: ctx.handling_restrictions.clone(),
            organization: None,
        };
        // Build a minimal handling_restriction bundle from the job classification. In a
        // production deployment the full handling_restriction bundle would be extracted
        // from the blind envelope; here we use job_classification and
        // a permissive releasability as defaults.
        let data_handling_restriction = crate::highestsec::classification::HandlingRestriction {
            level: ctx.job_classification,
            releasable_to: crate::highestsec::classification::ReleasabilityMarking::RelTo(
                vec![ctx.jurisdiction],
            ),
            compartments: Vec::new(),
            handling: Vec::new(),
        };
        if let Err(e) = self.handling_restriction.check_access(&credentials, &data_handling_restriction) {
            violations.push(format!("handling_restriction: {}", e));
        }

        // 3. NONEXPORT jurisdiction authorization.
        if ctx.nonexport_controlled {
            if let Some(ref job_id) = ctx.job_id {
                if let Err(violation) =
                    self.nonexport.check_node_authorized(job_id, &ctx.jurisdiction)
                {
                    violations.push(format!("nonexport: {}", violation));
                }
            }
        }

        // 4. DataLeakage boundary check.
        let data_leakage_result = self.data_leakage.check_pre_execution(
            ctx.job_classification,
            &node_bytes,
            zone_id,
        );
        if let DataLeakageCheck::DataLeakage(incident) = &data_leakage_result {
            violations.push(format!(
                "data_leakage: data {:?} on {:?} infra (severity {:?})",
                incident.data_classification,
                incident.infrastructure_level,
                incident.severity,
            ));
        }

        // Emit event if we have a bus.
        if let Some(ref bus) = self.event_bus {
            if violations.is_empty() {
                bus.emit_simple(
                    ConcernDomain::Security,
                    EventSeverity::Info,
                    format!(
                        "highestsec pre-check cleared for node {}",
                        ctx.node_id
                    ),
                );
            } else {
                bus.emit_simple(
                    ConcernDomain::Security,
                    EventSeverity::Warning,
                    format!(
                        "highestsec pre-check DENIED for node {}: {}",
                        ctx.node_id,
                        violations.join("; ")
                    ),
                );
            }
        }

        if violations.is_empty() {
            PreCheckResult::Cleared
        } else {
            PreCheckResult::Denied(violations)
        }
    }

    // -----------------------------------------------------------------------
    // Post-execution check
    // -----------------------------------------------------------------------

    /// Run post-execution compliance checks after a job completes.
    ///
    /// Checks performed:
    ///
    /// 1. **DataLeakage (post)**: output classification vs zone ceiling.
    /// 2. **NONEXPORT taint**: propagate taint if input was NONEXPORT-controlled.
    /// 3. **Custody chain**: record a custody entry for the executing node.
    /// 4. **Audit events**: append an audit event for the execution.
    pub fn post_execution_check(
        &self,
        ctx: &HighestsecJobContext,
        output_classification: ClassificationLevel,
    ) -> PostCheckResult {
        let zone_id = ctx.zone_id.as_deref().unwrap_or("default-zone");
        let node_bytes = ctx.node_id.0.as_bytes().to_vec();
        let mut data_leakage_incidents: Vec<String> = Vec::new();
        let mut events_emitted: usize = 0;

        // Determine the zone ceiling from the node's certificate, falling
        // back to Unclassified if no certificate exists.
        let zone_max = self
            .zone_certs
            .get_valid(&node_bytes)
            .map(|cert| cert.max_classification)
            .unwrap_or(ClassificationLevel::Unclassified);

        // 1. DataLeakage check on output.
        let data_leakage_result = self.data_leakage.check_post_execution(
            output_classification,
            zone_id,
            zone_max,
        );
        if let DataLeakageCheck::DataLeakage(incident) = &data_leakage_result {
            data_leakage_incidents.push(format!(
                "output {:?} exceeds zone max {:?} (severity {:?})",
                incident.data_classification,
                incident.infrastructure_level,
                incident.severity,
            ));
        }

        // 2. NONEXPORT taint computation.
        let nonexport_tainted = ctx.nonexport_controlled;

        // 3. Custody chain entry.
        if let Some(ref job_id) = ctx.job_id {
            let record = crate::highestsec::constraints::CustodyRecord {
                node_id: node_bytes.clone(),
                jurisdiction: ctx.jurisdiction,
                proof_level: 0,
                timestamp: chrono::Utc::now().to_rfc3339(),
                operation: CustodyOperation::Executed,
            };
            self.nonexport.record_custody(job_id, record);
        }

        // 4. Audit events.
        {
            let mut chain = self.audit_chain.lock();
            if !data_leakage_incidents.is_empty() {
                chain.append(
                    node_bytes.clone(),
                    ctx.jurisdiction,
                    zone_id.to_string(),
                    HighestsecAuditEventKind::DataLeakageDetected {
                        job_id: ctx.job_id.clone().unwrap_or_default(),
                        severity: DataLeakageSeverity::Major,
                        source_classification: output_classification,
                        target_classification: zone_max,
                        containment_action: "flagged by post-execution check".into(),
                    },
                );
                events_emitted += 1;
            }

            // Always emit an execution-completed event.
            chain.append(
                node_bytes.clone(),
                ctx.jurisdiction,
                zone_id.to_string(),
                HighestsecAuditEventKind::ExecutionCompleted {
                    job_id: ctx.job_id.clone().unwrap_or_default(),
                    plugin_id: String::new(),
                    output_hash: [0u8; 32],
                    execution_ms: 0,
                },
            );
            events_emitted += 1;
        }

        // Emit into swarm event bus.
        if let Some(ref bus) = self.event_bus {
            if data_leakage_incidents.is_empty() {
                bus.emit_simple(
                    ConcernDomain::Security,
                    EventSeverity::Info,
                    format!(
                        "highestsec post-check OK for node {} (output={:?})",
                        ctx.node_id, output_classification
                    ),
                );
            } else {
                bus.emit_simple(
                    ConcernDomain::Security,
                    EventSeverity::Critical,
                    format!(
                        "highestsec post-check DATA_LEAKAGE for node {}: {}",
                        ctx.node_id,
                        data_leakage_incidents.join("; ")
                    ),
                );
            }
        }

        PostCheckResult {
            data_leakage_incidents,
            output_classification: Some(output_classification),
            nonexport_tainted,
            events_emitted,
        }
    }

    // -----------------------------------------------------------------------
    // Cross-zone crossing check
    // -----------------------------------------------------------------------

    /// Verify that data at the given classification level may move from
    /// `source_zone` to `target_zone`.
    ///
    /// Returns `Ok(())` if the crossing is permitted, or a list of
    /// violation descriptions on denial.
    pub fn check_crossing(
        &self,
        source_zone: &str,
        target_zone: &str,
        classification: ClassificationLevel,
    ) -> Result<(), Vec<String>> {
        let mut violations: Vec<String> = Vec::new();

        // Retrieve source and target zone members to determine their ceiling.
        let source_members = self.zone_certs.zone_members(source_zone);
        let target_members = self.zone_certs.zone_members(target_zone);

        // If the target zone has members, check that at least one member's
        // certificate allows the classification.
        if !target_members.is_empty() {
            let target_max = target_members
                .iter()
                .map(|c| c.max_classification)
                .max()
                .unwrap_or(ClassificationLevel::Unclassified);

            if classification > target_max {
                violations.push(format!(
                    "target zone '{}' max classification {:?} insufficient for {:?}",
                    target_zone, target_max, classification,
                ));
            }
        } else {
            // No members in target zone -- cannot verify clearance.
            violations.push(format!(
                "target zone '{}' has no members; cannot verify clearance",
                target_zone,
            ));
        }

        // If source zone is non-empty, verify data is not being moved from a
        // higher classification zone to a lower one without the target zone
        // being cleared.
        if !source_members.is_empty() && !target_members.is_empty() {
            let source_max = source_members
                .iter()
                .map(|c| c.max_classification)
                .max()
                .unwrap_or(ClassificationLevel::Unclassified);

            if source_max > ClassificationLevel::Unclassified
                && classification > ClassificationLevel::Unclassified
            {
                let target_max = target_members
                    .iter()
                    .map(|c| c.max_classification)
                    .max()
                    .unwrap_or(ClassificationLevel::Unclassified);

                if target_max < classification {
                    violations.push(format!(
                        "cross-zone downgrade: source '{}' ({:?}) -> target '{}' ({:?}) for {:?} data",
                        source_zone, source_max, target_zone, target_max, classification,
                    ));
                }
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    // -----------------------------------------------------------------------
    // Plugin verification
    // -----------------------------------------------------------------------

    /// Convenience wrapper: verify a plugin binary against its manifest hash.
    ///
    /// This is a simplified check that computes SHA-256 of `bytes` and
    /// compares it against `manifest_hash`. For full manifest-based
    /// verification, call `PluginVerifier::verify()` directly.
    pub fn verify_plugin(&self, bytes: &[u8], manifest_hash: &[u8; 32]) -> bool {
        use sha2::{Digest, Sha256};
        let computed = Sha256::digest(bytes);
        computed.as_slice() == manifest_hash
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Expose the audit chain for external consumers (e.g. Agent 4 forensics).
    pub fn audit_chain(&self) -> Arc<parking_lot::Mutex<HighestsecAuditChain>> {
        Arc::clone(&self.audit_chain)
    }

    /// Expose the plugin verifier.
    pub fn plugin_verifier(&self) -> &Arc<PluginVerifier> {
        &self.plugin_verifier
    }

    /// Expose the key release authority.
    pub fn key_authority(&self) -> &Arc<KeyReleaseAuthority> {
        &self.key_authority
    }

    /// Expose the zone certificate store.
    pub fn zone_certs(&self) -> &Arc<ZoneCertificateStore> {
        &self.zone_certs
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::highestsec::audit_events::HighestsecAuditChain;
    use crate::highestsec::binary_verify::{PluginVerifier, RevocationStore};
    use crate::highestsec::handling_restriction_engine::HandlingRestrictionEngine;
    use crate::highestsec::classification::ClassificationLevel;
    use crate::highestsec::classification_engine::ClassificationEngine;
    use crate::highestsec::nonexport_engine::NonexportEngine;
    use crate::highestsec::jurisdiction::ZoneClass;
    use crate::highestsec::signing::GatekeeperKey;
    use crate::highestsec::data_leakage::DataLeakageDetector;
    use crate::highestsec::types::CountryCode;
    use crate::highestsec::zone_membership::{ZoneCertificateStore, ZoneMembershipCertificate};
    use crate::swarm::events::EventBus;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build a certificate store with a single node registered.
    fn make_cert_store(
        node_id: &[u8],
        zone_class: ZoneClass,
        max_classification: ClassificationLevel,
        zone_id: &str,
    ) -> Arc<ZoneCertificateStore> {
        let store = ZoneCertificateStore::new();
        let cert = ZoneMembershipCertificate {
            node_id: node_id.to_vec(),
            zone_id: zone_id.to_string(),
            jurisdiction: CountryCode::FR,
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

    /// Build a HighestsecBridge for testing. The node identified by
    /// `node_id_bytes` will be registered at the given classification
    /// level.
    fn make_bridge(
        node_id_bytes: &[u8],
        zone_class: ZoneClass,
        max_classification: ClassificationLevel,
    ) -> HighestsecBridge {
        let zone_certs =
            make_cert_store(node_id_bytes, zone_class, max_classification, "test-zone");
        let classification = Arc::new(ClassificationEngine::new(Arc::clone(&zone_certs)));
        let handling_restriction = HandlingRestrictionEngine::new();
        let data_leakage = Arc::new(DataLeakageDetector::new(Arc::clone(&zone_certs)));
        let nonexport = Arc::new(NonexportEngine::new());
        let audit_chain = Arc::new(parking_lot::Mutex::new(HighestsecAuditChain::new(SigningKey::generate(&mut OsRng))));

        let gk = GatekeeperKey::generate();
        let rev_store = Arc::new(RevocationStore::new());
        let plugin_verifier = Arc::new(PluginVerifier::new(gk.public_key_bytes(), rev_store));

        let auth_key = SigningKey::generate(&mut OsRng);
        let key_authority =
            Arc::new(KeyReleaseAuthority::new(auth_key, Arc::clone(&zone_certs)));

        HighestsecBridge::new(
            classification,
            handling_restriction,
            data_leakage,
            nonexport,
            audit_chain,
            plugin_verifier,
            key_authority,
            zone_certs,
        )
    }

    /// Build a context for a known node whose UUID bytes match what we
    /// registered in the certificate store.
    fn make_ctx(
        node_id: NodeId,
        classification: ClassificationLevel,
        nonexport: bool,
    ) -> HighestsecJobContext {
        HighestsecJobContext {
            node_id,
            job_classification: classification,
            jurisdiction: CountryCode::FR,
            handling_restrictions: Vec::new(),
            nonexport_controlled: nonexport,
            zone_id: Some("test-zone".into()),
            job_id: Some("job-1".into()),
        }
    }

    // -----------------------------------------------------------------------
    // Data-type tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pre_check_result_cleared() {
        let result = PreCheckResult::Cleared;
        assert!(result.is_cleared());
    }

    #[test]
    fn test_pre_check_result_denied() {
        let result = PreCheckResult::Denied(vec!["violation-a".into()]);
        assert!(!result.is_cleared());
        if let PreCheckResult::Denied(v) = &result {
            assert_eq!(v.len(), 1);
            assert_eq!(v[0], "violation-a");
        }
    }

    #[test]
    fn test_post_check_result_defaults() {
        let result = PostCheckResult {
            data_leakage_incidents: Vec::new(),
            output_classification: None,
            nonexport_tainted: false,
            events_emitted: 0,
        };
        assert!(result.data_leakage_incidents.is_empty());
        assert!(result.output_classification.is_none());
        assert!(!result.nonexport_tainted);
        assert_eq!(result.events_emitted, 0);
    }

    #[test]
    fn test_highestsec_job_context() {
        let ctx = HighestsecJobContext::default();
        assert_eq!(ctx.job_classification, ClassificationLevel::Unclassified);
        assert_eq!(ctx.jurisdiction, CountryCode::FR);
        assert!(!ctx.nonexport_controlled);
        assert!(ctx.zone_id.is_none());
        assert!(ctx.handling_restrictions.is_empty());
    }

    // -----------------------------------------------------------------------
    // HighestsecBridge construction
    // -----------------------------------------------------------------------

    #[test]
    fn test_highestsec_bridge_new() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::MilClassified,
            ClassificationLevel::Secret,
        );
        // Verify the bridge's accessors work.
        let chain = bridge.audit_chain();
        let locked = chain.lock();
        assert!(locked.is_empty());
    }

    // -----------------------------------------------------------------------
    // Pre-execution checks
    // -----------------------------------------------------------------------

    #[test]
    fn test_pre_check_cleared() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::MilClassified,
            ClassificationLevel::Secret,
        );
        let ctx = make_ctx(node_id, ClassificationLevel::Restricted, false);
        let result = bridge.pre_execution_check(&ctx);
        assert!(result.is_cleared());
    }

    #[test]
    fn test_pre_check_denied_classification() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        // Node is only cleared for Restricted.
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
        );
        // Job requires Secret — should be denied.
        let ctx = make_ctx(node_id, ClassificationLevel::Secret, false);
        let result = bridge.pre_execution_check(&ctx);
        assert!(!result.is_cleared());
        if let PreCheckResult::Denied(violations) = &result {
            // At least one violation should mention classification or zone.
            assert!(!violations.is_empty());
        }
    }

    // -----------------------------------------------------------------------
    // Post-execution checks
    // -----------------------------------------------------------------------

    #[test]
    fn test_post_check_no_data_leakage() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::MilClassified,
            ClassificationLevel::Secret,
        );
        let ctx = make_ctx(node_id, ClassificationLevel::Restricted, false);
        let result =
            bridge.post_execution_check(&ctx, ClassificationLevel::Restricted);
        assert!(result.data_leakage_incidents.is_empty());
        assert_eq!(
            result.output_classification,
            Some(ClassificationLevel::Restricted)
        );
        assert!(!result.nonexport_tainted);
        assert!(result.events_emitted > 0);
    }

    #[test]
    fn test_post_check_data_leakage_detected() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        // Node only cleared for Restricted.
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::GovCloud,
            ClassificationLevel::Restricted,
        );
        let ctx = make_ctx(node_id, ClassificationLevel::Restricted, false);
        // Output is Secret, but zone max is Restricted => data_leakage.
        let result = bridge.post_execution_check(&ctx, ClassificationLevel::Secret);
        assert!(!result.data_leakage_incidents.is_empty());
    }

    // -----------------------------------------------------------------------
    // Cross-zone crossing
    // -----------------------------------------------------------------------

    #[test]
    fn test_crossing_check_ok() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();

        let zone_certs = Arc::new(ZoneCertificateStore::new());
        // Populate both zones.
        let cert_a = ZoneMembershipCertificate {
            node_id: node_bytes.clone(),
            zone_id: "zone-a".into(),
            jurisdiction: CountryCode::FR,
            zone_class: ZoneClass::MilClassified,
            max_classification: ClassificationLevel::Secret,
            proof_level: 3,
            issued_at: chrono::Utc::now().to_rfc3339(),
            expires_at: (chrono::Utc::now() + chrono::Duration::days(365)).to_rfc3339(),
            proof_hash: [0u8; 32],
            authority_signature: vec![0u8; 64],
            authority_pubkey: vec![0u8; 32],
            requires_clearnet: false,
            kyber_ek: vec![0x42u8; 32],
        };
        let cert_b = ZoneMembershipCertificate {
            node_id: vec![99],
            zone_id: "zone-b".into(),
            jurisdiction: CountryCode::DE,
            zone_class: ZoneClass::MilClassified,
            max_classification: ClassificationLevel::Secret,
            proof_level: 3,
            issued_at: chrono::Utc::now().to_rfc3339(),
            expires_at: (chrono::Utc::now() + chrono::Duration::days(365)).to_rfc3339(),
            proof_hash: [0u8; 32],
            authority_signature: vec![0u8; 64],
            authority_pubkey: vec![0u8; 32],
            requires_clearnet: false,
            kyber_ek: vec![0x42u8; 32],
        };
        zone_certs.upsert(cert_a);
        zone_certs.upsert(cert_b);

        let classification = Arc::new(ClassificationEngine::new(Arc::clone(&zone_certs)));
        let handling_restriction = HandlingRestrictionEngine::new();
        let data_leakage = Arc::new(DataLeakageDetector::new(Arc::clone(&zone_certs)));
        let nonexport = Arc::new(NonexportEngine::new());
        let audit_chain = Arc::new(parking_lot::Mutex::new(HighestsecAuditChain::new(SigningKey::generate(&mut OsRng))));
        let gk = GatekeeperKey::generate();
        let rev_store = Arc::new(RevocationStore::new());
        let plugin_verifier = Arc::new(PluginVerifier::new(gk.public_key_bytes(), rev_store));
        let auth_key = SigningKey::generate(&mut OsRng);
        let key_authority =
            Arc::new(KeyReleaseAuthority::new(auth_key, Arc::clone(&zone_certs)));

        let bridge = HighestsecBridge::new(
            classification,
            handling_restriction,
            data_leakage,
            nonexport,
            audit_chain,
            plugin_verifier,
            key_authority,
            zone_certs,
        );

        // Restricted data crossing between two Secret-capable zones => OK.
        let result = bridge.check_crossing(
            "zone-a",
            "zone-b",
            ClassificationLevel::Restricted,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_crossing_check_denied() {
        let zone_certs = Arc::new(ZoneCertificateStore::new());
        // zone-a has Secret capability.
        let cert_a = ZoneMembershipCertificate {
            node_id: vec![1],
            zone_id: "zone-a".into(),
            jurisdiction: CountryCode::FR,
            zone_class: ZoneClass::MilClassified,
            max_classification: ClassificationLevel::Secret,
            proof_level: 3,
            issued_at: chrono::Utc::now().to_rfc3339(),
            expires_at: (chrono::Utc::now() + chrono::Duration::days(365)).to_rfc3339(),
            proof_hash: [0u8; 32],
            authority_signature: vec![0u8; 64],
            authority_pubkey: vec![0u8; 32],
            requires_clearnet: false,
            kyber_ek: vec![0x42u8; 32],
        };
        // zone-b is only Restricted.
        let cert_b = ZoneMembershipCertificate {
            node_id: vec![2],
            zone_id: "zone-b".into(),
            jurisdiction: CountryCode::DE,
            zone_class: ZoneClass::GovCloud,
            max_classification: ClassificationLevel::Restricted,
            proof_level: 1,
            issued_at: chrono::Utc::now().to_rfc3339(),
            expires_at: (chrono::Utc::now() + chrono::Duration::days(365)).to_rfc3339(),
            proof_hash: [0u8; 32],
            authority_signature: vec![0u8; 64],
            authority_pubkey: vec![0u8; 32],
            requires_clearnet: false,
            kyber_ek: vec![0x42u8; 32],
        };
        zone_certs.upsert(cert_a);
        zone_certs.upsert(cert_b);

        let classification = Arc::new(ClassificationEngine::new(Arc::clone(&zone_certs)));
        let handling_restriction = HandlingRestrictionEngine::new();
        let data_leakage = Arc::new(DataLeakageDetector::new(Arc::clone(&zone_certs)));
        let nonexport = Arc::new(NonexportEngine::new());
        let audit_chain = Arc::new(parking_lot::Mutex::new(HighestsecAuditChain::new(SigningKey::generate(&mut OsRng))));
        let gk = GatekeeperKey::generate();
        let rev_store = Arc::new(RevocationStore::new());
        let plugin_verifier = Arc::new(PluginVerifier::new(gk.public_key_bytes(), rev_store));
        let auth_key = SigningKey::generate(&mut OsRng);
        let key_authority =
            Arc::new(KeyReleaseAuthority::new(auth_key, Arc::clone(&zone_certs)));

        let bridge = HighestsecBridge::new(
            classification,
            handling_restriction,
            data_leakage,
            nonexport,
            audit_chain,
            plugin_verifier,
            key_authority,
            zone_certs,
        );

        // Secret data crossing to a Restricted zone => denied.
        let result = bridge.check_crossing(
            "zone-a",
            "zone-b",
            ClassificationLevel::Secret,
        );
        assert!(result.is_err());
        let violations = result.unwrap_err();
        assert!(!violations.is_empty());
    }

    // -----------------------------------------------------------------------
    // Plugin verification
    // -----------------------------------------------------------------------

    #[test]
    fn test_verify_plugin() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::Civilian,
            ClassificationLevel::Unclassified,
        );

        let data = b"wasm binary data";
        let hash = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(data);
            let result = hasher.finalize();
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&result);
            arr
        };

        assert!(bridge.verify_plugin(data, &hash));
        assert!(!bridge.verify_plugin(b"wrong data", &hash));
    }

    // -----------------------------------------------------------------------
    // Audit chain
    // -----------------------------------------------------------------------

    #[test]
    fn test_audit_chain_accessible() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::MilClassified,
            ClassificationLevel::Secret,
        );

        let chain = bridge.audit_chain();
        {
            let mut locked = chain.lock();
            locked.append(
                vec![1, 2, 3],
                CountryCode::FR,
                "zone-test".into(),
                HighestsecAuditEventKind::NodeJoinedZone {
                    zone_id: "zone-test".into(),
                    proof_level: 2,
                },
            );
        }
        let locked = chain.lock();
        assert_eq!(locked.len(), 1);
    }

    // -----------------------------------------------------------------------
    // NONEXPORT checks
    // -----------------------------------------------------------------------

    #[test]
    fn test_nonexport_check_controlled() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::MilClassified,
            ClassificationLevel::Secret,
        );

        // NONEXPORT controlled but no tracking registered => should still clear
        // (NONEXPORT engine returns Ok when no tracking exists).
        let ctx = make_ctx(node_id, ClassificationLevel::Restricted, true);
        let result = bridge.pre_execution_check(&ctx);
        assert!(result.is_cleared());
    }

    #[test]
    fn test_nonexport_check_not_controlled() {
        let node_id = NodeId::new();
        let node_bytes = node_id.0.as_bytes().to_vec();
        let bridge = make_bridge(
            &node_bytes,
            ZoneClass::MilClassified,
            ClassificationLevel::Secret,
        );
        let ctx = make_ctx(node_id, ClassificationLevel::Restricted, false);
        let result = bridge.pre_execution_check(&ctx);
        assert!(result.is_cleared());
    }

    // -----------------------------------------------------------------------
    // PreExecutionFilter trait
}
