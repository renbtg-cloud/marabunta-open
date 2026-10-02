// Marabunta - Licensed under the MIT License.
//! Consent layer for bilateral trust between nodes and the swarm.
//!
//! Before a node can join a swarm, it receives a [`ConfigManifest`] describing
//! the swarm's policies, compliance profiles, and operational parameters. The
//! node must accept or reject the manifest, and the signed [`ConfigConsent`] is
//! recorded for audit. Only after consent is the admission jury selection
//! triggered.
//!
//! # Flow
//!
//! ```text
//! Candidate → JoinRequest → Swarm
//! Swarm → ConfigManifest → Candidate
//! Candidate → ConfigConsent(accepted=true/false) → Swarm
//! if accepted: proceed to jury selection
//! if rejected: record rejection, close
//! ```
//!
//! # Headless nodes
//!
//! Automated deployments use `--accept-manifest` to skip the interactive
//! prompt. The consent record still notes `auto_accepted: true`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::types::NodeId;

// ============================================================================
// ConfigSummary
// ============================================================================

/// A human-readable summary of the swarm's configuration for consent review.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSummary {
    /// Whether sandboxing is enforced for compute tasks.
    pub sandbox_enabled: bool,
    /// Whether API authentication is required.
    pub auth_required: bool,
    /// Whether the Neuromancer intelligence layer is active.
    pub neuromancer_enabled: bool,
    /// Data residency regions (empty = unrestricted).
    pub data_residency_regions: Vec<String>,
    /// Maximum chunk timeout in seconds.
    pub max_chunk_timeout_secs: u64,
    /// Number of verification replicas required.
    pub verification_replicas: u32,
    /// Whether PostgreSQL time-travel is enabled.
    pub postgres_enabled: bool,
    /// Active compliance profile name (if any).
    pub compliance_profile: Option<String>,
}

// ============================================================================
// ComplianceProfileInfo
// ============================================================================

/// Summary of an active compliance profile for consent display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceProfileInfo {
    /// Profile name (e.g., "soc2", "hipaa").
    pub name: String,
    /// Profile version.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Number of controls enforced.
    pub controls_count: usize,
}

// ============================================================================
// ConfigManifest
// ============================================================================

/// The swarm's configuration manifest sent to candidates for review.
///
/// Contains everything a node needs to decide whether to join.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigManifest {
    /// High-level config summary for human review.
    pub config_summary: ConfigSummary,
    /// Active compliance profiles.
    pub compliance_profiles: Vec<ComplianceProfileInfo>,
    /// Additional policy details (geofence, rate limits, etc.).
    pub policies: serde_json::Value,
    /// SHA-256 hash of the serialized manifest (excluding this field).
    pub manifest_hash: String,
    /// The swarm node that sent this manifest.
    pub from: NodeId,
    /// When this manifest was generated.
    pub generated_at: DateTime<Utc>,
    /// Swarm version.
    pub swarm_version: String,
}

impl ConfigManifest {
    /// Compute the SHA-256 hash of the manifest content.
    ///
    /// Hashes the config_summary + compliance_profiles + policies
    /// (everything except manifest_hash, from, generated_at, swarm_version).
    pub fn compute_hash(
        config_summary: &ConfigSummary,
        compliance_profiles: &[ComplianceProfileInfo],
        policies: &serde_json::Value,
    ) -> String {
        use blake3::Hasher;

        let mut hasher = Hasher::new();
        let summary_bytes = serde_json::to_vec(config_summary).unwrap_or_default();
        hasher.update(&summary_bytes);
        let profiles_bytes = serde_json::to_vec(compliance_profiles).unwrap_or_default();
        hasher.update(&profiles_bytes);
        let policies_bytes = serde_json::to_vec(policies).unwrap_or_default();
        hasher.update(&policies_bytes);

        hasher.finalize().to_hex().to_string()
    }

    /// Build a manifest from a SwarmConfig and compliance info.
    pub fn build(
        config: &super::config::SwarmConfig,
        from: NodeId,
    ) -> Self {
        let config_summary = ConfigSummary {
            sandbox_enabled: crate::swarm::hardware::get_bounds().sandbox_default_memory_mb > 0,
            auth_required: config.api_auth_required,
            neuromancer_enabled: config.enable_neuromancer,
            data_residency_regions: Vec::new(), // Populated from policy engine if available
            max_chunk_timeout_secs: config.chunk_timeout.as_secs(),
            verification_replicas: super::config::VERIFICATION_DEFAULT_REPLICAS as u32,
            postgres_enabled: config.enable_postgres,
            compliance_profile: super::config::COMPLIANCE_PROFILE_NAME.map(|s: &str| s.to_string()),
        };

        let compliance_profiles = if let Some(name) = super::config::COMPLIANCE_PROFILE_NAME {
            vec![ComplianceProfileInfo {
                name: name.to_string(),
                version: super::config::COMPLIANCE_PROFILE_VERSION
                    .unwrap_or("unknown")
                    .to_string(),
                description: super::config::COMPLIANCE_PROFILE_DESCRIPTION
                    .unwrap_or("")
                    .to_string(),
                controls_count: super::config::compliance_overrides().len(),
            }]
        } else {
            Vec::new()
        };

        let policies = serde_json::json!({});

        let manifest_hash =
            Self::compute_hash(&config_summary, &compliance_profiles, &policies);

        Self {
            config_summary,
            compliance_profiles,
            policies,
            manifest_hash,
            from,
            generated_at: Utc::now(),
            swarm_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Verify the manifest hash matches its content.
    pub fn verify_hash(&self) -> bool {
        let expected = Self::compute_hash(
            &self.config_summary,
            &self.compliance_profiles,
            &self.policies,
        );
        self.manifest_hash == expected
    }
}

// ============================================================================
// ConfigConsent
// ============================================================================

/// A node's consent (or rejection) of a swarm's configuration manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigConsent {
    /// The node expressing consent.
    pub node_id: NodeId,
    /// Hash of the manifest being consented to.
    pub manifest_hash: String,
    /// Whether the node accepts the manifest.
    pub accepted: bool,
    /// Reason for rejection (if !accepted).
    pub rejection_reason: Option<String>,
    /// Whether this was auto-accepted via --accept-manifest.
    pub auto_accepted: bool,
    /// When consent was given.
    pub timestamp: DateTime<Utc>,
}

impl ConfigConsent {
    /// Create an acceptance consent.
    pub fn accept(node_id: NodeId, manifest_hash: String, auto: bool) -> Self {
        Self {
            node_id,
            manifest_hash,
            accepted: true,
            rejection_reason: None,
            auto_accepted: auto,
            timestamp: Utc::now(),
        }
    }

    /// Create a rejection consent.
    pub fn reject(node_id: NodeId, manifest_hash: String, reason: String) -> Self {
        Self {
            node_id,
            manifest_hash,
            accepted: false,
            rejection_reason: Some(reason),
            auto_accepted: false,
            timestamp: Utc::now(),
        }
    }
}

// ============================================================================
// ConsentRecord (for PG storage)
// ============================================================================

/// A consent record as stored in the consent_records PG table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsentRecord {
    /// Database row ID.
    pub id: i64,
    /// The node that gave consent.
    pub node_id: String,
    /// Hash of the manifest.
    pub manifest_hash: String,
    /// Whether consent was granted.
    pub accepted: bool,
    /// Reason for rejection.
    pub rejection_reason: Option<String>,
    /// Auto-accepted flag.
    pub auto_accepted: bool,
    /// When consent was recorded.
    pub recorded_at: DateTime<Utc>,
}

// ============================================================================
// Console display helpers
// ============================================================================

impl ConfigManifest {
    /// Format the manifest for terminal display (consent prompt).
    pub fn display_for_consent(&self) -> String {
        let mut out = String::new();
        out.push_str("=== Swarm Configuration Manifest ===\n\n");
        out.push_str(&format!("Swarm version: {}\n", self.swarm_version));
        out.push_str(&format!("Generated at: {}\n", self.generated_at.to_rfc3339()));
        out.push_str(&format!("Manifest hash: {:.16}...\n\n", self.manifest_hash));

        out.push_str("--- Configuration Summary ---\n");
        out.push_str(&format!(
            "  Sandbox enforced: {}\n",
            if self.config_summary.sandbox_enabled {
                "yes"
            } else {
                "no"
            }
        ));
        out.push_str(&format!(
            "  Auth required: {}\n",
            if self.config_summary.auth_required {
                "yes"
            } else {
                "no"
            }
        ));
        out.push_str(&format!(
            "  Neuromancer (AI layer): {}\n",
            if self.config_summary.neuromancer_enabled {
                "active"
            } else {
                "inactive"
            }
        ));
        out.push_str(&format!(
            "  PostgreSQL time-travel: {}\n",
            if self.config_summary.postgres_enabled {
                "enabled"
            } else {
                "disabled"
            }
        ));
        out.push_str(&format!(
            "  Max chunk timeout: {}s\n",
            self.config_summary.max_chunk_timeout_secs
        ));
        out.push_str(&format!(
            "  Verification replicas: {}\n",
            self.config_summary.verification_replicas
        ));

        if self.config_summary.data_residency_regions.is_empty() {
            out.push_str("  Data residency: unrestricted\n");
        } else {
            out.push_str(&format!(
                "  Data residency: {}\n",
                self.config_summary.data_residency_regions.join(", ")
            ));
        }

        if !self.compliance_profiles.is_empty() {
            out.push_str("\n--- Compliance Profiles ---\n");
            for p in &self.compliance_profiles {
                out.push_str(&format!(
                    "  {} v{} — {} ({} controls)\n",
                    p.name, p.version, p.description, p.controls_count
                ));
            }
        }

        out.push_str("\nBy joining this swarm, you agree to operate under these policies.\n");
        out
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_node_id(suffix: &str) -> NodeId {
        NodeId(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_DNS, suffix.as_bytes()))
    }

    #[test]
    fn test_config_summary_serde() {
        let summary = ConfigSummary {
            sandbox_enabled: true,
            auth_required: true,
            neuromancer_enabled: false,
            data_residency_regions: vec!["us-east-1".into()],
            max_chunk_timeout_secs: 300,
            verification_replicas: 2,
            postgres_enabled: true,
            compliance_profile: Some("soc2".into()),
        };
        let json = serde_json::to_string(&summary).unwrap();
        let back: ConfigSummary = serde_json::from_str(&json).unwrap();
        assert!(back.sandbox_enabled);
        assert_eq!(back.compliance_profile, Some("soc2".into()));
    }

    #[test]
    fn test_compliance_profile_info_serde() {
        let info = ComplianceProfileInfo {
            name: "hipaa".into(),
            version: "1.0".into(),
            description: "HIPAA compliance".into(),
            controls_count: 8,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: ComplianceProfileInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "hipaa");
        assert_eq!(back.controls_count, 8);
    }

    #[test]
    fn test_manifest_hash_computation() {
        let summary = ConfigSummary {
            sandbox_enabled: true,
            auth_required: true,
            neuromancer_enabled: true,
            data_residency_regions: vec![],
            max_chunk_timeout_secs: 300,
            verification_replicas: 2,
            postgres_enabled: false,
            compliance_profile: None,
        };
        let profiles = vec![];
        let policies = serde_json::json!({});

        let hash1 = ConfigManifest::compute_hash(&summary, &profiles, &policies);
        let hash2 = ConfigManifest::compute_hash(&summary, &profiles, &policies);

        // Deterministic
        assert_eq!(hash1, hash2);
        // 64 hex chars (blake3)
        assert_eq!(hash1.len(), 64);
    }

    #[test]
    fn test_manifest_hash_changes_with_content() {
        let summary1 = ConfigSummary {
            sandbox_enabled: true,
            auth_required: true,
            neuromancer_enabled: true,
            data_residency_regions: vec![],
            max_chunk_timeout_secs: 300,
            verification_replicas: 2,
            postgres_enabled: false,
            compliance_profile: None,
        };
        let summary2 = ConfigSummary {
            sandbox_enabled: false, // changed
            ..summary1.clone()
        };

        let policies = serde_json::json!({});
        let hash1 = ConfigManifest::compute_hash(&summary1, &[], &policies);
        let hash2 = ConfigManifest::compute_hash(&summary2, &[], &policies);

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_manifest_build_and_verify() {
        let config = super::super::config::SwarmConfig::default();
        let node_id = test_node_id("swarm-leader");

        let manifest = ConfigManifest::build(&config, node_id);
        assert!(manifest.verify_hash());
    }

    #[test]
    fn test_manifest_tampered_hash_fails_verify() {
        let config = super::super::config::SwarmConfig::default();
        let node_id = test_node_id("swarm-leader");

        let mut manifest = ConfigManifest::build(&config, node_id);
        manifest.manifest_hash = "0000000000000000000000000000000000000000000000000000000000000000".into();
        assert!(!manifest.verify_hash());
    }

    #[test]
    fn test_manifest_serde_roundtrip() {
        let config = super::super::config::SwarmConfig::default();
        let node_id = test_node_id("swarm-leader");

        let manifest = ConfigManifest::build(&config, node_id);
        let json = serde_json::to_string(&manifest).unwrap();
        let back: ConfigManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(back.manifest_hash, manifest.manifest_hash);
        assert!(back.verify_hash());
    }

    #[test]
    fn test_consent_accept() {
        let node_id = test_node_id("candidate");
        let consent = ConfigConsent::accept(node_id.clone(), "abc123".into(), false);

        assert!(consent.accepted);
        assert!(!consent.auto_accepted);
        assert!(consent.rejection_reason.is_none());
        assert_eq!(consent.manifest_hash, "abc123");
    }

    #[test]
    fn test_consent_reject() {
        let node_id = test_node_id("candidate");
        let consent = ConfigConsent::reject(
            node_id.clone(),
            "abc123".into(),
            "Neuromancer not acceptable".into(),
        );

        assert!(!consent.accepted);
        assert!(!consent.auto_accepted);
        assert_eq!(
            consent.rejection_reason,
            Some("Neuromancer not acceptable".into())
        );
    }

    #[test]
    fn test_consent_auto_accept() {
        let node_id = test_node_id("headless-node");
        let consent = ConfigConsent::accept(node_id, "def456".into(), true);

        assert!(consent.accepted);
        assert!(consent.auto_accepted);
    }

    #[test]
    fn test_consent_serde_roundtrip() {
        let consent = ConfigConsent::accept(test_node_id("node"), "hash".into(), false);
        let json = serde_json::to_string(&consent).unwrap();
        let back: ConfigConsent = serde_json::from_str(&json).unwrap();
        assert!(back.accepted);
        assert_eq!(back.manifest_hash, "hash");
    }

    #[test]
    fn test_consent_record_serde() {
        let record = ConsentRecord {
            id: 1,
            node_id: test_node_id("node").to_string(),
            manifest_hash: "abc".into(),
            accepted: true,
            rejection_reason: None,
            auto_accepted: false,
            recorded_at: Utc::now(),
        };
        let json = serde_json::to_string(&record).unwrap();
        let back: ConsentRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, 1);
        assert!(back.accepted);
    }

    #[test]
    fn test_manifest_display() {
        let config = super::super::config::SwarmConfig::default();
        let manifest = ConfigManifest::build(&config, test_node_id("leader"));

        let display = manifest.display_for_consent();
        assert!(display.contains("Swarm Configuration Manifest"));
        assert!(display.contains("Sandbox enforced:"));
        assert!(display.contains("Auth required:"));
        assert!(display.contains("By joining this swarm"));
    }
}
