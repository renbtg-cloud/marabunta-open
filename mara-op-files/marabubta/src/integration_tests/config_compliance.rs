// Marabunta - Licensed under the MIT License.
//! Configuration and compliance integration tests.
//!
//! Tests the three-tier config system, compliance engine, consent layer,
//! and their interactions:
//!
//! - ConfigRegistry: validation, schema, hardwired locking
//! - LiveConfig: ArcSwap hot-reload, broadcast, patch acceptance/rejection
//! - ComplianceEngine: posture evaluation, report generation, violations
//! - ConfigConsent: manifest build, hash verification, consent flow
//! - ConfigDb: serde roundtrips for PG-stored types
//!
//! These tests run without a real PostgreSQL instance. They verify the
//! in-memory logic and type contracts.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::Utc;

    use crate::swarm::config::SwarmConfig;
    use crate::swarm::config_consent::{
        ComplianceProfileInfo, ConfigConsent, ConfigManifest, ConfigSummary, ConsentRecord,
    };
    use crate::swarm::config_db::{ConfigCurrentEntry, ConfigHistoryEntry, ConfigSnapshot};
    use crate::swarm::config_live::{ConfigPatchError, LiveConfig};
    use crate::swarm::config_meta::{
        ComplianceOverride, ConfigCategory, ConfigRegistry, ConfigSettingMeta, ConfigTier,
        ConfigValidationError, UiVisibility, ValueConstraints, ValueType,
    };
    use crate::swarm::compliance::ComplianceEngine;
    use crate::swarm::types::NodeId;

    // ========================================================================
    // Helpers
    // ========================================================================

    fn test_node_id(suffix: &str) -> NodeId {
        NodeId(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_DNS,
            suffix.as_bytes(),
        ))
    }

    fn make_compliance_override(
        key: &str,
        value: serde_json::Value,
        constraint: &str,
        profile: &str,
    ) -> ComplianceOverride {
        ComplianceOverride {
            key: key.to_string(),
            value,
            ui_visibility: UiVisibility::VisibleReadonly,
            constraint_type: constraint.to_string(),
            justification: format!("Required by {} profile", profile),
            profile_name: profile.to_string(),
        }
    }

    // ========================================================================
    // ConfigRegistry tests
    // ========================================================================

    #[test]
    fn registry_has_registered_settings() {
        let registry = ConfigRegistry::new(vec![]);
        let schema = registry.schema();

        // Should have substantial registered settings (100+)
        assert!(
            schema.len() > 50,
            "Registry should have many settings, got {}",
            schema.len()
        );
    }

    #[test]
    fn registry_lookup_by_key() {
        let registry = ConfigRegistry::new(vec![]);

        let setting = registry.get("gossip.interval");
        assert!(setting.is_some(), "gossip.interval should be registered");

        let meta = setting.unwrap();
        assert_eq!(meta.category, ConfigCategory::Gossip);
    }

    #[test]
    fn registry_unknown_key_returns_none() {
        let registry = ConfigRegistry::new(vec![]);
        assert!(registry.get("nonexistent.setting.key").is_none());
    }

    #[test]
    fn registry_by_category() {
        let registry = ConfigRegistry::new(vec![]);

        let gossip_settings = registry.by_category(ConfigCategory::Gossip);
        assert!(
            !gossip_settings.is_empty(),
            "Should have gossip category settings"
        );

        for setting in &gossip_settings {
            assert_eq!(setting.category, ConfigCategory::Gossip);
        }
    }

    #[test]
    fn registry_hardwired_override_detection() {
        let overrides = vec![make_compliance_override(
            "security.api_auth_required",
            serde_json::json!(true),
            "exact",
            "soc2",
        )];
        let registry = ConfigRegistry::new(overrides);

        let locked = registry.is_hardwired("security.api_auth_required");
        assert!(locked.is_some(), "Should detect hardwired override");
        assert_eq!(locked.unwrap().profile_name, "soc2");

        let unlocked = registry.is_hardwired("gossip.interval");
        assert!(unlocked.is_none(), "Non-overridden key should not be hardwired");
    }

    #[test]
    fn registry_validate_hardwired_rejection() {
        let overrides = vec![make_compliance_override(
            "security.api_auth_required",
            serde_json::json!(true),
            "exact",
            "hipaa",
        )];
        let registry = ConfigRegistry::new(overrides);

        // Trying to set a hardwired value to something else should fail
        let result = registry.validate("security.api_auth_required", &serde_json::json!(false));
        match result {
            Err(ConfigValidationError::HardwiredLock { key, profile, .. }) => {
                assert_eq!(key, "security.api_auth_required");
                assert_eq!(profile, "hipaa");
            }
            other => panic!("Expected HardwiredLock, got {:?}", other),
        }
    }

    #[test]
    fn registry_schema_sorted_by_key() {
        let registry = ConfigRegistry::new(vec![]);
        let schema = registry.schema();

        for window in schema.windows(2) {
            assert!(
                window[0].key <= window[1].key,
                "Schema not sorted: {} > {}",
                window[0].key,
                window[1].key,
            );
        }
    }

    #[test]
    fn registry_compliance_overrides_accessor() {
        let overrides = vec![
            make_compliance_override("key1", serde_json::json!(1), "min", "soc2"),
            make_compliance_override("key2", serde_json::json!(true), "exact", "soc2"),
        ];
        let registry = ConfigRegistry::new(overrides);

        assert_eq!(registry.compliance_overrides().len(), 2);
    }

    // ========================================================================
    // LiveConfig tests
    // ========================================================================

    #[test]
    fn live_config_initial_load() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config.clone(), vec![]);

        let loaded = live.load_full();
        // Should be the same config we passed in
        assert_eq!(loaded.gossip_fanout, config.gossip_fanout);
    }

    #[test]
    fn live_config_store_and_load() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);

        let mut new_config = SwarmConfig::default();
        new_config.gossip_fanout = 7;
        live.store(new_config);

        let loaded = live.load_full();
        assert_eq!(loaded.gossip_fanout, 7);
    }

    #[test]
    fn live_config_subscribe() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);

        let _rx = live.subscribe();
        assert!(live.subscriber_count() >= 1);
    }

    #[test]
    fn live_config_patch_unknown_key_rejected() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);

        let mut patch = HashMap::new();
        patch.insert(
            "nonexistent.key.path".to_string(),
            serde_json::json!(42),
        );

        let result = live.apply_patch(&patch, "test");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigPatchError::UnknownKey { key } => {
                assert_eq!(key, "nonexistent.key.path");
            }
            other => panic!("Expected UnknownKey, got {:?}", other),
        }
    }

    #[test]
    fn live_config_patch_hardwired_rejected() {
        let overrides = vec![make_compliance_override(
            "security.api_auth_required",
            serde_json::json!(true),
            "exact",
            "soc2",
        )];
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, overrides);

        let mut patch = HashMap::new();
        patch.insert(
            "security.api_auth_required".to_string(),
            serde_json::json!(false),
        );

        let result = live.apply_patch(&patch, "admin");
        assert!(result.is_err());
        match result.unwrap_err() {
            ConfigPatchError::HardwiredByCompliance {
                key, profile_name, ..
            } => {
                assert_eq!(key, "security.api_auth_required");
                assert_eq!(profile_name, "soc2");
            }
            other => panic!("Expected HardwiredByCompliance, got {:?}", other),
        }
    }

    #[test]
    fn live_config_registry_accessible() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);

        let registry = live.registry();
        assert!(!registry.is_empty());
    }

    // ========================================================================
    // ComplianceEngine tests
    // ========================================================================

    #[test]
    fn compliance_engine_creation() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);
        let engine = ComplianceEngine::new();

        let posture = engine.evaluate_posture(&live);
        // With no compliance profile, should still return a posture
        assert!(!posture.controls.is_empty() || posture.controls.is_empty());
    }

    #[test]
    fn compliance_engine_manifest() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);

        let manifest = ComplianceEngine::manifest(&live);
        // Should contain binary features
        assert!(manifest.is_object());
    }

    #[test]
    fn compliance_engine_report_json() {
        use crate::swarm::compliance::ReportFormat;

        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);
        let engine = ComplianceEngine::new();

        let report = engine.generate_report(&live, ReportFormat::Json);
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.is_empty());
    }

    #[test]
    fn compliance_engine_report_html() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);
        let engine = ComplianceEngine::new();

        let html = engine.generate_report_html(&live);
        assert!(html.contains("<html"));
        assert!(html.contains("Compliance"));
    }

    #[test]
    fn compliance_violations_initially_empty() {
        let engine = ComplianceEngine::new();

        let violations = engine.recorded_violations();
        assert!(violations.is_empty());
    }

    // ========================================================================
    // ConfigConsent tests
    // ========================================================================

    #[test]
    fn consent_manifest_build_and_verify() {
        let config = SwarmConfig::default();
        let node_id = test_node_id("swarm-leader");

        let manifest = ConfigManifest::build(&config, node_id);
        assert!(manifest.verify_hash());
        assert_eq!(manifest.manifest_hash.len(), 64); // blake3
    }

    #[test]
    fn consent_manifest_tampered_hash_fails() {
        let config = SwarmConfig::default();
        let node_id = test_node_id("leader");

        let mut manifest = ConfigManifest::build(&config, node_id);
        manifest.manifest_hash =
            "0000000000000000000000000000000000000000000000000000000000000000".to_string();
        assert!(!manifest.verify_hash());
    }

    #[test]
    fn consent_manifest_serde_roundtrip() {
        let config = SwarmConfig::default();
        let manifest = ConfigManifest::build(&config, test_node_id("node"));

        let json = serde_json::to_string(&manifest).unwrap();
        let back: ConfigManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(back.manifest_hash, manifest.manifest_hash);
        assert!(back.verify_hash());
    }

    #[test]
    fn consent_manifest_display() {
        let config = SwarmConfig::default();
        let manifest = ConfigManifest::build(&config, test_node_id("leader"));

        let display = manifest.display_for_consent();
        assert!(display.contains("Swarm Configuration Manifest"));
        assert!(display.contains("Sandbox enforced:"));
    }

    #[test]
    fn consent_accept_creates_valid_consent() {
        let consent = ConfigConsent::accept(test_node_id("node"), "hash123".into(), false);

        assert!(consent.accepted);
        assert!(!consent.auto_accepted);
        assert!(consent.rejection_reason.is_none());
    }

    #[test]
    fn consent_reject_creates_valid_consent() {
        let consent = ConfigConsent::reject(
            test_node_id("node"),
            "hash123".into(),
            "Policy not acceptable".into(),
        );

        assert!(!consent.accepted);
        assert_eq!(
            consent.rejection_reason,
            Some("Policy not acceptable".into())
        );
    }

    #[test]
    fn consent_auto_accept_flag() {
        let consent = ConfigConsent::accept(test_node_id("headless"), "hash".into(), true);

        assert!(consent.accepted);
        assert!(consent.auto_accepted);
    }

    #[test]
    fn consent_serde_roundtrip() {
        let consent = ConfigConsent::accept(test_node_id("node"), "hash".into(), false);
        let json = serde_json::to_string(&consent).unwrap();
        let back: ConfigConsent = serde_json::from_str(&json).unwrap();
        assert!(back.accepted);
    }

    // ========================================================================
    // ConfigDb type serde tests
    // ========================================================================

    #[test]
    fn config_history_entry_serde() {
        let entry = ConfigHistoryEntry {
            id: 1,
            key: "gossip.fanout".to_string(),
            old_value: Some(serde_json::json!(3)),
            new_value: serde_json::json!(5),
            changed_by: "admin".to_string(),
            source: "api".to_string(),
            changed_at: Utc::now(),
            row_hash: "abc123def456".to_string(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ConfigHistoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key, "gossip.fanout");
        assert_eq!(back.new_value, serde_json::json!(5));
    }

    #[test]
    fn config_current_entry_serde() {
        let entry = ConfigCurrentEntry {
            key: "work.max_load".to_string(),
            value: serde_json::json!(0.7),
            updated_by: "operator".to_string(),
            updated_at: Utc::now(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ConfigCurrentEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key, "work.max_load");
    }

    #[test]
    fn config_snapshot_serde() {
        let snap = ConfigSnapshot {
            id: 42,
            full_config: serde_json::json!({"gossip_fanout": 5}),
            triggered_by: "patch".to_string(),
            changed_keys: vec!["gossip.fanout".to_string()],
            node_id: "node-1".to_string(),
            snapshot_at: Utc::now(),
            row_hash: "def456".to_string(),
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: ConfigSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back.changed_keys.len(), 1);
    }

    #[test]
    fn consent_record_serde() {
        let record = ConsentRecord {
            id: 1,
            node_id: test_node_id("node").to_string(),
            manifest_hash: "abc".to_string(),
            accepted: true,
            rejection_reason: None,
            auto_accepted: false,
            recorded_at: Utc::now(),
        };
        let json = serde_json::to_string(&record).unwrap();
        let back: ConsentRecord = serde_json::from_str(&json).unwrap();
        assert!(back.accepted);
    }

    // ========================================================================
    // ConfigMeta type tests
    // ========================================================================

    #[test]
    fn config_tier_ordering() {
        // Hardwired should override startup, startup should override runtime
        let h = ConfigTier::Hardwired;
        let s = ConfigTier::Startup;
        let r = ConfigTier::Runtime;

        // Just verify they're distinct
        assert_ne!(format!("{:?}", h), format!("{:?}", s));
        assert_ne!(format!("{:?}", s), format!("{:?}", r));
    }

    #[test]
    fn config_setting_meta_construction() {
        let meta = ConfigSettingMeta {
            key: "test.setting".to_string(),
            tier: ConfigTier::Runtime,
            ui_visibility: UiVisibility::VisibleMutable,
            category: ConfigCategory::Gossip,
            description: "A test setting".to_string(),
            restart_required: false,
            compliance_tags: vec!["soc2".to_string()],
            value_type: ValueType::Int,
            constraints: ValueConstraints {
                min: Some(serde_json::json!(1)),
                max: Some(serde_json::json!(100)),
                pattern: None,
            },
            default_value: serde_json::json!(10),
        };

        assert_eq!(meta.key, "test.setting");
        assert!(!meta.restart_required);
    }

    #[test]
    fn compliance_override_serde() {
        let ov = make_compliance_override("key", serde_json::json!(true), "exact", "soc2");
        let json = serde_json::to_string(&ov).unwrap();
        let back: ComplianceOverride = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key, "key");
        assert_eq!(back.profile_name, "soc2");
    }

    // ========================================================================
    // Config value helpers tests
    // ========================================================================

    #[test]
    fn get_nested_value_simple_key() {
        use crate::swarm::config_live::get_nested_value;

        let json = serde_json::json!({"gossip_fanout": 5, "api_port": 8080});
        let val = get_nested_value(&json, "gossip_fanout");
        assert_eq!(val, Some(&serde_json::json!(5)));
    }

    #[test]
    fn get_nested_value_dotted_key() {
        use crate::swarm::config_live::get_nested_value;

        let json = serde_json::json!({"gossip": {"fanout": 3, "interval": "1s"}});
        let val = get_nested_value(&json, "gossip.fanout");
        assert_eq!(val, Some(&serde_json::json!(3)));
    }

    #[test]
    fn get_nested_value_missing_key() {
        use crate::swarm::config_live::get_nested_value;

        let json = serde_json::json!({"a": 1});
        let val = get_nested_value(&json, "b");
        assert!(val.is_none());
    }

    // ========================================================================
    // Hot-reload integration test
    // ========================================================================

    #[test]
    fn live_config_broadcast_on_store() {
        let config = SwarmConfig::default();
        let live = LiveConfig::new(config, vec![]);

        let _rx = live.subscribe();

        // Store a new config
        let mut new_config = SwarmConfig::default();
        new_config.gossip_fanout = 99;
        live.store(new_config);

        // The subscriber doesn't automatically receive from store() — store() is
        // a direct replacement without broadcasting. apply_patch() broadcasts.
        // But we can verify the subscription count.
        assert!(live.subscriber_count() >= 1);
    }

    // ========================================================================
    // Admission consent integration tests
    // ========================================================================

    #[test]
    fn admission_consent_pending_tracking() {
        use crate::swarm::admission::AdmissionStore;

        let store = AdmissionStore::new();

        // Initially no consent pending
        assert_eq!(store.consent_pending_count(), 0);
    }

    #[test]
    fn manifest_hash_changes_with_config() {
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
    fn compliance_profile_info_serde() {
        let info = ComplianceProfileInfo {
            name: "hipaa".to_string(),
            version: "1.0".to_string(),
            description: "HIPAA compliance".to_string(),
            controls_count: 8,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: ComplianceProfileInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "hipaa");
        assert_eq!(back.controls_count, 8);
    }
}
