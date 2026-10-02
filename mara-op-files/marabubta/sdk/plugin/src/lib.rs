// Marabunta - Licensed under the MIT License.
//! Marabunta Plugin SDK
//!
//! This crate provides the traits and types contractors need to build
//! plugins for the Marabunta Radiation platform.

pub mod prelude;
pub mod ffi;
pub mod testing;

// === Plugin traits ===
pub use types::ComputePlugin;
pub use types::SovereigntyPlugin;
pub use types::ValidationPlugin;

// === Error and result types ===
pub use types::PluginError;
pub use types::PluginErrorKind;
pub use types::Verdict;
pub use types::VerdictDecision;
pub use types::VerdictReason;
pub use types::CriticalityLevel;

// === Jurisdiction types ===
pub use types::CountryCode;
pub use types::ZoneClass;
pub use types::Jurisdiction;
pub use types::JurisdictionSet;
pub use types::AllianceOrg;
pub use types::NationalAuthority;
pub use types::AllianceResolver;
pub use types::StaticAllianceResolver;

// === Classification types ===
pub use types::ClassificationLevel;
pub use types::HandlingRestriction;
pub use types::ReleasabilityMarking;
pub use types::Compartment;
pub use types::HandlingHandlingRestriction;
pub use types::ClassificationRequirements;

// === Manifest ===
pub use types::PluginManifest;
pub use types::ManifestBuilder;
pub use types::ManifestValidationError;
pub use types::PluginKind;
pub use types::SemVer;
pub use types::Ed25519Signature;
pub use types::PluginState;

// === Constraint types ===
pub use types::DataConstraint;
pub use types::ConstraintSet;
pub use types::NonexportTracking;
pub use types::CustodyRecord;
pub use types::CustodyOperation;

// === Crossing/membrane types ===
pub use types::CrossingRule;
pub use types::CrossingDirection;
pub use types::CrossingAction;
pub use types::MembraneTransform;
pub use types::FieldPattern;
pub use types::AggregationMethod;
pub use types::KeyReleasePolicy;
pub use types::ClassificationPolicy;
pub use types::AuthorityChain;
pub use types::AuthoritySignature;
pub use types::Attestation;
pub use types::ReferenceDataManifest;

// === Audit ===
pub use types::HighestsecAuditEventKind;
pub use types::LoadFailureReason;
pub use types::RevocationReason;
pub use types::DataLeakageSeverity;

// === ID type aliases ===
pub use types::PluginId;
pub use types::ZoneId;
pub use types::KeyId;
pub use types::JobId;

pub use marabunta_plugin_macros::blind;

// Re-export the proc macro
pub use marabunta_plugin_macros::marabunta_plugin;

// Internal types module — these are COPIES of the highestsec types,
// not re-exports from the core crate. The SDK is standalone.
mod types;

/// Audit event emission for plugins.
pub mod audit {
    /// Emit a custom audit event from within a plugin.
    ///
    /// Rate-limited: max 100 events per execution.
    /// Size-limited: max 4 KiB per event payload.
    ///
    /// This is a no-op when running in the test harness.
    /// In the real sandbox, it becomes a host call.
    pub fn emit(event_type: &str, payload: serde_json::Value) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            AUDIT_BUFFER.with(|buf| {
                let mut buf = buf.borrow_mut();
                if buf.len() < 100 {
                    let payload_str = payload.to_string();
                    if payload_str.len() <= 4096 {
                        buf.push((event_type.to_string(), payload));
                    }
                }
            });
        }

        #[cfg(target_arch = "wasm32")]
        {
            extern "C" {
                fn __marabunta_audit_emit(ptr: *const u8, len: u32);
            }
            let msg = serde_json::json!({
                "event_type": event_type,
                "payload": payload,
            });
            let bytes = serde_json::to_vec(&msg).unwrap_or_default();
            unsafe {
                __marabunta_audit_emit(bytes.as_ptr(), bytes.len() as u32);
            }
        }
    }

    thread_local! {
        static AUDIT_BUFFER: std::cell::RefCell<Vec<(String, serde_json::Value)>> =
            std::cell::RefCell::new(Vec::new());
    }

    /// Drain buffered audit events (for test harness).
    pub fn drain_events() -> Vec<(String, serde_json::Value)> {
        AUDIT_BUFFER.with(|buf| buf.borrow_mut().drain(..).collect())
    }
}
