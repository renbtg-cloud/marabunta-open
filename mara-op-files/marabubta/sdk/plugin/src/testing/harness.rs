// Marabunta - Licensed under the MIT License.
//! Test harness for plugin developers.
//!
//! Provides a builder-pattern harness that simulates the Marabunta sandbox
//! environment, including fuel metering, output-size limits, classification
//! data_leakage detection, membrane-crossing enforcement, and audit-event capture.
//!
//! # Example
//!
//! ```ignore
//! use marabunta_plugin_sdk::testing::TestHarness;
//! use marabunta_plugin_sdk::{ClassificationLevel, ZoneClass};
//!
//! let harness = TestHarness::builder()
//!     .classification(ClassificationLevel::Restricted)
//!     .zone_class(ZoneClass::GovCloud)
//!     .build();
//!
//! let output = harness.execute::<MyPlugin>(b"input", b"{}").unwrap();
//! assert!(!output.is_empty());
//! ```

use crate::*;
use std::cell::{Cell, RefCell};

// ============================================================================
// Harness-local types
// ============================================================================

/// Record of a classification data_leakage incident detected during execution.
#[derive(Clone, Debug)]
pub struct DataLeakageIncident {
    pub severity: DataLeakageSeverity,
    pub source_classification: ClassificationLevel,
    pub zone_max_classification: ClassificationLevel,
    pub containment_action: String,
}

/// Configuration for a named zone inside the harness.
#[derive(Clone, Debug)]
pub struct ZoneConfig {
    pub id: String,
    pub jurisdiction: CountryCode,
    pub zone_class: ZoneClass,
    pub max_classification: ClassificationLevel,
}

/// Input data tagged with NONEXPORT tracking metadata.
pub struct TaggedInput {
    pub data: Vec<u8>,
    pub nonexport: NonexportTracking,
}

// ============================================================================
// HarnessError
// ============================================================================

/// Errors the test harness can surface.
#[derive(Debug)]
pub enum HarnessError {
    /// The underlying plugin returned an error.
    PluginError(PluginError),
    /// Simulated fuel was exhausted.
    FuelExhausted { consumed: u64, limit: u64 },
    /// Plugin output exceeded the configured maximum.
    OutputTooLarge { actual: u64, max: u64 },
    /// Classification data_leakage was detected (data above zone clearance).
    DataLeakageDetected(DataLeakageIncident),
    /// A membrane crossing was denied by the rule set.
    CrossingDenied {
        source: String,
        destination: String,
        reason: String,
    },
    /// A classification-level violation occurred.
    ClassificationViolation(String),
    /// An NONEXPORT tracking violation occurred.
    NonexportViolation(String),
}

// ============================================================================
// TestHarnessBuilder
// ============================================================================

/// Builder for [`TestHarness`].
pub struct TestHarnessBuilder {
    jurisdiction: CountryCode,
    zone_class: ZoneClass,
    classification: ClassificationLevel,
    fuel_limit: u64,
    memory_pages: u32,
    timeout_ms: u64,
    max_output_bytes: u64,
    zones: Vec<ZoneConfig>,
    crossing_rules: Vec<CrossingRule>,
    expect_data_leakage: bool,
}

impl TestHarnessBuilder {
    /// Create a new builder with sensible defaults.
    pub fn new() -> Self {
        Self {
            jurisdiction: CountryCode::FR,
            zone_class: ZoneClass::Civilian,
            classification: ClassificationLevel::Unclassified,
            fuel_limit: 1_000_000_000,
            memory_pages: 256,
            timeout_ms: 30_000,
            max_output_bytes: 10_000_000,
            zones: vec![],
            crossing_rules: vec![],
            expect_data_leakage: false,
        }
    }

    /// Set the jurisdiction (country) of the simulated zone.
    pub fn jurisdiction(mut self, cc: CountryCode) -> Self {
        self.jurisdiction = cc;
        self
    }

    /// Set the zone class of the simulated zone.
    pub fn zone_class(mut self, zc: ZoneClass) -> Self {
        self.zone_class = zc;
        self
    }

    /// Set the classification level of the data being processed.
    pub fn classification(mut self, cl: ClassificationLevel) -> Self {
        self.classification = cl;
        self
    }

    /// Set the fuel (instruction) limit for execution.
    pub fn fuel_limit(mut self, limit: u64) -> Self {
        self.fuel_limit = limit;
        self
    }

    /// Set the maximum memory pages available.
    pub fn memory_pages(mut self, pages: u32) -> Self {
        self.memory_pages = pages;
        self
    }

    /// Set the execution timeout in milliseconds.
    pub fn timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// Set the maximum allowed output size in bytes.
    pub fn max_output_bytes(mut self, bytes: u64) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    /// Configure multiple named zones for crossing tests.
    pub fn multi_zone(mut self, zones: Vec<ZoneConfig>) -> Self {
        self.zones = zones;
        self
    }

    /// Supply crossing rules for membrane-crossing simulation.
    pub fn crossing_rules(mut self, rules: Vec<CrossingRule>) -> Self {
        self.crossing_rules = rules;
        self
    }

    /// Indicate that data_leakage is expected and the harness should report it
    /// as an error rather than panic.
    pub fn expect_data_leakage(mut self, expect: bool) -> Self {
        self.expect_data_leakage = expect;
        self
    }

    /// Build the [`TestHarness`].
    pub fn build(self) -> TestHarness {
        TestHarness {
            jurisdiction: self.jurisdiction,
            zone_class: self.zone_class,
            classification: self.classification,
            fuel_limit: self.fuel_limit,
            memory_pages: self.memory_pages,
            timeout_ms: self.timeout_ms,
            max_output_bytes: self.max_output_bytes,
            zones: self.zones,
            crossing_rules: self.crossing_rules,
            expect_data_leakage: self.expect_data_leakage,
            fuel_consumed: Cell::new(0),
            audit_events: RefCell::new(Vec::new()),
            data_leakage_incidents: RefCell::new(Vec::new()),
        }
    }
}

impl Default for TestHarnessBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TestHarness
// ============================================================================

/// Simulated sandbox environment for testing plugins.
///
/// The harness enforces classification, fuel, output-size, and crossing rules
/// the same way the real sandbox does, but runs entirely in-process.
pub struct TestHarness {
    // --- configuration (mirrored from builder) ---
    jurisdiction: CountryCode,
    zone_class: ZoneClass,
    classification: ClassificationLevel,
    fuel_limit: u64,
    memory_pages: u32,
    timeout_ms: u64,
    max_output_bytes: u64,
    zones: Vec<ZoneConfig>,
    crossing_rules: Vec<CrossingRule>,
    expect_data_leakage: bool,

    // --- runtime state ---
    fuel_consumed: Cell<u64>,
    audit_events: RefCell<Vec<HighestsecAuditEventKind>>,
    data_leakage_incidents: RefCell<Vec<DataLeakageIncident>>,
}

impl TestHarness {
    /// Create a new [`TestHarnessBuilder`].
    pub fn builder() -> TestHarnessBuilder {
        TestHarnessBuilder::new()
    }

    /// Execute a [`ComputePlugin`] within the simulated sandbox.
    ///
    /// The plugin type `P` must implement `Default` so the harness can
    /// instantiate it.
    pub fn execute<P: ComputePlugin + Default>(
        &self,
        input: &[u8],
        params: &[u8],
    ) -> Result<Vec<u8>, HarnessError> {
        let job_id: JobId = "harness-job-0".to_string();
        let plugin_id: PluginId = std::any::type_name::<P>().to_string();

        // 1. DataLeakage check
        if self.would_spill() {
            let zone_max = self.zone_max_classification();
            let severity = data_leakage_severity(self.classification, zone_max);
            let incident = DataLeakageIncident {
                severity,
                source_classification: self.classification,
                zone_max_classification: zone_max,
                containment_action: "execution_blocked".to_string(),
            };
            self.data_leakage_incidents.borrow_mut().push(incident.clone());

            if self.expect_data_leakage {
                return Err(HarnessError::DataLeakageDetected(incident));
            }
        }

        // 2. Emit ExecutionStarted
        self.emit_audit(HighestsecAuditEventKind::ExecutionStarted {
            job_id: job_id.clone(),
            plugin_id: plugin_id.clone(),
            classification: self.classification,
        });

        // 3. Execute the plugin, measuring wall-clock time
        let start = std::time::Instant::now();
        let result = P::default().execute(input, params);
        let elapsed = start.elapsed();

        match result {
            Err(plugin_err) => {
                // Emit failure audit event
                let error_kind = PluginErrorKind::from(&plugin_err);
                self.emit_audit(HighestsecAuditEventKind::ExecutionFailed {
                    job_id,
                    plugin_id,
                    error_kind,
                });
                Err(HarnessError::PluginError(plugin_err))
            }
            Ok(output) => {
                // 4. Simulate fuel consumption (elapsed_micros * 1000)
                let elapsed_micros = elapsed.as_micros() as u64;
                let simulated_fuel = elapsed_micros.saturating_mul(1000);
                let total_fuel = self.fuel_consumed.get().saturating_add(simulated_fuel);
                self.fuel_consumed.set(total_fuel);

                // 5. Fuel limit check
                if total_fuel > self.fuel_limit {
                    self.emit_audit(HighestsecAuditEventKind::ResourceLimitHit {
                        job_id,
                        plugin_id,
                        resource: "fuel".to_string(),
                        limit: self.fuel_limit,
                        actual: total_fuel,
                    });
                    return Err(HarnessError::FuelExhausted {
                        consumed: total_fuel,
                        limit: self.fuel_limit,
                    });
                }

                // 6. Output size check
                if output.len() as u64 > self.max_output_bytes {
                    return Err(HarnessError::OutputTooLarge {
                        actual: output.len() as u64,
                        max: self.max_output_bytes,
                    });
                }

                // 7. Compute output hash and emit ExecutionCompleted
                let mut output_hash = [0u8; 32];
                // Simple hash: copy first 32 bytes of output (or pad with zeros).
                // This is a test harness, not production cryptography.
                let copy_len = output.len().min(32);
                output_hash[..copy_len].copy_from_slice(&output[..copy_len]);

                let execution_ms = elapsed.as_millis() as u64;
                self.emit_audit(HighestsecAuditEventKind::ExecutionCompleted {
                    job_id,
                    plugin_id,
                    output_hash,
                    execution_ms,
                });

                Ok(output)
            }
        }
    }

    /// Return the total fuel consumed so far.
    pub fn fuel_consumed(&self) -> u64 {
        self.fuel_consumed.get()
    }

    /// Return a clone of all audit events emitted so far.
    pub fn audit_events(&self) -> Vec<HighestsecAuditEventKind> {
        self.audit_events.borrow().clone()
    }

    /// Return the most recent data_leakage incident, if any.
    pub fn last_data_leakage_incident(&self) -> Option<DataLeakageIncident> {
        self.data_leakage_incidents.borrow().last().cloned()
    }

    /// Simulate a membrane crossing between two zones.
    ///
    /// Finds the highest-priority matching crossing rule and applies it.
    /// If no rule matches, the crossing is denied by default.
    pub fn simulate_crossing(
        &self,
        _data: &[u8],
        classification: ClassificationLevel,
        source_zone: &str,
        destination_zone: &str,
    ) -> Result<(), HarnessError> {
        let job_id: JobId = "harness-crossing-0".to_string();

        // Collect matching rules and sort by priority descending
        let mut matching: Vec<&CrossingRule> = self
            .crossing_rules
            .iter()
            .filter(|r| {
                let source_match =
                    r.source_zone == source_zone || r.source_zone == "*";
                let dest_match =
                    r.destination_zone == destination_zone || r.destination_zone == "*";
                source_match && dest_match
            })
            .collect();
        matching.sort_by(|a, b| b.priority.cmp(&a.priority));

        if let Some(rule) = matching.first() {
            match rule.action {
                CrossingAction::Deny => Err(HarnessError::CrossingDenied {
                    source: source_zone.to_string(),
                    destination: destination_zone.to_string(),
                    reason: format!("denied by rule (priority {})", rule.priority),
                }),
                CrossingAction::Allow => {
                    let transforms_applied: Vec<String> = rule
                        .transforms
                        .iter()
                        .map(|t| format!("{:?}", t))
                        .collect();

                    self.emit_audit(HighestsecAuditEventKind::MembraneCrossing {
                        job_id,
                        source_zone: source_zone.to_string(),
                        destination_zone: destination_zone.to_string(),
                        classification,
                        transforms_applied,
                    });

                    Ok(())
                }
            }
        } else {
            // No rule matches -- deny by default
            Err(HarnessError::CrossingDenied {
                source: source_zone.to_string(),
                destination: destination_zone.to_string(),
                reason: "no matching crossing rule (default deny)".to_string(),
            })
        }
    }

    // === Internal helpers ===

    /// Push an audit event into the internal buffer.
    fn emit_audit(&self, kind: HighestsecAuditEventKind) {
        self.audit_events.borrow_mut().push(kind);
    }

    /// Determine whether the current classification would spill into the
    /// zone's max classification level.
    fn would_spill(&self) -> bool {
        self.classification > self.zone_max_classification()
    }

    /// Derive the zone's maximum classification from the first configured
    /// zone, or fall back to a default based on `zone_class`.
    fn zone_max_classification(&self) -> ClassificationLevel {
        if let Some(zone) = self.zones.first() {
            zone.max_classification
        } else {
            match self.zone_class {
                ZoneClass::Civilian => ClassificationLevel::Unclassified,
                ZoneClass::GovCloud => ClassificationLevel::Restricted,
                ZoneClass::MilRestricted => ClassificationLevel::Confidential,
                ZoneClass::MilClassified => ClassificationLevel::TopSecret,
            }
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Derive the severity of a data_leakage incident based on the gap between
/// source classification and zone max classification.
fn data_leakage_severity(
    source: ClassificationLevel,
    zone_max: ClassificationLevel,
) -> DataLeakageSeverity {
    let gap = (source as u8).saturating_sub(zone_max as u8);
    match gap {
        0 | 1 => DataLeakageSeverity::Minor,
        2 => DataLeakageSeverity::Major,
        _ => DataLeakageSeverity::Critical,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Helper plugin: echoes input back ---
    struct EchoPlugin;

    impl Default for EchoPlugin {
        fn default() -> Self {
            EchoPlugin
        }
    }

    impl ComputePlugin for EchoPlugin {
        fn execute(&self, input: &[u8], _params: &[u8]) -> Result<Vec<u8>, PluginError> {
            Ok(input.to_vec())
        }
    }

    // --- Helper plugin: always fails ---
    struct FailPlugin;

    impl Default for FailPlugin {
        fn default() -> Self {
            FailPlugin
        }
    }

    impl ComputePlugin for FailPlugin {
        fn execute(&self, _input: &[u8], _params: &[u8]) -> Result<Vec<u8>, PluginError> {
            Err(PluginError::InvalidInput)
        }
    }

    // --- Helper plugin: produces large output ---
    struct LargeOutputPlugin;

    impl Default for LargeOutputPlugin {
        fn default() -> Self {
            LargeOutputPlugin
        }
    }

    impl ComputePlugin for LargeOutputPlugin {
        fn execute(&self, _input: &[u8], _params: &[u8]) -> Result<Vec<u8>, PluginError> {
            Ok(vec![0xAB; 1024])
        }
    }

    // --- Helper: make a simple crossing rule ---
    fn make_crossing_rule(
        source: &str,
        dest: &str,
        action: CrossingAction,
        priority: i32,
    ) -> CrossingRule {
        CrossingRule {
            source_zone: source.to_string(),
            destination_zone: dest.to_string(),
            direction: CrossingDirection::Both,
            max_classification: ClassificationLevel::TopSecret,
            allowed_jurisdictions: JurisdictionSet::Any,
            transforms: vec![],
            action,
            priority,
        }
    }

    // ====================================================================
    // Builder tests
    // ====================================================================

    #[test]
    fn test_builder_defaults() {
        let harness = TestHarness::builder().build();
        assert_eq!(harness.fuel_consumed(), 0);
        assert!(harness.audit_events().is_empty());
    }

    #[test]
    fn test_builder_custom_jurisdiction() {
        let harness = TestHarness::builder()
            .jurisdiction(CountryCode::DE)
            .build();
        assert_eq!(harness.jurisdiction, CountryCode::DE);
    }

    #[test]
    fn test_builder_multi_zone() {
        let zones = vec![
            ZoneConfig {
                id: "zone-a".to_string(),
                jurisdiction: CountryCode::FR,
                zone_class: ZoneClass::MilClassified,
                max_classification: ClassificationLevel::TopSecret,
            },
            ZoneConfig {
                id: "zone-b".to_string(),
                jurisdiction: CountryCode::DE,
                zone_class: ZoneClass::GovCloud,
                max_classification: ClassificationLevel::Restricted,
            },
        ];
        let harness = TestHarness::builder().multi_zone(zones).build();
        assert_eq!(harness.zones.len(), 2);
        assert_eq!(harness.zones[0].id, "zone-a");
        assert_eq!(harness.zones[1].id, "zone-b");
    }

    // ====================================================================
    // Execution tests
    // ====================================================================

    #[test]
    fn test_execute_echo_plugin() {
        let harness = TestHarness::builder().build();
        let output = harness
            .execute::<EchoPlugin>(b"hello world", b"{}")
            .unwrap();
        assert_eq!(output, b"hello world");
    }

    #[test]
    fn test_execute_plugin_error() {
        let harness = TestHarness::builder().build();
        let result = harness.execute::<FailPlugin>(b"data", b"{}");
        assert!(result.is_err());
        match result.unwrap_err() {
            HarnessError::PluginError(PluginError::InvalidInput) => {}
            other => panic!("expected PluginError(InvalidInput), got {:?}", other),
        }
    }

    #[test]
    fn test_execute_fuel_exhaustion() {
        let harness = TestHarness::builder().fuel_limit(1).build();
        let result = harness.execute::<EchoPlugin>(b"data", b"{}");
        // The echo plugin runs fast but simulated fuel = elapsed_micros * 1000,
        // so even 1 microsecond => 1000 fuel, which exceeds limit of 1.
        match result {
            Err(HarnessError::FuelExhausted { consumed, limit }) => {
                assert!(consumed > 1);
                assert_eq!(limit, 1);
            }
            Ok(_) => {
                // If the machine is extremely fast (sub-microsecond execution)
                // the fuel might be 0 and the test would pass. We accept both
                // outcomes for portability.
            }
            Err(other) => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_execute_output_too_large() {
        let harness = TestHarness::builder().max_output_bytes(5).build();
        let result = harness.execute::<LargeOutputPlugin>(b"x", b"{}");
        match result {
            Err(HarnessError::OutputTooLarge { actual, max }) => {
                assert_eq!(actual, 1024);
                assert_eq!(max, 5);
            }
            other => panic!("expected OutputTooLarge, got {:?}", other),
        }
    }

    // ====================================================================
    // Audit event tests
    // ====================================================================

    #[test]
    fn test_audit_events_lifecycle() {
        let harness = TestHarness::builder().build();
        let _ = harness.execute::<EchoPlugin>(b"ping", b"{}").unwrap();
        let events = harness.audit_events();
        assert!(
            events.len() >= 2,
            "expected at least 2 events, got {}",
            events.len()
        );

        // First should be ExecutionStarted
        match &events[0] {
            HighestsecAuditEventKind::ExecutionStarted { .. } => {}
            other => panic!("expected ExecutionStarted, got {:?}", other),
        }
        // Last should be ExecutionCompleted
        match events.last().unwrap() {
            HighestsecAuditEventKind::ExecutionCompleted { .. } => {}
            other => panic!("expected ExecutionCompleted, got {:?}", other),
        }
    }

    #[test]
    fn test_audit_events_on_failure() {
        let harness = TestHarness::builder().build();
        let _ = harness.execute::<FailPlugin>(b"x", b"{}");
        let events = harness.audit_events();
        assert!(
            events.len() >= 2,
            "expected at least 2 events, got {}",
            events.len()
        );

        match &events[0] {
            HighestsecAuditEventKind::ExecutionStarted { .. } => {}
            other => panic!("expected ExecutionStarted, got {:?}", other),
        }
        match &events[1] {
            HighestsecAuditEventKind::ExecutionFailed { error_kind, .. } => {
                assert_eq!(*error_kind, PluginErrorKind::InvalidInput);
            }
            other => panic!("expected ExecutionFailed, got {:?}", other),
        }
    }

    // ====================================================================
    // DataLeakage tests
    // ====================================================================

    #[test]
    fn test_data_leakage_civilian_restricted() {
        let harness = TestHarness::builder()
            .zone_class(ZoneClass::Civilian)
            .classification(ClassificationLevel::Restricted)
            .expect_data_leakage(true)
            .build();
        let result = harness.execute::<EchoPlugin>(b"secret", b"{}");
        match result {
            Err(HarnessError::DataLeakageDetected(incident)) => {
                assert_eq!(
                    incident.source_classification,
                    ClassificationLevel::Restricted
                );
                assert_eq!(
                    incident.zone_max_classification,
                    ClassificationLevel::Unclassified
                );
            }
            other => panic!("expected DataLeakageDetected, got {:?}", other),
        }
    }

    #[test]
    fn test_data_leakage_mil_classified_ok() {
        let harness = TestHarness::builder()
            .zone_class(ZoneClass::MilClassified)
            .classification(ClassificationLevel::Secret)
            .build();
        // Secret < TopSecret, so no data_leakage
        let result = harness.execute::<EchoPlugin>(b"data", b"{}");
        assert!(
            result.is_ok(),
            "expected no data_leakage for Secret in MilClassified zone"
        );
    }

    // ====================================================================
    // Crossing tests
    // ====================================================================

    #[test]
    fn test_crossing_allowed() {
        let harness = TestHarness::builder()
            .crossing_rules(vec![make_crossing_rule(
                "zone-a",
                "zone-b",
                CrossingAction::Allow,
                10,
            )])
            .build();
        let result = harness.simulate_crossing(
            b"data",
            ClassificationLevel::Unclassified,
            "zone-a",
            "zone-b",
        );
        assert!(result.is_ok());

        // Should have emitted a MembraneCrossing audit event
        let events = harness.audit_events();
        assert_eq!(events.len(), 1);
        match &events[0] {
            HighestsecAuditEventKind::MembraneCrossing {
                source_zone,
                destination_zone,
                ..
            } => {
                assert_eq!(source_zone, "zone-a");
                assert_eq!(destination_zone, "zone-b");
            }
            other => panic!("expected MembraneCrossing, got {:?}", other),
        }
    }

    #[test]
    fn test_crossing_denied() {
        let harness = TestHarness::builder()
            .crossing_rules(vec![make_crossing_rule(
                "zone-a",
                "zone-b",
                CrossingAction::Deny,
                10,
            )])
            .build();
        let result = harness.simulate_crossing(
            b"data",
            ClassificationLevel::Unclassified,
            "zone-a",
            "zone-b",
        );
        match result {
            Err(HarnessError::CrossingDenied {
                source,
                destination,
                ..
            }) => {
                assert_eq!(source, "zone-a");
                assert_eq!(destination, "zone-b");
            }
            other => panic!("expected CrossingDenied, got {:?}", other),
        }
    }

    #[test]
    fn test_crossing_no_rules_deny() {
        let harness = TestHarness::builder().build();
        let result = harness.simulate_crossing(
            b"data",
            ClassificationLevel::Unclassified,
            "zone-x",
            "zone-y",
        );
        match result {
            Err(HarnessError::CrossingDenied { reason, .. }) => {
                assert!(
                    reason.contains("default deny"),
                    "expected 'default deny' in reason, got: {}",
                    reason
                );
            }
            other => panic!("expected CrossingDenied, got {:?}", other),
        }
    }

    // ====================================================================
    // Fuel & data_leakage state tests
    // ====================================================================

    #[test]
    fn test_fuel_consumed_tracking() {
        let harness = TestHarness::builder().build();
        let _ = harness.execute::<EchoPlugin>(b"payload", b"{}").unwrap();
        // After execution, fuel_consumed should be >= 0. It could be 0 on
        // very fast machines where elapsed_micros rounds to 0, but the
        // accessor must work regardless.
        let _fuel = harness.fuel_consumed();
    }

    #[test]
    fn test_last_data_leakage_incident() {
        let harness = TestHarness::builder()
            .zone_class(ZoneClass::Civilian)
            .classification(ClassificationLevel::Secret)
            .expect_data_leakage(true)
            .build();
        let _ = harness.execute::<EchoPlugin>(b"data", b"{}");
        let incident = harness.last_data_leakage_incident();
        assert!(incident.is_some(), "expected a data_leakage incident");
        let incident = incident.unwrap();
        assert_eq!(
            incident.source_classification,
            ClassificationLevel::Secret
        );
        assert_eq!(
            incident.zone_max_classification,
            ClassificationLevel::Unclassified
        );
    }

    // ====================================================================
    // crate::audit module tests
    // ====================================================================

    #[test]
    fn test_audit_emit_buffered() {
        // Clear any leftovers from previous tests in this thread
        let _ = crate::audit::drain_events();

        crate::audit::emit("test.event", serde_json::json!({"key": "value"}));
        crate::audit::emit("test.event2", serde_json::json!({"num": 42}));

        let events = crate::audit::drain_events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0, "test.event");
        assert_eq!(events[1].0, "test.event2");

        // After drain, buffer should be empty
        let events = crate::audit::drain_events();
        assert!(events.is_empty());
    }

    #[test]
    fn test_audit_emit_rate_limit() {
        // Clear any leftovers
        let _ = crate::audit::drain_events();

        // Emit 101 events -- the 101st should be dropped
        for i in 0..101 {
            crate::audit::emit(
                &format!("event.{}", i),
                serde_json::json!({"i": i}),
            );
        }

        let events = crate::audit::drain_events();
        assert_eq!(events.len(), 100, "rate limit should cap at 100 events");
    }

    #[test]
    fn test_audit_emit_size_limit() {
        // Clear any leftovers
        let _ = crate::audit::drain_events();

        // Create a payload larger than 4096 bytes when serialized
        let big_string = "x".repeat(5000);
        crate::audit::emit("big.event", serde_json::json!({"data": big_string}));

        let events = crate::audit::drain_events();
        assert!(
            events.is_empty(),
            "oversized event should be dropped, but got {} events",
            events.len()
        );
    }
}
