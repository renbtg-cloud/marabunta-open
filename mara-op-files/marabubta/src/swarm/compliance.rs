// Marabunta - Licensed under the MIT License.
//! Compliance engine — evaluate, report, and enforce regulatory posture.
//!
//! Works with the three-tier config system (config_meta, config_live) and
//! build-time compliance profiles (build.rs → `compliance_generated.rs`).
//!
//! # Workflow
//!
//! 1. At compile time, `build.rs` reads `compliance/*.toml` and generates
//!    `COMPLIANCE_PROFILE_NAME`, `COMPLIANCE_PROFILE_VERSION`, and
//!    `compliance_overrides()`.
//! 2. `ConfigRegistry` stores both setting metadata and compliance overrides.
//! 3. `ComplianceEngine::evaluate_posture()` checks each override against the
//!    live config, producing a green/amber/red status per control.
//! 4. `ComplianceEngine::generate_report()` produces a full JSON or HTML report.
//! 5. `ComplianceEngine::check_violations()` returns only failing controls.
//!
//! # Integration
//!
//! Wired into `SwarmNode` as `compliance_engine: Option<Arc<ComplianceEngine>>`.
//! Exposed via 6 REST endpoints under `/api/v1/compliance/`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::config::{
    COMPLIANCE_PROFILE_DESCRIPTION, COMPLIANCE_PROFILE_NAME, COMPLIANCE_PROFILE_VERSION,
};
use super::config_live::{get_nested_value, LiveConfig};
use super::config_meta::ComplianceOverride;

// ============================================================================
// Types
// ============================================================================

/// Status of a single compliance control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlStatus {
    /// Current value satisfies the control.
    Passing,
    /// Current value is close to threshold (within 10% of min/max).
    Warning,
    /// Current value violates the control.
    Violation,
    /// Cannot evaluate (config key missing or unparseable).
    Unknown,
}

/// A single evaluated compliance control.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceControl {
    /// The config key being checked.
    pub key: String,
    /// The required value from the compliance profile.
    pub required_value: serde_json::Value,
    /// The current value in the live config.
    pub current_value: serde_json::Value,
    /// Constraint type (exact, min, max).
    pub constraint_type: String,
    /// Evaluation result.
    pub status: ControlStatus,
    /// Human-readable justification from the compliance profile.
    pub justification: String,
    /// Which profile this control belongs to.
    pub profile_name: String,
    /// UI visibility setting.
    pub ui_visibility: String,
}

/// Aggregate compliance posture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompliancePosture {
    /// Active compliance profile name (if any).
    pub profile_name: Option<String>,
    /// Profile version.
    pub profile_version: Option<String>,
    /// Profile description.
    pub profile_description: Option<String>,
    /// Overall status: passing if ALL controls pass, violation if ANY fail.
    pub overall_status: ControlStatus,
    /// Individual control evaluations.
    pub controls: Vec<ComplianceControl>,
    /// Summary counts.
    pub passing_count: usize,
    pub warning_count: usize,
    pub violation_count: usize,
    pub unknown_count: usize,
    /// When this posture was evaluated.
    pub evaluated_at: DateTime<Utc>,
}

/// A recorded compliance violation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceViolation {
    /// When the violation was detected.
    pub detected_at: DateTime<Utc>,
    /// The control that failed.
    pub control: ComplianceControl,
    /// Whether auto-remediation was attempted.
    pub remediation_attempted: bool,
    /// Remediation result (if attempted).
    pub remediation_result: Option<String>,
}

/// Report format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportFormat {
    Json,
    Html,
}

/// Full compliance report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReport {
    /// Report title.
    pub title: String,
    /// When the report was generated.
    pub generated_at: DateTime<Utc>,
    /// Current posture snapshot.
    pub posture: CompliancePosture,
    /// Binary attestation: which cargo features were enabled.
    pub build_features: Vec<String>,
    /// Marabunta-compute version (from Cargo.toml).
    pub version: String,
}

// ============================================================================
// ComplianceEngine
// ============================================================================

/// Evaluates and reports on compliance posture.
///
/// Stateless — all checks are computed from the live config and the
/// compile-time compliance overrides stored in the `ConfigRegistry`.
pub struct ComplianceEngine {
    /// Recorded violations (ring buffer, last N).
    violations: parking_lot::Mutex<Vec<ComplianceViolation>>,
    /// Max violations to keep.
    max_violations: usize,
}

impl ComplianceEngine {
    /// Create a new compliance engine.
    pub fn new() -> Self {
        Self {
            violations: parking_lot::Mutex::new(Vec::new()),
            max_violations: 10_000,
        }
    }

    /// Evaluate the current compliance posture.
    ///
    /// Checks each compliance override against the live config state.
    pub fn evaluate_posture(&self, live_config: &LiveConfig) -> CompliancePosture {
        let now = Utc::now();
        let overrides = live_config.registry().compliance_overrides();

        if overrides.is_empty() {
            return CompliancePosture {
                profile_name: COMPLIANCE_PROFILE_NAME.map(|s: &str| s.to_string()),
                profile_version: COMPLIANCE_PROFILE_VERSION.map(|s: &str| s.to_string()),
                profile_description: COMPLIANCE_PROFILE_DESCRIPTION.map(|s: &str| s.to_string()),
                overall_status: ControlStatus::Passing,
                controls: Vec::new(),
                passing_count: 0,
                warning_count: 0,
                violation_count: 0,
                unknown_count: 0,
                evaluated_at: now,
            };
        }

        // Serialize the current config to JSON for nested value lookups.
        let config_arc = live_config.load_full();
        let config_json = serde_json::to_value(config_arc.as_ref()).unwrap_or_default();

        let mut controls = Vec::with_capacity(overrides.len());
        let mut passing = 0usize;
        let mut warnings = 0usize;
        let mut violations = 0usize;
        let mut unknowns = 0usize;

        for ovr in overrides {
            let current_value = get_nested_value(&config_json, &ovr.key)
                .cloned()
                .unwrap_or(serde_json::Value::Null);

            let status = evaluate_control(ovr, &current_value);

            match status {
                ControlStatus::Passing => passing += 1,
                ControlStatus::Warning => warnings += 1,
                ControlStatus::Violation => violations += 1,
                ControlStatus::Unknown => unknowns += 1,
            }

            controls.push(ComplianceControl {
                key: ovr.key.clone(),
                required_value: ovr.value.clone(),
                current_value,
                constraint_type: ovr.constraint_type.clone(),
                status,
                justification: ovr.justification.clone(),
                profile_name: ovr.profile_name.clone(),
                ui_visibility: format!("{:?}", ovr.ui_visibility),
            });
        }

        let overall_status = if violations > 0 {
            ControlStatus::Violation
        } else if warnings > 0 {
            ControlStatus::Warning
        } else if unknowns > 0 {
            ControlStatus::Unknown
        } else {
            ControlStatus::Passing
        };

        CompliancePosture {
            profile_name: COMPLIANCE_PROFILE_NAME.map(|s: &str| s.to_string()),
            profile_version: COMPLIANCE_PROFILE_VERSION.map(|s: &str| s.to_string()),
            profile_description: COMPLIANCE_PROFILE_DESCRIPTION.map(|s: &str| s.to_string()),
            overall_status,
            controls,
            passing_count: passing,
            warning_count: warnings,
            violation_count: violations,
            unknown_count: unknowns,
            evaluated_at: now,
        }
    }

    /// Check for violations only (subset of evaluate_posture).
    pub fn check_violations(&self, live_config: &LiveConfig) -> Vec<ComplianceViolation> {
        let posture = self.evaluate_posture(live_config);
        let now = posture.evaluated_at;

        let new_violations: Vec<ComplianceViolation> = posture
            .controls
            .into_iter()
            .filter(|c| c.status == ControlStatus::Violation)
            .map(|control| ComplianceViolation {
                detected_at: now,
                control,
                remediation_attempted: false,
                remediation_result: None,
            })
            .collect();

        // Record violations.
        if !new_violations.is_empty() {
            let mut stored = self.violations.lock();
            for v in &new_violations {
                stored.push(v.clone());
            }
            // Trim to max.
            while stored.len() > self.max_violations {
                stored.remove(0);
            }
        }

        new_violations
    }

    /// Generate a compliance report.
    pub fn generate_report(
        &self,
        live_config: &LiveConfig,
        _format: ReportFormat,
    ) -> ComplianceReport {
        let posture = self.evaluate_posture(live_config);

        let build_features = active_compliance_features();

        ComplianceReport {
            title: format!(
                "Compliance Report — {}",
                posture
                    .profile_name
                    .as_deref()
                    .unwrap_or("No Profile")
            ),
            generated_at: posture.evaluated_at,
            posture,
            build_features,
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Generate an HTML compliance report.
    pub fn generate_report_html(
        &self,
        live_config: &LiveConfig,
    ) -> String {
        let report = self.generate_report(live_config, ReportFormat::Html);
        render_html_report(&report)
    }

    /// Get all recorded violations.
    pub fn recorded_violations(&self) -> Vec<ComplianceViolation> {
        self.violations.lock().clone()
    }

    /// Get the compliance manifest (profile metadata + overrides).
    pub fn manifest(live_config: &LiveConfig) -> serde_json::Value {
        let overrides: Vec<_> = live_config
            .registry()
            .compliance_overrides()
            .iter()
            .map(|o| serde_json::to_value(o).unwrap_or_default())
            .collect();

        serde_json::json!({
            "profile_name": COMPLIANCE_PROFILE_NAME,
            "profile_version": COMPLIANCE_PROFILE_VERSION,
            "profile_description": COMPLIANCE_PROFILE_DESCRIPTION,
            "overrides": overrides,
            "overrides_count": overrides.len(),
            "build_features": active_compliance_features(),
        })
    }
}

impl Default for ComplianceEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Control evaluation logic
// ============================================================================

/// Evaluate a single compliance control against a current value.
fn evaluate_control(ovr: &ComplianceOverride, current: &serde_json::Value) -> ControlStatus {
    if current.is_null() {
        return ControlStatus::Unknown;
    }

    match ovr.constraint_type.as_str() {
        "exact" => evaluate_exact(&ovr.value, current),
        "min" => evaluate_min(&ovr.value, current),
        "max" => evaluate_max(&ovr.value, current),
        _ => ControlStatus::Unknown,
    }
}

/// Exact match: current must equal required.
fn evaluate_exact(required: &serde_json::Value, current: &serde_json::Value) -> ControlStatus {
    // For booleans, direct comparison.
    if required.is_boolean() && current.is_boolean() {
        return if required == current {
            ControlStatus::Passing
        } else {
            ControlStatus::Violation
        };
    }

    // For strings, direct comparison.
    if required.is_string() && current.is_string() {
        return if required == current {
            ControlStatus::Passing
        } else {
            ControlStatus::Violation
        };
    }

    // For numbers, compare as f64.
    if let (Some(r), Some(c)) = (required.as_f64(), current.as_f64()) {
        return if (r - c).abs() < f64::EPSILON {
            ControlStatus::Passing
        } else {
            ControlStatus::Violation
        };
    }

    // Generic JSON comparison.
    if required == current {
        ControlStatus::Passing
    } else {
        ControlStatus::Violation
    }
}

/// Minimum constraint: current must be >= required.
fn evaluate_min(required: &serde_json::Value, current: &serde_json::Value) -> ControlStatus {
    // Try duration strings (e.g., "86400s").
    if let (Some(r_secs), Some(c_secs)) = (
        parse_duration_secs(required),
        parse_duration_secs(current),
    ) {
        return if c_secs >= r_secs {
            ControlStatus::Passing
        } else if c_secs as f64 >= r_secs as f64 * 0.9 {
            ControlStatus::Warning
        } else {
            ControlStatus::Violation
        };
    }

    // Numeric comparison.
    if let (Some(r), Some(c)) = (required.as_f64(), current.as_f64()) {
        return if c >= r {
            ControlStatus::Passing
        } else if c >= r * 0.9 {
            ControlStatus::Warning
        } else {
            ControlStatus::Violation
        };
    }

    ControlStatus::Unknown
}

/// Maximum constraint: current must be <= required.
fn evaluate_max(required: &serde_json::Value, current: &serde_json::Value) -> ControlStatus {
    // Try duration strings.
    if let (Some(r_secs), Some(c_secs)) = (
        parse_duration_secs(required),
        parse_duration_secs(current),
    ) {
        return if c_secs <= r_secs {
            ControlStatus::Passing
        } else if c_secs as f64 <= r_secs as f64 * 1.1 {
            ControlStatus::Warning
        } else {
            ControlStatus::Violation
        };
    }

    // Numeric comparison.
    if let (Some(r), Some(c)) = (required.as_f64(), current.as_f64()) {
        return if c <= r {
            ControlStatus::Passing
        } else if c <= r * 1.1 {
            ControlStatus::Warning
        } else {
            ControlStatus::Violation
        };
    }

    ControlStatus::Unknown
}

/// Parse a duration string like "86400s" or "3600s" into seconds.
fn parse_duration_secs(val: &serde_json::Value) -> Option<u64> {
    let s = val.as_str()?;
    let s = s.trim();
    if s.ends_with('s') {
        s[..s.len() - 1].parse::<u64>().ok()
    } else if s.ends_with('m') {
        s[..s.len() - 1].parse::<u64>().ok().map(|m| m * 60)
    } else if s.ends_with('h') {
        s[..s.len() - 1].parse::<u64>().ok().map(|h| h * 3600)
    } else if s.ends_with('d') {
        s[..s.len() - 1].parse::<u64>().ok().map(|d| d * 86400)
    } else {
        s.parse::<u64>().ok()
    }
}

/// Detect which compliance cargo features are active.
fn active_compliance_features() -> Vec<String> {
    let mut features = Vec::new();
    if cfg!(feature = "compliance-soc2") {
        features.push("compliance-soc2".to_string());
    }
    if cfg!(feature = "compliance-hipaa") {
        features.push("compliance-hipaa".to_string());
    }
    if cfg!(feature = "compliance-gdpr") {
        features.push("compliance-gdpr".to_string());
    }
    if cfg!(feature = "compliance-fedramp") {
        features.push("compliance-fedramp".to_string());
    }
    features
}

// ============================================================================
// HTML report rendering
// ============================================================================

fn render_html_report(report: &ComplianceReport) -> String {
    let posture = &report.posture;
    let overall_color = match posture.overall_status {
        ControlStatus::Passing => "#22c55e",
        ControlStatus::Warning => "#f59e0b",
        ControlStatus::Violation => "#ef4444",
        ControlStatus::Unknown => "#6b7280",
    };

    let mut controls_html = String::new();
    for c in &posture.controls {
        let status_color = match c.status {
            ControlStatus::Passing => "#22c55e",
            ControlStatus::Warning => "#f59e0b",
            ControlStatus::Violation => "#ef4444",
            ControlStatus::Unknown => "#6b7280",
        };
        let status_label = match c.status {
            ControlStatus::Passing => "PASS",
            ControlStatus::Warning => "WARN",
            ControlStatus::Violation => "FAIL",
            ControlStatus::Unknown => "N/A",
        };
        controls_html.push_str(&format!(
            r#"<tr>
  <td><code>{key}</code></td>
  <td>{constraint}</td>
  <td><code>{required}</code></td>
  <td><code>{current}</code></td>
  <td style="color:{color};font-weight:bold">{status}</td>
  <td style="font-size:0.85em">{justification}</td>
</tr>"#,
            key = html_escape(&c.key),
            constraint = html_escape(&c.constraint_type),
            required = html_escape(&c.required_value.to_string()),
            current = html_escape(&c.current_value.to_string()),
            color = status_color,
            status = status_label,
            justification = html_escape(&c.justification),
        ));
    }

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1rem; color: #1f2937; }}
  h1 {{ border-bottom: 2px solid #e5e7eb; padding-bottom: 0.5rem; }}
  .badge {{ display: inline-block; padding: 0.25rem 0.75rem; border-radius: 9999px; color: white; font-weight: 600; font-size: 0.9rem; }}
  table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; }}
  th, td {{ text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #e5e7eb; }}
  th {{ background: #f9fafb; font-weight: 600; }}
  .meta {{ display: grid; grid-template-columns: auto 1fr; gap: 0.25rem 1rem; margin: 1rem 0; }}
  .meta dt {{ font-weight: 600; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p>Generated: {generated_at}</p>

<p>Overall status: <span class="badge" style="background:{overall_color}">{overall_status:?}</span></p>

<dl class="meta">
  <dt>Profile</dt><dd>{profile_name}</dd>
  <dt>Version</dt><dd>{profile_version}</dd>
  <dt>Description</dt><dd>{profile_desc}</dd>
  <dt>Build features</dt><dd>{features}</dd>
  <dt>Marabunta version</dt><dd>{version}</dd>
</dl>

<h2>Controls ({total})</h2>
<p>{passing} passing, {warnings} warnings, {violations} violations, {unknowns} unknown</p>

<table>
<thead>
<tr><th>Key</th><th>Constraint</th><th>Required</th><th>Current</th><th>Status</th><th>Justification</th></tr>
</thead>
<tbody>
{controls_html}
</tbody>
</table>
</body>
</html>"#,
        title = html_escape(&report.title),
        generated_at = report.generated_at.to_rfc3339(),
        overall_color = overall_color,
        overall_status = posture.overall_status,
        profile_name = posture.profile_name.as_deref().unwrap_or("None"),
        profile_version = posture.profile_version.as_deref().unwrap_or("N/A"),
        profile_desc = posture.profile_description.as_deref().unwrap_or("N/A"),
        features = report.build_features.join(", "),
        version = html_escape(&report.version),
        total = posture.controls.len(),
        passing = posture.passing_count,
        warnings = posture.warning_count,
        violations = posture.violation_count,
        unknowns = posture.unknown_count,
        controls_html = controls_html,
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- ControlStatus tests --

    #[test]
    fn test_control_status_serde() {
        let json = serde_json::to_string(&ControlStatus::Passing).unwrap();
        assert_eq!(json, "\"passing\"");

        let back: ControlStatus = serde_json::from_str("\"violation\"").unwrap();
        assert_eq!(back, ControlStatus::Violation);
    }

    // -- evaluate_exact tests --

    #[test]
    fn test_exact_bool_match() {
        assert_eq!(
            evaluate_exact(&serde_json::json!(true), &serde_json::json!(true)),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_exact_bool_mismatch() {
        assert_eq!(
            evaluate_exact(&serde_json::json!(true), &serde_json::json!(false)),
            ControlStatus::Violation,
        );
    }

    #[test]
    fn test_exact_string_match() {
        assert_eq!(
            evaluate_exact(&serde_json::json!("hello"), &serde_json::json!("hello")),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_exact_string_mismatch() {
        assert_eq!(
            evaluate_exact(&serde_json::json!("hello"), &serde_json::json!("world")),
            ControlStatus::Violation,
        );
    }

    #[test]
    fn test_exact_number_match() {
        assert_eq!(
            evaluate_exact(&serde_json::json!(42), &serde_json::json!(42)),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_exact_number_mismatch() {
        assert_eq!(
            evaluate_exact(&serde_json::json!(42), &serde_json::json!(43)),
            ControlStatus::Violation,
        );
    }

    // -- evaluate_min tests --

    #[test]
    fn test_min_passing() {
        assert_eq!(
            evaluate_min(&serde_json::json!(100), &serde_json::json!(200)),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_min_exact() {
        assert_eq!(
            evaluate_min(&serde_json::json!(100), &serde_json::json!(100)),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_min_warning() {
        // 95 is >= 100 * 0.9 = 90, so warning.
        assert_eq!(
            evaluate_min(&serde_json::json!(100), &serde_json::json!(95)),
            ControlStatus::Warning,
        );
    }

    #[test]
    fn test_min_violation() {
        // 50 is < 100 * 0.9 = 90, so violation.
        assert_eq!(
            evaluate_min(&serde_json::json!(100), &serde_json::json!(50)),
            ControlStatus::Violation,
        );
    }

    // -- evaluate_max tests --

    #[test]
    fn test_max_passing() {
        assert_eq!(
            evaluate_max(&serde_json::json!(100), &serde_json::json!(50)),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_max_exact() {
        assert_eq!(
            evaluate_max(&serde_json::json!(100), &serde_json::json!(100)),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_max_warning() {
        // 105 is <= 100 * 1.1 = 110, so warning.
        assert_eq!(
            evaluate_max(&serde_json::json!(100), &serde_json::json!(105)),
            ControlStatus::Warning,
        );
    }

    #[test]
    fn test_max_violation() {
        // 200 is > 100 * 1.1 = 110, so violation.
        assert_eq!(
            evaluate_max(&serde_json::json!(100), &serde_json::json!(200)),
            ControlStatus::Violation,
        );
    }

    // -- Duration parsing tests --

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration_secs(&serde_json::json!("86400s")), Some(86400));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration_secs(&serde_json::json!("60m")), Some(3600));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_secs(&serde_json::json!("24h")), Some(86400));
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration_secs(&serde_json::json!("7d")), Some(604800));
    }

    #[test]
    fn test_parse_duration_bare_number() {
        assert_eq!(parse_duration_secs(&serde_json::json!("3600")), Some(3600));
    }

    #[test]
    fn test_parse_duration_not_string() {
        assert_eq!(parse_duration_secs(&serde_json::json!(3600)), None);
    }

    // -- Duration constraint tests --

    #[test]
    fn test_max_duration_passing() {
        // Max 86400s, current 3600s — passing.
        assert_eq!(
            evaluate_max(&serde_json::json!("86400s"), &serde_json::json!("3600s")),
            ControlStatus::Passing,
        );
    }

    #[test]
    fn test_max_duration_violation() {
        // Max 86400s, current 172800s — violation.
        assert_eq!(
            evaluate_max(&serde_json::json!("86400s"), &serde_json::json!("172800s")),
            ControlStatus::Violation,
        );
    }

    #[test]
    fn test_min_duration_passing() {
        // Min 3600s, current 86400s — passing.
        assert_eq!(
            evaluate_min(&serde_json::json!("3600s"), &serde_json::json!("86400s")),
            ControlStatus::Passing,
        );
    }

    // -- evaluate_control tests --

    #[test]
    fn test_evaluate_control_null_value() {
        let ovr = ComplianceOverride {
            key: "test.key".to_string(),
            value: serde_json::json!(true),
            ui_visibility: super::super::config_meta::UiVisibility::VisibleReadonly,
            constraint_type: "exact".to_string(),
            justification: "test".to_string(),
            profile_name: "test".to_string(),
        };
        assert_eq!(
            evaluate_control(&ovr, &serde_json::Value::Null),
            ControlStatus::Unknown,
        );
    }

    #[test]
    fn test_evaluate_control_unknown_constraint() {
        let ovr = ComplianceOverride {
            key: "test.key".to_string(),
            value: serde_json::json!(42),
            ui_visibility: super::super::config_meta::UiVisibility::VisibleReadonly,
            constraint_type: "banana".to_string(),
            justification: "test".to_string(),
            profile_name: "test".to_string(),
        };
        assert_eq!(
            evaluate_control(&ovr, &serde_json::json!(42)),
            ControlStatus::Unknown,
        );
    }

    // -- ComplianceEngine tests --

    #[test]
    fn test_engine_creation() {
        let engine = ComplianceEngine::new();
        assert!(engine.recorded_violations().is_empty());
    }

    #[test]
    fn test_engine_default() {
        let engine = ComplianceEngine::default();
        assert!(engine.recorded_violations().is_empty());
    }

    // -- active_compliance_features tests --

    #[test]
    fn test_active_compliance_features_no_features() {
        // Without compliance features enabled, the list depends on build.
        let features = active_compliance_features();
        // We can't assert the exact contents but can check it's a valid Vec.
        assert!(features.len() <= 4);
    }

    // -- HTML report tests --

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("<script>alert('xss')</script>"), "&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;");
    }

    #[test]
    fn test_html_escape_ampersand() {
        assert_eq!(html_escape("foo & bar"), "foo &amp; bar");
    }

    // -- Serde roundtrip tests --

    #[test]
    fn test_compliance_control_serde() {
        let control = ComplianceControl {
            key: "security.api_auth_required".to_string(),
            required_value: serde_json::json!(true),
            current_value: serde_json::json!(true),
            constraint_type: "exact".to_string(),
            status: ControlStatus::Passing,
            justification: "SOC2 CC6.1".to_string(),
            profile_name: "soc2".to_string(),
            ui_visibility: "VisibleReadonly".to_string(),
        };
        let json = serde_json::to_string(&control).unwrap();
        let back: ComplianceControl = serde_json::from_str(&json).unwrap();
        assert_eq!(back.key, "security.api_auth_required");
        assert_eq!(back.status, ControlStatus::Passing);
    }

    #[test]
    fn test_compliance_posture_serde() {
        let posture = CompliancePosture {
            profile_name: Some("soc2".to_string()),
            profile_version: Some("1.0".to_string()),
            profile_description: Some("SOC 2".to_string()),
            overall_status: ControlStatus::Passing,
            controls: vec![],
            passing_count: 5,
            warning_count: 0,
            violation_count: 0,
            unknown_count: 0,
            evaluated_at: Utc::now(),
        };
        let json = serde_json::to_string(&posture).unwrap();
        let back: CompliancePosture = serde_json::from_str(&json).unwrap();
        assert_eq!(back.profile_name, Some("soc2".to_string()));
        assert_eq!(back.passing_count, 5);
    }

    #[test]
    fn test_compliance_violation_serde() {
        let violation = ComplianceViolation {
            detected_at: Utc::now(),
            control: ComplianceControl {
                key: "test".to_string(),
                required_value: serde_json::json!(true),
                current_value: serde_json::json!(false),
                constraint_type: "exact".to_string(),
                status: ControlStatus::Violation,
                justification: "test".to_string(),
                profile_name: "test".to_string(),
                ui_visibility: "VisibleReadonly".to_string(),
            },
            remediation_attempted: false,
            remediation_result: None,
        };
        let json = serde_json::to_string(&violation).unwrap();
        let back: ComplianceViolation = serde_json::from_str(&json).unwrap();
        assert_eq!(back.control.status, ControlStatus::Violation);
        assert!(!back.remediation_attempted);
    }

    #[test]
    fn test_report_format_serde() {
        let json = serde_json::to_string(&ReportFormat::Json).unwrap();
        assert_eq!(json, "\"json\"");
        let html: ReportFormat = serde_json::from_str("\"html\"").unwrap();
        assert_eq!(html, ReportFormat::Html);
    }
}
